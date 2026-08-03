use std::io::{Read as _, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{Context as _, Result, anyhow, bail};
use chrono::{DateTime, TimeDelta, Utc};
use serde::Serialize;
use serde_json::{Map, Value};
use uuid::Uuid;
use vyane_message::{
    DeliveryMailbox, DeliveryRecord, EndpointKind, EndpointRef, IdempotencyKey, LeaseRequest,
    MailboxQuery, MessageDirection, MessageRecord, MessageStore, NewDelivery, NewMessage,
    SqliteMessageStore,
};

use crate::app::StoragePaths;
use crate::cli::{A2aCommand, A2aCommonArgs, A2aInboxArgs, A2aReadArgs, A2aSendArgs};

const A2A_ROUTE: &str = "local-a2a";
const READ_CONSUMER: &str = "vyane-a2a-cli";
const READ_LEASE_SECONDS: u64 = 30;

#[derive(Debug, Serialize)]
struct MessageView {
    id: String,
    from_code: String,
    to_code: String,
    body: String,
    created_at: DateTime<Utc>,
    deliver_after: DateTime<Utc>,
    owner_user_id: String,
    delivered_at: Option<DateTime<Utc>>,
    read_at: Option<DateTime<Utc>>,
    thread_id: String,
    trace_id: Option<String>,
    kind: String,
    payload: Value,
    delivery_status: String,
}

#[derive(Debug, Serialize)]
struct SendOutput {
    status: &'static str,
    message: MessageView,
    db: String,
}

#[derive(Debug, Serialize)]
struct InboxOutput {
    status: &'static str,
    messages: Vec<MessageView>,
    count: usize,
    has_more: bool,
    db: String,
}

#[derive(Debug, Serialize)]
struct ReadOutput {
    status: &'static str,
    message: MessageView,
    db: String,
}

#[derive(Debug, Serialize)]
struct ErrorOutput<'a> {
    status: &'static str,
    error: &'a str,
}

pub fn run(command: A2aCommand) -> Result<ExitCode> {
    let json = common(&command).json;
    match run_inner(command) {
        Ok(code) => Ok(code),
        Err(error) => {
            let message = format!("{error:#}");
            if json {
                if let Err(write_error) = print_json(&ErrorOutput {
                    status: "error",
                    error: &message,
                }) {
                    eprintln!(
                        "{}",
                        crate::output::format_a2a_error_line(&format!(
                            "{message}; could not write JSON error: {write_error:#}"
                        ))
                    );
                }
            } else {
                eprintln!("{}", crate::output::format_a2a_error_line(&message));
            }
            Ok(ExitCode::from(2))
        }
    }
}

fn run_inner(command: A2aCommand) -> Result<ExitCode> {
    match command {
        A2aCommand::Send(args) => send(args),
        A2aCommand::Inbox(args) => inbox(args),
        A2aCommand::Read(args) => read(args),
    }
}

fn send(args: A2aSendArgs) -> Result<ExitCode> {
    let body = read_body(args.body)?;
    let payload = parse_payload(&args.payload)?;
    let now = Utc::now();
    let available_at = args
        .delay_seconds
        .map(|seconds| {
            let seconds = i64::try_from(seconds)
                .map_err(|_| anyhow!("--delay-seconds is outside the supported range"))?;
            now.checked_add_signed(TimeDelta::seconds(seconds))
                .ok_or_else(|| anyhow!("--delay-seconds is outside the supported range"))
        })
        .transpose()?;
    let (store, db) = open_store(&args.common)?;
    let conversation_id = args
        .thread_id
        .unwrap_or_else(|| format!("a2a-{}", Uuid::now_v7()));
    let message = NewMessage {
        conversation_id,
        session_id: None,
        direction: MessageDirection::Internal,
        kind: args.kind,
        sender: EndpointRef {
            kind: EndpointKind::Agent,
            id: args.from.clone(),
        },
        body,
        payload,
        reply_to: None,
        trace_id: args.trace_id,
        correlation_id: None,
        idempotency: IdempotencyKey {
            producer: format!("a2a-cli:{}", args.from),
            key: Uuid::now_v7().to_string(),
        },
        deliveries: vec![NewDelivery {
            route: A2A_ROUTE.into(),
            target: EndpointRef {
                kind: EndpointKind::Agent,
                id: args.to,
            },
            available_at,
            expires_at: None,
            max_attempts: 3,
        }],
    };
    let outcome = store
        .enqueue(&args.common.owner, &message)
        .context("queue local A2A message")?;
    let delivery = outcome
        .bundle
        .deliveries
        .first()
        .ok_or_else(|| anyhow!("queued message has no delivery"))?;
    let view = message_view(&outcome.bundle.message, delivery);
    if args.common.json {
        print_json(&SendOutput {
            status: "success",
            message: view,
            db: path_text(&db),
        })?;
    } else {
        println!("{}", format_a2a_send_line(&view));
    }
    Ok(ExitCode::SUCCESS)
}

fn inbox(args: A2aInboxArgs) -> Result<ExitCode> {
    if !(1..=1_000).contains(&args.limit) {
        bail!("--limit must be between 1 and 1000");
    }
    let (store, db) = open_store(&args.common)?;
    let mailbox = mailbox(&args.to);
    let page = store
        .list_mailbox(
            &args.common.owner,
            &mailbox,
            &MailboxQuery {
                include_acknowledged: args.include_read,
                include_future: args.include_future,
                limit: args.limit,
            },
        )
        .context("list local A2A mailbox")?;
    let views = page
        .items
        .iter()
        .map(|item| message_view(&item.message, &item.delivery))
        .collect::<Vec<_>>();
    if args.common.json {
        print_json(&InboxOutput {
            status: "success",
            count: views.len(),
            messages: views,
            has_more: page.has_more,
            db: path_text(&db),
        })?;
    } else {
        for view in views {
            println!("{}", format_a2a_inbox_line(&view));
        }
        if page.has_more {
            eprintln!("{}", crate::output::format_a2a_more_messages_line());
        }
    }
    Ok(ExitCode::SUCCESS)
}

fn read(args: A2aReadArgs) -> Result<ExitCode> {
    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();
    read_with_writer(args, &mut stdout)
}

fn read_with_writer(args: A2aReadArgs, stdout: &mut impl Write) -> Result<ExitCode> {
    let (store, db) = open_store(&args.common)?;
    let mailbox = mailbox(&args.to);
    let claimed = store
        .claim_message(
            &args.common.owner,
            &mailbox,
            &args.message_id,
            &LeaseRequest {
                consumer: READ_CONSUMER.into(),
                lease_seconds: READ_LEASE_SECONDS,
            },
        )
        .context("claim exact local A2A message")?
        .ok_or_else(|| {
            anyhow!("message is absent, already read, or not currently claimable in this mailbox")
        })?;
    let delivered = store
        .mark_delivered(&args.common.owner, &mailbox, &claimed.receipt)
        .context("mark local A2A message delivered")?;
    let view = message_view(&claimed.message, &delivered);
    if args.common.json {
        write_json(
            stdout,
            &ReadOutput {
                status: "success",
                message: view,
                db: path_text(&db),
            },
        )?;
    } else {
        write!(stdout, "{}", format_a2a_read_message(&view))
            .context("write local A2A message response")?;
        stdout.flush().context("flush local A2A message response")?;
    }
    store
        .acknowledge(&args.common.owner, &mailbox, &claimed.receipt)
        .context("acknowledge local A2A message after response flush")?;
    Ok(ExitCode::SUCCESS)
}

fn open_store(common: &A2aCommonArgs) -> Result<(SqliteMessageStore, PathBuf)> {
    let path = match &common.db {
        Some(path) => path.clone(),
        None => StoragePaths::resolve()?.message_db_path(),
    };
    let store = match SqliteMessageStore::open(&path) {
        Ok(store) => store,
        Err(error) => {
            // Kind-only pure line; path stays only in anyhow context (WP-392).
            tracing::error!(
                error = error.as_str(),
                "{}",
                crate::output::format_message_store_error_kind_line(&error)
            );
            return Err(error).with_context(|| format!("open message database {}", path.display()));
        }
    };
    Ok((store, path))
}

fn common(command: &A2aCommand) -> &A2aCommonArgs {
    match command {
        A2aCommand::Send(args) => &args.common,
        A2aCommand::Inbox(args) => &args.common,
        A2aCommand::Read(args) => &args.common,
    }
}

fn mailbox(to: &str) -> DeliveryMailbox {
    DeliveryMailbox {
        route: A2A_ROUTE.into(),
        target: EndpointRef {
            kind: EndpointKind::Agent,
            id: to.into(),
        },
    }
}

fn read_body(words: Vec<String>) -> Result<String> {
    let mut body = words.join(" ").trim().to_string();
    if body.is_empty() {
        std::io::stdin()
            .read_to_string(&mut body)
            .context("read A2A message body from stdin")?;
        body = body.trim().to_string();
    }
    if body.is_empty() {
        bail!("message body is required");
    }
    Ok(body)
}

fn parse_payload(items: &[String]) -> Result<Value> {
    let mut payload = Map::new();
    for raw in items {
        let item = raw.trim();
        if item.is_empty() {
            continue;
        }
        if item.starts_with('{') || item.starts_with('[') {
            let value: Value = serde_json::from_str(item).context("parse --payload JSON")?;
            let Value::Object(values) = value else {
                bail!("JSON payload must be an object");
            };
            payload.extend(values);
            continue;
        }
        let Some((key, value)) = item.split_once('=') else {
            bail!("payload item must be KEY=VALUE or a JSON object");
        };
        let key = key.trim();
        if key.is_empty() {
            bail!("payload key must not be empty");
        }
        payload.insert(key.to_string(), Value::String(value.to_string()));
    }
    Ok(Value::Object(payload))
}

fn message_view(message: &MessageRecord, delivery: &DeliveryRecord) -> MessageView {
    // Kind-only pure line; no bodies/ids in the pure status message (WP-391).
    tracing::info!(
        status = delivery.status.as_str(),
        "{}",
        crate::output::format_delivery_status_line(delivery.status)
    );
    MessageView {
        id: message.id.clone(),
        from_code: message.sender.id.clone(),
        to_code: delivery.target.id.clone(),
        body: message.body.clone(),
        created_at: message.created_at,
        deliver_after: delivery.available_at,
        owner_user_id: message.owner.clone(),
        delivered_at: delivery.first_delivered_at,
        read_at: delivery.acknowledged_at,
        thread_id: message.conversation_id.clone(),
        trace_id: message.trace_id.clone(),
        kind: message.kind.clone(),
        payload: message.payload.clone(),
        delivery_status: delivery.status.as_str().to_string(),
    }
}

fn print_json(value: &impl Serialize) -> Result<()> {
    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();
    write_json(&mut stdout, value)
}

fn write_json(writer: &mut impl Write, value: &impl Serialize) -> Result<()> {
    serde_json::to_writer(&mut *writer, value).context("write JSON response")?;
    writer.write_all(b"\n").context("finish JSON response")?;
    writer.flush().context("flush JSON response")
}

fn path_text(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn terminal_safe(value: &str) -> String {
    value.chars().flat_map(char::escape_default).collect()
}

/// Human one-line local A2A send result: terminal-safe message id.
fn format_a2a_send_line(view: &MessageView) -> String {
    terminal_safe(&view.id)
}

/// Human one-line local A2A inbox row: id, from, to, kind, delivery status.
fn format_a2a_inbox_line(view: &MessageView) -> String {
    format!(
        "{} {} -> {} {} {}",
        terminal_safe(&view.id),
        terminal_safe(&view.from_code),
        terminal_safe(&view.to_code),
        terminal_safe(&view.kind),
        terminal_safe(&view.delivery_status),
    )
}

/// Human local A2A read projection: header line then body line (both terminal-safe).
fn format_a2a_read_message(view: &MessageView) -> String {
    format!(
        "{} {} -> {} {}\n{}\n",
        terminal_safe(&view.id),
        terminal_safe(&view.from_code),
        terminal_safe(&view.to_code),
        terminal_safe(&view.kind),
        terminal_safe(&view.body),
    )
}

#[cfg(test)]
mod tests {
    use super::{
        MessageView, format_a2a_inbox_line, format_a2a_read_message, format_a2a_send_line,
    };
    use chrono::{TimeZone, Utc};
    use serde_json::json;

    fn sample_view(id: &str, body: &str) -> MessageView {
        let at = Utc.with_ymd_and_hms(2026, 8, 2, 17, 0, 0).unwrap();
        MessageView {
            id: id.into(),
            from_code: "alice\n".into(),
            to_code: "bob".into(),
            body: body.into(),
            created_at: at,
            deliver_after: at,
            owner_user_id: "local".into(),
            delivered_at: None,
            read_at: None,
            thread_id: "t1".into(),
            trace_id: None,
            kind: "text\tplain".into(),
            payload: json!({}),
            delivery_status: "available".into(),
        }
    }

    #[test]
    fn human_a2a_send_line_is_terminal_safe_id() {
        let view = sample_view("msg-1\u{1b}[31m", "hello");
        let text = format_a2a_send_line(&view);
        assert!(!text.contains('\u{1b}'), "no raw ESC:\n{text}");
        assert!(text.contains("msg-1"), "{text}");
    }

    #[test]
    fn human_a2a_inbox_line_prints_safe_route_and_status() {
        let view = sample_view("m1", "body");
        let text = format_a2a_inbox_line(&view);
        assert!(text.contains("m1"), "{text}");
        assert!(text.contains("alice\\n"), "{text}");
        assert!(text.contains("-> bob"), "{text}");
        assert!(text.contains("text\\tplain"), "{text}");
        assert!(text.contains("available"), "{text}");
        assert!(!text.contains('\n') || text.contains("\\n"), "{text}");
    }

    #[test]
    fn human_a2a_read_message_prints_header_and_body() {
        let view = sample_view("m2", "hello\tworld\u{1b}");
        let text = format_a2a_read_message(&view);
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2, "{text}");
        assert!(lines[0].contains("m2"), "{text}");
        assert!(lines[0].contains("-> bob"), "{text}");
        assert!(!text.contains('\u{1b}'), "no raw ESC:\n{text}");
        assert!(lines[1].contains("hello"), "{text}");
    }
}
