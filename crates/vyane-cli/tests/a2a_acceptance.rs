#![allow(clippy::unwrap_used)]

use std::collections::BTreeSet;
#[cfg(target_os = "linux")]
use std::fs::OpenOptions;
use std::path::Path;
#[cfg(target_os = "linux")]
use std::process::{Command as ProcessCommand, Stdio};
#[cfg(target_os = "linux")]
use std::sync::Arc;

use assert_cmd::Command;
#[cfg(target_os = "linux")]
use chrono::{DateTime, TimeDelta, Utc};
use serde_json::Value;
use tempfile::TempDir;
#[cfg(target_os = "linux")]
use vyane_message::{DeliveryStatus, MessageClock, MessageStore as _, SqliteMessageStore};

#[cfg(target_os = "linux")]
struct FixedClock(DateTime<Utc>);

#[cfg(target_os = "linux")]
impl MessageClock for FixedClock {
    fn now(&self) -> DateTime<Utc> {
        self.0
    }
}

fn vyane() -> Command {
    Command::cargo_bin("vyane").expect("vyane binary")
}

fn json_output(args: &[&str], success: bool) -> Value {
    let output = vyane().args(args).output().unwrap();
    assert_eq!(
        output.status.success(),
        success,
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn db_text(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn send(db: &Path, owner: &str, recipient: &str, body: &str) -> Value {
    let db = db_text(db);
    json_output(
        &[
            "a2a", "send", "--db", &db, "--owner", owner, "--json", "--from", "sender", recipient,
            body,
        ],
        true,
    )
}

#[test]
fn send_inbox_read_round_trip_has_stable_json_and_strict_scope() {
    let directory = TempDir::new().unwrap();
    let db = directory.path().join("messages.sqlite3");
    let db_text = db_text(&db);
    let sent = json_output(
        &[
            "a2a",
            "send",
            "--db",
            &db_text,
            "--owner-user-id",
            "owner-a",
            "--json",
            "--from-code",
            "sender",
            "--thread-id",
            "thread-1",
            "--trace-id",
            "trace-1",
            "--kind",
            "review",
            "--payload",
            "card=public-1",
            "recipient",
            "review ready",
        ],
        true,
    );
    assert_eq!(sent["status"], "success");
    let message = &sent["message"];
    assert_eq!(message["from_code"], "sender");
    assert_eq!(message["to_code"], "recipient");
    assert_eq!(message["body"], "review ready");
    assert_eq!(message["thread_id"], "thread-1");
    assert_eq!(message["trace_id"], "trace-1");
    assert_eq!(message["kind"], "review");
    assert_eq!(message["payload"]["card"], "public-1");
    assert_eq!(message["owner_user_id"], "owner-a");
    assert_eq!(message["delivery_status"], "pending");
    let keys = message
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        keys,
        BTreeSet::from([
            "body",
            "created_at",
            "deliver_after",
            "delivered_at",
            "delivery_status",
            "from_code",
            "id",
            "kind",
            "owner_user_id",
            "payload",
            "read_at",
            "thread_id",
            "to_code",
            "trace_id",
        ])
    );
    let message_id = message["id"].as_str().unwrap();

    let owner_b = json_output(
        &[
            "a2a",
            "inbox",
            "--db",
            &db_text,
            "--owner",
            "owner-b",
            "--json",
            "recipient",
        ],
        true,
    );
    assert_eq!(owner_b["messages"], serde_json::json!([]));
    let sender_mailbox = json_output(
        &[
            "a2a", "inbox", "--db", &db_text, "--owner", "owner-a", "--json", "sender",
        ],
        true,
    );
    assert_eq!(sender_mailbox["messages"], serde_json::json!([]));

    let inbox = json_output(
        &[
            "a2a",
            "inbox",
            "--db",
            &db_text,
            "--owner",
            "owner-a",
            "--json",
            "recipient",
        ],
        true,
    );
    assert_eq!(inbox["count"], 1);
    assert_eq!(inbox["has_more"], false);
    assert_eq!(inbox["messages"][0]["id"], message_id);

    let cross_mailbox = json_output(
        &[
            "a2a",
            "read",
            "--db",
            &db_text,
            "--owner",
            "owner-a",
            "--json",
            "other-recipient",
            message_id,
        ],
        false,
    );
    assert_eq!(cross_mailbox["status"], "error");
    assert!(cross_mailbox["error"].as_str().unwrap().contains("absent"));
    let cross_owner = json_output(
        &[
            "a2a",
            "read",
            "--db",
            &db_text,
            "--owner",
            "owner-b",
            "--json",
            "recipient",
            message_id,
        ],
        false,
    );
    assert_eq!(cross_owner["status"], "error");

    let read = json_output(
        &[
            "a2a",
            "read",
            "--db",
            &db_text,
            "--owner",
            "owner-a",
            "--json",
            "recipient",
            message_id,
        ],
        true,
    );
    assert_eq!(read["message"]["id"], message_id);
    assert_eq!(read["message"]["delivery_status"], "delivered");
    assert!(!read["message"]["delivered_at"].is_null());
    assert!(read["message"]["read_at"].is_null());

    let empty = json_output(
        &[
            "a2a",
            "inbox",
            "--db",
            &db_text,
            "--owner",
            "owner-a",
            "--json",
            "recipient",
        ],
        true,
    );
    assert_eq!(empty["messages"], serde_json::json!([]));
    let history = json_output(
        &[
            "a2a",
            "inbox",
            "--db",
            &db_text,
            "--owner",
            "owner-a",
            "--json",
            "--include-read",
            "recipient",
        ],
        true,
    );
    assert_eq!(history["messages"][0]["id"], message_id);
    assert_eq!(history["messages"][0]["delivery_status"], "acknowledged");
    assert!(!history["messages"][0]["read_at"].is_null());

    let repeated = json_output(
        &[
            "a2a",
            "read",
            "--db",
            &db_text,
            "--owner",
            "owner-a",
            "--json",
            "recipient",
            message_id,
        ],
        false,
    );
    assert_eq!(repeated["status"], "error");
}

#[cfg(target_os = "linux")]
#[test]
fn read_does_not_acknowledge_when_stdout_rejects_the_response() {
    let directory = TempDir::new().unwrap();
    let db = directory.path().join("messages.sqlite3");
    let db_text = db_text(&db);
    let sent = send(&db, "local", "recipient", "must remain recoverable");
    let message_id = sent["message"]["id"].as_str().unwrap();
    let full = OpenOptions::new().write(true).open("/dev/full").unwrap();

    let binary = vyane().get_program().to_owned();
    let status = ProcessCommand::new(binary)
        .args([
            "a2a",
            "read",
            "--db",
            &db_text,
            "--json",
            "recipient",
            message_id,
        ])
        .stdout(Stdio::from(full))
        .status()
        .unwrap();
    assert!(!status.success());

    let store = SqliteMessageStore::open(&db).unwrap();
    let bundle = store.get("local", message_id).unwrap().unwrap();
    assert_eq!(bundle.deliveries[0].status, DeliveryStatus::Delivered);
    assert!(bundle.deliveries[0].acknowledged_at.is_none());

    let future = Utc::now()
        .checked_add_signed(TimeDelta::seconds(60))
        .unwrap();
    let reclaiming =
        SqliteMessageStore::open_with_clock(&db, Arc::new(FixedClock(future))).unwrap();
    assert_eq!(reclaiming.reclaim_expired("local", 10).unwrap(), 1);
    let reclaimed = reclaiming.get("local", message_id).unwrap().unwrap();
    assert_eq!(reclaimed.deliveries[0].status, DeliveryStatus::Pending);
}

#[test]
fn human_read_prints_the_body_and_help_states_the_local_authority_boundary() {
    let directory = TempDir::new().unwrap();
    let db = directory.path().join("messages.sqlite3");
    let db_text = db_text(&db);
    let sent = send(&db, "local", "recipient", "visible review body");
    let message_id = sent["message"]["id"].as_str().unwrap();

    let output = vyane()
        .args(["a2a", "read", "--db", &db_text, "recipient", message_id])
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("visible review body"));

    let a2a_help = vyane().args(["a2a", "--help"]).output().unwrap();
    let a2a_help = String::from_utf8(a2a_help.stdout).unwrap();
    assert!(a2a_help.contains("local SQLite inbox"));
    assert!(a2a_help.contains("not authenticated A2A protocol transport"));

    let send_help = vyane().args(["a2a", "send", "--help"]).output().unwrap();
    let send_help = String::from_utf8(send_help.stdout).unwrap();
    assert!(send_help.contains("Caller-supplied sender label"));
    assert!(send_help.contains("not an authenticated identity"));
}

#[test]
fn delayed_messages_and_bounded_pages_are_explicit() {
    let directory = TempDir::new().unwrap();
    let db = directory.path().join("messages.sqlite3");
    let db_text = db_text(&db);
    let delayed = json_output(
        &[
            "a2a",
            "send",
            "--db",
            &db_text,
            "--json",
            "--from",
            "sender",
            "--delay-seconds",
            "3600",
            "recipient",
            "later",
        ],
        true,
    );
    let first = send(&db, "local", "recipient", "first");
    let second = send(&db, "local", "recipient", "second");

    let due = json_output(
        &[
            "a2a",
            "inbox",
            "--db",
            &db_text,
            "--json",
            "--limit",
            "1",
            "recipient",
        ],
        true,
    );
    assert_eq!(due["count"], 1);
    assert_eq!(due["has_more"], true);
    assert_eq!(due["messages"][0]["id"], first["message"]["id"]);

    let all = json_output(
        &[
            "a2a",
            "inbox",
            "--db",
            &db_text,
            "--json",
            "--include-future",
            "recipient",
        ],
        true,
    );
    assert_eq!(all["count"], 3);
    assert_eq!(all["messages"][0]["id"], first["message"]["id"]);
    assert_eq!(all["messages"][1]["id"], second["message"]["id"]);
    assert_eq!(all["messages"][2]["id"], delayed["message"]["id"]);
}

#[test]
fn invalid_inputs_return_stable_json_errors() {
    let directory = TempDir::new().unwrap();
    let db = directory.path().join("messages.sqlite3");
    let db_text = db_text(&db);
    let payload = json_output(
        &[
            "a2a",
            "send",
            "--db",
            &db_text,
            "--json",
            "--from",
            "sender",
            "--payload",
            "[]",
            "recipient",
            "body",
        ],
        false,
    );
    assert_eq!(payload["status"], "error");
    assert!(payload["error"].as_str().unwrap().contains("object"));
    assert!(!db.exists());

    let limit = json_output(
        &[
            "a2a",
            "inbox",
            "--db",
            &db_text,
            "--json",
            "--limit",
            "0",
            "recipient",
        ],
        false,
    );
    assert_eq!(limit["status"], "error");
    assert!(limit["error"].as_str().unwrap().contains("between 1"));
    assert!(!db.exists());

    let empty_target = json_output(
        &[
            "a2a", "send", "--db", &db_text, "--json", "--from", "sender", "", "body",
        ],
        false,
    );
    assert_eq!(empty_target["status"], "error");
    assert!(
        empty_target["error"]
            .as_str()
            .unwrap()
            .contains("target id")
    );

    let output = vyane()
        .args([
            "a2a",
            "send",
            "--db",
            &db_text,
            "--json",
            "--from",
            "sender",
            "recipient",
        ])
        .write_stdin("")
        .output()
        .unwrap();
    assert!(!output.status.success());
    let empty_body: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(empty_body["status"], "error");
    assert!(
        empty_body["error"]
            .as_str()
            .unwrap()
            .contains("body is required")
    );
}
