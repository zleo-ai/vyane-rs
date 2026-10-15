# Terminal walkthrough and three uses

These examples were checked against `fe43941` on 2026-10-15. Start at the
[quickstart](../README.md#quickstart); keep the shell with `VYANE` open.
This page distinguishes an actual routing preview, scripted local responses,
and commands that call your own provider. It contains no live-model recording.

## See the execution path without a provider account

After installing, run the bundled demo with Python 3:

```sh
python3 docs/examples/fixture_demo.py --binary "$VYANE"
```

Recorded output from the installed Rust CLI and the local HTTP fixture:

```text
Local HTTP fixtures only; no live model calls.
dispatch: fixture: chat-model; recorded attempts=1
broadcast: 2 separate successful results (chat-model, review-model)
failover: fixture: review-model; recorded attempts=2
```

The Python script starts a loopback-only HTTP server with canned responses,
creates temporary configuration and storage, calls the actual installed CLI,
and checks its JSON results. The primary endpoint deliberately returns HTTP 500;
the backup returns a fixed string. The script stops the server and removes its
temporary directory afterwards. It needs no keys and does not call an external
model. These results prove this bounded execution path, not model quality,
provider availability, harness compatibility, or native session handoff.

## Preview routing without dispatch

```sh
"$VYANE" --config docs/examples/offline.toml route "Explain this function" --json
```

Recorded JSON output:

```json
{
  "selection_key": "offline",
  "provider": "offline",
  "model": "offline-placeholder",
  "effort": "low",
  "tier": "economy",
  "tag": "",
  "intent": "analysis",
  "complexity_score": 0.0,
  "reason": "preference matched (tag/stage/tier): offline"
}
```

This selects from a one-profile example and sends no request. The endpoint is
intentionally invalid. A larger configuration can use tier/tag/stage metadata;
see [routing](CLI-GUIDE.md#smart-routing).

## Prepare your own models

Complete [provider setup](QUICKSTART.md#configure-a-real-target) to create
`./target/model-config.toml` with provider `demo` and profile `chat`. Then add:

```toml
[profiles.review]
provider = "demo"
protocol = "openai_chat"
harness = "none"
model = "YOUR_SECOND_PROVIDER_MODEL_ID"

[profiles.resilient]
provider = "demo"
protocol = "openai_chat"
harness = "none"
model = "YOUR_PROVIDER_MODEL_ID"
failover = ["review"]
```

Replace both model placeholders with models supported by your provider. This
small example uses one provider; for an independent backup account or provider,
add a separate provider configuration and point `review` to it. Two models on
the same account may share the same quota. Credentials stay in environment
variables. `check` verifies configuration and credential presence, not live
endpoint acceptance:

```sh
"$VYANE" --config ./target/model-config.toml check
```

The following commands make real calls once you supply working configuration.
Prompts are literal text: mentioning a file does not attach its contents to a
direct HTTP request. Calls can incur provider charges.

## One task, one target

```sh
"$VYANE" --config ./target/model-config.toml dispatch "Explain a Rust trait in one paragraph" --target chat --json
```

A successful result includes `output` and a run `record` with its attempts.
Use `--stream` instead of `--json` to display streamed text. For repository
work, configure a Claude Code or Codex CLI profile using the
[configuration reference](../profiles.example.toml) and supply that installed
harness and its access separately. The `chat` example above has no file or
shell tools; adapters do not imply support for every model or vendor SDK.

## One prompt, several targets

```sh
"$VYANE" --config ./target/model-config.toml broadcast "Compare an enum and a trait for a plugin interface" --targets chat,review --json
```

Vyane runs the targets concurrently and returns separate results in the input
order. A failed target does not turn another target's answer into consensus.
You compare the answers yourself. Each target can make a billable call.

## An explicit backup

```sh
"$VYANE" --config ./target/model-config.toml dispatch "Explain a Rust trait in one paragraph" --target resilient --json
```

The backup is attempted only after an eligible error. In the current kernel,
authentication, rate-limit, timeout, transport, protocol, process-spawn and
harness failures are eligible. Configuration, unsupported-operation and
cancellation errors stop the chain; the full classifier is in
[`ErrorKind::failover_eligible`](../crates/vyane-core/src/error.rs).
Permission checks still apply. Targets are resolved individually, including
their provider/model binding. This is a new attempt, not transparent session
resume or a guarantee that the backup will succeed.

## Inspect the evidence

These existing regression checks can be rerun without external model access:

```sh
cargo test -p vyane-cli --test cli_acceptance dispatch_openai_chat_writes_success_ledger_and_json_parses -- --exact
cargo test -p vyane-cli --test cli_acceptance dispatch_failover_and_history_work_end_to_end -- --exact
cargo test -p vyane-kernel --test acceptance broadcast_preserves_input_order_with_partial_failure -- --exact
cargo test -p vyane-kernel --test acceptance failover_table_is_mirrored_for_every_error_kind -- --exact
```

The CLI fixtures exercise HTTP dispatch and recorded failover/history. The
kernel fixtures cover broadcast ordering with partial failure and the full
error-classification table. They use local mocks, not installed vendor CLIs.
See the [CLI implementation](../crates/vyane-cli/src/command.rs),
[CLI fixtures](../crates/vyane-cli/tests/cli_acceptance.rs),
[kernel fixtures](../crates/vyane-kernel/tests/acceptance.rs), and
[broader capability/migration limits](PROJECT-STATUS.md).
