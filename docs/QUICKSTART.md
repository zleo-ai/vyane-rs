# First run and provider setup

Start with the [short quickstart](../README.md#quickstart). This page retains the full setup explanation. Commands run from the repository root.


Install stable Rust with Cargo and a platform C/C++ build toolchain. The declared minimum Rust version is 1.88. The repository selects stable with rustfmt and Clippy. Run from the repository root:

```sh
git clone https://github.com/zleo-ai/vyane-rs.git
cd vyane-rs
cargo install --locked --path crates/vyane-cli --root ./target/install
VYANE="$PWD/target/install/bin/vyane"
"$VYANE" --version
"$VYANE" --help
```

On the `v0.1.0` tag the version prints `vyane 0.1.0`. A later commit adds the git description in parentheses, for example `vyane 0.1.0 (v0.1.0-3-gabcdef0)`. This installs into this checkout, so it does not replace another `vyane` on your PATH. Keep this shell open for the following commands. The `fe43941` baseline identifies the implementation used for verification; it predates the bundled demo files. Reproduce the examples from a revision containing this documentation and `docs/examples/`, and record `git rev-parse HEAD` alongside your output.

### First run without credentials

```sh
cat > ./target/offline-config.toml <<'TOML'
[providers.offline]
base_url = "https://example.invalid/v1"
auth_style = "bearer"
protocol = "openai_chat"
default_model = "offline-placeholder"

[profiles.offline]
provider = "offline"
harness = "none"
model = "offline-placeholder"
TOML
"$VYANE" --config ./target/offline-config.toml check
"$VYANE" --config ./target/offline-config.toml route "Explain this function" --json
```

`check` should report a valid configuration with the `offline` profile. `route` returns JSON with `"selection_key": "offline"` and `"model": "offline-placeholder"` without dispatching. This is an offline configuration example, not a mock model response or a working provider; do not dispatch to its deliberately invalid endpoint.

### Configure a real target

Create `./target/model-config.toml` with the following shape. Replace the example endpoint and model with values supported by your provider; `example.invalid` is deliberately not a working service.

```toml
[providers.demo]
base_url = "https://example.invalid/v1"
api_key_env = "VYANE_DEMO_API_KEY"
auth_style = "bearer"
protocol = "openai_chat"
default_model = "YOUR_PROVIDER_MODEL_ID"

[profiles.chat]
provider = "demo"
protocol = "openai_chat"
harness = "none"
model = "YOUR_PROVIDER_MODEL_ID"
```

Set `VYANE_DEMO_API_KEY` through your normal secret manager or shell environment; never put its value in the TOML or commit it. Then:

```sh
"$VYANE" --config ./target/model-config.toml check
"$VYANE" --config ./target/model-config.toml dispatch "Explain a Rust trait in one paragraph" --target chat
```

The first command checks configuration and credential presence; it does not prove the endpoint accepts your credentials or model. The second sends a real request once the placeholders are replaced. Live provider success is not part of the offline quickstart verification. For coding harnesses, permissions, multiple profiles and failover, see [configuration examples](../profiles.example.toml) and the [CLI guide](CLI-GUIDE.md).

