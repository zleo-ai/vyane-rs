#!/usr/bin/env bash
# Cloud Agent environment bootstrap for vyane-rs.
#
# Idempotent: safe to run repeatedly and against cached/partial state.
# System packages here mirror the Linux job in .github/workflows/ci.yml so
# the bubblewrap-backed command-sandbox tests run for real instead of being
# skipped. Rust itself ships in the base image (stable, edition 2024).
set -euo pipefail

# --- Linux command sandbox dependencies (bubblewrap confinement) ----------
# The native command tool shells out to /usr/bin/bwrap and /usr/bin/keyctl;
# install them only when missing so re-runs stay fast.
if ! command -v bwrap >/dev/null 2>&1 || ! command -v keyctl >/dev/null 2>&1; then
  sudo apt-get update
  sudo apt-get install --yes \
    apparmor-profiles apparmor-utils bubblewrap keyutils
fi

# Best-effort AppArmor userns-restrict profile (CI parity). Harmless to skip:
# on hosts that do not enforce a userns-restrict profile, bwrap already works.
if [[ -f /usr/share/apparmor/extra-profiles/bwrap-userns-restrict ]]; then
  sudo install -m 0644 \
    /usr/share/apparmor/extra-profiles/bwrap-userns-restrict \
    /etc/apparmor.d/bwrap-userns-restrict 2>/dev/null || true
  sudo apparmor_parser -r /etc/apparmor.d/bwrap-userns-restrict 2>/dev/null || true
fi

# --- Rust dependencies + warm build ---------------------------------------
# --locked honors Cargo.lock; --all-targets also compiles the test/bench
# targets so the first `cargo test` after boot is fast.
cargo fetch --locked
cargo build --workspace --all-targets --locked

echo "vyane-rs environment ready: $(cargo --version), bwrap $(bwrap --version 2>/dev/null || echo 'n/a')"
