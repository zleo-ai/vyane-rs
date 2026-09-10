#!/usr/bin/env bash
# Cloud Agent install for vyane-rs. Idempotent: safe to re-run on a warm VM.
#
# The base image already ships a rustup toolchain that honors the pinned
# rust-toolchain.toml (stable + rustfmt + clippy) and a C toolchain for the
# bundled rusqlite build, so this script only adds what is missing: the
# command-sandbox system packages the acceptance tests need, and a warm build
# cache for fmt/clippy/test.
set -euo pipefail

# vyane's command-sandbox acceptance tests exec bubblewrap (`bwrap`); CI also
# installs the AppArmor tooling and keyutils. Only install when bwrap is absent
# so re-runs on a warm/snapshotted VM are fast no-ops.
if ! command -v bwrap >/dev/null 2>&1; then
  sudo apt-get update
  sudo apt-get install --yes \
    apparmor-profiles apparmor-utils bubblewrap keyutils
fi

# Best-effort: load the userns-restrict AppArmor profile when the host exposes
# it, mirroring CI. A VM without a mounted securityfs simply skips this; the
# tests guard on the profile file's presence, so absence is not fatal.
if [[ -f /usr/share/apparmor/extra-profiles/bwrap-userns-restrict ]]; then
  sudo install -m 0644 \
    /usr/share/apparmor/extra-profiles/bwrap-userns-restrict \
    /etc/apparmor.d/bwrap-userns-restrict || true
  sudo apparmor_parser -r /etc/apparmor.d/bwrap-userns-restrict 2>/dev/null || true
fi

# Warm the workspace so fmt/clippy/test start from a primed cache. Building all
# targets compiles the test binaries too. --locked keeps Cargo.lock authoritative.
cargo fetch --locked
cargo build --workspace --all-targets --locked
