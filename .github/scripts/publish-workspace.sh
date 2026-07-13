#!/usr/bin/env bash

# Release the Vyane workspace without hiding partial failures.
#
# The package order is explicit because crates.io's sparse index is eventually
# consistent. Each package is uploaded separately, then its exact version is
# observed through the crates.io API before a dependent package is attempted.
# A rerun after a partial release skips versions that already exist; every other
# failure is retried a bounded number of times and ultimately fails the job.

set -euo pipefail

readonly CRATES_IO_API="https://crates.io/api/v1"
readonly CRATES_IO_USER_AGENT="vyane-rs-release-workflow (github.com/zleo-ai/vyane-rs)"

# This is both the publication order and the complete expected workspace set.
# verify_workspace rejects missing, extra, duplicate, or dependency-misordered
# packages, so adding a crate requires an intentional release-plan update.
readonly PUBLISH_ORDER=(
  vyane-core
  vyane-agent
  vyane-message
  vyane-goal
  vyane-task
  vyane-provider
  vyane-config
  vyane-protocol
  vyane-harness
  vyane-ledger
  vyane-broker
  vyane-kernel
  vyane-router
  vyane-workflow
  vyane-service
  vyane-mcp
  vyane-cli
)

usage() {
  cat >&2 <<'EOF'
usage: publish-workspace.sh <preflight|publish> --tag vX.Y.Z [--allow-dirty]

  preflight    validate the tag/workspace and package+verify every crate
  publish      publish one crate at a time, idempotently, in dependency order

--allow-dirty is intended only for a local preflight of uncommitted changes.
It is rejected in publish mode.
EOF
}

die() {
  echo "release error: $*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

mode="${1:-}"
if [[ -z "$mode" ]]; then
  usage
  exit 2
fi
shift

release_tag=""
allow_dirty=false
while (($# > 0)); do
  case "$1" in
    --tag)
      (($# >= 2)) || die "--tag requires a value"
      release_tag="$2"
      shift 2
      ;;
    --allow-dirty)
      allow_dirty=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

case "$mode" in
  preflight|publish) ;;
  *)
    usage
    exit 2
    ;;
esac

[[ -n "$release_tag" ]] || die "a release tag is required"
[[ "$release_tag" == v* ]] || die "release tag must start with v: $release_tag"
if [[ "$mode" == "publish" && "$allow_dirty" == true ]]; then
  die "--allow-dirty is not permitted while publishing"
fi

require_command cargo
require_command curl
require_command python3

# Validate the exact workspace membership, one shared version, and the
# topological order of every intra-workspace dependency. Prints tab-separated
# version, repository, and target-directory facts on stdout; all diagnostics go
# to stderr so command substitution remains safe.
verify_workspace() {
  local metadata_file workspace_facts
  metadata_file="$(mktemp)"
  cargo metadata --locked --format-version 1 --no-deps >"$metadata_file"
  if ! workspace_facts="$(python3 - "$metadata_file" "${PUBLISH_ORDER[@]}" <<'PY'
import json
import sys

metadata_file = sys.argv[1]
expected = sys.argv[2:]
try:
    with open(metadata_file, "r", encoding="utf-8") as handle:
        metadata = json.load(handle)
except Exception as exc:
    print(f"release error: could not parse cargo metadata: {exc}", file=sys.stderr)
    raise SystemExit(1)

member_ids = set(metadata["workspace_members"])
packages = [pkg for pkg in metadata["packages"] if pkg["id"] in member_ids]
by_name = {pkg["name"]: pkg for pkg in packages}

actual = set(by_name)
expected_set = set(expected)
if len(expected) != len(expected_set):
    print("release error: PUBLISH_ORDER contains a duplicate", file=sys.stderr)
    raise SystemExit(1)
if len(packages) != len(by_name):
    print("release error: workspace contains duplicate package names", file=sys.stderr)
    raise SystemExit(1)
if actual != expected_set:
    missing = sorted(actual - expected_set)
    stale = sorted(expected_set - actual)
    if missing:
        print(
            "release error: workspace packages missing from PUBLISH_ORDER: "
            + ", ".join(missing),
            file=sys.stderr,
        )
    if stale:
        print(
            "release error: PUBLISH_ORDER entries absent from workspace: "
            + ", ".join(stale),
            file=sys.stderr,
        )
    raise SystemExit(1)

versions = {pkg["version"] for pkg in packages}
if len(versions) != 1:
    detail = ", ".join(
        f"{pkg['name']}={pkg['version']}" for pkg in sorted(packages, key=lambda p: p["name"])
    )
    print(f"release error: workspace package versions differ: {detail}", file=sys.stderr)
    raise SystemExit(1)

repositories = {pkg.get("repository") for pkg in packages}
if None in repositories or "" in repositories or len(repositories) != 1:
    detail = ", ".join(
        f"{pkg['name']}={pkg.get('repository')!r}"
        for pkg in sorted(packages, key=lambda p: p["name"])
    )
    print(f"release error: workspace package repositories differ or are missing: {detail}", file=sys.stderr)
    raise SystemExit(1)

target_directory = metadata.get("target_directory")
if not isinstance(target_directory, str) or not target_directory:
    print("release error: cargo metadata omitted target_directory", file=sys.stderr)
    raise SystemExit(1)
if any("\t" in value or "\n" in value for value in (*versions, *repositories, target_directory)):
    print("release error: workspace release facts contain a tab or newline", file=sys.stderr)
    raise SystemExit(1)

position = {name: index for index, name in enumerate(expected)}
for package in packages:
    package_position = position[package["name"]]
    for dependency in package["dependencies"]:
        dependency_name = dependency["name"]
        if dependency_name not in position:
            continue
        if position[dependency_name] >= package_position:
            print(
                "release error: PUBLISH_ORDER places "
                f"{package['name']} before its workspace dependency {dependency_name}",
                file=sys.stderr,
            )
            raise SystemExit(1)

print("\t".join((versions.pop(), repositories.pop(), target_directory)))
PY
  )"; then
    rm -f "$metadata_file"
    return 1
  fi
  rm -f "$metadata_file"
  printf '%s\n' "$workspace_facts"
}

workspace_facts="$(verify_workspace)"
IFS=$'\t' read -r workspace_version workspace_repository target_directory <<<"$workspace_facts"
[[ -n "$workspace_version" && -n "$workspace_repository" && -n "$target_directory" ]] || die \
  "workspace metadata did not produce version, repository, and target directory"
readonly workspace_version workspace_repository target_directory
readonly package_target_directory="${target_directory%/}/package-target"
expected_tag="v${workspace_version}"
[[ "$release_tag" == "$expected_tag" ]] || die \
  "tag/version mismatch: tag is $release_tag but every workspace crate is $workspace_version"

package_archive_path() {
  local crate="$1"
  local version="$2"
  printf '%s/package/%s-%s.crate\n' "$package_target_directory" "$crate" "$version"
}

archive_sha256() {
  local archive="$1"
  python3 - "$archive" <<'PY'
import hashlib
import pathlib
import sys

archive = pathlib.Path(sys.argv[1])
if not archive.is_file():
    print(f"release error: package archive is missing: {archive}", file=sys.stderr)
    raise SystemExit(1)

digest = hashlib.sha256()
with archive.open("rb") as handle:
    for chunk in iter(lambda: handle.read(1024 * 1024), b""):
        digest.update(chunk)
print(digest.hexdigest())
PY
}

verify_package_archives() {
  local crate archive checksum
  for crate in "${PUBLISH_ORDER[@]}"; do
    archive="$(package_archive_path "$crate" "$workspace_version")"
    checksum="$(archive_sha256 "$archive")" || return 1
    [[ "$checksum" =~ ^[0-9a-f]{64}$ ]] || die \
      "invalid SHA-256 for package archive $archive: $checksum"
    echo "package artifact: $crate@$workspace_version sha256=$checksum"
  done
}

clean_package_staging() {
  local package_cargo_home
  package_cargo_home="${target_directory%/}/package-cargo-home"
  [[ -n "$target_directory" \
    && "$target_directory" != "/" \
    && -n "$package_target_directory" \
    && "$package_target_directory" != "/package-target" \
    && "$package_cargo_home" != "/package-cargo-home" ]] || die \
      "refusing to clean unsafe package staging paths"
  # `cargo package --workspace` builds an internal temporary registry and also
  # compiles each unpacked crate. Both the target directory and Cargo home must
  # be isolated: otherwise those builds can leave normal workspace artifacts
  # whose metadata points at the temporary registry, making a subsequent
  # `cargo test --doc` fail before Cargo notices that the source path changed.
  # Reusing either staging area after source changes at the same workspace
  # version can also verify a dependent crate against a stale archive.
  rm -rf -- "$package_target_directory" "$package_cargo_home"
  mkdir -p -- "$package_cargo_home"
  printf '%s\n' "$package_cargo_home"
}

# Validate the crates.io response against the exact local package artifact. HTTP
# 200 alone is not evidence of an idempotent release: the version can be yanked,
# foreign, or the response can be malformed. Missing/invalid fields fail closed.
validate_published_version() {
  local body_file="$1"
  local crate="$2"
  local version="$3"
  local archive
  archive="$(package_archive_path "$crate" "$version")"

  python3 - "$body_file" "$crate" "$version" "$workspace_repository" "$archive" <<'PY'
import hashlib
import json
import pathlib
import re
import sys

body_file, expected_crate, expected_version, expected_repository, archive_raw = sys.argv[1:]
archive = pathlib.Path(archive_raw)

def fail(message: str) -> None:
    print(
        f"release error: crates.io identity check failed for "
        f"{expected_crate}@{expected_version}: {message}",
        file=sys.stderr,
    )
    raise SystemExit(1)

def normalize_repository(value):
    if not isinstance(value, str) or not value.strip():
        return None
    normalized = value.strip().rstrip("/")
    if normalized.lower().endswith(".git"):
        normalized = normalized[:-4]
    return normalized.lower()

try:
    with open(body_file, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
except (OSError, json.JSONDecodeError) as exc:
    fail(f"invalid JSON response: {exc}")

published = payload.get("version") if isinstance(payload, dict) else None
if not isinstance(published, dict):
    fail("response is missing the version object")
if published.get("crate") != expected_crate:
    fail(f"crate field is {published.get('crate')!r}")
if published.get("num") != expected_version:
    fail(f"num field is {published.get('num')!r}")
if published.get("yanked") is not False:
    fail(f"yanked field is {published.get('yanked')!r}, expected false")

actual_repository = published.get("repository")
if normalize_repository(actual_repository) != normalize_repository(expected_repository):
    fail(
        f"repository is {actual_repository!r}, expected {expected_repository!r}"
    )

published_checksum = published.get("checksum")
if not isinstance(published_checksum, str) or re.fullmatch(r"[0-9a-fA-F]{64}", published_checksum) is None:
    fail(f"checksum is missing or invalid: {published_checksum!r}")
if not archive.is_file():
    fail(f"local package archive is missing: {archive}")

local_digest = hashlib.sha256()
with archive.open("rb") as handle:
    for chunk in iter(lambda: handle.read(1024 * 1024), b""):
        local_digest.update(chunk)
local_checksum = local_digest.hexdigest()
if published_checksum.lower() != local_checksum:
    fail(
        f"checksum is {published_checksum.lower()}, local artifact is {local_checksum}"
    )
PY
}

# Emit a synthetic crates.io version response for the verifier self-check. This
# never replaces the live API check; it exercises every fail-closed identity
# branch before the irreversible publish step becomes reachable.
identity_fixture() {
  local crate="$1"
  local version="$2"
  local checksum="$3"
  local yanked="$4"
  local repository="$5"
  python3 - "$crate" "$version" "$checksum" "$yanked" "$repository" <<'PY'
import json
import sys

crate, version, checksum, yanked, repository = sys.argv[1:]
print(
    json.dumps(
        {
            "version": {
                "crate": crate,
                "num": version,
                "checksum": checksum,
                "yanked": yanked == "true",
                "repository": repository,
            }
        }
    )
)
PY
}

self_check_identity_verifier() {
  local crate archive checksum wrong_checksum
  crate="${PUBLISH_ORDER[0]}"
  archive="$(package_archive_path "$crate" "$workspace_version")"
  checksum="$(archive_sha256 "$archive")"
  wrong_checksum="$(printf '0%.0s' {1..64})"

  if ! validate_published_version \
    <(identity_fixture \
      "$crate" \
      "$workspace_version" \
      "$checksum" \
      false \
      "${workspace_repository}.git/") \
    "$crate" \
    "$workspace_version"; then
    die "published-version verifier rejected a valid synthetic identity"
  fi

  if validate_published_version \
    <(identity_fixture "$crate" "$workspace_version" "$checksum" true "$workspace_repository") \
    "$crate" "$workspace_version" 2>/dev/null; then
    die "published-version verifier accepted a yanked artifact"
  fi
  if validate_published_version \
    <(identity_fixture "$crate" "$workspace_version" "$checksum" false "https://example.invalid/foreign") \
    "$crate" "$workspace_version" 2>/dev/null; then
    die "published-version verifier accepted a foreign repository"
  fi
  if validate_published_version \
    <(identity_fixture "$crate" "$workspace_version" "$wrong_checksum" false "$workspace_repository") \
    "$crate" "$workspace_version" 2>/dev/null; then
    die "published-version verifier accepted a checksum mismatch"
  fi
  if validate_published_version \
    <(identity_fixture "foreign-crate" "$workspace_version" "$checksum" false "$workspace_repository") \
    "$crate" "$workspace_version" 2>/dev/null; then
    die "published-version verifier accepted the wrong crate name"
  fi
  if validate_published_version \
    <(identity_fixture "$crate" "9.9.9" "$checksum" false "$workspace_repository") \
    "$crate" "$workspace_version" 2>/dev/null; then
    die "published-version verifier accepted the wrong version"
  fi
  if validate_published_version \
    <(printf '%s\n' '{"version":{}}') \
    "$crate" "$workspace_version" 2>/dev/null; then
    die "published-version verifier accepted missing identity fields"
  fi

  echo "release preflight: published-version identity verifier self-check passed"
}

if [[ "$mode" == "preflight" ]]; then
  package_cargo_home=""
  echo "release preflight: tag $release_tag matches all ${#PUBLISH_ORDER[@]} workspace crates"
  package_args=(--workspace --locked --target-dir "$package_target_directory")
  if [[ "$allow_dirty" == true ]]; then
    package_args+=(--allow-dirty)
  fi
  # Cargo constructs a temporary local registry for workspace packages, then
  # builds each packaged artifact against that registry. This validates all
  # package manifests and dependency edges before any irreversible upload.
  package_cargo_home="$(clean_package_staging)"
  env CARGO_HOME="$package_cargo_home" cargo package "${package_args[@]}"
  verify_package_archives
  self_check_identity_verifier
  echo "release preflight: packaged and verified ${#PUBLISH_ORDER[@]} crates"
  exit 0
fi

verify_package_archives
[[ -n "${CARGO_REGISTRY_TOKEN:-}" ]] || die "CARGO_REGISTRY_TOKEN is not set"

readonly PUBLISH_MAX_ATTEMPTS="${PUBLISH_MAX_ATTEMPTS:-6}"
readonly PUBLISH_RETRY_DELAY_SECONDS="${PUBLISH_RETRY_DELAY_SECONDS:-15}"
readonly PROPAGATION_MAX_ATTEMPTS="${PROPAGATION_MAX_ATTEMPTS:-30}"
readonly PROPAGATION_RETRY_DELAY_SECONDS="${PROPAGATION_RETRY_DELAY_SECONDS:-10}"

for numeric_value in \
  "$PUBLISH_MAX_ATTEMPTS" \
  "$PUBLISH_RETRY_DELAY_SECONDS" \
  "$PROPAGATION_MAX_ATTEMPTS" \
  "$PROPAGATION_RETRY_DELAY_SECONDS"; do
  [[ "$numeric_value" =~ ^[1-9][0-9]*$ ]] || die \
    "release retry settings must be positive integers (got $numeric_value)"
done

# Return codes: 0 = exact verified artifact exists, 1 = not found,
# 2 = API/identity failure.
version_is_published() {
  local crate="$1"
  local version="$2"
  local body_file http_code
  body_file="$(mktemp)"

  if ! http_code="$(curl \
    --silent \
    --show-error \
    --location \
    --retry 4 \
    --retry-delay 2 \
    --retry-all-errors \
    --connect-timeout 15 \
    --max-time 60 \
    --header "User-Agent: $CRATES_IO_USER_AGENT" \
    --output "$body_file" \
    --write-out '%{http_code}' \
    "$CRATES_IO_API/crates/$crate/$version")"; then
    echo "crates.io lookup failed for $crate@$version" >&2
    rm -f "$body_file"
    return 2
  fi

  case "$http_code" in
    200)
      if validate_published_version "$body_file" "$crate" "$version"; then
        rm -f "$body_file"
        return 0
      fi
      rm -f "$body_file"
      return 2
      ;;
    404)
      rm -f "$body_file"
      return 1
      ;;
    *)
      echo "crates.io lookup for $crate@$version returned HTTP $http_code:" >&2
      sed -n '1,20p' "$body_file" >&2
      rm -f "$body_file"
      return 2
      ;;
  esac
}

wait_until_published() {
  local crate="$1"
  local version="$2"
  local attempt status
  for ((attempt = 1; attempt <= PROPAGATION_MAX_ATTEMPTS; attempt++)); do
    if version_is_published "$crate" "$version"; then
      echo "publish observed: $crate@$version"
      return 0
    else
      status=$?
    fi
    if ((status == 2)); then
      return 1
    fi
    if ((attempt < PROPAGATION_MAX_ATTEMPTS)); then
      echo "waiting for crates.io propagation: $crate@$version ($attempt/$PROPAGATION_MAX_ATTEMPTS)"
      sleep "$PROPAGATION_RETRY_DELAY_SECONDS"
    fi
  done
  echo "release error: $crate@$version was not visible after publication" >&2
  return 1
}

publish_one() {
  local crate="$1"
  local version="$2"
  local attempt lookup_status publish_status

  if version_is_published "$crate" "$version"; then
    echo "already published: $crate@$version (idempotent skip)"
    return 0
  else
    lookup_status=$?
  fi
  ((lookup_status == 1)) || return 1

  for ((attempt = 1; attempt <= PUBLISH_MAX_ATTEMPTS; attempt++)); do
    echo "publishing $crate@$version (attempt $attempt/$PUBLISH_MAX_ATTEMPTS)"
    # The token-free preflight already built and verified every archive. Do not
    # rebuild package code while the registry credential is in the environment.
    if cargo publish --locked --no-verify --package "$crate"; then
      wait_until_published "$crate" "$version"
      return
    else
      publish_status=$?
    fi

    # The upload can succeed while the client loses the response. Treat the
    # exact artifact becoming visible as success; this is the only swallowed
    # publish error, and its metadata + checksum are independently verified.
    if version_is_published "$crate" "$version"; then
      echo "publish command exited $publish_status, but $crate@$version now exists; continuing"
      return 0
    else
      lookup_status=$?
    fi
    ((lookup_status == 1)) || return 1

    if ((attempt == PUBLISH_MAX_ATTEMPTS)); then
      echo "release error: cargo publish failed for $crate@$version after $attempt attempts" >&2
      return "$publish_status"
    fi
    echo "publish failed before the version appeared; retrying after ${PUBLISH_RETRY_DELAY_SECONDS}s"
    sleep "$PUBLISH_RETRY_DELAY_SECONDS"
  done
}

for crate in "${PUBLISH_ORDER[@]}"; do
  publish_one "$crate" "$workspace_version"
done

# A final exact-artifact audit makes a partial or foreign release impossible to
# report as successful, including when the last upload response was ambiguous.
for crate in "${PUBLISH_ORDER[@]}"; do
  if ! version_is_published "$crate" "$workspace_version"; then
    die "final crates.io audit did not find $crate@$workspace_version"
  fi
done

echo "release complete: published ${#PUBLISH_ORDER[@]} crates at $workspace_version"
