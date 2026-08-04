#!/usr/bin/env bash
# Run the full cargo-deny gate (advisories + bans + licenses + sources).
#
# The project's build toolchain is pinned to rustc 1.85 (see
# rust-toolchain.toml). cargo-deny releases new enough to parse CVSS 4.0
# advisory entries (0.20.x+) require rustc 1.88+ to *build*, which is a
# constraint on the cargo-deny binary only -- it does not affect the pinned
# build toolchain used to compile pg_flashback itself. cargo-deny is a
# dev/CI tool, not a project dependency, so building it with a newer
# toolchain does not change the extension's MSRV.
#
# This script installs (once, cached) a CVSS-4.0-capable cargo-deny using a
# separate `stable` rustup toolchain, then runs the complete check against
# this repository's Cargo.lock. It never touches the pinned 1.85 toolchain
# used by cargo pgrx build/test.
#
# Usage:
#   ./scripts/run_cargo_deny_full.sh

set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

MIN_VERSION="0.20.0"

version_ge() {
    # returns 0 if $1 >= $2 (dotted numeric versions)
    [[ "$(printf '%s\n%s\n' "$1" "$2" | sort -V | head -n1)" == "$2" ]]
}

current_version=""
if command -v cargo-deny >/dev/null 2>&1; then
    current_version="$(cargo-deny --version 2>/dev/null | awk '{print $2}')"
fi

if [[ -z "$current_version" ]] || ! version_ge "$current_version" "$MIN_VERSION"; then
    echo "cargo-deny missing or too old for CVSS 4.0 support (found: ${current_version:-none}, need >= $MIN_VERSION)" >&2
    if ! rustup toolchain list | grep -q '^stable'; then
        echo "installing rustup stable toolchain (build tool only, not the project MSRV)..." >&2
        rustup toolchain install stable --profile minimal
    fi
    echo "installing cargo-deny >= $MIN_VERSION via the stable toolchain..." >&2
    cargo +stable install cargo-deny --version "$MIN_VERSION" --locked
fi

cargo-deny --version

cargo deny check
