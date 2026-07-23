#!/usr/bin/env bash
# Focused regression for post-restore verification SQL helpers.
# Usage: ./scripts/run_restore_verify_regression.sh
set -Eeuo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
cargo pgrx test pg17 it_restore_verify_regression --features pg17
