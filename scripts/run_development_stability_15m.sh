#!/usr/bin/env bash
# Development-only accelerated stability/drill regression.
# This wrapper can never emit the exact 24-hour qualification kind.

set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

export PG_FLASHBACK_STABILITY_MODE=accelerated
export PG_FLASHBACK_STABILITY_ACCELERATED_SECONDS=900
export PG_FLASHBACK_SOAK_SAMPLE_INTERVAL_SECONDS=5
export PG_FLASHBACK_SOAK_MAX_WORK_BYTES="${PG_FLASHBACK_SOAK_MAX_WORK_BYTES:-268435456}"
export PG_FLASHBACK_SOAK_WORK_STOP_HEADROOM_BYTES="${PG_FLASHBACK_SOAK_WORK_STOP_HEADROOM_BYTES:-67108864}"
export PGFB_STABILITY_KEEP="${PGFB_STABILITY_KEEP:-0}"

exec "$REPO_ROOT/scripts/run_exact_rc_24h_stability_soak.sh"
