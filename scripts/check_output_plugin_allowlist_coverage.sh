#!/usr/bin/env bash
# Static lint: any script that configures a real PostgreSQL cluster with both
# wal_level=logical and shared_preload_libraries='pg_flashback' -- i.e. a
# script that will create/use the pg_flashback logical output plugin against
# a real slot -- must route that cluster bootstrap through
# scripts/lib/output_plugin_allowlist.sh. Current security-patched
# PostgreSQL minors reject logical slot creation for an output plugin that
# is not explicitly allowlisted via output_plugin_libraries; a script that
# configures wal_level=logical + shared_preload_libraries=pg_flashback
# without also sourcing the compatibility helper would silently break on
# those minors instead of failing loudly, or worse, appear to hang/timeout
# in a way that is hard to diagnose.
#
# Heuristic: "sets wal_level=logical" AND "sets shared_preload_libraries to
# include pg_flashback" in the same file is the same signal every real
# cluster-bootstrap script in this repo already carries (verified against
# every script under scripts/ at the time this lint was added). A script
# matching both must also contain the string "output_plugin_allowlist.sh"
# (i.e. source the helper) somewhere in the file.
#
# Explicit exceptions: scripts intentionally exercising a real cluster
# WITHOUT the compatibility helper, to prove the documented prerequisite
# failure ("library \"pg_flashback\" may not be used as an output plugin")
# actually occurs on current minors when the allowlist is omitted. Each
# entry must name the file and the reason. There are none today; the map
# stays here as the single place a future such negative test registers
# itself, so this lint can never be silently defeated by adding a bare
# "# shellcheck" or similar comment instead.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

declare -A EXCEPTIONS=()

fail=0
checked=0

while IFS= read -r -d '' f; do
    rel="${f#./}"
    if [[ -n "${EXCEPTIONS[$rel]:-}" ]]; then
        continue
    fi

    sets_wal_level=0
    sets_preload=0
    grep -Eq "wal_level[[:space:]]*=[[:space:]]*'?logical'?" "$f" && sets_wal_level=1
    grep -Eq "shared_preload_libraries[[:space:]]*=[[:space:]]*\\\\?'pg_flashback\\\\?'" "$f" && sets_preload=1

    if [[ "$sets_wal_level" == 1 && "$sets_preload" == 1 ]]; then
        checked=$((checked + 1))
        if ! grep -q "output_plugin_allowlist.sh" "$f"; then
            echo "ERROR: $rel configures wal_level=logical + shared_preload_libraries='pg_flashback' but never sources scripts/lib/output_plugin_allowlist.sh" >&2
            fail=1
        fi
    fi
done < <(find scripts -type f -name '*.sh' -print0)

if [[ "$checked" == 0 ]]; then
    echo "ERROR: output-plugin-allowlist coverage lint matched zero real-slot harness scripts; the detection heuristic itself is broken" >&2
    exit 1
fi

if [[ "$fail" != 0 ]]; then
    echo "FAIL: output-plugin-allowlist coverage lint ($checked real-slot harness scripts checked)" >&2
    exit 1
fi

echo "PASS: output-plugin-allowlist coverage lint ($checked real-slot harness scripts checked, 0 missing the compatibility helper)"
