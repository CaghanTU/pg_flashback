#!/usr/bin/env bash
# Fail if production SQL outside SnapshotStore (sql/functions/snapshot_store.sql)
# reintroduces a direct coupling to the snapshot payload's physical storage:
# raw CTAS of a snapshot relation, a direct DROP of one, using snapshot_table
# as a restore source, to_regclass(snapshot_table) backend resolution, a
# direct INSERT/UPDATE/DELETE on flashback.snapshots, or
# pg_total_relation_size(snapshot_table).
#
# Allowlist is strictly narrow and file-based (SnapshotStore's own implementation
# and schema_bootstrap.sql's migration/backfill). Generic comment-based allow-blocks
# (snapshot-store-lint:allow-block) are forbidden and rejected unconditionally.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

python3 - <<'EOF'
import sys, os, re

ALLOWLISTED_FILES = {"snapshot_store.sql", "schema_bootstrap.sql"}

def clean_sql(text):
    text = re.sub(r'--.*$', '', text, flags=re.MULTILINE)
    text = re.sub(r'/\*.*?\*/', '', text, flags=re.DOTALL)
    return text

CHECKS = [
    (
        "CTAS reintroduced for snapshot payload (use flashback_internal_snapshot_create)",
        re.compile(r'CREATE\s+TABLE\s+\S+\s+AS\s+TABLE\b', re.IGNORECASE),
    ),
    (
        "Direct DROP of snapshot payload (use flashback_internal_snapshot_retire)",
        re.compile(r'\bflashback_drop_payload_table\s*\(\s*(?:to_regclass\s*\()?[^)]*snapshot_table\b', re.IGNORECASE),
    ),
    (
        "to_regclass(snapshot_table) backend resolution (use flashback_internal_snapshot_resolve)",
        re.compile(r'to_regclass\s*\(\s*[a-zA-Z_][a-zA-Z0-9_.]*\.?snapshot_table\b', re.IGNORECASE),
    ),
    (
        "Direct UPDATE on flashback.snapshots (use SnapshotStore primitives)",
        re.compile(r'\bUPDATE\s+flashback\.snapshots\b', re.IGNORECASE),
    ),
    (
        "Scattered INSERT into flashback.snapshots outside SnapshotStore",
        re.compile(r'\bINSERT\s+INTO\s+flashback\.snapshots\b', re.IGNORECASE),
    ),
    (
        "Scattered DELETE from flashback.snapshots outside SnapshotStore",
        re.compile(r'\bDELETE\s+FROM\s+flashback\.snapshots\b', re.IGNORECASE),
    ),
    (
        "New direct pg_total_relation_size(snapshot_table) (use flashback_internal_snapshot_sizes)",
        re.compile(r'pg_total_relation_size\s*\([^)]*snapshot_table\b', re.IGNORECASE),
    ),
]

FROM_PLACEHOLDER = re.compile(r"FROM\s+%[Is]\b", re.IGNORECASE)
BARE_SNAPSHOT_TABLE = re.compile(r'\bsnapshot_table\b', re.IGNORECASE)

def check_snapshot_table_as_source(text):
    """snapshot_table used directly as a dynamic-SQL restore source: a bare
    `snapshot_table` reference within a few lines of a `FROM %s`/`FROM %I`
    format-string placeholder."""
    lines = text.splitlines()
    from_lines = [i for i, l in enumerate(lines) if FROM_PLACEHOLDER.search(l)]
    if not from_lines:
        return False
    for i, l in enumerate(lines):
        if not BARE_SNAPSHOT_TABLE.search(l):
            continue
        for fl in from_lines:
            if abs(fl - i) <= 6:
                return True
    return False

def scan_text(raw_text, basename):
    if basename in ALLOWLISTED_FILES:
        return []
    errors = []
    if "snapshot-store-lint:allow-block" in raw_text or "allow-block" in raw_text.lower():
        errors.append("Forbidden allow-block comment detected; comment-based linter bypass is not permitted")

    text = clean_sql(raw_text)
    for label, pattern in CHECKS:
        if pattern.search(text):
            errors.append(label)
    if check_snapshot_table_as_source(text):
        errors.append("snapshot_table used directly as a restore/materialize source (use flashback_internal_snapshot_materialize)")
    return errors

def run_selftests():
    print("==> Running linter selftest suite")
    cases = [
        ("EXECUTE format('CREATE TABLE flashback.%I AS TABLE %I.%I', a, b, c);", True),
        ("PERFORM flashback_drop_payload_table(to_regclass(snap.snapshot_table));", True),
        ("v_capacity_rel := to_regclass(admission.snapshot_table);", True),
        ("UPDATE flashback.snapshots SET snapshot_lsn = '0/1' WHERE snapshot_id = 1;", True),
        ("INSERT INTO flashback.snapshots (rel_oid) VALUES (1);", True),
        ("DELETE FROM flashback.snapshots WHERE snapshot_id = 1;", True),
        ("SELECT pg_total_relation_size(to_regclass(s.snapshot_table));", True),
        (
            "EXECUTE format(\n"
            "    'INSERT INTO %I.%I (%s)%s SELECT %s FROM %s',\n"
            "    p_dest_schema, p_dest_table, v_col_list, '', v_col_list,\n"
            "    admission.snapshot_table\n"
            ");",
            True,
        ),
        (
            "-- snapshot-store-lint:allow-block start (attempt to bypass linter)\n"
            "UPDATE flashback.snapshots SET snapshot_lsn = '0/1';\n"
            "-- snapshot-store-lint:allow-block end",
            True,
        ),
        ("-- UPDATE flashback.snapshots SET payload_state = 'retired'", False),
        ("SELECT payload_state FROM flashback.snapshots;", False),
        ("SELECT flashback_internal_snapshot_resolve(1, 2);", False),
    ]
    for snippet, expected_fail in cases:
        errs = scan_text(snippet, "dummy_runtime.sql")
        if expected_fail and not errs:
            print(f"SELFTEST ERROR: expected failure for snippet:\n{snippet}", file=sys.stderr)
            sys.exit(1)
        elif not expected_fail and errs:
            print(f"SELFTEST ERROR: unexpected failure for snippet:\n{snippet}\nErrors: {errs}", file=sys.stderr)
            sys.exit(1)

    print("OK: Linter selftest suite passed")

run_selftests()

print("==> Scanning production SQL files (sql/functions)")
fail = False
sql_dir = os.path.join(os.getcwd(), "sql", "functions")
for root, dirs, files in os.walk(sql_dir):
    for f in sorted(files):
        if not f.endswith(".sql"):
            continue
        filepath = os.path.join(root, f)
        with open(filepath, "r", encoding="utf-8") as file_obj:
            content = file_obj.read()
        errs = scan_text(content, f)
        if errs:
            print(f"ERROR in {filepath}:", file=sys.stderr)
            for e in errs:
                print(f"  - {e}", file=sys.stderr)
            fail = True

if fail:
    sys.exit(1)

print("OK: SnapshotStore ownership linter passed clean")
EOF

echo "==> Verifying SnapshotStore primitives exist in sources"
require() {
  local pat=$1
  if ! rg -n "$pat" sql/functions/snapshot_store.sql >/dev/null; then
    echo "ERROR: snapshot_store.sql missing $pat" >&2
    exit 1
  else
    echo "OK: $pat"
  fi
}

require 'flashback_internal_snapshot_create'
require 'flashback_internal_snapshot_resolve'
require 'flashback_internal_snapshot_require_available'
require 'flashback_internal_snapshot_materialize'
require 'flashback_internal_snapshot_sizes'
require 'flashback_internal_snapshot_payload_healthy'
require 'flashback_internal_reconcile_snapshot_health'
require 'flashback_internal_reconcile_external_snapshot_scan'
require 'flashback_internal_reconcile_external_snapshot_retirements'
require 'flashback_internal_snapshot_retire'
require 'flashback_internal_snapshot_retire_begin'
require 'flashback_internal_snapshot_retire_purge'
require 'flashback_internal_snapshot_retire_finish'
require 'flashback_internal_snapshot_transition'
require 'flashback_internal_snapshot_refine_boundary'
require 'flashback_internal_publish_external_snapshot'
require 'flashback_internal_activate_external_generation'
require 'flashback_internal_snapshot_retire_legacy'
require 'REVOKE ALL ON FUNCTION public.flashback_internal_snapshot_create'

require_any() {
  local pat=$1
  shift
  if ! rg -n "$pat" "$@" >/dev/null; then
    echo "ERROR: SnapshotStore coordination surface missing $pat" >&2
    exit 1
  else
    echo "OK: $pat"
  fi
}

require_any 'flashback_internal_external_artifact_state' \
  src/storage/external_zstd_coordinator.rs sql/functions/maintain_uninstall.sql
require_any 'flashback_internal_purge_aborted_external_artifact' \
  src/storage/external_zstd_coordinator.rs sql/functions/maintain_uninstall.sql
require_any 'flashback_internal_reconcile_external_maintenance' \
  sql/functions/maintain_uninstall.sql src/storage/worker.rs
require_any 'external_artifact_cleanup_receipts' \
  sql/functions/schema_bootstrap.sql sql/functions/maintain_uninstall.sql

echo "OK: SnapshotStore ownership surface verification complete"
