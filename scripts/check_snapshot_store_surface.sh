#!/usr/bin/env bash
# Fail if production SQL outside SnapshotStore (sql/functions/snapshot_store.sql)
# reintroduces a direct coupling to the snapshot payload's physical storage:
# raw CTAS of a snapshot relation, a direct DROP of one, using snapshot_table
# as a restore source, to_regclass(snapshot_table) backend resolution, a
# direct payload_state mutation, or pg_total_relation_size(snapshot_table).
#
# Allowlist is narrow and file-based (SnapshotStore's own implementation and
# schema_bootstrap.sql's migration/backfill, matching
# check_centralized_state_surface.sh's convention), plus an inline
# `snapshot-store-lint:allow-block start/end` marker pair for production code
# that is provably unreachable (an early guard in the same function always
# raises first) rather than genuinely still coupled to the payload directly.
# Every such marker in this tree must cite the regression test that proves
# the guard fires.
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

def strip_allowed_blocks(raw_text):
    """Remove text between `-- snapshot-store-lint:allow-block start` and
    `-- snapshot-store-lint:allow-block end` markers (inclusive), so it is
    never scanned. Comments are stripped by the caller from the *returned*
    text, but markers must be matched on raw_text since clean_sql() would
    already have blanked them."""
    out = []
    depth = 0
    for line in raw_text.splitlines():
        if 'snapshot-store-lint:allow-block start' in line:
            depth += 1
            continue
        if 'snapshot-store-lint:allow-block end' in line:
            if depth == 0:
                raise ValueError("allow-block end without matching start")
            depth -= 1
            continue
        if depth == 0:
            out.append(line)
    if depth != 0:
        raise ValueError("allow-block start without matching end")
    return "\n".join(out)

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
        "Direct payload_state mutation on flashback.snapshots (use flashback_internal_snapshot_transition)",
        re.compile(r'UPDATE\s+flashback\.snapshots\b[^;]*?\bSET\b[^;]*?\bpayload_state\s*=', re.IGNORECASE | re.DOTALL),
    ),
    (
        "Scattered INSERT into flashback.snapshots outside SnapshotStore",
        re.compile(r'INSERT\s+INTO\s+flashback\.snapshots\b', re.IGNORECASE),
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
    format-string placeholder (the shape of the pattern removed from
    restore_lsn.sql -- `format('... SELECT %s FROM %s', ..., admission.snapshot_table)`).
    Proximity-based (not exact-paren dynamic-SQL parsing) by design."""
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
    try:
        stripped = strip_allowed_blocks(raw_text)
    except ValueError as e:
        return [f"malformed allow-block marker: {e}"]
    text = clean_sql(stripped)
    errors = []
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
        ("UPDATE flashback.snapshots SET payload_state = 'retired' WHERE snapshot_id = 1;", True),
        ("INSERT INTO flashback.snapshots (rel_oid) VALUES (1);", True),
        ("SELECT pg_total_relation_size(to_regclass(s.snapshot_table));", True),
        (
            "EXECUTE format(\n"
            "    'INSERT INTO %I.%I (%s)%s SELECT %s FROM %s',\n"
            "    p_dest_schema, p_dest_table, v_col_list, '', v_col_list,\n"
            "    admission.snapshot_table\n"
            ");",
            True,
        ),
        ("-- UPDATE flashback.snapshots SET payload_state = 'retired'", False),
        ("SELECT payload_state FROM flashback.snapshots;", False),
        ("SELECT flashback_internal_snapshot_resolve(1, 2);", False),
        (
            "-- snapshot-store-lint:allow-block start (dead code, proven by test X)\n"
            "PERFORM flashback_drop_payload_table(to_regclass(snap.snapshot_table));\n"
            "-- snapshot-store-lint:allow-block end\n",
            False,
        ),
    ]
    for snippet, expected_fail in cases:
        errs = scan_text(snippet, "dummy_runtime.sql")
        if expected_fail and not errs:
            print(f"SELFTEST ERROR: expected failure for snippet:\n{snippet}", file=sys.stderr)
            sys.exit(1)
        elif not expected_fail and errs:
            print(f"SELFTEST ERROR: unexpected failure for snippet:\n{snippet}\nErrors: {errs}", file=sys.stderr)
            sys.exit(1)

    unmatched_end = "-- snapshot-store-lint:allow-block end\nSELECT 1;"
    if not scan_text(unmatched_end, "dummy_runtime.sql"):
        print("SELFTEST ERROR: unmatched allow-block end did not fail", file=sys.stderr)
        sys.exit(1)
    unmatched_start = "-- snapshot-store-lint:allow-block start\nSELECT 1;"
    if not scan_text(unmatched_start, "dummy_runtime.sql"):
        print("SELFTEST ERROR: unmatched allow-block start did not fail", file=sys.stderr)
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
require 'flashback_internal_snapshot_retire'
require 'flashback_internal_snapshot_transition'
require 'REVOKE ALL ON FUNCTION public.flashback_internal_snapshot_create'

echo "OK: SnapshotStore ownership surface verification complete"
