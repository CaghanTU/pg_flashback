#!/usr/bin/env bash
# Fail if runtime SQL (outside state_authority.sql / schema_bootstrap.sql)
# performs direct state UPDATEs, scattered initial INSERTs, operations header mutation,
# or calls generic flashback_set_state.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

python3 - <<'EOF'
import sys, os, re

def clean_sql(text):
    text = re.sub(r'--.*$', '', text, flags=re.MULTILINE)
    text = re.sub(r'/\*.*?\*/', '', text, flags=re.DOTALL)
    return text

def scan_text(raw_text, filename):
    text = clean_sql(raw_text)
    basename = os.path.basename(filename)
    errors = []

    if basename in ('state_authority.sql', 'schema_bootstrap.sql'):
        return errors

    for match in re.finditer(
        r'\bUPDATE\s+flashback\.(capture_streams|coverage_generations|generation_payload_retirements)\b(?P<body>[^;]*)',
        text,
        re.IGNORECASE | re.DOTALL,
    ):
        set_clause = re.split(r'\bWHERE\b', match.group('body'), maxsplit=1, flags=re.IGNORECASE)[0]
        if re.search(r'\bSET\b[\s\S]*?(?<![A-Za-z0-9_])state\s*=', set_clause, re.IGNORECASE):
            errors.append("Direct UPDATE state= outside state_authority")
            break

    if re.search(r'\bINSERT\s+INTO\s+flashback\.(capture_streams|coverage_generations|generation_payload_retirements)\b', text, re.IGNORECASE):
        errors.append("Scattered INSERT into state tables outside state_authority")

    if re.search(r'\b(UPDATE|DELETE\s+FROM)\s+flashback\.operations\b', text, re.IGNORECASE):
        errors.append("Banned UPDATE/DELETE on flashback.operations header")

    if re.search(r'\b(UPDATE|DELETE\s+FROM)\s+flashback\.operation_events\b', text, re.IGNORECASE):
        errors.append("Banned UPDATE/DELETE on flashback.operation_events")

    if re.search(r'\bflashback_set_state\s*\(', text, re.IGNORECASE):
        errors.append("Generic flashback_set_state call is forbidden")

    return errors

def run_selftests():
    print("==> Running linter selftest suite")
    test_cases = [
        ("UPDATE flashback.capture_streams\nSET\n  state = 'broken'", True),
        ("UPDATE flashback.coverage_generations SET boundary_xid=7 WHERE state='building'", False),
        ("INSERT INTO flashback.coverage_generations (tracking_id) VALUES (1)", True),
        ("UPDATE flashback.operations SET details = '{}'", True),
        ("DELETE FROM flashback.operation_events WHERE operation_id = 1", True),
        ("PERFORM flashback_set_state('active')", True),
        ("-- UPDATE flashback.coverage_generations SET state = 'active'", False), # comment ignored
        ("SELECT state FROM flashback.coverage_generations", False),
    ]

    for snippet, expected_fail in test_cases:
        errs = scan_text(snippet, "dummy_runtime.sql")
        if expected_fail and not errs:
            print(f"SELFTEST ERROR: expected failure for snippet:\n{snippet}", file=sys.stderr)
            sys.exit(1)
        elif not expected_fail and errs:
            print(f"SELFTEST ERROR: unexpected failure for snippet:\n{snippet}\nErrors: {errs}", file=sys.stderr)
            sys.exit(1)
    print("OK: Linter selftest suite passed")

run_selftests()

print("==> Scanning runtime SQL files")
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

print("OK: Centralized state authority linter passed clean")
EOF

echo "==> Verifying state_authority primitives exist in sources"
require() {
  local pat=$1
  if ! rg -n "$pat" sql/functions/state_authority.sql >/dev/null; then
    echo "ERROR: state_authority.sql missing $pat" >&2
    exit 1
  else
    echo "OK: $pat"
  fi
}

require 'flashback_internal_transition_capture_stream'
require 'flashback_internal_transition_coverage_generation'
require 'flashback_internal_transition_retirement'
require 'flashback_internal_advance_capture_stream_progress'
require 'flashback_internal_advance_generation_watermark'
require 'flashback_internal_lock_database_stream'
require 'flashback_internal_lock_lifecycles'
require 'flashback_internal_create_capture_stream'
require 'flashback_internal_create_coverage_generation'
require 'flashback_internal_create_online_generation_reservation'
require 'flashback_internal_create_retirement_intent'
require 'REVOKE ALL ON FUNCTION public.flashback_internal_transition_capture_stream'

echo "OK: centralized state surface verification complete"
