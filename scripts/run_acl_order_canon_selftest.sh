#!/usr/bin/env bash
# Self-test for scripts/lib/acl_order_canon.sh -- NOT release qualification,
# needs no postgres/candidate. Runs canonicalize_acl_order() directly against
# small synthetic schema-dump fixtures.
#
# Positive control: GRANT statement order alone must stop mattering.
# Negative controls (Blocker 2): the oracle must still catch a missing
# privilege, an extra privilege, and a WITH GRANT OPTION difference.
# Non-ACL control: lines outside a GRANT/REVOKE run must never be reordered,
# merged, or dropped by canonicalization.
#
# Usage: ./scripts/run_acl_order_canon_selftest.sh

set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/lib/acl_order_canon.sh
source "$ROOT/scripts/lib/acl_order_canon.sh"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

FAILED=0
log() { printf '[acl-order-canon-selftest] %s\n' "$*"; }

# expect_equal_after_canon NAME FILE_A FILE_B
# The oracle's whole job: after canonicalization, these two must compare
# equal despite differing only in GRANT statement order.
expect_equal_after_canon() {
    local name=$1 a=$2 b=$3 ca cb
    ca="$WORK/$name.a.canon"
    cb="$WORK/$name.b.canon"
    canonicalize_acl_order "$a" "$ca"
    canonicalize_acl_order "$b" "$cb"
    if cmp -s "$ca" "$cb"; then
        log "PASS: $name (equal after canonicalization, as expected)"
    else
        log "FAIL: $name (expected equal after canonicalization; diff follows)"
        diff -u "$ca" "$cb" || true
        FAILED=$((FAILED + 1))
    fi
}

# expect_different_after_canon NAME FILE_A FILE_B
# Negative control: canonicalization must NOT hide a real semantic
# difference (missing/extra privilege, WITH GRANT OPTION mismatch).
expect_different_after_canon() {
    local name=$1 a=$2 b=$3 ca cb
    ca="$WORK/$name.a.canon"
    cb="$WORK/$name.b.canon"
    canonicalize_acl_order "$a" "$ca"
    canonicalize_acl_order "$b" "$cb"
    if cmp -s "$ca" "$cb"; then
        log "FAIL: $name (expected a real difference to survive canonicalization, but none was found)"
        FAILED=$((FAILED + 1))
    else
        log "PASS: $name (real difference correctly still detected after canonicalization)"
    fi
}

HEADER='--
-- Name: TABLE orders; Type: ACL; Schema: public; Owner: smoke_owner
--
'
FOOTER='
--
-- PostgreSQL database dump complete
--'

# 1) Positive control: order-only difference must canonicalize to equal.
printf '%s%s\n%s\n%s' "$HEADER" \
    'GRANT SELECT ON TABLE public.orders TO smoke_reader;' \
    'GRANT SELECT,INSERT,UPDATE ON TABLE public.orders TO smoke_app;' \
    "$FOOTER" >"$WORK/order-only.a.sql"
printf '%s%s\n%s\n%s' "$HEADER" \
    'GRANT SELECT,INSERT,UPDATE ON TABLE public.orders TO smoke_app;' \
    'GRANT SELECT ON TABLE public.orders TO smoke_reader;' \
    "$FOOTER" >"$WORK/order-only.b.sql"
expect_equal_after_canon order-only "$WORK/order-only.a.sql" "$WORK/order-only.b.sql"

# 2) Negative control: a privilege missing on one side must still differ.
printf '%s%s\n%s\n%s' "$HEADER" \
    'GRANT SELECT ON TABLE public.orders TO smoke_reader;' \
    'GRANT SELECT,INSERT,UPDATE ON TABLE public.orders TO smoke_app;' \
    "$FOOTER" >"$WORK/missing.a.sql"
printf '%s%s\n%s' "$HEADER" \
    'GRANT SELECT,INSERT,UPDATE ON TABLE public.orders TO smoke_app;' \
    "$FOOTER" >"$WORK/missing.b.sql"
expect_different_after_canon missing-privilege "$WORK/missing.a.sql" "$WORK/missing.b.sql"

# 3) Negative control: an extra privilege on one side must still differ.
printf '%s%s\n%s\n%s' "$HEADER" \
    'GRANT SELECT ON TABLE public.orders TO smoke_reader;' \
    'GRANT SELECT,INSERT,UPDATE ON TABLE public.orders TO smoke_app;' \
    "$FOOTER" >"$WORK/extra.a.sql"
printf '%s%s\n%s\n%s\n%s' "$HEADER" \
    'GRANT SELECT ON TABLE public.orders TO smoke_reader;' \
    'GRANT SELECT,INSERT,UPDATE ON TABLE public.orders TO smoke_app;' \
    'GRANT SELECT ON TABLE public.orders TO smoke_extra;' \
    "$FOOTER" >"$WORK/extra.b.sql"
expect_different_after_canon extra-privilege "$WORK/extra.a.sql" "$WORK/extra.b.sql"

# 4) Negative control: WITH GRANT OPTION differing must still differ.
printf '%s%s\n%s' "$HEADER" \
    'GRANT SELECT,INSERT,UPDATE ON TABLE public.orders TO smoke_app WITH GRANT OPTION;' \
    "$FOOTER" >"$WORK/gopt.a.sql"
printf '%s%s\n%s' "$HEADER" \
    'GRANT SELECT,INSERT,UPDATE ON TABLE public.orders TO smoke_app;' \
    "$FOOTER" >"$WORK/gopt.b.sql"
expect_different_after_canon with-grant-option "$WORK/gopt.a.sql" "$WORK/gopt.b.sql"

# 5) Non-ACL control: reordering elsewhere in the file (indexes, comments,
# etc.) must still be detected -- canonicalization only ever touches
# contiguous GRANT/REVOKE runs, never anything else.
cat >"$WORK/nonacl.a.sql" <<'EOF'
--
-- Name: orders_note_idx; Type: INDEX; Schema: public; Owner: smoke_owner
--

CREATE INDEX orders_note_idx ON public.orders USING btree (note);

--
-- Name: TABLE orders; Type: COMMENT; Schema: public; Owner: smoke_owner
--

COMMENT ON TABLE public.orders IS 'clean-host rich metadata';
EOF
cat >"$WORK/nonacl.b.sql" <<'EOF'
--
-- Name: TABLE orders; Type: COMMENT; Schema: public; Owner: smoke_owner
--

COMMENT ON TABLE public.orders IS 'clean-host rich metadata';

--
-- Name: orders_note_idx; Type: INDEX; Schema: public; Owner: smoke_owner
--

CREATE INDEX orders_note_idx ON public.orders USING btree (note);
EOF
expect_different_after_canon non-acl-reorder-still-strict "$WORK/nonacl.a.sql" "$WORK/nonacl.b.sql"

# 6) Non-ACL control: canonicalization must be a byte-identical no-op when
# there is no GRANT/REVOKE line at all in the input.
canonicalize_acl_order "$WORK/nonacl.a.sql" "$WORK/nonacl.a.canon"
if cmp -s "$WORK/nonacl.a.sql" "$WORK/nonacl.a.canon"; then
    log "PASS: no-acl-input-unchanged (canonicalization is a no-op with no GRANT/REVOKE lines)"
else
    log "FAIL: no-acl-input-unchanged (canonicalization altered a file with no ACL lines)"
    diff -u "$WORK/nonacl.a.sql" "$WORK/nonacl.a.canon" || true
    FAILED=$((FAILED + 1))
fi

if [[ "$FAILED" == "0" ]]; then
    log "all checks passed"
    exit 0
else
    log "failed_checks=$FAILED"
    exit 1
fi
