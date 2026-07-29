#!/usr/bin/env bash
# ACL-order canonicalization for schema-only pg_dump comparisons.
#
# flashback_collect_schema_def captures ACL entries ordered by resolved
# grantee/privilege identity (not the live relacl array's grant-issuance
# order) so the schema contract stays comparable across clusters where role
# OIDs differ -- see sql/functions/api_track_capture.sql. Replaying GRANTs in
# that canonical order after a real DROP+restore therefore does not, in
# general, reproduce the original relacl array's element order, even though
# the resulting privilege set is identical. A raw byte diff of pg_dump's ACL
# section would flag that as drift when nothing is actually wrong.
#
# canonicalize_acl_order rewrites ONLY contiguous GRANT/REVOKE lines
# (pg_dump's ACL blocks) in place, sorted, so GRANT statement order stops
# mattering while every other line -- indexes, constraints, defaults,
# triggers, RLS, comments, sequence ownership/options, collation, and all
# other DDL -- stays byte-for-byte where it already was. It never merges,
# drops, deduplicates, or invents a line: a missing privilege, an extra
# privilege, or a WITH GRANT OPTION difference is still a different line in
# the sorted block and still fails a comparison of the canonicalized output.

canonicalize_acl_order() {
    local input=$1 output=$2
    awk '
        function flush_acl(    i, j, tmp) {
            for (i = 2; i <= n; i++) {
                tmp = buf[i]
                j = i - 1
                while (j >= 1 && buf[j] > tmp) {
                    buf[j + 1] = buf[j]
                    j--
                }
                buf[j + 1] = tmp
            }
            for (i = 1; i <= n; i++) print buf[i]
            n = 0
        }
        /^GRANT / || /^REVOKE / {
            n++
            buf[n] = $0
            next
        }
        { if (n > 0) flush_acl(); print }
        END { if (n > 0) flush_acl() }
    ' "$input" >"$output"
}
