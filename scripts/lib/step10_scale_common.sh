#!/usr/bin/env bash
# Shared primitives for the Step 10 production external_zstd scale
# qualification harness (scripts/run_step10_scale_qualification.sh).
#
# Extracted so the fail-closed evidence/fingerprint/capacity logic can be
# regression-tested in isolation by
# scripts/run_step10_harness_selftest.sh without allocating a scale tier.
#
# Requires bash 4+. Callers must set S10_PSQL (an array holding a complete
# psql invocation, e.g. (psql -h SOCK -p PORT -d DB)) before calling any
# function that talks to PostgreSQL.
#
# Public entrypoints:
#   s10_free_bytes <path>
#   s10_dir_bytes <path>
#   s10_q <sql>                       one-shot scalar query
#   s10_qf <file>                     run a SQL file, return stdout
#   s10_estimate_tier_peak_bytes <table_bytes> <index_bytes>
#   s10_capacity_precheck <label> <table_bytes> <index_bytes> <path> <reserve_bytes>
#   s10_data_fingerprint <qualified_table>
#   s10_schema_fingerprint <pg_dump_bin> <conn_args...> -- <db> <table> <outfile>
#   s10_relation_metrics <qualified_table>
#   s10_percentile <file_of_numbers> <pct>
#   s10_now_ms

# ---------------------------------------------------------------------
# Filesystem
# ---------------------------------------------------------------------
s10_free_bytes() {
    df -B1 --output=avail "${1:-.}" | awk 'NR==2 {print $1}'
}

s10_dir_bytes() {
    local p=${1:-.}
    [[ -d "$p" ]] || { echo 0; return 0; }
    du -sB1 "$p" 2>/dev/null | awk '{print $1}'
}

s10_now_ms() { date +%s%3N; }

# ---------------------------------------------------------------------
# psql helpers. S10_PSQL must be an array with a full psql invocation.
# ---------------------------------------------------------------------
s10_q() {
    "${S10_PSQL[@]}" -X -v ON_ERROR_STOP=1 -qAtc "$1"
}

s10_qf() {
    "${S10_PSQL[@]}" -X -v ON_ERROR_STOP=1 -qAt -f "$1"
}

# ---------------------------------------------------------------------
# Capacity planning
#
# Conservative peak-space estimate for one tier, in bytes:
#   source heap+TOAST                            (table_bytes)
# + source indexes                               (index_bytes)
# + external compressed artifact          <= 1.0 x table_bytes (no
#                                          compression is ever assumed)
# + restore successor/workspace           ~ table_bytes + index_bytes
#                                          (shadow relation + rebuilt idx)
# + WAL headroom                          25% of table_bytes, min 4 GiB
# Callers add their own emergency reserve on top.
# ---------------------------------------------------------------------
s10_estimate_tier_peak_bytes() {
    local table_bytes=$1 index_bytes=$2
    local wal=$(( table_bytes / 4 ))
    local wal_min=$(( 4 * 1024 * 1024 * 1024 ))
    (( wal < wal_min )) && wal=$wal_min
    echo $(( table_bytes + index_bytes + table_bytes + table_bytes + index_bytes + wal ))
}

# Returns 0 when the tier fits while preserving reserve_bytes, else 1.
# Always prints a JSON object describing the decision.
s10_capacity_precheck() {
    local label=$1 table_bytes=$2 index_bytes=$3 path=$4 reserve_bytes=$5
    local free est need verdict
    free="$(s10_free_bytes "$path")"
    est="$(s10_estimate_tier_peak_bytes "$table_bytes" "$index_bytes")"
    need=$(( est + reserve_bytes ))
    if (( free >= need )); then verdict=ok; else verdict=BLOCKED_CAPACITY; fi
    # NOTE: the jq argument is named tier_label, not label: "label" is a
    # reserved keyword in jq's grammar (label $out | ...) and $label fails to
    # compile.
    jq -n --arg tier_label "$label" --arg verdict "$verdict" \
        --argjson free "$free" --argjson estimated_peak "$est" \
        --argjson reserve "$reserve_bytes" --argjson required "$need" \
        --argjson table_bytes "$table_bytes" --argjson index_bytes "$index_bytes" \
        '{tier:$tier_label, verdict:$verdict, path_free_bytes:$free,
          estimated_peak_bytes:$estimated_peak, emergency_reserve_bytes:$reserve,
          required_free_bytes:$required, source_table_bytes:$table_bytes,
          source_index_bytes:$index_bytes}'
    [[ "$verdict" == "ok" ]]
}

# ---------------------------------------------------------------------
# Deterministic, bounded-memory logical data fingerprint.
#
# Algorithm "bucketed-md5-v1":
#   h  = md5(row::text)                     per row, whole-row content
#   bk = first byte of h                    -> 256 buckets, uniform
#   bd = md5(string_agg(h ORDER BY h))      per bucket, order-independent
#                                           input, deterministic output
#   digest = md5(concat(bd ORDER BY bk))    over the 256 bucket digests
# Each bucket aggregates only ~n/256 hashes and spills to disk under
# work_mem like any other sort, so this never has to hold the whole
# relation in memory the way a single global string_agg would.
#
# Two independent commutative checksums are recorded alongside it
# (bit_xor over one 64-bit slice, sum over a different 32-bit slice), so
# a pathological pair of row changes that cancels in one aggregate still
# has to survive the other two plus the exact per-bucket ordering.
# ---------------------------------------------------------------------
s10_data_fingerprint() {
    local rel=$1
    s10_q "
WITH r AS (
    SELECT md5(t.*::text) AS h FROM ${rel} t
), b AS (
    SELECT h, ('x' || substr(h, 1, 2))::bit(8)::int AS bk FROM r
), g AS (
    SELECT bk, count(*) AS n, md5(string_agg(h, '' ORDER BY h)) AS bd
    FROM b GROUP BY bk
)
SELECT jsonb_build_object(
    'algorithm', 'bucketed-md5-v1',
    'algorithm_detail', 'h=md5(row::text); bucket=first byte of h (256 buckets); bucket_digest=md5(string_agg(h ORDER BY h)); digest=md5(concat(bucket_digest ORDER BY bucket)); plus bit_xor(bigint slice) and sum(int slice)',
    'row_count', (SELECT count(*) FROM r),
    'bucket_count', (SELECT count(*) FROM g),
    'digest', (SELECT md5(string_agg(bd, '' ORDER BY bk)) FROM g),
    'xor64', (SELECT bit_xor(('x' || substr(h, 1, 16))::bit(64)::bigint) FROM r),
    'sum32', (SELECT sum(('x' || substr(h, 17, 8))::bit(32)::bigint) FROM r)
);"
}

# Relation size metrics: heap+TOAST, indexes, total, and live row count.
s10_relation_metrics() {
    local rel=$1
    s10_q "
SELECT jsonb_build_object(
    'pg_table_size', pg_table_size('${rel}'),
    'pg_indexes_size', pg_indexes_size('${rel}'),
    'pg_total_relation_size', pg_total_relation_size('${rel}'),
    'heap_bytes', pg_relation_size('${rel}', 'main'),
    'toast_bytes', COALESCE((SELECT pg_total_relation_size(reltoastrelid)
                             FROM pg_class WHERE oid = '${rel}'::regclass
                               AND reltoastrelid <> 0), 0),
    'exact_row_count', (SELECT count(*) FROM ${rel})
);"
}

# Owner, table-level ACL, comments, constraints, indexes, RLS, identity.
# Everything the Step 10 correctness matrix asserts across DROP/recover,
# in one canonical, order-stable JSON document.
s10_metadata_fingerprint() {
    local rel=$1
    s10_q "
SELECT jsonb_build_object(
    'owner', (SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid='${rel}'::regclass),
    'relacl', (SELECT COALESCE(array_to_json(ARRAY(
                   SELECT a FROM unnest(COALESCE(relacl, '{}'::aclitem[])) a ORDER BY a::text
               ))::jsonb, '[]'::jsonb)
               FROM pg_class WHERE oid='${rel}'::regclass),
    'rls_enabled', (SELECT relrowsecurity FROM pg_class WHERE oid='${rel}'::regclass),
    'rls_forced', (SELECT relforcerowsecurity FROM pg_class WHERE oid='${rel}'::regclass),
    'policies', COALESCE((SELECT jsonb_agg(jsonb_build_object(
                       'name', polname, 'cmd', polcmd,
                       'qual', pg_get_expr(polqual, polrelid),
                       'withcheck', pg_get_expr(polwithcheck, polrelid))
                     ORDER BY polname)
                   FROM pg_policy WHERE polrelid='${rel}'::regclass), '[]'::jsonb),
    'table_comment', obj_description('${rel}'::regclass, 'pg_class'),
    'column_comments', COALESCE((SELECT jsonb_object_agg(a.attname,
                           col_description(a.attrelid, a.attnum))
                         FROM pg_attribute a
                         WHERE a.attrelid='${rel}'::regclass AND a.attnum>0
                           AND NOT a.attisdropped
                           AND col_description(a.attrelid, a.attnum) IS NOT NULL), '{}'::jsonb),
    'constraints', COALESCE((SELECT jsonb_agg(jsonb_build_object(
                       'name', conname, 'type', contype,
                       'def', pg_get_constraintdef(oid)) ORDER BY conname)
                     FROM pg_constraint WHERE conrelid='${rel}'::regclass), '[]'::jsonb),
    'indexes', COALESCE((SELECT jsonb_agg(jsonb_build_object(
                       'name', indexname, 'def', indexdef) ORDER BY indexname)
                     FROM pg_indexes
                     WHERE schemaname||'.'||tablename = '${rel}'), '[]'::jsonb),
    'columns', COALESCE((SELECT jsonb_agg(jsonb_build_object(
                       'name', a.attname, 'type', format_type(a.atttypid, a.atttypmod),
                       'notnull', a.attnotnull, 'identity', a.attidentity,
                       'default', pg_get_expr(d.adbin, d.adrelid))
                     ORDER BY a.attnum)
                     FROM pg_attribute a
                     LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
                     WHERE a.attrelid='${rel}'::regclass AND a.attnum>0
                       AND NOT a.attisdropped), '[]'::jsonb)
);"
}

# ---------------------------------------------------------------------
# Percentile over a file of one number per line (integers, ms).
# Uses nearest-rank on the sorted sample. Prints 0 for an empty sample.
# ---------------------------------------------------------------------
s10_percentile() {
    local file=$1 pct=$2
    awk -v p="$pct" '
        /^[0-9]+$/ { v[n++] = $1 }
        END {
            if (n == 0) { print 0; exit }
            for (i = 0; i < n; i++)
                for (j = i + 1; j < n; j++)
                    if (v[j] < v[i]) { t = v[i]; v[i] = v[j]; v[j] = t }
            idx = int((p / 100.0) * n + 0.9999) - 1
            if (idx < 0) idx = 0
            if (idx >= n) idx = n - 1
            print v[idx]
        }' "$file"
}
