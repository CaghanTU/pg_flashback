#!/usr/bin/env bash
# Step 10 -- production external_zstd scale qualification (1/10/25/50 GiB).
#
# Proves the exact production candidate can protect, track, DROP, recover,
# verify and clean up supported ordinary PostgreSQL tables at scale, using
# the installed CLI and the real online external_zstd path throughout.
#
# Evidence contract: the sticky summary is PASS only when every planned
# step for the selected tier set is present and marked pass, the process
# was not interrupted by a trapped signal, and the process's own exit code
# is 0 (scripts/lib/qualification_step_tracker.sh). A run cut short by
# SIGHUP/SIGINT/SIGTERM always leaves a FAIL summary naming the interrupted
# and still-missing steps. This is regression-tested by
# scripts/run_step10_harness_selftest.sh.
#
# NEVER started by this harness: the 24-hour soak.
#
# Usage:
#   CANDIDATE_DIR=target/candidate/<commit> ./scripts/run_step10_scale_qualification.sh
#
# Env:
#   CANDIDATE_DIR   (mandatory) exact candidate archive directory
#   PG_BIN          PostgreSQL bindir (default: derived from candidate pg_major)
#   S10_TIERS       space-separated tier ids (default: all)
#   S10_WORK_ROOT   large-data root (default: /home/$USER/pgfb-step10)
#   S10_RESERVE_GIB emergency free-space reserve (default: 40)
#   S10_MAX_COPIER_RSS_BYTES fail the run before one copier can exhaust the host
#                            (default: 2 GiB; qualification guard, not a product GUC)
#   S10_KEEP=1      keep cluster + data after the run (debugging only)
#   S10_RUN_ID      override run id (resume only)
#   S10_RESUME=1    resume an existing run id whose manifest matches exactly
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/lib/exact_candidate_identity.sh
source "$ROOT/scripts/lib/exact_candidate_identity.sh"
# shellcheck source=scripts/lib/qualification_step_tracker.sh
source "$ROOT/scripts/lib/qualification_step_tracker.sh"
# shellcheck source=scripts/lib/step10_scale_common.sh
source "$ROOT/scripts/lib/step10_scale_common.sh"
# shellcheck source=scripts/lib/schema_dump_normalize.sh
source "$ROOT/scripts/lib/schema_dump_normalize.sh"
# shellcheck source=scripts/lib/output_plugin_allowlist.sh
source "$ROOT/scripts/lib/output_plugin_allowlist.sh"

[[ -n "${CANDIDATE_DIR:-}" ]] || { echo "FAIL: CANDIDATE_DIR is mandatory" >&2; exit 2; }

GIB=$((1024 * 1024 * 1024))
RESERVE_BYTES=$(( ${S10_RESERVE_GIB:-40} * GIB ))
RUN_ID="${S10_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
WORK_ROOT="${S10_WORK_ROOT:-/home/$(id -un)/pgfb-step10}/$RUN_ID"
DATA="$WORK_ROOT/data"
ARTIFACT_ROOT="$WORK_ROOT/artifacts"
EVIDENCE_DIR="$WORK_ROOT/evidence"
LOG_DIR="$WORK_ROOT/log"
PGLOG="$LOG_DIR/postgresql.log"
WRITER_DIR="$WORK_ROOT/writer"
# /tmp holds only the unix socket dir and tiny coordination files.
SOCKET="/tmp/pgfb-s10-$$"
PORT="${S10_PORT:-29170}"
DB_NAME="s10db"
SUMMARY_JSON="$EVIDENCE_DIR/summary.json"
HEARTBEAT_JSONL="$EVIDENCE_DIR/heartbeat.jsonl"
RUN_MANIFEST="$EVIDENCE_DIR/run-manifest.json"
RESOURCE_GUARD_JSON="$EVIDENCE_DIR/resource-guard.json"
MAX_COPIER_RSS_BYTES="${S10_MAX_COPIER_RSS_BYTES:-2147483648}"

ALL_TIERS="t1_mixed t1_toast t1_rich t1_negative_capacity t10_mixed t10_toast t25_mixed t50_hybrid"
TIERS="${S10_TIERS:-$ALL_TIERS}"

HEARTBEAT_PID=""
WRITER_PID=""
STARTED_PG=0
RESUMING=0

log() { printf '[step10] %s %s\n' "$(date -u +%H:%M:%SZ)" "$*" >&2; }
die() { echo "FAIL: $*" >&2; exit 1; }

# ---------------------------------------------------------------------
# Cleanup: only ever touches resources this run created. Never pkill.
# ---------------------------------------------------------------------
cleanup() {
    local rc=$?
    set +e
    if [[ -n "$HEARTBEAT_PID" ]]; then kill "$HEARTBEAT_PID" 2>/dev/null; fi
    if [[ -n "$WRITER_PID" ]]; then kill "$WRITER_PID" 2>/dev/null; fi
    local extra summary
    extra="$(build_extra_json 2>/dev/null || echo '{}')"
    summary="$(qst_compute_summary_json "$RUN_ID" "step10_scale" "$rc" "$extra" 2>/dev/null)"
    mkdir -p "$EVIDENCE_DIR" 2>/dev/null
    qst_write_summary_atomic "$SUMMARY_JSON" "$summary" 2>/dev/null
    if (( STARTED_PG == 1 )) && [[ "${S10_KEEP:-0}" != "1" ]]; then
        "$PG_BIN/pg_ctl" -D "$DATA" -m immediate stop >/dev/null 2>&1
    fi
    if [[ "${S10_KEEP:-0}" != "1" ]]; then
        rm -rf "$DATA" "$ARTIFACT_ROOT" "$SOCKET" 2>/dev/null
    else
        log "S10_KEEP=1: leaving $WORK_ROOT and cluster in place"
    fi
    log "summary: $SUMMARY_JSON"
    if [[ -f "$SUMMARY_JSON" ]]; then
        jq -r '"STEP10 RUN STATUS: " + .status' "$SUMMARY_JSON" >&2 2>/dev/null
    fi
    exit "$rc"
}
trap cleanup EXIT
trap 'qst_on_signal INT' INT
trap 'qst_on_signal TERM' TERM
trap 'qst_on_signal HUP' HUP

# Merged into the sticky summary by qst_compute_summary_json.
build_extra_json() {
    local tiers_json='[]' f
    if [[ -d "$EVIDENCE_DIR/tiers" ]]; then
        for f in "$EVIDENCE_DIR"/tiers/*.json; do
            [[ -f "$f" ]] || continue
            tiers_json="$(jq -n --argjson acc "$tiers_json" --slurpfile t "$f" '$acc + $t')"
        done
    fi
    jq -n --argjson tiers "$tiers_json" \
        --arg commit "${EC_SOURCE_COMMIT:-}" --arg tree "${EC_SOURCE_TREE:-}" \
        --arg pkg "${EC_PACKAGE_SHA:-}" --arg extbin "${EC_EXT_BIN_SHA:-}" \
        --arg clibin "${EC_CLI_BIN_SHA:-}" --arg pgmajor "${EC_PG_MAJOR:-}" \
        --arg arch "${EC_ARCH:-}" \
        --arg host "$(uname -srm)" --arg fs "$(findmnt -n -T /home -o FSTYPE 2>/dev/null || echo unknown)" \
        --argjson reserve "$RESERVE_BYTES" \
        --arg soak24h "not_started" \
        '{qualification_kind:"step10_external_zstd_scale",
          candidate:{source_commit:$commit, source_tree:$tree, package_sha256:$pkg,
                     extension_binary_sha256:$extbin, cli_binary_sha256:$clibin,
                     pg_major:$pgmajor, arch:$arch},
          host:{uname:$host, home_fstype:$fs},
          emergency_reserve_bytes:$reserve,
          soak_24h:$soak24h,
          tiers:$tiers}'
}

# ---------------------------------------------------------------------
# Candidate + cluster bring-up
# ---------------------------------------------------------------------
exact_candidate_bind_dir "$CANDIDATE_DIR" || exit 1
exact_candidate_install_into_prefix || exit 1
# PG_CONFIG intentionally not exported: every psql/pg_dump call uses explicit -h/-p.
PSQL="$PG_BIN/psql"
PG_DUMP="$PG_BIN/pg_dump"
CLI="$WORK_ROOT/bin/pg_flashback"

mkdir -p "$WORK_ROOT" "$LOG_DIR" "$EVIDENCE_DIR/tiers" "$WRITER_DIR" "$SOCKET" \
         "$ARTIFACT_ROOT" "$WORK_ROOT/bin"
chmod 0700 "$ARTIFACT_ROOT"

# The packaged CLI from the candidate archive, never the source tree copy.
install -m 0755 "$EC_EXT_ROOT/bin/pg_flashback" "$CLI" \
    || die "packaged CLI missing under candidate extraction root"

# exact_candidate_identity.sh does not expose the CLI digest; read it from
# the manifest here and verify the packaged binary matches it, so the
# evidence names a CLI hash that was actually executed.
EC_CLI_BIN_SHA="$(jq -r '.artifacts.cli_binary_sha256' "$CANDIDATE_DIR/MANIFEST.json")"
got_cli="$(exact_candidate_sha256 "$CLI")"
[[ "$got_cli" == "$EC_CLI_BIN_SHA" ]] \
    || die "installed CLI sha $got_cli != manifest cli_binary_sha256 $EC_CLI_BIN_SHA"

log "run_id=$RUN_ID work_root=$WORK_ROOT"
log "candidate commit=$EC_SOURCE_COMMIT pkg=$EC_PACKAGE_SHA"

# Resume guard: only continue an existing run when the manifest proves it is
# byte-for-byte the same candidate and the same run identity. Anything else
# refuses rather than silently mixing two runs' evidence.
if [[ -f "$RUN_MANIFEST" ]]; then
    if [[ "${S10_RESUME:-0}" != "1" ]]; then
        die "run manifest already exists at $RUN_MANIFEST (set S10_RESUME=1 to continue this exact run)"
    fi
    prev_commit="$(jq -r '.candidate.source_commit' "$RUN_MANIFEST")"
    prev_pkg="$(jq -r '.candidate.package_sha256' "$RUN_MANIFEST")"
    prev_run="$(jq -r '.run_id' "$RUN_MANIFEST")"
    [[ "$prev_commit" == "$EC_SOURCE_COMMIT" ]] \
        || die "resume refused: manifest commit $prev_commit != candidate $EC_SOURCE_COMMIT"
    [[ "$prev_pkg" == "$EC_PACKAGE_SHA" ]] \
        || die "resume refused: manifest package sha $prev_pkg != candidate $EC_PACKAGE_SHA"
    [[ "$prev_run" == "$RUN_ID" ]] \
        || die "resume refused: manifest run_id $prev_run != $RUN_ID"
    RESUMING=1
    log "resuming run $RUN_ID (identity verified)"
    # A resumed qualification starts a fresh isolated PostgreSQL instance.
    # Compact evidence for completed tiers is retained, while a cluster or
    # artifact tree left by an interrupted tier is never trusted/reused.
    if [[ -f "$DATA/postmaster.pid" ]]; then
        stale_pid="$(head -n1 "$DATA/postmaster.pid" 2>/dev/null || true)"
        if [[ "$stale_pid" =~ ^[0-9]+$ ]] && kill -0 "$stale_pid" 2>/dev/null; then
            die "resume refused: prior qualification postmaster pid $stale_pid is still running"
        fi
    fi
    rm -rf "$DATA" "$ARTIFACT_ROOT" "$SOCKET" "$WRITER_DIR"
    mkdir -p "$LOG_DIR" "$EVIDENCE_DIR/tiers" "$WRITER_DIR" "$SOCKET" "$ARTIFACT_ROOT"
    chmod 0700 "$ARTIFACT_ROOT"
elif [[ "${S10_RESUME:-0}" == "1" ]]; then
    die "resume requested but no run manifest exists at $RUN_MANIFEST"
fi

"$PG_BIN/initdb" -D "$DATA" --locale=C.UTF-8 -A trust >"$LOG_DIR/initdb.log" 2>&1 \
    || die "initdb failed; see $LOG_DIR/initdb.log"
opal_configure_postgresql_conf "$PG_BIN" "$DATA"
cat >>"$DATA/postgresql.conf" <<EOF
port = $PORT
unix_socket_directories = '$SOCKET'
listen_addresses = ''
shared_preload_libraries = 'pg_flashback'
wal_level = logical
max_replication_slots = 16
max_wal_senders = 16
max_worker_processes = 16
shared_buffers = 2GB
work_mem = 256MB
maintenance_work_mem = 2GB
max_wal_size = 16GB
min_wal_size = 2GB
checkpoint_timeout = 30min
checkpoint_completion_target = 0.9
max_slot_wal_keep_size = 64GB
fsync = on
log_line_prefix = '%m [%p] %q%a '
log_checkpoints = on
pg_flashback.enabled = on
pg_flashback.capture_mode = wal
pg_flashback.worker_interval_ms = 50
pg_flashback.max_workers = 4
pg_flashback.target_databases = '$DB_NAME'
pg_flashback.snapshot_storage_backend = 'external_zstd'
pg_flashback.external_snapshot_root = '$ARTIFACT_ROOT'
pg_flashback.external_snapshot_min_free_bytes = '2GB'
pg_flashback.external_snapshot_safety_reserve_bytes = '1GB'
pg_flashback.local_max_snapshot_bytes = '80GB'
pg_flashback.local_max_restore_peak_bytes = '160GB'
pg_flashback.local_min_filesystem_bytes = '8GB'
pg_flashback.local_safety_reserve_bytes = '1GB'
pg_flashback.local_boundary_write_stall_ms = 120000
pg_flashback.restore_work_mem = '512MB'
pg_flashback.index_build_work_mem = '2GB'
EOF

"$PG_BIN/pg_ctl" -D "$DATA" -l "$PGLOG" -w -t 120 start >/dev/null || die "pg_ctl start failed"
STARTED_PG=1

# shellcheck disable=SC2034  # read by scripts/lib/step10_scale_common.sh
S10_PSQL=("$PSQL" -h "$SOCKET" -p "$PORT" -d "$DB_NAME")
QP=("$PSQL" -h "$SOCKET" -p "$PORT" -d postgres)

"${QP[@]}" -X -v ON_ERROR_STOP=1 -qAtc "CREATE DATABASE $DB_NAME;" >/dev/null
s10_q "CREATE EXTENSION pg_flashback;" >/dev/null

export PGHOST="$SOCKET" PGPORT="$PORT" PGDATABASE="$DB_NAME"
PGUSER_NAME="$(id -un)"
export PGUSER="$PGUSER_NAME"
# Scale-appropriate CLI bounds. These are timeouts, never correctness
# synchronisation: every wait below still polls durable state and fails
# closed on expiry.
export PG_FLASHBACK_STATEMENT_TIMEOUT_MS="${PG_FLASHBACK_STATEMENT_TIMEOUT_MS:-10800000}"
export PG_FLASHBACK_PROTECT_ONLINE_TIMEOUT_S="${PG_FLASHBACK_PROTECT_ONLINE_TIMEOUT_S:-10800}"
export PG_FLASHBACK_RECOVER_HEALTH_TIMEOUT_S="${PG_FLASHBACK_RECOVER_HEALTH_TIMEOUT_S:-7200}"
export PG_FLASHBACK_DISCOVERY_TIMEOUT_S="${PG_FLASHBACK_DISCOVERY_TIMEOUT_S:-600}"
export PATH="$WORK_ROOT/bin:$PG_BIN:$PATH"

# Wait for admitted capture + maintenance workers on the target database.
wait_workers_ready() {
    local deadline=$(( $(date +%s) + 180 )) st
    while (( $(date +%s) <= deadline )); do
        st="$(s10_q "SELECT admission_state || ':' || capture_running::text || ':' || maintenance_running::text
                     FROM flashback_worker_readiness();" 2>/dev/null || true)"
        [[ "$st" == "ready:true:true" ]] && return 0
        sleep 0.5
    done
    die "capture/maintenance workers never became ready (last=$st)"
}
wait_workers_ready
log "workers ready; slot=$(s10_q "SELECT flashback_effective_slot_name();")"

jq -n --arg run_id "$RUN_ID" --arg commit "$EC_SOURCE_COMMIT" \
    --arg tree "$EC_SOURCE_TREE" --arg pkg "$EC_PACKAGE_SHA" \
    --arg extbin "$EC_EXT_BIN_SHA" --arg clibin "$EC_CLI_BIN_SHA" \
    --arg tiers "$TIERS" --arg work "$WORK_ROOT" \
    --arg pgver "$(s10_q 'SELECT version();')" \
    '{run_id:$run_id, tiers:$tiers, work_root:$work, postgresql_version:$pgver,
      candidate:{source_commit:$commit, source_tree:$tree, package_sha256:$pkg,
                 extension_binary_sha256:$extbin, cli_binary_sha256:$clibin}}' \
    > "$RUN_MANIFEST"

# Heartbeat: periodic durable progress/free-space samples.
heartbeat_loop() {
    while true; do
        local progress copier_pid copier_rss_bytes
        progress="$(s10_q "SELECT jsonb_build_object(
            'worker', (SELECT to_jsonb(w) FROM flashback_worker_readiness() w),
            'stream', (SELECT jsonb_build_object(
                'stream_id',stream_id,'state',state,
                'confirmed_flush_lsn',confirmed_flush_lsn::text,
                'invalidation_reason',invalidation_reason)
              FROM flashback.capture_streams
              WHERE database_oid=(SELECT oid FROM pg_database WHERE datname=current_database())
              ORDER BY epoch_no DESC LIMIT 1),
            'lifecycles', COALESCE((SELECT jsonb_agg(jsonb_build_object(
                'tracking_id',tracking_id,'table',format('%I.%I',schema_name,table_name),
                'active',is_active,'protection_state',protection_state)
                ORDER BY tracking_id)
              FROM flashback.tracked_tables
              WHERE is_active OR protection_state IN ('starting','stopping')), '[]'::jsonb)
        );" 2>/dev/null || echo null)"
        jq -e . >/dev/null 2>&1 <<<"$progress" || progress=null
        copier_pid="$(s10_q "SELECT pid FROM pg_stat_activity WHERE backend_type='pg_flashback external_zstd copier' ORDER BY backend_start DESC LIMIT 1;" 2>/dev/null || true)"
        copier_rss_bytes=0
        if [[ "$copier_pid" =~ ^[0-9]+$ ]] && [[ -r "/proc/$copier_pid/status" ]]; then
            copier_rss_bytes="$(( $(awk '/^VmRSS:/ {print $2; exit}' "/proc/$copier_pid/status") * 1024 ))"
        fi
        jq -cn --arg ts "$(date -u +%FT%TZ)" \
            --argjson free_home "$(s10_free_bytes /home)" \
            --argjson pgdata_bytes "$(s10_dir_bytes "$DATA")" \
            --argjson artifact_bytes "$(s10_dir_bytes "$ARTIFACT_ROOT")" \
            --argjson copier_pid "${copier_pid:-0}" \
            --argjson copier_rss_bytes "$copier_rss_bytes" \
            --argjson progress "$progress" \
            '{ts:$ts, free_home_bytes:$free_home, pgdata_bytes:$pgdata_bytes,
              artifact_root_bytes:$artifact_bytes, copier_pid:$copier_pid,
              copier_rss_bytes:$copier_rss_bytes, progress:$progress}' >> "$HEARTBEAT_JSONL" 2>/dev/null || true
        if (( copier_rss_bytes > MAX_COPIER_RSS_BYTES )); then
            jq -cn --arg ts "$(date -u +%FT%TZ)" \
                --argjson copier_pid "$copier_pid" \
                --argjson copier_rss_bytes "$copier_rss_bytes" \
                --argjson limit_bytes "$MAX_COPIER_RSS_BYTES" \
                '{status:"FAIL",reason:"copier_rss_limit_exceeded",ts:$ts,
                  copier_pid:$copier_pid,copier_rss_bytes:$copier_rss_bytes,
                  limit_bytes:$limit_bytes}' > "$RESOURCE_GUARD_JSON"
            kill -TERM "$copier_pid" 2>/dev/null || true
            return 1
        fi
        sleep 5
    done
}
heartbeat_loop &
HEARTBEAT_PID=$!

# ---------------------------------------------------------------------
# Entropy chunk pool.
#
# Payload bytes must be genuinely incompressible so a byte target reflects
# real stored data, never a compression artifact. TOAST compresses each
# datum independently, so a per-row slice of high-entropy material is
# incompressible per datum even though the pool is shared. The pool is
# built once from sha512 digests; per-row uniqueness comes from an md5
# prefix keyed on the row id.
# ---------------------------------------------------------------------
ensure_chunk_pool() {
    local have
    have="$(s10_q "SELECT count(*) FROM pg_class WHERE relname='s10_chunks' AND relnamespace='public'::regnamespace;")"
    if [[ "$have" == "1" ]]; then return 0; fi
    log "building entropy chunk pool (1024 x 8KiB high-entropy chunks)"
    s10_q "CREATE TABLE public.s10_chunks(cid int PRIMARY KEY, body text);" >/dev/null
    s10_q "INSERT INTO public.s10_chunks(cid, body)
           SELECT c, string_agg(encode(sha512((c::text||':'||i::text)::bytea),'hex'),'' ORDER BY i)
           FROM generate_series(0,1023) c CROSS JOIN generate_series(1,64) i
           GROUP BY c;" >/dev/null
    s10_q "ANALYZE public.s10_chunks;" >/dev/null
}

# Shape DDL. Each creates $1 (schema-qualified) with a distinct supported
# shape from docs/SUPPORT.md. All are ordinary permanent LOGGED tables.
create_shape_mixed() {
    local rel=$1
    s10_q "
CREATE TABLE $rel (
    id           bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    account_no   integer NOT NULL,
    amount       numeric(14,2) NOT NULL,
    ratio        numeric(12,6),
    observed_at  timestamp,
    created_at   timestamptz NOT NULL DEFAULT now(),
    is_active    boolean NOT NULL DEFAULT true,
    label        varchar(64) NOT NULL,
    notes        text,
    CONSTRAINT ${rel##*.}_amount_nonneg CHECK (amount >= 0),
    CONSTRAINT ${rel##*.}_label_uq UNIQUE (label)
);
CREATE INDEX ${rel##*.}_account_idx ON $rel (account_no);
CREATE INDEX ${rel##*.}_created_idx ON $rel (created_at);
COMMENT ON TABLE $rel IS 'step10 mixed-width relational shape';
COMMENT ON COLUMN $rel.amount IS 'monetary amount, non-negative';
COMMENT ON COLUMN $rel.label IS 'unique business label';
GRANT SELECT ON $rel TO s10_reader;
" >/dev/null
}

create_shape_toast() {
    local rel=$1
    s10_q "
CREATE TABLE $rel (
    id           bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    kind         varchar(16) NOT NULL,
    incompressible text NOT NULL,
    compressible   text NOT NULL,
    CONSTRAINT ${rel##*.}_kind_chk CHECK (kind IN ('mixed','dense'))
);
CREATE INDEX ${rel##*.}_kind_idx ON $rel (kind);
COMMENT ON TABLE $rel IS 'step10 TOAST-heavy shape (compressible + incompressible payloads)';
COMMENT ON COLUMN $rel.incompressible IS 'high-entropy TOAST payload';
GRANT SELECT ON $rel TO s10_reader;
" >/dev/null
}

create_shape_rich() {
    local rel=$1 short=${1##*.}
    s10_q "
CREATE TABLE public.s10_parent_$short (
    parent_id bigint PRIMARY KEY,
    parent_tag text NOT NULL
);
INSERT INTO public.s10_parent_$short
SELECT g, 'tag-'||g FROM generate_series(1,512) g;
CREATE TABLE $rel (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    parent_id   bigint NOT NULL REFERENCES public.s10_parent_$short(parent_id),
    tenant      text NOT NULL,
    code        varchar(48) NOT NULL,
    qty         integer NOT NULL,
    payload     text,
    CONSTRAINT ${short}_qty_chk CHECK (qty > 0),
    CONSTRAINT ${short}_code_uq UNIQUE (code)
);
CREATE INDEX ${short}_tenant_idx ON $rel (tenant);
ALTER TABLE $rel ENABLE ROW LEVEL SECURITY;
ALTER TABLE $rel FORCE ROW LEVEL SECURITY;
CREATE POLICY ${short}_tenant_policy ON $rel
    USING (tenant = current_setting('s10.tenant', true));
COMMENT ON TABLE $rel IS 'step10 rich supported schema (FK, RLS/FORCE RLS, identity, ACL)';
COMMENT ON COLUMN $rel.tenant IS 'RLS tenant discriminator';
COMMENT ON COLUMN $rel.code IS 'unique code';
GRANT SELECT, INSERT ON $rel TO s10_reader;
" >/dev/null
}

create_shape_hybrid() {
    local rel=$1 short=${1##*.}
    s10_q "
CREATE TABLE $rel (
    id          bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    account_no  integer NOT NULL,
    amount      numeric(14,2) NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    is_active   boolean NOT NULL DEFAULT true,
    code        varchar(64) NOT NULL,
    doc         text,
    CONSTRAINT ${short}_amount_chk CHECK (amount >= 0),
    CONSTRAINT ${short}_code_uq UNIQUE (code)
);
CREATE INDEX ${short}_account_idx ON $rel (account_no);
CREATE INDEX ${short}_active_idx ON $rel (is_active);
COMMENT ON TABLE $rel IS 'step10 realistic hybrid shape (relational columns + bounded TOASTed docs)';
COMMENT ON COLUMN $rel.doc IS 'bounded TOASTed document payload';
GRANT SELECT ON $rel TO s10_reader;
" >/dev/null
}

# Per-shape batch insert. $1=rel $2=first id (exclusive) $3=row count.
# Every payload stays well under pg_flashback.max_row_size (64 KiB).
insert_batch() {
    local shape=$1 rel=$2 start=$3 n=$4
    local lo=$(( start + 1 )) hi=$(( start + n ))
    case "$shape" in
      mixed)
        s10_q "INSERT INTO $rel(id,account_no,amount,ratio,observed_at,created_at,is_active,label,notes)
               SELECT g, (g % 100000)::int, ((g % 900000)::numeric)/100,
                      ((g % 977)::numeric / 977.0),
                      timestamp '2024-01-01 00:00:00' + (g % 500000) * interval '1 second',
                      timestamptz '2024-01-01 00:00:00+00' + (g % 500000) * interval '1 second',
                      (g % 7) <> 0, 'lbl-'||g,
                      substr(c.body, 1, 180) || '-' || g
               FROM generate_series($lo,$hi) g
               JOIN public.s10_chunks c ON c.cid = (g % 1024);" >/dev/null
        ;;
      toast)
        # incompressible: per-row slice of the high-entropy pool (~6 KiB)
        # compressible:   repetitive text (~2 KiB raw) that TOAST will shrink
        s10_q "INSERT INTO $rel(id,kind,incompressible,compressible)
               SELECT g, CASE WHEN g % 2 = 0 THEN 'mixed' ELSE 'dense' END,
                      md5(g::text) || substr(c.body, 33, 6144),
                      repeat('compressible-block-' || (g % 97) || '.', 100)
               FROM generate_series($lo,$hi) g
               JOIN public.s10_chunks c ON c.cid = (g % 1024);" >/dev/null
        ;;
      rich)
        s10_q "INSERT INTO $rel(id,parent_id,tenant,code,qty,payload)
               OVERRIDING SYSTEM VALUE
               SELECT g, (g % 512) + 1,
                      't'||(g % 4), 'code-'||g, (g % 97) + 1,
                      substr(c.body, 1, 900) || '-' || g
               FROM generate_series($lo,$hi) g
               JOIN public.s10_chunks c ON c.cid = (g % 1024);" >/dev/null
        ;;
      hybrid)
        # ~70% of rows carry a bounded TOASTed doc, the rest are purely
        # relational -- a realistic mixed table rather than a uniform blob.
        s10_q "INSERT INTO $rel(id,account_no,amount,created_at,is_active,code,doc)
               SELECT g, (g % 250000)::int, ((g % 900000)::numeric)/100,
                      timestamptz '2024-01-01 00:00:00+00' + (g % 2000000) * interval '1 second',
                      (g % 11) <> 0, 'code-'||g,
                      CASE WHEN g % 10 < 7
                           THEN md5(g::text) || substr(c.body, 33)
                           ELSE 'short-' || g END
               FROM generate_series($lo,$hi) g
               JOIN public.s10_chunks c ON c.cid = (g % 1024);" >/dev/null
        ;;
      *) die "insert_batch: unknown shape $shape" ;;
    esac
}

# Parallel batch insert across N workers for the large tiers.
insert_batch_parallel() {
    local shape=$1 rel=$2 start=$3 n=$4 par=${5:-4}
    local per=$(( n / par )) k lo cnt pids=()
    (( per < 1 )) && { insert_batch "$shape" "$rel" "$start" "$n"; return 0; }
    for (( k = 0; k < par; k++ )); do
        lo=$(( start + k * per ))
        cnt=$per
        (( k == par - 1 )) && cnt=$(( n - k * per ))
        ( insert_batch "$shape" "$rel" "$lo" "$cnt" ) &
        pids+=($!)
    done
    local p rc=0
    for p in "${pids[@]}"; do wait "$p" || rc=1; done
    (( rc == 0 )) || die "parallel insert batch failed for $rel"
}

# Load $rel to within +/-5% of $target_bytes measured by pg_table_size
# (heap+TOAST only; indexes are recorded separately and never counted
# toward the tier). Learns real bytes/row from a small calibration batch
# rather than assuming a width, then converges on 98% of target.
# Sets LOAD_ROWS / LOAD_BYTES / LOAD_ITERS / LOAD_SECONDS.
load_to_target() {
    local shape=$1 rel=$2 target_bytes=$3 par=${4:-4}
    local low=$(( target_bytes * 96 / 100 ))
    local aim=$(( target_bytes * 98 / 100 ))
    local rows=0 bytes=0 iter=0 batch=2000 bpr t0 t1
    t0="$(s10_now_ms)"
    while (( bytes < low && iter < 200 )); do
        iter=$(( iter + 1 ))
        (( batch < 1 )) && batch=1
        if (( batch >= 20000 )); then
            insert_batch_parallel "$shape" "$rel" "$rows" "$batch" "$par"
        else
            insert_batch "$shape" "$rel" "$rows" "$batch"
        fi
        rows=$(( rows + batch ))
        bytes="$(s10_q "SELECT pg_table_size('$rel');")"
        [[ "$bytes" =~ ^[0-9]+$ ]] || die "load_to_target: non-numeric pg_table_size for $rel ('$bytes')"
        bpr=$(( bytes / rows )); (( bpr < 1 )) && bpr=1
        if (( bytes < aim )); then
            batch=$(( (aim - bytes) / bpr ))
            (( batch < 1 )) && batch=1
            # cap a single batch so progress stays observable in heartbeats
            (( batch > 400000 )) && batch=400000
        else
            batch=0
        fi
        if (( iter % 5 == 0 )); then
            log "  load $rel: rows=$rows bytes=$bytes/$target_bytes ($(( bytes * 100 / target_bytes ))%)"
        fi
    done
    t1="$(s10_now_ms)"
    LOAD_ROWS=$rows
    # shellcheck disable=SC2034  # recorded for operator debugging of the load loop
    LOAD_BYTES=$bytes
    # shellcheck disable=SC2034  # recorded for operator debugging of the load loop
    LOAD_ITERS=$iter
    LOAD_SECONDS=$(( (t1 - t0) / 1000 ))
}

# ---------------------------------------------------------------------
# Concurrent writer.
#
# Writes into the protected table itself, one committed transaction per
# iteration, recording per-commit latency and the commit wall clock. Every
# Nth iteration deliberately ROLLBACKs an insert carrying a poisoned
# marker so recovery can be proven to contain no aborted work.
#
# Emits: $WRITER_DIR/<tag>.lat (ms per commit), <tag>.commits (one
# "seq epoch_ms" per committed row), <tag>.rollbacks (count).
# ---------------------------------------------------------------------
writer_start() {
    local shape=$1 rel=$2 tag=$3 base_id=$4
    local lat="$WRITER_DIR/$tag.lat"
    local commits="$WRITER_DIR/$tag.commits"
    local rolled="$WRITER_DIR/$tag.rollbacks"
    : > "$lat"; : > "$commits"; : > "$rolled"
    (
        set +e
        local i=0 id t0 t1
        while [[ ! -f "$WRITER_DIR/$tag.stop" ]]; do
            i=$(( i + 1 ))
            id=$(( base_id + i ))
            t0=$(date +%s%3N)
            if (( i % 20 == 0 )); then
                # Aborted transaction: must never appear after recovery.
                "$PSQL" -h "$SOCKET" -p "$PORT" -d "$DB_NAME" -X -qAt >/dev/null 2>&1 <<SQL
BEGIN;
$(writer_insert_sql "$shape" "$rel" "$id" "ROLLBACK")
ROLLBACK;
SQL
                echo 1 >> "$rolled"
            else
                "$PSQL" -h "$SOCKET" -p "$PORT" -d "$DB_NAME" -X -v ON_ERROR_STOP=1 -qAt >/dev/null 2>&1 <<SQL
$(writer_insert_sql "$shape" "$rel" "$id" "COMMIT")
SQL
                if (( $? == 0 )); then
                    t1=$(date +%s%3N)
                    echo $(( t1 - t0 )) >> "$lat"
                    echo "$id $t1" >> "$commits"
                fi
            fi
        done
    ) &
    WRITER_PID=$!
    log "  writer started (pid=$WRITER_PID tag=$tag base_id=$base_id)"
}

# Per-shape single-row writer statement. WRITER_MARK distinguishes writer
# rows from bulk-loaded rows; poisoned rollback rows use a marker that must
# never survive.
writer_insert_sql() {
    local shape=$1 rel=$2 id=$3 mode=$4
    local tag="w"; [[ "$mode" == "ROLLBACK" ]] && tag="rollback-poison"
    case "$shape" in
      mixed)
        echo "INSERT INTO $rel(id,account_no,amount,ratio,observed_at,created_at,is_active,label,notes)
              VALUES ($id, $(( id % 100000 )), 1.25, 0.5, localtimestamp, now(), true,
                      '$tag-lbl-$id', '$tag-note-$id');"
        ;;
      toast)
        echo "INSERT INTO $rel(id,kind,incompressible,compressible)
              VALUES ($id, 'dense', md5('$tag$id') || repeat(md5('$id'), 8), '$tag-compressible-$id');"
        ;;
      hybrid)
        echo "INSERT INTO $rel(id,account_no,amount,created_at,is_active,code,doc)
              VALUES ($id, $(( id % 250000 )), 2.50, now(), true, '$tag-code-$id', '$tag-doc-$id');"
        ;;
      *) echo "SELECT 1;" ;;
    esac
}

writer_stop() {
    local tag=$1
    [[ -n "$WRITER_PID" ]] || return 0
    touch "$WRITER_DIR/$tag.stop"
    local _
    for _ in $(seq 1 100); do
        kill -0 "$WRITER_PID" 2>/dev/null || break
        sleep 0.2
    done
    kill "$WRITER_PID" 2>/dev/null || true
    wait "$WRITER_PID" 2>/dev/null || true
    WRITER_PID=""
}

# ---------------------------------------------------------------------
# Durable capture progress. Polls the pg_flashback-owned watermark, never
# a fixed sleep and never a manual flashback_consume_wal() bypass.
# ---------------------------------------------------------------------
wait_capture_caught_up() {
    local label=$1 timeout_s=${2:-3600}
    local deadline=$(( $(date +%s) + timeout_s )) target lag
    target="$(s10_q "SELECT pg_current_wal_lsn();")"
    while (( $(date +%s) <= deadline )); do
        lag="$(s10_q "SELECT COALESCE(pg_wal_lsn_diff('$target'::pg_lsn,
                         (SELECT confirmed_flush_lsn FROM flashback.capture_streams
                           WHERE database_oid=(SELECT oid FROM pg_database WHERE datname=current_database())
                           ORDER BY epoch_no DESC LIMIT 1)), 9223372036854775807)::bigint;")"
        [[ "$lag" =~ ^-?[0-9]+$ ]] || { sleep 1; continue; }
        (( lag <= 0 )) && return 0
        sleep 1
    done
    die "$label: durable capture frontier did not reach $target within ${timeout_s}s (last lag=${lag}B)"
}

# Non-fatal variant: returns 1 instead of exiting, so a verification step
# can record an explicit failed check rather than aborting the whole run.
wait_lifecycle_healthy_soft() {
    local rel=$1 timeout_s=${2:-1800}
    local deadline=$(( $(date +%s) + timeout_s )) h
    while (( $(date +%s) <= deadline )); do
        h="$(s10_q "SELECT COALESCE((SELECT health FROM flashback_health()
                      WHERE table_name='$rel' ORDER BY generation_id DESC LIMIT 1),'missing');" 2>/dev/null || true)"
        [[ "$h" == "healthy" ]] && return 0
        sleep 1
    done
    return 1
}

wait_lifecycle_healthy() {
    local rel=$1 timeout_s=${2:-1800}
    local deadline=$(( $(date +%s) + timeout_s )) h
    while (( $(date +%s) <= deadline )); do
        h="$(s10_q "SELECT COALESCE((SELECT health FROM flashback_health()
                      WHERE table_name='$rel' ORDER BY generation_id DESC LIMIT 1),'missing');")"
        [[ "$h" == "healthy" ]] && return 0
        sleep 1
    done
    die "lifecycle for $rel never became healthy (last=$h)"
}

# ---------------------------------------------------------------------
# Assertion plumbing. Every check appends a structured record; a tier is
# PASS only when zero checks failed.
# ---------------------------------------------------------------------
TIER_CHECKS_JSON='[]'
TIER_FAILED=0

chk() {
    local name=$1 ok=$2 detail=${3:-}
    if [[ "$ok" == "1" || "$ok" == "true" ]]; then
        TIER_CHECKS_JSON="$(jq -n --argjson a "$TIER_CHECKS_JSON" --arg n "$name" --arg d "$detail" \
            '$a + [{check:$n, status:"pass", detail:$d}]')"
        log "    ok: $name${detail:+ ($detail)}"
    else
        TIER_CHECKS_JSON="$(jq -n --argjson a "$TIER_CHECKS_JSON" --arg n "$name" --arg d "$detail" \
            '$a + [{check:$n, status:"fail", detail:$d}]')"
        TIER_FAILED=$(( TIER_FAILED + 1 ))
        log "    FAIL: $name${detail:+ ($detail)}"
    fi
}

chk_eq() {
    local name=$1 expected=$2 actual=$3
    if [[ "$expected" == "$actual" ]]; then
        chk "$name" 1 "value=$actual"
    else
        chk "$name" 0 "expected=$expected actual=$actual"
    fi
}

schema_fingerprint_file() {
    local rel=$1 out=$2
    "$PG_DUMP" -h "$SOCKET" -p "$PORT" -d "$DB_NAME" --schema-only \
        -t "$rel" > "$out" 2>/dev/null || return 1
    normalize_schema_dump_in_place "$out"
    sha256sum "$out" | awk '{print $1}'
}

artifact_root_bytes_now() { s10_dir_bytes "$ARTIFACT_ROOT"; }

# ---------------------------------------------------------------------
# One tier. Sequential, self-cleaning, evidence written before teardown.
# ---------------------------------------------------------------------
run_tier() {
    local tier=$1
    local shape target_gib writer=0 churn_pct=0 negative=0
    case "$tier" in
      t1_mixed)              shape=mixed;  target_gib=1;  writer=0; churn_pct=5 ;;
      t1_toast)              shape=toast;  target_gib=1;  writer=0; churn_pct=5 ;;
      t1_rich)               shape=rich;   target_gib=1;  writer=0; churn_pct=5 ;;
      t1_negative_capacity)  shape=mixed;  target_gib=1;  writer=0; churn_pct=0; negative=1 ;;
      t10_mixed)             shape=mixed;  target_gib=10; writer=1; churn_pct=5 ;;
      t10_toast)             shape=toast;  target_gib=10; writer=0; churn_pct=5 ;;
      t25_mixed)             shape=mixed;  target_gib=25; writer=1; churn_pct=2 ;;
      t50_hybrid)            shape=hybrid; target_gib=50; writer=1; churn_pct=1 ;;
      *) die "unknown tier $tier" ;;
    esac

    local rel="public.s10_${tier}"
    local tag="$tier"
    local target_bytes=$(( target_gib * GIB ))
    local tier_json="$EVIDENCE_DIR/tiers/$tier.json"
    TIER_CHECKS_JSON='[]'
    TIER_FAILED=0

    log "=== tier $tier (shape=$shape target=${target_gib}GiB writer=$writer churn=${churn_pct}%) ==="

    # -- capacity precheck: refuse to allocate rather than fake a pass ----
    local est_index_bytes=$(( target_bytes / 4 ))
    local cap_json
    if ! cap_json="$(s10_capacity_precheck "$tier" "$target_bytes" "$est_index_bytes" /home "$RESERVE_BYTES")"; then
        log "  BLOCKED_CAPACITY for $tier"
        jq -n --arg tier "$tier" --arg shape "$shape" --argjson cap "$cap_json" \
            '{tier:$tier, shape:$shape, status:"BLOCKED_CAPACITY", capacity:$cap,
              note:"tier not allocated; emergency reserve would be violated"}' > "$tier_json"
        return 1
    fi
    log "  capacity ok: free=$(jq -r '.path_free_bytes' <<<"$cap_json") est_peak=$(jq -r '.estimated_peak_bytes' <<<"$cap_json")"

    local free_before; free_before="$(s10_free_bytes /home)"
    ensure_chunk_pool

    # -- create + load --------------------------------------------------
    case "$shape" in
      mixed)  create_shape_mixed  "$rel" ;;
      toast)  create_shape_toast  "$rel" ;;
      rich)   create_shape_rich   "$rel" ;;
      hybrid) create_shape_hybrid "$rel" ;;
    esac
    load_to_target "$shape" "$rel" "$target_bytes" 4
    s10_q "ANALYZE $rel;" >/dev/null
    s10_q "CHECKPOINT;" >/dev/null
    local sizes; sizes="$(s10_relation_metrics "$rel")"
    local actual_bytes pct_of_target
    actual_bytes="$(jq -r '.pg_table_size' <<<"$sizes")"
    pct_of_target=$(( actual_bytes * 100 / target_bytes ))
    log "  loaded rows=$LOAD_ROWS pg_table_size=$actual_bytes (${pct_of_target}% of target) in ${LOAD_SECONDS}s"
    chk "tier_size_within_5pct" \
        "$(( pct_of_target >= 95 && pct_of_target <= 105 ? 1 : 0 ))" \
        "pg_table_size=$actual_bytes target=$target_bytes pct=${pct_of_target}"

    # -- negative capacity/admission scenario ---------------------------
    if (( negative == 1 )); then
        run_negative_capacity "$tier" "$rel" "$sizes" "$tier_json" "$free_before"
        return $?
    fi

    # -- concurrent writer over the online copy -------------------------
    local writer_base_id=$(( 900000000 )) wdead
    local copy_start_ms copy_end_ms
    if (( writer == 1 )); then
        writer_start "$shape" "$rel" "$tag" "$writer_base_id"
        # Deterministic warm-up: wait until the writer has actually recorded
        # a commit, so protect never starts against an idle writer. This is a
        # readiness poll with a deadline, not a correctness sleep -- the real
        # overlap proof is the commit-window assertion after protect.
        wdead=$(( $(date +%s) + 120 ))
        while (( $(date +%s) <= wdead )); do
            [[ -s "$WRITER_DIR/$tag.commits" ]] && break
            sleep 0.2
        done
        [[ -s "$WRITER_DIR/$tag.commits" ]] \
            || die "concurrent writer produced no commit within 120s; overlap could not be established"
    fi

    # -- protect (production CLI, real online external_zstd path) -------
    local protect_log="$LOG_DIR/$tier-protect.log"
    copy_start_ms="$(s10_now_ms)"
    local protect_rc=0
    "$CLI" protect "$rel" > "$protect_log" 2>&1 || protect_rc=$?
    copy_end_ms="$(s10_now_ms)"
    local protect_ms=$(( copy_end_ms - copy_start_ms ))
    chk_eq "protect_exit_zero" "0" "$protect_rc"
    log "  protect completed in ${protect_ms}ms (rc=$protect_rc)"
    if (( protect_rc != 0 )); then
        log "  protect failed; see $protect_log"
        sed -n '1,40p' "$protect_log" >&2
        (( writer == 1 )) && writer_stop "$tag"
        cleanup_tier_lifecycle "$rel" "" || true
        jq -n --arg tier "$tier" --arg shape "$shape" --argjson sizes "$sizes" \
            --argjson checks "$TIER_CHECKS_JSON" --argjson failed "$TIER_FAILED" \
            --arg protect_err "$(tail -c 2000 "$protect_log" | tr -d '\000')" \
            '{tier:$tier, shape:$shape, status:"FAIL", failed_checks:$failed,
              measured:$sizes, failure_stage:"protect", protect_error:$protect_err,
              checks:$checks}' > "$tier_json"
        log "=== tier $tier: FAIL (protect) ==="
        return 1
    fi

    local tracking_id snapshot_id
    tracking_id="$(s10_q "SELECT tracking_id FROM flashback.tracked_tables
                          WHERE is_active AND format('%I.%I',schema_name,table_name)='$rel'
                          ORDER BY tracking_id DESC LIMIT 1;")"
    chk "tracking_id_present" "$([[ -n "$tracking_id" ]] && echo 1 || echo 0)" "tracking_id=$tracking_id"

    # -- writer overlap proof -------------------------------------------
    local writer_json='null'
    if (( writer == 1 )); then
        writer_stop "$tag"
        local commits_file="$WRITER_DIR/$tag.commits"
        local lat_file="$WRITER_DIR/$tag.lat"
        local total_commits during_commits rollbacks
        total_commits="$(wc -l < "$commits_file" 2>/dev/null || echo 0)"
        during_commits="$(awk -v a="$copy_start_ms" -v b="$copy_end_ms" \
            '$2 >= a && $2 <= b {n++} END {print n+0}' "$commits_file" 2>/dev/null || echo 0)"
        rollbacks="$(wc -l < "$WRITER_DIR/$tag.rollbacks" 2>/dev/null || echo 0)"
        local p50 p95 p99 pmax
        p50="$(s10_percentile "$lat_file" 50)"; p95="$(s10_percentile "$lat_file" 95)"
        p99="$(s10_percentile "$lat_file" 99)"; pmax="$(s10_percentile "$lat_file" 100)"
        local expected_writer_sha
        expected_writer_sha="$(awk '{print $1}' "$commits_file" | LC_ALL=C sort -n | sha256sum | awk '{print $1}')"
        writer_json="$(jq -n --argjson total "$total_commits" --argjson during "$during_commits" \
            --argjson rb "$rollbacks" --argjson p50 "$p50" --argjson p95 "$p95" \
            --argjson p99 "$p99" --argjson pmax "$pmax" \
            --argjson cs "$copy_start_ms" --argjson ce "$copy_end_ms" --arg idsha "$expected_writer_sha" \
            '{total_commits:$total, commits_during_protect:$during, rolled_back_txns:$rb,
              expected_id_sha256:$idsha,
              latency_ms:{p50:$p50,p95:$p95,p99:$p99,max:$pmax},
              protect_window_ms:{start:$cs,end:$ce}}')"
        # A writer that never actually overlapped the copy proves nothing.
        chk "writer_overlapped_online_copy" \
            "$(( during_commits > 0 ? 1 : 0 ))" \
            "commits_during_protect=$during_commits total=$total_commits"
        log "  writer: total=$total_commits during_copy=$during_commits rollbacks=$rollbacks p50=${p50}ms p95=${p95}ms p99=${p99}ms max=${pmax}ms"
    fi

    wait_capture_caught_up "$tier post-protect" 3600
    wait_lifecycle_healthy "$rel" 1800

    # -- artifact assertions (before any churn/DROP) --------------------
    local snap_json
    snap_json="$(s10_q "SELECT jsonb_build_object(
        'snapshot_id', s.snapshot_id, 'storage_backend', s.storage_backend,
        'payload_state', s.payload_state, 'external_codec', s.external_codec,
        'external_compressed_bytes', s.external_compressed_bytes,
        'external_uncompressed_bytes', s.external_uncompressed_bytes,
        'external_checksum_sha256', s.external_checksum_sha256,
        'schema_def_sha256', s.schema_def_sha256,
        'snapshot_lsn', s.snapshot_lsn::text,
        'locator', s.locator, 'row_count', s.row_count)
      FROM flashback.snapshots s
      WHERE s.tracking_id=$tracking_id AND s.payload_state='available'
      ORDER BY s.snapshot_id DESC LIMIT 1;")"
    snapshot_id="$(jq -r '.snapshot_id // empty' <<<"$snap_json")"
    chk_eq "artifact_backend_external_zstd" "external_zstd" "$(jq -r '.storage_backend // ""' <<<"$snap_json")"
    chk_eq "artifact_payload_state_available" "available" "$(jq -r '.payload_state // ""' <<<"$snap_json")"
    chk "artifact_sha256_bound" \
        "$(jq -r '(.external_checksum_sha256 // "") | test("^[0-9a-f]{64}$") | if . then 1 else 0 end' <<<"$snap_json")" \
        "sha256=$(jq -r '.external_checksum_sha256 // "none"' <<<"$snap_json")"
    chk "artifact_locator_bound" \
        "$(jq -r 'if (.locator != null) then 1 else 0 end' <<<"$snap_json")" \
        "locator=$(jq -c '.locator' <<<"$snap_json")"
    # No half-written staging artifact may ever be presented as available.
    local staging_available
    staging_available="$(s10_q "SELECT count(*) FROM flashback.snapshots
        WHERE tracking_id=$tracking_id AND payload_state='available'
          AND (external_checksum_sha256 IS NULL OR locator IS NULL OR snapshot_lsn IS NULL);")"
    chk_eq "no_partial_artifact_marked_available" "0" "$staging_available"
    # The product's own payload-health gate (what restore admission uses).
    local payload_health
    payload_health="$(s10_q "SELECT status FROM public.flashback_internal_snapshot_payload_healthy($snapshot_id, $tracking_id, false) LIMIT 1;" 2>/dev/null || echo unknown)"
    chk_eq "artifact_payload_health_healthy" "healthy" "$payload_health"
    local artifact_bytes_fs; artifact_bytes_fs="$(artifact_root_bytes_now)"
    log "  artifact: compressed=$(jq -r '.external_compressed_bytes' <<<"$snap_json") uncompressed=$(jq -r '.external_uncompressed_bytes' <<<"$snap_json") fs_bytes=$artifact_bytes_fs"

    # -- churn -----------------------------------------------------------
    local churn_json='null'
    if (( churn_pct > 0 )); then
        churn_json="$(apply_churn "$shape" "$rel" "$churn_pct" "$LOAD_ROWS")"
        wait_capture_caught_up "$tier post-churn" 3600
    fi

    # -- expected evidence, captured independently before the DROP ------
    local pre_fp pre_meta pre_schema_sha
    pre_fp="$(s10_data_fingerprint "$rel")"
    pre_meta="$(s10_metadata_fingerprint "$rel")"
    pre_schema_sha="$(schema_fingerprint_file "$rel" "$LOG_DIR/$tier-schema-before.sql")"
    local pre_pk_distinct pre_pk_min pre_pk_max
    pre_pk_distinct="$(s10_q "SELECT count(DISTINCT id) FROM $rel;")"
    pre_pk_min="$(s10_q "SELECT COALESCE(min(id)::text,'none') FROM $rel;")"
    pre_pk_max="$(s10_q "SELECT COALESCE(max(id)::text,'none') FROM $rel;")"
    log "  pre-DROP: rows=$(jq -r '.row_count' <<<"$pre_fp") digest=$(jq -r '.digest' <<<"$pre_fp")"

    # -- immediate DROP, then recovery with no fixed sleep --------------
    local t_drop0 t_drop1
    t_drop0="$(s10_now_ms)"
    s10_q "DROP TABLE $rel;" >/dev/null
    t_drop1="$(s10_now_ms)"

    local discover_ms t_disc0 t_disc1 dryrun_rc=0
    t_disc0="$(s10_now_ms)"
    "$CLI" --json recover "$rel" --dry-run --latest-drop > "$LOG_DIR/$tier-dryrun.json" 2>"$LOG_DIR/$tier-dryrun.err" || dryrun_rc=$?
    t_disc1="$(s10_now_ms)"
    discover_ms=$(( t_disc1 - t_disc0 ))
    chk_eq "drop_discovery_dryrun_exit_zero" "0" "$dryrun_rc"
    chk_eq "drop_discovery_code_dry_run" "dry_run" \
        "$(jq -r '.code // ""' "$LOG_DIR/$tier-dryrun.json" 2>/dev/null || echo parse_error)"
    log "  DROP discovered + planned in ${discover_ms}ms (no fixed sleep)"

    local plan_tracking plan_generation plan_event
    plan_tracking="$(jq -r '.data.plan.tracking_id // ""' "$LOG_DIR/$tier-dryrun.json" 2>/dev/null || echo "")"
    plan_generation="$(jq -r '.data.plan.generation_id // ""' "$LOG_DIR/$tier-dryrun.json" 2>/dev/null || echo "")"
    plan_event="$(jq -r '.data.plan.disaster_event_id // ""' "$LOG_DIR/$tier-dryrun.json" 2>/dev/null || echo "")"
    chk_eq "plan_binds_expected_tracking_id" "$tracking_id" "$plan_tracking"
    chk "plan_binds_disaster_event" "$([[ -n "$plan_event" && "$plan_event" != "null" ]] && echo 1 || echo 0)" \
        "disaster_event_id=$plan_event generation_id=$plan_generation"

    local recover_ms t_rec0 t_rec1 recover_rc=0
    t_rec0="$(s10_now_ms)"
    "$CLI" --json recover "$rel" --latest-drop --yes > "$LOG_DIR/$tier-recover.json" 2>"$LOG_DIR/$tier-recover.err" || recover_rc=$?
    t_rec1="$(s10_now_ms)"
    recover_ms=$(( t_rec1 - t_rec0 ))
    chk_eq "recover_exit_zero" "0" "$recover_rc"
    (( recover_rc == 0 )) || sed -n '1,40p' "$LOG_DIR/$tier-recover.err" >&2
    log "  recovery completed in ${recover_ms}ms (rc=$recover_rc)"

    # -- post-recovery correctness matrix -------------------------------
    verify_after_recovery "$tier" "$rel" "$shape" "$tracking_id" \
        "$pre_fp" "$pre_meta" "$pre_schema_sha" "$pre_pk_distinct" "$pre_pk_min" "$pre_pk_max" \
        "$churn_json" "$writer"

    local post_sizes; post_sizes="$(s10_relation_metrics "$rel" 2>/dev/null || echo '{}')"
    local peak_free_min
    peak_free_min="$(jq -s 'map(.free_home_bytes) | min // 0' "$HEARTBEAT_JSONL" 2>/dev/null || echo 0)"

    # -- cleanup + reclamation ------------------------------------------
    local t_cl0 t_cl1 cleanup_ms
    t_cl0="$(s10_now_ms)"
    local cleanup_rc=0
    cleanup_tier_lifecycle "$rel" "$tracking_id" || cleanup_rc=$?
    t_cl1="$(s10_now_ms)"
    cleanup_ms=$(( t_cl1 - t_cl0 ))
    local free_after; free_after="$(s10_free_bytes /home)"
    local artifact_after; artifact_after="$(artifact_root_bytes_now)"
    log "  cleanup ${cleanup_ms}ms; artifact_root now ${artifact_after}B; free ${free_after}B"
    local cleanup_active cleanup_payloads
    cleanup_active="$(s10_q "SELECT count(*) FROM flashback.tracked_tables
        WHERE tracking_id=$tracking_id AND is_active;" 2>/dev/null || echo -1)"
    cleanup_payloads="$(s10_q "SELECT count(*) FROM flashback.snapshots
        WHERE tracking_id=$tracking_id
          AND payload_state IN ('creating','available','retiring');" 2>/dev/null || echo -1)"
    chk_eq "cleanup_cli_exit_zero" "0" "$cleanup_rc"
    chk_eq "cleanup_no_active_lifecycle" "0" "$cleanup_active"
    chk_eq "cleanup_no_live_payload" "0" "$cleanup_payloads"
    chk "cleanup_external_bytes_reclaimed" \
        "$(( artifact_after <= 1048576 ? 1 : 0 ))" \
        "artifact_root_bytes_after=$artifact_after"

    local status=PASS
    (( TIER_FAILED == 0 )) || status=FAIL

    jq -n --arg tier "$tier" --arg shape "$shape" --arg status "$status" \
        --argjson target_bytes "$target_bytes" --argjson sizes "$sizes" \
        --argjson post_sizes "$post_sizes" \
        --argjson load_rows "$LOAD_ROWS" --argjson load_seconds "$LOAD_SECONDS" \
        --argjson capacity "$cap_json" --argjson artifact "$snap_json" \
        --argjson artifact_fs_bytes "$artifact_bytes_fs" \
        --argjson writer "$writer_json" --argjson churn "$churn_json" \
        --argjson checks "$TIER_CHECKS_JSON" --argjson failed "$TIER_FAILED" \
        --argjson protect_ms "$protect_ms" --argjson discovery_ms "$discover_ms" \
        --argjson recover_ms "$recover_ms" --argjson cleanup_ms "$cleanup_ms" \
        --argjson drop_ms "$(( t_drop1 - t_drop0 ))" \
        --argjson free_before "$free_before" --argjson free_after "$free_after" \
        --argjson free_min_observed "$peak_free_min" \
        --argjson artifact_after "$artifact_after" \
        --argjson pre_fp "$pre_fp" \
        '{tier:$tier, shape:$shape, status:$status, failed_checks:$failed,
          requested_bytes:$target_bytes, measured:$sizes, measured_after_recovery:$post_sizes,
          load:{rows:$load_rows, seconds:$load_seconds},
          capacity:$capacity, artifact:$artifact, artifact_root_fs_bytes:$artifact_fs_bytes,
          concurrent_writer:$writer, churn:$churn,
          durations_ms:{protect_bundled:$protect_ms, drop_statement:$drop_ms,
                        drop_discovery_and_plan:$discovery_ms,
                        recover_bundled:$recover_ms, cleanup:$cleanup_ms},
          duration_note:"protect_bundled and recover_bundled are whole-CLI-command wall clock; sub-phases inside the server-side copy/restore are not separately instrumented and are deliberately not reported as separate phases",
          disk:{free_home_before:$free_before, free_home_after:$free_after,
                free_home_min_observed:$free_min_observed,
                artifact_root_bytes_after_cleanup:$artifact_after},
          pre_drop_fingerprint:$pre_fp,
          checks:$checks}' > "$tier_json"

    log "=== tier $tier: $status (failed_checks=$TIER_FAILED) ==="
    [[ "$status" == "PASS" ]]
}

# ---------------------------------------------------------------------
# Post-protection churn: INSERT + UPDATE + DELETE over ~pct% of rows,
# in small batches plus one bounded larger transaction. Every effect is
# marked so it can be asserted individually at the DROP boundary.
# ---------------------------------------------------------------------
apply_churn() {
    local shape=$1 rel=$2 pct=$3 loaded_rows=$4
    local n=$(( loaded_rows * pct / 100 ))
    (( n < 100 )) && n=100
    local ins=$(( n / 3 )) upd=$(( n / 3 )) del=$(( n / 3 ))
    local ins_base=$(( 800000000 ))
    local t0 t1
    t0="$(s10_now_ms)"

    # small-batch INSERTs (several transactions)
    local b batch=$(( ins / 4 )); (( batch < 1 )) && batch=1
    for b in 0 1 2 3; do
        insert_batch "$shape" "$rel" $(( ins_base + b * batch )) "$batch"
    done
    # one bounded larger transaction combining UPDATE + DELETE
    case "$shape" in
      mixed)
        s10_q "BEGIN;
               UPDATE $rel SET notes='churn-upd-'||id, amount=amount+1
                 WHERE id BETWEEN 1 AND $upd;
               DELETE FROM $rel WHERE id BETWEEN $(( upd + 1 )) AND $(( upd + del ));
               COMMIT;" >/dev/null ;;
      toast)
        s10_q "BEGIN;
               UPDATE $rel SET compressible='churn-upd-'||id WHERE id BETWEEN 1 AND $upd;
               DELETE FROM $rel WHERE id BETWEEN $(( upd + 1 )) AND $(( upd + del ));
               COMMIT;" >/dev/null ;;
      rich)
        s10_q "BEGIN;
               UPDATE $rel SET payload='churn-upd-'||id WHERE id BETWEEN 1 AND $upd;
               DELETE FROM $rel WHERE id BETWEEN $(( upd + 1 )) AND $(( upd + del ));
               COMMIT;" >/dev/null ;;
      hybrid)
        s10_q "BEGIN;
               UPDATE $rel SET doc='churn-upd-'||id, amount=amount+1 WHERE id BETWEEN 1 AND $upd;
               DELETE FROM $rel WHERE id BETWEEN $(( upd + 1 )) AND $(( upd + del ));
               COMMIT;" >/dev/null ;;
    esac
    t1="$(s10_now_ms)"

    local upd_now del_now ins_now upd_col=notes
    case "$shape" in
      toast)  upd_col=compressible ;;
      rich)   upd_col=payload ;;
      hybrid) upd_col=doc ;;
    esac
    upd_now="$(s10_q "SELECT count(*) FROM $rel WHERE $upd_col LIKE 'churn-upd-%';")"
    del_now="$(s10_q "SELECT count(*) FROM $rel WHERE id BETWEEN $(( upd + 1 )) AND $(( upd + del ));")"
    ins_now="$(s10_q "SELECT count(*) FROM $rel WHERE id > $ins_base;")"

    jq -n --argjson pct "$pct" --argjson planned_ins "$ins" --argjson planned_upd "$upd" \
        --argjson planned_del "$del" --argjson observed_ins "$ins_now" \
        --argjson observed_upd "$upd_now" --argjson still_present_in_deleted_range "$del_now" \
        --argjson ins_base "$ins_base" --argjson upd_hi "$upd" \
        --argjson del_lo "$(( upd + 1 ))" --argjson del_hi "$(( upd + del ))" \
        --argjson ms "$(( t1 - t0 ))" \
        '{percent_of_rows:$pct, planned:{insert:$planned_ins,update:$planned_upd,delete:$planned_del},
          observed:{inserted_rows_above_base:$observed_ins, updated_rows_marked:$observed_upd,
                    rows_remaining_in_deleted_range:$still_present_in_deleted_range},
          markers:{insert_id_base:$ins_base, update_id_max:$upd_hi,
                   delete_id_range:[$del_lo,$del_hi]},
          duration_ms:$ms}'
}

# ---------------------------------------------------------------------
# Post-recovery correctness matrix (the 16 required proofs).
# ---------------------------------------------------------------------
verify_after_recovery() {
    local tier=$1 rel=$2 shape=$3 tracking_id=$4
    local pre_fp=$5 pre_meta=$6 pre_schema_sha=$7
    local pre_pk_distinct=$8 pre_pk_min=$9 pre_pk_max=${10}
    local churn_json=${11} writer=${12}

    local exists
    exists="$(s10_q "SELECT count(*) FROM pg_class WHERE oid = to_regclass('$rel');" 2>/dev/null || echo 0)"
    chk_eq "recovered_relation_exists" "1" "$exists"
    [[ "$exists" == "1" ]] || return 0

    # 1 + 2: exact row count and full logical fingerprint
    local post_fp; post_fp="$(s10_data_fingerprint "$rel")"
    chk_eq "row_count_exact" "$(jq -r '.row_count' <<<"$pre_fp")" "$(jq -r '.row_count' <<<"$post_fp")"
    chk_eq "logical_data_digest" "$(jq -r '.digest' <<<"$pre_fp")" "$(jq -r '.digest' <<<"$post_fp")"
    chk_eq "logical_data_xor64" "$(jq -r '.xor64' <<<"$pre_fp")" "$(jq -r '.xor64' <<<"$post_fp")"
    chk_eq "logical_data_sum32" "$(jq -r '.sum32' <<<"$pre_fp")" "$(jq -r '.sum32' <<<"$post_fp")"

    # 3: primary-key domain and uniqueness
    local post_pk_distinct post_pk_min post_pk_max post_rows
    post_pk_distinct="$(s10_q "SELECT count(DISTINCT id) FROM $rel;")"
    post_pk_min="$(s10_q "SELECT COALESCE(min(id)::text,'none') FROM $rel;")"
    post_pk_max="$(s10_q "SELECT COALESCE(max(id)::text,'none') FROM $rel;")"
    post_rows="$(s10_q "SELECT count(*) FROM $rel;")"
    chk_eq "pk_distinct_count" "$pre_pk_distinct" "$post_pk_distinct"
    chk_eq "pk_unique_equals_rowcount" "$post_rows" "$post_pk_distinct"
    chk_eq "pk_min" "$pre_pk_min" "$post_pk_min"
    chk_eq "pk_max" "$pre_pk_max" "$post_pk_max"

    # 4: exact INSERT/UPDATE/DELETE effects at the DROP boundary
    if [[ "$churn_json" != "null" ]]; then
        local m_ins m_upd_hi m_del_lo m_del_hi exp_ins exp_upd
        m_ins="$(jq -r '.markers.insert_id_base' <<<"$churn_json")"
        m_upd_hi="$(jq -r '.markers.update_id_max' <<<"$churn_json")"
        m_del_lo="$(jq -r '.markers.delete_id_range[0]' <<<"$churn_json")"
        m_del_hi="$(jq -r '.markers.delete_id_range[1]' <<<"$churn_json")"
        exp_ins="$(jq -r '.observed.inserted_rows_above_base' <<<"$churn_json")"
        exp_upd="$(jq -r '.observed.updated_rows_marked' <<<"$churn_json")"
        chk_eq "churn_inserts_present" "$exp_ins" "$(s10_q "SELECT count(*) FROM $rel WHERE id > $m_ins;")"
        local upd_col=notes
        case "$shape" in toast) upd_col=compressible ;; rich) upd_col=payload ;; hybrid) upd_col=doc ;; esac
        chk_eq "churn_updates_visible" "$exp_upd" \
            "$(s10_q "SELECT count(*) FROM $rel WHERE $upd_col LIKE 'churn-upd-%' AND id <= $m_upd_hi;")"
        chk_eq "churn_deletes_absent" "0" \
            "$(s10_q "SELECT count(*) FROM $rel WHERE id BETWEEN $m_del_lo AND $m_del_hi;")"
    fi

    # writer commits exactly once; aborted work absent
    if (( writer == 1 )); then
        local w_rows w_dupes poison_col poison expected_ids_sha actual_ids_sha expected_writer_count
        expected_writer_count="$(wc -l < "$WRITER_DIR/$tier.commits" 2>/dev/null || echo 0)"
        expected_ids_sha="$(awk '{print $1}' "$WRITER_DIR/$tier.commits" | LC_ALL=C sort -n | sha256sum | awk '{print $1}')"
        actual_ids_sha="$("$PSQL" -h "$SOCKET" -p "$PORT" -d "$DB_NAME" -X -qAtc \
            "COPY (SELECT id::text FROM $rel WHERE id >= 900000000 ORDER BY id) TO STDOUT" \
            | sha256sum | awk '{print $1}')"
        w_rows="$(s10_q "SELECT count(*) FROM $rel WHERE id >= 900000000;")"
        w_dupes="$(s10_q "SELECT COALESCE(count(*),0) FROM (SELECT id FROM $rel WHERE id >= 900000000 GROUP BY id HAVING count(*)>1) d;")"
        chk_eq "writer_rows_no_duplicates" "0" "$w_dupes"
        chk_eq "writer_commit_count_exact" "$expected_writer_count" "$w_rows"
        chk_eq "writer_committed_id_set_exact" "$expected_ids_sha" "$actual_ids_sha"
        poison_col=label
        case "$shape" in toast) poison_col=incompressible ;; hybrid) poison_col=code ;; esac
        poison="$(s10_q "SELECT count(*) FROM $rel WHERE $poison_col LIKE 'rollback-poison%';" 2>/dev/null || echo 0)"
        chk_eq "rolled_back_writer_txns_absent" "0" "$poison"
    fi

    # 5: schema fingerprint
    local post_schema_sha
    post_schema_sha="$(schema_fingerprint_file "$rel" "$LOG_DIR/$tier-schema-after.sql" || echo dump_failed)"
    chk_eq "schema_dump_sha256" "$pre_schema_sha" "$post_schema_sha"
    if [[ "$pre_schema_sha" != "$post_schema_sha" ]]; then
        diff -u "$LOG_DIR/$tier-schema-before.sql" "$LOG_DIR/$tier-schema-after.sql" \
            > "$LOG_DIR/$tier-schema.diff" 2>&1 || true
    fi

    # 6-9: constraints/indexes/FK, owner+ACL, comments, RLS
    local post_meta; post_meta="$(s10_metadata_fingerprint "$rel")"
    local f
    for f in owner relacl rls_enabled rls_forced policies table_comment column_comments constraints indexes columns; do
        chk_eq "metadata_$f" "$(jq -cS ".$f" <<<"$pre_meta")" "$(jq -cS ".$f" <<<"$post_meta")"
    done

    # 10: sequence/identity next value must be past the recovered data edge
    local seqname nextv maxid
    seqname="$(s10_q "SELECT COALESCE(pg_get_serial_sequence('$rel','id'),'none');")"
    if [[ "$seqname" != "none" && -n "$seqname" ]]; then
        nextv="$(s10_q "SELECT last_value FROM $seqname;")"
        maxid="$(s10_q "SELECT COALESCE(max(id),0) FROM $rel;")"
        chk "identity_sequence_past_data_edge" "$(( nextv >= maxid ? 1 : 0 ))" \
            "sequence_last_value=$nextv max_id=$maxid"
    else
        chk "identity_sequence_past_data_edge" 1 "no owned sequence for this shape"
    fi

    # 11-12: artifact binding survived and no partial artifact is available
    local avail_bad
    avail_bad="$(s10_q "SELECT count(*) FROM flashback.snapshots
        WHERE payload_state='available'
          AND storage_backend='external_zstd'
          AND (external_checksum_sha256 IS NULL OR locator IS NULL OR snapshot_lsn IS NULL);")"
    chk_eq "no_partial_artifact_available_post_recovery" "0" "$avail_bad"

    # 13: identity of the successor lifecycle/generation
    local post_tracking
    post_tracking="$(s10_q "SELECT tracking_id FROM flashback.tracked_tables
        WHERE is_active AND format('%I.%I',schema_name,table_name)='$rel'
        ORDER BY tracking_id DESC LIMIT 1;")"
    chk_eq "successor_tracking_identity_preserved" "$tracking_id" "$post_tracking"

    # 14-15: truthful post-recovery health, no hidden open gap
    wait_lifecycle_healthy_soft "$rel" 1800 || true
    local health gaps
    health="$(s10_q "SELECT health FROM flashback_health() WHERE table_name='$rel' ORDER BY generation_id DESC LIMIT 1;")"
    gaps="$(s10_q "SELECT COALESCE(open_gap_count,0) FROM flashback_health() WHERE table_name='$rel' ORDER BY generation_id DESC LIMIT 1;")"
    chk_eq "post_recovery_health_healthy" "healthy" "$health"
    chk_eq "post_recovery_no_open_gap" "0" "$gaps"
    local doctor_errors
    doctor_errors="$(s10_q "SELECT count(*) FROM flashback_doctor() WHERE status='error';")"
    chk_eq "doctor_reports_no_error" "0" "$doctor_errors"
}

# ---------------------------------------------------------------------
# Negative capacity/admission scenario. Deliberately starves the budget,
# proves protect refuses before any destructive mutation or publication,
# then restores the real settings.
# ---------------------------------------------------------------------
run_negative_capacity() {
    local tier=$1 rel=$2 sizes=$3 tier_json=$4 free_before=$5
    log "  negative capacity scenario on $rel"
    TIER_CHECKS_JSON='[]'; TIER_FAILED=0

    local pre_fp pre_rows
    pre_fp="$(s10_data_fingerprint "$rel")"
    pre_rows="$(jq -r '.row_count' <<<"$pre_fp")"

    # Starve the snapshot budget far below the real table size.
    s10_q "ALTER SYSTEM SET pg_flashback.local_max_snapshot_bytes = '8MB';" >/dev/null
    s10_q "ALTER SYSTEM SET pg_flashback.local_max_restore_peak_bytes = '16MB';" >/dev/null
    s10_q "SELECT pg_reload_conf();" >/dev/null
    wait_setting_value pg_flashback.local_max_snapshot_bytes 8MB 30
    wait_setting_value pg_flashback.local_max_restore_peak_bytes 16MB 30

    local rc=0
    "$CLI" protect "$rel" > "$LOG_DIR/$tier-negative-protect.log" 2>&1 || rc=$?
    chk "negative_protect_refused" "$(( rc != 0 ? 1 : 0 ))" "cli_exit=$rc"
    chk "negative_output_actionable" \
        "$(grep -qiE 'budget|capacity|admission|local_max_snapshot|insufficient|exceeds' "$LOG_DIR/$tier-negative-protect.log" && echo 1 || echo 0)" \
        "$(head -c 300 "$LOG_DIR/$tier-negative-protect.log" | tr '\n' ' ')"

    # Nothing destructive may have happened and nothing may look recoverable.
    local still_rows avail_art active_life
    still_rows="$(s10_q "SELECT count(*) FROM $rel;")"
    chk_eq "negative_source_table_intact" "$pre_rows" "$still_rows"
    chk_eq "negative_data_digest_intact" "$(jq -r '.digest' <<<"$pre_fp")" \
        "$(jq -r '.digest' <<<"$(s10_data_fingerprint "$rel")")"
    avail_art="$(s10_q "SELECT count(*) FROM flashback.snapshots s
        JOIN flashback.tracked_tables t ON t.tracking_id=s.tracking_id
        WHERE format('%I.%I',t.schema_name,t.table_name)='$rel'
          AND s.payload_state='available';")"
    chk_eq "negative_no_available_artifact" "0" "$avail_art"
    active_life="$(s10_q "SELECT count(*) FROM flashback_health() WHERE table_name='$rel' AND health='healthy';")"
    chk_eq "negative_no_falsely_recoverable_generation" "0" "$active_life"
    local doctor_json
    doctor_json="$(s10_q "SELECT jsonb_agg(jsonb_build_object('check',check_name,'status',status,'action',action))
                          FROM flashback_doctor() WHERE status <> 'ok';" 2>/dev/null || echo 'null')"

    # Restore the real settings for the remaining tiers.
    s10_q "ALTER SYSTEM SET pg_flashback.local_max_snapshot_bytes = '80GB';" >/dev/null
    s10_q "ALTER SYSTEM SET pg_flashback.local_max_restore_peak_bytes = '160GB';" >/dev/null
    s10_q "SELECT pg_reload_conf();" >/dev/null
    wait_setting_value pg_flashback.local_max_snapshot_bytes 80GB 30
    wait_setting_value pg_flashback.local_max_restore_peak_bytes 160GB 30
    local restored
    restored="$(s10_q "SELECT current_setting('pg_flashback.local_max_snapshot_bytes');")"
    chk_eq "negative_capacity_settings_restored" "80GB" "$restored"

    local cleanup_rc=0
    cleanup_tier_lifecycle "$rel" "" || cleanup_rc=$?
    chk_eq "negative_cleanup_exit_zero" "0" "$cleanup_rc"
    local free_after; free_after="$(s10_free_bytes /home)"
    local status=PASS; (( TIER_FAILED == 0 )) || status=FAIL
    jq -n --arg tier "$tier" --arg status "$status" --argjson sizes "$sizes" \
        --argjson checks "$TIER_CHECKS_JSON" --argjson failed "$TIER_FAILED" \
        --argjson doctor "${doctor_json:-null}" \
        --argjson free_before "$free_before" --argjson free_after "$free_after" \
        '{tier:$tier, shape:"mixed", scenario:"negative_capacity_admission", status:$status,
          failed_checks:$failed, measured:$sizes, doctor_non_ok:$doctor,
          disk:{free_home_before:$free_before, free_home_after:$free_after},
          checks:$checks}' > "$tier_json"
    log "=== tier $tier: $status (failed_checks=$TIER_FAILED) ==="
    [[ "$status" == "PASS" ]]
}

# ---------------------------------------------------------------------
# Tier teardown. Only ever touches this run's own objects.
# ---------------------------------------------------------------------
cleanup_tier_lifecycle() {
    local rel=$1 tid=$2
    local cleanup_rc=0
    set +e
    if [[ -z "$tid" ]]; then
        tid="$(s10_q "SELECT tracking_id FROM flashback.tracked_tables
                      WHERE is_active AND format('%I.%I',schema_name,table_name)='$rel'
                      ORDER BY tracking_id DESC LIMIT 1;" 2>/dev/null)"
    fi
    if [[ -n "$tid" && "$tid" != "null" ]]; then
        "$CLI" unprotect "$rel" --yes >/dev/null 2>&1 || cleanup_rc=1
        local _
        for _ in $(seq 1 240); do
            local st
            st="$(s10_q "SELECT COALESCE((SELECT protection_state FROM flashback.tracked_tables
                          WHERE tracking_id=$tid),'gone');" 2>/dev/null)"
            [[ "$st" == "unprotected" || "$st" == "gone" || "$st" == "cleaned" ]] && break
            s10_q "SELECT flashback_finalize_unprotect_operations();" >/dev/null 2>&1
            sleep 0.5
        done
        "$CLI" cleanup --tracking-id "$tid" --yes >/dev/null 2>&1 || cleanup_rc=1
    fi
    s10_q "DROP TABLE IF EXISTS $rel CASCADE;" >/dev/null 2>&1
    s10_q "DROP TABLE IF EXISTS public.s10_parent_${rel##*.} CASCADE;" >/dev/null 2>&1
    s10_q "VACUUM;" >/dev/null 2>&1
    s10_q "CHECKPOINT;" >/dev/null 2>&1
    set -e
    return "$cleanup_rc"
}

wait_setting_value() {
    local name=$1 expected=$2 timeout_s=${3:-30}
    local deadline=$(( $(date +%s) + timeout_s )) observed=""
    while (( $(date +%s) <= deadline )); do
        observed="$(s10_q "SELECT current_setting('$name');" 2>/dev/null || true)"
        [[ "$observed" == "$expected" ]] && return 0
        sleep 0.1
    done
    die "configuration reload did not expose $name=$expected (last=$observed)"
}

# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
s10_q "DO \$\$ BEGIN
   IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='s10_reader') THEN
      CREATE ROLE s10_reader NOLOGIN;
   END IF;
END \$\$;" >/dev/null

# shellcheck disable=SC2086
qst_init $TIERS

# Resume only compact PASS evidence bound by this run's verified manifest.
# Failed/missing tiers are rerun from a fresh cluster; completed tiers are
# never silently inferred from logs or a stale summary.
if (( RESUMING == 1 )); then
    for tier in $TIERS; do
        tier_file="$EVIDENCE_DIR/tiers/$tier.json"
        if [[ -f "$tier_file" ]] && [[ "$(jq -r '.status // ""' "$tier_file")" == "PASS" ]]; then
            qst_mark_step "$tier" pass "resumed: existing tier evidence verified under run manifest"
        fi
    done
fi

OVERALL_RC=0
for tier in $TIERS; do
    if [[ "${QST_STEP_STATUS[$tier]:-pending}" == "pass" ]]; then
        log "=== tier $tier: already PASS in this identity-bound run; skipping ==="
        continue
    fi
    qst_mark_step "$tier" running ""
    QST_CURRENT_STEP="$tier"
    if run_tier "$tier"; then
        qst_mark_step "$tier" pass "see evidence/tiers/$tier.json"
    else
        rc=$?
        if [[ -f "$EVIDENCE_DIR/tiers/$tier.json" ]] \
           && [[ "$(jq -r '.status' "$EVIDENCE_DIR/tiers/$tier.json")" == "BLOCKED_CAPACITY" ]]; then
            qst_mark_step "$tier" fail "BLOCKED_CAPACITY"
        else
            qst_mark_step "$tier" fail "tier failed rc=$rc"
        fi
        QST_FAILED=$(( QST_FAILED + 1 ))
        OVERALL_RC=1
    fi
    # shellcheck disable=SC2034  # read by qst_on_signal in the trap handler
    QST_CURRENT_STEP=""
    # Durably record progress after every tier, before the next allocation.
    qst_write_summary_atomic "$SUMMARY_JSON" \
        "$(qst_compute_summary_json "$RUN_ID" "step10_scale" 0 "$(build_extra_json)")"
done

log "all requested tiers finished (overall_rc=$OVERALL_RC)"
exit "$OVERALL_RC"
