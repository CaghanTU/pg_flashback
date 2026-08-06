#!/usr/bin/env bash
# Real production-build external_zstd persist/finalize/activate proof.
set -euo pipefail

BINDIR="${1:-/usr/local/pgsql-17/bin}"
for bin in initdb pg_ctl psql; do
    [[ -x "$BINDIR/$bin" ]] || { echo "FAIL: missing $BINDIR/$bin"; exit 1; }
done

RUN_ID="${PGFB_RUN_ID:-$$}"
BASE="${PGFB_EXTZSTD_E2E_ROOT:-/tmp/pgfb-extzstd-artifact-$RUN_ID}"
DATA="$BASE/data"
SOCKET="$BASE/socket"
ARTIFACT_ROOT="$BASE/artifacts"
LOG="$BASE/postgres.log"
PORT="${PGFB_PORT:-$((32000 + RUN_ID % 1000))}"
LOCK_PID=""
CONSUMER_PID=""

cleanup() {
    local rc=$?
    if [[ -n "$LOCK_PID" ]]; then
        kill "$LOCK_PID" >/dev/null 2>&1 || true
        wait "$LOCK_PID" >/dev/null 2>&1 || true
    fi
    if [[ -n "$CONSUMER_PID" ]]; then
        kill "$CONSUMER_PID" >/dev/null 2>&1 || true
        wait "$CONSUMER_PID" >/dev/null 2>&1 || true
    fi
    "$BINDIR/pg_ctl" -D "$DATA" -m immediate stop >/dev/null 2>&1 || true
    if [[ "${PGFB_E2E_KEEP:-0}" != 1 ]]; then rm -rf "$BASE"; fi
    exit "$rc"
}
trap cleanup EXIT

rm -rf "$BASE"
mkdir -p "$SOCKET" "$ARTIFACT_ROOT"
chmod 700 "$ARTIFACT_ROOT"
"$BINDIR/initdb" -D "$DATA" --no-locale --encoding=UTF8 >/dev/null
cat >>"$DATA/postgresql.conf" <<EOF
shared_preload_libraries = 'pg_flashback'
wal_level = logical
max_replication_slots = 8
max_wal_senders = 8
max_worker_processes = 16
port = $PORT
unix_socket_directories = '$SOCKET'
# Keep the periodic capture/maintenance pair out of this deterministic
# protocol proof. WAL consumption is invoked explicitly below; the dynamic
# external copier itself is still a real background worker.
pg_flashback.target_database = 'pgfb_unused'
pg_flashback.capture_mode = 'wal'
pg_flashback.external_snapshot_root = '$ARTIFACT_ROOT'
pg_flashback.snapshot_storage_backend = 'external_zstd'
pg_flashback.external_snapshot_min_free_bytes = '1MB'
pg_flashback.external_snapshot_safety_reserve_bytes = '1MB'
pg_flashback.local_max_snapshot_bytes = '10GB'
pg_flashback.local_max_restore_peak_bytes = '10GB'
pg_flashback.local_min_filesystem_bytes = '1MB'
pg_flashback.local_safety_reserve_bytes = '1MB'
pg_flashback.external_snapshot_batch_rows = 128
pg_flashback.external_snapshot_zstd_level = 3
EOF
"$BINDIR/pg_ctl" -D "$DATA" -l "$LOG" start >/dev/null

export PGHOST="$SOCKET" PGPORT="$PORT" PGDATABASE=postgres
q() { "$BINDIR/psql" -X -v ON_ERROR_STOP=1 -Atqc "$1"; }

q "CREATE EXTENSION pg_flashback"
SLOT=pg_flashback_postgres
q "SELECT slot_name FROM pg_create_logical_replication_slot('$SLOT','pg_flashback')" >/dev/null

IDS=$(q "DO \$setup\$
DECLARE
    v_db oid := (SELECT oid FROM pg_database WHERE datname=current_database());
    v_stream bigint; v_tracking bigint; v_parent_snapshot bigint;
    v_parent bigint; v_rel oid;
    v_confirmed pg_lsn; v_restart pg_lsn;
BEGIN
    CREATE TABLE public.ext_artifact_e2e(id bigint PRIMARY KEY, note text NOT NULL);
    INSERT INTO public.ext_artifact_e2e
    SELECT g, repeat(md5(g::text), 8) FROM generate_series(1, 5000) g;
    v_rel := 'public.ext_artifact_e2e'::regclass;
    SELECT confirmed_flush_lsn, restart_lsn INTO v_confirmed, v_restart
    FROM pg_replication_slots WHERE slot_name = '$SLOT';
    v_stream := public.flashback_internal_create_capture_stream(
        p_database_oid => v_db,
        p_initial_state => 'active',
        p_slot_name => '$SLOT',
        p_plugin_name => 'pg_flashback',
        p_confirmed_flush_lsn => v_confirmed,
        p_restart_lsn => v_restart
    );
    INSERT INTO flashback.tracked_tables
        (rel_oid,schema_name,table_name,base_snapshot_table,recovery_profile)
    VALUES (v_rel,'public','ext_artifact_e2e',NULL,'local_delta')
    RETURNING tracking_id INTO v_tracking;
    v_parent_snapshot := public.flashback_internal_snapshot_create(
        v_tracking,v_rel,'public','ext_artifact_e2e','0/1000','initial_track'
    );
    v_parent := public.flashback_internal_create_coverage_generation(
        p_tracking_id => v_tracking,
        p_generation_no => 1,
        p_stream_id => v_stream,
        p_boundary_kind => 'initial_track',
        p_rel_oid_at_boundary => v_rel,
        p_boundary_snapshot_id => v_parent_snapshot,
        p_boundary_xid => txid_current(),
        p_boundary_marker => 'ext-artifact-parent'
    );
    PERFORM public.flashback_internal_transition_coverage_generation(
        v_parent,v_tracking,'building','active','fixture',
        '0/1000',clock_timestamp(),'0/1000',clock_timestamp(),NULL,NULL,'{}'
    );
END \$setup\$;
$(if [[ "${PGFB_EXTZSTD_MAINTAIN:-0}" == 1 || "${PGFB_EXTZSTD_CLI:-0}" == 1 || "${PGFB_EXTZSTD_RECONCILE:-0}" == 1 ]]; then cat <<'SQL'
SELECT '0|0|0';
SQL
else cat <<SQL
WITH tt AS (
  SELECT tracking_id,rel_oid FROM flashback.tracked_tables
  WHERE table_name='ext_artifact_e2e' AND is_active
), ids AS (
  SELECT tt.*,
    (SELECT stream_id FROM flashback.capture_streams WHERE state='active') stream_id,
    (SELECT generation_id FROM flashback.coverage_generations cg
     WHERE cg.tracking_id=tt.tracking_id AND state='active') parent_id
  FROM tt
)
SELECT generation_id||'|'||snapshot_id||'|'||tracking_id
FROM ids, LATERAL public.flashback_internal_reserve_online_generation(
  ids.tracking_id,ids.rel_oid,ids.stream_id,2,ids.parent_id,
  'external_zstd',990001
)
SQL
fi)")
IFS='|' read -r GENERATION SNAPSHOT TRACKING <<<"$IDS"

if [[ "${PGFB_EXTZSTD_CLI:-0}" == 1 ]]; then
    CLI_RESULT=$("$(dirname "$0")/pg_flashback" --json maintain public.ext_artifact_e2e --yes)
    [[ "$(jq -r '.data.finalize.status' <<<"$CLI_RESULT")" == sealed ]] || {
        echo "FAIL: operator CLI did not finish external maintain: $CLI_RESULT"; exit 1;
    }
    [[ "$(q "SELECT count(*) FROM flashback.snapshots WHERE storage_backend='external_zstd' AND payload_state='available'")" == 1 ]] || {
        echo "FAIL: operator CLI did not leave one available external artifact"; exit 1;
    }
    q "UPDATE public.ext_artifact_e2e SET note='post-external-boundary' WHERE id=1" >/dev/null
    EXPECTED_FP=$(q "SELECT md5(string_agg(id::text||':'||note,',' ORDER BY id)) FROM public.ext_artifact_e2e")
    # The isolated harness has no continuously scheduled capture worker. Prove
    # that the committed post-boundary UPDATE is durable before asking the DDL
    # hook to admit DROP; otherwise the expected fail-closed write-stall guard
    # would reject the DROP for an undrained commit.
    for _ in $(seq 1 300); do
        q "SELECT public.flashback_consume_wal(50000)" >/dev/null
        [[ "$(q "SELECT count(*) FROM flashback.delta_log WHERE tracking_id=1 AND event_type='UPDATE'")" -ge 1 ]] && break
        sleep 0.05
    done
    [[ "$(q "SELECT count(*) FROM flashback.delta_log WHERE tracking_id=1 AND event_type='UPDATE'")" -ge 1 ]] || {
        echo "FAIL: post-boundary UPDATE did not become durable before DROP"; exit 1;
    }
    q "DROP TABLE public.ext_artifact_e2e" >/dev/null
    for _ in $(seq 1 300); do
        q "SELECT public.flashback_consume_wal(50000)" >/dev/null
        [[ "$(q "SELECT count(*) FROM flashback.delta_log WHERE tracking_id=1 AND event_type='DROP'")" == 1 ]] && break
        sleep 0.05
    done
    # Recovery commits a post-restore boundary and then waits for the capture
    # worker to resolve/verify it. This deterministic harness disables the
    # periodic worker, so run the same public consume primitive concurrently
    # while the real operator CLI performs and observes recovery.
    (
        while true; do
            "$BINDIR/psql" -X -v ON_ERROR_STOP=1 -Atqc \
                "SELECT public.flashback_consume_wal(50000); SELECT public.flashback_finalize_recover_operations()" \
                >/dev/null 2>&1 || true
            sleep 0.05
        done
    ) &
    CONSUMER_PID=$!
    RECOVER_RESULT=$("$(dirname "$0")/pg_flashback" --json recover public.ext_artifact_e2e --latest-drop --yes)
    kill "$CONSUMER_PID" >/dev/null 2>&1 || true
    wait "$CONSUMER_PID" >/dev/null 2>&1 || true
    CONSUMER_PID=""
    [[ "$(jq -r '.status' <<<"$RECOVER_RESULT")" == ok ]] || {
        echo "FAIL: operator CLI external DROP recovery failed: $RECOVER_RESULT"; exit 1;
    }
    RESTORED_FP=$(q "SELECT md5(string_agg(id::text||':'||note,',' ORDER BY id)) FROM public.ext_artifact_e2e")
    [[ "$RESTORED_FP" == "$EXPECTED_FP" ]] || {
        echo "FAIL: external DROP recovery fingerprint $RESTORED_FP != $EXPECTED_FP"; exit 1;
    }
    [[ "$(q "SELECT count(*) FROM pg_constraint WHERE conrelid='public.ext_artifact_e2e'::regclass AND contype='p'")" == 1 ]] || {
        echo "FAIL: external DROP recovery did not restore primary key"; exit 1;
    }
    echo "EXTERNAL_ZSTD_ARTIFACT_E2E=PASS mode=cli-maintain-drop-recover"
    exit 0
fi

if [[ "${PGFB_EXTZSTD_RECONCILE:-0}" == 1 ]]; then
    command -v python3 >/dev/null || { echo "FAIL: python3 is required"; exit 1; }
    SYSTEM_ID=$(q "SELECT system_identifier FROM pg_control_system()")
    DB_OID=$(q "SELECT oid FROM pg_database WHERE datname=current_database()")

    # A reservation whose copier never started becomes durable failed/aborted
    # first; physical cleanup and its receipt happen only on a later call.
    BEGIN1=$(q "SELECT public.flashback_maintain_begin('public.ext_artifact_e2e')")
    OP1=$(jq -r .operation_id <<<"$BEGIN1")
    GEN1=$(jq -r .new_generation_id <<<"$BEGIN1")
    SNAP1=$(jq -r .snapshot_id <<<"$BEGIN1")
    R1=$(q "SELECT public.flashback_internal_reconcile_external_maintenance(interval '0 seconds',10)")
    [[ "$(q "SELECT state FROM flashback.operation_current_state WHERE operation_id=$OP1")" == failed ]] || {
        echo "FAIL: absent copier operation was not failed: $R1"; exit 1;
    }
    [[ "$(q "SELECT state FROM flashback.coverage_generations WHERE generation_id=$GEN1")" == aborted ]] || {
        echo "FAIL: absent copier generation was not aborted"; exit 1;
    }
    [[ "$(q "SELECT payload_state FROM flashback.snapshots WHERE snapshot_id=$SNAP1")" == aborted ]] || {
        echo "FAIL: absent copier snapshot was not aborted"; exit 1;
    }
    [[ "$(q "SELECT count(*) FROM flashback.external_artifact_cleanup_receipts WHERE snapshot_id=$SNAP1")" == 0 ]] || {
        echo "FAIL: cleanup receipt committed in the abort transaction"; exit 1;
    }
    q "SELECT public.flashback_internal_reconcile_external_maintenance(interval '0 seconds',10)" >/dev/null
    [[ "$(q "SELECT count(*) FROM flashback.external_artifact_cleanup_receipts WHERE snapshot_id=$SNAP1")" == 1 ]] || {
        echo "FAIL: later cleanup did not record exact receipt"; exit 1;
    }

    # A held exact lease must defer rather than deleting or aborting a live
    # copier. Once the lease disappears, the incomplete orphan is aborted and
    # is cleaned on the following transaction.
    BEGIN2=$(q "SELECT public.flashback_maintain_begin('public.ext_artifact_e2e')")
    OP2=$(jq -r .operation_id <<<"$BEGIN2")
    NONCE2=$(jq -r .operation_nonce <<<"$BEGIN2")
    STAGE2="$ARTIFACT_ROOT/$SYSTEM_ID/$DB_OID/.staging/$NONCE2"
    mkdir -p "$STAGE2"
    chmod 700 "$STAGE2"
    : >"$STAGE2/lease"
    chmod 600 "$STAGE2/lease"
    python3 - "$STAGE2/lease" <<'PY' &
import fcntl
import sys
import time

with open(sys.argv[1], "r+") as lease:
    fcntl.flock(lease.fileno(), fcntl.LOCK_EX)
    time.sleep(60)
PY
    LOCK_PID=$!
    sleep 0.2
    R2=$(q "SELECT public.flashback_internal_reconcile_external_maintenance(interval '0 seconds',10)")
    [[ "$(q "SELECT state FROM flashback.operation_current_state WHERE operation_id=$OP2")" == started ]] || {
        echo "FAIL: active copier lease was not deferred: $R2"; exit 1;
    }
    kill "$LOCK_PID" >/dev/null 2>&1 || true
    wait "$LOCK_PID" >/dev/null 2>&1 || true
    LOCK_PID=""
    R2B=$(q "SELECT public.flashback_internal_reconcile_external_maintenance(interval '0 seconds',10)")
    [[ "$(q "SELECT state FROM flashback.operation_current_state WHERE operation_id=$OP2")" == failed ]] || {
        echo "FAIL: incomplete orphan was not failed after lease release: $R2B"; exit 1;
    }
    q "SELECT public.flashback_internal_reconcile_external_maintenance(interval '0 seconds',10)" >/dev/null
    [[ ! -e "$STAGE2" ]] || { echo "FAIL: incomplete orphan directory survived cleanup"; exit 1; }

    # A fully copied artifact with a consumed boundary is resumed by the
    # reconciler through publication, activation, and journal finalization.
    BEGIN3=$(q "SELECT public.flashback_maintain_begin('public.ext_artifact_e2e')")
    OP3=$(jq -r .operation_id <<<"$BEGIN3")
    TRACK3=$(jq -r .tracking_id <<<"$BEGIN3")
    GEN3=$(jq -r .new_generation_id <<<"$BEGIN3")
    SNAP3=$(jq -r .snapshot_id <<<"$BEGIN3")
    q "SELECT public.flashback_maintain_external_copy($OP3)" >/dev/null
    for _ in $(seq 1 600); do
        ARTIFACT3=$(q "SELECT public.flashback_internal_external_artifact_state(
          $TRACK3,$GEN3,$SNAP3)")
        [[ "$(jq -r .status <<<"$ARTIFACT3")" =~ ^(staging_committed|published)$ ]] && break
        sleep 0.05
    done
    ARTIFACT3=${ARTIFACT3:-'{}'}
    [[ "$(jq -r .status <<<"$ARTIFACT3")" =~ ^(staging_committed|published)$ ]] || {
        echo "FAIL: resume fixture copier did not commit its artifact: ${ARTIFACT3:-missing}"; exit 1;
    }
    for _ in $(seq 1 300); do
        q "SELECT public.flashback_consume_wal(50000)" >/dev/null
        [[ "$(q "SELECT snapshot_lsn IS NOT NULL FROM flashback.snapshots WHERE snapshot_id=$SNAP3")" == t ]] && break
        sleep 0.05
    done
    [[ "$(q "SELECT snapshot_lsn IS NOT NULL FROM flashback.snapshots WHERE snapshot_id=$SNAP3")" == t ]] || {
        echo "FAIL: resume fixture boundary did not resolve"; exit 1;
    }
    R3=$(q "SELECT public.flashback_internal_reconcile_external_maintenance(interval '0 seconds',10)")
    [[ "$(q "SELECT state FROM flashback.operation_current_state WHERE operation_id=$OP3")" == sealed ]] || {
        echo "FAIL: committed staged artifact did not resume to sealed: $R3"; exit 1;
    }
    [[ "$(q "SELECT payload_state FROM flashback.snapshots WHERE snapshot_id=$SNAP3")" == available ]] || {
        echo "FAIL: resumed artifact did not become available"; exit 1;
    }
    [[ "$(q "SELECT state FROM flashback.coverage_generations WHERE generation_id=$GEN3")" == active ]] || {
        echo "FAIL: resumed generation did not activate"; exit 1;
    }
    echo "EXTERNAL_ZSTD_ARTIFACT_E2E=PASS mode=reconcile"
    exit 0
fi

if [[ "${PGFB_EXTZSTD_MAINTAIN:-0}" == 1 ]]; then
    PLAN=$(q "SELECT public.flashback_maintain_plan('public.ext_artifact_e2e')")
    [[ "$(jq -r .snapshot_storage_backend <<<"$PLAN")" == external_zstd ]] || {
        echo "FAIL: maintain plan did not select external_zstd: $PLAN"; exit 1;
    }
    BEGIN_RESULT=$(q "SELECT public.flashback_maintain_begin('public.ext_artifact_e2e')")
    [[ "$(jq -r .status <<<"$BEGIN_RESULT")" == reserved ]] || {
        echo "FAIL: external maintain begin did not reserve: $BEGIN_RESULT"; exit 1;
    }
    OPERATION=$(jq -r .operation_id <<<"$BEGIN_RESULT")
    GENERATION=$(jq -r .new_generation_id <<<"$BEGIN_RESULT")
    SNAPSHOT=$(jq -r .snapshot_id <<<"$BEGIN_RESULT")
    TRACKING=$(jq -r .tracking_id <<<"$BEGIN_RESULT")
    COPY_RESULT=$(q "SELECT public.flashback_maintain_external_copy($OPERATION)")
    [[ "$(jq -r .status <<<"$COPY_RESULT")" == copy_in_progress ]] || {
        echo "FAIL: external maintain copy did not start: $COPY_RESULT"; exit 1;
    }
    for _ in $(seq 1 200); do
        q "SELECT public.flashback_consume_wal(50000)" >/dev/null
        PUBLISH_RESULT=$(q "SELECT public.flashback_maintain_external_publish($OPERATION)")
        [[ "$(jq -r .status <<<"$PUBLISH_RESULT")" == sealed ]] && break
        sleep 0.05
    done
    [[ "$(jq -r .status <<<"$PUBLISH_RESULT")" == sealed ]] || {
        echo "FAIL: external maintain publish did not seal: ${PUBLISH_RESULT:-missing}"; exit 1;
    }
    [[ "$(q "SELECT payload_state||'|'||storage_backend FROM flashback.snapshots WHERE snapshot_id=$SNAPSHOT")" == "available|external_zstd" ]] || {
        echo "FAIL: public maintain did not publish external snapshot"; exit 1;
    }
    echo "EXTERNAL_ZSTD_ARTIFACT_E2E=PASS mode=operator-maintain"
    exit 0
fi

[[ -n "$GENERATION" && -n "$SNAPSHOT" && -n "$TRACKING" ]] || {
    echo "FAIL: reservation identities missing"; exit 1;
}

q "SELECT public.flashback_internal_run_external_marker_transaction(
      $TRACKING, 'public.ext_artifact_e2e'::regclass::oid::bigint,
      $GENERATION, $SNAPSHOT)" >/dev/null

SYSTEM_ID=$(q "SELECT system_identifier FROM pg_control_system()")
DB_OID=$(q "SELECT oid FROM pg_database WHERE datname=current_database()")
PROVISIONAL="$ARTIFACT_ROOT/$SYSTEM_ID/$DB_OID/.staging/990001/provisional.json"
for _ in $(seq 1 300); do [[ -s "$PROVISIONAL" ]] && break; sleep 0.1; done
[[ -s "$PROVISIONAL" ]] || { echo "FAIL: committed provisional artifact missing"; exit 1; }

for _ in $(seq 1 200); do
    q "SELECT public.flashback_consume_wal(50000)" >/dev/null
    [[ "$(q "SELECT snapshot_lsn IS NOT NULL FROM flashback.snapshots WHERE snapshot_id=$SNAPSHOT")" == t ]] && break
    sleep 0.05
done
[[ "$(q "SELECT state||'|'||(boundary_lsn IS NULL)::text FROM flashback.coverage_generations WHERE generation_id=$GENERATION")" == "building|true" ]] || {
    echo "FAIL: marker consumption activated external generation"; exit 1;
}
[[ "$(q "SELECT payload_state FROM flashback.snapshots WHERE snapshot_id=$SNAPSHOT")" == creating ]] || {
    echo "FAIL: marker consumption published external snapshot"; exit 1;
}

RESULT=$(q "SELECT public.flashback_internal_finalize_external_snapshot($TRACKING,$GENERATION,$SNAPSHOT)")
[[ "$(jq -r .status <<<"$RESULT")" == available ]] || { echo "FAIL: finalizer result=$RESULT"; exit 1; }
[[ "$(q "SELECT payload_state FROM flashback.snapshots WHERE snapshot_id=$SNAPSHOT")" == available ]] || {
    echo "FAIL: snapshot not available"; exit 1;
}
[[ "$(q "SELECT state FROM flashback.coverage_generations WHERE generation_id=$GENERATION")" == active ]] || {
    echo "FAIL: generation not active"; exit 1;
}
[[ "$(q "SELECT state FROM flashback.coverage_generations WHERE tracking_id=$TRACKING AND generation_no=1")" == sealed ]] || {
    echo "FAIL: predecessor not sealed"; exit 1;
}

FINAL_DIR="$ARTIFACT_ROOT/$SYSTEM_ID/$DB_OID/$TRACKING/$SNAPSHOT-990001"
[[ -s "$FINAL_DIR/artifact.zst" && -s "$FINAL_DIR/manifest.json" ]] || {
    echo "FAIL: immutable published files missing"; exit 1;
}
[[ ! -e "$FINAL_DIR/provisional.json" && ! -e "$FINAL_DIR/lease" ]] || {
    echo "FAIL: coordination files survived accepted publication"; exit 1;
}
ROW_COUNT=$(jq -r .row_count "$FINAL_DIR/manifest.json")
[[ "$ROW_COUNT" == 5000 ]] || { echo "FAIL: manifest row_count=$ROW_COUNT"; exit 1; }

# Retry after cleanup must reconstruct evidence from the immutable manifest.
RETRY=$(q "SELECT public.flashback_internal_finalize_external_snapshot($TRACKING,$GENERATION,$SNAPSHOT)")
[[ "$(jq -r .status <<<"$RETRY")" == available ]] || { echo "FAIL: retry=$RETRY"; exit 1; }

q "CREATE TABLE public.ext_artifact_restored
     (LIKE public.ext_artifact_e2e INCLUDING ALL);
   SELECT public.flashback_internal_snapshot_materialize(
     $SNAPSHOT,$TRACKING,'public','ext_artifact_restored','\"id\",\"note\"',''
   )" >/dev/null
SOURCE_FP=$(q "SELECT md5(string_agg(id::text||':'||note,',' ORDER BY id)) FROM public.ext_artifact_e2e")
RESTORED_FP=$(q "SELECT md5(string_agg(id::text||':'||note,',' ORDER BY id)) FROM public.ext_artifact_restored")
[[ "$SOURCE_FP" == "$RESTORED_FP" ]] || {
    echo "FAIL: restored fingerprint $RESTORED_FP != source $SOURCE_FP"; exit 1;
}
[[ "$(q "SELECT count(*) FROM public.ext_artifact_restored")" == 5000 ]] || {
    echo "FAIL: restored row count mismatch"; exit 1;
}

[[ "$(q "SELECT status FROM public.flashback_internal_snapshot_payload_healthy($SNAPSHOT,$TRACKING,false)")" == healthy ]] || {
    echo "FAIL: shallow health rejected valid artifact"; exit 1;
}
[[ "$(q "SELECT status FROM public.flashback_internal_snapshot_payload_healthy($SNAPSHOT,$TRACKING,true)")" == healthy ]] || {
    echo "FAIL: deep health rejected valid artifact"; exit 1;
}
SCAN=$(q "SELECT public.flashback_internal_reconcile_external_snapshot_scan(1)")
[[ "$(jq -r .deep_checked <<<"$SCAN")" == 1 ]] || {
    echo "FAIL: maintenance health scan did not deep-check one artifact: $SCAN"; exit 1;
}
[[ "$(q "SELECT status||'|'||(deep_checked_at IS NOT NULL)::text FROM flashback.snapshot_health_audits WHERE snapshot_id=$SNAPSHOT AND tracking_id=$TRACKING")" == "healthy|true" ]] || {
    echo "FAIL: maintenance health audit was not persisted"; exit 1;
}

if [[ "${PGFB_EXTZSTD_CLEANUP:-0}" == 1 ]]; then
    UNPROTECT=$(q "SELECT public.flashback_unprotect('public.ext_artifact_e2e')")
    [[ "$(jq -r .status <<<"$UNPROTECT")" == stopping ]] || {
        echo "FAIL: external cleanup unprotect did not enter stopping: $UNPROTECT"; exit 1;
    }
    for _ in $(seq 1 200); do
        q "SELECT public.flashback_consume_wal(50000)" >/dev/null
        q "SELECT public.flashback_finalize_unprotect_operations()" >/dev/null
        [[ "$(q "SELECT protection_state FROM flashback.tracked_tables WHERE tracking_id=$TRACKING")" == unprotected ]] && break
        sleep 0.05
    done
    [[ "$(q "SELECT protection_state||'|'||is_active::text FROM flashback.tracked_tables WHERE tracking_id=$TRACKING")" == "unprotected|false" ]] || {
        echo "FAIL: external lifecycle did not finish unprotected"; exit 1;
    }

    CLEANUP1=$(q "SELECT public.flashback_cleanup($TRACKING,false)")
    [[ "$(jq -r .status <<<"$CLEANUP1")" == cleanup_pending ]] || {
        echo "FAIL: first external cleanup transaction did not stop after durable retire begin: $CLEANUP1"; exit 1;
    }
    [[ -d "$FINAL_DIR" ]] || { echo "FAIL: first cleanup transaction removed bytes"; exit 1; }
    [[ "$(q "SELECT payload_state FROM flashback.snapshots WHERE snapshot_id=$SNAPSHOT")" == retiring ]] || {
        echo "FAIL: first cleanup transaction did not commit retiring"; exit 1;
    }

    RECONCILE_RETIRE=$(q "SELECT public.flashback_internal_reconcile_external_snapshot_retirements(1)")
    [[ "$(jq -r .finished <<<"$RECONCILE_RETIRE")" == 1 ]] || {
        echo "FAIL: maintenance did not finish pending external retirement: $RECONCILE_RETIRE"; exit 1;
    }
    CLEANUP2=$(q "SELECT public.flashback_cleanup($TRACKING,false)")
    [[ "$(jq -r .status <<<"$CLEANUP2")" == cleaned ]] || {
        echo "FAIL: second external cleanup transaction did not finish: $CLEANUP2"; exit 1;
    }
    [[ ! -e "$FINAL_DIR" ]] || { echo "FAIL: cleanup left external artifact bytes"; exit 1; }
    [[ "$(q "SELECT payload_state FROM flashback.snapshots WHERE snapshot_id=$SNAPSHOT")" == retired ]] || {
        echo "FAIL: cleanup did not finish snapshot retired"; exit 1;
    }
    echo "EXTERNAL_ZSTD_ARTIFACT_E2E=PASS mode=operator-cleanup"
    exit 0
fi

if [[ "${PGFB_EXTZSTD_RETIRE:-0}" == 1 ]]; then
    [[ "$(q "SELECT public.flashback_internal_snapshot_retire_begin($SNAPSHOT,$TRACKING)")" == t ]] || {
        echo "FAIL: retirement begin did not transition artifact"; exit 1;
    }
    [[ "$(q "SELECT payload_state FROM flashback.snapshots WHERE snapshot_id=$SNAPSHOT")" == retiring ]] || {
        echo "FAIL: retiring state did not commit before purge"; exit 1;
    }
    [[ -d "$FINAL_DIR" ]] || { echo "FAIL: begin physically removed artifact"; exit 1; }

    # A restore reader holds a shared flock on the immutable artifact. Purge
    # must acquire the corresponding exclusive non-blocking lock and refuse
    # deletion while that reader is live.
    LOCK_READY="$BASE/restore-lock.ready"
    python3 - "$FINAL_DIR/artifact.zst" "$LOCK_READY" <<'PY' &
import fcntl
import pathlib
import sys
import time

with open(sys.argv[1], "rb") as artifact:
    fcntl.flock(artifact, fcntl.LOCK_SH)
    pathlib.Path(sys.argv[2]).write_text("ready", encoding="utf-8")
    time.sleep(60)
PY
    LOCK_PID=$!
    for _ in $(seq 1 100); do [[ -s "$LOCK_READY" ]] && break; sleep 0.05; done
    [[ -s "$LOCK_READY" ]] || { echo "FAIL: restore lock holder did not become ready"; exit 1; }
    if q "SELECT public.flashback_internal_snapshot_retire_purge($SNAPSHOT,$TRACKING)" \
        >"$BASE/purge-while-read.out" 2>&1; then
        echo "FAIL: purge succeeded while a restore held the artifact lock"; exit 1
    fi
    grep -q 'in use by a restore' "$BASE/purge-while-read.out" || {
        echo "FAIL: concurrent purge failed for the wrong reason"; exit 1;
    }
    [[ -s "$FINAL_DIR/artifact.zst" ]] || { echo "FAIL: refused purge removed artifact"; exit 1; }
    [[ "$(q "SELECT payload_state FROM flashback.snapshots WHERE snapshot_id=$SNAPSHOT")" == retiring ]] || {
        echo "FAIL: refused purge changed durable retirement state"; exit 1;
    }
    kill "$LOCK_PID"
    wait "$LOCK_PID" >/dev/null 2>&1 || true
    LOCK_PID=""

    [[ "$(q "SELECT public.flashback_internal_snapshot_retire_purge($SNAPSHOT,$TRACKING)")" == t ]] || {
        echo "FAIL: physical purge did not remove artifact"; exit 1;
    }
    [[ ! -e "$FINAL_DIR" ]] || { echo "FAIL: published artifact directory survived purge"; exit 1; }

    # Simulate a crash after irreversible filesystem deletion but before the
    # database terminal transition. The committed `retiring` row must survive
    # restart and finish idempotently without requiring the files to reappear.
    "$BINDIR/pg_ctl" -D "$DATA" -m immediate restart -l "$LOG" >/dev/null
    [[ "$(q "SELECT payload_state FROM flashback.snapshots WHERE snapshot_id=$SNAPSHOT")" == retiring ]] || {
        echo "FAIL: restart lost the durable retiring state"; exit 1;
    }
    [[ "$(q "SELECT public.flashback_internal_snapshot_retire_finish($SNAPSHOT,$TRACKING,'retired')")" == t ]] || {
        echo "FAIL: retirement finish did not transition artifact"; exit 1;
    }
    [[ "$(q "SELECT payload_state FROM flashback.snapshots WHERE snapshot_id=$SNAPSHOT")" == retired ]] || {
        echo "FAIL: artifact did not finish retired"; exit 1;
    }
    [[ "$(q "SELECT public.flashback_internal_snapshot_retire_finish($SNAPSHOT,$TRACKING,'retired')")" == f ]] || {
        echo "FAIL: retirement finish retry was not idempotent"; exit 1;
    }
    echo "EXTERNAL_ZSTD_ARTIFACT_E2E=PASS mode=retirement"
    exit 0
fi

# A missing payload and one-byte corruption must be visible to the read-only
# health probe, and the restore reader must fail closed without leaving rows.
mv "$FINAL_DIR/artifact.zst" "$FINAL_DIR/artifact.zst.missing"
[[ "$(q "SELECT status FROM public.flashback_internal_snapshot_payload_healthy($SNAPSHOT,$TRACKING,false)")" == unhealthy ]] || {
    echo "FAIL: shallow health accepted missing artifact"; exit 1;
}
mv "$FINAL_DIR/artifact.zst.missing" "$FINAL_DIR/artifact.zst"

cp "$FINAL_DIR/artifact.zst" "$FINAL_DIR/artifact.zst.good"
python3 - "$FINAL_DIR/artifact.zst" <<'PY'
import os, sys
path = sys.argv[1]
with open(path, "r+b") as f:
    size = os.fstat(f.fileno()).st_size
    pos = max(1, size // 2)
    f.seek(pos)
    old = f.read(1)
    f.seek(pos)
    f.write(bytes([old[0] ^ 0x01]))
    f.flush()
    os.fsync(f.fileno())
PY
[[ "$(q "SELECT status FROM public.flashback_internal_snapshot_payload_healthy($SNAPSHOT,$TRACKING,true)")" == unhealthy ]] || {
    echo "FAIL: deep health accepted corrupted artifact"; exit 1;
}
q "CREATE TABLE public.ext_artifact_corrupt_target
     (LIKE public.ext_artifact_e2e INCLUDING ALL)" >/dev/null
set +e
CORRUPT_ERROR=$("$BINDIR/psql" -X -v ON_ERROR_STOP=1 -d postgres -qAtc "SELECT public.flashback_internal_snapshot_materialize(
  $SNAPSHOT,$TRACKING,'public','ext_artifact_corrupt_target','\"id\",\"note\"',''
)" 2>&1)
CORRUPT_RC=$?
set -e
[[ "$CORRUPT_RC" -ne 0 ]] || { echo "FAIL: corrupted artifact restore succeeded"; exit 1; }
grep -Eq 'external artifact|zstd|checksum|integrity|decode' <<<"$CORRUPT_ERROR" || {
    echo "FAIL: corrupted artifact failed for an unrelated reason: $CORRUPT_ERROR"; exit 1;
}
[[ "$(q "SELECT count(*) FROM public.ext_artifact_corrupt_target")" == 0 ]] || {
    echo "FAIL: corrupted artifact left partial rows"; exit 1;
}
RECONCILE=$(q "SELECT public.flashback_internal_reconcile_snapshot_health(
  $SNAPSHOT,$TRACKING,true
)")
[[ "$(jq -r .changed <<<"$RECONCILE")" == true ]] || {
    echo "FAIL: corrupt artifact reconciliation did not change state: $RECONCILE"; exit 1;
}
[[ "$(q "SELECT payload_state FROM flashback.snapshots WHERE snapshot_id=$SNAPSHOT")" == missing ]] || {
    echo "FAIL: reconciliation did not mark corrupt artifact missing"; exit 1;
}
[[ "$(q "SELECT count(*) FROM flashback.coverage_gaps WHERE tracking_id=$TRACKING AND source_generation_id=$GENERATION AND reason='snapshot_payload_missing_or_corrupt'")" == 1 ]] || {
    echo "FAIL: reconciliation did not persist exactly one coverage gap"; exit 1;
}
[[ "$(q "SELECT state_reason FROM flashback.coverage_generations WHERE generation_id=$GENERATION")" == snapshot_payload_missing_or_corrupt ]] || {
    echo "FAIL: reconciliation did not freeze generation health"; exit 1;
}
mv -f "$FINAL_DIR/artifact.zst.good" "$FINAL_DIR/artifact.zst"
[[ "$(q "SELECT status FROM public.flashback_internal_snapshot_payload_healthy($SNAPSHOT,$TRACKING,true)")" == unhealthy ]] || {
    echo "FAIL: terminal missing state was silently healed by replacing files"; exit 1;
}

echo "EXTERNAL_ZSTD_ARTIFACT_E2E=PASS"
echo "tracking_id=$TRACKING generation_id=$GENERATION snapshot_id=$SNAPSHOT rows=$ROW_COUNT fingerprint=$RESTORED_FP"
