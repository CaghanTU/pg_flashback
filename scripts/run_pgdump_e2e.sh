#!/usr/bin/env bash
# Real pg_dump/pg_restore round trip for the logical-restore contract.
#
# The extension deliberately exports no tracking or recovery state. Runtime
# snapshot/partition/artifact tables must therefore be extension members too;
# otherwise pg_dump would restore them as meaningless orphan application data.
#
# Prerequisite: install the current build for the selected PG_CONFIG, e.g.
#   cargo pgrx install --pg-config /usr/local/pgsql-17/bin/pg_config
# Then run:
#   PG_CONFIG=/usr/local/pgsql-17/bin/pg_config ./scripts/run_pgdump_e2e.sh
#
# Set PGDUMP_E2E_KEEP=1 to preserve the temporary cluster for debugging.

set -Eeuo pipefail
umask 077

PG_CONFIG="${PG_CONFIG:-pg_config}"
command -v "$PG_CONFIG" >/dev/null 2>&1 || {
    printf 'FAIL: PG_CONFIG is not executable: %s\n' "$PG_CONFIG" >&2
    exit 69
}

PG_BIN="$($PG_CONFIG --bindir)"
for binary in initdb pg_ctl psql createdb pg_dump pg_restore; do
    [[ -x "$PG_BIN/$binary" ]] || {
        printf 'FAIL: required PostgreSQL binary is missing: %s/%s\n' "$PG_BIN" "$binary" >&2
        exit 69
    }
done

WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/pgfb-pgdump-e2e.XXXXXXXX")"
PGDATA="$WORKDIR/data"
SOCKET_DIR="$WORKDIR/socket"
LOGFILE="$WORKDIR/postgresql.log"
DUMPFILE="$WORKDIR/source.dump"
PORT="${PGDUMP_E2E_PORT:-55432}"
STARTED=0

cleanup() {
    local rc=$?
    trap - EXIT INT TERM
    set +e
    if [[ "${PGDUMP_E2E_KEEP:-0}" == "1" ]]; then
        printf 'PGDUMP_E2E_KEEP=1: temporary cluster preserved at %s\n' "$WORKDIR" >&2
        printf 'connection: %s/psql -h %s -p %s -d pgdump_dst\n' \
            "$PG_BIN" "$SOCKET_DIR" "$PORT" >&2
    else
        if [[ "$STARTED" == "1" ]]; then
            "$PG_BIN/pg_ctl" -D "$PGDATA" stop -m immediate -w >/dev/null 2>&1 || true
        fi
        rm -rf -- "$WORKDIR"
    fi
    if [[ "$rc" == "0" ]]; then
        printf 'pg_dump E2E: all checks passed\n'
    else
        printf 'pg_dump E2E: FAILED (exit=%s)\n' "$rc" >&2
    fi
    exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'printf "FAIL: command failed at line %s\n" "$LINENO" >&2' ERR

mkdir -p "$SOCKET_DIR"
"$PG_BIN/initdb" -D "$PGDATA" --no-locale --encoding=UTF8 -U postgres \
    >"$WORKDIR/initdb.log"

{
    printf "listen_addresses = ''\n"
    printf 'port = %s\n' "$PORT"
    printf "unix_socket_directories = '%s'\n" "$SOCKET_DIR"
    printf "shared_preload_libraries = 'pg_flashback'\n"
    printf "wal_level = 'logical'\n"
    printf "max_replication_slots = 8\n"
    printf "max_wal_senders = 8\n"
    printf "max_worker_processes = 16\n"
    printf "pg_flashback.enabled = on\n"
    printf "pg_flashback.capture_mode = 'wal'\n"
    printf "pg_flashback.target_databases = 'pgdump_src'\n"
    printf "pg_flashback.max_workers = 4\n"
    printf "pg_flashback.local_max_snapshot_bytes = '8GB'\n"
    printf "pg_flashback.local_max_restore_peak_bytes = '16GB'\n"
    printf "pg_flashback.local_min_filesystem_bytes = '64MB'\n"
    printf "pg_flashback.local_safety_reserve_bytes = '16MB'\n"
} >>"$PGDATA/postgresql.conf"

"$PG_BIN/pg_ctl" -D "$PGDATA" -l "$LOGFILE" -w start >/dev/null
STARTED=1

PSQL=("$PG_BIN/psql" -X -v ON_ERROR_STOP=1 -U postgres -h "$SOCKET_DIR" -p "$PORT")
CREATEDB=("$PG_BIN/createdb" -U postgres -h "$SOCKET_DIR" -p "$PORT")

"${CREATEDB[@]}" pgdump_src
# Capture workers bind at postmaster start; restart so pgdump_src is admitted.
"$PG_BIN/pg_ctl" -D "$PGDATA" -l "$LOGFILE" -w restart >/dev/null

"${PSQL[@]}" -d pgdump_src <<'SQL'
CREATE EXTENSION pg_flashback;
CREATE TABLE public.orders (
    id integer PRIMARY KEY,
    customer text NOT NULL,
    amount numeric(12,2) NOT NULL
);
INSERT INTO public.orders VALUES
    (1, 'Ada', 10.00),
    (2, 'Linus', 20.00),
    (3, 'Grace', 30.00);
SQL

# flashback_track requires a dedicated transaction with an admitted capture worker.
"${PSQL[@]}" -d pgdump_src -c "SELECT flashback_track('public.orders');"

"${PSQL[@]}" -d pgdump_src <<'SQL'
-- WAL capture is asynchronous; for dump-compat we only need application
-- rows + extension-owned payload adoption, not drained deltas or legacy
-- full-table checkpoints (disabled for correctness-qualified WAL generations).
UPDATE public.orders SET amount = amount + 5 WHERE id = 2;
INSERT INTO public.orders VALUES (4, 'Edsger', 40.00);

-- Simulate payload created by a pre-fix installation, then run the explicit
-- upgrade helper. The second call proves idempotency.
CREATE TABLE flashback.snap_999999_999999 AS TABLE public.orders;
DO $verify_adoption$
DECLARE
    v_adopted integer;
BEGIN
    v_adopted := flashback_adopt_existing_payload_tables();
    IF v_adopted <> 1 THEN
        RAISE EXCEPTION 'expected 1 legacy payload adoption, got %', v_adopted;
    END IF;
    IF flashback_adopt_existing_payload_tables() <> 0 THEN
        RAISE EXCEPTION 'payload adoption is not idempotent';
    END IF;
END;
$verify_adoption$;

DO $verify_source$
DECLARE
    v_payloads integer;
    v_orphans integer;
BEGIN
    SELECT count(*),
           count(*) FILTER (WHERE member.objid IS NULL)
      INTO v_payloads, v_orphans
    FROM pg_class c
    LEFT JOIN (
        SELECT d.objid
        FROM pg_depend d
        JOIN pg_extension e ON e.oid = d.refobjid
        WHERE d.classid = 'pg_class'::regclass
          AND d.refclassid = 'pg_extension'::regclass
          AND d.deptype = 'e'
          AND e.extname = 'pg_flashback'
    ) member ON member.objid = c.oid
    WHERE flashback_payload_kind(c.oid::regclass) IS NOT NULL;

    IF v_payloads < 2 THEN
        RAISE EXCEPTION 'source payload exercise is incomplete: only % relations', v_payloads;
    END IF;
    IF v_orphans <> 0 THEN
        RAISE EXCEPTION 'source has % orphan payload relations', v_orphans;
    END IF;
END;
$verify_source$;
SQL

"$PG_BIN/pg_dump" -Fc -U postgres -h "$SOCKET_DIR" -p "$PORT" \
    -d pgdump_src -f "$DUMPFILE"
"${CREATEDB[@]}" pgdump_dst
"$PG_BIN/pg_restore" --exit-on-error --single-transaction \
    -U postgres -h "$SOCKET_DIR" -p "$PORT" -d pgdump_dst "$DUMPFILE"

"${PSQL[@]}" -d pgdump_dst <<'SQL'
DO $verify_restore$
DECLARE
    v_extconfig integer;
    v_state_rows bigint;
    v_payloads integer;
BEGIN
    SELECT count(*) INTO v_extconfig
    FROM pg_extension e
    CROSS JOIN LATERAL unnest(e.extconfig) config_oid
    WHERE e.extname = 'pg_flashback';
    IF v_extconfig <> 0 THEN
        RAISE EXCEPTION 'restored pg_flashback extconfig is not empty: %', v_extconfig;
    END IF;

    SELECT
          (SELECT count(*) FROM flashback.tracking_lifecycles)
        + (SELECT count(*) FROM flashback.tracked_tables)
        + (SELECT count(*) FROM flashback.capture_streams)
        + (SELECT count(*) FROM flashback.capture_commits)
        + (SELECT count(*) FROM flashback.coverage_generations)
        + (SELECT count(*) FROM flashback.coverage_gaps)
        + (SELECT count(*) FROM flashback.pending_wal_events)
        + (SELECT count(*) FROM flashback.snapshots)
        + (SELECT count(*) FROM flashback.delta_log)
        + (SELECT count(*) FROM flashback.schema_versions)
        + (SELECT count(*) FROM flashback.restore_log)
      INTO v_state_rows;
    IF v_state_rows <> 0 THEN
        RAISE EXCEPTION 'logical restore imported % tracking/recovery rows', v_state_rows;
    END IF;

    SELECT count(*) INTO v_payloads
    FROM pg_class c
    WHERE flashback_payload_kind(c.oid::regclass) IS NOT NULL;
    IF v_payloads <> 0 THEN
        RAISE EXCEPTION 'logical restore contains % orphan runtime payload relations', v_payloads;
    END IF;

    IF (SELECT count(*) FROM public.orders) <> 4
       OR (SELECT amount FROM public.orders WHERE id = 2) <> 25.00
    THEN
        RAISE EXCEPTION 'application data did not survive pg_dump/pg_restore';
    END IF;
END;
$verify_restore$;
SQL
