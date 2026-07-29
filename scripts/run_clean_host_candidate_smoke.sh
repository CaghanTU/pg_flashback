#!/usr/bin/env bash
# Clean-host smoke for the supported local_delta candidate archive.
# Installs extension + CLI only from CANDIDATE_DIR; never cargo-builds and has
# no external backup dependency.
set -Eeuo pipefail

CANDIDATE_DIR="${CANDIDATE_DIR:?CANDIDATE_DIR is required}"
PG_BIN="${PG_BIN:?PG_BIN is required}"
KEEP="${KEEP:-0}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
WORK="${CLEAN_HOST_WORK:-/tmp/pgfb-clean-host-$RUN_ID}"
# Persistent by default: transient WORK is removed on a completed, non-KEEP
# run, so the evidence file itself must not live under WORK.
RESULT_JSON="${CLEAN_HOST_RESULT:-$ROOT/target/qualification/clean-host-smoke-$RUN_ID.json}"
MANIFEST="$CANDIDATE_DIR/MANIFEST.json"
PORT=$((36000 + ($$ % 20000)))
SOCKET="/tmp/pgfb-ch-$RUN_ID"
DATA="$WORK/data"
INSTALL_ROOT="$WORK/install"
PRIMARY_STARTED=0
PREFIX_INSTALLED=0
RUN_COMPLETE=0
PASSED=0

log() { printf '[clean-host-smoke] %s %s\n' "$(date +%H:%M:%S)" "$*"; }
die() { log "FAIL: $*"; exit 1; }
pass() { PASSED=$((PASSED + 1)); log "PASS[$PASSED]: $1"; }
sha() { sha256sum "$1" | awk '{print $1}'; }

for exe in initdb pg_ctl psql pg_config; do
    [[ -x "$PG_BIN/$exe" ]] || die "missing $PG_BIN/$exe"
done
command -v jq >/dev/null || die "jq is required"
[[ -f "$MANIFEST" ]] || die "MANIFEST.json missing"

SOURCE_COMMIT="$(jq -r '.provenance.source_commit' "$MANIFEST")"
SOURCE_TREE="$(jq -r '.provenance.source_tree' "$MANIFEST")"
ARCH="$(jq -r '.provenance.arch' "$MANIFEST")"
PG_MAJOR="$(jq -r '.provenance.pg_major' "$MANIFEST")"
EXT_ARCHIVE="$(jq -r '.artifacts.extension_archive.name' "$MANIFEST")"
PACKAGE_SHA="$(jq -r '.artifacts.package_sha256' "$MANIFEST")"
EXT_SHA="$(jq -r '.artifacts.extension_binary_sha256' "$MANIFEST")"
CLI_SHA="$(jq -r '.artifacts.cli_binary_sha256' "$MANIFEST")"
[[ -f "$CANDIDATE_DIR/$EXT_ARCHIVE" ]] || die "extension archive missing"
(
    cd "$CANDIDATE_DIR"
    sha256sum -c SHA256SUMS >/dev/null
    echo "$PACKAGE_SHA  $EXT_ARCHIVE" | sha256sum -c - >/dev/null
) || die "candidate digest verification failed"

PKGLIB="$("$PG_BIN/pg_config" --pkglibdir)"
SHARE_EXT="$("$PG_BIN/pg_config" --sharedir)/extension"
STASH="$WORK/prefix-stash"

stash_file() {
    local path=$1 rel=$2
    mkdir -p "$STASH/$(dirname "$rel")"
    if [[ -e "$path" ]]; then
        cp -a "$path" "$STASH/$rel"
        printf 'present\n' >"$STASH/$rel.state"
    else
        printf 'absent\n' >"$STASH/$rel.state"
    fi
}

restore_prefix() {
    local state rel dest
    [[ -d "$STASH" ]] || return 0
    while IFS= read -r -d '' state; do
        rel="${state#"$STASH/"}"
        rel="${rel%.state}"
        case "$rel" in
            lib/*) dest="$PKGLIB/${rel#lib/}" ;;
            share/*) dest="$SHARE_EXT/${rel#share/}" ;;
            *) continue ;;
        esac
        if [[ "$(cat "$state")" == present ]]; then
            cp -a "$STASH/$rel" "$dest"
        else
            rm -f "$dest"
        fi
    done < <(find "$STASH" -type f -name '*.state' -print0)
}

write_result() {
    local rc=$1 status=failed
    [[ "$rc" == 0 && "$RUN_COMPLETE" == 1 ]] && status=passed
    mkdir -p "$(dirname "$RESULT_JSON")"
    jq -n \
        --arg status "$status" \
        --arg source_commit "$SOURCE_COMMIT" \
        --arg source_tree "$SOURCE_TREE" \
        --arg package_sha256 "$PACKAGE_SHA" \
        --arg extension_sha256 "$EXT_SHA" \
        --arg cli_sha256 "$CLI_SHA" \
        --arg arch "$ARCH" \
        --arg pg_major "$PG_MAJOR" \
        --argjson assertions "$PASSED" \
        '{
          qualification_kind:"clean_host_local_delta",
          status:$status,
          source_commit:$source_commit,
          source_tree:$source_tree,
          package_sha256:$package_sha256,
          extension_binary_sha256:$extension_sha256,
          cli_binary_sha256:$cli_sha256,
          arch:$arch,
          pg_major:$pg_major,
          install_source:"candidate_archives_only",
          assertions_passed:$assertions,
          external_backup_dependency:false
        }' >"$RESULT_JSON"
}

cleanup() {
    local rc=$?
    trap - EXIT INT TERM
    set +e
    if [[ "$PRIMARY_STARTED" == 1 ]]; then
        "$PG_BIN/pg_ctl" -D "$DATA" stop -m immediate -w >/dev/null 2>&1 || true
    fi
    if [[ "$PREFIX_INSTALLED" == 1 ]]; then restore_prefix || rc=1; fi
    write_result "$rc"
    rm -rf "$SOCKET"
    if [[ "$RUN_COMPLETE" == 1 && "$KEEP" != 1 ]]; then
        rm -rf "$WORK"
    else
        log "artifacts kept at $WORK"
    fi
    exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

mkdir -p "$INSTALL_ROOT" "$SOCKET" "$WORK/log"
chmod 700 "$SOCKET"
tar -C "$INSTALL_ROOT" -xzf "$CANDIDATE_DIR/$EXT_ARCHIVE"
EXT_ROOT="$(find "$INSTALL_ROOT" -maxdepth 1 -type d -name 'pg_flashback-candidate-*' -print -quit)"
[[ -n "$EXT_ROOT" ]] || die "unexpected archive layout"
CLI="$EXT_ROOT/bin/pg_flashback"
[[ -x "$CLI" ]] || die "packaged CLI missing"
[[ "$(sha "$EXT_ROOT/lib/pg_flashback.so")" == "$EXT_SHA" ]] || die "extension hash mismatch"
[[ "$(sha "$CLI")" == "$CLI_SHA" ]] || die "CLI hash mismatch"
pass "candidate archive and binary hashes verified"

stash_file "$PKGLIB/pg_flashback.so" lib/pg_flashback.so
stash_file "$SHARE_EXT/pg_flashback.control" share/pg_flashback.control
for sql in "$EXT_ROOT"/share/extension/pg_flashback--*.sql; do
    stash_file "$SHARE_EXT/$(basename "$sql")" "share/$(basename "$sql")"
done
install -m 0755 "$EXT_ROOT/lib/pg_flashback.so" "$PKGLIB/pg_flashback.so"
install -m 0644 "$EXT_ROOT/share/extension/pg_flashback.control" \
    "$EXT_ROOT"/share/extension/pg_flashback--*.sql "$SHARE_EXT/"
PREFIX_INSTALLED=1
[[ "$(sha "$PKGLIB/pg_flashback.so")" == "$EXT_SHA" ]] || die "installed extension hash mismatch"
pass "installed extension and CLI from candidate archive only"

"$PG_BIN/initdb" -D "$DATA" --no-locale --encoding=UTF8 --auth=trust >/dev/null
cat >>"$DATA/postgresql.conf" <<EOF
shared_preload_libraries = 'pg_flashback'
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
unix_socket_directories = '$SOCKET'
port = $PORT
pg_flashback.enabled = on
pg_flashback.capture_mode = wal
pg_flashback.worker_interval_ms = 50
pg_flashback.target_databases = 'postgres'
pg_flashback.local_max_snapshot_bytes = 8GB
pg_flashback.local_max_restore_peak_bytes = 16GB
pg_flashback.local_min_filesystem_bytes = 64MB
pg_flashback.local_safety_reserve_bytes = 16MB
pg_flashback.allow_unaudited_restore = on
EOF
"$PG_BIN/pg_ctl" -D "$DATA" -l "$WORK/log/postgresql.log" start -w >/dev/null
PRIMARY_STARTED=1

export PGHOST="$SOCKET" PGPORT="$PORT" PGDATABASE=postgres
q() { "$PG_BIN/psql" -X -v ON_ERROR_STOP=1 -qAtc "$1"; }
fingerprint_of() {
    local rel=$1
    q "SELECT count(*)::text || '|' ||
              COALESCE(bit_xor(hashtextextended(row_to_json(t)::text, 0)), 0)::text
       FROM $rel AS t;"
}
data_sha256_of() {
    local rel=$1
    "$PG_BIN/psql" -X -qAt -v ON_ERROR_STOP=1 \
        -c "COPY (SELECT * FROM $rel ORDER BY id) TO STDOUT (FORMAT binary)" \
        | sha256sum | awk '{print $1}'
}
expect_sql_failure() {
    local label=$1 sql=$2
    if q "$sql" >/dev/null 2>&1; then
        die "$label unexpectedly succeeded"
    fi
}
q "CREATE EXTENSION pg_flashback;"
for _ in $(seq 1 200); do
    [[ "$(q "SELECT admission_state FROM flashback_worker_readiness();")" == ready ]] && break
    sleep 0.05
done
[[ "$(q "SELECT admission_state FROM flashback_worker_readiness();")" == ready ]] || die "workers not ready"
"$CLI" doctor >/dev/null
pass "fresh CREATE EXTENSION and doctor"

q "CREATE ROLE smoke_owner;
   CREATE ROLE smoke_reader;
   CREATE ROLE smoke_app;
   CREATE TABLE public.smoke_parent(id integer PRIMARY KEY);
   INSERT INTO public.smoke_parent VALUES (1), (2);
   CREATE TABLE public.smoke_trigger_audit(
     audit_id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
     row_id bigint NOT NULL,
     fired_at timestamptz NOT NULL DEFAULT clock_timestamp()
   );
   CREATE FUNCTION public.smoke_orders_audit()
   RETURNS trigger
   LANGUAGE plpgsql
   SECURITY DEFINER
   SET search_path = pg_catalog, public
   AS \$fn\$
   BEGIN
     INSERT INTO public.smoke_trigger_audit(row_id) VALUES (NEW.id);
     RETURN NEW;
   END
   \$fn\$;
   REVOKE ALL ON FUNCTION public.smoke_orders_audit() FROM PUBLIC;
   CREATE TABLE public.orders(
     id bigint GENERATED BY DEFAULT AS IDENTITY
       (START WITH 11 INCREMENT BY 2 CACHE 1),
     parent_id integer NOT NULL,
     external_key text COLLATE pg_catalog.\"C\" NOT NULL,
     qty integer NOT NULL,
     note text NOT NULL,
     payload jsonb NOT NULL DEFAULT '{}'::jsonb,
     tags text[] NOT NULL DEFAULT ARRAY[]::text[],
     toast_payload text,
     tenant text NOT NULL,
     created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
     CONSTRAINT orders_identity_pk
       PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED,
     CONSTRAINT orders_external_key_uq UNIQUE (external_key),
     CONSTRAINT orders_qty_ck CHECK (qty BETWEEN 0 AND 100),
     CONSTRAINT orders_parent_fk FOREIGN KEY (parent_id)
       REFERENCES public.smoke_parent(id)
   );
   CREATE INDEX orders_note_idx ON public.orders USING btree(note);
   CREATE TRIGGER orders_audit
     AFTER INSERT ON public.orders
     FOR EACH ROW EXECUTE FUNCTION public.smoke_orders_audit();
   COMMENT ON TABLE public.orders IS 'clean-host rich metadata';
   COMMENT ON COLUMN public.orders.toast_payload IS 'out-of-line payload';
   ALTER TABLE public.orders ENABLE ROW LEVEL SECURITY;
   ALTER TABLE public.orders FORCE ROW LEVEL SECURITY;
   CREATE POLICY orders_tenant_policy ON public.orders
     FOR ALL TO smoke_app
     USING (tenant = current_user)
     WITH CHECK (tenant = current_user);
   ALTER TABLE public.orders OWNER TO smoke_owner;
   REVOKE ALL ON public.orders FROM PUBLIC;
   GRANT SELECT ON public.orders TO smoke_reader;
   GRANT SELECT, INSERT, UPDATE ON public.orders TO smoke_app;"
[[ "$(q "SELECT flashback_local_compatibility('public.orders'::regclass)->>'supported';")" == true ]] \
    || die "rich supported fixture was rejected by compatibility gate"
pass "rich schema accepted by the machine compatibility contract"
"$CLI" protect public.orders >/dev/null
for _ in $(seq 1 300); do
    [[ "$(q "SELECT health FROM flashback_health() WHERE table_name='public.orders';")" == healthy ]] && break
    sleep 0.05
done
[[ "$(q "SELECT health FROM flashback_health() WHERE table_name='public.orders';")" == healthy ]] \
    || die "coverage not healthy"
q "INSERT INTO public.orders(
       parent_id, external_key, qty, note, payload, tags, toast_payload, tenant
   )
   SELECT 1 + (g % 2),
          'key-'||g,
          g % 100,
          'row-'||g,
          jsonb_build_object('v',g,'unicode','İstanbul-ğüşiöç'),
          ARRAY['tag-'||(g % 5), 'common'],
          (SELECT string_agg(md5(g::text||':'||s::text), '')
             FROM generate_series(1,256) AS s),
          CASE WHEN g % 2 = 0 THEN 'smoke_app' ELSE 'other' END
   FROM generate_series(1,100) AS g;
   UPDATE public.orders
      SET payload = payload || '{\"updated\":true}'::jsonb
    WHERE id IN (11,13,15);
   DELETE FROM public.orders WHERE id IN (17,19);"
for _ in $(seq 1 300); do
    [[ "$(q "SELECT count(*) FROM flashback.delta_log
             WHERE table_name='public.orders'
               AND event_type IN ('INSERT','UPDATE','DELETE');")" -ge 105 ]] && break
    sleep 0.05
done
FP="$(fingerprint_of public.orders)"
DATA_SHA="$(data_sha256_of public.orders)"
OLD_MAX_ID="$(q "SELECT max(id) FROM public.orders;")"
AUDIT_BEFORE_DROP="$(q "SELECT count(*) FROM public.smoke_trigger_audit;")"
EXPECTED_INVENTORY_DIGEST="$(q "
  WITH s AS (
    SELECT flashback_collect_schema_def('public.orders'::regclass) AS j
  )
  SELECT flashback_inventory_digest(
    (flashback_canonical_inventory_from_schema_def(s.j)
      || jsonb_build_object('replica_identity','f')) - 'sequences'
  )
  FROM s;")"
EXPECTED_COLLATION="$(q "
  SELECT format('%I.%I', n.nspname, coll.collname)
  FROM pg_attribute a
  JOIN pg_collation coll ON coll.oid=a.attcollation
  JOIN pg_namespace n ON n.oid=coll.collnamespace
  WHERE a.attrelid='public.orders'::regclass
    AND a.attname='external_key';")"
q "DROP TABLE public.orders;"
for _ in $(seq 1 300); do
    [[ "$(q "SELECT count(*) FROM flashback_disaster_points('public.orders', interval '1 hour')
             WHERE event_type='DROP' AND status='restorable';")" -ge 1 ]] && break
    sleep 0.05
done
"$CLI" recover public.orders --latest-drop --yes >/dev/null
[[ "$(fingerprint_of public.orders)" == "$FP" ]] \
    || die "recovered fingerprint mismatch"
[[ "$(data_sha256_of public.orders)" == "$DATA_SHA" ]] \
    || die "recovered COPY-binary SHA-256 mismatch"
[[ "$(q "SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid='public.orders'::regclass;")" == smoke_owner ]] \
    || die "owner mismatch"
[[ "$(q "SELECT has_table_privilege('smoke_reader','public.orders','SELECT');")" == t ]] \
    || die "ACL mismatch"
[[ "$(q "SELECT has_table_privilege('smoke_app','public.orders','INSERT')
                  AND has_table_privilege('smoke_app','public.orders','UPDATE');")" == t ]] \
    || die "application ACL mismatch"
[[ "$(q "SELECT flashback_inventory_digest(
                    flashback_canonical_inventory_from_relation(
                      'public.orders'::regclass
                    ) - 'sequences'
                  );")" == "$EXPECTED_INVENTORY_DIGEST" ]] \
    || die "restored canonical metadata inventory mismatch"
[[ "$(q "SELECT format('%I.%I', n.nspname, coll.collname)
              FROM pg_attribute a
              JOIN pg_collation coll ON coll.oid=a.attcollation
              JOIN pg_namespace n ON n.oid=coll.collnamespace
             WHERE a.attrelid='public.orders'::regclass
               AND a.attname='external_key';")" == "$EXPECTED_COLLATION" ]] \
    || die "column collation mismatch"
pass "exact DROP recovery preserved data, TOAST bytes and canonical metadata"

[[ "$(q "SELECT count(*) FROM pg_constraint
             WHERE conrelid='public.orders'::regclass
               AND contype IN ('p','u','c','f');")" == 4 ]] \
    || die "constraint set mismatch"
[[ "$(q "SELECT conname||'|'||pg_get_constraintdef(oid,true)
             FROM pg_constraint
            WHERE conrelid='public.orders'::regclass AND contype='p';")" \
      == "orders_identity_pk|PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED" ]] \
    || die "named/deferrable primary key mismatch"
[[ "$(q "SELECT confrelid='public.smoke_parent'::regclass
             FROM pg_constraint
            WHERE conrelid='public.orders'::regclass
              AND conname='orders_parent_fk';")" == t ]] \
    || die "outgoing foreign key target mismatch"
[[ "$(q "SELECT i.indisvalid AND am.amname='btree'
             FROM pg_class idx
             JOIN pg_index i ON i.indexrelid=idx.oid
             JOIN pg_am am ON am.oid=idx.relam
            WHERE idx.relname='orders_note_idx'
              AND i.indrelid='public.orders'::regclass;")" == t ]] \
    || die "secondary btree index missing or invalid"
expect_sql_failure "UNIQUE constraint probe" \
    "INSERT INTO public.orders(id,parent_id,external_key,qty,note,payload,tags,tenant)
     VALUES (-9001,1,'key-2',1,'dup','{}','{}','smoke_app');"
expect_sql_failure "CHECK constraint probe" \
    "INSERT INTO public.orders(id,parent_id,external_key,qty,note,payload,tags,tenant)
     VALUES (-9002,1,'bad-check',101,'bad','{}','{}','smoke_app');"
expect_sql_failure "outgoing FK probe" \
    "INSERT INTO public.orders(id,parent_id,external_key,qty,note,payload,tags,tenant)
     VALUES (-9003,999999,'bad-fk',1,'bad','{}','{}','smoke_app');"
pass "PK, UNIQUE, CHECK, outgoing FK and secondary index are behaviorally valid"

[[ "$(q "SELECT relrowsecurity AND relforcerowsecurity
             FROM pg_class WHERE oid='public.orders'::regclass;")" == t ]] \
    || die "RLS/FORCE RLS state mismatch"
[[ "$(q "SELECT count(*)=1 FROM pg_policy
             WHERE polrelid='public.orders'::regclass
               AND polname='orders_tenant_policy';")" == t ]] \
    || die "RLS policy missing"
RLS_COUNTS="$(q "SET ROLE smoke_app;
  SELECT count(*)||'|'||count(*) FILTER (WHERE tenant <> current_user)
  FROM public.orders;
  RESET ROLE;")"
[[ "${RLS_COUNTS##*$'\n'}" =~ ^[1-9][0-9]*\\|0$ ]] \
    || die "RLS behavioral filter mismatch: $RLS_COUNTS"
expect_sql_failure "RLS WITH CHECK probe" \
    "SET ROLE smoke_app;
     INSERT INTO public.orders(id,parent_id,external_key,qty,note,payload,tags,tenant)
     VALUES (-9004,1,'bad-rls',1,'bad','{}','{}','other');"
[[ "$(q "SELECT obj_description('public.orders'::regclass,'pg_class');")" \
      == "clean-host rich metadata" ]] \
    || die "table comment mismatch"
[[ "$(q "SELECT col_description(
                    'public.orders'::regclass,
                    (SELECT attnum FROM pg_attribute
                      WHERE attrelid='public.orders'::regclass
                        AND attname='toast_payload')
                  );")" == "out-of-line payload" ]] \
    || die "column comment mismatch"
pass "RLS/FORCE policy, owner/ACL and table/column comments are functional"

AUDIT_BEFORE_PROBE="$(q "SELECT count(*) FROM public.smoke_trigger_audit;")"
[[ "$AUDIT_BEFORE_PROBE" == "$AUDIT_BEFORE_DROP" ]] \
    || die "restore unexpectedly fired the user INSERT trigger"
NEW_ID="$(q "INSERT INTO public.orders(
                 parent_id,external_key,qty,note,payload,tags,toast_payload,tenant
               )
               VALUES (1,'post-restore',1,'probe','{}','{}','trigger-probe','smoke_app')
               RETURNING id;")"
[[ "$NEW_ID" == "$((OLD_MAX_ID + 2))" ]] \
    || die "identity sequence resumed at $NEW_ID, expected $((OLD_MAX_ID + 2))"
[[ "$(q "SELECT count(*) FROM public.smoke_trigger_audit;")" \
      == "$((AUDIT_BEFORE_PROBE + 1))" ]] \
    || die "restored trigger did not fire exactly once"
[[ "$(q "SELECT pg_get_serial_sequence('public.orders','id') IS NOT NULL;")" == t ]] \
    || die "identity sequence ownership missing"
pass "identity edge and ordinary trigger behavior survived recovery"

q "CREATE TABLE public.smoke_in_parent(id integer PRIMARY KEY);
   CREATE TABLE public.smoke_in_child(
     id integer PRIMARY KEY,
     parent_id integer REFERENCES public.smoke_in_parent(id)
   );
   CREATE TABLE public.smoke_part(id integer, d date) PARTITION BY RANGE(d);
   CREATE TABLE public.smoke_part_p PARTITION OF public.smoke_part
     FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');"
[[ "$(q "SELECT flashback_local_compatibility(
                    'public.smoke_in_parent'::regclass
                  )->'rejected_features' @> '[\"incoming_foreign_keys\"]'::jsonb;")" == t ]] \
    || die "incoming FK parent was not rejected"
[[ "$(q "SELECT flashback_local_compatibility(
                    'public.smoke_in_child'::regclass
                  )->>'supported';")" == true ]] \
    || die "outgoing FK child should remain supported"
[[ "$(q "SELECT flashback_local_compatibility(
                    'public.smoke_part'::regclass
                  )->'rejected_features' @> '[\"partitioned_table\"]'::jsonb;")" == t ]] \
    || die "partitioned table was not rejected"
if "$CLI" protect public.smoke_in_parent >/dev/null 2>&1; then
    die "CLI protect accepted a table with incoming foreign keys"
fi
if "$CLI" protect public.smoke_part >/dev/null 2>&1; then
    die "CLI protect accepted a partitioned table"
fi
[[ "$(q "SELECT count(*) FROM flashback.tracked_tables
             WHERE is_active
               AND schema_name='public'
               AND table_name IN ('smoke_in_parent','smoke_part');")" == 0 ]] \
    || die "unsupported relation left an active tracking lifecycle"
pass "incoming FK and partitioned-table negatives fail closed without residue"

RUN_COMPLETE=1
log "PASS: local-only clean-host smoke ($PASSED assertions)"
