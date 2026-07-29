#!/usr/bin/env bash
# Clean-host smoke for the supported local_delta candidate archive.
# Installs extension + CLI only from CANDIDATE_DIR; never cargo-builds and has
# no external backup dependency.
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/lib/acl_order_canon.sh
source "$ROOT/scripts/lib/acl_order_canon.sh"

CANDIDATE_DIR="${CANDIDATE_DIR:?CANDIDATE_DIR is required}"
PG_BIN="${PG_BIN:?PG_BIN is required}"
KEEP="${KEEP:-0}"
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
RACE_PID=""

log() { printf '[clean-host-smoke] %s %s\n' "$(date +%H:%M:%S)" "$*"; }
die() { log "FAIL: $*"; exit 1; }
pass() { PASSED=$((PASSED + 1)); log "PASS[$PASSED]: $1"; }
sha() { sha256sum "$1" | awk '{print $1}'; }

for exe in initdb pg_ctl psql pg_config pg_dump; do
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
    if [[ -n "$RACE_PID" ]]; then
        kill "$RACE_PID" >/dev/null 2>&1 || true
        wait "$RACE_PID" >/dev/null 2>&1 || true
    fi
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
# From this point onward cleanup must restore the prefix even if an install
# command is interrupted halfway through.
PREFIX_INSTALLED=1
install -m 0755 "$EXT_ROOT/lib/pg_flashback.so" "$PKGLIB/pg_flashback.so"
install -m 0644 "$EXT_ROOT/share/extension/pg_flashback.control" \
    "$EXT_ROOT"/share/extension/pg_flashback--*.sql "$SHARE_EXT/"
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
expect_sqlstate() {
    local label=$1 expected_state=$2 sql=$3
    q "DO \$probe\$
       BEGIN
         BEGIN
           $sql
           RAISE EXCEPTION '$label: probe unexpectedly succeeded'
             USING ERRCODE = 'P0001';
         EXCEPTION WHEN OTHERS THEN
           IF SQLSTATE = 'P0001' THEN
             RAISE;
           END IF;
           IF SQLSTATE IS DISTINCT FROM '$expected_state' THEN
             RAISE EXCEPTION '$label raised SQLSTATE %, expected $expected_state',
               SQLSTATE;
           END IF;
         END;
       END
       \$probe\$;" >/dev/null
}
schema_dump_of_orders() {
    local output=$1
    LC_ALL=C TZ=UTC "$PG_BIN/pg_dump" --schema-only \
        --table=public.orders \
        --table=public.orders_id_seq \
        --table=public.orders_ticket_seq \
        >"$output"
    sed -i \
        -e '/^\\\\restrict /d' \
        -e '/^\\\\unrestrict /d' \
        -e '/^-- Dumped /d' \
        -e '/^-- Started /d' \
        -e '/^-- Completed /d' \
        "$output"
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
   CREATE ROLE \"smoke Reporter\";
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
     ticket_no integer NOT NULL,
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
       ON UPDATE CASCADE ON DELETE RESTRICT
       DEFERRABLE INITIALLY DEFERRED
   );
   CREATE SEQUENCE public.orders_ticket_seq AS integer
     START WITH 1000 INCREMENT BY -3 MINVALUE -2147483648 MAXVALUE 1000
     CACHE 1 NO CYCLE;
   ALTER SEQUENCE public.orders_ticket_seq
     OWNED BY public.orders.ticket_no;
   ALTER TABLE public.orders ALTER COLUMN ticket_no
     SET DEFAULT nextval('public.orders_ticket_seq'::regclass);
   CREATE INDEX orders_note_idx ON public.orders USING btree(note);
   CREATE TRIGGER orders_audit
     AFTER INSERT ON public.orders
     FOR EACH ROW EXECUTE FUNCTION public.smoke_orders_audit();
   ALTER TABLE public.orders ENABLE ALWAYS TRIGGER orders_audit;
   COMMENT ON TABLE public.orders IS 'clean-host rich metadata';
   COMMENT ON COLUMN public.orders.toast_payload IS 'out-of-line payload';
   ALTER TABLE public.orders ENABLE ROW LEVEL SECURITY;
   ALTER TABLE public.orders FORCE ROW LEVEL SECURITY;
   CREATE POLICY orders_tenant_policy ON public.orders
     FOR ALL TO smoke_app, \"smoke Reporter\"
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
TRACKING_ID="$(q "SELECT tracking_id FROM flashback.tracked_tables
                   WHERE is_active AND schema_name='public'
                     AND table_name='orders';")"
[[ -n "$TRACKING_ID" ]] || die "orders tracking identity missing"
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
OLD_MIN_TICKET="$(q "SELECT min(ticket_no) FROM public.orders;")"
SCHEMA_DUMP_BEFORE="$WORK/orders-schema-before.sql"
SCHEMA_DUMP_BEFORE_2="$WORK/orders-schema-before-2.sql"
SCHEMA_DUMP_AFTER="$WORK/orders-schema-after.sql"
schema_dump_of_orders "$SCHEMA_DUMP_BEFORE"
schema_dump_of_orders "$SCHEMA_DUMP_BEFORE_2"
cmp -s "$SCHEMA_DUMP_BEFORE" "$SCHEMA_DUMP_BEFORE_2" \
    || die "schema-only pg_dump oracle is nondeterministic before DROP"
q "DROP TABLE public.orders;"
for _ in $(seq 1 300); do
    [[ "$(q "SELECT count(*) FROM flashback_disaster_points('public.orders', interval '1 hour')
             WHERE event_type='DROP' AND status='restorable';")" -ge 1 ]] && break
    sleep 0.05
done
"$CLI" recover public.orders --latest-drop --yes >/dev/null
for _ in $(seq 1 300); do
    [[ "$(q "SELECT health FROM flashback_health()
             WHERE tracking_id=$TRACKING_ID;")" == healthy ]] && break
    sleep 0.05
done
RECOVER_STATE="$(q "SELECT s.state||'|'||COALESCE(o.disaster_event_id::text,'')
                      FROM flashback.operations o
                      JOIN flashback.operation_current_state s
                        ON s.operation_id=o.operation_id
                     WHERE o.command='recover'
                       AND o.tracking_id=$TRACKING_ID
                     ORDER BY o.operation_id DESC LIMIT 1;")"
LATEST_DROP_ID="$(q "SELECT event_id FROM flashback.delta_log
                      WHERE tracking_id=$TRACKING_ID
                        AND event_type='DROP'
                      ORDER BY event_id DESC LIMIT 1;")"
[[ "$RECOVER_STATE" == "verified|$LATEST_DROP_ID" ]] \
    || die "audited recovery journal mismatch: state/event=$RECOVER_STATE expected verified|$LATEST_DROP_ID"
[[ "$(q "SELECT health FROM flashback_health()
             WHERE tracking_id=$TRACKING_ID;")" == healthy ]] \
    || die "successor coverage is not healthy after recover"
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
schema_dump_of_orders "$SCHEMA_DUMP_AFTER"
SCHEMA_DUMP_BEFORE_CANON="$WORK/orders-schema-before.acl-canon.sql"
SCHEMA_DUMP_AFTER_CANON="$WORK/orders-schema-after.acl-canon.sql"
canonicalize_acl_order "$SCHEMA_DUMP_BEFORE" "$SCHEMA_DUMP_BEFORE_CANON"
canonicalize_acl_order "$SCHEMA_DUMP_AFTER" "$SCHEMA_DUMP_AFTER_CANON"
if ! cmp -s "$SCHEMA_DUMP_BEFORE_CANON" "$SCHEMA_DUMP_AFTER_CANON"; then
    diff -u "$SCHEMA_DUMP_BEFORE_CANON" "$SCHEMA_DUMP_AFTER_CANON" \
        >"$WORK/orders-schema.diff" || true
    die "independent schema-only pg_dump manifest mismatch after ACL-order canonicalization (see $WORK/orders-schema.diff)"
fi
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
expect_sqlstate "UNIQUE constraint probe" 23505 \
    "INSERT INTO public.orders(id,parent_id,external_key,qty,note,payload,tags,tenant)
     VALUES (-9001,1,'key-2',1,'dup','{}','{}','smoke_app');"
expect_sqlstate "CHECK constraint probe" 23514 \
    "INSERT INTO public.orders(id,parent_id,external_key,qty,note,payload,tags,tenant)
     VALUES (-9002,1,'bad-check',101,'bad','{}','{}','smoke_app');"
expect_sqlstate "outgoing FK probe" 23503 \
    "SET CONSTRAINTS orders_parent_fk IMMEDIATE;
     INSERT INTO public.orders(id,parent_id,external_key,qty,note,payload,tags,tenant)
     VALUES (-9003,999999,'bad-fk',1,'bad','{}','{}','smoke_app');"
pass "PK, UNIQUE, CHECK, outgoing FK and secondary index are behaviorally valid"

[[ "$(q "SELECT relrowsecurity AND relforcerowsecurity
             FROM pg_class WHERE oid='public.orders'::regclass;")" == t ]] \
    || die "RLS/FORCE RLS state mismatch"
[[ "$(q "SELECT count(*)=1 FROM pg_policy
             WHERE polrelid='public.orders'::regclass
               AND polname='orders_tenant_policy';")" == t ]] \
    || die "RLS policy missing"
[[ "$(q "SELECT ARRAY(
                  SELECT r.rolname::text
                  FROM pg_policy p
                  JOIN pg_roles r ON r.oid=ANY(p.polroles)
                  WHERE p.polrelid='public.orders'::regclass
                    AND p.polname='orders_tenant_policy'
                  ORDER BY r.rolname
                ) = ARRAY['smoke Reporter','smoke_app'];")" == t ]] \
    || die "RLS multi-role identity mismatch"
RLS_COUNTS="$(q "SET ROLE smoke_app;
  SELECT count(*)||'|'||count(*) FILTER (WHERE tenant <> current_user)
  FROM public.orders;
  RESET ROLE;")"
[[ "${RLS_COUNTS##*$'\n'}" =~ ^[1-9][0-9]*\\|0$ ]] \
    || die "RLS behavioral filter mismatch: $RLS_COUNTS"
expect_sqlstate "RLS WITH CHECK probe" 42501 \
    "SET ROLE smoke_app;
     INSERT INTO public.orders(id,ticket_no,parent_id,external_key,qty,note,payload,tags,tenant)
     VALUES (-9004,-9004,1,'bad-rls',1,'bad','{}','{}','other');"
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
NEW_EDGE="$(q "INSERT INTO public.orders(
                 parent_id,external_key,qty,note,payload,tags,toast_payload,tenant
               )
               VALUES (1,'post-restore',1,'probe','{}','{}','trigger-probe','smoke_app')
               RETURNING id::text||'|'||ticket_no::text;")"
NEW_ID="${NEW_EDGE%%|*}"
NEW_TICKET="${NEW_EDGE##*|}"
[[ "$NEW_ID" == "$((OLD_MAX_ID + 2))" ]] \
    || die "identity sequence resumed at $NEW_ID, expected $((OLD_MAX_ID + 2))"
[[ "$NEW_TICKET" == "$((OLD_MIN_TICKET - 3))" ]] \
    || die "descending SERIAL sequence resumed at $NEW_TICKET, expected $((OLD_MIN_TICKET - 3))"
[[ "$(q "SELECT count(*) FROM public.smoke_trigger_audit;")" \
      == "$((AUDIT_BEFORE_PROBE + 1))" ]] \
    || die "restored trigger did not fire exactly once"
[[ "$(q "SELECT pg_get_serial_sequence('public.orders','id') IS NOT NULL;")" == t ]] \
    || die "identity sequence ownership missing"
[[ "$(q "SELECT s.seqstart=11 AND s.seqincrement=2
                  AND s.seqmin=1 AND s.seqmax=9223372036854775807
                  AND s.seqcache=1 AND NOT s.seqcycle
             FROM pg_sequence s
            WHERE s.seqrelid=to_regclass(
              pg_get_serial_sequence('public.orders','id')
            );")" == t ]] \
    || die "identity sequence options drifted"
[[ "$(q "SELECT s.seqstart=1000 AND s.seqincrement=(-3)
                  AND s.seqmin=(-2147483648) AND s.seqmax=1000
                  AND s.seqcache=1 AND NOT s.seqcycle
             FROM pg_sequence s
            WHERE s.seqrelid='public.orders_ticket_seq'::regclass;")" == t ]] \
    || die "descending SERIAL sequence options drifted"
[[ "$(q "SELECT tgenabled='A'
             FROM pg_trigger
            WHERE tgrelid='public.orders'::regclass
              AND tgname='orders_audit';")" == t ]] \
    || die "ENABLE ALWAYS trigger state drifted"
pass "identity/SERIAL edges, sequence options and trigger state survived recovery"

q "CREATE TABLE public.smoke_schema_drift(
     id serial PRIMARY KEY,
     payload text NOT NULL
   );
   INSERT INTO public.smoke_schema_drift(payload) VALUES ('baseline');"
"$CLI" protect public.smoke_schema_drift >/dev/null
DRIFT_TRACKING_ID="$(q "SELECT tracking_id FROM flashback.tracked_tables
                         WHERE is_active AND schema_name='public'
                           AND table_name='smoke_schema_drift';")"
q "ALTER SEQUENCE public.smoke_schema_drift_id_seq CACHE 7;
   COMMENT ON SEQUENCE public.smoke_schema_drift_id_seq
     IS 'post-protect metadata drift';"
expect_sqlstate "pre-DROP schema drift barrier" 55000 \
    "DROP TABLE public.smoke_schema_drift;"
[[ "$(q "SELECT to_regclass('public.smoke_schema_drift') IS NOT NULL;")" == t ]] \
    || die "schema-drift barrier did not preserve the live table"
[[ "$(q "SELECT count(*) FROM flashback.delta_log
             WHERE tracking_id=$DRIFT_TRACKING_ID
               AND event_type='DROP';")" == 0 ]] \
    || die "rejected schema-drift DROP left a committed disaster event"
pass "post-protect related-object metadata drift blocks DROP atomically"

# Prove the schema check is made after the final-strength lock, not before it.
# CREATE INDEX commits while DROP is waiting to upgrade SHARE→ACCESS EXCLUSIVE;
# DROP must then observe the newly committed metadata and fail closed.
q "CREATE TABLE public.smoke_drop_race(
     id integer PRIMARY KEY,
     payload text NOT NULL
   );
   INSERT INTO public.smoke_drop_race VALUES (1,'baseline');"
"$CLI" protect public.smoke_drop_race >/dev/null
DROP_RACE_TRACKING_ID="$(q "SELECT tracking_id FROM flashback.tracked_tables
                              WHERE is_active AND schema_name='public'
                                AND table_name='smoke_drop_race';")"
DROP_RACE_BARRIER="$WORK/drop-race-index-ready"
"$PG_BIN/psql" -X -v ON_ERROR_STOP=1 -q >"$WORK/drop-race-writer.log" 2>&1 <<SQL &
BEGIN;
CREATE INDEX smoke_drop_race_payload_idx
  ON public.smoke_drop_race(payload);
\! touch "$DROP_RACE_BARRIER"
SELECT pg_sleep(1);
COMMIT;
SQL
RACE_PID=$!
for _ in $(seq 1 200); do
    [[ -f "$DROP_RACE_BARRIER" ]] && break
    sleep 0.01
done
[[ -f "$DROP_RACE_BARRIER" ]] || die "DROP metadata race writer never reached barrier"
expect_sqlstate "post-lock pre-DROP schema race barrier" 55000 \
    "DROP TABLE public.smoke_drop_race;"
wait "$RACE_PID"
RACE_PID=""
[[ "$(q "SELECT to_regclass('public.smoke_drop_race') IS NOT NULL
                  AND to_regclass('public.smoke_drop_race_payload_idx') IS NOT NULL;")" == t ]] \
    || die "DROP schema-race barrier did not preserve table and committed index"
[[ "$(q "SELECT count(*) FROM flashback.delta_log
             WHERE tracking_id=$DROP_RACE_TRACKING_ID
               AND event_type='DROP';")" == 0 ]] \
    || die "rejected schema-race DROP left a committed disaster event"
pass "DROP waits for concurrent metadata DDL, then validates under ACCESS EXCLUSIVE"

# The live-table restore path has the same invariant: metadata may commit while
# recover waits for ACCESS EXCLUSIVE, but the swap must not start from the
# stale epoch after the lock is granted.
q "CREATE TABLE public.smoke_restore_race(
     id integer PRIMARY KEY,
     payload text NOT NULL
   );
   INSERT INTO public.smoke_restore_race VALUES (1,'baseline');"
"$CLI" protect public.smoke_restore_race >/dev/null
RESTORE_RACE_TRACKING_ID="$(q "SELECT tracking_id
                               FROM flashback.tracked_tables
                              WHERE is_active AND schema_name='public'
                                AND table_name='smoke_restore_race';")"
RESTORE_RACE_LSN="$(q "SELECT boundary_lsn
                        FROM flashback.coverage_generations
                       WHERE tracking_id=$RESTORE_RACE_TRACKING_ID
                         AND state='active';")"
q "UPDATE public.smoke_restore_race
      SET payload='changed'
    WHERE id=1;"
for _ in $(seq 1 300); do
    [[ "$(q "SELECT count(*) FROM flashback.delta_log
             WHERE tracking_id=$RESTORE_RACE_TRACKING_ID
               AND event_type='UPDATE';")" -ge 1 ]] && break
    sleep 0.05
done
RESTORE_RACE_BARRIER="$WORK/restore-race-index-ready"
"$PG_BIN/psql" -X -v ON_ERROR_STOP=1 -q >"$WORK/restore-race-writer.log" 2>&1 <<SQL &
BEGIN;
CREATE INDEX smoke_restore_race_payload_idx
  ON public.smoke_restore_race(payload);
\! touch "$RESTORE_RACE_BARRIER"
SELECT pg_sleep(1);
COMMIT;
SQL
RACE_PID=$!
for _ in $(seq 1 200); do
    [[ -f "$RESTORE_RACE_BARRIER" ]] && break
    sleep 0.01
done
[[ -f "$RESTORE_RACE_BARRIER" ]] || die "restore metadata race writer never reached barrier"
expect_sqlstate "post-lock live-restore schema race barrier" 55000 \
    "PERFORM set_config('pg_flashback.allow_unaudited_restore','on',true);
     PERFORM flashback_restore_lsn(
       'public.smoke_restore_race',
       '$RESTORE_RACE_LSN'::pg_lsn
     );"
wait "$RACE_PID"
RACE_PID=""
[[ "$(q "SELECT payload FROM public.smoke_restore_race WHERE id=1;")" == changed ]] \
    || die "failed schema-race restore mutated live data"
[[ "$(q "SELECT to_regclass('public.smoke_restore_race_payload_idx') IS NOT NULL;")" == t ]] \
    || die "failed schema-race restore lost the concurrently committed index"
pass "live restore validates schema only after ACCESS EXCLUSIVE"

q "CREATE TABLE public.smoke_in_parent(id integer PRIMARY KEY);
   CREATE TABLE public.smoke_in_child(
     id integer PRIMARY KEY,
     parent_id integer REFERENCES public.smoke_in_parent(id)
   );
   CREATE TABLE public.smoke_part(id integer, d date) PARTITION BY RANGE(d);
   CREATE TABLE public.smoke_part_p PARTITION OF public.smoke_part
     FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
   CREATE TABLE public.smoke_storage(id integer PRIMARY KEY, payload text);
   ALTER TABLE public.smoke_storage ALTER COLUMN payload SET STORAGE EXTERNAL;
   CREATE TABLE public.smoke_compression(id integer PRIMARY KEY, payload text);
   ALTER TABLE public.smoke_compression ALTER COLUMN payload SET COMPRESSION pglz;
   CREATE TABLE public.smoke_seq_meta(id serial PRIMARY KEY);
   COMMENT ON SEQUENCE public.smoke_seq_meta_id_seq IS 'custom metadata';
   CREATE TABLE public.smoke_seq_acl(id serial PRIMARY KEY);
   GRANT USAGE ON SEQUENCE public.smoke_seq_acl_id_seq TO smoke_reader;
   CREATE SEQUENCE public.smoke_external_seq;
   CREATE TABLE public.smoke_ext_seq(
     id bigint PRIMARY KEY
       DEFAULT nextval('public.smoke_external_seq'::regclass)
   );"
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
[[ "$(q "SELECT flashback_local_compatibility(
                    'public.smoke_storage'::regclass
                  )->'rejected_features' @> '[\"column_storage_or_compression\"]'::jsonb;")" == t ]] \
    || die "custom column storage was not rejected"
[[ "$(q "SELECT flashback_local_compatibility(
                    'public.smoke_compression'::regclass
                  )->'rejected_features' @> '[\"column_storage_or_compression\"]'::jsonb;")" == t ]] \
    || die "custom column compression was not rejected"
[[ "$(q "SELECT flashback_local_compatibility(
                    'public.smoke_seq_meta'::regclass
                  )->'rejected_features' @> '[\"custom_owned_sequence_metadata\"]'::jsonb;")" == t ]] \
    || die "custom owned-sequence metadata was not rejected"
[[ "$(q "SELECT flashback_local_compatibility(
                    'public.smoke_seq_acl'::regclass
                  )->'rejected_features' @> '[\"custom_owned_sequence_metadata\"]'::jsonb;")" == t ]] \
    || die "custom owned-sequence ACL was not rejected"
[[ "$(q "SELECT flashback_local_compatibility(
                    'public.smoke_ext_seq'::regclass
                  )->'rejected_features' @> '[\"external_sequence_default\"]'::jsonb;")" == t ]] \
    || die "external sequence default was not rejected"
if "$CLI" protect public.smoke_in_parent >/dev/null 2>&1; then
    die "CLI protect accepted a table with incoming foreign keys"
fi
if "$CLI" protect public.smoke_part >/dev/null 2>&1; then
    die "CLI protect accepted a partitioned table"
fi
if "$CLI" protect public.smoke_storage >/dev/null 2>&1; then
    die "CLI protect accepted custom column storage"
fi
if "$CLI" protect public.smoke_compression >/dev/null 2>&1; then
    die "CLI protect accepted custom column compression"
fi
if "$CLI" protect public.smoke_seq_meta >/dev/null 2>&1; then
    die "CLI protect accepted custom sequence metadata"
fi
if "$CLI" protect public.smoke_seq_acl >/dev/null 2>&1; then
    die "CLI protect accepted custom sequence ACL"
fi
if "$CLI" protect public.smoke_ext_seq >/dev/null 2>&1; then
    die "CLI protect accepted an external sequence default"
fi
[[ "$(q "SELECT count(*) FROM flashback.tracked_tables
             WHERE schema_name='public'
               AND table_name IN (
                 'smoke_in_parent','smoke_part','smoke_storage',
                 'smoke_compression','smoke_seq_meta','smoke_seq_acl',
                 'smoke_ext_seq'
               );")" == 0 ]] \
    || die "unsupported relation left a tracking lifecycle"
pass "unsupported FK/topology/storage/sequence metadata fail closed without residue"

RUN_COMPLETE=1
log "PASS: local-only clean-host smoke ($PASSED assertions)"
