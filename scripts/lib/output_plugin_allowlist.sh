#!/usr/bin/env bash
# Version-tolerant compatibility policy for PostgreSQL's output_plugin_libraries
# GUC. Current security-patched PostgreSQL minors (across every supported
# major) added this allowlist GUC; a real logical replication slot using the
# pg_flashback output plugin fails to be created without an explicit entry:
#
#   library "pg_flashback" may not be used as an output plugin
#
# Older PostgreSQL minors do not have this GUC at all. This library never
# hardcodes a version threshold: it probes the actual selected postgres
# binary/instance for the GUC's existence and only ever adds pg_flashback
# when the probe proves the GUC exists.
#
# output_plugin_libraries is a LIST GUC. A host may already allowlist other
# logical output plugins (decoderbufs, wal2json, test_decoding, ...). This
# library always treats the value as a comma-separated list: it preserves
# every existing entry byte-for-byte, adds pg_flashback exactly once if
# missing, and never emits '*' or drops another plugin's entry. It never
# touches authentication and never silently swallows an unrelated
# unrecognized-configuration-parameter/probe error.
#
# Sourced by every script that initdb's a real cluster and creates/uses the
# pg_flashback logical output plugin. Requires bash 4+. Callers must set
# REPO_ROOT before sourcing, or leave it unset so this library resolves it
# from this file's location.
#
# Public entrypoints:
#   opal_output_plugin_libraries_supported <pg_bin_dir> <data_dir>
#       Returns 0 if the target postgres binary recognizes
#       output_plugin_libraries, 1 if it genuinely does not exist on this
#       minor. Any other failure (bad data dir, unrelated config error) is
#       propagated as a hard error (exit >1 semantics via `return 2` and a
#       message on stderr), never silently treated as "unsupported".
#   opal_configure_postgresql_conf <pg_bin_dir> <data_dir> [conf_file]
#       Probes the EFFECTIVE configured value (via `postgres -C`, which
#       already reflects any prior output_plugin_libraries line(s) in
#       conf_file -- this is not a blind grep) and, only when the GUC is
#       supported and pg_flashback is not already in that list, appends one
#       new line to conf_file (default: <data_dir>/postgresql.conf):
#         output_plugin_libraries = '<preserved-entries>, pg_flashback'
#       (or just 'pg_flashback' if the effective value was empty). Because
#       postgresql.conf is last-assignment-wins, this new line becomes the
#       effective value without disturbing whatever earlier line(s) set it.
#       Call this after initdb and before pg_ctl start. Idempotent: a
#       second call against the same conf_file/instance state is a no-op
#       (the probe already observes pg_flashback in the merged value).
#       Prints its decision (and why) to stderr so harness logs show
#       whether the compatibility line was applied, merged, or skipped as
#       not-applicable.
#
#   opal_live_ensure_output_plugin_libraries <psql_invocation...>
#       For scripts that reuse an already-running, already-initdb'd instance
#       (e.g. a persistent cargo-pgrx dev instance) instead of writing
#       postgresql.conf pre-start. <psql_invocation...> must be a full psql
#       command line (already carrying -h/-p/-d/etc) able to run -qAtc
#       against a database where the connecting role can ALTER SYSTEM; each
#       invocation of "$@" is its own fresh psql process/connection.
#       output_plugin_libraries has GUC context=superuser (verified against
#       real PostgreSQL 15.19/16.15/17.11/18.6 installs): a new value takes
#       effect for new backends after pg_reload_conf(), no restart required
#       -- this function therefore reloads and verifies inline rather than
#       asking the caller to restart. Reads the current list, merges
#       pg_flashback into it (never replacing it with a bare 'pg_flashback'
#       when other entries exist), issues ALTER SYSTEM SET with the merged
#       list, reloads, and re-verifies with a fresh connection that
#       pg_flashback is now effective. Not applicable (older minor) and
#       already-correct are both silent no-ops. Fails closed (non-zero
#       return, message on stderr) if the ALTER SYSTEM, reload, or
#       post-reload verification does not succeed -- it never reports
#       success, and never reports a misleading "restart required", when a
#       reload was already sufficient and verified effective.

if [[ -z "${REPO_ROOT:-}" ]]; then
    REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
fi

opal_die() {
    echo "FAIL[output-plugin-allowlist]: $*" >&2
    return 1
}

# Internal: trim leading/trailing whitespace.
opal_trim() {
    local s="$1"
    s="${s#"${s%%[![:space:]]*}"}"
    s="${s%"${s##*[![:space:]]}"}"
    printf '%s' "$s"
}

# Internal: true (rc 0) if comma-separated list $1 contains element $2 as an
# exact match after per-element whitespace trimming. Empty tokens (from
# leading/trailing/double commas) are ignored, never matched.
opal_plugin_list_contains() {
    local list=$1 name=$2 tok
    local IFS=','
    for tok in $list; do
        tok="$(opal_trim "$tok")"
        [[ -n "$tok" && "$tok" == "$name" ]] && return 0
    done
    return 1
}

# Internal: print $1 (a comma-separated list, possibly empty) with $2
# appended exactly once, one element per line -- preserving every existing
# element byte-for-byte (only surrounding whitespace is trimmed per
# element) and its order. Never introduces '*'. Idempotent: if $2 is
# already present, the input elements are reprinted unchanged and $2 is
# not duplicated.
opal_merge_plugin_elements() {
    local list=$1 add=$2
    local -a tokens=()
    local IFS=','
    local tok trimmed already=0 t
    for tok in $list; do
        trimmed="$(opal_trim "$tok")"
        [[ -n "$trimmed" ]] || continue
        tokens+=("$trimmed")
    done
    for t in "${tokens[@]}"; do
        [[ "$t" == "$add" ]] && { already=1; break; }
    done
    [[ "$already" == 1 ]] || tokens+=("$add")
    printf '%s\n' "${tokens[@]}"
}

# Internal: comma-space-joined form of opal_merge_plugin_elements, suitable
# for a single postgresql.conf line value (PostgreSQL's own conf-file GUC
# list parser splits top-level commas correctly -- verified directly).
opal_merge_plugin_list() {
    local list=$1 add=$2
    local out="" first=1 line
    while IFS= read -r line; do
        [[ -z "$line" ]] && continue
        if [[ "$first" == 1 ]]; then out="$line"; first=0; else out="$out, $line"; fi
    done < <(opal_merge_plugin_elements "$list" "$add")
    printf '%s' "$out"
}

# Internal: SQL value-list form of opal_merge_plugin_elements, e.g.
# 'decoderbufs', 'wal2json', 'pg_flashback' -- for ALTER SYSTEM SET.
# ALTER SYSTEM SET's own GUC_LIST_QUOTE serialization stores a single
# comma-joined SQL string argument (e.g. SET x = 'a, b') as ONE quoted
# list element in postgresql.auto.conf, not three (verified directly
# against a real PostgreSQL 17.11 instance: `ALTER SYSTEM SET
# shared_preload_libraries = 'a, b'` round-trips as the single element
# "a, b", silently losing the second library). Passing each element as
# its own separately-quoted SQL value -- 'a', 'b' -- is the form that
# round-trips correctly. Each element is also SQL-quote-escaped (doubled
# single quotes) as a defensive measure.
opal_merge_plugin_sql_values() {
    local list=$1 add=$2
    local out="" first=1 line escaped
    while IFS= read -r line; do
        [[ -z "$line" ]] && continue
        escaped="${line//\'/\'\'}"
        if [[ "$first" == 1 ]]; then out="'$escaped'"; first=0; else out="$out, '$escaped'"; fi
    done < <(opal_merge_plugin_elements "$list" "$add")
    printf '%s' "$out"
}

# Internal: probe the postgres binary's effective output_plugin_libraries.
# rc 0: prints the effective value (possibly empty string) on stdout.
# rc 1: the GUC does not exist on this minor (not an error).
# rc 2: any other failure (bad data dir, unrelated startup error); a
#       message is printed via opal_die and the caller must propagate it.
opal_output_plugin_libraries_probe() {
    local pg_bin=$1 data_dir=$2
    [[ -n "$pg_bin" && -x "$pg_bin/postgres" ]] || { opal_die "opal_output_plugin_libraries_probe: invalid pg_bin '$pg_bin'"; return 2; }
    [[ -n "$data_dir" && -d "$data_dir" ]] || { opal_die "opal_output_plugin_libraries_probe: invalid data_dir '$data_dir'"; return 2; }

    local out rc
    out="$("$pg_bin/postgres" -D "$data_dir" -C output_plugin_libraries 2>&1)"
    rc=$?

    if [[ $rc -eq 0 ]]; then
        printf '%s' "$out"
        return 0
    fi

    if [[ "$out" == *'unrecognized configuration parameter "output_plugin_libraries"'* ]]; then
        # Older minor: the GUC genuinely does not exist. Not an error.
        return 1
    fi

    # Any other failure (bad data dir, unrelated startup error, permission
    # problem, etc.) must never be silently reinterpreted as "unsupported".
    opal_die "opal_output_plugin_libraries_probe: probe failed for an unrelated reason (rc=$rc): $out"
    return 2
}

opal_output_plugin_libraries_supported() {
    opal_output_plugin_libraries_probe "$1" "$2" > /dev/null
}

opal_configure_postgresql_conf() {
    local pg_bin=$1 data_dir=$2 conf_file=${3:-$2/postgresql.conf}
    local effective probe_rc merged

    [[ -f "$conf_file" ]] || { opal_die "opal_configure_postgresql_conf: conf file not found: $conf_file"; return 2; }

    effective="$(opal_output_plugin_libraries_probe "$pg_bin" "$data_dir")"
    probe_rc=$?
    case "$probe_rc" in
        1)
            echo "opal: output_plugin_libraries is not present on this PostgreSQL minor ($("$pg_bin/postgres" --version)); skipping (older-minor compatibility)" >&2
            return 0
            ;;
        2)
            return 2
            ;;
    esac

    if opal_plugin_list_contains "$effective" "pg_flashback"; then
        echo "opal: output_plugin_libraries already allows pg_flashback (effective: ${effective:-<empty>}); leaving $conf_file as-is" >&2
        return 0
    fi

    merged="$(opal_merge_plugin_list "$effective" "pg_flashback")"
    printf '%s\n' "output_plugin_libraries = '${merged}'" >> "$conf_file"
    echo "opal: output_plugin_libraries is supported by $("$pg_bin/postgres" --version); merged effective value (${effective:-<empty>}) with pg_flashback -> '$merged' in $conf_file" >&2
}

opal_live_ensure_output_plugin_libraries() {
    local is_null current merged verify_is_null verify_current

    is_null="$("$@" -qAtc "SELECT (current_setting('output_plugin_libraries', true) IS NULL)::text")" \
        || { opal_die "opal_live_ensure_output_plugin_libraries: could not probe current_setting"; return 2; }
    if [[ "$is_null" == "t" ]]; then
        echo "opal: output_plugin_libraries is not present on this PostgreSQL minor; skipping (older-minor compatibility)" >&2
        return 0
    fi

    current="$("$@" -qAtc "SELECT current_setting('output_plugin_libraries')")" \
        || { opal_die "opal_live_ensure_output_plugin_libraries: could not read current_setting"; return 2; }
    if opal_plugin_list_contains "$current" "pg_flashback"; then
        echo "opal: output_plugin_libraries already allows pg_flashback (observed: $current)" >&2
        return 0
    fi

    merged="$(opal_merge_plugin_list "$current" "pg_flashback")"
    local sql_values
    sql_values="$(opal_merge_plugin_sql_values "$current" "pg_flashback")"

    # Deliberately NOT `= '${merged}'` (one quoted SQL string): ALTER
    # SYSTEM SET's GUC_LIST_QUOTE serialization would store that as a
    # single quoted list element, silently collapsing every plugin (see
    # opal_merge_plugin_sql_values). Each element must be its own SQL
    # value.
    "$@" -qAtc "ALTER SYSTEM SET output_plugin_libraries = ${sql_values}" > /dev/null \
        || { opal_die "opal_live_ensure_output_plugin_libraries: ALTER SYSTEM SET failed"; return 2; }
    "$@" -qAtc "SELECT pg_reload_conf()" > /dev/null \
        || { opal_die "opal_live_ensure_output_plugin_libraries: pg_reload_conf() failed"; return 2; }

    # Verify with fresh connections (each "$@" invocation is its own new
    # psql process), not the pre-reload value captured above: a reload that
    # silently failed to apply (e.g. a syntax problem PostgreSQL only
    # reports asynchronously) must not be reported as success.
    verify_is_null="$("$@" -qAtc "SELECT (current_setting('output_plugin_libraries', true) IS NULL)::text")" \
        || { opal_die "opal_live_ensure_output_plugin_libraries: post-reload verification connection failed"; return 2; }
    if [[ "$verify_is_null" == "t" ]]; then
        opal_die "opal_live_ensure_output_plugin_libraries: output_plugin_libraries reports unset after reload (was applicable before ALTER SYSTEM SET)"
        return 2
    fi
    verify_current="$("$@" -qAtc "SELECT current_setting('output_plugin_libraries')")" \
        || { opal_die "opal_live_ensure_output_plugin_libraries: post-reload verification read failed"; return 2; }
    if ! opal_plugin_list_contains "$verify_current" "pg_flashback"; then
        opal_die "opal_live_ensure_output_plugin_libraries: verification after reload did not observe pg_flashback (observed: $verify_current)"
        return 2
    fi

    echo "opal: merged output_plugin_libraries '$current' -> '$merged', reloaded, and verified pg_flashback is effective (no restart required)" >&2
}
