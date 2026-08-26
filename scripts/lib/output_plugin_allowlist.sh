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
# binary for the GUC's existence and only ever appends the exact required
# line when the probe proves the GUC exists. It never sets '*', never
# touches authentication, and never silently swallows an unrelated
# unrecognized-configuration-parameter error.
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
#       Probes the target binary and, only when supported, appends exactly
#       one line to conf_file (default: <data_dir>/postgresql.conf):
#         output_plugin_libraries = 'pg_flashback'
#       Call this after initdb and before pg_ctl start (before or after any
#       other postgresql.conf lines -- GUC order does not matter here since
#       only one output_plugin_libraries line is ever written). Idempotent
#       guard: refuses to double-append if the line is already present.
#       Prints its decision (and why) to stderr so harness logs show whether
#       the compatibility line was applied or skipped as not-applicable.
#
#   opal_live_ensure_output_plugin_libraries <psql_invocation...>
#       For scripts that reuse an already-running, already-initdb'd instance
#       (e.g. a persistent cargo-pgrx dev instance) instead of writing
#       postgresql.conf pre-start. <psql_invocation...> must be a full psql
#       command line (already carrying -h/-p/-d/etc) able to run -qAtc
#       against a database where the connecting role can ALTER SYSTEM.
#       output_plugin_libraries has GUC context=superuser (verified against
#       real PostgreSQL 15.19/16.15/17.11/18.6 installs): a new value takes
#       effect for new backends after pg_reload_conf(), no restart required.
#       Not applicable (older minor) and already-correct are both silent
#       no-ops on stdout; prints "restart_required" on stdout only when it
#       just issued ALTER SYSTEM SET (callers that always restart/reload
#       afterward for other GUCs can ignore this; callers that do not must
#       call pg_reload_conf() before creating any pg_flashback slot).

if [[ -z "${REPO_ROOT:-}" ]]; then
    REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
fi

opal_die() {
    echo "FAIL[output-plugin-allowlist]: $*" >&2
    return 1
}

opal_output_plugin_libraries_supported() {
    local pg_bin=$1 data_dir=$2
    [[ -n "$pg_bin" && -x "$pg_bin/postgres" ]] || { opal_die "opal_output_plugin_libraries_supported: invalid pg_bin '$pg_bin'"; return 2; }
    [[ -n "$data_dir" && -d "$data_dir" ]] || { opal_die "opal_output_plugin_libraries_supported: invalid data_dir '$data_dir'"; return 2; }

    local out rc
    out="$("$pg_bin/postgres" -D "$data_dir" -C output_plugin_libraries 2>&1)"
    rc=$?

    if [[ $rc -eq 0 ]]; then
        return 0
    fi

    if [[ "$out" == *'unrecognized configuration parameter "output_plugin_libraries"'* ]]; then
        # Older minor: the GUC genuinely does not exist. Not an error.
        return 1
    fi

    # Any other failure (bad data dir, unrelated startup error, permission
    # problem, etc.) must never be silently reinterpreted as "unsupported".
    opal_die "opal_output_plugin_libraries_supported: probe failed for an unrelated reason (rc=$rc): $out"
    return 2
}

opal_configure_postgresql_conf() {
    local pg_bin=$1 data_dir=$2 conf_file=${3:-$2/postgresql.conf}
    local supported_rc

    [[ -f "$conf_file" ]] || { opal_die "opal_configure_postgresql_conf: conf file not found: $conf_file"; return 2; }

    if grep -Eq '^[[:space:]]*output_plugin_libraries[[:space:]]*=' "$conf_file"; then
        echo "opal: output_plugin_libraries already present in $conf_file; leaving as-is" >&2
        return 0
    fi

    opal_output_plugin_libraries_supported "$pg_bin" "$data_dir"
    supported_rc=$?
    case "$supported_rc" in
        0)
            printf '%s\n' "output_plugin_libraries = 'pg_flashback'" >> "$conf_file"
            echo "opal: output_plugin_libraries is supported by $("$pg_bin/postgres" --version); appended allowlist line to $conf_file" >&2
            ;;
        1)
            echo "opal: output_plugin_libraries is not present on this PostgreSQL minor ($("$pg_bin/postgres" --version)); skipping (older-minor compatibility)" >&2
            ;;
        *)
            return 2
            ;;
    esac
}

opal_live_ensure_output_plugin_libraries() {
    local is_null current

    is_null="$("$@" -qAtc "SELECT (current_setting('output_plugin_libraries', true) IS NULL)::text")"
    if [[ "$is_null" == "t" ]]; then
        echo "opal: output_plugin_libraries is not present on this PostgreSQL minor; skipping (older-minor compatibility)" >&2
        return 0
    fi

    current="$("$@" -qAtc "SELECT current_setting('output_plugin_libraries')")"
    if [[ "$current" =~ (^|,)[[:space:]]*pg_flashback[[:space:]]*(,|$) ]]; then
        echo "opal: output_plugin_libraries already allows pg_flashback (observed: $current)" >&2
        return 0
    fi

    "$@" -qAtc "ALTER SYSTEM SET output_plugin_libraries = 'pg_flashback'" > /dev/null
    echo "opal: issued ALTER SYSTEM SET output_plugin_libraries = 'pg_flashback' (was: $current)" >&2
    echo "restart_required"
}
