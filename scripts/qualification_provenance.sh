#!/usr/bin/env bash

# Shared exact-source provenance for development and release qualification.
# Call qualification_provenance_init with repository root and pg_config path.

qualification_sha256_or_null() {
    local path=$1
    if [[ -f "$path" ]]; then
        printf '"%s"' "$(sha256sum "$path" | awk '{print $1}')"
    else
        printf 'null'
    fi
}

qualification_provenance_init() {
    local root=$1
    local pg_config=$2
    QUALIFICATION_STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    QUALIFICATION_TESTED_COMMIT="$(git -C "$root" rev-parse HEAD)"
    QUALIFICATION_BRANCH="$(git -C "$root" branch --show-current)"
    QUALIFICATION_EXACT_TAG="$(git -C "$root" tag --points-at HEAD | paste -sd, -)"
    if [[ -z "$(git -C "$root" status --porcelain=v1)" ]]; then
        QUALIFICATION_TREE_CLEAN=true
    else
        QUALIFICATION_TREE_CLEAN=false
    fi
    if [[ "${PG_FLASHBACK_REQUIRE_CLEAN_TREE:-0}" == "1" &&
          "$QUALIFICATION_TREE_CLEAN" != true ]]; then
        echo "FAIL: release qualification requires a clean source tree" >&2
        return 1
    fi
    QUALIFICATION_POSTGRES_VERSION="$("$pg_config" --version)"
    QUALIFICATION_EXTENSION_SHA256="$(
        qualification_sha256_or_null "$("$pg_config" --pkglibdir)/pg_flashback.so"
    )"
    QUALIFICATION_HELPER_SHA256="$(
        qualification_sha256_or_null \
            "$root/tools/pg_flashback_recovery/target/release/pg-flashback-recovery"
    )"
}

qualification_provenance_json() {
    local finished_at=$1
    cat <<EOF
  "provenance": {
    "tested_commit": "$QUALIFICATION_TESTED_COMMIT",
    "tree_clean": $QUALIFICATION_TREE_CLEAN,
    "branch": "$QUALIFICATION_BRANCH",
    "exact_tag": "$QUALIFICATION_EXACT_TAG",
    "postgresql_version": "$QUALIFICATION_POSTGRES_VERSION",
    "extension_binary_sha256": $QUALIFICATION_EXTENSION_SHA256,
    "helper_release_binary_sha256": $QUALIFICATION_HELPER_SHA256,
    "started_at": "$QUALIFICATION_STARTED_AT",
    "finished_at": "$finished_at"
  }
EOF
}
