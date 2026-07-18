#!/usr/bin/env bash
# Exact-candidate identity and installation primitives.
#
# Sourced by packaged qualification suites. Never cargo-builds extension/helper.
# Requires bash 4+. Callers must set REPO_ROOT before sourcing, or leave it unset
# so this library resolves it from this file's location.
#
# Public entrypoints:
#   exact_candidate_bind_dir <CANDIDATE_DIR>
#   exact_candidate_install_into_prefix
#   exact_candidate_verify_installed
#   exact_candidate_verify_end_state
#   exact_candidate_restore_prefix
#   exact_candidate_sha256 <path>
#   exact_candidate_free_bytes <path>
#   exact_candidate_monotonic_now_ns

if [[ -z "${REPO_ROOT:-}" ]]; then
    REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
fi

exact_candidate_sha256() {
    sha256sum "$1" | awk '{print $1}'
}

exact_candidate_free_bytes() {
    local path=${1:-.}
    df -B1 --output=avail "$path" | awk 'NR==2 {print $1}'
}

exact_candidate_monotonic_now_ns() {
    # Prefer CLOCK_MONOTONIC via python for suspension-aware duration.
    python3 - <<'PY'
import time
print(int(time.monotonic_ns()))
PY
}

exact_candidate_die() {
    echo "FAIL[exact-candidate-identity]: $*" >&2
    return 1
}

exact_candidate_bind_dir() {
    local candidate_dir=$1
    [[ -d "$candidate_dir" ]] || exact_candidate_die "CANDIDATE_DIR missing: $candidate_dir" || return 1
    CANDIDATE_DIR="$(cd "$candidate_dir" && pwd)"
    MANIFEST="$CANDIDATE_DIR/MANIFEST.json"
    [[ -f "$MANIFEST" ]] || exact_candidate_die "MANIFEST.json missing in $CANDIDATE_DIR" || return 1
    [[ -f "$CANDIDATE_DIR/SHA256SUMS" ]] || exact_candidate_die "SHA256SUMS missing" || return 1

    EC_SOURCE_COMMIT="$(jq -r '.provenance.source_commit' "$MANIFEST")"
    EC_SOURCE_TREE="$(jq -r '.provenance.source_tree' "$MANIFEST")"
    EC_PG_MAJOR="$(jq -r '.provenance.pg_major' "$MANIFEST")"
    EC_ARCH="$(jq -r '.provenance.arch' "$MANIFEST")"
    EC_PACKAGE_SHA="$(jq -r '.artifacts.package_sha256' "$MANIFEST")"
    EC_EXT_ARCHIVE="$(jq -r '.artifacts.extension_archive.name' "$MANIFEST")"
    EC_HELPER_ARCHIVE="$(jq -r '.artifacts.helper_archive.name' "$MANIFEST")"
    EC_SRC_ARCHIVE="$(jq -r '.artifacts.source_archive.name' "$MANIFEST")"
    EC_EXT_BIN_SHA="$(jq -r '.artifacts.extension_binary_sha256' "$MANIFEST")"
    EC_HELPER_BIN_SHA="$(jq -r '.artifacts.helper_binary_sha256' "$MANIFEST")"
    EC_SRC_SHA="$(jq -r '.artifacts.source_archive.sha256' "$MANIFEST")"
    EC_EXT_ARCHIVE_SHA="$(jq -r '.artifacts.extension_archive.sha256' "$MANIFEST")"
    EC_HELPER_ARCHIVE_SHA="$(jq -r '.artifacts.helper_archive.sha256' "$MANIFEST")"

    [[ -n "$EC_SOURCE_COMMIT" && "$EC_SOURCE_COMMIT" != null ]] || exact_candidate_die "manifest source_commit missing" || return 1
    [[ -f "$CANDIDATE_DIR/$EC_EXT_ARCHIVE" ]] || exact_candidate_die "missing $EC_EXT_ARCHIVE" || return 1
    [[ -f "$CANDIDATE_DIR/$EC_HELPER_ARCHIVE" ]] || exact_candidate_die "missing $EC_HELPER_ARCHIVE" || return 1
    [[ -f "$CANDIDATE_DIR/$EC_SRC_ARCHIVE" ]] || exact_candidate_die "missing $EC_SRC_ARCHIVE" || return 1

    # Verify archive digests before any extract/install.
    (
        cd "$CANDIDATE_DIR"
        sha256sum -c SHA256SUMS >/dev/null
        echo "$EC_PACKAGE_SHA  $EC_EXT_ARCHIVE" | sha256sum -c - >/dev/null
        echo "$EC_SRC_SHA  $EC_SRC_ARCHIVE" | sha256sum -c - >/dev/null
        echo "$EC_EXT_ARCHIVE_SHA  $EC_EXT_ARCHIVE" | sha256sum -c - >/dev/null
        echo "$EC_HELPER_ARCHIVE_SHA  $EC_HELPER_ARCHIVE" | sha256sum -c - >/dev/null
    ) || { exact_candidate_die "archive SHA-256 verification failed"; return 1; }

    # Source HEAD/tree must match the packaged candidate.
    local head tree dirty host_arch pg_major
    head="$(git -C "$REPO_ROOT" rev-parse HEAD)"
    tree="$(git -C "$REPO_ROOT" rev-parse 'HEAD^{tree}')"
    dirty="$(git -C "$REPO_ROOT" status --porcelain=v1)"
    [[ -z "$dirty" ]] || exact_candidate_die "source tree is dirty; refuse candidate qualification" || return 1
    [[ "$head" == "$EC_SOURCE_COMMIT" ]] || exact_candidate_die "HEAD $head != manifest source_commit $EC_SOURCE_COMMIT" || return 1
    [[ "$tree" == "$EC_SOURCE_TREE" ]] || exact_candidate_die "HEAD tree $tree != manifest source_tree $EC_SOURCE_TREE" || return 1

    host_arch="$(uname -m)"
    case "$host_arch" in
        x86_64|amd64) host_arch=x86_64 ;;
        aarch64|arm64) host_arch=aarch64 ;;
    esac
    [[ "$host_arch" == "$EC_ARCH" ]] || exact_candidate_die "host arch $host_arch != manifest arch $EC_ARCH" || return 1

    PG_BIN="${PG_BIN:-/usr/local/pgsql-${EC_PG_MAJOR}/bin}"
    [[ -x "$PG_BIN/pg_config" ]] || exact_candidate_die "pg_config missing under $PG_BIN" || return 1
    pg_major="$("$PG_BIN/pg_config" --version | awk '{print $2}' | cut -d. -f1)"
    [[ "$pg_major" == "$EC_PG_MAJOR" ]] || exact_candidate_die "PG major $pg_major != manifest $EC_PG_MAJOR" || return 1

    EC_PKGLIB="$("$PG_BIN/pg_config" --pkglibdir)"
    EC_SHARE_EXT="$("$PG_BIN/pg_config" --sharedir)/extension"
    EC_STASH_DIR="${EC_STASH_DIR:-$REPO_ROOT/target/exact-candidate-prefix-stash/$$}"
    EC_EXTRACT_DIR="${EC_EXTRACT_DIR:-$REPO_ROOT/target/exact-candidate-extract/$$}"
    mkdir -p "$EC_STASH_DIR" "$EC_EXTRACT_DIR/ext" "$EC_EXTRACT_DIR/helper"

    # Refuse unsafe shared-prefix alteration when foreign postgres holds the .so.
    if [[ -f "$EC_PKGLIB/pg_flashback.so" ]] && command -v lsof >/dev/null 2>&1; then
        if lsof "$EC_PKGLIB/pg_flashback.so" 2>/dev/null | awk 'NR>1 {print}' | grep -q .; then
            if [[ "${EXACT_CANDIDATE_FORCE_PREFIX:-0}" != "1" ]]; then
                exact_candidate_die "pg_flashback.so is open by another process; refuse prefix install (set EXACT_CANDIDATE_FORCE_PREFIX=1 only if isolated)"
                return 1
            fi
        fi
    fi

    # Extract archives only (no cargo).
    tar -C "$EC_EXTRACT_DIR/ext" -xzf "$CANDIDATE_DIR/$EC_EXT_ARCHIVE"
    tar -C "$EC_EXTRACT_DIR/helper" -xzf "$CANDIDATE_DIR/$EC_HELPER_ARCHIVE"
    EC_EXT_ROOT="$(find "$EC_EXTRACT_DIR/ext" -maxdepth 1 -type d -name 'pg_flashback-candidate-*' -print -quit)"
    EC_HELPER_ROOT="$(find "$EC_EXTRACT_DIR/helper" -maxdepth 1 -type d -name 'pg-flashback-recovery-candidate-*' -print -quit)"
    [[ -n "$EC_EXT_ROOT" && -n "$EC_HELPER_ROOT" ]] || exact_candidate_die "unexpected archive layout" || return 1
    EC_HELPER_BIN="$EC_HELPER_ROOT/bin/pg-flashback-recovery"
    [[ -x "$EC_HELPER_BIN" ]] || exact_candidate_die "helper binary missing in archive" || return 1
    [[ "$(cat "$EC_EXT_ROOT/PG_MAJOR")" == "$EC_PG_MAJOR" ]] || exact_candidate_die "archive PG_MAJOR mismatch" || return 1

    local got_ext got_helper
    got_ext="$(exact_candidate_sha256 "$EC_EXT_ROOT/lib/pg_flashback.so")"
    got_helper="$(exact_candidate_sha256 "$EC_HELPER_BIN")"
    [[ "$got_ext" == "$EC_EXT_BIN_SHA" ]] || exact_candidate_die "extracted extension SHA $got_ext != manifest $EC_EXT_BIN_SHA" || return 1
    [[ "$got_helper" == "$EC_HELPER_BIN_SHA" ]] || exact_candidate_die "extracted helper SHA $got_helper != manifest $EC_HELPER_BIN_SHA" || return 1

    EC_BOUND=1
    return 0
}

exact_candidate_stash_prefix_file() {
    local src=$1 rel=$2
    if [[ -e "$src" || -L "$src" ]]; then
        mkdir -p "$(dirname "$EC_STASH_DIR/$rel")"
        cp -a -- "$src" "$EC_STASH_DIR/$rel"
        exact_candidate_sha256 "$src" > "$EC_STASH_DIR/${rel}.sha256"
        printf 'present\n' > "$EC_STASH_DIR/${rel}.state"
    else
        printf 'absent\n' > "$EC_STASH_DIR/${rel}.state"
    fi
}

exact_candidate_install_into_prefix() {
    [[ "${EC_BOUND:-0}" == "1" ]] || exact_candidate_die "call exact_candidate_bind_dir first" || return 1

    # Preserve pre-existing prefix files safely.
    exact_candidate_stash_prefix_file "$EC_PKGLIB/pg_flashback.so" "lib/pg_flashback.so"
    exact_candidate_stash_prefix_file "$EC_SHARE_EXT/pg_flashback.control" "share/pg_flashback.control"
    local sql
    for sql in "$EC_SHARE_EXT"/pg_flashback--*.sql; do
        [[ -e "$sql" ]] || continue
        exact_candidate_stash_prefix_file "$sql" "share/$(basename "$sql")"
    done
    # Also stash any control-adjacent sql that archive will install.
    for sql in "$EC_EXT_ROOT"/share/extension/pg_flashback--*.sql; do
        local base
        base="$(basename "$sql")"
        if [[ ! -f "$EC_STASH_DIR/share/${base}.state" ]]; then
            exact_candidate_stash_prefix_file "$EC_SHARE_EXT/$base" "share/$base"
        fi
    done

    install -d -m 0755 "$EC_PKGLIB" "$EC_SHARE_EXT"
    install -m 0755 "$EC_EXT_ROOT/lib/pg_flashback.so" "$EC_PKGLIB/pg_flashback.so"
    install -m 0644 "$EC_EXT_ROOT/share/extension/pg_flashback.control" \
        "$EC_EXT_ROOT"/share/extension/pg_flashback--*.sql \
        "$EC_SHARE_EXT/"
    EC_INSTALLED=1
    exact_candidate_verify_installed || return 1
}

exact_candidate_verify_installed() {
    [[ "${EC_BOUND:-0}" == "1" ]] || exact_candidate_die "not bound" || return 1
    local got_so got_helper
    got_so="$(exact_candidate_sha256 "$EC_PKGLIB/pg_flashback.so")"
    got_helper="$(exact_candidate_sha256 "$EC_HELPER_BIN")"
    [[ "$got_so" == "$EC_EXT_BIN_SHA" ]] || exact_candidate_die "installed .so SHA $got_so != manifest $EC_EXT_BIN_SHA" || return 1
    [[ "$got_helper" == "$EC_HELPER_BIN_SHA" ]] || exact_candidate_die "helper SHA $got_helper != manifest $EC_HELPER_BIN_SHA" || return 1
    EC_INSTALLED_SO_PATH="$EC_PKGLIB/pg_flashback.so"
    EC_INSTALLED_SO_SHA="$got_so"
    EC_INSTALLED_HELPER_PATH="$EC_HELPER_BIN"
    EC_INSTALLED_HELPER_SHA="$got_helper"
    return 0
}

exact_candidate_restore_prefix() {
    [[ -d "${EC_STASH_DIR:-}" ]] || return 0
    local rel state dest
    # Restore known stashed files; remove archive-only files that were absent.
    if [[ -d "$EC_STASH_DIR/share" ]]; then
        while IFS= read -r -d '' state; do
            rel="${state#"$EC_STASH_DIR/"}"
            rel="${rel%.state}"
            dest=""
            case "$rel" in
                lib/*) dest="$EC_PKGLIB/${rel#lib/}" ;;
                share/*) dest="$EC_SHARE_EXT/${rel#share/}" ;;
                *) continue ;;
            esac
            if [[ "$(cat "$state")" == "absent" ]]; then
                rm -f -- "$dest"
            else
                mkdir -p "$(dirname "$dest")"
                cp -a -- "$EC_STASH_DIR/$rel" "$dest"
                local want got
                want="$(cat "$EC_STASH_DIR/${rel}.sha256")"
                got="$(exact_candidate_sha256 "$dest")"
                [[ "$got" == "$want" ]] || exact_candidate_die "restored $dest hash mismatch ($got != $want)" || return 1
            fi
        done < <(find "$EC_STASH_DIR" -type f -name '*.state' -print0)
    fi
    EC_INSTALLED=0
    return 0
}

exact_candidate_verify_end_state() {
    [[ "${EC_BOUND:-0}" == "1" ]] || exact_candidate_die "not bound" || return 1
    local head tree dirty
    head="$(git -C "$REPO_ROOT" rev-parse HEAD)"
    tree="$(git -C "$REPO_ROOT" rev-parse 'HEAD^{tree}')"
    dirty="$(git -C "$REPO_ROOT" status --porcelain=v1)"
    [[ -z "$dirty" ]] || exact_candidate_die "source tree became dirty during qualification" || return 1
    [[ "$head" == "$EC_SOURCE_COMMIT" ]] || exact_candidate_die "HEAD changed during qualification" || return 1
    [[ "$tree" == "$EC_SOURCE_TREE" ]] || exact_candidate_die "source tree hash changed during qualification" || return 1

    # Candidate archives unchanged.
    (
        cd "$CANDIDATE_DIR"
        sha256sum -c SHA256SUMS >/dev/null
        echo "$EC_PACKAGE_SHA  $EC_EXT_ARCHIVE" | sha256sum -c - >/dev/null
    ) || { exact_candidate_die "candidate archives changed during qualification"; return 1; }

    # If still installed, binaries must still match; if restored, stash restore already checked.
    if [[ "${EC_INSTALLED:-0}" == "1" ]]; then
        exact_candidate_verify_installed || return 1
    fi
    return 0
}

exact_candidate_identity_json() {
    jq -n \
        --arg source_commit "$EC_SOURCE_COMMIT" \
        --arg source_tree "$EC_SOURCE_TREE" \
        --arg package_sha "$EC_PACKAGE_SHA" \
        --arg ext_bin "$EC_EXT_BIN_SHA" \
        --arg helper_bin "$EC_HELPER_BIN_SHA" \
        --arg so_path "${EC_INSTALLED_SO_PATH:-}" \
        --arg so_sha "${EC_INSTALLED_SO_SHA:-}" \
        --arg helper_path "${EC_INSTALLED_HELPER_PATH:-}" \
        --arg helper_sha "${EC_INSTALLED_HELPER_SHA:-}" \
        --arg arch "$EC_ARCH" \
        --arg pg_major "$EC_PG_MAJOR" \
        --arg candidate_dir "$CANDIDATE_DIR" \
        '{
          candidate_dir: $candidate_dir,
          source_commit: $source_commit,
          source_tree: $source_tree,
          package_sha256: $package_sha,
          extension_binary_sha256: $ext_bin,
          helper_binary_sha256: $helper_bin,
          installed_extension_path: $so_path,
          installed_extension_sha256: $so_sha,
          installed_helper_path: $helper_path,
          installed_helper_sha256: $helper_sha,
          arch: $arch,
          pg_major: $pg_major,
          install_source: "candidate_archives_only"
        }'
}
