#!/usr/bin/env bash
# Build an exact release-candidate archive from a clean source commit.
# Does not create tags or GitHub releases.
#
# Output under target/candidate/<source_commit>/:
#   pg_flashback-candidate-<commit>-src.tar.gz
#   pg_flashback-candidate-<commit>-pg17-<arch>-linux.tar.gz
#   pg-flashback-recovery-candidate-<commit>-<arch>-linux.tar.gz
#   MANIFEST.json (source commit/tree + SHA-256 digests)
#
# Usage:
#   ./scripts/build_candidate_archive.sh
#   PG_MAJOR=17 ./scripts/build_candidate_archive.sh

set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PG_MAJOR="${PG_MAJOR:-17}"
PG_CONFIG="${PG_CONFIG:-}"
ARCH="$(uname -m)"
case "$ARCH" in
    x86_64|amd64) ARCH_LABEL=x86_64 ;;
    aarch64|arm64) ARCH_LABEL=aarch64 ;;
    *) ARCH_LABEL=$ARCH ;;
esac

cd "$ROOT"
if [[ -n "$(git status --porcelain=v1)" ]]; then
    echo "FAIL: candidate archive requires a clean source tree" >&2
    exit 1
fi

SOURCE_COMMIT="$(git rev-parse HEAD)"
SOURCE_TREE="$(git rev-parse 'HEAD^{tree}')"
SHORT="$(git rev-parse --short=12 HEAD)"
OUT_ROOT="${CANDIDATE_OUT:-$ROOT/target/candidate/$SOURCE_COMMIT}"
STAGE="$OUT_ROOT/stage"
# Drop every prior candidate tree so CI cache/uploads cannot mix digests.
rm -rf "$ROOT/target/candidate"
mkdir -p "$STAGE" "$OUT_ROOT"

if [[ -z "$PG_CONFIG" ]]; then
    if command -v cargo >/dev/null && cargo pgrx info pg-config "$PG_MAJOR" >/dev/null 2>&1; then
        PG_CONFIG="$(cargo pgrx info pg-config "$PG_MAJOR")"
    else
        PG_CONFIG="/usr/local/pgsql-${PG_MAJOR}/bin/pg_config"
    fi
fi
[[ -x "$PG_CONFIG" ]] || {
    echo "FAIL: pg_config not found for PG${PG_MAJOR}: $PG_CONFIG" >&2
    exit 1
}

echo "Building candidate from source_commit=$SOURCE_COMMIT tree=$SOURCE_TREE pg=$PG_MAJOR"

# 1) Exact source archive (git archive of HEAD).
SRC_NAME="pg_flashback-candidate-${SHORT}-src"
git archive --format=tar.gz --prefix="${SRC_NAME}/" -o "$OUT_ROOT/${SRC_NAME}.tar.gz" HEAD

# 2) Extension package via pgrx.
cargo pgrx package \
    --manifest-path "$ROOT/Cargo.toml" \
    --pg-config "$PG_CONFIG" \
    --no-default-features \
    --features "pg${PG_MAJOR}" \
    --out-dir "$STAGE/pgrx-package"

EXT_NAME="pg_flashback-candidate-${SHORT}-pg${PG_MAJOR}-${ARCH_LABEL}-linux"
EXT_DIR="$STAGE/$EXT_NAME"
mkdir -p "$EXT_DIR/lib" "$EXT_DIR/share/extension" "$EXT_DIR/docs" "$EXT_DIR/scripts"
package_so="$(find "$STAGE/pgrx-package" -type f -name pg_flashback.so -print -quit)"
package_control="$(find "$STAGE/pgrx-package" -type f -name pg_flashback.control -print -quit)"
package_sql="$(find "$STAGE/pgrx-package" -type f -name 'pg_flashback--*.sql' -print -quit)"
[[ -n "$package_so" && -n "$package_control" && -n "$package_sql" ]]
install -m 0755 "$package_so" "$EXT_DIR/lib/pg_flashback.so"
install -m 0644 "$package_control" "$package_sql" "$EXT_DIR/share/extension/"
printf '%s\n' "$PG_MAJOR" > "$EXT_DIR/PG_MAJOR"
cp README.md LICENSE CHANGELOG.md SECURITY.md THIRD_PARTY_NOTICES.md "$EXT_DIR/"
cp docs/RELEASE_SCOPE.md docs/BACKUP_RESTORE_RUNBOOK.md "$EXT_DIR/docs/"
mkdir -p "$EXT_DIR/scripts/lib"
install -m 0755 \
    "$ROOT/scripts/run_clean_host_candidate_smoke.sh" \
    "$ROOT/scripts/run_exact_candidate_functional_suite.sh" \
    "$ROOT/scripts/run_exact_candidate_drop_qualification.sh" \
    "$ROOT/scripts/run_exact_rc_chaos_suite.sh" \
    "$ROOT/scripts/run_exact_rc_24h_stability_soak.sh" \
    "$ROOT/scripts/run_exact_rc_24h_soak.sh" \
    "$ROOT/scripts/run_exact_rc_harness_selftest.sh" \
    "$EXT_DIR/scripts/"
install -m 0644 "$ROOT/scripts/lib/exact_candidate_identity.sh" "$EXT_DIR/scripts/lib/"
tar -C "$STAGE" -czf "$OUT_ROOT/${EXT_NAME}.tar.gz" "$EXT_NAME"

# 3) Recovery helper release binary.
cargo build --release --locked --manifest-path "$ROOT/tools/pg_flashback_recovery/Cargo.toml"
HELPER_NAME="pg-flashback-recovery-candidate-${SHORT}-${ARCH_LABEL}-linux"
HELPER_DIR="$STAGE/$HELPER_NAME"
mkdir -p "$HELPER_DIR/bin" "$HELPER_DIR/examples" "$HELPER_DIR/docs" "$HELPER_DIR/scripts"
install -m 0755 \
    "$ROOT/tools/pg_flashback_recovery/target/release/pg-flashback-recovery" \
    "$HELPER_DIR/bin/"
install -m 0755 \
    "$ROOT/scripts/pg_flashback_backup_restore.sh" \
    "$ROOT/scripts/pgbackrest_with_flashback_lock.sh" \
    "$HELPER_DIR/bin/"
cp "$ROOT/tools/pg_flashback_recovery/examples/"*.json "$HELPER_DIR/examples/" 2>/dev/null || true
cp "$ROOT/tools/pg_flashback_recovery/README.md" "$HELPER_DIR/README.md"
cp LICENSE CHANGELOG.md SECURITY.md THIRD_PARTY_NOTICES.md "$HELPER_DIR/"
cp docs/RECOVERY_HELPER_DESIGN.md docs/BACKUP_RESTORE_RUNBOOK.md \
    docs/RELEASE_SCOPE.md "$HELPER_DIR/docs/"
mkdir -p "$HELPER_DIR/scripts/lib"
install -m 0755 \
    "$ROOT/scripts/run_clean_host_candidate_smoke.sh" \
    "$ROOT/scripts/run_exact_candidate_functional_suite.sh" \
    "$ROOT/scripts/run_exact_candidate_drop_qualification.sh" \
    "$ROOT/scripts/run_exact_rc_chaos_suite.sh" \
    "$ROOT/scripts/run_exact_rc_24h_stability_soak.sh" \
    "$ROOT/scripts/run_exact_rc_24h_soak.sh" \
    "$ROOT/scripts/run_exact_rc_harness_selftest.sh" \
    "$HELPER_DIR/scripts/"
install -m 0644 "$ROOT/scripts/lib/exact_candidate_identity.sh" "$HELPER_DIR/scripts/lib/"
tar -C "$STAGE" -czf "$OUT_ROOT/${HELPER_NAME}.tar.gz" "$HELPER_NAME"

SRC_SHA="$(sha256sum "$OUT_ROOT/${SRC_NAME}.tar.gz" | awk '{print $1}')"
EXT_SHA="$(sha256sum "$OUT_ROOT/${EXT_NAME}.tar.gz" | awk '{print $1}')"
HELPER_PKG_SHA="$(sha256sum "$OUT_ROOT/${HELPER_NAME}.tar.gz" | awk '{print $1}')"
HELPER_BIN_SHA="$(sha256sum "$HELPER_DIR/bin/pg-flashback-recovery" | awk '{print $1}')"
EXT_BIN_SHA="$(sha256sum "$EXT_DIR/lib/pg_flashback.so" | awk '{print $1}')"

jq -n \
    --arg source_commit "$SOURCE_COMMIT" \
    --arg source_tree "$SOURCE_TREE" \
    --arg pg_major "$PG_MAJOR" \
    --arg arch "$ARCH_LABEL" \
    --arg src_name "${SRC_NAME}.tar.gz" \
    --arg ext_name "${EXT_NAME}.tar.gz" \
    --arg helper_name "${HELPER_NAME}.tar.gz" \
    --arg src_sha "$SRC_SHA" \
    --arg ext_sha "$EXT_SHA" \
    --arg helper_pkg_sha "$HELPER_PKG_SHA" \
    --arg helper_bin_sha "$HELPER_BIN_SHA" \
    --arg ext_bin_sha "$EXT_BIN_SHA" \
    --arg built_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg pg_version "$("$PG_CONFIG" --version)" \
    '{
      provenance: {
        source_commit: $source_commit,
        source_tree: $source_tree,
        pg_major: $pg_major,
        arch: $arch,
        postgresql_version: $pg_version,
        built_at: $built_at,
        tree_clean: true
      },
      artifacts: {
        source_archive: {name: $src_name, sha256: $src_sha},
        extension_archive: {name: $ext_name, sha256: $ext_sha},
        helper_archive: {name: $helper_name, sha256: $helper_pkg_sha},
        extension_binary_sha256: $ext_bin_sha,
        helper_binary_sha256: $helper_bin_sha,
        package_sha256: $ext_sha
      }
    }' > "$OUT_ROOT/MANIFEST.json"

(
    cd "$OUT_ROOT"
    sha256sum ./*.tar.gz > SHA256SUMS
    sha256sum -c SHA256SUMS >/dev/null
)

echo "Candidate archives written under $OUT_ROOT"
jq . "$OUT_ROOT/MANIFEST.json"
