#!/usr/bin/env bash
# Canonical local_delta package builder (extension + CLI).
# Does not include the experimental pg_flashback_recovery helper.
#
# Usage:
#   PG_MAJOR=17 ./scripts/build_local_package.sh
#   OUT_DIR=target/local-package ./scripts/build_local_package.sh
#
# Fresh-install-only contract for 0.2.0 external staging: upgrade SQL from
# 0.1.0 is not claimed and is not shipped in this archive.

set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PG_MAJOR="${PG_MAJOR:-17}"
PG_CONFIG="${PG_CONFIG:-}"
OUT_DIR="${OUT_DIR:-$ROOT/target/local-package}"
ARCH="$(uname -m)"
case "$ARCH" in
    x86_64|amd64) ARCH_LABEL=x86_64 ;;
    aarch64|arm64) ARCH_LABEL=aarch64 ;;
    *) ARCH_LABEL=$ARCH ;;
esac

cd "$ROOT"
VERSION="$(sed -n 's/^version = "\([^"]*\)"/\1/p' Cargo.toml | head -1)"
SOURCE_COMMIT="$(git rev-parse HEAD)"
SHORT="$(git rev-parse --short=12 HEAD)"
export SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-$(git log -1 --format=%ct HEAD)}"
export TZ=UTC

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

STAGE="$OUT_DIR/stage"
ARCHIVE_ROOT="pg_flashback-${VERSION}-pg${PG_MAJOR}-${ARCH_LABEL}-linux"
rm -rf "$STAGE/$ARCHIVE_ROOT"
mkdir -p "$STAGE/$ARCHIVE_ROOT"/{lib,bin,share/extension,docs/samples,scripts} "$OUT_DIR"

echo "Building local package version=$VERSION pg=$PG_MAJOR commit=$SOURCE_COMMIT"

cargo pgrx package \
    --manifest-path "$ROOT/Cargo.toml" \
    --pg-config "$PG_CONFIG" \
    --no-default-features \
    --features "pg${PG_MAJOR}" \
    --out-dir "$STAGE/pgrx-package"

package_so="$(find "$STAGE/pgrx-package" -type f -name pg_flashback.so -print -quit)"
package_control="$(find "$STAGE/pgrx-package" -type f -name pg_flashback.control -print -quit)"
mapfile -t package_sql < <(find "$STAGE/pgrx-package" -type f -name 'pg_flashback--*.sql' | sort)
[[ -n "$package_so" && -n "$package_control" && ${#package_sql[@]} -gt 0 ]]

install -m 0755 "$package_so" "$STAGE/$ARCHIVE_ROOT/lib/pg_flashback.so"
install -m 0755 "$ROOT/scripts/pg_flashback" "$STAGE/$ARCHIVE_ROOT/bin/pg_flashback"
install -m 0644 "$package_control" "${package_sql[@]}" "$STAGE/$ARCHIVE_ROOT/share/extension/"
# Fresh-install-only: do not ship broken 0.1→0.2 upgrade paths.
printf '%s\n' "$PG_MAJOR" > "$STAGE/$ARCHIVE_ROOT/PG_MAJOR"
printf '%s\n' "fresh-install-only" > "$STAGE/$ARCHIVE_ROOT/INSTALL_CONTRACT"
printf '%s\n' \
    "Install:" \
    "  sudo install -m 0755 lib/pg_flashback.so \"\$(pg_config --pkglibdir)/\"" \
    "  sudo install -m 0644 share/extension/* \"\$(pg_config --sharedir)/extension/\"" \
    "  sudo install -m 0755 bin/pg_flashback /usr/local/bin/pg_flashback" \
    "  # configure postgresql.conf from docs/samples/postgresql.pg_flashback.conf" \
    "  # restart PostgreSQL, then: CREATE EXTENSION pg_flashback;" \
    "  command -v pg_flashback && pg_flashback version" \
    > "$STAGE/$ARCHIVE_ROOT/INSTALL.txt"

cp README.md LICENSE CHANGELOG.md SECURITY.md THIRD_PARTY_NOTICES.md "$STAGE/$ARCHIVE_ROOT/"
cp docs/QUICKSTART.md docs/SUPPORT.md docs/ARCHITECTURE.md docs/DEVELOPMENT.md \
    "$STAGE/$ARCHIVE_ROOT/docs/"
cp docs/samples/postgresql.pg_flashback.conf "$STAGE/$ARCHIVE_ROOT/docs/samples/"
install -m 0755 "$ROOT/scripts/run_local_package_smoke.sh" "$STAGE/$ARCHIVE_ROOT/scripts/" 2>/dev/null || true

# MANIFEST + digests
SO_SHA="$(sha256sum "$STAGE/$ARCHIVE_ROOT/lib/pg_flashback.so" | awk '{print $1}')"
CLI_SHA="$(sha256sum "$STAGE/$ARCHIVE_ROOT/bin/pg_flashback" | awk '{print $1}')"
{
    echo "pg_flashback.so  $SO_SHA"
    echo "pg_flashback     $CLI_SHA"
    (cd "$STAGE/$ARCHIVE_ROOT/share/extension" && sha256sum pg_flashback*)
} > "$STAGE/$ARCHIVE_ROOT/SHA256SUMS"

jq -n \
    --arg version "$VERSION" \
    --arg pg "$PG_MAJOR" \
    --arg arch "$ARCH_LABEL" \
    --arg commit "$SOURCE_COMMIT" \
    --arg so "$SO_SHA" \
    --arg cli "$CLI_SHA" \
    --arg contract "fresh-install-only" \
    --arg profile "local_delta" \
    '{
      schema_version:1,
      product:"pg_flashback",
      profile:$profile,
      version:$version,
      postgresql_major:$pg,
      arch:$arch,
      source_commit:$commit,
      install_contract:$contract,
      artifacts:{
        shared_object_sha256:$so,
        cli_binary_sha256:$cli
      },
      excludes:["pg_flashback_recovery","pgBackRest helper scripts"],
      notes:["Experimental backup helper is packaged separately and is not part of the local product."]
    }' > "$STAGE/$ARCHIVE_ROOT/MANIFEST.json"

# Minimal SPDX-ish SBOM stub (file inventory).
{
    echo "{"
    echo "  \"spdxVersion\": \"SPDX-2.3\","
    echo "  \"name\": \"pg_flashback-${VERSION}-pg${PG_MAJOR}\","
    echo "  \"packages\": ["
    echo "    {\"name\":\"pg_flashback\",\"versionInfo\":\"${VERSION}\",\"downloadLocation\":\"NOASSERTION\"}"
    echo "  ]"
    echo "}"
} > "$STAGE/$ARCHIVE_ROOT/SBOM.spdx.json"

ARCHIVE_PATH="$OUT_DIR/${ARCHIVE_ROOT}.tar.gz"
if tar --help 2>&1 | grep -q -- '--mtime'; then
    tar --sort=name --mtime="@${SOURCE_DATE_EPOCH}" --owner=0 --group=0 --numeric-owner \
        -C "$STAGE" -czf "$ARCHIVE_PATH" "$ARCHIVE_ROOT"
else
    tar -C "$STAGE" -czf "$ARCHIVE_PATH" "$ARCHIVE_ROOT"
fi

echo "OK: $ARCHIVE_PATH"
echo "MANIFEST: $STAGE/$ARCHIVE_ROOT/MANIFEST.json"
