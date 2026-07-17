#!/usr/bin/env bash
# Run repository-mutating pgBackRest backup operations under the exclusive lock
# that the recovery helper shares. Backup must disable pgBackRest's automatic
# expire phase; coordinated expiration needs the helper's durable DB lease.

set -Eeuo pipefail
umask 077

if (($# < 3)) || [[ "$2" != "--" || "$1" != /* ]]; then
    printf 'usage: %s /absolute/path/to/expire.lock -- pgbackrest [args...]\n' "$0" >&2
    exit 64
fi

LOCK_PATH="$1"
shift 2
[[ -d "$(dirname "$LOCK_PATH")" && ! -L "$LOCK_PATH" ]] || {
    printf 'lock parent must exist and lock path must not be a symlink: %s\n' "$LOCK_PATH" >&2
    exit 73
}

OPERATION=""
EXPIRE_AUTO_DISABLED=0
for argument in "$@"; do
    case "$argument" in
        backup|expire) OPERATION="$argument" ;;
        --no-expire-auto) EXPIRE_AUTO_DISABLED=1 ;;
    esac
done
if [[ "$OPERATION" == "expire" ]]; then
    printf 'raw pgBackRest expire is unsafe; use pg-flashback-recovery expire --config ...\n' >&2
    exit 64
fi
if [[ "$OPERATION" == "backup" && "$EXPIRE_AUTO_DISABLED" != "1" ]]; then
    printf 'backup must include --no-expire-auto so pinned generation anchors are not deleted\n' >&2
    exit 64
fi

exec 9>> "$LOCK_PATH"
flock --exclusive 9
exec "$@"
