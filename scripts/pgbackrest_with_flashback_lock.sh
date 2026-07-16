#!/usr/bin/env bash
# Run repository-mutating pgBackRest operations under the exclusive lock that
# the recovery helper shares. Use this wrapper for scheduled backup and expire.

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

exec 9>> "$LOCK_PATH"
flock --exclusive 9
exec "$@"
