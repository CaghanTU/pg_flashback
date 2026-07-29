#!/usr/bin/env bash
# Normalize pg_dump output fields that are intentionally nondeterministic.

normalize_schema_dump_in_place() {
    local dump_file=$1

    sed -i \
        -e '/^\\restrict /d' \
        -e '/^\\unrestrict /d' \
        -e '/^-- Dumped /d' \
        -e '/^-- Started /d' \
        -e '/^-- Completed /d' \
        "$dump_file"
}
