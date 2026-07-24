#!/usr/bin/env python3
"""Validate tests/sql/integration/INVENTORY.json against the filesystem and lib.rs."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INTEGRATION = ROOT / "tests" / "sql" / "integration"
INVENTORY = INTEGRATION / "INVENTORY.json"
LIB_RS = ROOT / "src" / "lib.rs"
ALLOWED = {"supported-core", "infra"}
DIRECT_DELTA_RE = re.compile(
    r"INSERT\s+INTO\s+flashback\.(delta_log|pending_wal_events)\b",
    re.IGNORECASE,
)


def main() -> int:
    errors: list[str] = []
    data = json.loads(INVENTORY.read_text(encoding="utf-8"))
    files = data.get("files")
    if not isinstance(files, dict):
        print("ERROR: INVENTORY.json missing files object", file=sys.stderr)
        return 1

    classified: dict[str, dict] = {}
    for name, meta in files.items():
        if not isinstance(meta, dict) or "class" not in meta:
            errors.append(f"{name}: inventory entry must be an object with class")
            continue
        cls = meta["class"]
        if cls not in ALLOWED:
            errors.append(f"{name}: invalid class {cls!r}")
        if name in classified:
            errors.append(f"{name}: duplicate inventory entry")
        classified[name] = meta

    on_disk = sorted(p.name for p in INTEGRATION.glob("*.sql"))
    for name in on_disk:
        if name not in classified:
            errors.append(f"{name}: on disk but missing from inventory")
    for name in sorted(classified):
        if name not in on_disk:
            errors.append(f"{name}: in inventory but missing on disk")

    if "_common_setup.sql" in classified and classified["_common_setup.sql"].get("class") != "infra":
        errors.append("_common_setup.sql must be class=infra")

    lib = LIB_RS.read_text(encoding="utf-8")
    # sql_test! registrations only — exclude include_str!("..._common_setup.sql").
    registered_tests = set(
        re.findall(
            r'sql_test!\(\s*[A-Za-z0-9_]+,\s*"\.\./tests/sql/integration/([^"]+\.sql)"',
            lib,
        )
    )
    shared_setup_files = {"_common_setup.sql"} if "_common_setup.sql" in classified else set()

    for name in sorted(registered_tests):
        if name not in classified:
            errors.append(f"{name}: registered in src/lib.rs but missing from inventory")

    orphans = sorted(
        n
        for n in classified
        if n.endswith(".sql") and n not in shared_setup_files and n not in registered_tests
    )
    for name in orphans:
        errors.append(f"{name}: in inventory but not registered in src/lib.rs (orphan)")

    # Infra fixtures that write product capture tables must be adversarial.
    for name, meta in sorted(classified.items()):
        if meta.get("class") != "infra":
            continue
        if name == "_common_setup.sql":
            # Shared harness may define helpers; still require explicit flag when
            # the file itself contains direct product-table inserts.
            pass
        text = (INTEGRATION / name).read_text(encoding="utf-8")
        if DIRECT_DELTA_RE.search(text) and not meta.get("adversarial_fixture"):
            errors.append(
                f"{name}: infra file inserts into delta_log/pending_wal_events "
                "but adversarial_fixture is not true"
            )

    counts = {c: 0 for c in ALLOWED}
    for meta in classified.values():
        counts[meta["class"]] = counts.get(meta["class"], 0) + 1

    if errors:
        print("Integration inventory validation FAILED:", file=sys.stderr)
        for err in errors:
            print(f"  - {err}", file=sys.stderr)
        return 1

    print("Integration inventory OK")
    print(f"  total_sql={len(on_disk)}")
    for cls in ("supported-core", "infra"):
        print(f"  {cls}={counts[cls]}")
    print(f"  registered_tests={len(registered_tests)}")
    print(f"  shared_setup_files={len(shared_setup_files)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
