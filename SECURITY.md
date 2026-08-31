# Security policy

## Supported versions

Until the first stable series is published, only the latest tagged release is
eligible for security fixes. Development branches are not supported releases.

## Reporting a vulnerability

Please do not open a public issue for a suspected vulnerability. Use GitHub's
private vulnerability reporting for this repository. Include the affected
PostgreSQL version, pg_flashback version/commit, topology, reproduction steps
and whether the helper or production database was exposed.

Do not include database credentials, repository secrets, production dumps or
customer data. A receipt should be acknowledged within seven days; validation
and remediation timing depend on severity and reproducibility.

## Trust assumptions

The extension is superuser-installed. Its supported and rejected local
topologies are defined in [`docs/SUPPORT.md`](docs/SUPPORT.md).

Snapshot and WAL-derived row data are as sensitive as the protected source
table. Monitoring access does not imply permission to read those payloads.

The `external_zstd` root must be accessible only to the PostgreSQL operating-
system account and must be protected like database storage. pg_flashback is
not an off-host backup system; compromise or loss of the database host may also
compromise or remove locally attached snapshot artifacts.
