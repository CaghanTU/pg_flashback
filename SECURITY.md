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

The extension is superuser-installed. The backup recovery helper and reference
controller must run as a dedicated, non-root operating-system account with
private configuration and work directories. The first-release trust boundary
and rejected topologies are defined in
[`docs/RELEASE_SCOPE.md`](docs/RELEASE_SCOPE.md).
