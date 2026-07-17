# Backup-backed table restore runbook

Status: **helper/result workflow implemented; coverage-generation integration
and post-restore re-anchor are pending**

This is the helper qualification procedure, not a release-qualified production
runbook. The current controller can recover, validate, import and swap an
artifact, but it does not yet establish or invalidate the adopted coverage
generation atomically.

This runbook is for tables that cannot satisfy the local profile's capacity,
change-rate, write-stall or RTO budget. The backup profile adds table
selection, DDL recovery markers, validation, extraction and a controlled
production swap on top of pgBackRest. It does not replace the organization's
normal backup or disaster-recovery policy.

Read [`STORAGE_POLICY.md`](STORAGE_POLICY.md) and
[`RELEASE_SCOPE.md`](RELEASE_SCOPE.md) before operating this path.

## 1. Prerequisites

- Linux and PostgreSQL 15, 16, 17 or 18.
- The extension binary and helper binary built from the same release.
- A local POSIX pgBackRest repository with completed full backups and
  continuous archived WAL for the advertised window.
- Matching PostgreSQL client/server binaries in `pg_bin_dir`.
- `jq`, `sha256sum`, `flock`, `cp`, `psql` and `pg_restore`.
- A service account that owns `work_root` and `socket_root` and is allowed to
  start the temporary PostgreSQL cluster. In a normal installation this is the
  PostgreSQL operating-system account, never root.
- A database login that is a member of both `flashback_admin` and
  `flashback_recovery_agent` for the reference controller.

The first release targets ordinary LOGGED, non-partitioned tables only. Do not
use this path for partitioned/foreign/unlogged tables, tablespaces, a table
renamed across the target, or a dropped table whose incoming foreign
keys/dependent views must be recreated automatically.

## 2. Repository layout

Keep the normal pgBackRest repository. For the fast path, add a short-retention
local repository key to the same stanza. Configure that repository's backup
jobs with compression, bundle and block storage disabled so a completed full
backup contains a directly readable `pg_data` tree. If this condition is not
met, the helper selects classic pgBackRest restore instead of snapshot-direct.

Example concepts (adapt paths and retention to the installation):

```ini
[global]
repo1-path=/var/lib/pgbackrest
repo1-retention-full=4

repo2-path=/xfs/pgbackrest-flashback
repo2-retention-full=2
repo2-bundle=n
repo2-block=n
repo2-hardlink=y

[app]
pg1-path=/var/lib/pgsql/17/data
```

Use `--repo=2 --compress-type=none` for the fast-tier full backup. pgBackRest
remains an external MIT-licensed dependency; no pgBackRest source is embedded
or linked into pg_flashback.

## 3. Coordinate backup and expire

Create the lock parent once and give it only to the recovery service account:

```bash
install -d -m 0700 -o postgres -g postgres /run/pg_flashback
```

Route every `backup` and `expire` that can mutate the selected repository
through the exclusive wrapper:

```bash
scripts/pgbackrest_with_flashback_lock.sh \
  /run/pg_flashback/app-repo2.lock -- \
  /usr/bin/pgbackrest --config=/etc/pgbackrest/pgbackrest.conf \
  --stanza=app --repo=2 --type=full --compress-type=none backup

scripts/pgbackrest_with_flashback_lock.sh \
  /run/pg_flashback/app-repo2.lock -- \
  /usr/bin/pgbackrest --config=/etc/pgbackrest/pgbackrest.conf \
  --stanza=app --repo=2 expire
```

Do not wrap `archive-push`: WAL must continue arriving while a recovery holds
the shared lock. The helper holds its shared lock from backup revalidation
through PostgreSQL promotion, so expire cannot remove either the base backup
or required WAL mid-recovery.

Schedule the fail-closed anchor audit independently of backup jobs (for
example every five minutes) and alert on a non-zero exit or
`status=degraded`:

```bash
pg-flashback-recovery audit-anchors \
  --config /etc/pg_flashback/app-repo2.json
```

This does not make an uncoordinated external `pgbackrest expire` safe. It
limits the failure mode: the next audit durably freezes every missing/corrupt
generation, and restore admission rejects its interval instead of claiming
coverage from metadata alone.

## 4. Configure the helper

Create a service-account-owned mode-0600 JSON file, or a root-managed
non-writable file readable by the service account's group. Symlinked and
group/world-writable configuration files are rejected. Restore requests cannot
override any of these paths or options.

```json
{
  "profile": "app_repo2",
  "pgbackrest_bin": "/usr/bin/pgbackrest",
  "pgbackrest_config": "/etc/pgbackrest/pgbackrest.conf",
  "pg_bin_dir": "/usr/pgsql-17/bin",
  "cp_bin": "/usr/bin/cp",
  "repository_path": "/xfs/pgbackrest-flashback",
  "repository_key": 2,
  "stanza": "app",
  "work_root": "/xfs/pg_flashback/work",
  "socket_root": "/run/pg_flashback/sockets",
  "recovery_port": 25432,
  "recovery_user": "postgres",
  "snapshot_provider": "xfs_reflink",
  "expire_lock_path": "/run/pg_flashback/app-repo2.lock",
  "max_work_bytes": 1099511627776,
  "max_work_root_bytes": 2199023255552,
  "min_free_bytes": 67108864,
  "artifact_ttl_seconds": 86400,
  "max_retained_artifacts": 32,
  "max_retained_artifact_bytes": 1099511627776,
  "command_timeout_seconds": 3600,
  "recovery_timeout_seconds": 7200,
  "proof_hmac_key_file": "/etc/pg_flashback/proof-hmac.key",
  "controller": {
    "host": "/run/postgresql",
    "port": 5432,
    "database": "app",
    "user": "pg_flashback_recovery"
  }
}
```

Create `proof-hmac.key` as 32 random bytes encoded as 64 hexadecimal
characters, mode 0600, readable by the PostgreSQL/helper operating-system
account. Configure the same absolute path as
`pg_flashback.proof_hmac_key_file` in `postgresql.conf` and reload. The key is
never stored in SQL. A recovery-agent login can ask PostgreSQL for the
canonical payload, but cannot install a proof without the helper's HMAC; raw,
missing, replayed-against-different-fields and malformed attestations fail
closed. Rotate the key only while no verification request is in flight.

`max_work_bytes` is a logical request-tree ceiling. Set it above the selected
full backup's logical size even when reflinks make the additional physical
allocation small. `max_work_root_bytes` bounds the aggregate of all request
directories under `work_root`. The helper continuously enforces
`min_free_bytes` during materialize/replay/export, pins completed artifacts
until import (`unpin`), and expires them with `gc` / `gc --dry-run`.

Probe before enabling the profile:

```bash
pg-flashback-recovery probe --config /etc/pg_flashback/app-repo2.json | jq .
```

`snapshot_direct_eligible=true` means the exact configured paths passed a real
`cp --reflink=always` content check. A false value is not silent: `plan` shows
why classic restore will be used.

## 5. Enable tracking and coverage

This section documents the current helper/controller workflow and the adopted
release-required protocol. It is not a release-qualified production enablement
path until coverage-generation integration lands. Do not treat a successful
helper restore as proof of an admissible generation.

Backup tracking stores schema/DDL metadata only. It does not create a table
snapshot or DML capture triggers:

```sql
SELECT flashback_track_backup('public.orders', 'app_repo2');
```

`flashback_track_backup()` creates a building backup generation and a LOGGED
tracking marker. Coverage stays **unanchored (zero active)** until a recovery
agent installs a one-time verified FULL backup proof and that proof is consumed.
`flashback_set_backup_coverage()`, `flashback_activate_backup_anchor(...)`, and
`flashback_advance_backup_frontier(...)` are fail-closed stubs: caller-supplied
LSNs or manifest digests are not recoverability evidence.

The release-qualified initial-tracking protocol must first commit a durable
LOGGED tracking marker and resolve its real commit coordinate. Tracking remains
unanchored with zero active generations until pgBackRest completes a **new
full backup whose start LSN is strictly after that resolved marker
commit**. A backup that already existed or was in progress when tracking began
does not qualify even if it stops afterward. Once verified under the repository
shared lock, the helper/controller installs an immutable proof
(`verification_request_id`, repository/profile, stanza, label, sysid, timeline,
manifest reference + SHA-256, start/stop LSN) and consumes it exactly once for
that tracking lifecycle.

The recovery helper now implements the supported verification path. A request
contains only a request ID and tracking ID:

```json
{"request_id":"verify-req-001","tracking_id":42}
```

```bash
pg-flashback-recovery verify-anchor \
  --config /etc/pg_flashback/app_repo2.json \
  --request verify-req-001.json
pg-flashback-recovery verify-frontier \
  --config /etc/pg_flashback/app_repo2.json \
  --request frontier-req-001.json
```

The helper obtains label/type/system identifier/timeline/manifest/start-stop
LSNs from `pgbackrest info` plus the repository manifest under the shared
lock. Frontier verification scans contiguous archived WAL segments beginning
at the anchor. The SQL install/consume functions are controller internals, not
an operator procedure. Timeline mismatch commits a durable freeze/gap before
the helper exits unsuccessfully.

This implementation still requires exact-RC qualification before v0.1.0
release status. In particular, uncontrolled external `pgbackrest expire` is
unsupported; use `pg-flashback-recovery expire`, which takes the exclusive
repository lock and rejects active generation pins.

Coverage metadata is an admission-control assertion, not a substitute for the
helper's real backup/WAL checks. Never advance `valid_through_lsn` beyond WAL
that has actually reached the configured repository.

DDL disaster-point metadata may be retired only when generation-aware coverage
proves that no advertised target needs it. The current seven-day age-based
marker behavior is legacy and is not release-qualified; physical backup/WAL
retention alone does not prove that the marker catalog is complete.

## 6. Recover after DROP/TRUNCATE/ALTER

List DDL markers. `target_lsn` is captured inside the DDL transaction before
the destructive transaction commits; native recovery to this point aborts the
uncommitted DDL and exposes the pre-disaster table:

```sql
SELECT *
FROM flashback_backup_disaster_points('public.orders', interval '2 hours');
```

Force/archive the segment containing the chosen marker, verify pgBackRest, and
advance the verified coverage end. Then run the reference controller:

```bash
export PGHOST=/run/postgresql
export PGPORT=5432
export PGUSER=pg_flashback_operator
export PGPASSWORD='use-a-secret-provider-in-production'

scripts/pg_flashback_backup_restore.sh \
  --config /etc/pg_flashback/app-repo2.json \
  --dbname appdb \
  --table public.orders \
  --target-lsn 0/8F12340 \
  --helper /usr/local/bin/pg-flashback-recovery
```

The controller performs this sequence:

1. prepare and claim an immutable extension request;
2. run the helper and require result format 3;
3. verify the artifact SHA-256 locally;
4. import the custom dump in one transaction with no owner/ACL side effects;
5. submit the manifest;
6. let the extension verify schema, rows, table identity, owner and ACL;
7. perform the transactional shadow swap.

Set a connection-level `lock_timeout` for the operator role if production
policy requires a bounded wait for the final `ACCESS EXCLUSIVE` lock. A
same-name table with a different OID is never overwritten; finalization fails
and leaves the unrelated table unchanged.

### Post-restore coverage (required, not wired)

The backup profile never creates a local row snapshot after the swap. Doing so
would silently turn it into `local_delta` and defeat the profile's storage
contract.

For the first release, backup finalization must instead atomically write a
durable pending swap-XID marker, mark the tracking lifecycle `unanchored`, and
open a persistent `post_restore_unanchored` gap. Its pre-commit LSN is not the
swap's COMMIT LSN; a post-commit resolver may fill the real commit coordinate,
seal the predecessor at that exclusive coordinate, and leave the successor
`building`, but the gap remains open. Zero active generations is intentional in
this state. The swap may be complete, but new targets are rejected while that
gap is open; admission must not fall back to the predecessor. The current
finalizer does not yet write this state, so its successful return is functional
evidence only.

Re-anchor is allowed only after pgBackRest completes a **new full backup whose
start LSN is strictly after the resolved production-swap
commit**. A backup already running during the swap does not qualify merely
because its stop boundary is later. The controller verifies that full backup
and its archived-WAL coverage, then activates the new backup generation at the
verified stop boundary. Activation closes the open gap's upper endpoint; it
never makes targets inside the gap valid. Reusing the old backup range or
merely observing later archived WAL is not a first-release re-anchor.

## 7. Manual/resume protocol

Requests are visible in `flashback.backup_restore_requests` with states
`pending`, `running`, `artifact_ready`, `completed`, `failed` or `cancelled`.
The controller can resume an already claimed request:

```bash
scripts/pg_flashback_backup_restore.sh \
  --config /etc/pg_flashback/app-repo2.json \
  --dbname appdb \
  --request /secure/request.json
```

If finalization fails after manifest acceptance, the request remains
`artifact_ready` and the deterministic import table remains for inspection.
Correct the external condition (for example, remove an unintended replacement
table or restore a missing role) and call:

```sql
SELECT flashback_finalize_backup_restore('fb-...');
```

Do not edit a request JSON or reuse its ID with another profile. Both are
rejected as `request_conflict`.

## 8. Monitoring and cleanup

```sql
SELECT request_id, schema_name, table_name, target_lsn, status,
       created_at, updated_at, completed_at, error_message
FROM flashback.backup_restore_requests
ORDER BY created_at DESC;
```

Successful helper requests retain only `result.json`, logs and the table dump.
Temporary `pgdata` and socket directories must be absent. Failed handled
requests retain logs/state but no pgdata or artifact. After a hard kill, the
next helper execution reconciles the abandoned process group/cluster before it
starts new work.

Capacity alerts must cover the repository, WAL archive, helper work filesystem
and production tablespace independently.

## 9. Stable helper errors

| Code | Meaning / action |
|---|---|
| `target_before_oldest_backup` | Choose a newer target or restore retained coverage. |
| `no_eligible_backup` | Create/retain a completed full backup. |
| `recovery_target_unreachable` | Required archived WAL is missing. Do not import anything. |
| `repository_busy` | Backup/expire owns the exclusive lock; retry later. |
| `selected_backup_changed` | Catalog changed during planning; rebuild the request/plan. |
| `work_quota_exceeded` | Increase the configured logical ceiling/headroom or use another filesystem. |
| `unsupported_topology` | The request uses a deliberately unsupported storage/table layout. |
| `table_identity_mismatch` | The OID at target does not match the tracked table. Investigate OID/name history. |
| `schema_fingerprint_mismatch` | Target schema metadata and recovered table disagree. Stop and investigate. |
| `fingerprint_mismatch` | Expected and recovered row sets disagree. Stop and investigate. |
| `request_conflict` | The request ID was changed or used with another helper profile. |
| `recovery_busy` | Another restore owns the work-root execution lock. |
| `cancelled` / `command_timeout` | Retry the identical request after cleanup/reconciliation. |

Never bypass an identity, schema, fingerprint or checksum error merely to make
the restore continue.
