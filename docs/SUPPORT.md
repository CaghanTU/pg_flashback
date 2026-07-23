# Support matrix

This document defines the current local DROP-recovery contract. Code may exist
outside this matrix; that does not make it a supported product path.

## Platforms

| Area | Supported |
|---|---|
| PostgreSQL | 15, 16, 17, 18 |
| Operating system | Linux |
| Capture | Logical WAL |
| Topology | Writable primary with one local logical slot per configured database |
| CLI dependencies | `psql`, `jq`, libpq connection settings |

Native macOS is not supported. Linux/aarch64 development under Lima and
Linux/x86_64 builds are separate environments; evidence from one architecture
is not silently generalized to the other.

## Tables

| Table property | Behavior |
|---|---|
| Ordinary permanent `LOGGED` table | Supported |
| Primary/secondary indexes | Preserved |
| Unique and check constraints | Preserved |
| Identity and serial sequences | Preserved; original names restored |
| TOAST / large values | Supported |
| Quoted names and non-`public` schemas | Supported |
| Owner and ACL | Preserved |
| Row-level security metadata | Preserved when the complete policy contract can be reconstructed |
| Partitioned table or partition | Rejected |
| Foreign table | Rejected |
| Materialized view | Rejected |
| TEMP or `UNLOGGED` table | Rejected |
| Classical inheritance topology | Rejected unless the exact topology is proven by the recovery plan |

## Incidents

| Incident | Behavior |
|---|---|
| Ordinary `DROP TABLE` | Recover latest proven pre-DROP state |
| Multiple historical DROP events | Latest safe DROP by default; explicit event selection available |
| Ambiguous or non-restorable latest DROP | Rejected; never falls back silently |
| Same-name table recreated after DROP | Rejected to prevent overwrite |
| `DROP ... CASCADE` | Planned from the pre-DROP manifest; rejected if any dependency is unsupported |
| `TRUNCATE` or destructive DML | Captured by the engine; advanced LSN recovery remains available |
| DDL across an unproven schema epoch | Rejected |

## Transaction semantics

- Only committed transactions become recoverable.
- A transaction that changes rows and drops the table atomically is recovered
  to the state before that entire transaction.
- Rollbacks and aborted subtransactions do not become history.
- Event ordering follows transaction COMMIT LSN, not wall-clock timestamps.
- Duplicate delivery is tolerated; incomplete transactions do not advance the
  coverage watermark.

## Failure and HA behavior

| Condition | Behavior |
|---|---|
| PostgreSQL restart | Worker resumes from the logical slot |
| Worker crash | Postmaster restarts it; health reports the interruption |
| Slot lost/replaced/externally advanced | Coverage freezes and a durable gap opens |
| Timeline mismatch or promotion without proven continuity | Recovery rejected; re-anchor required |
| Command run on a standby | Mutating operation rejected |
| Split brain and external fencing | Operator responsibility; not automated |

## Recovery guarantees

A supported recovery:

1. selects one exact disaster event and one complete WAL prefix;
2. recomputes the plan at execution time;
3. materializes a shadow relation;
4. validates data/schema identity and required metadata;
5. swaps atomically;
6. records a durable operation result;
7. waits for a correctly bound successor generation before reporting verified.

An error may leave a journal entry to reconcile, but must not be reported as a
successful verified recovery.

## Lifecycle

| Operation | Support |
|---|---|
| `protect` | Supported |
| `unprotect` | Supported two-phase stop |
| Re-protect before old cleanup | Supported as a new lifecycle |
| `cleanup --tracking-id` | Supported with dry-run and safety checks |
| Downgrade | Unsupported |
| Extension upgrade | `0.1.0` to `0.2.0` only |
| PostgreSQL major `pg_upgrade` | Not yet a supported workflow |

## Backup-backed subsystem

The pgBackRest provider is experimental and not part of this local support
contract. Differential/incremental chains, other backup providers, and a
general large-database claim are not supported.
