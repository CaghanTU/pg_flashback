# Third-party notices

pg_flashback's own source is licensed under MIT. The release contains Rust
dependencies under permissive open-source licenses and interoperates with
external PostgreSQL tools. This file is a notice, not a replacement for each
project's license text.

## External programs

These programs are invoked as separate executables and are not embedded or
linked into pg_flashback release binaries:

| Program | Use | License |
|---|---|---|
| [PostgreSQL](https://www.postgresql.org/) | Extension host, native PITR and client tools | PostgreSQL License |
| [pgBackRest](https://pgbackrest.org/) | Physical backup catalog, archive retrieval and classic restore | MIT |
| `jq` | Reference controller JSON validation | MIT |
| GNU coreutils (`cp`, `sha256sum`) | Reflink materialization and checksum verification | GPL-3.0-or-later |
| util-linux `flock` | External repository coordination | GPL-2.0-or-later |

The GPL-licensed command-line programs above are optional system programs
communicating through normal process and file interfaces; their source is not
included in or linked with pg_flashback.

## Direct Rust dependencies

The locked dependency trees include the following direct libraries. Their
transitive dependencies and exact versions are recorded in `Cargo.lock` and
`tools/pg_flashback_recovery/Cargo.lock`.

| Crate | Component | License |
|---|---|---|
| `pgrx`, `pgrx-tests` | PostgreSQL extension framework and tests | MIT |
| `serde`, `serde_json` | JSON serialization | MIT OR Apache-2.0 |
| `sha2` | SHA-256 implementation | MIT OR Apache-2.0 |
| `clap` | Helper command-line parser | MIT OR Apache-2.0 |
| `fs2` | File locking | MIT OR Apache-2.0 |
| `nix` | Unix process and signal handling | MIT |
| `signal-hook` | Signal delivery | MIT OR Apache-2.0 |
| `thiserror` | Error definitions | MIT OR Apache-2.0 |

At the 0.1.0 release candidate, RustSec also reports two maintenance-only
warnings in pgrx's locked transitive tree: `paste` (RUSTSEC-2024-0436, used by
`pgrx-tests`) and `serde_cbor` (RUSTSEC-2021-0127, used by `pgrx`). Neither is a
known vulnerability, and replacing them requires an upstream pgrx dependency
change. They must be re-evaluated on every pgrx upgrade; vulnerability findings
remain a release blocker.

Before publishing a release, run the dependency and license gates in
[`docs/RELEASE_CHECKLIST.md`](docs/RELEASE_CHECKLIST.md). If a dependency or
license changes, update this notice in the same pull request.
