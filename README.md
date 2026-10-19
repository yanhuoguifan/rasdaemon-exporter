# rasdaemon-exporter

A small host-side Prometheus textfile producer for rasdaemon's SQLite event database. It reads incrementally, preserves approved source values, and publishes metrics through node_exporter without an HTTP listener.

**Design in progress.** No initial release has been finalized. This directory is the home of the new standalone project and its current design. The new implementation has not yet been written. The earlier source, configuration, service example and SQL reference remain in the legacy workspace directory as references; they are not copied here or claimed to implement this design.

## Current documentation

There are **three authoritative design documents** and one acceptance document:

| Document | Sole responsibility |
| --- | --- |
| [DESIGN.md](DESIGN.md) | Runtime behavior, SQL access, active tables, incremental counting, state, safety and failure handling |
| [METRICS.md](METRICS.md) | All metric names, exact labels, source fields, count semantics and interpretation limits |
| [SOURCE_LAYOUT.md](SOURCE_LAYOUT.md) | Per-table source organization, shared responsibilities, English-language conventions and the 150-code-line limit |
| [TESTING.md](TESTING.md) | Evidence boundaries and acceptance requirements; not a claim that the target passed |

Read DESIGN.md first for behavior, METRICS.md for the exported data contract, and SOURCE_LAYOUT.md for code review. Do not duplicate field inventories in this README or runtime policy in each table's documentation. Keep each decision in its owning document and update links instead of appending dated override paragraphs.

## Compatibility policy

The rasdaemon compatibility target is **upstream v0.8.4 only**. This is a target to validate, not a completed acceptance claim. Other rasdaemon versions are best effort: use the same explicit SQL, continue when query execution and value reading work, and report actual errors. There are no upstream version gates, schema fingerprints, declared-type preflight checks, schema-version polling or historical query adapters.

This upstream compatibility target is different from development drafts of this exporter. Maintain **one latest design**, not several prototype compatibility contracts. Do not build state migrations, dual metric names, legacy APIs or transitional runtime behavior merely to accommodate intermediate development changes. Runtime corrupt-state checks and restart continuity still matter; they are safety properties, not promises to support earlier prototypes.

## Public boundaries

- Working public name: `rasdaemon-exporter`; metric namespace: `rasdaemon_`.
- One process, one output file, one implementation package with per-table source files.
- Project-authored release material is English. Source event text is preserved in its original language.
- No automatic source database cleanup, hardware fault injection or database repair.
- Repository ownership, final license and external publication remain separate decisions. This is not an official rasdaemon or Prometheus project.
- Host deployment and Kubernetes/Alertmanager changes require a separate task.

## Local references, not current specifications

- [Legacy workspace](../xian_os/observability/artifacts/rasdaemon-collector/README.md): earlier code, configuration, tests and historical evidence remain there unchanged. This local link is a development reference, not a dependency of the standalone project.

No legacy source, state, configuration or test artifact was deleted or migrated. New production source will belong in this project's `internal/collector/`, and all new test code and fixtures in `tests/`, as defined in SOURCE_LAYOUT.md. These source directories are not populated by this documentation move. Historical records impose no backward-compatibility obligation on this evolving design.
