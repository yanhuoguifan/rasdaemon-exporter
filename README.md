# rasdaemon-exporter

A small host-side Prometheus textfile producer for rasdaemon's SQLite event database. It reads incrementally, preserves approved source values, and publishes metrics through node_exporter without an HTTP listener.

**Design in progress.** No initial release has been finalized. A memory-only devlink query and counting engine is available for testing. Durable collection and metric publication are not implemented yet. The earlier source, configuration, service example and SQL reference remain in the legacy workspace directory as references; they are not copied here or claimed to implement this design.

## Makefile commands

Requires GNU Make, Go, CGO and a C compiler. The Makefile is a thin wrapper
around Go tooling and currently provides only these targets:

| Command | Behavior |
| --- | --- |
| `make build` | Build `build/rasdaemon-exporter` with CGO enabled |
| `make test` | Run `go test ./...` with CGO enabled |
| `make clean` | Remove only `build/rasdaemon-exporter`; keep unrelated files and Go caches |

The build directory is ignored by Git. Build and clean reject a symlinked
build directory. Use `GO=/path/to/go` to select the Go executable. There are no
installation, dependency-upgrade or service-management targets.

## Runtime paths

| Option | Default | Purpose |
| --- | --- | --- |
| `--db PATH` | `/var/lib/rasdaemon/ras-mc_event.db` | Existing SQLite source, opened read-only |
| `--state-dir PATH` | `/var/lib/rasdaemon-exporter` | Private directory for the single-instance lock and future collector state |

Both paths must be absolute. The source path must differ from the state
directory, its `collector.lock` file and its `state.json` file. The program creates the final state
directory with mode `0700` if absent; its parent must already exist and be
trusted. An existing directory must be owned by the running user, have mode
`0700`, and not be a symbolic link. Existing permissions are never changed.

```sh
make build
./build/rasdaemon-exporter --help
./build/rasdaemon-exporter --db /absolute/path/to/test.db --state-dir /absolute/path/to/private-state
```

Use synthetic databases and a writable private state path during development.
The default locations may require privileges; the program never elevates itself.
Instances using the same state directory compete for `collector.lock` before
opening SQLite. A second instance fails immediately while the lock is held.
Normal return or process termination releases the kernel lock; the file remains
and must not be deleted to force an unlock. Different state directories have
independent locks, even when the database path is the same.

Only `devlink_event` is registered. The CLI still performs startup checks only:
if that table exists, it returns `metrics publication is not implemented` and
releases its resources. A database without a supported table returns the
no-collectors error. This is not yet a continuously running exporter.

## In-memory collection API

`OpenEngine` acquires the lock and opens the source read-only. `Engine.Poll`
applies the round budget and calls the table collector for bounded batches.
`DevlinkCollector.CollectBatch` reads one batch and updates its public in-memory
`LastID` and cumulative `Counts` only after that batch succeeds. `Close` releases
the engine resources. Callers must validate configuration first and serialize
access to each collector and engine. Stop the collector before replacing or
restoring the database file. The collector does not monitor path replacement
while running.

Identical five-field label tuples accumulate in the same map entry; new tuples
start from zero. A failed batch leaves its counts and cursor unchanged. Earlier
successful batches in the same poll remain applied. There is no label-key JSON
encoding, custom map container or historical/live split. Devlink values are
kept as scanned: there are no per-value length, UTF-8 or control-character
checks; the aggregate series cap still applies.

This API does not read or write `state.json` or `.prom`. Opening a new engine
starts from ID zero with empty counters, even if a state file exists. It may
create only the private directory and `collector.lock`. Do not treat a memory
cursor as published progress or use this slice as a restart-safe exporter.

## Current documentation

There are **three authoritative design documents** and one acceptance document:

| Document | Sole responsibility |
| --- | --- |
| [DESIGN.md](DESIGN.md) | Runtime behavior, SQL access, active tables, incremental counting, state, safety and failure handling |
| [METRICS.md](METRICS.md) | All metric names, exact labels, source fields, count semantics and interpretation limits |
| [SOURCE_LAYOUT.md](SOURCE_LAYOUT.md) | Per-table source organization, cohesive shared responsibilities and readable English code |
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
