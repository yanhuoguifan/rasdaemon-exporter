# rasdaemon Exporter Design

Status: evolving design, 2026-10-05. No first exporter release has been finalized. The intended upstream compatibility target is rasdaemon `v0.8.4` only, pending acceptance testing. This document describes the intended runtime contract; it does not claim that the current workspace prototype implements or passes it. Changes during design and implementation do not need to preserve compatibility with earlier prototype state files or metric names. Repository and license selection remain open.

The `v0.8.4` target is independent of exporter prototype versions. The exporter performs no rasdaemon-version gate. It uses fixed SQL projections: if a query and its value scans work, collection continues; SQL or scan errors are reported. Successful SQL does not prove that upstream semantics are unchanged.

## Documentation map

- [README.md](README.md) is the project entry point and operator-facing overview.
- [METRICS.md](METRICS.md) is the source of truth for metric names, labels, units, and per-table counting semantics.
- [SOURCE_LAYOUT.md](SOURCE_LAYOUT.md) describes the proposed English source tree and file responsibilities.
- [TESTING.md](TESTING.md) describes isolated tests and acceptance evidence.

Keep runtime behavior here, metric meaning in `METRICS.md`, code organization in `SOURCE_LAYOUT.md`, and verification requirements in `TESTING.md`. Do not use historical notes or the current prototype as an implicit compatibility contract.

## Goal and boundary

The exporter is a small host service that reads rasdaemon's SQLite event database and publishes accumulated Prometheus textfile metrics:

```text
kernel RAS reports → rasdaemon → SQLite database (read-only)
                                  ↓
                         rasdaemon exporter
                            ├── private durable state
                            └── one atomic .prom file
                                      ↓
                           existing node_exporter
                                      ↓
                                  Prometheus
```

The exporter counts and exposes selected values already stored by rasdaemon. It is not a kernel tracepoint consumer, hardware decoder, database repair tool, event-message queue, or full database auditor. It adds no Kubernetes workload, HTTP listener, remote-write client, database, schema migration, index, automatic retention, or event cleanup. The rasdaemon database remains the detailed event record.

## SQLite path selection

Expose `--db PATH` as the SQLite source-file parameter. Its default is
`/var/lib/rasdaemon/ras-mc_event.db`; omitting the flag selects this path without
requiring a configuration file. Implement the parameter with the initial
runtime skeleton rather than hard-coding a second path for tests. Tests always
pass their own temporary database path and never run against the host default.

Accept an absolute local filesystem path, not a caller-supplied SQLite URI or
connection string. Reject an explicitly empty path and a non-regular source;
do not create a missing database or its parent directories. Encode the path
when constructing the driver's internal URI so `?`, `#` and spaces in filenames
cannot inject connection options. Read-only access, bounded locking and query
timeouts remain program-controlled. No version or schema preflight is added.

The initial skeleton uses the standard-library CLI parser and defaults, not a
new JSON/environment/CLI precedence framework. Additional runtime configuration
can be added when its corresponding behavior is implemented. This section
does not authorize connecting to the physical host database during development.

## Input discovery and compatibility behavior

The compiled exporter knows these table names:

```text
aer_event                 arm_event
devlink_event             disk_errors
extlog_event              hip08_oem_type1_event_v2
hip08_oem_type2_event_v2  hip08_pcie_local_event_v2
hisi_common_section_v2    mc_event
mce_record                non_standard_event
```

At process startup, list known table names present in `sqlite_master`. This is table discovery only: do not inspect column declarations, indexes, constraints, schema-version counters, complete DDL, or schema hashes. Each registered table has a compiled SQL projection naming only the columns it consumes. Extra unrelated tables and columns are ignored. A missing selected column, SQL execution error, or incompatible runtime value is a normal, explicit collection error; do not guess a replacement field or silently skip an enrolled table.

On first safe initialization, persist the known supported tables present at startup as the active set, in registry order. A database with no known supported tables is an error. A supported table absent during enrollment is simply inactive: emit no business sample or per-table health sample for it. On later starts, disappearance of an active table is fatal. Newly appearing supported tables are reported as pending and remain inactive until an operator performs the explicit backed-up reinitialization procedure. Discovery occurs at startup, not continuously.

Do not compare a reported rasdaemon version or add per-release SQL fallbacks. The sole intended upstream compatibility target is `v0.8.4`, and acceptance remains pending. For other releases, fixed SQL is best-effort: continue if the projection and scans work, otherwise report the table and error. If upstream changes a field's meaning while retaining a queryable representation, the exporter cannot detect that semantic change. The v0.8.4 reference fixture anchors the target; it does not show acceptance has passed.

## Collection and counting

Use each table's integer `id` as the incremental key; never sort or resume by event timestamp. The input assumption is that each active table is append-only for the lifetime of its database file and receives unique, increasing integer IDs. ID gaps are allowed. Do not require the DDL to use `AUTOINCREMENT` and do not modify rasdaemon's schema.

At initial enrollment, capture each active table's maximum ID in one short, consistent read snapshot and durably save the boundaries before consuming rows. Keep separate cursors for:

- `baseline`: historical rows with `id <= baseline_end_id`; these increment baseline-scoped counters and do not represent newly observed faults.
- `live`: rows with `id > baseline_end_id`; these are collected independently, including while historical baseline import is still in progress.

Fairly schedule bounded work by `(table, lane)` so a large history or busy table cannot starve live data or quieter tables. Retain the two cursors until the baseline boundary has been consumed; never resample a committed boundary on restart. Metric scope and exact row-vs-weighted counting are defined in `METRICS.md`.

Starting defaults for implementation are a 5-second poll interval, at most 500 rows per query, and at most 6,000 rows across a poll. A cooperative 2-second per-poll work budget, 500 ms query deadline, and 250 ms SQLite busy timeout are tuning defaults, not latency guarantees; the deadline may not preempt filesystem sync. Use one SQLite connection and short read transactions. Finish mapping the batch and close its transaction before saving exporter state or writing output.

Probe live backlog with an ordered, bounded query of at most 1,001 IDs per active table, not an unbounded `COUNT(*)` or `MAX(id)-cursor` estimate. Report values through 1,000 exactly and 1,001 as “at least 1,001.” Database size is the main database file's logical size. File-system free space is covered by existing node-exporter metrics.

## State, failure, and output durability

The database is opened read-only. Use a bounded busy timeout and query-only protection. Never use SQLite options that suppress normal locking, never write the source database, and do not clean up or truncate rasdaemon history. Check source file device/inode identity at bounded collection points; replacement is fatal rather than a reason to start a new baseline silently.

Persist the active table set, per-table baseline boundary and cursors, observed maximum IDs, counter samples, and scheduling/health state in an exporter-owned state directory. Hold one exclusive process lock for the lifetime of the collector. Protect state and output paths against unsafe symlink use and enforce configured size/series bounds.

Compare each observed table maximum with the last durably observed maximum. If it moves backwards, or a previously non-empty active table is observed empty, stop with a clear error and preserve the last valid state. This is a rollback detector, not a complete history auditor. The exporter deliberately does not hash anchor rows or reread consumed history: it will not detect old-row edits/deletions, and cannot detect every ID reuse (for example, reuse while the observed maximum remains higher). These violate the append-only input assumption and are not repaired automatically.

The exporter state may carry a format marker and integrity check so malformed or incompatible state is rejected. Do not build migration machinery or guarantee compatibility with state produced by earlier in-progress exporter implementations. Corrupt state, an unsupported current state format, disappearance of an enrolled table, or an ambiguous state/output combination is an explicit error; never silently reset counters or resample a baseline. A newly appearing supported table remains pending as described above. An operator may explicitly reinitialize only after the old state and owned output have been safely backed up. Reinitialization starts fresh counters and a new baseline; it does not migrate aggregates or modify the source database.

Automatic first initialization is permitted only when both the exporter state and its owned output are absent, after acquiring the lock and passing source/configuration checks. A valid state with a missing output is recoverable by republishing from durable samples. An output without state is not a first install and must not be overwritten. If both files are removed, the process cannot distinguish a clean install from simultaneous loss; this ambiguity must be documented and must not be disguised as a continuity guarantee.

For each successful batch:

1. Read and map bounded rows, then close the SQLite read transaction.
2. Validate candidate counters and checkpoints.
3. Durably write the complete state snapshot using a same-directory temporary file, `fsync`, atomic rename, and directory sync.
4. Encode the committed samples and atomically replace the single `.prom` file using the same durable-file pattern.

State is committed before output. If output publication fails, retry publication from saved counters; do not recount the committed batch. If state commit fails, do not advance in-memory committed cursors or publish candidate counters. Errors are logged with a stable reason, table/path context, and recovery action; never log credentials or dump whole event records as a substitute for metrics.

SQLite busy/locked errors and bounded query timeouts are retryable on a later poll. Keep event counters and cursors unchanged for the failed task, set its query health to failure, preserve its last-success time, and commit/publish that health update when state/output remain safe and writable. Missing required columns, unreadable source values, ID rollback and database replacement are fatal rather than endlessly retried compatibility cases. On a fatal error, publish failure health only if it can be done from trusted state without advancing events; then exit with a diagnostic. Corrupt state, unsafe paths or storage failures may prevent any update. The old textfile can therefore remain visible with stale success values: operators must also check last-success time and node_exporter textfile modification time. No health metric guarantees a final failure sample after a crash or write failure.

## Metrics and display enrichment

Use the `rasdaemon_` public metric namespace in the evolving metric catalog. Business counters represent rows or the explicitly documented source weight; expose `scope="baseline"|"live"` so historical import is not confused with new collection. Do not synthesize zero-valued event series for absent tables or unseen label combinations. Do not decode firmware payloads, infer root cause/severity, normalize source strings, or add speculative labels. Apply only required Prometheus text escaping, bounded input validation, and representation conversions explicitly listed in `METRICS.md`.

Per active table, expose only query duration, query success, last successful query time, and bounded live backlog; expose database file size and business-series count once per node. Compute `rasdaemon_series_count` from the final hardware counter sample set and publish it in the same atomic textfile, excluding all collector gauges. No additional database query, persisted count or textfile-size metric is needed. These are collection-health measures, not a complete audit trail. Successful query/scan and durable state commit are required before success time advances; an empty result is also success. The rate of a live counter means records consumed by the exporter over that Prometheus range; catching up on backlog can make it differ from the hardware event occurrence rate. A newly observed series can start with a positive count without a preceding zero sample, so increase-based rules can miss its first event. The exporter does not guarantee one notification per hardware record.

PCI and block-device names are display-only enrichment. Resolve the current local mapping using bounded, read-only lookup (PCI metadata for AER and sysfs for block devices), outside the SQLite transaction and outside persistent aggregation identity. Use a modest bounded cache and execution/response limits. Missing, raced, unsupported, or stacked-device mappings become `unknown`; they never block counting or cursor advancement. A resolved name/model describes the current system, not proof of which physical device was present at event time. Do not write device configuration, issue hardware-management commands, or guess a leaf physical disk for a stacked block device.

For PCI lookup, use the fixed local command `/usr/bin/lspci -A linux-sysfs -D -vmm -s BDF` with an argument array, no shell or network access. Validate BDF only for lookup; never change the source label. Bound execution to one second and output to 64 KiB. Each resolver uses at most 256 cache entries, five-minute refresh eligibility, and at most one lookup per publication, with fair refresh scheduling. Block lookup reads `/sys/dev/block/major:minor` and explicitly related model attributes; it does not open block devices, issue ioctls or recursively guess through backing-device stacks. Names are optional and both lookup paths can be disabled. These limits are implementation defaults, not new per-table configuration frameworks.

Keep resource budgets explicit: the starting business-series budget is 10,000, persisted state is bounded to 2 MiB and text output to 4 MiB. Individual source-value and label limits are defined in METRICS.md. Budget exhaustion is reported rather than silently dropping records. Full raw-text retention, particularly DEVLINK messages, can reach the series budget even when the source database is small.

## Decisions still open

- No first exporter release has been approved; the sole intended rasdaemon compatibility target is upstream `v0.8.4`, pending acceptance.
- Public repository/module path, license, package/distribution format, and release process remain open.
- Final metric details are governed by `METRICS.md`; implementation and acceptance are governed by `TESTING.md`.
- This design has not been implemented or validated as a whole. Earlier prototype test results are historical evidence only and do not establish conformance.
