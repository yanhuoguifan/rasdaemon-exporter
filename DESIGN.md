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

## Runtime loop

`--output PATH` selects the snapshot file (default
`/var/lib/node_exporter/textfile_collector/rasdaemon.prom`). `--interval DURATION`
defaults to `5s` and must be positive. CLI configuration is validated once before
opening the engine. After startup recovery, collect and commit immediately, then
wait the configured interval after each completed round. Run one serial loop;
do not overlap rounds or accumulate timer ticks. SIGINT/SIGTERM cancel waits and
context-aware queries. Once a file commit starts, retain its write ordering
without inserting cancellation between snapshot and state. Any recovery,
collection, or commit failure stops the engine; this implementation does not
retry automatically. Only cancellation without other errors is a successful
CLI shutdown; joined cleanup or I/O failures must remain visible.

After each successful poll, compare the consumed cursor with the committed
cursor. If it has not advanced, wait for the next round without creating a
backup, writing snapshot/state, or advancing generation. Pending historical
rows still require collection and commits even when SQLite has no new writes;
do not use database mtime or a file hash to decide whether work remains.
Initialization and backup recovery may still write files. Generation counts
data commits, not successful polls; unchanged output mtime is not evidence of
a stalled collector. Per-poll health gauges need a separate publication policy
and must not cause idle event snapshots to be rewritten.

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

Use one monotonically advancing ID cursor per active table. On first enrollment,
start at ID zero and read retained rows in bounded, ascending-ID batches. Rows
already present at startup and rows appended later use the same counting path;
there is no baseline boundary, second cursor, or baseline/live metric scope.
Fairly schedule bounded work by table so a large history or busy table cannot
starve quieter tables. The cursor advances only after its batch has been
successfully incorporated into the cumulative counters. Exact labels and
row-versus-weighted counting are defined in `METRICS.md`.

Starting defaults for implementation are a 5-second poll interval, at most 500 rows per query, and at most 6,000 rows across a poll. A cooperative 2-second per-poll work budget, 500 ms query deadline, and 250 ms SQLite busy timeout are tuning defaults, not latency guarantees; the deadline may not preempt filesystem sync. Use one SQLite connection and short read transactions. Finish mapping the batch and close its transaction before saving exporter state or writing output.

Probe backlog after each table cursor with an ordered, bounded query of at most
1,001 IDs per active table, not an unbounded `COUNT(*)` or `MAX(id)-cursor`
estimate. Report values through 1,000 exactly and 1,001 as “at least 1,001.”
Database size is the main database file's logical size. File-system free space
is covered by existing node-exporter metrics.

## State, failure, and output durability

The database is opened read-only. Use a bounded busy timeout and query-only
protection. Never use SQLite options that suppress normal locking, never write
the source database, and do not clean up or truncate rasdaemon history. Check
Do not replace or restore the source file while the collector is running; stop
it first. The exporter does not monitor path replacement.

Persist the source identity, snapshot generation, and one cursor per active
table in the exporter-owned state directory. Cumulative counter samples belong
to the metrics snapshot, not the cursor state. Hold one
exclusive process lock for the lifetime of the collector. Protect state and
output paths against unsafe symlink use; enforce state-size and applicable
per-table series bounds.

Compare each observed table maximum with its consumed cursor. If the maximum is lower, stop with a clear error and preserve the last valid state. This is a rollback detector, not a complete history auditor. The exporter deliberately does not hash anchor rows or reread consumed history: it will not detect old-row edits/deletions, and cannot detect every ID reuse while the maximum remains at or above the cursor. These violate the append-only input assumption and are not repaired automatically.

The exporter state may carry a format marker and integrity check so malformed or incompatible state is rejected. Do not build migration machinery or guarantee compatibility with state produced by earlier in-progress exporter implementations. Corrupt state, an unsupported current state format, disappearance of an enrolled table, or an ambiguous state/output combination is an explicit error; never silently reset counters or cursor progress. A newly appearing supported table remains pending as described above. An operator may explicitly reinitialize only after the old state and owned output have been safely backed up. Reinitialization starts fresh counters and cursors at zero; it does not migrate aggregates or modify the source database.

Automatic first initialization is permitted when state, output, and backup are
absent, after acquiring the lock. An orphaned generation-one output completes
initialization by writing zero-cursor state; generation one is trusted to be the
empty initialization snapshot. Other missing-state cases are errors. When state
exists, a matching backup can restore missing or damaged output. If all files
are removed, the process cannot distinguish a clean install from simultaneous
loss; this is not a continuity guarantee.

The text snapshot stores a positive generation in the comment
`# rasdaemon-exporter generation=N` and cumulative counter families.
This is not an additional exported metric. Snapshot load/save is independent
of SQLite and state. In-memory counts remain `uint64`; text encoding and parsing
use the Prometheus library's `float64` values directly. Exact snapshot restoration
is guaranteed for counts from zero through `2^53`, inclusive. Saving larger
counts fails before replacing the previous output; loading rejects parsed values
that are non-finite, negative, fractional, or above `2^53`. Loading does not
inspect raw decimal tokens for precision lost before the library returns a
floating-point value, so this guarantee applies to exporter-produced snapshots.

`Snapshot` contains generic `CounterFamily` and `CounterSample` data, not
table-specific keys or live collectors. Each table implements `MetricCodec`
to export independent label/count data and restore its own counts after
semantic validation. Restoration constructs a temporary map and replaces the
table's counts only on success, leaving its cursor unchanged. The snapshot
layer sorts metric families by name and label names within each sample, while
preserving the caller's sample order; it also owns text encoding/decoding and
file operations. The generating caller guarantees unique family names and
unique sample label sets within each family. Saving, loading, and table
restoration do not repeat sample-uniqueness checks. Table modules own metric names and label meaning. This
boundary does not require reflection, dynamic plugins, or automatic routing.

Emit only families with samples. Empty families have no HELP/TYPE lines, and
an empty snapshot consists only of its generation comment. Restoration of a
known, enabled table treats its absent family as empty counts, without changing
the cursor. The coordinating layer knows which tables are enabled and must
reject unknown/unrouted families; missing or invalid snapshot files remain
errors. The snapshot does not independently prove that an absent family had no
events. Delegate HELP/TYPE syntax to the Prometheus library instead of scanning
metadata or inserting temporary samples to preserve empty families.

For each successful collection round that advances a cursor:

1. Read and map bounded rows, then close the SQLite read transaction.
2. Validate candidate counters and checkpoints.
3. Durably save the current committed snapshot to `<snapshotPath>.prev`, leaving the current output readable. Its generation must match the committed state.
4. Encode candidate samples with the next generation and durably replace the `.prom` snapshot using a same-directory temporary file, `fsync`, atomic rename, and directory sync.
5. Durably save the matching generation and cursors in `state.json` using the same durable-file pattern.
6. Remove the backup on a best-effort basis. Cleanup failure does not undo a successful commit.

Output is published before state. Stop on backup, publication, or state-save
failure; do not continue consuming rows. On restart, a valid matching current
snapshot wins over any stale backup. Otherwise, a valid backup matching state
is atomically restored to the current output, and collection resumes from that
state's cursor and the backup's counts. Do not rename the current output away
or delete the backup before restoration is durable. The `.prev` suffix is not
scraped by node_exporter's textfile collector.

If neither the current snapshot nor the previous snapshot matches the committed
state, startup fails without rebuilding from SQLite history. First
initialization has no prior snapshot to back up. Only one backup is retained,
and stale-backup deletion is not required for correctness. The standalone
snapshot codec does not implement this coordination. Errors include context,
not complete event records.

SQLite busy/locked errors and bounded query timeouts are retryable on a later poll. Keep event counters and cursors unchanged for the failed task, set its query health to failure, preserve its last-success time, and commit/publish that health update when state/output remain safe and writable. Missing required columns, unreadable source values, and ID rollback are fatal rather than endlessly retried compatibility cases. On a fatal error, publish failure health only if it can be done from trusted state without advancing events; then exit with a diagnostic. Corrupt state, unsafe paths or storage failures may prevent any update. The old textfile can therefore remain visible with stale success values: operators must also check last-success time and node_exporter textfile modification time. No health metric guarantees a final failure sample after a crash or write failure.

## Metrics and display enrichment

Use the `rasdaemon_` public metric namespace in the evolving metric catalog.
Business counters represent rows or the explicitly documented source weight and
accumulate all retained records through the table's single cursor. The exporter
emits event-derived labels but no `node` or `scope` label. Prometheus scrape
configuration adds the stable host identity uniformly. Do not synthesize
zero-valued event series for absent tables or unseen label combinations. Do not
decode firmware payloads, infer root cause/severity, normalize source strings,
or add speculative labels. Apply only required Prometheus text escaping,
bounded input validation, and representation conversions explicitly listed in
`METRICS.md`.

Per active table, expose only query duration, query success, last successful
query time, and bounded backlog; expose database file size and business-series
count once per exporter process. Compute `rasdaemon_series_count` from the final
hardware counter sample set and publish it in the same atomic textfile,
excluding all collector gauges. No additional database query, persisted count
or textfile-size metric is needed. These are collection-health measures, not a
complete audit trail. Successful query/scan and durable state commit are
required before success time advances; an empty result is also success. Counter
growth during startup or backlog catch-up means records consumed by the exporter
over that Prometheus range, not necessarily the hardware event occurrence rate.
A newly observed series can start with a positive count without a preceding zero
sample, so increase-based rules can miss its first event. The exporter does not
guarantee one notification per hardware record.

PCI and block-device names are display-only enrichment. Resolve the current local mapping using bounded, read-only lookup (PCI metadata for AER and sysfs for block devices), outside the SQLite transaction and outside persistent aggregation identity. Use a modest bounded cache and execution/response limits. Missing, raced, unsupported, or stacked-device mappings become `unknown`; they never block counting or cursor advancement. A resolved name/model describes the current system, not proof of which physical device was present at event time. Do not write device configuration, issue hardware-management commands, or guess a leaf physical disk for a stacked block device.

For PCI lookup, use the fixed local command `/usr/bin/lspci -A linux-sysfs -D -vmm -s BDF` with an argument array, no shell or network access. Validate BDF only for lookup; never change the source label. Bound execution to one second and output to 64 KiB. Each resolver uses at most 256 cache entries, five-minute refresh eligibility, and at most one lookup per publication, with fair refresh scheduling. Block lookup reads `/sys/dev/block/major:minor` and explicitly related model attributes; it does not open block devices, issue ioctls or recursively guess through backing-device stacks. Names are optional and both lookup paths can be disabled. These limits are implementation defaults, not new per-table configuration frameworks.

Keep resource budgets explicit: persisted state is bounded to 2 MiB. The
devlink collector has a table-local 10,000-series limit. Per-value and label
limits, where applicable, are defined in METRICS.md; the devlink collector
preserves its four identity fields as scanned and replaces invalid UTF-8 runs
only in `msg` with `<invalid>` before grouping. It does not impose per-value
length or control-character checks. Budget exhaustion is reported rather than
silently dropping records. Dynamic DEVLINK messages can reach the table-local
series limit even when the source database is small.

## Decisions still open

- No first exporter release has been approved; the sole intended rasdaemon compatibility target is upstream `v0.8.4`, pending acceptance.
- Public repository/module path, license, package/distribution format, and release process remain open.
- Final metric details are governed by `METRICS.md`; implementation and acceptance are governed by `TESTING.md`.
- This design has not been implemented or validated as a whole. Earlier prototype test results are historical evidence only and do not establish conformance.
