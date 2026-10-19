# rasdaemon Exporter Metrics

This document defines the intended Prometheus metric contract for the rasdaemon
textfile exporter. It describes data read from rasdaemon's SQLite event tables
and a small set of collector health metrics.

The exporter implementation is a reference for this contract, not evidence that
every metric below has been implemented or accepted. Existing tests cover the
current code; they do not validate target behavior that is not implemented yet.

The collector does not decode hardware records into a new taxonomy or infer a
root cause. Apart from the explicitly documented mappings and display helpers,
it exports values already stored by rasdaemon. It has no version-specific
compatibility adapters or schema-version matrix.

## Metric inventory

There are 18 metric families: 12 hardware counters and six gauges. Label names
are exact. The exporter emits event-derived labels but does not add host identity
or collection-lane labels. Configure Prometheus scraping to add a stable `node`
label; Prometheus also adds `job` and `instance`. Label order in exposition is
not semantically significant.

| Metric family | Type | SQLite source | Labels written by the exporter | Increment or value |
|---|---|---|---|---|
| `rasdaemon_memory_errors_total` | Counter | `mc_event` | `error_type, mc, top_layer, middle_layer, lower_layer, dimm, err_msg` | Add the row's `err_count` |
| `rasdaemon_memory_events_total` | Counter | `mc_event` | `error_type, mc, top_layer, middle_layer, lower_layer, dimm, err_msg` | Add one per row |
| `rasdaemon_pcie_aer_events_total` | Counter | `aer_event` | `dev_name, err_type, err_msg, device_name` | Add one per row |
| `rasdaemon_mce_events_total` | Counter | `mce_record` | `cpuvendor, cpuid, socketid, cpu, bank, bank_name, error_msg, mcistatus_msg, mcastatus_msg, mc_location` | Add one per row |
| `rasdaemon_arm_processor_events_total` | Counter | `arm_event` | `mpidr, affinity, error_type, error_flags, running_state, psci_state` | Add one per row |
| `rasdaemon_firmware_memory_events_total` | Counter | `extlog_event` | `etype, severity, fru_id, fru_text` | Add one per row; do not add `error_count` |
| `rasdaemon_devlink_health_events_total` | Counter | `devlink_event` | `bus_name, dev_name, driver_name, reporter_name, msg` | Add one per row |
| `rasdaemon_block_io_error_events_total` | Counter | `disk_errors` | `dev, error, rwbs, device_name, device_model` | Add one per row |
| `rasdaemon_firmware_section_events_total` | Counter | `non_standard_event` | `sec_type, fru_id, fru_text, severity` | Add one per row |
| `rasdaemon_hisi_oem_events_total` | Counter | `hip08_oem_type1_event_v2`, `hip08_oem_type2_event_v2` | `record_type, version, soc_id, socket_id, nimbus_id, module_id, sub_module_id, err_severity` | Add one per row; `record_type` is `type1` or `type2` according to the source table |
| `rasdaemon_hisi_pcie_local_events_total` | Counter | `hip08_pcie_local_event_v2` | `version, soc_id, socket_id, nimbus_id, sub_module_id, core_id, port_id, err_type, err_severity` | Add one per row |
| `rasdaemon_hisi_common_events_total` | Counter | `hisi_common_section_v2` | `version, soc_id, socket_id, totem_id, nimbus_id, sub_system_id, module_id, sub_module_id, core_id, port_id, err_type, pcie_info, err_severity` | Add one per row |
| `rasdaemon_query_duration_seconds` | Gauge | Collector state, per tracked table | `table` | Duration of the most recent table polling attempt, in seconds |
| `rasdaemon_query_success` | Gauge | Collector state, per tracked table | `table` | `1` after a successful table poll; `0` after a failed or not-yet-successful poll |
| `rasdaemon_query_last_success_timestamp_seconds` | Gauge | Collector state, per tracked table | `table` | Unix timestamp of the last successful table poll; `0` if none has succeeded |
| `rasdaemon_database_size_bytes` | Gauge | Filesystem stat of the SQLite database file | None | Logical file size in bytes |
| `rasdaemon_series_count` | Gauge | Final business sample set prepared for textfile publication | None | Number of distinct hardware counter series in that output snapshot; excludes collector gauges |
| `rasdaemon_backlog_records_lower_bound` | Gauge | Bounded query after a table's cursor | `table` | Exact count from 0 through 1000; `1001` means at least 1001 pending rows |

The exporter emits HELP and TYPE metadata for its metric families. A metadata
line does not create a sample. It does not pre-create zero-valued hardware
series for unseen devices, error types, or source tables. Per-table collector
samples apply to tables actually selected for collection; a table not present
in the source database is not represented by invented zero or failure samples.

## Shared counter semantics

In-memory counts use `uint64`. Snapshots use the Prometheus library's floating-
point representation and support exact integer counts through `2^53`, inclusive.
Larger in-memory counts cannot be published and cause an error instead of being
silently rounded. Snapshot loading checks the parsed floating-point value for
this range and integrality; externally modified decimal text may already have
been rounded by the parser.

- Each table has one ID cursor. All retained rows are counted through the same
  path, whether they were present at startup or appended later; there is no
  baseline/live split or `scope` label. Counter values are cumulative across
  successfully consumed rows.
- These are counts of rasdaemon database reports, not lifetime hardware
  counters. A row can represent a report containing multiple lower-level error
  indications. Unless stated otherwise, each row contributes exactly one.
- Labels use the source values and the small, explicit transformations listed
  below. SQL NULL is rendered as an empty label unless a metric documents a
  specific missing-value representation. Prometheus escaping changes only the
  exposition encoding, not the decoded label value.
- A metric family may have no sample until a corresponding row has been
  consumed. Do not interpret an absent business series as a measured zero or as
  proof that collection is healthy; use the per-table collector gauges too.
- The same underlying incident can appear in more than one rasdaemon table.
  In particular, generic firmware sections and vendor-decoded HISI/HIP08 rows
  may describe the same report. Do not sum different families as unique
  physical failures.

## Hardware counter details

### EDAC memory: `mc_event`

Both memory families use the same grouping labels. `dimm` is the database
`label` text, preserved as supplied; it can be generic, ambiguous, or empty and
is not guaranteed to identify one physical DIMM. `mc` and the layer labels come
from the matching database columns. A missing or negative location is shown as
`unknown` rather than being presented as a real hardware position.

`error_type` is the explicitly approved classification of `err_type`:

| `mc_event.err_type` | `error_type` |
|---|---|
| `Corrected` | `ce` |
| `Uncorrected` | `ue` |
| `Fatal` | `fatal` |
| `Deferred` | `deferred` |
| `Info` | `info` |
| Other or unavailable | `unknown` |

`rasdaemon_memory_errors_total` is weighted by `err_count`, the count reported
by the EDAC event. A zero `err_count` adds zero errors; it is not forced to one.
`rasdaemon_memory_events_total` counts database rows, so it still increases by
one for a zero-count row. Fatal, deferred, and informational categories remain
separate from CE and UE.

`err_msg` retains the useful source description while omitting the approved
internal location values (such as address, row, column, and bank-address
numbers). The exporter does not expose `driver_detail` as a label. These
messages are bounded; over-budget input is an error, not silently truncated or
merged into another series.

### PCIe Advanced Error Reporting: `aer_event`

`dev_name`, `err_type`, and `err_msg` preserve the database values except for
the approved removal of internal address/row/column values and a trailing TLP
Header payload from `err_msg`. Error bits are not split into multiple events
and severity is not remapped. Each row adds one report.

`device_name` is an auxiliary, current display name obtained from local PCI
device information using the source BDF, composed from the local PCI class,
vendor, and device names. It is `unknown` when lookup is not available or
cannot identify the device. It is not an event-time identity: a BDF may later
refer to a different device, and a name lookup can change without changing the
stored event count.

### Machine Check records: `mce_record`

The labels preserve selected source fields, not a new MCE classification:

- `cpuvendor`, `cpuid`, `socketid`, `cpu`, and `bank` are source integer values
  rendered as decimal text. They are not decoded into vendor, model, topology,
  or a presumed faulting unit.
- `bank_name` and `mcistatus_msg` are rasdaemon text values.
- `error_msg`, `mcastatus_msg`, and `mc_location` retain the problem and useful
  hardware-location text while omitting approved internal addresses,
  row/column values, syndrome numbers, and dynamic `n_errors` values.

Every row adds one. `n_errors` is not a row weight. The exporter does not infer
severity, context corruption, overflow, recovery outcome, or a CPU-core root
cause from MCE status bits. This metric is not interchangeable with the EDAC
CE/UE counters.

### ARM processor records: `arm_event`

Every database row adds one, even if it contains several processor error
records; `error_count` is not used as a weight. `mpidr` is the SQLite integer
rendered in decimal, not decoded into topology. `error_type` and `error_flags`
are preserved as text, including their order, whitespace, and repeated words.
`running_state` and `psci_state` are SQLite integer values rendered in decimal,
including `-1`; they are event-time context, not live processor-health gauges
or severity. `affinity` is rendered as its non-negative integer; a negative or
missing value is `unknown`. SQL NULL text/state values otherwise become empty
labels. The exporter does not decode PSCI state names or infer CE/UE.

### Firmware extended-memory reports: `extlog_event`

Each row adds one. `error_count` is actually an extended-record sequence value,
not an error multiplicity, and is not added to the counter.

- `etype` and `severity` preserve their SQLite integer values as decimal text.
  The `etype` sentinel `-1` is retained; it is not translated to a guessed
  error name. These are independent fields and are not combined into CE/UE.
- `fru_id` is the 16-byte GUID rendered in standard GUID text form: the first
  4-, 2-, and 2-byte fields use the GUID's little-endian layout and the final
  8 bytes retain their order. An all-zero GUID remains all zeroes.
- `fru_text` is the source description, without DIMM or model inference.
- Physical `address` is not exported. The `cper_data` blob is not read or
  decoded.

This table represents firmware extended-log memory sections, not every APEI,
GHES, or firmware event. FRU values may not identify a DIMM or even a unique
replaceable part.

### Devlink reports: `devlink_event`

The four identity labels (`bus_name`, `dev_name`, `driver_name`, and
`reporter_name`) preserve their SQLite text values. Empty and NULL values remain
empty. For `msg`, empty and NULL values remain empty, and valid text—including
whitespace, newlines, addresses, queue identifiers, and other dynamic values—is
preserved. Each run of invalid UTF-8 bytes in `msg` is replaced with the literal
`<invalid>` before grouping; the remaining message text is preserved. The
metrics layer does not sanitize source labels. The accepted trade-off is that
distinct messages can create distinct series. The devlink collector applies a
per-table series limit; the snapshot codec has no global series or file-size
limit.

Rows can originate from devlink health reports or a network transmit timeout.
`dev_name` can therefore be a bus device name or a network interface, and the
other fields can be empty. A row means rasdaemon recorded a report; it does not
establish hardware root cause or recovery outcome. Any filtering that rasdaemon
performed before inserting a row is not repeated by the exporter.

### Block I/O reports: `disk_errors`

`dev`, `error`, and `rwbs` are the database values. `error` is already a
rasdaemon text mapping of the kernel error code; the exporter does not map it
again. `rwbs` is kept as the complete operation/flag string, not reduced to
read/write/flush categories. Each row adds one; `sector` and `nr_sector` are
not exported or accumulated as bad-sector counts.

`device_name` and `device_model` are auxiliary values resolved from the current
read-only sysfs mapping for `dev` (`major:minor`). If the mapping is absent,
ambiguous, changes during lookup, or cannot reliably identify a stacked device,
the helper label is `unknown`; this must not discard the event. These values
describe the device at lookup time, not necessarily the historical device that
produced the row. They do not replace `dev` or define the persistent event
grouping key.

The metric counts errors captured through rasdaemon's block trace path. An
abort action by itself is not a row; a timed-out I/O may be absent if retry
succeeds, and a failure is recorded only if it reaches the observed error
completion path. This is neither a complete count of all failed I/O nor a
bad-block scan.

### Non-standard firmware sections: `non_standard_event`

Each row adds one. `sec_type` and `fru_id` are 16-byte GUIDs rendered as
standard GUID text: the first 4-, 2-, and 2-byte fields use the GUID's
little-endian layout and the final 8 bytes retain their order. Unknown GUIDs
remain distinct and are not mapped to an `unknown` bucket. `fru_text` and
`severity` preserve the SQLite text. NULL values become empty labels; an
all-zero GUID remains an explicit all-zero GUID. The binary `error` payload is
not read or decoded. A non-NULL GUID value that is not exactly 16 bytes cannot
be faithfully formatted; report it as invalid source data rather than
truncating it or inventing a replacement label.

There are upstream data-quality limitations that the exporter does not
repair:

- The examined rasdaemon writer passes a negative length to
  `sqlite3_bind_blob()` for `sec_type`; SQLite documents a negative BLOB length
  as undefined behavior. A malformed stored value cannot be made trustworthy
  by truncating or guessing a GUID.
- Severity text can originate through kernel paths that do not share the same
  severity enumeration. The SQLite row has no reliable source discriminator;
  preserve the text and do not reinterpret it as a verified hardware severity.

### HiSilicon / HIP08 records

These counters expose the values rasdaemon stored in the vendor-specific
decoded tables. Integer columns are rendered as signed decimal text, including
zero and negative values. Text columns are preserved, including literal
`none` or `unknown`. NULL becomes an empty label. The exporter does not
re-decode module IDs, severity, validity bits, PCI information, or error codes.
The row's `version` is an ordinary source label, not a parser switch.

The two OEM source tables share `rasdaemon_hisi_oem_events_total`; the constant
`record_type` (`type1` or `type2`) keeps them separate. `pcie_info` is retained
as text and is not parsed into a BDF or device name. Register dumps are not
exported. Vendor-decoded rows can describe a section also present in
`non_standard_event`; counters across those tables are not deduplicated.

## Collector gauges

Where present, the `table` label is the SQLite table name. Per-table gauges
describe enrolled tables, not hypothetical tables that are absent. The exporter
does not emit a `node` label; Prometheus scrape configuration supplies the host
identity uniformly. Database size and business-series count have no exporter-
emitted labels.

- `rasdaemon_query_duration_seconds` is the duration of the most recent
  scheduled poll of that table, in seconds. It covers table query/scan and
  handling for that poll, not the whole collector loop or later output-file
  publication.
- `rasdaemon_query_success` is `1` when that table query, scan/mapping, and durable state commit complete successfully
  (an empty query result is success), and `0` when it fails or has not yet
  completed successfully. A failed poll must not refresh the last-success time.
- `rasdaemon_query_last_success_timestamp_seconds` is the Unix timestamp of
  the last successful poll, including a successful poll with no new rows. It
  is `0` before the first successful poll.
- `rasdaemon_database_size_bytes` is the SQLite main file's logical size from
  a filesystem stat. It is not allocated disk usage, a capacity limit, or a
  measurement of the containing filesystem's free space.
- `rasdaemon_series_count` counts distinct hardware counter samples in the
  final output snapshot. Identity is the metric name plus its complete exported
  label set, including any display-only device labels. Rows from startup history
  and later additions with the same labels contribute to the same counter; the
  two MC metric families count separately.
  Count a present zero-valued business sample, but not HELP/TYPE lines, unseen
  combinations, collector gauges, or this gauge itself. An output with no
  business samples reports zero. Compute the value from the existing output
  sample collection, without another SQL query or a separately persisted
  counter. Publish it atomically with those samples; if publication fails, the
  previous textfile and its previous count remain visible. This is an output
  cardinality measurement, not a row/event count, byte size, or count of all
  historical series retained by Prometheus. Display-label churn can increase
  historical Prometheus cardinality without increasing this snapshot count.
  No textfile-size metric is included in this design.
- `rasdaemon_backlog_records_lower_bound` is measured after the table's cursor
  with a query bounded at 1001 rows. Values 0–1000 are exact for that
  query snapshot; 1001 means at least 1001. It is not `MAX(id) - cursor`, an
  event age, or a total derived from ID gaps. Emit it only when the probe
  succeeds; absence is not zero.

Filesystem free space is already collected by node_exporter and is not
duplicated here. The exporter does not expose its internal cursors, source
identity, initialization progress, or error-detail metadata as additional
metric families.

Failure values are observable only when a safe health update can be persisted
and published. A crash, corrupt state, unsafe path or write failure may leave
the previous textfile showing `query_success=1`. Check last-success age and
node_exporter's textfile modification time as well; absence of an updated zero
does not prove that collection is still running. Retryable and fatal error
handling is defined in DESIGN.md.

## Input and label bounds

Prometheus escaping preserves the decoded value; do not silently truncate input.
Except for DEVLINK fields described above, source text must be valid UTF-8. The
approved raw-text policy permits LF, CR and TAB but rejects unsupported control
characters. Missing required columns are SQL errors, not NULL values. A non-NULL
GUID must be a 16-byte BLOB; NULL maps to an empty label. Integer source labels
preserve SQLite integer values in decimal except for the explicitly documented
MC/ARM location conventions.

The raw-text budget is 4096 bytes for MC `dimm`/`err_msg`, AER's three source
text labels, the five MCE description labels, ARM `error_type`/`error_flags`,
and EXTLOG/non-standard `fru_text`. Ordinary identifiers retain the 128-byte
budget. Auxiliary device names/models have a 512-byte budget and use `unknown`
on lookup failure or over-budget helper output. These are input limits, not
permission to normalize unknown source values. Table-specific collector limits
apply where documented; the snapshot codec does not impose a global series or
output-size cap. Exceeding an applicable business-data budget is an explicit
error, not a silently skipped event.

## Source evidence

The field review uses rasdaemon v0.8.4 (`5a1efb8f324498df8cbaaa5adff0e9db96f648a9`)
and Linux v7.2. Relevant rasdaemon files are `ras-record.c`, the per-event
`ras-*-handler.c` files, `non-standard-hisi_hip08.c`, and
`non-standard-hisilicon.c`. Relevant Linux definitions include
`include/ras/ras_event.h`, the devlink/block trace headers, ACPI GHES/ExtLog
handlers and `kernel/trace/blktrace.c`. These are source facts, not proof of
actual hardware-event coverage. The negative BLOB length issue is documented by
[SQLite's bind interface](https://www.sqlite.org/c3ref/bind_blob.html); it is an
upstream source risk, not evidence that the host database is already damaged.

Runtime behavior is defined in [DESIGN.md](DESIGN.md), source organization in
[SOURCE_LAYOUT.md](SOURCE_LAYOUT.md), and acceptance in [TESTING.md](TESTING.md).

## Interpretation limits

- `increase()` and `rate()` need samples over time. A newly seen series can
  begin with a positive value and may not yield a useful first-window increase.
- During startup or backlog catch-up, counter growth describes rows consumed by
  the collector, not necessarily the physical event occurrence rate.
- A successful poll proves the selected SQL and row handling completed; it
  does not prove every possible kernel or firmware event was emitted or stored.
- This exporter summarizes rasdaemon's retained database rows. It is not an
  event-log replacement, a root-cause decoder, or a guarantee of one alert per
  physical incident.
