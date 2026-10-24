# Metrics

This document describes the metric contract. The devlink counter and two MC
counters are implemented. The other nine event counters and six
collector gauges below are planned; they are not currently available to scrape.

The exporter reports rasdaemon database records. A record is not necessarily a
distinct physical failure, and these metrics do not infer root cause. The
exporter adds no `node` or `scope` label. Configure Prometheus to add a stable
`node` label; Prometheus supplies `job` and `instance` when scraping.

## Common counter and label rules

Text labels use source text unless a table-specific mapping below says
otherwise. SQL NULL becomes an empty label unless explicitly mapped. Rows with
the same complete event-key label tuple accumulate together; auxiliary device
names and models do not change that persistent grouping key. Counters describe
recorded reports, not current fault state, and do not include event timestamps.
Startup catch-up can count older rows, so counter growth measures rows consumed
over the query window, not necessarily when the physical events occurred. A
series appears only after a matching row is consumed; unseen combinations are
not emitted as zero.

## Implemented metrics

| Metric | Source | Exported labels | Counter increment |
|---|---|---|---|
| `rasdaemon_devlink_health_events_total` | `devlink_event` | `bus_name, dev_name, driver_name, reporter_name, msg` | 1 per row |
| `rasdaemon_memory_errors_total` | `mc_event` | `error_type, mc, top_layer, middle_layer, lower_layer, dimm, err_msg` | `err_count` |
| `rasdaemon_memory_events_total` | `mc_event` | `error_type, mc, top_layer, middle_layer, lower_layer, dimm, err_msg` | 1 per row |

For devlink, each run of invalid UTF-8 bytes in `msg` is replaced by `<invalid>`
before grouping.

For MC, `error_type` retains the `err_type` source text; SQL NULL becomes an
empty label. `dimm` is the raw `label` text. Missing or negative `mc` and layer
values become `unknown`. Invalid UTF-8 runs in `err_msg` become `<invalid>`.
Labeled internal numeric locations in `err_msg` (address, offset, row, column,
and bank coordinates) become `<omitted>`;
`driver_detail` is not exported. Zero `err_count` adds zero errors but one
event; NULL or negative counts are rejected. Both MC counters use the same
label groups, including zero-valued error samples.

## Planned event counters

These additional mappings are not implemented and are not available to scrape.

| Metric | Source | Exported labels | Counter increment |
|---|---|---|---|
| `rasdaemon_pcie_aer_events_total` | `aer_event` | `dev_name, err_type, err_msg, device_name` | 1 per row |
| `rasdaemon_mce_events_total` | `mce_record` | `cpuvendor, cpuid, socketid, cpu, bank, bank_name, error_msg, mcistatus_msg, mcastatus_msg, mc_location` | 1 per row |
| `rasdaemon_arm_processor_events_total` | `arm_event` | `mpidr, affinity, error_type, error_flags, running_state, psci_state` | 1 per row |
| `rasdaemon_firmware_memory_events_total` | `extlog_event` | `etype, severity, fru_id, fru_text` | 1 per row; not `error_count` |
| `rasdaemon_block_io_error_events_total` | `disk_errors` | `dev, error, rwbs, device_name, device_model` | 1 per row |
| `rasdaemon_firmware_section_events_total` | `non_standard_event` | `sec_type, fru_id, fru_text, severity` | 1 per row |
| `rasdaemon_hisi_oem_events_total` | `hip08_oem_type1_event_v2`, `hip08_oem_type2_event_v2` | `record_type, version, soc_id, socket_id, nimbus_id, module_id, sub_module_id, err_severity` | 1 per row |
| `rasdaemon_hisi_pcie_local_events_total` | `hip08_pcie_local_event_v2` | `version, soc_id, socket_id, nimbus_id, sub_module_id, core_id, port_id, err_type, err_severity` | 1 per row |
| `rasdaemon_hisi_common_events_total` | `hisi_common_section_v2` | `version, soc_id, socket_id, totem_id, nimbus_id, sub_system_id, module_id, sub_module_id, core_id, port_id, err_type, pcie_info, err_severity` | 1 per row |

### Label mappings

- **`aer_event`:** `dev_name` and `err_type` retain their source values;
  `err_msg` omits internal address/row/column values and a trailing TLP Header
  payload. `device_name` is a best-effort current local PCI display name looked
  up from the source BDF; it is `unknown` when unavailable and is not an
  event-time identity. No error-bit or severity expansion is performed.
- **`mce_record`:** source integer fields use decimal text, not decoded vendor
  names or a new fault taxonomy. `error_msg`, `mcastatus_msg`, and `mc_location`
  omit internal addresses, row/column values, syndrome numbers, and dynamic
  `n_errors` values. Register dumps are excluded; `n_errors` is not a weight.
- **`arm_event`:** `mpidr`, `error_type`, and `error_flags` are not decoded into
  topology or a new classification. `running_state` and `psci_state` are
  signed decimal values (including `-1`), not health gauges. `error_count` is
  not a weight. `affinity` is decimal text, with a
  missing or negative value represented as `unknown`.
- **`extlog_event`:** integer `etype` and `severity` values are decimal text;
  `etype=-1` is retained. `fru_id` is rendered as a GUID using the standard
  little-endian layout of its first 4-, 2-, and 2-byte fields. `fru_text` is
  source text. `error_count` is not a weight; address and `cper_data` are not
  exported.
- **`disk_errors`:** `dev`, `error`, and `rwbs` retain rasdaemon's stored
  values; `error` is not remapped by this exporter. `device_name` and
  `device_model` are best-effort current sysfs display values for `dev`, and
  are `unknown` when lookup is unavailable. Sector, length, and command fields
  are not exported.
- **`non_standard_event`:** `sec_type` and `fru_id` are GUIDs rendered with
  the same standard byte layout as `extlog_event`; unknown GUIDs remain
  distinct. `fru_text` and `severity` retain source text. The binary `error`
  payload is not exported. Non-NULL GUIDs must be 16 bytes; malformed values
  are rejected rather than truncated, and all-zero GUIDs remain explicit.
- **HiSilicon/HIP08 tables:** source integer fields are decimal text and source
  text is retained; values are not decoded into a new taxonomy. The two OEM
  tables share one metric family and use `record_type="type1"` or `"type2"`
  to distinguish the source table. `version` is a label, not a parser switch.
  Register dumps are not exported. Rows in different tables are not
  deduplicated.

## Limits

- Each implemented table collector permits at most 10,000 distinct label groups.
  MC exports two counter series per group.
- Snapshot counters are exact through `2^53`, inclusive; larger values are
  rejected because Prometheus snapshot values use floating-point numbers.

## Collector gauges (planned; not emitted)

The following six gauges are part of the planned contract, but none is emitted
by the current implementation. Their publication cadence, particularly while
the database has no new rows, is not yet specified.

| Metric | Labels | Intended value |
|---|---|---|
| `rasdaemon_query_duration_seconds` | `table` | Duration of the most recent table polling attempt, in seconds |
| `rasdaemon_query_success` | `table` | `1` after a successful poll and any required commit; `0` after failure or before the first success |
| `rasdaemon_query_last_success_timestamp_seconds` | `table` | Unix timestamp of the last successful poll; `0` if none |
| `rasdaemon_database_size_bytes` | none | Logical size of the SQLite main database file |
| `rasdaemon_series_count` | none | Distinct hardware counter samples in the output snapshot; excludes gauges |
| `rasdaemon_backlog_records_lower_bound` | `table` | Pending-row count: exact from 0–1000, with `1001` meaning at least 1001 |

The backlog value is a bounded row count after the table cursor, not an ID
difference. Node-exporter already reports filesystem capacity, so this exporter
does not plan to duplicate filesystem-free-space metrics.

## Upstream references

Metric fields are derived from rasdaemon's SQLite records; this exporter does
not redefine the underlying kernel or rasdaemon event taxonomy. See the
[rasdaemon v0.8.4 source](https://github.com/mchehab/rasdaemon/tree/v0.8.4),
the [rasdaemon record code](https://github.com/mchehab/rasdaemon/blob/v0.8.4/ras-record.c),
the [Linux EDAC documentation](https://docs.kernel.org/driver-api/edac.html),
and the [Linux devlink health documentation](https://docs.kernel.org/networking/devlink/devlink-health.html).
