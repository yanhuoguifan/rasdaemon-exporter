# Source Layout

This document defines source-file responsibilities for a small Go exporter that reads rasdaemon's SQLite database and publishes Prometheus textfile metrics. Runtime behavior and safety rules belong in `DESIGN.md`; exact metric names, labels, and source-field meanings belong in `METRICS.md`.

The workspace prototype is a reference for existing behavior, not the target source organization. It has not yet been reorganized to this layout, and its existing tests are not acceptance evidence for the design described here.

## Project boundary

- Working project and executable name: `rasdaemon-exporter`.
- One process, one Go package for collector implementation, and one local textfile output for node_exporter.
- Keep production code, comments, identifiers, logs, CLI text, tests, and distributable documentation in English. Preserve source event strings as data.
- Do not build a hardware decoder, generic plugin framework, schema-audit tool, or version-adaptation framework.
- Repository publication, license selection, and upstream endorsement are separate decisions and are not changed by this source layout.

## Proposed source tree

```text
rasdaemon-exporter/
├── Makefile
├── .gitignore
├── main.go
├── go.mod
├── go.sum
├── config.example.json
├── rasdaemon-exporter.service
├── README.md
├── DESIGN.md
├── METRICS.md
├── SOURCE_LAYOUT.md
├── TESTING.md
├── CONTRIBUTING.md
├── SECURITY.md
├── tests/
│   ├── helpers_test.go
│   ├── table_mc_event_test.go
│   ├── table_aer_event_test.go
│   ├── table_mce_record_test.go
│   ├── table_arm_event_test.go
│   ├── table_extlog_event_test.go
│   ├── table_devlink_event_test.go
│   ├── table_disk_errors_test.go
│   ├── table_non_standard_event_test.go
│   ├── table_hip08_oem_type1_event_v2_test.go
│   ├── table_hip08_oem_type2_event_v2_test.go
│   ├── table_hip08_pcie_local_event_v2_test.go
│   ├── table_hisi_common_section_v2_test.go
│   ├── registry_test.go
│   ├── engine_test.go
│   ├── state_test.go
│   ├── sqlite_test.go
│   ├── prometheus_test.go
│   ├── cli_test.go
│   ├── smoke-test.py
│   └── testdata/
│       └── rasdaemon-0.8.4.sql
└── internal/collector/
    ├── config.go
    ├── registry.go
    ├── engine.go
    ├── sqlite.go
    ├── state.go
    ├── files.go
    ├── metrics.go
    ├── device_name.go
    ├── table_mc_event.go
    ├── table_aer_event.go
    ├── table_mce_record.go
    ├── table_arm_event.go
    ├── table_extlog_event.go
    ├── table_devlink_event.go
    ├── table_disk_errors.go
    ├── table_non_standard_event.go
    ├── table_hip08_oem_type1_event_v2.go
    ├── table_hip08_oem_type2_event_v2.go
    ├── table_hip08_pcie_local_event_v2.go
    ├── table_hisi_common_section_v2.go
    └── hisi_oem_common.go
```

The tree is a responsibility guide, not a requirement to create files that only forward calls. Keep the implementation in the single `internal/collector` package unless an actual independent package boundary improves ownership or testing. Keep all Go test source, test fixtures, and test-only smoke tooling under the top-level `tests/` directory; production directories must not contain test files or fixtures.

The files in `tests/` form a separate Go package within the same module, not a nested module. Since that package remains below the module root, it may import `internal/collector` under Go's `internal` import rule while keeping tests outside the production package.

## Per-table source modules

Each of the 12 table modules owns that table's explicit named SQL projection, row scan, source-field handling, metric-family descriptor, row-to-counter mapping, and short comments explaining non-obvious source semantics. Its focused tests live in the matching `tests/table_..._test.go` file. Do not maintain the same table mapping or metric definition in a second registry/catalog file.

| Source module | Responsibility boundary |
| --- | --- |
| `table_mc_event.go` | EDAC memory-controller events and their approved row-count semantics. |
| `table_aer_event.go` | PCIe AER event records and table-local handling of approved message fields. |
| `table_mce_record.go` | Machine Check records without collector-side MCA decoding. |
| `table_arm_event.go` | ARM processor error records using the approved SQLite values. |
| `table_extlog_event.go` | Firmware extended-log memory reports; one increment per row. |
| `table_devlink_event.go` | Devlink queries and counters, approved message handling, metric definitions, and label/count export and restoration through `MetricCodec`. |
| `table_disk_errors.go` | Block I/O error reports and their approved current-device display enrichment input. |
| `table_non_standard_event.go` | Generic firmware sections; do not decode the binary payload. |
| `table_hip08_oem_type1_event_v2.go` | HIP08 OEM type 1 records. |
| `table_hip08_oem_type2_event_v2.go` | HIP08 OEM type 2 records. |
| `table_hip08_pcie_local_event_v2.go` | HIP08 PCIe-local records. |
| `table_hisi_common_section_v2.go` | HiSilicon common-section records. |

The two HIP08 OEM modules remain separate table readers and cursor owners while referring to one shared family definition in `hisi_oem_common.go`. `record_type` distinguishes the two record sources. Do not merge their SQL or checkpoint state.

Table modules may contain their own small formatting or validation helper when it is specific to that table. There is no shared `compact_message.go`, global message rewriting pipeline, or cross-table normalization layer. Any approved transformation stays next to the source field it handles; the complete field contract remains in `METRICS.md`.

## Shared collector code

Shared code is intentionally small and limited to mechanics used across tables.

| File | Responsibility |
| --- | --- |
| `main.go` | Parse commands, handle process signals, invoke the collector, and map fatal errors to exit status. |
| `config.go` | Load defaults and validate paths, intervals, and resource bounds. |
| `registry.go` | Assemble table descriptors in stable order; do not duplicate per-table SQL or metric definitions. |
| `engine.go` | Coordinate the collector lifecycle, collection and snapshot publication; choose the runtime lock-file path. |
| `sqlite.go` | Open the source read-only, apply bounded query safeguards, and run compiled named projections. |
| `state.go` | Own state structures, JSON encoding/decoding, validation, generation/cursor transitions, identity checks and state load/save orchestration. |
| `files.go` | Provide private-directory preparation, locking a caller-supplied path, bounded regular-file reading and atomic byte-file replacement. No state structures, JSON rules or fixed state filenames. |
| `metrics.go` | Own generic snapshot/family/sample data and `MetricCodec`, Prometheus text encoding/decoding and snapshot load/save; sort family and label names but preserve caller sample order; use library float values with the inclusive `2^53` exact-count bound, without table-specific branches. |
| `device_name.go` | Perform optional bounded local device-name/model lookups outside counter identity and cursor logic. |

Responsibility and cohesion take priority over file length. Keep all state semantics in `state.go`, rather than splitting its model, codec, persistence orchestration and transitions solely to meet a line quota. Keep generic filesystem mechanics in `files.go`; these helpers receive paths and bytes, not state objects. Line counts are review information, not a hard pass/fail limit. Do not compress statements, abbreviate names, omit useful comments or add one-function wrapper files to reduce a count.

Comments should explain why a safety check exists, what a source field actually represents, which sentinel or NULL behavior is intentional, and why a failure stops or continues collection. Avoid comments that merely restate the next line. Use Go formatting and descriptive English names. If line counting is automated later, count Go code positions rather than applying a formatting-sensitive regular expression; this design does not require a checker.

## Module interaction

```text
main.go → config.go → engine.go
                       ├─ table module → sqlite.go
                       ├─ state.go → files.go
                       ├─ device_name.go (optional enrichment)
                       └─ metrics.go → textfile snapshot
```

`DESIGN.md` owns startup, state, cursor, database replacement, failure, bounds, and durability rules. This file only establishes source-module responsibilities. SQL text and selected field semantics are owned by each table module and documented centrally in `METRICS.md`; do not add SQL snippets or a second metrics inventory here.

## SQL and version scope

The rasdaemon compatibility target remains upstream v0.8.4 only, pending implementation and acceptance testing. Other upstream versions are best effort when the same queries and row reading work. Maintaining one latest exporter design does not mean tracking rasdaemon master automatically or expanding the upstream compatibility promise. Use explicit named columns and bound values. Do not add runtime release checks, schema hashes, DDL introspection, declared-type gates, per-release fallbacks, migrations, or semantic compatibility branches. Query preparation, execution, and row-scan errors identify the affected table and use the normal error path. A semantic change behind a still-readable SQL value is not automatically detectable.

The schema-only fixture is test input for named queries and row mapping. Tests should cover relevant present/absent fields and values without asserting a full database fingerprint or DDL compatibility gate. Tests from the workspace prototype do not establish that this proposed layout or current target has passed acceptance.

## Test organization

- Put all Go tests in the top-level `tests/` directory. Use one focused test file per source table plus focused files for shared behavior; do not put `*_test.go`, test fixtures, or smoke-test code beside production files.
- Use an external test package (for example, `collector_test`) and exercise table projections and mappings through the collector's normal exported lifecycle API using isolated synthetic SQLite databases and temporary output paths. Do not export private mapping helpers, add test-only production APIs, use `linkname`, or mirror internals solely to make tests easier.
- Keep only narrow dependency seams that represent real runtime boundaries (such as a bounded device-name lookup) injectable where needed. Do not introduce broad interfaces or a generic test framework.
- Keep schema fixtures and test-only helpers under `tests/`; production code must not embed the fixture or depend on test files.
- Tests must never read or modify the host's rasdaemon database. Do not create tests merely to mirror every source-file boundary, and do not test historical-version adapters that the project does not maintain.

## Source evidence

Comments and operator-facing documentation should name the rasdaemon/Linux source versions and tracepoint/table behavior actually checked. Do not infer runtime behavior from a source file at another version, or claim a host event sample without an explicitly approved read-only observation. Changing the upstream compatibility target requires a separate decision and new evidence; changing this development draft alone does not change that target.
