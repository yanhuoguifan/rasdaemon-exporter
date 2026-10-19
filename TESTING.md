# Verification and Acceptance

The current design is under review, not a finalized release. The existing source is a reference implementation and has not been brought into alignment with all decisions in DESIGN.md, METRICS.md and SOURCE_LAYOUT.md.

## Evidence boundaries

Earlier isolated verification is preserved in the [legacy workspace record](../xian_os/observability/artifacts/rasdaemon-collector/验证记录.md). It records tests of the source at the time they ran, not acceptance of the latest design. Do not infer present correctness, deployment, or hardware fault coverage from those results. Historical format numbers in that record are evidence only, not supported development-version compatibility contracts. This is a local evidence link, not a runtime or test dependency of the standalone project.

The input reference is upstream rasdaemon v0.8.4 and the supplied schema-only fixture associated with the observed Arch package 0.8.4-2. It contains no production events. The compatibility target is v0.8.4; acceptance remains pending. Other releases are best effort as described in DESIGN.md.

The [host textfile verification record](../xian_os/observability/artifacts/host-textfile/验证记录.md) establishes a separate node_exporter ingestion path. It does not validate the RAS exporter implementation.

## Test source organization

Keep every test-only source and input under the project-root `tests/` directory. This includes Go test files, shared test helpers, the schema-only SQLite fixture, and the Python smoke test. Production directories such as `internal/collector/` must contain no `*_test.go` files or test fixtures. The proposed names and boundaries are listed in `SOURCE_LAYOUT.md`.

Use one external Go test package under `tests/` (for example, `collector_test`); because it is inside the same module root, it can import `internal/collector` under Go's `internal` import rule. Tests exercise the collector through its normal exported lifecycle API. Table-focused tests create isolated synthetic databases, run collection into a temporary textfile, and assert the emitted metric families, labels, and values. CLI tests run the built command as a subprocess rather than importing the root `main` package. Tests must not reach into private mapping functions: do not export implementation details solely for tests, use `go:linkname`, or add a test-only production API. A narrow fake at a real dependency boundary may be used where a test must avoid host device lookup.

The SQL fixture is input to tests only; the production binary must neither embed nor read it. The Python smoke test is test tooling, not a separately installed runtime script.

## Required acceptance checks

- Test every supported table against independent synthetic SQLite data and the exact labels and count semantics in METRICS.md.
- Verify SQL preparation, row iteration and typed reads, including missing required columns, NULL handling, invalid values and bounds. Extra unconsumed columns/tables must not trigger version rejection.
- Verify supported-table subsets and startup coverage, active-table disappearance, pending additions, no supported tables, and stable scheduling.
- Exercise baseline/live separation, bounded queries and backlog, restart continuity, observed maximum-ID rollback, database replacement, and documented append-only limitations.
- Exercise automatic first initialization, corrupted state rejection, exclusive locking, atomic state/output writes and failure ordering. Do not require migration compatibility with intermediate development prototypes.
- Verify GUID byte order, exact raw text and escaping, approved table-local message handling, shared OEM metric identity and publication-only device labels.
- Verify `rasdaemon_series_count` against distinct final business samples: an empty business sample set yields zero, baseline/live and separate MC families count independently, present zero-valued counters count, gauges and metadata do not, and failed publication leaves the old count with the old file. Device-label changes must not cumulatively inflate this snapshot gauge.
- Use fake PCI command output and synthetic sysfs trees; do not query or change production hardware during automated tests.
- Run Go tests, race detection, vet, build checks, the shared-source 150-code-line check and Prometheus text parsing against generated output.
- Run Go checks from the module root so the separate test package is included, for example `go test ./...`, `go test -race ./...`, `go vet ./...`, and `go build ./...`. If measuring coverage of the collector from the external test package, use `go test -coverpkg=./internal/collector ./tests`.
- Measure representative idle and event-burst workloads before making performance claims. Old benchmark results are not estimates for the redesigned implementation.

All tests use a dedicated temporary directory and synthetic data. No production database cleanup, schema writes, hardware injection, service changes or Kubernetes changes are part of this acceptance plan.

Record commands, toolchain/dependency versions, fixture provenance, outcomes and remaining limitations after implementation. Documentation cleanup alone is not a test run.
