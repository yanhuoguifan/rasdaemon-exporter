# Design principles

These are the project's stable boundaries. Usage belongs in [README.md](README.md),
exported data in [METRICS.md](METRICS.md), and implementation details in source
comments. Git history records why behavior changed.

## Collection

- Read rasdaemon's existing SQLite database without modifying its schema or
  records. Database retention and repair are outside this project's scope.
- Consume records incrementally by increasing table IDs. IDs may have gaps;
  editing or reusing consumed records is not supported.
- Keep table-specific queries and metric mappings together. Preserve source
  information unless the metric contract explicitly describes a transformation.
- Export recorded events, not a new hardware diagnosis or severity taxonomy.

## Persistence and operation

- Run one collector per state directory. Keep cumulative counters and their
  cursor checkpoints consistent across restarts.
- Publish complete textfiles atomically. Retain a previous snapshot while
  committing its matching state, allowing interrupted commits to recover.
- Fail on unrecoverable data or persistence errors rather than silently
  resetting counters. Never automatically delete the source database or state.
- Poll serially. When no cursor advances, leave the snapshot and state unchanged.
  File age alone therefore does not indicate collector health.
- Keep deployment and process supervision separate from collection. No HTTP
  endpoint is needed: node-exporter exposes the generated textfile.

## Maintenance

Prefer Go's standard library and established format libraries over custom
frameworks. Keep tests separate from production code and use synthetic inputs.
Update user documentation when options, metrics, or operational requirements
change; keep local refactors and algorithm explanations in code comments and
commit messages instead of duplicating them across documents.
