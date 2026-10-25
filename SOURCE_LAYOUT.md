# Source Layout

```text
.
├── main.go
├── internal/collector/
│   ├── config.go
│   ├── engine.go
│   ├── sqlite.go
│   ├── registry.go
│   ├── table_devlink_event.go
│   ├── table_mc_event.go
│   ├── table_aer_event.go
│   ├── metrics.go
│   ├── state.go
│   └── files.go
├── tests/
├── Makefile
└── rasdaemon-exporter.service
```

- `main.go` handles command-line options, signals, and process exit status.
- `internal/collector/` contains configuration, SQLite access, the collection
  engine, per-table collectors, Prometheus snapshots, and persisted
  state.
- `tests/` contains the external Go test package and its synthetic fixtures.
- `Makefile` builds, tests, cleans, and installs the program. The systemd unit
  is `rasdaemon-exporter.service`.
