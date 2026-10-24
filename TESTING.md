# Testing

Run checks from the module root. SQLite support requires CGO and a C compiler.

```sh
make test
CGO_ENABLED=1 go test -race ./...
go vet ./...
make build
```

The tests use temporary SQLite databases and temporary files. They cover the
implemented table collectors, snapshots, state recovery,
locking, CLI behavior, and the collection loop. They do not access the host's
rasdaemon database, inject hardware faults, or test unimplemented event tables.
