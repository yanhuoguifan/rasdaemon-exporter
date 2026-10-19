package collector_test

import (
	"context"
	"database/sql"
	"errors"
	"maps"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"rasdaemon-exporter/internal/collector"
)

const devlinkSchema = `CREATE TABLE devlink_event (
	id INTEGER PRIMARY KEY,
	timestamp TEXT,
	bus_name TEXT,
	dev_name TEXT,
	driver_name TEXT,
	reporter_name TEXT,
	msg TEXT
)`

type devlinkFixture struct {
	id           int64
	timestamp    string
	busName      any
	devName      any
	driverName   any
	reporterName any
	msg          any
}

func openDevlinkDatabase(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open synthetic devlink database: %v", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(context.Background(), "PRAGMA busy_timeout=250"); err != nil {
		_ = db.Close()
		t.Fatalf("set synthetic devlink busy timeout: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close synthetic devlink database: %v", err)
		}
	})
	return db
}

func createDevlinkDatabase(t *testing.T, path string, events ...devlinkFixture) {
	t.Helper()
	createSQLite(t, path, devlinkSchema)
	insertDevlinkEvents(t, path, events...)
}

func insertDevlinkEvents(t *testing.T, path string, events ...devlinkFixture) {
	t.Helper()
	if len(events) == 0 {
		return
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open synthetic devlink database for insertion: %v", err)
	}
	db.SetMaxOpenConns(1)
	defer db.Close()

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin synthetic devlink insert transaction: %v", err)
	}
	statement, err := tx.PrepareContext(context.Background(), `
		INSERT INTO devlink_event
		(id, timestamp, bus_name, dev_name, driver_name, reporter_name, msg)
		VALUES (?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("prepare synthetic devlink insert: %v", err)
	}
	for _, event := range events {
		if _, err := statement.ExecContext(context.Background(), event.id, event.timestamp,
			event.busName, event.devName, event.driverName, event.reporterName, event.msg); err != nil {
			_ = statement.Close()
			_ = tx.Rollback()
			t.Fatalf("insert synthetic devlink event %d: %v", event.id, err)
		}
	}
	if err := statement.Close(); err != nil {
		_ = tx.Rollback()
		t.Fatalf("close synthetic devlink insert: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit synthetic devlink events: %v", err)
	}
}

func TestDevlinkCollectorAccumulatesCompleteRawLabelTuples(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	createDevlinkDatabase(t, path)
	db := openDevlinkDatabase(t, path)
	devlink := collector.DevlinkCollector{}
	if devlink.LastID != 0 || len(devlink.Counts) != 0 {
		t.Fatalf("zero-value collector = (%d, %v), want zero cursor and empty counts", devlink.LastID, devlink.Counts)
	}

	message := "  Tx \"timeout\" on queue 3\n雪  "
	base := collector.DevlinkLabels{
		BusName: "pci", DevName: "0000:01:00.0", DriverName: "mlx5_core",
		ReporterName: "tx", Msg: "MDD event",
	}
	nullAndEmpty := collector.DevlinkLabels{DevName: "eth0", DriverName: "mlx5_core"}
	insertDevlinkEvents(t, path,
		devlinkFixture{id: 1, busName: "pci", devName: base.DevName, driverName: base.DriverName, reporterName: base.ReporterName, msg: base.Msg},
		devlinkFixture{id: 2, busName: "pci", devName: base.DevName, driverName: base.DriverName, reporterName: base.ReporterName, msg: base.Msg},
		devlinkFixture{id: 3, busName: "pci", devName: base.DevName, driverName: base.DriverName, reporterName: base.ReporterName, msg: message},
		devlinkFixture{id: 4, busName: "pci2", devName: base.DevName, driverName: base.DriverName, reporterName: base.ReporterName, msg: base.Msg},
		devlinkFixture{id: 5, busName: "pci", devName: "0000:02:00.0", driverName: base.DriverName, reporterName: base.ReporterName, msg: base.Msg},
		devlinkFixture{id: 6, busName: "pci", devName: base.DevName, driverName: "mlx5_other", reporterName: base.ReporterName, msg: base.Msg},
		devlinkFixture{id: 7, busName: "pci", devName: base.DevName, driverName: base.DriverName, reporterName: "rx", msg: base.Msg},
		devlinkFixture{id: 8, busName: nil, devName: "eth0", driverName: "mlx5_core", reporterName: nil, msg: nil},
		devlinkFixture{id: 9, busName: "", devName: "eth0", driverName: "mlx5_core", reporterName: "", msg: ""},
	)
	rows, err := devlink.CollectBatch(context.Background(), db, 500)
	if err != nil || rows != 9 {
		t.Fatalf("collect initial devlink rows = (%d, %v), want (9, nil)", rows, err)
	}
	want := map[collector.DevlinkLabels]uint64{
		base: 2,
		{BusName: "pci", DevName: base.DevName, DriverName: base.DriverName, ReporterName: base.ReporterName, Msg: message}:    1,
		{BusName: "pci2", DevName: base.DevName, DriverName: base.DriverName, ReporterName: base.ReporterName, Msg: base.Msg}:  1,
		{BusName: "pci", DevName: "0000:02:00.0", DriverName: base.DriverName, ReporterName: base.ReporterName, Msg: base.Msg}: 1,
		{BusName: "pci", DevName: base.DevName, DriverName: "mlx5_other", ReporterName: base.ReporterName, Msg: base.Msg}:      1,
		{BusName: "pci", DevName: base.DevName, DriverName: base.DriverName, ReporterName: "rx", Msg: base.Msg}:                1,
		nullAndEmpty: 2,
	}
	if devlink.LastID != 9 || !reflect.DeepEqual(devlink.Counts, want) {
		t.Fatalf("collector = (%d, %v), want (%d, %v)", devlink.LastID, devlink.Counts, 9, want)
	}

	insertDevlinkEvents(t, path,
		devlinkFixture{id: 15, busName: "pci", devName: base.DevName, driverName: base.DriverName, reporterName: base.ReporterName, msg: base.Msg},
		devlinkFixture{id: 20, busName: "pci", devName: base.DevName, driverName: base.DriverName, reporterName: base.ReporterName, msg: "new message"},
	)
	rows, err = devlink.CollectBatch(context.Background(), db, 500)
	if err != nil || rows != 2 {
		t.Fatalf("collect appended rows = (%d, %v), want (2, nil)", rows, err)
	}
	want[base]++
	want[collector.DevlinkLabels{BusName: "pci", DevName: base.DevName, DriverName: base.DriverName,
		ReporterName: "tx", Msg: "new message"}]++
	if devlink.LastID != 20 || !reflect.DeepEqual(devlink.Counts, want) {
		t.Fatalf("collector after appends = (%d, %v), want (%d, %v)", devlink.LastID, devlink.Counts, 20, want)
	}
	rows, err = devlink.CollectBatch(context.Background(), db, 500)
	if err != nil || rows != 0 || devlink.LastID != 20 || !reflect.DeepEqual(devlink.Counts, want) {
		t.Fatalf("empty collection = (%d rows, cursor %d, %v), want no changes", rows, devlink.LastID, devlink.Counts)
	}
}

func TestDevlinkCollectorPreservesUnvalidatedSourceStrings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	createDevlinkDatabase(t, path)
	db := openDevlinkDatabase(t, path)
	devlink := collector.DevlinkCollector{}

	long := strings.Repeat("x", 5000)
	control := "pci\x00\x01\x7f\n\r\t"
	invalidUTF8 := string([]byte{0xc3, 0x28, 0xff})
	want := map[collector.DevlinkLabels]uint64{
		{BusName: long, DevName: long, DriverName: long, ReporterName: long, Msg: long}:                                    1,
		{BusName: control, DevName: control, DriverName: control, ReporterName: control, Msg: control}:                     1,
		{BusName: invalidUTF8, DevName: invalidUTF8, DriverName: invalidUTF8, ReporterName: invalidUTF8, Msg: invalidUTF8}: 1,
	}
	insertDevlinkEvents(t, path,
		devlinkFixture{id: 1, busName: long, devName: long, driverName: long, reporterName: long, msg: long},
		devlinkFixture{id: 2, busName: control, devName: control, driverName: control, reporterName: control, msg: control},
		devlinkFixture{id: 3, busName: invalidUTF8, devName: invalidUTF8, driverName: invalidUTF8, reporterName: invalidUTF8, msg: invalidUTF8},
	)
	rows, err := devlink.CollectBatch(context.Background(), db, 500)
	if err != nil || rows != 3 {
		t.Fatalf("collect raw source strings = (%d, %v), want (3, nil)", rows, err)
	}
	if devlink.LastID != 3 || !reflect.DeepEqual(devlink.Counts, want) {
		t.Fatalf("source strings were changed: cursor=%d counts=%v", devlink.LastID, devlink.Counts)
	}
}

func TestDevlinkCollectorReadsRequestedBatchLimits(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	createDevlinkDatabase(t, path)
	const totalRows = 6501
	fixtures := make([]devlinkFixture, totalRows)
	for index := range fixtures {
		fixtures[index] = devlinkFixture{id: int64(index + 1), busName: "pci", devName: "0000:01:00.0",
			driverName: "mlx5_core", reporterName: "tx", msg: "same event"}
	}
	insertDevlinkEvents(t, path, fixtures...)
	db := openDevlinkDatabase(t, path)
	devlink := collector.DevlinkCollector{}
	labels := collector.DevlinkLabels{BusName: "pci", DevName: "0000:01:00.0", DriverName: "mlx5_core",
		ReporterName: "tx", Msg: "same event"}

	for total := 0; total < totalRows; {
		rows, err := devlink.CollectBatch(context.Background(), db, 500)
		if err != nil {
			t.Fatalf("collect next bounded batch: %v", err)
		}
		if rows == 0 || rows > 500 {
			t.Fatalf("batch returned %d rows; want 1..500 before the end", rows)
		}
		total += rows
		if devlink.LastID != int64(total) || devlink.Counts[labels] != uint64(total) {
			t.Fatalf("after %d rows: cursor=%d count=%d", total, devlink.LastID, devlink.Counts[labels])
		}
	}
	rows, err := devlink.CollectBatch(context.Background(), db, 500)
	if err != nil || rows != 0 || devlink.LastID != totalRows || devlink.Counts[labels] != totalRows {
		t.Fatalf("final empty batch = (%d, %d, %v), want unchanged total %d", rows, devlink.LastID, err, totalRows)
	}
}

func TestDevlinkCollectorSeriesLimitRejectsWholeBatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	createDevlinkDatabase(t, path,
		devlinkFixture{id: 10001, msg: "event 0"},
		devlinkFixture{id: 10002, msg: "new group beyond the limit"})
	db := openDevlinkDatabase(t, path)

	counts := make(map[collector.DevlinkLabels]uint64, 10000)
	for index := 0; index < 10000; index++ {
		counts[collector.DevlinkLabels{Msg: "event " + strconv.Itoa(index)}] = 1
	}
	devlink := collector.DevlinkCollector{LastID: 10000, Counts: counts}
	before := maps.Clone(devlink.Counts)
	if rows, err := devlink.CollectBatch(context.Background(), db, 500); err == nil || rows != 0 {
		t.Fatalf("collection beyond series limit = (%d, %v), want error and no rows", rows, err)
	}
	if devlink.LastID != 10000 || !reflect.DeepEqual(devlink.Counts, before) {
		t.Fatal("series-limit error partially applied the batch")
	}
}

func TestDevlinkCollectorCounterOverflowRejectsWholeBatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	labels := collector.DevlinkLabels{BusName: "pci", Msg: "event"}
	createDevlinkDatabase(t, path, devlinkFixture{id: 1, busName: labels.BusName, msg: labels.Msg})
	db := openDevlinkDatabase(t, path)
	devlink := collector.DevlinkCollector{
		Counts: map[collector.DevlinkLabels]uint64{labels: math.MaxUint64},
	}
	if rows, err := devlink.CollectBatch(context.Background(), db, 500); err == nil || rows != 0 {
		t.Fatalf("overflowing collection = (%d, %v), want error and no rows", rows, err)
	}
	if devlink.LastID != 0 || devlink.Counts[labels] != math.MaxUint64 || len(devlink.Counts) != 1 {
		t.Fatalf("overflow error changed collector state: (%d, %v)", devlink.LastID, devlink.Counts)
	}
}

func TestDevlinkCollectorDoesNotApplyPartiallyScannedBatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	schema := `CREATE TABLE devlink_event (
		id,
		timestamp TEXT,
		bus_name TEXT,
		dev_name TEXT,
		driver_name TEXT,
		reporter_name TEXT,
		msg TEXT
	)`
	createSQLite(t, path, schema)
	db := openDevlinkDatabase(t, path)
	insert := `INSERT INTO devlink_event
		(id, timestamp, bus_name, dev_name, driver_name, reporter_name, msg)
		VALUES (?, ?, ?, ?, ?, ?, ?)`
	for _, row := range []struct {
		id  any
		msg string
	}{
		{id: 1, msg: "valid event"},
		{id: 1.5, msg: "fractional ID fails int64 scan"},
		{id: 2, msg: "later valid event"},
	} {
		if _, err := db.ExecContext(context.Background(), insert, row.id, "", "pci", "eth0", "driver", "reporter", row.msg); err != nil {
			t.Fatalf("insert synthetic scan-failure row %v: %v", row.id, err)
		}
	}
	devlink := collector.DevlinkCollector{}

	if rows, err := devlink.CollectBatch(context.Background(), db, 500); err == nil || rows != 0 || !strings.Contains(err.Error(), "scan devlink event") {
		t.Fatalf("collection scan failure = (%d, %v), want scan error and no rows", rows, err)
	}
	if devlink.LastID != 0 || len(devlink.Counts) != 0 {
		t.Fatalf("failed batch changed collector state: (%d, %v)", devlink.LastID, devlink.Counts)
	}
	if _, err := db.ExecContext(context.Background(), "DELETE FROM devlink_event WHERE id = 1.5"); err != nil {
		t.Fatalf("remove synthetic scan-failure row: %v", err)
	}
	rows, err := devlink.CollectBatch(context.Background(), db, 500)
	first := collector.DevlinkLabels{BusName: "pci", DevName: "eth0", DriverName: "driver",
		ReporterName: "reporter", Msg: "valid event"}
	last := first
	last.Msg = "later valid event"
	if err != nil || rows != 2 || devlink.LastID != 2 || !reflect.DeepEqual(devlink.Counts,
		map[collector.DevlinkLabels]uint64{first: 1, last: 1}) {
		t.Fatalf("retry after fixture repair = (%d, %d, %v), want both valid rows", rows, devlink.LastID, err)
	}
}

func TestDevlinkCollectorHandlesCancellationAndMaximumIDRollback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	labels := collector.DevlinkLabels{BusName: "pci", DevName: "0000:01:00.0", DriverName: "mlx5_core",
		ReporterName: "tx", Msg: "event"}
	createDevlinkDatabase(t, path, devlinkFixture{id: 5, busName: labels.BusName, devName: labels.DevName,
		driverName: labels.DriverName, reporterName: labels.ReporterName, msg: labels.Msg})
	db := openDevlinkDatabase(t, path)
	devlink := collector.DevlinkCollector{}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if rows, err := devlink.CollectBatch(canceled, db, 500); !errors.Is(err, context.Canceled) || rows != 0 {
		t.Fatalf("collect with canceled context = (%d, %v), want context.Canceled", rows, err)
	}
	if devlink.LastID != 0 || len(devlink.Counts) != 0 {
		t.Fatalf("canceled collection changed state: (%d, %v)", devlink.LastID, devlink.Counts)
	}
	if rows, err := devlink.CollectBatch(context.Background(), db, 500); err != nil || rows != 1 {
		t.Fatalf("collect valid row = (%d, %v), want one row", rows, err)
	}
	before := maps.Clone(devlink.Counts)
	if _, err := db.ExecContext(context.Background(), "DELETE FROM devlink_event"); err != nil {
		t.Fatalf("delete rows to simulate ID rollback: %v", err)
	}
	if rows, err := devlink.CollectBatch(context.Background(), db, 500); err == nil || rows != 0 {
		t.Fatalf("collection after maximum ID rollback = (%d, %v), want error", rows, err)
	}
	if devlink.LastID != 5 || !reflect.DeepEqual(devlink.Counts, before) {
		t.Fatalf("rollback error changed collector state: (%d, %v)", devlink.LastID, devlink.Counts)
	}
}

func TestDevlinkCollectorRetriesAfterBoundedSQLiteBusyError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	row := devlinkFixture{id: 1, busName: "pci", devName: "0000:01:00.0", driverName: "mlx5_core",
		reporterName: "tx", msg: "event"}
	createDevlinkDatabase(t, path, row)
	db := openDevlinkDatabase(t, path)
	devlink := collector.DevlinkCollector{}

	locker, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	locker.SetMaxOpenConns(1)
	connection, err := locker.Conn(context.Background())
	if err != nil {
		locker.Close()
		t.Fatal(err)
	}
	if _, err := connection.ExecContext(context.Background(), "BEGIN EXCLUSIVE"); err != nil {
		connection.Close()
		locker.Close()
		t.Fatalf("acquire synthetic exclusive lock: %v", err)
	}
	started := time.Now()
	rows, err := devlink.CollectBatch(context.Background(), db, 500)
	elapsed := time.Since(started)
	if err == nil || rows != 0 || devlink.LastID != 0 || len(devlink.Counts) != 0 {
		t.Fatalf("busy collection = (%d, %v, (%d, %v)), want bounded error and unchanged state",
			rows, err, devlink.LastID, devlink.Counts)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("busy collection waited %s; want bounded return under 2s", elapsed)
	}
	if _, err := connection.ExecContext(context.Background(), "ROLLBACK"); err != nil {
		t.Fatalf("release synthetic exclusive lock: %v", err)
	}
	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
	if err := locker.Close(); err != nil {
		t.Fatal(err)
	}
	if rows, err := devlink.CollectBatch(context.Background(), db, 500); err != nil || rows != 1 {
		t.Fatalf("retry after lock release = (%d, %v), want one row", rows, err)
	}
	if devlink.LastID != 1 || len(devlink.Counts) != 1 {
		t.Fatalf("retry state = (%d, %v), want one row", devlink.LastID, devlink.Counts)
	}
}

func TestEnginePollCreatesNoPersistentStateOrPromFile(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "events.db")
	createDevlinkDatabase(t, path, devlinkFixture{id: 1, busName: "pci", devName: "0000:01:00.0",
		driverName: "mlx5_core", reporterName: "tx", msg: "event"})
	before := fileDigest(t, path)
	cfg := validatedConfig(t, path)

	engine, err := collector.OpenEngine(context.Background(), cfg)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	if err := engine.Poll(context.Background()); err != nil {
		t.Fatalf("poll engine: %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("close engine: %v", err)
	}

	// Reopening proves Close released the process lock.
	engine, err = collector.OpenEngine(context.Background(), cfg)
	if err != nil {
		t.Fatalf("reopen engine after close: %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("close reopened engine: %v", err)
	}
	if got := directoryEntries(t, cfg.StateDir); !reflect.DeepEqual(got, []string{"collector.lock"}) {
		t.Fatalf("engine created persistent state files: %v", got)
	}
	if _, err := os.Stat(filepath.Join(directory, "rasdaemon.prom")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("engine unexpectedly created .prom output: %v", err)
	}
	if _, err := os.Stat(filepath.Join(cfg.StateDir, "state.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("engine unexpectedly created state.json: %v", err)
	}
	if got := fileDigest(t, path); got != before {
		t.Fatal("engine changed the read-only SQLite source")
	}
}
