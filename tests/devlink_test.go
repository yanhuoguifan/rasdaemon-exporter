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

var _ collector.MetricCodec = (*collector.DevlinkCollector)(nil)

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

func TestDevlinkCollectorPreservesSourceFieldsAndSanitizesInvalidMessage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	createDevlinkDatabase(t, path)
	db := openDevlinkDatabase(t, path)
	devlink := collector.DevlinkCollector{}

	long := strings.Repeat("x", 5000)
	control := "pci\x00\x01\x7f\n\r\t"
	invalidUTF8 := string([]byte{0xc3, 0x28, 0xff})
	want := map[collector.DevlinkLabels]uint64{
		{BusName: long, DevName: long, DriverName: long, ReporterName: long, Msg: long}:                                              1,
		{BusName: control, DevName: control, DriverName: control, ReporterName: control, Msg: control}:                               1,
		{BusName: invalidUTF8, DevName: invalidUTF8, DriverName: invalidUTF8, ReporterName: invalidUTF8, Msg: "<invalid>(<invalid>"}: 1,
	}
	insertDevlinkEvents(t, path,
		devlinkFixture{id: 1, busName: long, devName: long, driverName: long, reporterName: long, msg: long},
		devlinkFixture{id: 2, busName: control, devName: control, driverName: control, reporterName: control, msg: control},
		devlinkFixture{id: 3, busName: invalidUTF8, devName: invalidUTF8, driverName: invalidUTF8, reporterName: invalidUTF8, msg: invalidUTF8},
	)
	rows, err := devlink.CollectBatch(context.Background(), db, 500)
	if err != nil || rows != 3 {
		t.Fatalf("collect source strings = (%d, %v), want (3, nil)", rows, err)
	}
	if devlink.LastID != 3 || !reflect.DeepEqual(devlink.Counts, want) {
		t.Fatalf("unexpected source fields or sanitized message: cursor=%d counts=%v", devlink.LastID, devlink.Counts)
	}
}

func TestDevlinkCollectorSanitizesInvalidMessageRunsBeforeGrouping(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	createDevlinkDatabase(t, path)
	db := openDevlinkDatabase(t, path)
	devlink := collector.DevlinkCollector{}

	// Non-message fields pass through untouched, even when a source string
	// contains invalid UTF-8 bytes.
	busName := string([]byte{'p', 'c', 'i', 0xff})
	deviceName := "  eth0  "
	driverName := "driver\x00name"
	reporterName := "tx\nrx"
	message := "before <invalid> middle <invalid>( after"
	firstMessage := append([]byte("before "), 0xff, 0xfe)
	firstMessage = append(firstMessage, []byte(" middle ")...)
	firstMessage = append(firstMessage, 0xc3, 0x28)
	firstMessage = append(firstMessage, []byte(" after")...)
	secondMessage := append([]byte("before "), 0xfd)
	secondMessage = append(secondMessage, []byte(" middle ")...)
	secondMessage = append(secondMessage, 0xff, 0x28)
	secondMessage = append(secondMessage, []byte(" after")...)
	insertDevlinkEvents(t, path,
		devlinkFixture{id: 1, busName: busName, devName: deviceName, driverName: driverName,
			reporterName: reporterName, msg: string(firstMessage)},
		devlinkFixture{id: 2, busName: busName, devName: deviceName, driverName: driverName,
			reporterName: reporterName, msg: string(secondMessage)},
		devlinkFixture{id: 3, busName: busName, devName: deviceName, driverName: driverName,
			reporterName: reporterName, msg: message},
	)

	rows, err := devlink.CollectBatch(context.Background(), db, 500)
	if err != nil || rows != 3 {
		t.Fatalf("collect messages with invalid UTF-8 = (%d, %v), want (3, nil)", rows, err)
	}
	wantLabels := collector.DevlinkLabels{
		BusName: busName, DevName: deviceName, DriverName: driverName,
		ReporterName: reporterName, Msg: message,
	}
	if devlink.LastID != 3 || !reflect.DeepEqual(devlink.Counts, map[collector.DevlinkLabels]uint64{wantLabels: 3}) {
		t.Fatalf("collector = (%d, %v), want sanitized tuple count 3", devlink.LastID, devlink.Counts)
	}
}

func TestCollectedDevlinkMessageRoundTripsSanitizedSnapshot(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "events.db")
	rawMessage := append([]byte("before "), 0xff, 0xfe)
	rawMessage = append(rawMessage, []byte(" after")...)
	createDevlinkDatabase(t, databasePath,
		devlinkFixture{
			id: 1, busName: "pci", devName: "0000:01:00.0", driverName: "driver",
			reporterName: "tx", msg: string(rawMessage),
		},
	)
	db := openDevlinkDatabase(t, databasePath)
	devlink := collector.DevlinkCollector{}
	if rows, err := devlink.CollectBatch(context.Background(), db, 500); err != nil || rows != 1 {
		t.Fatalf("collect sanitized event = (%d, %v), want (1, nil)", rows, err)
	}

	path := filepath.Join(t.TempDir(), "metrics.prom")
	families := devlink.ExportMetrics()
	if err := collector.SaveSnapshot(path, &collector.Snapshot{Generation: 1, Families: families}); err != nil {
		t.Fatalf("save collected devlink snapshot: %v", err)
	}
	got, err := collector.LoadSnapshot(path)
	if err != nil {
		t.Fatalf("load collected devlink snapshot: %v", err)
	}
	if got.Generation != 1 {
		t.Fatalf("snapshot generation = %d, want 1", got.Generation)
	}
	restored := collector.DevlinkCollector{LastID: 90, Counts: map[collector.DevlinkLabels]uint64{{Msg: "old"}: 2}}
	if err := restored.RestoreMetrics(got.Families); err != nil {
		t.Fatalf("restore devlink metric family: %v", err)
	}
	want := map[collector.DevlinkLabels]uint64{
		{BusName: "pci", DevName: "0000:01:00.0", DriverName: "driver",
			ReporterName: "tx", Msg: "before <invalid> after"}: 1,
	}
	if restored.LastID != 90 || !reflect.DeepEqual(restored.Counts, want) {
		t.Fatalf("restored collector = (%d, %v), want unchanged cursor and counts %v", restored.LastID, restored.Counts, want)
	}
}

func TestDevlinkMetricExportIsIndependentAndIncludesEmptyFamily(t *testing.T) {
	devlink := collector.DevlinkCollector{}
	empty := devlink.ExportMetrics()
	if len(empty) != 1 || empty[0].Name != "rasdaemon_devlink_health_events_total" ||
		empty[0].Help == "" || len(empty[0].Samples) != 0 {
		t.Fatalf("empty devlink export = %#v, want one empty family", empty)
	}

	labels := collector.DevlinkLabels{BusName: "pci", DevName: "eth0", DriverName: "driver",
		ReporterName: "tx", Msg: "timeout"}
	devlink.Counts = map[collector.DevlinkLabels]uint64{labels: 4}
	exported := devlink.ExportMetrics()
	if len(exported) != 1 || len(exported[0].Samples) != 1 {
		t.Fatalf("devlink export = %#v, want one sample", exported)
	}
	exported[0].Samples[0].Labels["msg"] = "changed"
	exported[0].Samples[0].Value = 99
	if devlink.Counts[labels] != 4 || len(devlink.Counts) != 1 {
		t.Fatalf("mutating exported data changed collector counters: %v", devlink.Counts)
	}
}

func TestEmptyDevlinkSnapshotRestoresEmptyCounts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metrics.prom")
	devlink := collector.DevlinkCollector{LastID: 17}
	if err := collector.SaveSnapshot(path, &collector.Snapshot{
		Generation: 1,
		Families:   devlink.ExportMetrics(),
	}); err != nil {
		t.Fatalf("save empty devlink snapshot: %v", err)
	}

	snapshot, err := collector.LoadSnapshot(path)
	if err != nil {
		t.Fatalf("load empty devlink snapshot: %v", err)
	}
	if snapshot.Generation != 1 || len(snapshot.Families) != 0 {
		t.Fatalf("loaded empty devlink snapshot = %#v, want generation 1 without families", snapshot)
	}

	devlink.Counts = map[collector.DevlinkLabels]uint64{{Msg: "stale"}: 9}
	if err := devlink.RestoreMetrics(snapshot.Families); err != nil {
		t.Fatalf("restore empty devlink snapshot: %v", err)
	}
	if devlink.LastID != 17 || len(devlink.Counts) != 0 {
		t.Fatalf("restored empty devlink snapshot = (%d, %v), want unchanged cursor and empty counts", devlink.LastID, devlink.Counts)
	}
}

func TestDevlinkMetricRestoreValidatesBeforeReplacingCounts(t *testing.T) {
	initial := collector.DevlinkLabels{Msg: "existing"}
	collectorState := collector.DevlinkCollector{
		LastID: 42,
		Counts: map[collector.DevlinkLabels]uint64{initial: 7},
	}
	validLabels := map[string]string{
		"bus_name": "pci", "dev_name": "eth0", "driver_name": "driver",
		"reporter_name": "tx", "msg": "timeout",
	}
	valid := collector.CounterFamily{
		Name:    "rasdaemon_devlink_health_events_total",
		Help:    "Number of collected devlink event records.",
		Samples: []collector.CounterSample{{Labels: validLabels, Value: 3}},
	}
	invalidFamilies := []struct {
		name     string
		families []collector.CounterFamily
	}{
		{name: "extra family", families: []collector.CounterFamily{valid, {Name: "unexpected"}}},
		{name: "wrong family", families: []collector.CounterFamily{{Name: "unexpected", Help: valid.Help}}},
		{name: "missing label", families: []collector.CounterFamily{{
			Name: valid.Name, Help: valid.Help,
			Samples: []collector.CounterSample{{Labels: map[string]string{"bus_name": "pci"}}},
		}}},
		{name: "extra label", families: []collector.CounterFamily{{
			Name: valid.Name, Help: valid.Help,
			Samples: []collector.CounterSample{{Labels: map[string]string{
				"bus_name": "pci", "dev_name": "eth0", "driver_name": "driver",
				"reporter_name": "tx", "msg": "timeout", "node": "host",
			}}},
		}}},
	}
	for _, test := range invalidFamilies {
		name, families := test.name, test.families
		t.Run(name, func(t *testing.T) {
			before := maps.Clone(collectorState.Counts)
			if err := collectorState.RestoreMetrics(families); err == nil {
				t.Fatal("RestoreMetrics succeeded; want validation error")
			}
			if collectorState.LastID != 42 || !reflect.DeepEqual(collectorState.Counts, before) {
				t.Fatalf("failed restore changed collector state: (%d, %v)", collectorState.LastID, collectorState.Counts)
			}
		})
	}

	tooMany := make([]collector.CounterSample, 10001)
	for index := range tooMany {
		labels := maps.Clone(validLabels)
		labels["msg"] = strconv.Itoa(index)
		tooMany[index] = collector.CounterSample{Labels: labels, Value: 1}
	}
	if err := collectorState.RestoreMetrics([]collector.CounterFamily{{Name: valid.Name, Help: valid.Help, Samples: tooMany}}); err == nil {
		t.Fatal("RestoreMetrics accepted more than the series limit")
	}
	if collectorState.LastID != 42 || !reflect.DeepEqual(collectorState.Counts, map[collector.DevlinkLabels]uint64{initial: 7}) {
		t.Fatal("series-limit error changed collector state")
	}

	changedHelp := valid
	changedHelp.Help = "Updated description; it does not change this metric's identity."
	if err := collectorState.RestoreMetrics([]collector.CounterFamily{changedHelp}); err != nil {
		t.Fatalf("restore valid metric family: %v", err)
	}
	wantLabels := collector.DevlinkLabels{BusName: "pci", DevName: "eth0", DriverName: "driver",
		ReporterName: "tx", Msg: "timeout"}
	if collectorState.LastID != 42 || !reflect.DeepEqual(collectorState.Counts, map[collector.DevlinkLabels]uint64{wantLabels: 3}) {
		t.Fatalf("successful restore = (%d, %v), want unchanged cursor and restored counts", collectorState.LastID, collectorState.Counts)
	}
	for name, families := range map[string][]collector.CounterFamily{
		"nil families":   nil,
		"empty families": {},
	} {
		if err := collectorState.RestoreMetrics(families); err != nil {
			t.Fatalf("restore %s: %v", name, err)
		}
		if collectorState.LastID != 42 || len(collectorState.Counts) != 0 {
			t.Fatalf("restoring %s = (%d, %v), want unchanged cursor and empty counts", name, collectorState.LastID, collectorState.Counts)
		}
	}
	if err := collectorState.RestoreMetrics([]collector.CounterFamily{{Name: valid.Name}}); err != nil {
		t.Fatalf("restore empty metric family: %v", err)
	}
	if collectorState.LastID != 42 || len(collectorState.Counts) != 0 {
		t.Fatalf("restoring empty family = (%d, %v), want unchanged cursor and empty counts", collectorState.LastID, collectorState.Counts)
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
