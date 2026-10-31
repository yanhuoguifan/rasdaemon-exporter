package collector_test

import (
	"context"
	"database/sql"
	"math"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	"rasdaemon-exporter/internal/collector"
)

const extlogTestSchema = `CREATE TABLE extlog_event (
	id INTEGER PRIMARY KEY,
	timestamp TEXT,
	etype INTEGER,
	error_count INTEGER,
	severity INTEGER,
	address INTEGER,
	fru_id BLOB,
	fru_text TEXT,
	cper_data BLOB
)`

type extlogTestRow struct {
	id         any
	timestamp  any
	etype      any
	errorCount any
	severity   any
	fruID      any
	fruText    any
	emptyGUID  bool
}

func createExtlogDatabase(t *testing.T, path string, rows ...extlogTestRow) {
	t.Helper()
	createSQLite(t, path, extlogTestSchema)
	insertExtlogRows(t, path, rows...)
}

func insertExtlogRows(t *testing.T, path string, rows ...extlogTestRow) {
	t.Helper()
	if len(rows) == 0 {
		return
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open synthetic extlog database: %v", err)
	}
	db.SetMaxOpenConns(1)
	defer db.Close()
	for _, row := range rows {
		query := `INSERT INTO extlog_event
			(id, timestamp, etype, error_count, severity, fru_id, fru_text)
			VALUES (?, ?, ?, ?, ?, ?, ?)`
		if row.emptyGUID {
			query = `INSERT INTO extlog_event
				(id, timestamp, etype, error_count, severity, fru_id, fru_text)
				VALUES (?, ?, ?, ?, ?, zeroblob(0), ?)`
			_, err = db.ExecContext(context.Background(), query,
				row.id, row.timestamp, row.etype, row.errorCount, row.severity, row.fruText)
		} else {
			_, err = db.ExecContext(context.Background(), query,
				row.id, row.timestamp, row.etype, row.errorCount, row.severity, row.fruID, row.fruText)
		}
		if err != nil {
			t.Fatalf("insert synthetic extlog row %v: %v", row.id, err)
		}
	}
}

func openExtlogDatabase(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open synthetic extlog database: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close synthetic extlog database: %v", err)
		}
	})
	return db
}

func extlogGUIDBytes() []byte {
	return []byte{
		0x33, 0x22, 0x11, 0x00, 0x55, 0x44, 0x77, 0x66,
		0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
	}
}

func TestExtlogCollectorFormatsGUIDAndCountsRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "extlog.db")
	guid := extlogGUIDBytes()
	createExtlogDatabase(t, path,
		extlogTestRow{id: 1, etype: 12, errorCount: 1000, severity: 2, fruID: guid, fruText: "DIMM-A"},
		extlogTestRow{id: 2, etype: 12, errorCount: nil, severity: 2, fruID: guid, fruText: "DIMM-A"},
		extlogTestRow{id: 3, etype: -1, errorCount: -90, severity: -2, fruID: nil, fruText: nil},
		extlogTestRow{id: 4, etype: nil, severity: nil, fruID: nil, fruText: "NULL labels"},
		extlogTestRow{id: 5, etype: 0, severity: 0, fruID: make([]byte, 16), fruText: "zero GUID"},
		extlogTestRow{id: 6, etype: 3, severity: 1, fruID: guid, fruText: "memory " + string([]byte{0xff}) + " event"},
	)
	digest := fileDigest(t, path)
	extlog := collector.ExtlogCollector{}
	rows, err := extlog.CollectBatch(context.Background(), openExtlogDatabase(t, path), 3)
	if err != nil || rows != 3 {
		t.Fatalf("first CollectBatch() = (%d, %v), want (3, nil)", rows, err)
	}
	rows, err = extlog.CollectBatch(context.Background(), openExtlogDatabase(t, path), 3)
	if err != nil || rows != 3 {
		t.Fatalf("second CollectBatch() = (%d, %v), want (3, nil)", rows, err)
	}
	want := map[collector.ExtlogLabels]uint64{
		{EType: "12", Severity: "2", FRUID: "00112233-4455-6677-8899-aabbccddeeff", FRUText: "DIMM-A"}:                2,
		{EType: "-1", Severity: "-2", FRUText: ""}:                                                                    1,
		{EType: "", Severity: "", FRUID: "", FRUText: "NULL labels"}:                                                  1,
		{EType: "0", Severity: "0", FRUID: "00000000-0000-0000-0000-000000000000", FRUText: "zero GUID"}:              1,
		{EType: "3", Severity: "1", FRUID: "00112233-4455-6677-8899-aabbccddeeff", FRUText: "memory <invalid> event"}: 1,
	}
	if extlog.LastID != 6 || !reflect.DeepEqual(extlog.Counts, want) {
		t.Fatalf("collector = (cursor %d, %#v), want cursor 6 and %#v", extlog.LastID, extlog.Counts, want)
	}
	if got := fileDigest(t, path); got != digest {
		t.Fatal("collection modified the source SQLite database")
	}
}

func TestExtlogCollectorPagesAndPreservesExistingTotals(t *testing.T) {
	path := filepath.Join(t.TempDir(), "extlog-pages.db")
	const total = 1007
	guid := extlogGUIDBytes()
	rows := make([]extlogTestRow, total)
	for i := range rows {
		rows[i] = extlogTestRow{id: i + 1, etype: 4, severity: 0, fruID: guid, fruText: "same report"}
	}
	createExtlogDatabase(t, path, rows...)
	extlog := collector.ExtlogCollector{Counts: map[collector.ExtlogLabels]uint64{{
		EType: "4", Severity: "0", FRUID: "00112233-4455-6677-8899-aabbccddeeff", FRUText: "same report",
	}: 2}}
	db := openExtlogDatabase(t, path)
	for round, wantCursor := range []int64{500, 1000, total} {
		rows, err := extlog.CollectBatch(context.Background(), db, 500)
		wantRows := 500
		if round == 2 {
			wantRows = 7
		}
		if err != nil || rows != wantRows || extlog.LastID != wantCursor {
			t.Fatalf("round %d = rows %d cursor %d err %v, want rows %d cursor %d", round+1, rows, extlog.LastID, err, wantRows, wantCursor)
		}
	}
	want := map[collector.ExtlogLabels]uint64{{
		EType: "4", Severity: "0", FRUID: "00112233-4455-6677-8899-aabbccddeeff", FRUText: "same report",
	}: total + 2}
	if !reflect.DeepEqual(extlog.Counts, want) {
		t.Fatalf("cumulative counts = %#v, want %#v", extlog.Counts, want)
	}
}

func TestExtlogInvalidGUIDLengthRejectsWholeBatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "extlog-invalid-guid.db")
	createExtlogDatabase(t, path,
		extlogTestRow{id: 1, etype: 2, severity: 0, fruID: extlogGUIDBytes(), fruText: "valid"},
		extlogTestRow{id: 2, etype: 3, severity: 1, fruID: make([]byte, 15), fruText: "invalid"},
	)
	db := openExtlogDatabase(t, path)
	extlog := collector.ExtlogCollector{Counts: map[collector.ExtlogLabels]uint64{{EType: "old"}: 4}}
	if rows, err := extlog.CollectBatch(context.Background(), db, 10); err == nil || rows != 0 {
		t.Fatalf("CollectBatch() = (%d, %v), want a zero-row error", rows, err)
	}
	initial := map[collector.ExtlogLabels]uint64{{EType: "old"}: 4}
	if extlog.LastID != 0 || !reflect.DeepEqual(extlog.Counts, initial) {
		t.Fatalf("collector changed on malformed GUID: cursor %d counts %#v", extlog.LastID, extlog.Counts)
	}
	if _, err := db.ExecContext(context.Background(), "UPDATE extlog_event SET fru_id = ? WHERE id = 2", extlogGUIDBytes()); err != nil {
		t.Fatalf("repair synthetic row for retry: %v", err)
	}
	if rows, err := extlog.CollectBatch(context.Background(), db, 10); err != nil || rows != 2 || extlog.LastID != 2 {
		t.Fatalf("retry CollectBatch() = (%d, %v) cursor %d, want (2, nil) cursor 2", rows, err, extlog.LastID)
	}
}

func TestExtlogCollectorKeepsCursorOnCancelRollbackAndOverflow(t *testing.T) {
	t.Run("canceled", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "extlog-canceled.db")
		createExtlogDatabase(t, path, extlogTestRow{id: 1, fruID: extlogGUIDBytes()})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		extlog := collector.ExtlogCollector{}
		if rows, err := extlog.CollectBatch(ctx, openExtlogDatabase(t, path), 1); err == nil || rows != 0 || extlog.LastID != 0 {
			t.Fatalf("canceled CollectBatch() = (%d,%v) cursor %d", rows, err, extlog.LastID)
		}
	})
	t.Run("source ID rollback", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "extlog-rollback.db")
		createExtlogDatabase(t, path, extlogTestRow{id: 1, fruID: extlogGUIDBytes()})
		extlog := collector.ExtlogCollector{LastID: 2}
		if rows, err := extlog.CollectBatch(context.Background(), openExtlogDatabase(t, path), 1); err == nil || rows != 0 || extlog.LastID != 2 {
			t.Fatalf("rollback CollectBatch() = (%d,%v) cursor %d", rows, err, extlog.LastID)
		}
	})
	t.Run("counter overflow", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "extlog-overflow.db")
		createExtlogDatabase(t, path, extlogTestRow{id: 1, etype: 2, severity: 0, fruID: extlogGUIDBytes(), fruText: "same"})
		labels := collector.ExtlogLabels{EType: "2", Severity: "0", FRUID: "00112233-4455-6677-8899-aabbccddeeff", FRUText: "same"}
		extlog := collector.ExtlogCollector{Counts: map[collector.ExtlogLabels]uint64{labels: math.MaxUint64}}
		if rows, err := extlog.CollectBatch(context.Background(), openExtlogDatabase(t, path), 1); err == nil || rows != 0 {
			t.Fatalf("overflow CollectBatch() = (%d,%v), want zero-row error", rows, err)
		}
		if extlog.LastID != 0 || extlog.Counts[labels] != math.MaxUint64 {
			t.Fatalf("collector changed on overflow: cursor %d counts %#v", extlog.LastID, extlog.Counts)
		}
	})
}

func TestExtlogCollectorRejectsSeventeenthFRUByteAtomically(t *testing.T) {
	path := filepath.Join(t.TempDir(), "extlog-seventeen.db")
	createExtlogDatabase(t, path, extlogTestRow{
		id: 1, etype: 2, severity: 0, fruID: append(extlogGUIDBytes(), 0), fruText: "invalid",
	})
	extlog := collector.ExtlogCollector{}
	if rows, err := extlog.CollectBatch(context.Background(), openExtlogDatabase(t, path), 1); err == nil || rows != 0 {
		t.Fatalf("17-byte FRU CollectBatch() = (%d,%v), want zero-row error", rows, err)
	}
	if extlog.LastID != 0 || len(extlog.Counts) != 0 {
		t.Fatalf("collector changed on 17-byte GUID: cursor %d counts %#v", extlog.LastID, extlog.Counts)
	}
}

func TestExtlogCollectorSeriesLimitFailureIsAtomic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "extlog-series-limit.db")
	createExtlogDatabase(t, path, extlogTestRow{id: 1, etype: 50000, severity: 0, fruText: "new group"})
	extlog := collector.ExtlogCollector{Counts: make(map[collector.ExtlogLabels]uint64, 10000)}
	for i := 0; i < 10000; i++ {
		labels := collector.ExtlogLabels{EType: strconv.Itoa(i), FRUText: "old group"}
		extlog.Counts[labels] = 1
	}
	if rows, err := extlog.CollectBatch(context.Background(), openExtlogDatabase(t, path), 1); err == nil || rows != 0 {
		t.Fatalf("series limit CollectBatch() = (%d,%v), want zero-row error", rows, err)
	}
	if extlog.LastID != 0 || len(extlog.Counts) != 10000 {
		t.Fatalf("collector changed on series-limit error: cursor %d series %d", extlog.LastID, len(extlog.Counts))
	}
}

func TestExtlogCollectorRestoresSnapshotLabelsVerbatim(t *testing.T) {
	labels := collector.ExtlogLabels{
		EType: "12", Severity: "-1", FRUID: "00112233-4455-6677-8899-aabbccddeeff", FRUText: "source text",
	}
	original := collector.ExtlogCollector{Counts: map[collector.ExtlogLabels]uint64{labels: 4}}
	path := filepath.Join(t.TempDir(), "extlog.prom")
	if err := collector.SaveSnapshot(path, &collector.Snapshot{Generation: 1, Families: original.ExportMetrics()}); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}
	snapshot, err := collector.LoadSnapshot(path)
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	var restored collector.ExtlogCollector
	if err := restored.RestoreMetrics(snapshot.Families); err != nil {
		t.Fatalf("RestoreMetrics() error = %v", err)
	}
	if !reflect.DeepEqual(restored.Counts, original.Counts) {
		t.Fatalf("restored counts = %#v, want %#v", restored.Counts, original.Counts)
	}
}

func TestEngineEnrollsExtlogAndDoesNotReplayAfterRestart(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "rasdaemon.db")
	devlinkSchema := `CREATE TABLE devlink_event (
		id INTEGER PRIMARY KEY, timestamp TEXT, bus_name TEXT, dev_name TEXT,
		driver_name TEXT, reporter_name TEXT, msg TEXT
	)`
	createSQLite(t, databasePath, devlinkSchema, extlogTestSchema)
	insertDevlinkEvents(t, databasePath, recoveryEvent(1, "already consumed"))
	insertExtlogRows(t, databasePath, extlogTestRow{id: 1, etype: 2, severity: 0, fruID: extlogGUIDBytes(), fruText: "firmware memory report"})
	config := validatedConfig(t, databasePath)
	fixture := engineRecoveryFixture{databasePath: databasePath, snapshotPath: config.SnapshotPath, config: config}
	devlinkLabels := recoveryLabels("already consumed")
	saveRecoveryState(t, fixture, 2, 1, sourceIdentityForTest(t, databasePath), map[string]int64{"devlink_event": 1})
	devlink := collector.DevlinkCollector{Counts: map[collector.DevlinkLabels]uint64{devlinkLabels: 1}}
	if err := collector.SaveSnapshot(fixture.snapshotPath, &collector.Snapshot{Generation: 2, Families: devlink.ExportMetrics()}); err != nil {
		t.Fatalf("save legacy snapshot: %v", err)
	}
	engine := restoreRecoveryEngine(t, fixture)
	if err := engine.CollectAndCommit(context.Background()); err != nil {
		t.Fatalf("collect after extlog enrollment: %v", err)
	}
	assertExtlogEnginePair(t, fixture, 3, 1, devlinkLabels)
	if err := engine.Close(); err != nil {
		t.Fatalf("close after extlog enrollment: %v", err)
	}
	engine = restoreRecoveryEngine(t, fixture)
	if err := engine.CollectAndCommit(context.Background()); err != nil {
		t.Fatalf("idle collect after restart: %v", err)
	}
	assertExtlogEnginePair(t, fixture, 3, 1, devlinkLabels)
}

func assertExtlogEnginePair(t *testing.T, fixture engineRecoveryFixture, generation uint64, cursor int64, devlinkLabels collector.DevlinkLabels) {
	t.Helper()
	state, err := collector.LoadState(fixture.config.StateDir)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if state.Generation != generation || state.Tables["extlog_event"] != cursor || state.Tables["devlink_event"] != 1 {
		t.Fatalf("state = %#v, want paired generation %d with both cursors", state, generation)
	}
	snapshot, err := collector.LoadSnapshot(fixture.snapshotPath)
	if err != nil || snapshot.Generation != generation {
		t.Fatalf("load snapshot = (%#v, %v), want generation %d", snapshot, err, generation)
	}
	var extlog collector.ExtlogCollector
	var extlogFamilies []collector.CounterFamily
	var devlink collector.DevlinkCollector
	var devlinkFamilies []collector.CounterFamily
	for _, family := range snapshot.Families {
		switch family.Name {
		case "rasdaemon_extlog_memory_events_total":
			extlogFamilies = append(extlogFamilies, family)
		case "rasdaemon_devlink_health_events_total":
			devlinkFamilies = append(devlinkFamilies, family)
		default:
			t.Fatalf("unexpected metric family %q", family.Name)
		}
	}
	if err := extlog.RestoreMetrics(extlogFamilies); err != nil {
		t.Fatalf("restore extlog family: %v", err)
	}
	wantExtlog := map[collector.ExtlogLabels]uint64{{
		EType: "2", Severity: "0", FRUID: "00112233-4455-6677-8899-aabbccddeeff", FRUText: "firmware memory report",
	}: 1}
	if !reflect.DeepEqual(extlog.Counts, wantExtlog) {
		t.Fatalf("extlog counts = %#v, want %#v", extlog.Counts, wantExtlog)
	}
	if err := devlink.RestoreMetrics(devlinkFamilies); err != nil {
		t.Fatalf("restore devlink family: %v", err)
	}
	if !reflect.DeepEqual(devlink.Counts, map[collector.DevlinkLabels]uint64{devlinkLabels: 1}) {
		t.Fatalf("existing devlink counts changed: %#v", devlink.Counts)
	}
}

func TestExtlogGUIDInvalidLengthAlsoRejectsNonNullEmptyBlob(t *testing.T) {
	path := filepath.Join(t.TempDir(), "extlog-empty-blob.db")
	createExtlogDatabase(t, path, extlogTestRow{id: 1, emptyGUID: true})
	extlog := collector.ExtlogCollector{}
	if rows, err := extlog.CollectBatch(context.Background(), openExtlogDatabase(t, path), 1); err == nil || rows != 0 {
		t.Fatalf("empty BLOB CollectBatch() = (%d,%v), want zero-row error", rows, err)
	}
	if extlog.LastID != 0 || len(extlog.Counts) != 0 {
		t.Fatalf("collector changed on empty BLOB: cursor %d counts %#v", extlog.LastID, extlog.Counts)
	}
}
