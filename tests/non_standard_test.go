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

const nonStandardTestSchema = `CREATE TABLE non_standard_event (
	id INTEGER PRIMARY KEY,
	timestamp TEXT,
	sec_type BLOB,
	fru_id BLOB,
	fru_text TEXT,
	severity TEXT,
	error BLOB
)`

type nonStandardTestRow struct {
	id           any
	secType      any
	fruID        any
	fruText      any
	severity     any
	error        any
	emptySecType bool
	emptyFRUID   bool
}

func createNonStandardDatabase(t *testing.T, path string, rows ...nonStandardTestRow) {
	t.Helper()
	createSQLite(t, path, nonStandardTestSchema)
	insertNonStandardRows(t, path, rows...)
}

func insertNonStandardRows(t *testing.T, path string, rows ...nonStandardTestRow) {
	t.Helper()
	if len(rows) == 0 {
		return
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open synthetic non_standard_event database: %v", err)
	}
	db.SetMaxOpenConns(1)
	defer db.Close()
	for _, row := range rows {
		query := `INSERT INTO non_standard_event
			(id, timestamp, sec_type, fru_id, fru_text, severity, error)
			VALUES (?, '', ?, ?, ?, ?, ?)`
		switch {
		case row.emptySecType && row.emptyFRUID:
			query = `INSERT INTO non_standard_event
				(id, timestamp, sec_type, fru_id, fru_text, severity, error)
				VALUES (?, '', zeroblob(0), zeroblob(0), ?, ?, ?)`
			_, err = db.ExecContext(context.Background(), query,
				row.id, row.fruText, row.severity, row.error)
		case row.emptySecType:
			query = `INSERT INTO non_standard_event
				(id, timestamp, sec_type, fru_id, fru_text, severity, error)
				VALUES (?, '', zeroblob(0), ?, ?, ?, ?)`
			_, err = db.ExecContext(context.Background(), query,
				row.id, row.fruID, row.fruText, row.severity, row.error)
		case row.emptyFRUID:
			query = `INSERT INTO non_standard_event
				(id, timestamp, sec_type, fru_id, fru_text, severity, error)
				VALUES (?, '', ?, zeroblob(0), ?, ?, ?)`
			_, err = db.ExecContext(context.Background(), query,
				row.id, row.secType, row.fruText, row.severity, row.error)
		default:
			_, err = db.ExecContext(context.Background(), query,
				row.id, row.secType, row.fruID, row.fruText, row.severity, row.error)
		}
		if err != nil {
			t.Fatalf("insert synthetic non_standard_event row %v: %v", row.id, err)
		}
	}
}

func openNonStandardDatabase(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open synthetic non_standard_event database: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close synthetic non_standard_event database: %v", err)
		}
	})
	return db
}

func TestNonStandardCollectorFormatsGUIDsAndCountsRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "non-standard.db")
	firstGUID := extlogGUIDBytes()
	secondSecType := []byte{
		0x04, 0x03, 0x02, 0x01, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}
	secondGUID := []byte{
		0x44, 0x33, 0x22, 0x11, 0x66, 0x55, 0x88, 0x77,
		0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00,
	}
	createNonStandardDatabase(t, path,
		nonStandardTestRow{id: 1, secType: firstGUID, fruID: secondGUID, fruText: "firmware section", severity: "OEM-Severity-X", error: []byte("not exported")},
		nonStandardTestRow{id: 2, secType: firstGUID, fruID: secondGUID, fruText: "firmware section", severity: "OEM-Severity-X", error: []byte{0xff, 0x00}},
		nonStandardTestRow{id: 3, secType: nil, fruID: nil, fruText: nil, severity: nil},
		nonStandardTestRow{id: 4, secType: make([]byte, 16), fruID: make([]byte, 16), fruText: "all-zero GUID", severity: "0"},
		nonStandardTestRow{id: 5, secType: secondSecType, fruID: firstGUID,
			fruText: "vendor " + string([]byte{0xff}) + " report", severity: "Future severity"},
	)
	digest := fileDigest(t, path)
	extlog := collector.NonStandardCollector{}
	rows, err := extlog.CollectBatch(context.Background(), openNonStandardDatabase(t, path), 500)
	if err != nil || rows != 5 {
		t.Fatalf("CollectBatch() = (%d, %v), want (5, nil)", rows, err)
	}
	want := map[collector.NonStandardLabels]uint64{
		{SecType: "00112233-4455-6677-8899-aabbccddeeff", FRUID: "11223344-5566-7788-99aa-bbccddeeff00", FRUText: "firmware section", Severity: "OEM-Severity-X"}: 2,
		{FRUText: "", Severity: ""}: 1,
		{SecType: "00000000-0000-0000-0000-000000000000", FRUID: "00000000-0000-0000-0000-000000000000", FRUText: "all-zero GUID", Severity: "0"}:                         1,
		{SecType: "01020304-0000-0000-0000-000000000000", FRUID: "00112233-4455-6677-8899-aabbccddeeff", FRUText: "vendor <invalid> report", Severity: "Future severity"}: 1,
	}
	if extlog.LastID != 5 || !reflect.DeepEqual(extlog.Counts, want) {
		t.Fatalf("collector = (cursor %d, %#v), want cursor 5 and %#v", extlog.LastID, extlog.Counts, want)
	}
	if got := fileDigest(t, path); got != digest {
		t.Fatal("collection modified the source SQLite database")
	}
}

func TestNonStandardCollectorPagesAndPreservesExistingTotals(t *testing.T) {
	path := filepath.Join(t.TempDir(), "non-standard-pages.db")
	const total = 1007
	secType, fruID := extlogGUIDBytes(), []byte{
		0x44, 0x33, 0x22, 0x11, 0x66, 0x55, 0x88, 0x77,
		0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00,
	}
	rows := make([]nonStandardTestRow, total)
	for i := range rows {
		rows[i] = nonStandardTestRow{id: i + 1, secType: secType, fruID: fruID, fruText: "same section", severity: "notice"}
	}
	createNonStandardDatabase(t, path, rows...)
	extlog := collector.NonStandardCollector{Counts: map[collector.NonStandardLabels]uint64{{
		SecType: "00112233-4455-6677-8899-aabbccddeeff", FRUID: "11223344-5566-7788-99aa-bbccddeeff00",
		FRUText: "same section", Severity: "notice",
	}: 3}}
	db := openNonStandardDatabase(t, path)
	for round, wantCursor := range []int64{500, 1000, total} {
		gotRows, err := extlog.CollectBatch(context.Background(), db, 500)
		wantRows := 500
		if round == 2 {
			wantRows = 7
		}
		if err != nil || gotRows != wantRows || extlog.LastID != wantCursor {
			t.Fatalf("round %d = rows %d cursor %d err %v, want rows %d cursor %d", round+1, gotRows, extlog.LastID, err, wantRows, wantCursor)
		}
	}
	want := map[collector.NonStandardLabels]uint64{{
		SecType: "00112233-4455-6677-8899-aabbccddeeff", FRUID: "11223344-5566-7788-99aa-bbccddeeff00",
		FRUText: "same section", Severity: "notice",
	}: total + 3}
	if !reflect.DeepEqual(extlog.Counts, want) {
		t.Fatalf("cumulative counts = %#v, want %#v", extlog.Counts, want)
	}
}

func TestNonStandardCollectorRejectsInvalidGUIDsAtomically(t *testing.T) {
	for _, invalidColumn := range []string{"sec_type", "fru_id"} {
		for _, invalidLength := range []int{0, 15, 17} {
			t.Run(invalidColumn+"/"+strconv.Itoa(invalidLength), func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "invalid-guid.db")
				valid := extlogGUIDBytes()
				invalid := make([]byte, invalidLength)
				validRow := nonStandardTestRow{id: 1, secType: valid, fruID: valid, fruText: "valid", severity: "ok"}
				badRow := nonStandardTestRow{id: 2, secType: valid, fruID: valid, fruText: "bad", severity: "bad"}
				if invalidColumn == "sec_type" {
					badRow.secType = invalid
				} else {
					badRow.fruID = invalid
				}
				createNonStandardDatabase(t, path, validRow, badRow)
				db := openNonStandardDatabase(t, path)
				collectorUnderTest := collector.NonStandardCollector{Counts: map[collector.NonStandardLabels]uint64{{Severity: "old"}: 4}}
				if rows, err := collectorUnderTest.CollectBatch(context.Background(), db, 10); err == nil || rows != 0 {
					t.Fatalf("invalid GUID CollectBatch() = (%d, %v), want zero-row error", rows, err)
				}
				wantInitial := map[collector.NonStandardLabels]uint64{{Severity: "old"}: 4}
				if collectorUnderTest.LastID != 0 || !reflect.DeepEqual(collectorUnderTest.Counts, wantInitial) {
					t.Fatalf("collector changed on invalid GUID: cursor %d counts %#v", collectorUnderTest.LastID, collectorUnderTest.Counts)
				}
				query := "UPDATE non_standard_event SET " + invalidColumn + " = ? WHERE id = 2"
				if _, err := db.ExecContext(context.Background(), query, valid); err != nil {
					t.Fatalf("repair synthetic GUID: %v", err)
				}
				if rows, err := collectorUnderTest.CollectBatch(context.Background(), db, 10); err != nil || rows != 2 || collectorUnderTest.LastID != 2 {
					t.Fatalf("retry CollectBatch() = (%d, %v) cursor %d, want (2, nil) cursor 2", rows, err, collectorUnderTest.LastID)
				}
			})
		}
	}
}

func TestNonStandardCollectorKeepsCursorOnCancelRollbackAndOverflow(t *testing.T) {
	t.Run("canceled", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "canceled.db")
		createNonStandardDatabase(t, path, nonStandardTestRow{id: 1, secType: extlogGUIDBytes(), fruID: extlogGUIDBytes()})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		c := collector.NonStandardCollector{}
		if rows, err := c.CollectBatch(ctx, openNonStandardDatabase(t, path), 1); err == nil || rows != 0 || c.LastID != 0 {
			t.Fatalf("canceled CollectBatch() = (%d,%v) cursor %d", rows, err, c.LastID)
		}
	})
	t.Run("source ID rollback", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "rollback.db")
		createNonStandardDatabase(t, path, nonStandardTestRow{id: 1, secType: extlogGUIDBytes(), fruID: extlogGUIDBytes()})
		c := collector.NonStandardCollector{LastID: 2}
		if rows, err := c.CollectBatch(context.Background(), openNonStandardDatabase(t, path), 1); err == nil || rows != 0 || c.LastID != 2 {
			t.Fatalf("rollback CollectBatch() = (%d,%v) cursor %d", rows, err, c.LastID)
		}
	})
	t.Run("counter overflow", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "overflow.db")
		createNonStandardDatabase(t, path, nonStandardTestRow{id: 1, secType: extlogGUIDBytes(), fruID: extlogGUIDBytes(), fruText: "same", severity: "critical"})
		labels := collector.NonStandardLabels{SecType: "00112233-4455-6677-8899-aabbccddeeff", FRUID: "00112233-4455-6677-8899-aabbccddeeff", FRUText: "same", Severity: "critical"}
		c := collector.NonStandardCollector{Counts: map[collector.NonStandardLabels]uint64{labels: math.MaxUint64}}
		if rows, err := c.CollectBatch(context.Background(), openNonStandardDatabase(t, path), 1); err == nil || rows != 0 || c.LastID != 0 || c.Counts[labels] != math.MaxUint64 {
			t.Fatalf("overflow CollectBatch() = (%d,%v) cursor %d counts %#v", rows, err, c.LastID, c.Counts)
		}
	})
}

func TestNonStandardCollectorSeriesLimitFailureIsAtomic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "series-limit.db")
	createNonStandardDatabase(t, path, nonStandardTestRow{id: 1, secType: extlogGUIDBytes(), fruID: extlogGUIDBytes(), fruText: "new", severity: "critical"})
	c := collector.NonStandardCollector{Counts: make(map[collector.NonStandardLabels]uint64, 10000)}
	for i := 0; i < 10000; i++ {
		c.Counts[collector.NonStandardLabels{FRUText: "old" + strconv.Itoa(i)}] = 1
	}
	if rows, err := c.CollectBatch(context.Background(), openNonStandardDatabase(t, path), 1); err == nil || rows != 0 || c.LastID != 0 || len(c.Counts) != 10000 {
		t.Fatalf("series-limit CollectBatch() = (%d,%v) cursor %d series %d", rows, err, c.LastID, len(c.Counts))
	}
}

func TestNonStandardCollectorRestoresSnapshotLabelsVerbatim(t *testing.T) {
	labels := collector.NonStandardLabels{
		SecType: "00112233-4455-6677-8899-aabbccddeeff", FRUID: "00000000-0000-0000-0000-000000000000",
		FRUText: "raw value", Severity: "FutureVendorSeverity",
	}
	want := map[collector.NonStandardLabels]uint64{labels: 7}
	original := collector.NonStandardCollector{Counts: want}
	path := filepath.Join(t.TempDir(), "non-standard.prom")
	if err := collector.SaveSnapshot(path, &collector.Snapshot{Generation: 4, Families: original.ExportMetrics()}); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}
	snapshot, err := collector.LoadSnapshot(path)
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	var restored collector.NonStandardCollector
	if err := restored.RestoreMetrics(snapshot.Families); err != nil || !reflect.DeepEqual(restored.Counts, want) {
		t.Fatalf("restore = (%#v, %v), want %#v", restored.Counts, err, want)
	}
	if err := restored.RestoreMetrics(nil); err != nil || len(restored.Counts) != 0 {
		t.Fatalf("RestoreMetrics(nil) = (%v, %#v), want empty", err, restored.Counts)
	}
}

func TestEngineEnrollsNonStandardWithoutReplayingExistingTables(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "rasdaemon.db")
	devlinkSchema := `CREATE TABLE devlink_event (
		id INTEGER PRIMARY KEY, timestamp TEXT, bus_name TEXT, dev_name TEXT,
		driver_name TEXT, reporter_name TEXT, msg TEXT
	)`
	createSQLite(t, databasePath, devlinkSchema, nonStandardTestSchema)
	insertDevlinkEvents(t, databasePath, recoveryEvent(1, "already consumed"))
	guid := extlogGUIDBytes()
	insertNonStandardRows(t, databasePath, nonStandardTestRow{
		id: 1, secType: guid, fruID: guid, fruText: "new firmware report", severity: "vendor text",
	})
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
		t.Fatalf("collect after table enrollment: %v", err)
	}
	assertNonStandardEnginePair(t, fixture, 3, devlinkLabels)
	if err := engine.Close(); err != nil {
		t.Fatalf("close engine: %v", err)
	}
	engine = restoreRecoveryEngine(t, fixture)
	if err := engine.CollectAndCommit(context.Background()); err != nil {
		t.Fatalf("idle collect after restart: %v", err)
	}
	assertNonStandardEnginePair(t, fixture, 3, devlinkLabels)
}

func assertNonStandardEnginePair(t *testing.T, fixture engineRecoveryFixture, generation uint64, devlinkLabels collector.DevlinkLabels) {
	t.Helper()
	state, err := collector.LoadState(fixture.config.StateDir)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if state.Generation != generation || state.Tables["non_standard_event"] != 1 || state.Tables["devlink_event"] != 1 {
		t.Fatalf("state = %#v, want paired generation %d with both cursors at 1", state, generation)
	}
	snapshot, err := collector.LoadSnapshot(fixture.snapshotPath)
	if err != nil || snapshot.Generation != generation {
		t.Fatalf("load snapshot = (%#v, %v), want generation %d", snapshot, err, generation)
	}
	var nonStandard collector.NonStandardCollector
	var nonStandardFamilies []collector.CounterFamily
	var devlink collector.DevlinkCollector
	var devlinkFamilies []collector.CounterFamily
	for _, family := range snapshot.Families {
		switch family.Name {
		case "rasdaemon_non_standard_events_total":
			nonStandardFamilies = append(nonStandardFamilies, family)
		case "rasdaemon_devlink_health_events_total":
			devlinkFamilies = append(devlinkFamilies, family)
		default:
			t.Fatalf("unexpected metric family %q", family.Name)
		}
	}
	if err := nonStandard.RestoreMetrics(nonStandardFamilies); err != nil {
		t.Fatalf("restore non-standard family: %v", err)
	}
	guid := "00112233-4455-6677-8899-aabbccddeeff"
	want := map[collector.NonStandardLabels]uint64{{
		SecType: guid, FRUID: guid, FRUText: "new firmware report", Severity: "vendor text",
	}: 1}
	if !reflect.DeepEqual(nonStandard.Counts, want) {
		t.Fatalf("non-standard counts = %#v, want %#v", nonStandard.Counts, want)
	}
	if err := devlink.RestoreMetrics(devlinkFamilies); err != nil {
		t.Fatalf("restore devlink family: %v", err)
	}
	if !reflect.DeepEqual(devlink.Counts, map[collector.DevlinkLabels]uint64{devlinkLabels: 1}) {
		t.Fatalf("existing devlink counts changed: %#v", devlink.Counts)
	}
}
