package collector_test

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	"rasdaemon-exporter/internal/collector"
)

const (
	hip08OEMType1TestTable = "hip08_oem_type1_event_v2"
	hip08OEMType2TestTable = "hip08_oem_type2_event_v2"
)

type hip08OEMTestRow struct {
	id                                 any
	version, socID, socketID, nimbusID any
	moduleID, subModuleID, errSeverity any
	regsDump                           any
}

type hip08OEMCollectorCase struct {
	name, table, metric string
	newCollector        func() *collector.HIP08OEMCollector
}

func hip08OEMCases() []hip08OEMCollectorCase {
	return []hip08OEMCollectorCase{
		{"type1", hip08OEMType1TestTable, "rasdaemon_hisi_oem_type1_events_total", collector.NewHIP08OEMType1Collector},
		{"type2", hip08OEMType2TestTable, "rasdaemon_hisi_oem_type2_events_total", collector.NewHIP08OEMType2Collector},
	}
}

func hip08OEMSchema(table string) string {
	return fmt.Sprintf("CREATE TABLE %s ("+
		"id INTEGER PRIMARY KEY, timestamp TEXT, version INTEGER, soc_id INTEGER, "+
		"socket_id INTEGER, nimbus_id INTEGER, module_id TEXT, sub_module_id TEXT, "+
		"err_severity TEXT, regs_dump TEXT)", table)
}

func createHIP08OEMDatabase(t *testing.T, path, table string, rows ...hip08OEMTestRow) {
	t.Helper()
	createSQLite(t, path, hip08OEMSchema(table))
	insertHIP08OEMRows(t, path, table, rows...)
}

func insertHIP08OEMRows(t *testing.T, path, table string, rows ...hip08OEMTestRow) {
	t.Helper()
	if len(rows) == 0 {
		return
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open synthetic HIP08 OEM database: %v", err)
	}
	db.SetMaxOpenConns(1)
	defer db.Close()
	query := fmt.Sprintf("INSERT INTO %s "+
		"(id,version,soc_id,socket_id,nimbus_id,module_id,sub_module_id,err_severity,regs_dump) "+
		"VALUES (?,?,?,?,?,?,?,?,?)", table)
	for _, row := range rows {
		if _, err := db.ExecContext(context.Background(), query,
			row.id, row.version, row.socID, row.socketID, row.nimbusID,
			row.moduleID, row.subModuleID, row.errSeverity, row.regsDump,
		); err != nil {
			t.Fatalf("insert synthetic HIP08 OEM row %v: %v", row.id, err)
		}
	}
}

func openHIP08OEMDatabase(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open synthetic HIP08 OEM database: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close synthetic HIP08 OEM database: %v", err)
		}
	})
	return db
}

func hip08OEMCommonLabels() collector.HIP08OEMLabels {
	return collector.HIP08OEMLabels{
		Version: "2", SoCID: "1", SocketID: "0", NimbusID: "3",
		ModuleID: "DDRC", SubModuleID: "Channel 0", ErrSeverity: "Corrected",
	}
}

func TestHIP08OEMCollectorsKeepIndependentFamiliesAndCounters(t *testing.T) {
	labels := hip08OEMCommonLabels()
	for _, source := range hip08OEMCases() {
		t.Run(source.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "oem.db")
			createHIP08OEMDatabase(t, path, source.table,
				hip08OEMTestRow{id: 1, version: 2, socID: 1, socketID: 0, nimbusID: 3,
					moduleID: "DDRC", subModuleID: "Channel 0", errSeverity: "Corrected", regsDump: "first"},
				hip08OEMTestRow{id: 2, version: 2, socID: 1, socketID: 0, nimbusID: 3,
					moduleID: "DDRC", subModuleID: "Channel 0", errSeverity: "Corrected", regsDump: "different"},
				hip08OEMTestRow{id: 3, version: 99, socID: -1, socketID: nil, nimbusID: 7,
					moduleID: "future module  ", subModuleID: "unknown block", errSeverity: "Future severity "},
				hip08OEMTestRow{id: 4},
			)
			digest := fileDigest(t, path)
			oem := source.newCollector()
			if rows, err := oem.CollectBatch(context.Background(), openHIP08OEMDatabase(t, path), 500); err != nil || rows != 4 {
				t.Fatalf("CollectBatch() = (%d, %v), want (4, nil)", rows, err)
			}
			want := map[collector.HIP08OEMLabels]uint64{
				labels: 2,
				{Version: "99", SoCID: "-1", NimbusID: "7", ModuleID: "future module  ", SubModuleID: "unknown block", ErrSeverity: "Future severity "}: 1,
				{}: 1,
			}
			if oem.LastID != 4 || !reflect.DeepEqual(oem.Counts, want) {
				t.Fatalf("collector = (cursor %d, %#v), want cursor 4 and %#v", oem.LastID, oem.Counts, want)
			}
			families := oem.ExportMetrics()
			if len(families) != 1 || families[0].Name != source.metric {
				t.Fatalf("families = %#v, want only %s", families, source.metric)
			}
			for _, sample := range families[0].Samples {
				if _, exists := sample.Labels["record_type"]; exists {
					t.Fatal("separate OEM family unexpectedly has a record_type label")
				}
			}
			if fileDigest(t, path) != digest {
				t.Fatal("collector modified the SQLite source")
			}
		})
	}

	// Identical IDs and labels in different source tables remain separate counts.
	type1, type2 := collector.NewHIP08OEMType1Collector(), collector.NewHIP08OEMType2Collector()
	for _, tc := range []struct {
		table string
		oem   *collector.HIP08OEMCollector
	}{{hip08OEMType1TestTable, type1}, {hip08OEMType2TestTable, type2}} {
		path := filepath.Join(t.TempDir(), "same-labels.db")
		createHIP08OEMDatabase(t, path, tc.table, hip08OEMTestRow{
			id: 1, version: 2, socID: 1, socketID: 0, nimbusID: 3,
			moduleID: "DDRC", subModuleID: "Channel 0", errSeverity: "Corrected",
		})
		if rows, err := tc.oem.CollectBatch(context.Background(), openHIP08OEMDatabase(t, path), 1); err != nil || rows != 1 {
			t.Fatalf("collect %s = (%d, %v), want (1, nil)", tc.table, rows, err)
		}
	}
	if type1.Counts[labels] != 1 || type2.Counts[labels] != 1 || type1.LastID != 1 || type2.LastID != 1 {
		t.Fatalf("OEM type states were merged: type1=%#v/%d type2=%#v/%d", type1.Counts, type1.LastID, type2.Counts, type2.LastID)
	}
	if type1.ExportMetrics()[0].Name == type2.ExportMetrics()[0].Name {
		t.Fatal("OEM types share a metric family")
	}
}

func TestHIP08OEMCollectorsPageAndPreserveExistingTotals(t *testing.T) {
	for _, source := range hip08OEMCases() {
		t.Run(source.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "oem-pages.db")
			const total = 1007
			rows := make([]hip08OEMTestRow, total)
			for i := range rows {
				rows[i] = hip08OEMTestRow{id: int64(i + 1), version: 2, socID: 1, socketID: 0, nimbusID: 3,
					moduleID: "DDRC", subModuleID: "Channel 0", errSeverity: "Corrected"}
			}
			createHIP08OEMDatabase(t, path, source.table, rows...)
			labels := hip08OEMCommonLabels()
			oem := source.newCollector()
			oem.Counts = map[collector.HIP08OEMLabels]uint64{labels: 3}
			db := openHIP08OEMDatabase(t, path)
			for round, cursor := range []int64{500, 1000, total} {
				rowsRead, err := oem.CollectBatch(context.Background(), db, 500)
				wantRows := 500
				if round == 2 {
					wantRows = 7
				}
				if err != nil || rowsRead != wantRows || oem.LastID != cursor {
					t.Fatalf("round %d = (%d, %v) cursor %d, want (%d, nil) cursor %d", round+1, rowsRead, err, oem.LastID, wantRows, cursor)
				}
			}
			if oem.Counts[labels] != total+3 {
				t.Fatalf("count = %d, want %d", oem.Counts[labels], total+3)
			}
		})
	}
}

func TestHIP08OEMCollectorsRejectPartialPagesAndRetry(t *testing.T) {
	for _, source := range hip08OEMCases() {
		t.Run(source.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "oem-retry.db")
			createHIP08OEMDatabase(t, path, source.table,
				hip08OEMTestRow{id: 1, version: 2, moduleID: "valid", errSeverity: "valid"},
				hip08OEMTestRow{id: 2, version: "not-an-integer", moduleID: "invalid", errSeverity: "invalid"},
			)
			db := openHIP08OEMDatabase(t, path)
			oem := source.newCollector()
			if rows, err := oem.CollectBatch(context.Background(), db, 10); err == nil || rows != 0 {
				t.Fatalf("invalid page = (%d, %v), want zero-row error", rows, err)
			}
			if oem.LastID != 0 || len(oem.Counts) != 0 {
				t.Fatalf("collector changed after scan error: cursor=%d counts=%#v", oem.LastID, oem.Counts)
			}
			if _, err := db.ExecContext(context.Background(), fmt.Sprintf("UPDATE %s SET version=2 WHERE id=2", source.table)); err != nil {
				t.Fatalf("repair synthetic row: %v", err)
			}
			if rows, err := oem.CollectBatch(context.Background(), db, 10); err != nil || rows != 2 || oem.LastID != 2 {
				t.Fatalf("retry = (%d, %v) cursor %d, want (2, nil) cursor 2", rows, err, oem.LastID)
			}
		})
	}
}

func TestHIP08OEMCollectorsCancellationRollbackAndOverflowAreAtomic(t *testing.T) {
	for _, source := range hip08OEMCases() {
		t.Run(source.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "oem-atomic.db")
			createHIP08OEMDatabase(t, path, source.table, hip08OEMTestRow{
				id: 1, version: 2, socID: 1, socketID: 0, nimbusID: 3,
				moduleID: "DDRC", subModuleID: "Channel 0", errSeverity: "Corrected",
			})
			db := openHIP08OEMDatabase(t, path)
			oem := source.newCollector()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if rows, err := oem.CollectBatch(ctx, db, 1); err == nil || rows != 0 || oem.LastID != 0 {
				t.Fatalf("canceled collection = (%d, %v) cursor %d", rows, err, oem.LastID)
			}
			oem.LastID = 2
			if rows, err := oem.CollectBatch(context.Background(), db, 1); err == nil || rows != 0 || oem.LastID != 2 {
				t.Fatalf("rollback collection = (%d, %v) cursor %d", rows, err, oem.LastID)
			}

			label := hip08OEMCommonLabels()
			oem = source.newCollector()
			oem.Counts = map[collector.HIP08OEMLabels]uint64{label: math.MaxUint64}
			if rows, err := oem.CollectBatch(context.Background(), db, 1); err == nil || rows != 0 || oem.LastID != 0 {
				t.Fatalf("overflow collection = (%d, %v) cursor %d", rows, err, oem.LastID)
			}
			if oem.Counts[label] != math.MaxUint64 {
				t.Fatalf("count changed on overflow: %#v", oem.Counts)
			}
		})
	}
}

func TestHIP08OEMCollectorsEnforceSeriesLimit(t *testing.T) {
	for _, source := range hip08OEMCases() {
		t.Run(source.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "oem-series.db")
			createHIP08OEMDatabase(t, path, source.table, hip08OEMTestRow{id: 1, version: 2})
			oem := source.newCollector()
			oem.Counts = make(map[collector.HIP08OEMLabels]uint64, 10000)
			for i := 0; i < 10000; i++ {
				oem.Counts[collector.HIP08OEMLabels{ModuleID: "old-" + strconv.Itoa(i)}] = 1
			}
			if rows, err := oem.CollectBatch(context.Background(), openHIP08OEMDatabase(t, path), 1); err == nil || rows != 0 || oem.LastID != 0 {
				t.Fatalf("series limit collection = (%d, %v) cursor %d", rows, err, oem.LastID)
			}
			if len(oem.Counts) != 10000 {
				t.Fatalf("series count changed at limit: %d", len(oem.Counts))
			}
		})
	}
}

func TestHIP08OEMCollectorRestoresIndependentMetricFamilies(t *testing.T) {
	labels := hip08OEMCommonLabels()
	type1 := collector.NewHIP08OEMType1Collector()
	type2 := collector.NewHIP08OEMType2Collector()
	type1.Counts = map[collector.HIP08OEMLabels]uint64{labels: 7}
	type2.Counts = map[collector.HIP08OEMLabels]uint64{labels: 11}
	family1, family2 := type1.ExportMetrics()[0], type2.ExportMetrics()[0]
	if family1.Name == family2.Name {
		t.Fatal("type1 and type2 share a metric family")
	}
	restored1, restored2 := collector.NewHIP08OEMType1Collector(), collector.NewHIP08OEMType2Collector()
	for _, tc := range []struct {
		name   string
		family collector.CounterFamily
		target *collector.HIP08OEMCollector
		want   uint64
	}{
		{"type1", family1, restored1, 7},
		{"type2", family2, restored2, 11},
	} {
		if err := tc.target.RestoreMetrics([]collector.CounterFamily{tc.family}); err != nil || tc.target.Counts[labels] != tc.want {
			t.Fatalf("restore %s = (%v, %#v), want count %d", tc.name, err, tc.target.Counts, tc.want)
		}
	}
	existing := collector.NewHIP08OEMType1Collector()
	old := collector.HIP08OEMLabels{ModuleID: "preserve"}
	existing.Counts = map[collector.HIP08OEMLabels]uint64{old: 3}
	bad := family1
	delete(bad.Samples[0].Labels, "err_severity")
	if err := existing.RestoreMetrics([]collector.CounterFamily{bad}); err == nil {
		t.Fatal("RestoreMetrics accepted a sample missing err_severity")
	}
	if len(existing.Counts) != 1 || existing.Counts[old] != 3 {
		t.Fatalf("failed restore changed counts: %#v", existing.Counts)
	}
	if err := existing.RestoreMetrics(nil); err != nil || len(existing.Counts) != 0 {
		t.Fatalf("RestoreMetrics(nil) = (%v, %#v), want empty count map", err, existing.Counts)
	}
}

func newHIP08OEMRecoveryFixture(t *testing.T, type1Rows, type2Rows []hip08OEMTestRow) engineRecoveryFixture {
	t.Helper()
	databasePath := filepath.Join(t.TempDir(), "rasdaemon-oem.db")
	devlinkSchema := "CREATE TABLE devlink_event (" +
		"id INTEGER PRIMARY KEY, timestamp TEXT, bus_name TEXT, dev_name TEXT, " +
		"driver_name TEXT, reporter_name TEXT, msg TEXT)"
	statements := []string{devlinkSchema}
	if type1Rows != nil {
		statements = append(statements, hip08OEMSchema(hip08OEMType1TestTable))
	}
	if type2Rows != nil {
		statements = append(statements, hip08OEMSchema(hip08OEMType2TestTable))
	}
	createSQLite(t, databasePath, statements...)
	if len(type1Rows) > 0 {
		insertHIP08OEMRows(t, databasePath, hip08OEMType1TestTable, type1Rows...)
	}
	if len(type2Rows) > 0 {
		insertHIP08OEMRows(t, databasePath, hip08OEMType2TestTable, type2Rows...)
	}
	insertDevlinkEvents(t, databasePath, recoveryEvent(1, "already consumed"))
	config := validatedConfig(t, databasePath)
	return engineRecoveryFixture{databasePath: databasePath, snapshotPath: config.SnapshotPath, config: config}
}

func TestHIP08OEMEngineEnrollsTablesIndependentlyAndRestarts(t *testing.T) {
	t.Run("type1 only", func(t *testing.T) {
		testHIP08OEMEngineEnrollment(t, []hip08OEMTestRow{{id: 1, version: 1, moduleID: "type1"}}, nil)
	})
	t.Run("type2 only", func(t *testing.T) {
		testHIP08OEMEngineEnrollment(t, nil, []hip08OEMTestRow{{id: 1, version: 1, moduleID: "type2"}})
	})
	t.Run("both", func(t *testing.T) {
		testHIP08OEMEngineEnrollment(t,
			[]hip08OEMTestRow{{id: 1, version: 1, moduleID: "type1"}},
			[]hip08OEMTestRow{{id: 1, version: 1, moduleID: "type2"}},
		)
	})

	t.Run("enroll type2 after type1 is already saved", func(t *testing.T) {
		fixture := newHIP08OEMRecoveryFixture(t,
			[]hip08OEMTestRow{{id: 1, version: 1, moduleID: "type1"}}, nil,
		)
		devlink := recoveryLabels("already consumed")
		saveRecoveryState(t, fixture, 2, 1, sourceIdentityForTest(t, fixture.databasePath),
			map[string]int64{"devlink_event": 1, hip08OEMType1TestTable: 1})
		type1 := collector.NewHIP08OEMType1Collector()
		type1.Counts = map[collector.HIP08OEMLabels]uint64{{Version: "1", ModuleID: "type1"}: 1}
		devlinkCollector := collector.DevlinkCollector{Counts: map[collector.DevlinkLabels]uint64{devlink: 1}}
		families := append(devlinkCollector.ExportMetrics(), type1.ExportMetrics()...)
		if err := collector.SaveSnapshot(fixture.snapshotPath, &collector.Snapshot{Generation: 2, Families: families}); err != nil {
			t.Fatalf("save type1-only legacy snapshot: %v", err)
		}
		engine := restoreRecoveryEngine(t, fixture)
		if err := engine.CollectAndCommit(context.Background()); err != nil {
			t.Fatalf("collect type1-only database: %v", err)
		}
		assertHIP08OEMPair(t, fixture, 2, 1, devlink, true, false)
		if err := engine.Close(); err != nil {
			t.Fatalf("close type1-only engine: %v", err)
		}
		createSQLite(t, fixture.databasePath, hip08OEMSchema(hip08OEMType2TestTable))
		insertHIP08OEMRows(t, fixture.databasePath, hip08OEMType2TestTable, hip08OEMTestRow{id: 1, version: 1, moduleID: "type2"})
		engine = restoreRecoveryEngine(t, fixture)
		if err := engine.CollectAndCommit(context.Background()); err != nil {
			t.Fatalf("enroll type2 after type1: %v", err)
		}
		assertHIP08OEMPair(t, fixture, 3, 1, devlink, true, true)
		if err := engine.Close(); err != nil {
			t.Fatalf("close upgraded OEM engine: %v", err)
		}
		engine = restoreRecoveryEngine(t, fixture)
		if err := engine.CollectAndCommit(context.Background()); err != nil {
			t.Fatalf("idle collection after OEM restart: %v", err)
		}
		assertHIP08OEMPair(t, fixture, 3, 1, devlink, true, true)
	})
}

func testHIP08OEMEngineEnrollment(t *testing.T, type1Rows, type2Rows []hip08OEMTestRow) {
	t.Helper()
	fixture := newHIP08OEMRecoveryFixture(t, type1Rows, type2Rows)
	devlink := recoveryLabels("already consumed")
	saveRecoveryState(t, fixture, 2, 1, sourceIdentityForTest(t, fixture.databasePath), map[string]int64{"devlink_event": 1})
	legacyDevlink := collector.DevlinkCollector{Counts: map[collector.DevlinkLabels]uint64{devlink: 1}}
	if err := collector.SaveSnapshot(fixture.snapshotPath, &collector.Snapshot{Generation: 2, Families: legacyDevlink.ExportMetrics()}); err != nil {
		t.Fatalf("save legacy devlink-only snapshot: %v", err)
	}
	engine := restoreRecoveryEngine(t, fixture)
	if err := engine.CollectAndCommit(context.Background()); err != nil {
		t.Fatalf("collect OEM sources: %v", err)
	}
	cursor1, cursor2 := int64(0), int64(0)
	cursors := map[string]int64{"devlink_event": 1}
	if type1Rows != nil {
		cursor1 = int64(len(type1Rows))
		cursors[hip08OEMType1TestTable] = cursor1
	}
	if type2Rows != nil {
		cursor2 = int64(len(type2Rows))
		cursors[hip08OEMType2TestTable] = cursor2
	}
	assertHIP08OEMPair(t, fixture, 3, 1, devlink, type1Rows != nil, type2Rows != nil)
	assertHIP08OEMState(t, fixture, cursors)
	if err := engine.Close(); err != nil {
		t.Fatalf("close OEM enrollment engine: %v", err)
	}
	engine = restoreRecoveryEngine(t, fixture)
	if err := engine.CollectAndCommit(context.Background()); err != nil {
		t.Fatalf("idle collect after OEM restart: %v", err)
	}
	assertHIP08OEMPair(t, fixture, 3, 1, devlink, type1Rows != nil, type2Rows != nil)
}

func assertHIP08OEMState(t *testing.T, fixture engineRecoveryFixture, want map[string]int64) {
	t.Helper()
	state, err := collector.LoadState(fixture.config.StateDir)
	if err != nil {
		t.Fatalf("LoadState() error = %v", err)
	}
	if len(state.Tables) != len(want) {
		t.Fatalf("state cursors = %#v, want %#v", state.Tables, want)
	}
	for name, cursor := range want {
		if state.Tables[name] != cursor {
			t.Fatalf("state cursor %s=%d, want %d (state %#v)", name, state.Tables[name], cursor, state.Tables)
		}
	}
	for name := range state.Tables {
		if _, exists := want[name]; !exists {
			t.Fatalf("unexpected state cursor %s in %#v", name, state.Tables)
		}
	}
	count := len(want) // Each saved table has one counter family.
	snapshot, err := collector.LoadSnapshot(fixture.snapshotPath)
	if err != nil || snapshot.Generation != state.Generation || len(snapshot.Families) != count {
		t.Fatalf("snapshot/state mismatch: snapshot=%#v state=%#v", snapshot, state)
	}
}

func assertHIP08OEMPair(t *testing.T, fixture engineRecoveryFixture, generation uint64, devlinkCursor int64, devlinkLabels collector.DevlinkLabels, type1, type2 bool) {
	t.Helper()
	cursors := map[string]int64{"devlink_event": devlinkCursor}
	if type1 {
		cursors[hip08OEMType1TestTable] = 1
	}
	if type2 {
		cursors[hip08OEMType2TestTable] = 1
	}
	assertHIP08OEMState(t, fixture, cursors)
	state, err := collector.LoadState(fixture.config.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	if state.Generation != generation {
		t.Fatalf("generation=%d, want %d", state.Generation, generation)
	}
	snapshot, err := collector.LoadSnapshot(fixture.snapshotPath)
	if err != nil {
		t.Fatal(err)
	}
	var got = make(map[string][]collector.CounterFamily)
	for _, family := range snapshot.Families {
		got[family.Name] = append(got[family.Name], family)
	}
	wantNames := map[string]bool{"rasdaemon_devlink_health_events_total": true}
	if type1 {
		wantNames["rasdaemon_hisi_oem_type1_events_total"] = true
	}
	if type2 {
		wantNames["rasdaemon_hisi_oem_type2_events_total"] = true
	}
	if len(got) != len(wantNames) {
		t.Fatalf("snapshot family names=%v, want %v", got, wantNames)
	}
	for name := range wantNames {
		if len(got[name]) != 1 {
			t.Fatalf("snapshot has %d copies of family %q", len(got[name]), name)
		}
	}
	var devlinkCollector collector.DevlinkCollector
	if err := devlinkCollector.RestoreMetrics(got["rasdaemon_devlink_health_events_total"]); err != nil {
		t.Fatalf("restore devlink: %v", err)
	}
	if devlinkCollector.Counts[devlinkLabels] != 1 {
		t.Fatalf("existing devlink count changed: %#v", devlinkCollector.Counts)
	}
	for source, enabled := range map[string]bool{
		"rasdaemon_hisi_oem_type1_events_total": type1,
		"rasdaemon_hisi_oem_type2_events_total": type2,
	} {
		if !enabled {
			continue
		}
		var oem *collector.HIP08OEMCollector
		switch source {
		case "rasdaemon_hisi_oem_type1_events_total":
			oem = collector.NewHIP08OEMType1Collector()
		case "rasdaemon_hisi_oem_type2_events_total":
			oem = collector.NewHIP08OEMType2Collector()
		default:
			t.Fatalf("unexpected OEM family %q", source)
		}
		if err := oem.RestoreMetrics(got[source]); err != nil {
			t.Fatalf("restore %s: %v", source, err)
		}
		count := uint64(1)
		for labels, value := range oem.Counts {
			if labels.ModuleID != "" && value != count {
				t.Fatalf("unexpected %s labels/count: %#v=%d", source, labels, value)
			}
		}
	}
	if _, duplicate := got["rasdaemon_hisi_oem_events_total"]; duplicate {
		t.Fatal("combined OEM metric family was emitted")
	}
}
