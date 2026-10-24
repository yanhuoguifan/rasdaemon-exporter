package collector_test

import (
	"context"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"rasdaemon-exporter/internal/collector"
)

func newMultiTableFixture(t *testing.T, devlinkRows []devlinkFixture, mcRows []mcTestRow) engineRecoveryFixture {
	t.Helper()
	databasePath := filepath.Join(t.TempDir(), "rasdaemon.db")
	if devlinkRows == nil {
		if len(mcRows) == 0 {
			t.Fatal("multi-table fixture must include at least one registered table")
		}
		createSQLite(t, databasePath, mcTestSchema)
	} else {
		createDevlinkDatabase(t, databasePath, devlinkRows...)
	}
	if len(mcRows) > 0 {
		if devlinkRows != nil {
			createSQLite(t, databasePath, mcTestSchema)
		}
		insertMCTestRows(t, databasePath, mcRows...)
	}
	config := validatedConfig(t, databasePath)
	return engineRecoveryFixture{databasePath: databasePath, snapshotPath: config.SnapshotPath, config: config}
}

func multiTableMCRow(id int64, message string) mcTestRow {
	return mcTestRow{id: id, errCount: 2, errType: "Corrected", errMsg: message, dimm: "DIMM-A"}
}

func TestEngineUpgradesLegacyStateAndRecoversMatchingBackupBeforeNewTable(t *testing.T) {
	oldEvent := recoveryLabels("old")
	newEvent := recoveryLabels("pending")
	mcLabels := collector.MCLabels{
		ErrorType: "Corrected", MC: "unknown", TopLayer: "unknown", MiddleLayer: "unknown",
		LowerLayer: "unknown", DIMM: "DIMM-A", ErrMsg: "memory event",
	}
	fixture := newMultiTableFixture(t,
		[]devlinkFixture{recoveryEvent(1, "old"), recoveryEvent(2, "pending")},
		[]mcTestRow{multiTableMCRow(1, "memory event")},
	)
	identity := sourceIdentityForTest(t, fixture.databasePath)
	saveRecoveryState(t, fixture, 2, 1, identity, map[string]int64{"devlink_event": 1})
	oldDevlink := collector.DevlinkCollector{Counts: map[collector.DevlinkLabels]uint64{oldEvent: 1}}
	if err := collector.SaveSnapshot(previousSnapshotPath(fixture), &collector.Snapshot{
		Generation: 2, Families: oldDevlink.ExportMetrics(),
	}); err != nil {
		t.Fatalf("save matching previous snapshot: %v", err)
	}
	// This is a new, uncommitted generation with MC families but no MC cursor.
	// Restore must compare generations first, then use the paired backup.
	newerDevlink := collector.DevlinkCollector{Counts: map[collector.DevlinkLabels]uint64{
		oldEvent: 1, newEvent: 1,
	}}
	newerMC := collector.MCCollector{Counts: map[collector.MCLabels]collector.MCCounters{
		mcLabels: {Events: 1, Errors: 2},
	}}
	families := append(newerDevlink.ExportMetrics(), newerMC.ExportMetrics()...)
	if err := collector.SaveSnapshot(fixture.snapshotPath, &collector.Snapshot{Generation: 3, Families: families}); err != nil {
		t.Fatalf("save ahead snapshot fixture: %v", err)
	}

	engine := restoreRecoveryEngine(t, fixture)
	if err := engine.CollectAndCommit(context.Background()); err != nil {
		t.Fatalf("collect after legacy state recovery: %v", err)
	}
	assertMultiTableState(t, fixture, 3, map[string]int64{"devlink_event": 2, "mc_event": 1})
	assertMultiTableMetrics(t, fixture.snapshotPath,
		map[collector.DevlinkLabels]uint64{oldEvent: 1, newEvent: 1},
		map[collector.MCLabels]collector.MCCounters{mcLabels: {Events: 1, Errors: 2}},
	)
	if err := engine.Close(); err != nil {
		t.Fatalf("close upgraded engine: %v", err)
	}

	engine = restoreRecoveryEngine(t, fixture)
	if err := engine.CollectAndCommit(context.Background()); err != nil {
		t.Fatalf("idle collection after upgrade restart: %v", err)
	}
	assertMultiTableState(t, fixture, 3, map[string]int64{"devlink_event": 2, "mc_event": 1})
	assertMultiTableMetrics(t, fixture.snapshotPath,
		map[collector.DevlinkLabels]uint64{oldEvent: 1, newEvent: 1},
		map[collector.MCLabels]collector.MCCounters{mcLabels: {Events: 1, Errors: 2}},
	)
}

func TestEngineBackupRestoreReplacesPartialCurrentCollectorState(t *testing.T) {
	labels := collector.MCLabels{
		ErrorType: "Corrected", MC: "unknown", TopLayer: "unknown", MiddleLayer: "unknown",
		LowerLayer: "unknown", DIMM: "DIMM-A", ErrMsg: "memory event",
	}
	fixture := newMultiTableFixture(t,
		[]devlinkFixture{recoveryEvent(1, "consumed devlink event")},
		[]mcTestRow{multiTableMCRow(1, "memory event")},
	)
	saveRecoveryState(t, fixture, 2, 1, sourceIdentityForTest(t, fixture.databasePath), map[string]int64{
		"devlink_event": 1,
		"mc_event":      1,
	})

	backupMC := collector.MCCollector{Counts: map[collector.MCLabels]collector.MCCounters{
		labels: {Events: 4, Errors: 7},
	}}
	backupFamilies := backupMC.ExportMetrics()
	if err := collector.SaveSnapshot(previousSnapshotPath(fixture), &collector.Snapshot{
		Generation: 2, Families: backupFamilies,
	}); err != nil {
		t.Fatalf("save matching backup without devlink family: %v", err)
	}

	// The current pair partially restores this devlink count before its
	// incomplete MC family fails. The backup must replace it with empty counts.
	partialDevlink := collector.DevlinkCollector{Counts: map[collector.DevlinkLabels]uint64{
		recoveryLabels("uncommitted current value"): 99,
	}}
	if err := collector.SaveSnapshot(fixture.snapshotPath, &collector.Snapshot{
		Generation: 2,
		Families:   append(partialDevlink.ExportMetrics(), backupFamilies[0]),
	}); err != nil {
		t.Fatalf("save partially invalid current snapshot: %v", err)
	}

	engine := openRecoveryEngine(t, fixture.config)
	if err := engine.Restore(context.Background()); err != nil {
		t.Fatalf("restore matching backup: %v", err)
	}
	assertMultiTableMetrics(t, fixture.snapshotPath, nil,
		map[collector.MCLabels]collector.MCCounters{labels: {Events: 4, Errors: 7}})

	insertMCTestRows(t, fixture.databasePath, multiTableMCRow(2, "memory event"))
	if err := engine.CollectAndCommit(context.Background()); err != nil {
		t.Fatalf("collect after backup recovery: %v", err)
	}
	assertMultiTableState(t, fixture, 3, map[string]int64{"devlink_event": 1, "mc_event": 2})
	assertMultiTableMetrics(t, fixture.snapshotPath, nil,
		map[collector.MCLabels]collector.MCCounters{labels: {Events: 5, Errors: 9}})
}

func TestEngineCollectsMCOnlyDatabase(t *testing.T) {
	fixture := newMultiTableFixture(t, nil, []mcTestRow{multiTableMCRow(1, "memory event")})
	engine := restoreRecoveryEngine(t, fixture)
	if err := engine.CollectAndCommit(context.Background()); err != nil {
		t.Fatalf("collect MC-only source: %v", err)
	}
	labels := collector.MCLabels{
		ErrorType: "Corrected", MC: "unknown", TopLayer: "unknown", MiddleLayer: "unknown",
		LowerLayer: "unknown", DIMM: "DIMM-A", ErrMsg: "memory event",
	}
	assertMultiTableState(t, fixture, 2, map[string]int64{"mc_event": 1})
	assertMultiTableMetrics(t, fixture.snapshotPath, nil,
		map[collector.MCLabels]collector.MCCounters{labels: {Events: 1, Errors: 2}})
}

func TestEngineDoesNotEnableAnAbsentTable(t *testing.T) {
	fixture := newMultiTableFixture(t, []devlinkFixture{recoveryEvent(1, "event")}, nil)
	engine := restoreRecoveryEngine(t, fixture)
	if err := engine.CollectAndCommit(context.Background()); err != nil {
		t.Fatalf("collect devlink-only source: %v", err)
	}
	assertMultiTableState(t, fixture, 2, map[string]int64{"devlink_event": 1})
	assertMultiTableMetrics(t, fixture.snapshotPath,
		map[collector.DevlinkLabels]uint64{recoveryLabels("event"): 1}, nil)
}

func TestEngineRejectsUnknownSavedTableAndFamilyWithoutCursor(t *testing.T) {
	t.Run("unknown saved table", func(t *testing.T) {
		fixture := newMultiTableFixture(t, nil, []mcTestRow{multiTableMCRow(1, "event")})
		saveRecoveryState(t, fixture, 1, 0, sourceIdentityForTest(t, fixture.databasePath),
			map[string]int64{"devlink_event": 0, "removed_table": 0})
		saveRecoverySnapshot(t, fixture.snapshotPath, 1)
		engine := openRecoveryEngine(t, fixture.config)
		if err := engine.Restore(context.Background()); err == nil {
			t.Fatal("Restore() accepted an unregistered saved table")
		}
		_ = engine.Close()
	})

	t.Run("new table family without cursor", func(t *testing.T) {
		fixture := newMultiTableFixture(t, []devlinkFixture{}, []mcTestRow{multiTableMCRow(1, "event")})
		saveRecoveryState(t, fixture, 1, 0, sourceIdentityForTest(t, fixture.databasePath),
			map[string]int64{"devlink_event": 0})
		metrics := collector.MCCollector{Counts: map[collector.MCLabels]collector.MCCounters{
			{ErrMsg: "event"}: {Events: 1},
		}}
		families := metrics.ExportMetrics()
		if err := collector.SaveSnapshot(fixture.snapshotPath, &collector.Snapshot{Generation: 1, Families: families}); err != nil {
			t.Fatalf("save snapshot with unpaired new-table family: %v", err)
		}
		engine := openRecoveryEngine(t, fixture.config)
		if err := engine.Restore(context.Background()); err == nil || !strings.Contains(err.Error(), "without a saved cursor") {
			t.Fatalf("Restore() error = %v, want new-table family without saved cursor", err)
		}
		_ = engine.Close()
	})
}

func TestEngineEnrollsEmptyNewTableOnlyWhenItCollectsRows(t *testing.T) {
	fixture := newMultiTableFixture(t, []devlinkFixture{recoveryEvent(1, "old event")}, nil)
	createSQLite(t, fixture.databasePath, mcTestSchema)
	identity := sourceIdentityForTest(t, fixture.databasePath)
	saveRecoveryState(t, fixture, 2, 1, identity, map[string]int64{"devlink_event": 1})
	oldDevlink := collector.DevlinkCollector{Counts: map[collector.DevlinkLabels]uint64{recoveryLabels("old event"): 1}}
	if err := collector.SaveSnapshot(fixture.snapshotPath, &collector.Snapshot{
		Generation: 2, Families: oldDevlink.ExportMetrics(),
	}); err != nil {
		t.Fatalf("save old-generation snapshot: %v", err)
	}

	engine := openRecoveryEngine(t, fixture.config)
	if err := engine.Restore(context.Background()); err != nil {
		t.Fatalf("restore before new-table collection: %v", err)
	}
	paths := []string{fixture.snapshotPath, filepath.Join(fixture.config.StateDir, "state.json")}
	contents := make(map[string][]byte, len(paths))
	infos := make(map[string]os.FileInfo, len(paths))
	for _, path := range paths {
		contents[path], infos[path] = readCommittedFile(t, path)
	}
	if err := engine.CollectAndCommit(context.Background()); err != nil {
		t.Fatalf("idle collection before MC events: %v", err)
	}
	for _, path := range paths {
		assertCommittedFileUnchanged(t, path, contents[path], infos[path])
	}
	state, err := collector.LoadState(fixture.config.StateDir)
	if err != nil {
		t.Fatalf("load state before MC events: %v", err)
	}
	if _, exists := state.Tables["mc_event"]; exists {
		t.Fatalf("empty table was enrolled without consuming rows: %v", state.Tables)
	}

	insertMCTestRows(t, fixture.databasePath, multiTableMCRow(1, "new memory event"))
	if err := engine.CollectAndCommit(context.Background()); err != nil {
		t.Fatalf("collect first MC event: %v", err)
	}
	mcLabels := collector.MCLabels{
		ErrorType: "Corrected", MC: "unknown", TopLayer: "unknown", MiddleLayer: "unknown",
		LowerLayer: "unknown", DIMM: "DIMM-A", ErrMsg: "new memory event",
	}
	assertMultiTableState(t, fixture, 3, map[string]int64{"devlink_event": 1, "mc_event": 1})
	assertMultiTableMetrics(t, fixture.snapshotPath,
		map[collector.DevlinkLabels]uint64{recoveryLabels("old event"): 1},
		map[collector.MCLabels]collector.MCCounters{mcLabels: {Events: 1, Errors: 2}},
	)
}

func TestEngineReadsOneBatchFromEachActiveTablePerRound(t *testing.T) {
	const rowCount = 1201
	devlinkRows := make([]devlinkFixture, rowCount)
	mcRows := make([]mcTestRow, rowCount)
	for index := range devlinkRows {
		id := int64(index + 1)
		devlinkRows[index] = recoveryEvent(id, "same devlink event")
		mcRows[index] = multiTableMCRow(id, "same memory event")
	}
	fixture := newMultiTableFixture(t, devlinkRows, mcRows)
	engine := restoreRecoveryEngine(t, fixture)
	for round, wantCursor := range []int64{500, 1000, rowCount} {
		if err := engine.CollectAndCommit(context.Background()); err != nil {
			t.Fatalf("collect round %d: %v", round+1, err)
		}
		assertMultiTableState(t, fixture, uint64(round+2), map[string]int64{
			"devlink_event": wantCursor,
			"mc_event":      wantCursor,
		})
	}
	assertMultiTableMetrics(t, fixture.snapshotPath,
		map[collector.DevlinkLabels]uint64{recoveryLabels("same devlink event"): rowCount},
		map[collector.MCLabels]collector.MCCounters{{
			ErrorType: "Corrected", MC: "unknown", TopLayer: "unknown", MiddleLayer: "unknown",
			LowerLayer: "unknown", DIMM: "DIMM-A", ErrMsg: "same memory event",
		}: {Events: rowCount, Errors: rowCount * 2}},
	)
}

func TestEngineReadsIndependentBatchesWhenTableBacklogsDiffer(t *testing.T) {
	const mcRowsCount = 1201
	mcRows := make([]mcTestRow, mcRowsCount)
	for index := range mcRows {
		mcRows[index] = multiTableMCRow(int64(index+1), "memory backlog")
	}
	fixture := newMultiTableFixture(t, []devlinkFixture{recoveryEvent(1, "single devlink")}, mcRows)
	engine := restoreRecoveryEngine(t, fixture)
	for round, wantMCCursor := range []int64{500, 1000, mcRowsCount} {
		if err := engine.CollectAndCommit(context.Background()); err != nil {
			t.Fatalf("collect uneven round %d: %v", round+1, err)
		}
		assertMultiTableState(t, fixture, uint64(round+2), map[string]int64{
			"devlink_event": 1,
			"mc_event":      wantMCCursor,
		})
	}
}

func TestEngineTableFailureDoesNotPublishOtherTableProgress(t *testing.T) {
	fixture := newMultiTableFixture(t,
		[]devlinkFixture{recoveryEvent(1, "valid devlink event")},
		[]mcTestRow{{id: 1, errCount: nil, errType: "Corrected", errMsg: "bad row"}},
	)
	engine := restoreRecoveryEngine(t, fixture)
	beforeSnapshot, err := os.ReadFile(fixture.snapshotPath)
	if err != nil {
		t.Fatalf("read initialized snapshot: %v", err)
	}
	statePath := filepath.Join(fixture.config.StateDir, "state.json")
	beforeState, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatalf("read initialized state: %v", err)
	}
	if err := engine.CollectAndCommit(context.Background()); err == nil {
		t.Fatal("collection succeeded despite an invalid MC row")
	}
	afterSnapshot, err := os.ReadFile(fixture.snapshotPath)
	if err != nil || string(afterSnapshot) != string(beforeSnapshot) {
		t.Fatalf("snapshot changed on partial-round failure: read error %v", err)
	}
	afterState, err := os.ReadFile(statePath)
	if err != nil || string(afterState) != string(beforeState) {
		t.Fatalf("state changed on partial-round failure: read error %v", err)
	}
	assertStateLockReleased(t, fixture.config.StateDir)
}

func assertMultiTableState(t *testing.T, fixture engineRecoveryFixture, generation uint64, cursors map[string]int64) {
	t.Helper()
	state, err := collector.LoadState(fixture.config.StateDir)
	if err != nil {
		t.Fatalf("load multi-table state: %v", err)
	}
	if state.Generation != generation || !reflect.DeepEqual(state.Tables, cursors) {
		t.Fatalf("state = generation %d, tables %v; want generation %d, tables %v",
			state.Generation, state.Tables, generation, cursors)
	}
	metrics, err := collector.LoadSnapshot(fixture.snapshotPath)
	if err != nil || metrics.Generation != generation {
		t.Fatalf("snapshot = (%#v, %v), want generation %d", metrics, err, generation)
	}
}

func assertMultiTableMetrics(t *testing.T, path string, wantDevlink map[collector.DevlinkLabels]uint64, wantMC map[collector.MCLabels]collector.MCCounters) {
	t.Helper()
	snapshot, err := collector.LoadSnapshot(path)
	if err != nil {
		t.Fatalf("load multi-table metrics snapshot: %v", err)
	}
	var devlink collector.DevlinkCollector
	var mc collector.MCCollector
	var devlinkFamilies, mcFamilies []collector.CounterFamily
	for _, family := range snapshot.Families {
		switch family.Name {
		case "rasdaemon_devlink_health_events_total":
			devlinkFamilies = append(devlinkFamilies, family)
		case "rasdaemon_memory_events_total", "rasdaemon_memory_errors_total":
			mcFamilies = append(mcFamilies, family)
		default:
			t.Fatalf("unexpected metric family %q", family.Name)
		}
	}
	if err := devlink.RestoreMetrics(devlinkFamilies); err != nil {
		t.Fatalf("restore devlink family: %v", err)
	}
	if err := mc.RestoreMetrics(mcFamilies); err != nil {
		t.Fatalf("restore MC families: %v", err)
	}
	if !maps.Equal(devlink.Counts, wantDevlink) {
		t.Fatalf("devlink counts = %v, want %v", devlink.Counts, wantDevlink)
	}
	if !maps.Equal(mc.Counts, wantMC) {
		t.Fatalf("MC counts = %v, want %v", mc.Counts, wantMC)
	}
}

func TestEngineRejectsDatabaseWithoutAnyRegisteredTablesAfterRegistrationExpansion(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "unrelated.db")
	createSQLite(t, databasePath, "CREATE TABLE unrelated (id INTEGER PRIMARY KEY)")
	err := collector.Run(context.Background(), validatedConfig(t, databasePath))
	if !errors.Is(err, collector.ErrNoCollectors) {
		t.Fatalf("Run() error = %v, want ErrNoCollectors", err)
	}
}
