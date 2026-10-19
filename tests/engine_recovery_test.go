package collector_test

import (
	"context"
	"database/sql"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"

	"rasdaemon-exporter/internal/collector"
)

type engineRecoveryFixture struct {
	databasePath string
	snapshotPath string
	config       collector.Config
}

func newEngineRecoveryFixture(t *testing.T, events ...devlinkFixture) engineRecoveryFixture {
	t.Helper()
	databasePath := filepath.Join(t.TempDir(), "rasdaemon.db")
	createDevlinkDatabase(t, databasePath, events...)
	config := validatedConfig(t, databasePath)
	return engineRecoveryFixture{
		databasePath: databasePath,
		snapshotPath: config.SnapshotPath,
		config:       config,
	}
}

func openRecoveryEngine(t *testing.T, config collector.Config) *collector.Engine {
	t.Helper()
	engine, err := collector.OpenEngine(context.Background(), config)
	if err != nil {
		t.Fatalf("OpenEngine() error = %v", err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	return engine
}

func restoreRecoveryEngine(t *testing.T, fixture engineRecoveryFixture) *collector.Engine {
	t.Helper()
	engine := openRecoveryEngine(t, fixture.config)
	if err := engine.Restore(context.Background()); err != nil {
		t.Fatalf("Engine.Restore() error = %v", err)
	}
	return engine
}

func recoveryEvent(id int64, message string) devlinkFixture {
	return devlinkFixture{
		id: id, timestamp: "2026-10-12T00:00:00Z", busName: "pci",
		devName: "0000:01:00.0", driverName: "mlx5_core", reporterName: "tx", msg: message,
	}
}

func recoveryLabels(message string) collector.DevlinkLabels {
	return collector.DevlinkLabels{
		BusName: "pci", DevName: "0000:01:00.0", DriverName: "mlx5_core",
		ReporterName: "tx", Msg: message,
	}
}

func assertRecoveryPair(t *testing.T, fixture engineRecoveryFixture, generation uint64, cursor int64, counts map[collector.DevlinkLabels]uint64) {
	t.Helper()
	state, err := collector.LoadState(fixture.config.StateDir)
	if err != nil {
		t.Fatalf("LoadState() error = %v", err)
	}
	if state.Generation != generation || state.Tables["devlink_event"] != cursor {
		t.Fatalf("state = generation %d, cursor %d; want generation %d, cursor %d",
			state.Generation, state.Tables["devlink_event"], generation, cursor)
	}
	snapshot, err := collector.LoadSnapshot(fixture.snapshotPath)
	if err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	if snapshot.Generation != generation {
		t.Fatalf("snapshot generation = %d, want %d", snapshot.Generation, generation)
	}
	var devlink collector.DevlinkCollector
	if err := devlink.RestoreMetrics(snapshot.Families); err != nil {
		t.Fatalf("restore devlink metric family: %v", err)
	}
	if !reflect.DeepEqual(devlink.Counts, counts) {
		t.Fatalf("snapshot counts = %v, want %v", devlink.Counts, counts)
	}
}

func sourceIdentityForTest(t *testing.T, path string) collector.SourceIdentity {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat synthetic source: %v", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatal("synthetic source does not expose a stat identity")
	}
	return collector.SourceIdentity{Path: path, Device: uint64(stat.Dev), Inode: stat.Ino}
}

func saveRecoveryState(t *testing.T, fixture engineRecoveryFixture, generation uint64, cursor int64, source collector.SourceIdentity, tables map[string]int64) {
	t.Helper()
	if err := os.MkdirAll(fixture.config.StateDir, 0o700); err != nil {
		t.Fatalf("create state directory for fixture: %v", err)
	}
	if tables == nil {
		tables = map[string]int64{"devlink_event": cursor}
	}
	state := &collector.State{Generation: generation, Source: source, Tables: tables}
	if err := collector.SaveState(fixture.config.StateDir, state); err != nil {
		t.Fatalf("SaveState() error = %v", err)
	}
}

func saveRecoverySnapshot(t *testing.T, path string, generation uint64, samples ...collector.CounterSample) {
	t.Helper()
	snapshot := &collector.Snapshot{Generation: generation}
	if len(samples) > 0 {
		snapshot.Families = []collector.CounterFamily{{
			Name: "rasdaemon_devlink_health_events_total",
			Help: "Number of collected devlink event records.", Samples: samples,
		}}
	}
	if err := collector.SaveSnapshot(path, snapshot); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}
}

func previousSnapshotPath(fixture engineRecoveryFixture) string {
	return fixture.snapshotPath + ".prev"
}

func deleteRecoveryEventsThrough(t *testing.T, path string, id int64) {
	t.Helper()
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open synthetic SQLite database for recovery fixture: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec("DELETE FROM devlink_event WHERE id <= ?", id); err != nil {
		t.Fatalf("delete already-consumed synthetic rows: %v", err)
	}
}

func recoverySample(message string, value uint64) collector.CounterSample {
	labels := recoveryLabels(message)
	return collector.CounterSample{Labels: map[string]string{
		"bus_name": labels.BusName, "dev_name": labels.DevName, "driver_name": labels.DriverName,
		"reporter_name": labels.ReporterName, "msg": labels.Msg,
	}, Value: value}
}

func TestEngineRestoreInitializesAndCommitsCumulativeCounters(t *testing.T) {
	fixture := newEngineRecoveryFixture(t, recoveryEvent(1, "timeout"), recoveryEvent(2, "timeout"))
	engine := restoreRecoveryEngine(t, fixture)
	assertRecoveryPair(t, fixture, 1, 0, map[collector.DevlinkLabels]uint64{})

	if err := engine.CollectAndCommit(context.Background()); err != nil {
		t.Fatalf("first CollectAndCommit() error = %v", err)
	}
	want := map[collector.DevlinkLabels]uint64{recoveryLabels("timeout"): 2}
	assertRecoveryPair(t, fixture, 2, 2, want)

	paths := []string{fixture.snapshotPath, filepath.Join(fixture.config.StateDir, "state.json")}
	contents := make(map[string][]byte, len(paths))
	infos := make(map[string]os.FileInfo, len(paths))
	for _, path := range paths {
		contents[path], infos[path] = readCommittedFile(t, path)
	}
	for round := 0; round < 3; round++ {
		if err := engine.CollectAndCommit(context.Background()); err != nil {
			t.Fatalf("empty CollectAndCommit() round %d: %v", round+1, err)
		}
	}
	for _, path := range paths {
		assertCommittedFileUnchanged(t, path, contents[path], infos[path])
	}
	if _, err := os.Lstat(previousSnapshotPath(fixture)); !os.IsNotExist(err) {
		t.Fatalf("idle rounds created a previous snapshot: %v", err)
	}
	assertRecoveryPair(t, fixture, 2, 2, want)
}

func TestEngineBacklogDrainsAcrossRoundsWithoutDatabaseWrites(t *testing.T) {
	const totalRows = 6501
	events := make([]devlinkFixture, totalRows)
	for i := range events {
		events[i] = recoveryEvent(int64(i+1), "backlog")
	}
	fixture := newEngineRecoveryFixture(t, events...)
	databaseBefore := fileDigest(t, fixture.databasePath)
	engine := restoreRecoveryEngine(t, fixture)
	generation := uint64(1)
	var previousCursor int64
	for rounds := 0; previousCursor < totalRows; rounds++ {
		if rounds > totalRows/500+1 {
			t.Fatal("fixed backlog did not drain across bounded rounds")
		}
		if err := engine.CollectAndCommit(context.Background()); err != nil {
			t.Fatalf("collect backlog round %d: %v", rounds+1, err)
		}
		state, err := collector.LoadState(fixture.config.StateDir)
		if err != nil {
			t.Fatalf("load state after backlog round %d: %v", rounds+1, err)
		}
		if state.Tables["devlink_event"] <= previousCursor {
			t.Fatalf("backlog round %d did not advance cursor: before=%d after=%d",
				rounds+1, previousCursor, state.Tables["devlink_event"])
		}
		previousCursor = state.Tables["devlink_event"]
		generation = state.Generation
	}
	if generation < 3 {
		t.Fatalf("fixed backlog drained in generation %d; want multiple committed rounds", generation)
	}
	want := map[collector.DevlinkLabels]uint64{recoveryLabels("backlog"): totalRows}
	assertRecoveryPair(t, fixture, generation, totalRows, want)
	if got := fileDigest(t, fixture.databasePath); got != databaseBefore {
		t.Fatal("collection changed the fixed SQLite source")
	}

	snapshotBytes, err := os.ReadFile(fixture.snapshotPath)
	if err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(fixture.config.StateDir, "state.json")
	stateBytes, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.CollectAndCommit(context.Background()); err != nil {
		t.Fatalf("collect idle round after draining fixed database: %v", err)
	}
	if got, err := os.ReadFile(fixture.snapshotPath); err != nil || string(got) != string(snapshotBytes) {
		t.Fatalf("idle round changed drained snapshot: read error %v", err)
	}
	if got, err := os.ReadFile(statePath); err != nil || string(got) != string(stateBytes) {
		t.Fatalf("idle round changed drained state: read error %v", err)
	}
	if _, err := os.Lstat(previousSnapshotPath(fixture)); !os.IsNotExist(err) {
		t.Fatalf("idle round created previous snapshot: %v", err)
	}
}

func TestEngineIdleRoundDoesNotAdvanceMaximumGeneration(t *testing.T) {
	fixture := newEngineRecoveryFixture(t)
	identity := sourceIdentityForTest(t, fixture.databasePath)
	saveRecoveryState(t, fixture, math.MaxUint64, 0, identity, nil)
	saveRecoverySnapshot(t, fixture.snapshotPath, math.MaxUint64)
	engine := restoreRecoveryEngine(t, fixture)

	if err := engine.CollectAndCommit(context.Background()); err != nil {
		t.Fatalf("idle collection at maximum generation: %v", err)
	}
	assertRecoveryPair(t, fixture, math.MaxUint64, 0, map[collector.DevlinkLabels]uint64{})
	if _, err := os.Lstat(previousSnapshotPath(fixture)); !os.IsNotExist(err) {
		t.Fatalf("idle collection created previous snapshot: %v", err)
	}
}

func TestEngineCollectionRequiresRestore(t *testing.T) {
	fixture := newEngineRecoveryFixture(t, recoveryEvent(1, "timeout"), recoveryEvent(2, "timeout"))
	engine := openRecoveryEngine(t, fixture.config)
	if err := engine.CollectAndCommit(context.Background()); err == nil {
		t.Fatal("CollectAndCommit() succeeded before Restore")
	}
	if _, err := os.Stat(filepath.Join(fixture.config.StateDir, "state.json")); !os.IsNotExist(err) {
		t.Fatalf("state file exists after rejected collection: %v", err)
	}
	if _, err := os.Stat(fixture.snapshotPath); !os.IsNotExist(err) {
		t.Fatalf("snapshot exists after rejected collection: %v", err)
	}

	if err := engine.Restore(context.Background()); err != nil {
		t.Fatalf("Restore() after rejected collection: %v", err)
	}
	assertRecoveryPair(t, fixture, 1, 0, map[collector.DevlinkLabels]uint64{})

	if err := engine.CollectAndCommit(context.Background()); err != nil {
		t.Fatalf("CollectAndCommit() after Restore: %v", err)
	}
	assertRecoveryPair(t, fixture, 2, 2, map[collector.DevlinkLabels]uint64{recoveryLabels("timeout"): 2})
}

func TestEngineRestoreResumesCountsAndCollectsOnlyAppendedRows(t *testing.T) {
	fixture := newEngineRecoveryFixture(t, recoveryEvent(1, "timeout"), recoveryEvent(2, "timeout"))
	engine := restoreRecoveryEngine(t, fixture)
	if err := engine.CollectAndCommit(context.Background()); err != nil {
		t.Fatalf("initial CollectAndCommit() error = %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("close first engine: %v", err)
	}

	insertDevlinkEvents(t, fixture.databasePath, recoveryEvent(3, "timeout"), recoveryEvent(7, "new event"))
	engine = restoreRecoveryEngine(t, fixture)
	assertRecoveryPair(t, fixture, 2, 2, map[collector.DevlinkLabels]uint64{recoveryLabels("timeout"): 2})
	if err := engine.CollectAndCommit(context.Background()); err != nil {
		t.Fatalf("second CollectAndCommit() error = %v", err)
	}
	want := map[collector.DevlinkLabels]uint64{
		recoveryLabels("timeout"):   3,
		recoveryLabels("new event"): 1,
	}
	assertRecoveryPair(t, fixture, 3, 7, want)
	if err := engine.Close(); err != nil {
		t.Fatalf("close second engine: %v", err)
	}

	engine = restoreRecoveryEngine(t, fixture)
	assertRecoveryPair(t, fixture, 3, 7, want)
}

func TestEngineRestoreCompletesOnlyEmptyGenerationOneOrphanWithoutQuery(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "projection-not-needed.db")
	createSQLite(t, databasePath, `CREATE TABLE devlink_event (id INTEGER PRIMARY KEY)`)
	config := validatedConfig(t, databasePath)
	fixture := engineRecoveryFixture{
		databasePath: databasePath,
		snapshotPath: config.SnapshotPath,
		config:       config,
	}
	saveRecoverySnapshot(t, fixture.snapshotPath, 1)

	// Restore must only repair the missing first-generation cursor file. The
	// table intentionally lacks collector columns, so scanning it would fail.
	_ = restoreRecoveryEngine(t, fixture)
	state, err := collector.LoadState(fixture.config.StateDir)
	if err != nil {
		t.Fatalf("LoadState() after orphan repair: %v", err)
	}
	if state.Generation != 1 || state.Tables["devlink_event"] != 0 {
		t.Fatalf("repaired state = %#v, want generation 1 and cursor 0", state)
	}
	snapshot, err := collector.LoadSnapshot(fixture.snapshotPath)
	if err != nil || snapshot.Generation != 1 || len(snapshot.Families) != 0 {
		t.Fatalf("first-generation snapshot = (%#v, %v), want empty generation 1", snapshot, err)
	}
}

func TestEngineRestoreRejectsOrphanedAndMalformedPairs(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*testing.T, engineRecoveryFixture)
	}{
		{
			name: "prom generation above first",
			setup: func(t *testing.T, fixture engineRecoveryFixture) {
				saveRecoverySnapshot(t, fixture.snapshotPath, 2)
			},
		},
		{
			name: "state without metrics",
			setup: func(t *testing.T, fixture engineRecoveryFixture) {
				saveRecoveryState(t, fixture, 1, 0, sourceIdentityForTest(t, fixture.databasePath), nil)
			},
		},
		{
			name: "invalid metrics text",
			setup: func(t *testing.T, fixture engineRecoveryFixture) {
				if err := os.WriteFile(fixture.snapshotPath, []byte("not prometheus text\n"), 0o644); err != nil {
					t.Fatal(err)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newEngineRecoveryFixture(t)
			test.setup(t, fixture)
			engine := openRecoveryEngine(t, fixture.config)
			if err := engine.Restore(context.Background()); err == nil {
				t.Fatal("Restore() accepted an incomplete or malformed file pair")
			}
			if err := engine.Close(); err != nil {
				t.Fatalf("close failed engine: %v", err)
			}
			// Restore errors must release the process lock.
			reopened := openRecoveryEngine(t, fixture.config)
			if err := reopened.Close(); err != nil {
				t.Fatalf("close reopened engine: %v", err)
			}
		})
	}
}

func TestEngineRestoreRejectsGenerationSourceAndCursorMismatches(t *testing.T) {
	tests := []struct {
		name      string
		stateGen  uint64
		promGen   uint64
		source    func(*testing.T, engineRecoveryFixture) collector.SourceIdentity
		tableCurs map[string]int64
	}{
		{name: "prom behind state", stateGen: 2, promGen: 1},
		{name: "prom ahead by two", stateGen: 1, promGen: 3},
		{name: "maximum generation does not wrap", stateGen: 1, promGen: math.MaxUint64},
		{
			name: "different source identity", stateGen: 1, promGen: 1,
			source: func(t *testing.T, fixture engineRecoveryFixture) collector.SourceIdentity {
				identity := sourceIdentityForTest(t, fixture.databasePath)
				identity.Inode++
				return identity
			},
		},
		{name: "missing active table cursor", stateGen: 1, promGen: 1, tableCurs: map[string]int64{"other_table": 0}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newEngineRecoveryFixture(t)
			identity := sourceIdentityForTest(t, fixture.databasePath)
			if test.source != nil {
				identity = test.source(t, fixture)
			}
			saveRecoveryState(t, fixture, test.stateGen, 0, identity, test.tableCurs)
			saveRecoverySnapshot(t, fixture.snapshotPath, test.promGen)
			engine := openRecoveryEngine(t, fixture.config)
			if err := engine.Restore(context.Background()); err == nil {
				t.Fatal("Restore() accepted inconsistent state and metrics")
			}
			if err := engine.Close(); err != nil {
				t.Fatalf("close failed engine: %v", err)
			}
			reopened := openRecoveryEngine(t, fixture.config)
			_ = reopened.Close()
		})
	}
}

func TestEngineRestoreRejectsAheadSnapshotWithoutMatchingPreviousAndPreservesFiles(t *testing.T) {
	tests := []struct {
		name        string
		backupState string
		rowCount    int
	}{
		{name: "missing backup with retained history", backupState: "missing", rowCount: 6505},
		{name: "corrupt backup", backupState: "corrupt"},
		{name: "wrong-generation backup", backupState: "wrong generation"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newEngineRecoveryFixture(t)
			if test.rowCount > 0 {
				events := make([]devlinkFixture, test.rowCount)
				for i := range events {
					events[i] = recoveryEvent(int64(i+1), "retained history")
				}
				insertDevlinkEvents(t, fixture.databasePath, events...)
			}
			saveRecoveryState(t, fixture, 1, 0, sourceIdentityForTest(t, fixture.databasePath), nil)
			saveRecoverySnapshot(t, fixture.snapshotPath, 2, recoverySample("uncommitted", 99))
			previousPath := previousSnapshotPath(fixture)
			switch test.backupState {
			case "corrupt":
				if err := os.WriteFile(previousPath, []byte("not a snapshot\n"), 0o600); err != nil {
					t.Fatalf("write corrupt previous snapshot: %v", err)
				}
			case "wrong generation":
				saveRecoverySnapshot(t, previousPath, 3, recoverySample("stale", 10))
			}

			statePath := filepath.Join(fixture.config.StateDir, "state.json")
			stateBefore, err := os.ReadFile(statePath)
			if err != nil {
				t.Fatalf("read state before restore: %v", err)
			}
			snapshotBefore, err := os.ReadFile(fixture.snapshotPath)
			if err != nil {
				t.Fatalf("read snapshot before restore: %v", err)
			}
			var previousBefore []byte
			if test.backupState != "missing" {
				previousBefore, err = os.ReadFile(previousPath)
				if err != nil {
					t.Fatalf("read previous snapshot before restore: %v", err)
				}
			}

			engine := openRecoveryEngine(t, fixture.config)
			if err := engine.Restore(context.Background()); err == nil {
				t.Fatal("Restore() accepted an ahead snapshot without a matching previous snapshot")
			}
			if err := engine.Close(); err != nil {
				t.Fatalf("close failed engine: %v", err)
			}
			if got, err := os.ReadFile(statePath); err != nil || string(got) != string(stateBefore) {
				t.Fatalf("state changed after rejected restore: read error %v", err)
			}
			if got, err := os.ReadFile(fixture.snapshotPath); err != nil || string(got) != string(snapshotBefore) {
				t.Fatalf("current snapshot changed after rejected restore: read error %v", err)
			}
			if test.backupState != "missing" {
				if got, err := os.ReadFile(previousPath); err != nil || string(got) != string(previousBefore) {
					t.Fatalf("previous snapshot changed after rejected restore: read error %v", err)
				}
			} else if _, err := os.Stat(previousPath); !os.IsNotExist(err) {
				t.Fatalf("Restore() created a previous snapshot: %v", err)
			}
			reopened := openRecoveryEngine(t, fixture.config)
			if err := reopened.Close(); err != nil {
				t.Fatalf("close engine after lock release check: %v", err)
			}
		})
	}
}

func TestEngineRestoreRejectsUnknownFamilyInMatchingSnapshotWithoutChangingFiles(t *testing.T) {
	fixture := newEngineRecoveryFixture(t)
	saveRecoveryState(t, fixture, 1, 0, sourceIdentityForTest(t, fixture.databasePath), nil)
	if err := collector.SaveSnapshot(fixture.snapshotPath, &collector.Snapshot{
		Generation: 1,
		Families: []collector.CounterFamily{{
			Name: "unknown_events_total", Help: "Not owned by an enabled table.",
			Samples: []collector.CounterSample{{Labels: map[string]string{"event": "unknown"}, Value: 1}},
		}},
	}); err != nil {
		t.Fatalf("save snapshot with unknown family: %v", err)
	}
	snapshotBefore, err := os.ReadFile(fixture.snapshotPath)
	if err != nil {
		t.Fatalf("read snapshot before restore: %v", err)
	}
	statePath := filepath.Join(fixture.config.StateDir, "state.json")
	stateBefore, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatalf("read paired state before restore: %v", err)
	}

	engine := openRecoveryEngine(t, fixture.config)
	if err := engine.Restore(context.Background()); err == nil {
		t.Fatal("Restore() accepted an unknown metric family")
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("close failed engine: %v", err)
	}
	if got, err := os.ReadFile(fixture.snapshotPath); err != nil || string(got) != string(snapshotBefore) {
		t.Fatalf("snapshot changed during rejected restore: read error %v", err)
	}
	if got, err := os.ReadFile(statePath); err != nil || string(got) != string(stateBefore) {
		t.Fatalf("state changed during rejected restore: read error %v", err)
	}
}

func TestEngineRestoreRollsBackToMatchingPreviousSnapshot(t *testing.T) {
	tests := []struct {
		name        string
		currentMode string
	}{
		{name: "current snapshot missing", currentMode: "missing"},
		{name: "current snapshot malformed", currentMode: "malformed"},
		{name: "current snapshot from another generation", currentMode: "mismatch"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newEngineRecoveryFixture(t,
				recoveryEvent(1, "committed"), recoveryEvent(2, "committed"), recoveryEvent(3, "pending"))
			identity := sourceIdentityForTest(t, fixture.databasePath)
			saveRecoveryState(t, fixture, 2, 2, identity, nil)
			saveRecoverySnapshot(t, previousSnapshotPath(fixture), 2, recoverySample("committed", 2))
			switch test.currentMode {
			case "malformed":
				if err := os.WriteFile(fixture.snapshotPath, []byte("not a snapshot\n"), 0o644); err != nil {
					t.Fatalf("write malformed current snapshot: %v", err)
				}
			case "mismatch":
				saveRecoverySnapshot(t, fixture.snapshotPath, 3, recoverySample("uncommitted", 3))
			}

			engine := restoreRecoveryEngine(t, fixture)
			assertRecoveryPair(t, fixture, 2, 2, map[collector.DevlinkLabels]uint64{
				recoveryLabels("committed"): 2,
			})
			if _, err := os.Stat(previousSnapshotPath(fixture)); !os.IsNotExist(err) {
				t.Fatalf("previous snapshot remains after rollback: %v", err)
			}

			if err := engine.CollectAndCommit(context.Background()); err != nil {
				t.Fatalf("collect after restoring previous snapshot: %v", err)
			}
			assertRecoveryPair(t, fixture, 3, 3, map[collector.DevlinkLabels]uint64{
				recoveryLabels("committed"): 2,
				recoveryLabels("pending"):   1,
			})
		})
	}
}

func TestEngineRestorePrefersCommittedSnapshotAndBestEffortCleansPrevious(t *testing.T) {
	for _, backupKind := range []string{"wrong generation", "malformed regular file"} {
		t.Run(backupKind, func(t *testing.T) {
			fixture := newEngineRecoveryFixture(t, recoveryEvent(1, "committed"), recoveryEvent(2, "committed"))
			identity := sourceIdentityForTest(t, fixture.databasePath)
			saveRecoveryState(t, fixture, 2, 2, identity, nil)
			saveRecoverySnapshot(t, fixture.snapshotPath, 2, recoverySample("committed", 2))
			// A stale or malformed backup is not a reason to replace a matching current pair.
			if backupKind == "wrong generation" {
				saveRecoverySnapshot(t, previousSnapshotPath(fixture), 1, recoverySample("stale", 1))
			} else if err := os.WriteFile(previousSnapshotPath(fixture), []byte("not a snapshot\n"), 0o600); err != nil {
				t.Fatalf("write malformed previous snapshot: %v", err)
			}

			engine := restoreRecoveryEngine(t, fixture)
			assertRecoveryPair(t, fixture, 2, 2, map[collector.DevlinkLabels]uint64{
				recoveryLabels("committed"): 2,
			})
			if err := engine.Close(); err != nil {
				t.Fatalf("close restored engine: %v", err)
			}
			if _, err := os.Stat(previousSnapshotPath(fixture)); !os.IsNotExist(err) {
				t.Fatalf("stale previous snapshot remains: %v", err)
			}
		})
	}

	t.Run("cleanup failure does not invalidate committed pair", func(t *testing.T) {
		fixture := newEngineRecoveryFixture(t)
		identity := sourceIdentityForTest(t, fixture.databasePath)
		saveRecoveryState(t, fixture, 1, 0, identity, nil)
		saveRecoverySnapshot(t, fixture.snapshotPath, 1)
		previousPath := previousSnapshotPath(fixture)
		if err := os.Mkdir(previousPath, 0o700); err != nil {
			t.Fatalf("make backup cleanup fail: %v", err)
		}
		if err := os.WriteFile(filepath.Join(previousPath, "keep"), []byte("fixture"), 0o600); err != nil {
			t.Fatalf("populate backup directory: %v", err)
		}

		engine := restoreRecoveryEngine(t, fixture)
		if err := engine.Close(); err != nil {
			t.Fatalf("close restored engine after cleanup failure: %v", err)
		}
		assertRecoveryPair(t, fixture, 1, 0, map[collector.DevlinkLabels]uint64{})
		if _, err := os.Stat(filepath.Join(previousPath, "keep")); err != nil {
			t.Fatalf("best-effort cleanup removed or changed the backup fixture: %v", err)
		}
	})
}

func TestEngineRestoreRejectsPreviousSnapshotWithoutCurrentPair(t *testing.T) {
	fixture := newEngineRecoveryFixture(t)
	saveRecoverySnapshot(t, previousSnapshotPath(fixture), 1)
	engine := openRecoveryEngine(t, fixture.config)
	if err := engine.Restore(context.Background()); err == nil {
		t.Fatal("Restore() initialized state from a lone previous snapshot")
	}
	if _, err := os.Stat(fixture.snapshotPath); !os.IsNotExist(err) {
		t.Fatalf("Restore() created current snapshot from an unpaired backup: %v", err)
	}
	if _, err := os.Stat(filepath.Join(fixture.config.StateDir, "state.json")); !os.IsNotExist(err) {
		t.Fatalf("Restore() created state from an unpaired backup: %v", err)
	}
}

func TestEngineCommitFailureUsesPreviousSnapshot(t *testing.T) {
	t.Run("backup write failure leaves the committed pair unchanged", func(t *testing.T) {
		fixture := newEngineRecoveryFixture(t)
		engine := restoreRecoveryEngine(t, fixture)
		insertDevlinkEvents(t, fixture.databasePath, recoveryEvent(1, "pending"))
		beforeSnapshot, err := os.ReadFile(fixture.snapshotPath)
		if err != nil {
			t.Fatalf("read committed snapshot: %v", err)
		}
		statePath := filepath.Join(fixture.config.StateDir, "state.json")
		beforeState, err := os.ReadFile(statePath)
		if err != nil {
			t.Fatalf("read committed state: %v", err)
		}
		previousPath := previousSnapshotPath(fixture)
		if err := os.Mkdir(previousPath, 0o700); err != nil {
			t.Fatalf("install failing backup target: %v", err)
		}
		if err := engine.CollectAndCommit(context.Background()); err == nil {
			t.Fatal("CollectAndCommit() succeeded with a directory at the previous snapshot path")
		}
		if err := os.Remove(previousPath); err != nil {
			t.Fatalf("remove failing backup target directory: %v", err)
		}
		afterSnapshot, err := os.ReadFile(fixture.snapshotPath)
		if err != nil || string(afterSnapshot) != string(beforeSnapshot) {
			t.Fatalf("snapshot changed after backup failure: read error %v", err)
		}
		afterState, err := os.ReadFile(statePath)
		if err != nil || string(afterState) != string(beforeState) {
			t.Fatalf("state changed after backup failure: read error %v", err)
		}
		engine = restoreRecoveryEngine(t, fixture)
		if err := engine.CollectAndCommit(context.Background()); err != nil {
			t.Fatalf("retry after backup failure: %v", err)
		}
		assertRecoveryPair(t, fixture, 2, 1, map[collector.DevlinkLabels]uint64{recoveryLabels("pending"): 1})
		if _, err := os.Stat(previousPath); !os.IsNotExist(err) {
			t.Fatalf("previous snapshot remains after successful commit: %v", err)
		}
	})

	t.Run("state write failure", func(t *testing.T) {
		fixture := newEngineRecoveryFixture(t, recoveryEvent(1, "committed"), recoveryEvent(2, "committed"))
		engine := restoreRecoveryEngine(t, fixture)
		if err := engine.CollectAndCommit(context.Background()); err != nil {
			t.Fatalf("commit initial rows: %v", err)
		}
		insertDevlinkEvents(t, fixture.databasePath, recoveryEvent(3, "pending"))
		statePath := filepath.Join(fixture.config.StateDir, "state.json")
		backup := statePath + ".saved"
		if err := os.Rename(statePath, backup); err != nil {
			t.Fatalf("preserve old state: %v", err)
		}
		if err := os.Mkdir(statePath, 0o700); err != nil {
			t.Fatalf("install failing state target: %v", err)
		}
		if err := engine.CollectAndCommit(context.Background()); err == nil {
			t.Fatal("CollectAndCommit() succeeded with a directory at the state path")
		}
		if err := os.Remove(statePath); err != nil {
			t.Fatalf("remove temporary state target directory: %v", err)
		}
		if err := os.Rename(backup, statePath); err != nil {
			t.Fatalf("restore previous state: %v", err)
		}
		currentSnapshot, err := collector.LoadSnapshot(fixture.snapshotPath)
		if err != nil || currentSnapshot.Generation != 3 {
			t.Fatalf("current snapshot after state failure = (%#v, %v), want generation 3", currentSnapshot, err)
		}
		previousSnapshot, err := collector.LoadSnapshot(previousSnapshotPath(fixture))
		if err != nil || previousSnapshot.Generation != 2 {
			t.Fatalf("previous snapshot after state failure = (%#v, %v), want generation 2", previousSnapshot, err)
		}
		// The failed round's snapshot contains ID 3, while state still commits
		// cursor 2. Removing the old source rows proves recovery uses the paired
		// generation-2 snapshot without rereading already committed rows.
		deleteRecoveryEventsThrough(t, fixture.databasePath, 2)
		engine = openRecoveryEngine(t, fixture.config)
		if err := engine.Restore(context.Background()); err != nil {
			t.Fatalf("Restore() after snapshot led state: %v", err)
		}
		want := map[collector.DevlinkLabels]uint64{recoveryLabels("committed"): 2}
		assertRecoveryPair(t, fixture, 2, 2, want)
		if _, err := os.Stat(previousSnapshotPath(fixture)); !os.IsNotExist(err) {
			t.Fatalf("previous snapshot remains after rollback recovery: %v", err)
		}
		if err := engine.CollectAndCommit(context.Background()); err != nil {
			t.Fatalf("collect from restored cursor: %v", err)
		}
		want[recoveryLabels("pending")] = 1
		assertRecoveryPair(t, fixture, 3, 3, want)
	})
}

func TestEngineCommitCounterPrecisionFailurePreservesCommittedFilesAndStops(t *testing.T) {
	fixture := newEngineRecoveryFixture(t)
	identity := sourceIdentityForTest(t, fixture.databasePath)
	saveRecoveryState(t, fixture, 1, 0, identity, nil)
	saveRecoverySnapshot(t, fixture.snapshotPath, 1, recoverySample("at limit", 1<<53))
	engine := openRecoveryEngine(t, fixture.config)
	if err := engine.Restore(context.Background()); err != nil {
		t.Fatalf("Restore() at exact counter limit: %v", err)
	}
	beforeSnapshot, err := os.ReadFile(fixture.snapshotPath)
	if err != nil {
		t.Fatalf("read committed snapshot: %v", err)
	}
	insertDevlinkEvents(t, fixture.databasePath, recoveryEvent(1, "at limit"))
	if err := engine.CollectAndCommit(context.Background()); err == nil {
		t.Fatal("CollectAndCommit() accepted a counter above the exact float64 limit")
	}
	afterSnapshot, err := os.ReadFile(fixture.snapshotPath)
	if err != nil || string(afterSnapshot) != string(beforeSnapshot) {
		t.Fatalf("snapshot changed after rejected counter: read error %v", err)
	}
	state, err := collector.LoadState(fixture.config.StateDir)
	if err != nil || state.Generation != 1 || state.Tables["devlink_event"] != 0 {
		t.Fatalf("state after rejected counter = (%#v, %v), want generation 1 cursor 0", state, err)
	}
}

func TestEngineCanceledCommitDoesNotModifyCommittedPair(t *testing.T) {
	fixture := newEngineRecoveryFixture(t, recoveryEvent(1, "not consumed"))
	engine := restoreRecoveryEngine(t, fixture)
	beforeSnapshot, err := os.ReadFile(fixture.snapshotPath)
	if err != nil {
		t.Fatalf("read generation-one snapshot: %v", err)
	}
	beforeState, err := os.ReadFile(filepath.Join(fixture.config.StateDir, "state.json"))
	if err != nil {
		t.Fatalf("read generation-one state: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := engine.CollectAndCommit(ctx); err == nil {
		t.Fatal("CollectAndCommit() succeeded with an already-canceled context")
	}
	afterSnapshot, err := os.ReadFile(fixture.snapshotPath)
	if err != nil || string(afterSnapshot) != string(beforeSnapshot) {
		t.Fatalf("snapshot changed after canceled commit: read error %v", err)
	}
	afterState, err := os.ReadFile(filepath.Join(fixture.config.StateDir, "state.json"))
	if err != nil || string(afterState) != string(beforeState) {
		t.Fatalf("state changed after canceled commit: read error %v", err)
	}
	reopened, err := collector.OpenEngine(context.Background(), fixture.config)
	if err != nil {
		t.Fatalf("canceled commit did not release process lock: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("close engine after lock release check: %v", err)
	}
}
