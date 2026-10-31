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

const armTestSchema = `CREATE TABLE arm_event (
	id INTEGER PRIMARY KEY,
	timestamp TEXT,
	error_count INTEGER,
	affinity INTEGER,
	mpidr INTEGER,
	running_state INTEGER,
	psci_state INTEGER,
	err_info BLOB,
	context_info BLOB,
	vendor_info BLOB,
	error_type TEXT,
	error_flags TEXT,
	error_info INTEGER,
	virt_fault_addr INTEGER,
	phy_fault_addr INTEGER
)`

type armTestRow struct {
	id           any
	timestamp    any
	errorCount   any
	affinity     any
	mpidr        any
	runningState any
	psciState    any
	errInfo      any
	contextInfo  any
	vendorInfo   any
	errorType    any
	errorFlags   any
	errorInfo    any
	virtFault    any
	phyFault     any
}

func createARMDatabase(t *testing.T, path string, rows ...armTestRow) {
	t.Helper()
	createSQLite(t, path, armTestSchema)
	insertARMRows(t, path, rows...)
}

func insertARMRows(t *testing.T, path string, rows ...armTestRow) {
	t.Helper()
	if len(rows) == 0 {
		return
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open synthetic arm_event database: %v", err)
	}
	db.SetMaxOpenConns(1)
	defer db.Close()
	const query = `INSERT INTO arm_event (
		id, timestamp, error_count, affinity, mpidr, running_state, psci_state,
		err_info, context_info, vendor_info, error_type, error_flags, error_info,
		virt_fault_addr, phy_fault_addr
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	for _, row := range rows {
		if _, err := db.ExecContext(context.Background(), query,
			row.id, row.timestamp, row.errorCount, row.affinity, row.mpidr,
			row.runningState, row.psciState, row.errInfo, row.contextInfo,
			row.vendorInfo, row.errorType, row.errorFlags, row.errorInfo,
			row.virtFault, row.phyFault,
		); err != nil {
			t.Fatalf("insert synthetic arm_event row %v: %v", row.id, err)
		}
	}
}

func openARMDatabase(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open synthetic arm_event database: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close synthetic arm_event database: %v", err)
		}
	})
	return db
}

func TestARMCollectorPreservesFieldsAndCountsRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "arm-events.db")
	const identityFlags = "first error last error "
	createARMDatabase(t, path,
		armTestRow{
			id: 1, errorCount: 99, affinity: 1, mpidr: int64(0x0123456789abcdef),
			runningState: 1, psciState: 0x1234, errorType: "cache error bus error",
			errorFlags: identityFlags, errInfo: []byte{0xff, 0x00},
			errorInfo: 0x55, virtFault: 0x1111, phyFault: 0x2222,
		},
		armTestRow{
			id: 2, errorCount: 0, affinity: 1, mpidr: int64(0x0123456789abcdef),
			runningState: 1, psciState: 0x1234, errorType: "cache error bus error",
			errorFlags: identityFlags, errInfo: []byte{0xaa},
			errorInfo: 0x99, virtFault: 0x3333, phyFault: 0x4444,
		},
		armTestRow{
			id: 3, errorCount: -4, affinity: 255, mpidr: nil,
			runningState: -1, psciState: -1, errorType: "future type",
			errorFlags: "unknown flag source",
		},
		armTestRow{
			id: 4, affinity: -1, mpidr: 0, runningState: 0, psciState: 4294967295,
			errorType: "", errorFlags: "",
		},
		armTestRow{
			id: 5, affinity: nil, mpidr: nil, runningState: nil, psciState: nil,
			errorType: nil, errorFlags: nil,
		},
		armTestRow{
			id: 6, affinity: 2, mpidr: int64(0x100000000),
			runningState: 2, psciState: 3, errorType: "cache error bus error",
			errorFlags: "first error last error ",
		},
		armTestRow{
			id: 7, affinity: -2, mpidr: int64(-1), runningState: -2, psciState: -3,
			errorType: "negative source values", errorFlags: "future flag bits",
		},
	)
	digest := fileDigest(t, path)
	arm := collector.ARMCollector{}
	rows, err := arm.CollectBatch(context.Background(), openARMDatabase(t, path), 500)
	if err != nil || rows != 7 {
		t.Fatalf("CollectBatch() = (%d, %v), want (7, nil)", rows, err)
	}
	want := map[collector.ARMLabels]uint64{
		{
			MPIDR: "0x0123456789abcdef", Affinity: "1", RunningState: "1", PSCIState: "4660",
			ErrorType: "cache error bus error", ErrorFlags: identityFlags,
		}: 2,
		{
			MPIDR: "", Affinity: "255", RunningState: "-1", PSCIState: "-1",
			ErrorType: "future type", ErrorFlags: "unknown flag source",
		}: 1,
		{
			MPIDR: "0x0000000000000000", Affinity: "-1", RunningState: "0", PSCIState: "4294967295",
			ErrorType: "", ErrorFlags: "",
		}: 1,
		{}: 1,
		{
			MPIDR: "0x0000000100000000", Affinity: "2", RunningState: "2", PSCIState: "3",
			ErrorType: "cache error bus error", ErrorFlags: "first error last error ",
		}: 1,
		{
			MPIDR: "-1", Affinity: "-2", RunningState: "-2", PSCIState: "-3",
			ErrorType: "negative source values", ErrorFlags: "future flag bits",
		}: 1,
	}
	if arm.LastID != 7 || !reflect.DeepEqual(arm.Counts, want) {
		t.Fatalf("ARM collector = (cursor %d, %#v), want cursor 7 and %#v", arm.LastID, arm.Counts, want)
	}
	if got := fileDigest(t, path); got != digest {
		t.Fatal("collection modified the source SQLite database")
	}
}

func TestARMCollectorPagesAndGroupsByCompleteLabelTuple(t *testing.T) {
	path := filepath.Join(t.TempDir(), "arm-pages.db")
	const total = 1007
	rows := make([]armTestRow, total)
	for i := range rows {
		rows[i] = armTestRow{
			id: i + 1, affinity: 0, mpidr: 1, runningState: 1, psciState: 2,
			errorType: "same type", errorFlags: "first error",
		}
	}
	rows[1000].errorFlags = "last error"
	createARMDatabase(t, path, rows...)
	labels := collector.ARMLabels{
		MPIDR: "0x0000000000000001", Affinity: "0", RunningState: "1", PSCIState: "2",
		ErrorType: "same type", ErrorFlags: "first error",
	}
	arm := collector.ARMCollector{Counts: map[collector.ARMLabels]uint64{labels: 3}}
	db := openARMDatabase(t, path)
	for round, wantCursor := range []int64{500, 1000, total} {
		rowsRead, err := arm.CollectBatch(context.Background(), db, 500)
		wantRows := 500
		if round == 2 {
			wantRows = 7
		}
		if err != nil || rowsRead != wantRows || arm.LastID != wantCursor {
			t.Fatalf("round %d = rows %d cursor %d err %v, want rows %d cursor %d", round+1, rowsRead, arm.LastID, err, wantRows, wantCursor)
		}
	}
	want := map[collector.ARMLabels]uint64{
		labels: 1009,
		{MPIDR: "0x0000000000000001", Affinity: "0", RunningState: "1", PSCIState: "2", ErrorType: "same type", ErrorFlags: "last error"}: 1,
	}
	if !reflect.DeepEqual(arm.Counts, want) {
		t.Fatalf("counts = %#v, want %#v", arm.Counts, want)
	}
}

func TestARMCollectorKeepsCountsAndCursorOnFailure(t *testing.T) {
	t.Run("counter overflow", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "arm-overflow.db")
		createARMDatabase(t, path, armTestRow{
			id: 1, affinity: 1, mpidr: int64(0x0123456789abcdef),
			runningState: 1, psciState: 2, errorType: "overflow", errorFlags: "same",
		})
		labels := collector.ARMLabels{
			MPIDR: "0x0123456789abcdef", Affinity: "1", RunningState: "1", PSCIState: "2",
			ErrorType: "overflow", ErrorFlags: "same",
		}
		arm := collector.ARMCollector{Counts: map[collector.ARMLabels]uint64{labels: math.MaxUint64}}
		if rows, err := arm.CollectBatch(context.Background(), openARMDatabase(t, path), 1); err == nil || rows != 0 {
			t.Fatalf("overflow CollectBatch() = (%d, %v), want a zero-row error", rows, err)
		}
		if arm.LastID != 0 || len(arm.Counts) != 1 || arm.Counts[labels] != math.MaxUint64 {
			t.Fatalf("collector changed on overflow: cursor %d counts %#v", arm.LastID, arm.Counts)
		}
	})

	t.Run("invalid SQLite integer", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "arm-invalid.db")
		createARMDatabase(t, path,
			armTestRow{id: 1, affinity: 1, mpidr: 0x100, errorType: "valid"},
			armTestRow{id: 2, affinity: "not-an-integer", mpidr: 0x200, errorType: "invalid"},
		)
		db := openARMDatabase(t, path)
		arm := collector.ARMCollector{Counts: map[collector.ARMLabels]uint64{{ErrorType: "old"}: 4}}
		initial := map[collector.ARMLabels]uint64{{ErrorType: "old"}: 4}
		if rows, err := arm.CollectBatch(context.Background(), db, 10); err == nil || rows != 0 {
			t.Fatalf("CollectBatch() = (%d, %v), want zero-row error", rows, err)
		}
		if arm.LastID != 0 || !reflect.DeepEqual(arm.Counts, initial) {
			t.Fatalf("collector changed on invalid row: cursor %d counts %#v", arm.LastID, arm.Counts)
		}
		if _, err := db.ExecContext(context.Background(), "UPDATE arm_event SET affinity = 2 WHERE id = 2"); err != nil {
			t.Fatalf("repair fixture: %v", err)
		}
		if rows, err := arm.CollectBatch(context.Background(), db, 10); err != nil || rows != 2 || arm.LastID != 2 {
			t.Fatalf("retry = (%d, %v) cursor %d, want (2,nil) cursor 2", rows, err, arm.LastID)
		}
	})

	t.Run("canceled context", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "arm-canceled.db")
		createARMDatabase(t, path, armTestRow{id: 1, affinity: 0, mpidr: 0, errorType: "event"})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		arm := collector.ARMCollector{}
		if rows, err := arm.CollectBatch(ctx, openARMDatabase(t, path), 1); err == nil || rows != 0 || arm.LastID != 0 {
			t.Fatalf("canceled CollectBatch() = (%d, %v), cursor %d", rows, err, arm.LastID)
		}
	})

	t.Run("source ID rollback", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "arm-rollback.db")
		createARMDatabase(t, path, armTestRow{id: 1, affinity: 0, mpidr: 0, errorType: "event"})
		arm := collector.ARMCollector{LastID: 2}
		if rows, err := arm.CollectBatch(context.Background(), openARMDatabase(t, path), 1); err == nil || rows != 0 || arm.LastID != 2 {
			t.Fatalf("rollback CollectBatch() = (%d, %v), cursor %d", rows, err, arm.LastID)
		}
	})
}

func TestARMCollectorSeriesLimitFailureIsAtomic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "arm-series-limit.db")
	createARMDatabase(t, path, armTestRow{id: 1, affinity: 0, mpidr: 1, errorType: "new"})
	counts := make(map[collector.ARMLabels]uint64, 10000)
	for i := 0; i < 10000; i++ {
		counts[collector.ARMLabels{ErrorType: "old", ErrorFlags: strconv.Itoa(i)}] = 1
	}
	arm := collector.ARMCollector{Counts: counts}
	if rows, err := arm.CollectBatch(context.Background(), openARMDatabase(t, path), 1); err == nil || rows != 0 {
		t.Fatalf("series-limit CollectBatch() = (%d, %v), want zero-row error", rows, err)
	}
	if arm.LastID != 0 || len(arm.Counts) != 10000 {
		t.Fatalf("collector changed at series limit: cursor %d, groups %d", arm.LastID, len(arm.Counts))
	}
}

func TestARMCollectorRestoresSnapshotLabelsVerbatim(t *testing.T) {
	labels := collector.ARMLabels{
		MPIDR: "0x0000000000000001", Affinity: "255", RunningState: "-1", PSCIState: "4294967295",
		ErrorType: "Future type", ErrorFlags: "last error first error ",
	}
	want := map[collector.ARMLabels]uint64{labels: 12}
	original := collector.ARMCollector{Counts: want}
	path := filepath.Join(t.TempDir(), "arm.prom")
	if err := collector.SaveSnapshot(path, &collector.Snapshot{Generation: 7, Families: original.ExportMetrics()}); err != nil {
		t.Fatalf("save ARM snapshot: %v", err)
	}
	snapshot, err := collector.LoadSnapshot(path)
	if err != nil {
		t.Fatalf("load ARM snapshot: %v", err)
	}
	var restored collector.ARMCollector
	if err := restored.RestoreMetrics(snapshot.Families); err != nil {
		t.Fatalf("restore ARM snapshot: %v", err)
	}
	if !reflect.DeepEqual(restored.Counts, want) {
		t.Fatalf("restored = %#v, want %#v", restored.Counts, want)
	}
	if err := restored.RestoreMetrics(nil); err != nil || len(restored.Counts) != 0 {
		t.Fatalf("RestoreMetrics(nil) = (%v, %#v), want empty", err, restored.Counts)
	}
}

func TestEngineEnrollsARMWithoutReplayingExistingTables(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "rasdaemon.db")
	createSQLite(t, databasePath, devlinkSchema, armTestSchema)
	insertDevlinkEvents(t, databasePath, recoveryEvent(1, "already consumed"))
	insertARMRows(t, databasePath, armTestRow{
		id: 1, affinity: 255, mpidr: 0, runningState: -1, psciState: -1,
		errorType: "future type", errorFlags: "raw flags",
	})
	config := validatedConfig(t, databasePath)
	fixture := engineRecoveryFixture{databasePath: databasePath, snapshotPath: config.SnapshotPath, config: config}
	devlinkLabels := recoveryLabels("already consumed")
	saveRecoveryState(t, fixture, 2, 1, sourceIdentityForTest(t, databasePath), map[string]int64{"devlink_event": 1})
	devlink := collector.DevlinkCollector{Counts: map[collector.DevlinkLabels]uint64{devlinkLabels: 1}}
	if err := collector.SaveSnapshot(fixture.snapshotPath, &collector.Snapshot{
		Generation: 2, Families: devlink.ExportMetrics(),
	}); err != nil {
		t.Fatalf("save legacy devlink snapshot: %v", err)
	}

	engine := restoreRecoveryEngine(t, fixture)
	if err := engine.CollectAndCommit(context.Background()); err != nil {
		t.Fatalf("collect newly registered ARM table: %v", err)
	}
	assertARMEnginePair(t, fixture, 3, 1, devlinkLabels)
	if err := engine.Close(); err != nil {
		t.Fatalf("close upgraded engine: %v", err)
	}

	engine = restoreRecoveryEngine(t, fixture)
	if err := engine.CollectAndCommit(context.Background()); err != nil {
		t.Fatalf("idle collection after restart: %v", err)
	}
	assertARMEnginePair(t, fixture, 3, 1, devlinkLabels)
}

func assertARMEnginePair(t *testing.T, fixture engineRecoveryFixture, generation uint64, cursor int64, devlinkLabels collector.DevlinkLabels) {
	t.Helper()
	state, err := collector.LoadState(fixture.config.StateDir)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if state.Generation != generation || state.Tables["arm_event"] != cursor || state.Tables["devlink_event"] != 1 {
		t.Fatalf("state = %#v, want generation %d and ARM/devlink cursors %d/1", state, generation, cursor)
	}
	snapshot, err := collector.LoadSnapshot(fixture.snapshotPath)
	if err != nil || snapshot.Generation != generation {
		t.Fatalf("load snapshot = (%#v, %v), want generation %d", snapshot, err, generation)
	}
	var arm collector.ARMCollector
	var armFamilies []collector.CounterFamily
	var devlink collector.DevlinkCollector
	var devlinkFamilies []collector.CounterFamily
	for _, family := range snapshot.Families {
		switch family.Name {
		case "rasdaemon_arm_processor_events_total":
			armFamilies = append(armFamilies, family)
		case "rasdaemon_devlink_health_events_total":
			devlinkFamilies = append(devlinkFamilies, family)
		default:
			t.Fatalf("unexpected metric family %q", family.Name)
		}
	}
	if err := arm.RestoreMetrics(armFamilies); err != nil {
		t.Fatalf("restore ARM family: %v", err)
	}
	wantARM := map[collector.ARMLabels]uint64{armLabelsForEnrollment(): 1}
	if !reflect.DeepEqual(arm.Counts, wantARM) {
		t.Fatalf("ARM counts = %#v, want %#v", arm.Counts, wantARM)
	}
	if err := devlink.RestoreMetrics(devlinkFamilies); err != nil {
		t.Fatalf("restore devlink family: %v", err)
	}
	if !reflect.DeepEqual(devlink.Counts, map[collector.DevlinkLabels]uint64{devlinkLabels: 1}) {
		t.Fatalf("existing devlink counts changed: %#v", devlink.Counts)
	}
}

func armLabelsForEnrollment() collector.ARMLabels {
	return collector.ARMLabels{
		MPIDR: "0x0000000000000000", Affinity: "255", RunningState: "-1", PSCIState: "-1",
		ErrorType: "future type", ErrorFlags: "raw flags",
	}
}
