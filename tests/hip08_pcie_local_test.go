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

const hip08PCIeLocalSchema = `CREATE TABLE hip08_pcie_local_event_v2 (
	id INTEGER PRIMARY KEY,
	timestamp TEXT,
	version INTEGER,
	soc_id INTEGER,
	socket_id INTEGER,
	nimbus_id INTEGER,
	sub_module_id TEXT,
	core_id INTEGER,
	port_id INTEGER,
	err_type INTEGER,
	err_severity TEXT,
	regs_dump TEXT
)`

type hip08PCIeLocalTestRow struct {
	id                                             int64
	version, socID, socketID, nimbusID             any
	subModuleID                                    any
	coreID, portID, errType, errSeverity, regsDump any
}

func createHIP08PCIeLocalDatabase(t *testing.T, path string, rows ...hip08PCIeLocalTestRow) {
	t.Helper()
	createSQLite(t, path, hip08PCIeLocalSchema)
	insertHIP08PCIeLocalRows(t, path, rows...)
}

func insertHIP08PCIeLocalRows(t *testing.T, path string, rows ...hip08PCIeLocalTestRow) {
	t.Helper()
	if len(rows) == 0 {
		return
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open synthetic HIP08 PCIe database: %v", err)
	}
	db.SetMaxOpenConns(1)
	defer db.Close()
	const query = `INSERT INTO hip08_pcie_local_event_v2
		(id, version, soc_id, socket_id, nimbus_id, sub_module_id,
		 core_id, port_id, err_type, err_severity, regs_dump)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	for _, row := range rows {
		if _, err := db.ExecContext(context.Background(), query,
			row.id, row.version, row.socID, row.socketID, row.nimbusID,
			row.subModuleID, row.coreID, row.portID, row.errType,
			row.errSeverity, row.regsDump,
		); err != nil {
			t.Fatalf("insert synthetic HIP08 PCIe row %d: %v", row.id, err)
		}
	}
}

func openHIP08PCIeLocalDatabase(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open synthetic HIP08 PCIe database: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close synthetic HIP08 PCIe database: %v", err)
		}
	})
	return db
}

func TestHIP08PCIeLocalCollectorPreservesFieldsAndCountsRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hip08-pcie-local.db")
	createHIP08PCIeLocalDatabase(t, path,
		hip08PCIeLocalTestRow{id: 1, version: 2, socID: 1, socketID: 0, nimbusID: 3,
			subModuleID: " PCIe local ", coreID: 4, portID: 5, errType: 0,
			errSeverity: "Uncorrected (Non-Fatal)", regsDump: "ignored register dump"},
		hip08PCIeLocalTestRow{id: 2, version: 2, socID: 1, socketID: 0, nimbusID: 3,
			subModuleID: " PCIe local ", coreID: 4, portID: 5, errType: 0,
			errSeverity: "Uncorrected (Non-Fatal)", regsDump: "different ignored dump"},
		hip08PCIeLocalTestRow{id: 3, version: 99, socID: 2, socketID: -1, nimbusID: nil,
			subModuleID: "future module", coreID: nil, portID: 7, errType: 0xabcd,
			errSeverity: "Future severity  "},
		hip08PCIeLocalTestRow{id: 4, version: -1, socID: -2, socketID: 3, nimbusID: 4,
			subModuleID: "negative integers", coreID: -5, portID: -6, errType: -1,
			errSeverity: "vendor raw severity"},
		hip08PCIeLocalTestRow{id: 5, version: 2, socID: 1, socketID: 0, nimbusID: 3,
			subModuleID: "wide error code", coreID: 4, portID: 5, errType: int64(0x100001234),
			errSeverity: "Fatal"},
		hip08PCIeLocalTestRow{id: 6},
	)
	pcie := collector.HIP08PCIeLocalCollector{}
	rows, err := pcie.CollectBatch(context.Background(), openHIP08PCIeLocalDatabase(t, path), 500)
	if err != nil || rows != 6 {
		t.Fatalf("CollectBatch() = (%d, %v), want (6, nil)", rows, err)
	}
	want := map[collector.HIP08PCIeLocalLabels]uint64{
		{Version: "2", SoCID: "1", SocketID: "0", NimbusID: "3", SubModuleID: " PCIe local ", CoreID: "4", PortID: "5", ErrType: "0x0", ErrSeverity: "Uncorrected (Non-Fatal)"}:     2,
		{Version: "99", SoCID: "2", SocketID: "-1", SubModuleID: "future module", PortID: "7", ErrType: "0xabcd", ErrSeverity: "Future severity  "}:                                 1,
		{Version: "-1", SoCID: "-2", SocketID: "3", NimbusID: "4", SubModuleID: "negative integers", CoreID: "-5", PortID: "-6", ErrType: "-1", ErrSeverity: "vendor raw severity"}: 1,
		{Version: "2", SoCID: "1", SocketID: "0", NimbusID: "3", SubModuleID: "wide error code", CoreID: "4", PortID: "5", ErrType: "0x100001234", ErrSeverity: "Fatal"}:            1,
		{}: 1,
	}
	if pcie.LastID != 6 || !reflect.DeepEqual(pcie.Counts, want) {
		t.Fatalf("collector = (cursor %d, %#v), want cursor 6 and %#v", pcie.LastID, pcie.Counts, want)
	}
}

func TestHIP08PCIeLocalCollectorPagesAndPreservesTotals(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hip08-pcie-pages.db")
	const total = 1007
	rows := make([]hip08PCIeLocalTestRow, total)
	for i := range rows {
		rows[i] = hip08PCIeLocalTestRow{id: int64(i + 1), version: 2, socID: 1,
			subModuleID: "same module", errType: 0x21, errSeverity: "Corrected"}
	}
	createHIP08PCIeLocalDatabase(t, path, rows...)
	labels := collector.HIP08PCIeLocalLabels{Version: "2", SoCID: "1", SubModuleID: "same module", ErrType: "0x21", ErrSeverity: "Corrected"}
	pcie := collector.HIP08PCIeLocalCollector{Counts: map[collector.HIP08PCIeLocalLabels]uint64{labels: 3}}
	db := openHIP08PCIeLocalDatabase(t, path)
	for round, cursor := range []int64{500, 1000, total} {
		rowsRead, err := pcie.CollectBatch(context.Background(), db, 500)
		wantRows := 500
		if round == 2 {
			wantRows = 7
		}
		if err != nil || rowsRead != wantRows || pcie.LastID != cursor {
			t.Fatalf("round %d = (%d, %v) cursor %d, want (%d, nil) cursor %d",
				round+1, rowsRead, err, pcie.LastID, wantRows, cursor)
		}
	}
	if pcie.Counts[labels] != total+3 {
		t.Fatalf("count = %d, want %d", pcie.Counts[labels], total+3)
	}
}

func TestHIP08PCIeLocalCollectorFailureIsAtomic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hip08-pcie-invalid.db")
	createHIP08PCIeLocalDatabase(t, path,
		hip08PCIeLocalTestRow{id: 1, version: 1, errType: 1, errSeverity: "valid"},
		hip08PCIeLocalTestRow{id: 2, version: "not-an-integer", errType: 2, errSeverity: "invalid"},
	)
	db := openHIP08PCIeLocalDatabase(t, path)
	old := collector.HIP08PCIeLocalLabels{SubModuleID: "old label"}
	pcie := collector.HIP08PCIeLocalCollector{Counts: map[collector.HIP08PCIeLocalLabels]uint64{old: 4}}
	if rows, err := pcie.CollectBatch(context.Background(), db, 10); err == nil || rows != 0 {
		t.Fatalf("CollectBatch() = (%d, %v), want zero-row error", rows, err)
	}
	if pcie.LastID != 0 || !reflect.DeepEqual(pcie.Counts, map[collector.HIP08PCIeLocalLabels]uint64{old: 4}) {
		t.Fatalf("collector partially changed after scan error: cursor=%d counts=%#v", pcie.LastID, pcie.Counts)
	}
	if _, err := db.ExecContext(context.Background(), "UPDATE hip08_pcie_local_event_v2 SET version=2 WHERE id=2"); err != nil {
		t.Fatal(err)
	}
	if rows, err := pcie.CollectBatch(context.Background(), db, 10); err != nil || rows != 2 || pcie.LastID != 2 {
		t.Fatalf("retry = (%d, %v) cursor %d, want (2, nil) cursor 2", rows, err, pcie.LastID)
	}
}

func TestHIP08PCIeLocalCollectorFailureAndSnapshotRestore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hip08-pcie-overflow.db")
	createHIP08PCIeLocalDatabase(t, path, hip08PCIeLocalTestRow{id: 1, version: 2, errType: 3})
	labels := collector.HIP08PCIeLocalLabels{Version: "2", ErrType: "0x3"}
	pcie := collector.HIP08PCIeLocalCollector{Counts: map[collector.HIP08PCIeLocalLabels]uint64{labels: math.MaxUint64}}
	if rows, err := pcie.CollectBatch(context.Background(), openHIP08PCIeLocalDatabase(t, path), 1); err == nil || rows != 0 {
		t.Fatalf("overflow CollectBatch() = (%d, %v), want zero-row error", rows, err)
	}
	if pcie.LastID != 0 || pcie.Counts[labels] != math.MaxUint64 {
		t.Fatalf("collector changed on overflow: cursor=%d counts=%#v", pcie.LastID, pcie.Counts)
	}
	withCount := collector.HIP08PCIeLocalCollector{Counts: map[collector.HIP08PCIeLocalLabels]uint64{labels: 12}}
	families := withCount.ExportMetrics()
	var restored collector.HIP08PCIeLocalCollector
	if err := restored.RestoreMetrics(families); err != nil || restored.Counts[labels] != 12 {
		t.Fatalf("restore = (%v, %#v), want label count 12", err, restored.Counts)
	}
}

func TestHIP08PCIeLocalCollectorCancellationAndIDRollbackAreAtomic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hip08-pcie-cancel.db")
	createHIP08PCIeLocalDatabase(t, path, hip08PCIeLocalTestRow{id: 1, version: 2, errType: 3})
	old := collector.HIP08PCIeLocalLabels{SubModuleID: "old"}
	db := openHIP08PCIeLocalDatabase(t, path)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	pcie := collector.HIP08PCIeLocalCollector{Counts: map[collector.HIP08PCIeLocalLabels]uint64{old: 4}}
	if rows, err := pcie.CollectBatch(ctx, db, 1); err == nil || rows != 0 {
		t.Fatalf("canceled CollectBatch() = (%d, %v), want zero-row error", rows, err)
	}
	if pcie.LastID != 0 || len(pcie.Counts) != 1 || pcie.Counts[old] != 4 {
		t.Fatalf("collector changed after cancellation: cursor=%d counts=%#v", pcie.LastID, pcie.Counts)
	}
	pcie.LastID = 2
	if rows, err := pcie.CollectBatch(context.Background(), db, 1); err == nil || rows != 0 {
		t.Fatalf("rollback CollectBatch() = (%d, %v), want zero-row error", rows, err)
	}
	if pcie.LastID != 2 || len(pcie.Counts) != 1 || pcie.Counts[old] != 4 {
		t.Fatalf("collector changed after ID rollback: cursor=%d counts=%#v", pcie.LastID, pcie.Counts)
	}
}

func TestHIP08PCIeLocalCollectorSeriesLimitIsAtomic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hip08-pcie-series-limit.db")
	createHIP08PCIeLocalDatabase(t, path, hip08PCIeLocalTestRow{id: 1, version: 2, errType: 3})
	pcie := collector.HIP08PCIeLocalCollector{Counts: make(map[collector.HIP08PCIeLocalLabels]uint64, 10000)}
	for i := 0; i < 10000; i++ {
		pcie.Counts[collector.HIP08PCIeLocalLabels{SubModuleID: "old-" + strconv.Itoa(i)}] = 1
	}
	if rows, err := pcie.CollectBatch(context.Background(), openHIP08PCIeLocalDatabase(t, path), 1); err == nil || rows != 0 {
		t.Fatalf("series-limit CollectBatch() = (%d, %v), want zero-row error", rows, err)
	}
	if pcie.LastID != 0 || len(pcie.Counts) != 10000 {
		t.Fatalf("collector changed at series limit: cursor=%d series=%d", pcie.LastID, len(pcie.Counts))
	}
}

func TestHIP08PCIeLocalCollectorRestoreFailureIsAtomic(t *testing.T) {
	labels := collector.HIP08PCIeLocalLabels{Version: "2", ErrType: "0x3"}
	existing := collector.HIP08PCIeLocalLabels{SubModuleID: "old"}
	pcie := collector.HIP08PCIeLocalCollector{Counts: map[collector.HIP08PCIeLocalLabels]uint64{existing: 9}}
	populated := collector.HIP08PCIeLocalCollector{Counts: map[collector.HIP08PCIeLocalLabels]uint64{labels: 2}}
	badFamily := populated.ExportMetrics()
	delete(badFamily[0].Samples[0].Labels, "err_type")
	if err := pcie.RestoreMetrics(badFamily); err == nil {
		t.Fatal("RestoreMetrics accepted a sample missing err_type")
	}
	if len(pcie.Counts) != 1 || pcie.Counts[existing] != 9 {
		t.Fatalf("failed restore replaced existing counts: %#v", pcie.Counts)
	}
	if err := pcie.RestoreMetrics(nil); err != nil || len(pcie.Counts) != 0 {
		t.Fatalf("RestoreMetrics(nil) = (%v, %#v), want cleared counts", err, pcie.Counts)
	}
}

func TestEngineEnrollsHIP08PCIeLocalWithoutReplayingOtherTable(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "rasdaemon.db")
	createSQLite(t, databasePath, devlinkSchema, hip08PCIeLocalSchema)
	devlinkEvent := recoveryEvent(1, "already consumed")
	insertDevlinkEvents(t, databasePath, devlinkEvent)
	insertHIP08PCIeLocalRows(t, databasePath, hip08PCIeLocalTestRow{
		id: 1, version: 2, socID: 1, subModuleID: "local PCIe", errType: 7, errSeverity: "Fatal",
	})
	config := validatedConfig(t, databasePath)
	fixture := engineRecoveryFixture{databasePath: databasePath, snapshotPath: config.SnapshotPath, config: config}
	devlinkLabels := recoveryLabels("already consumed")
	saveRecoveryState(t, fixture, 2, 1, sourceIdentityForTest(t, databasePath), map[string]int64{"devlink_event": 1})
	priorDevlink := collector.DevlinkCollector{Counts: map[collector.DevlinkLabels]uint64{devlinkLabels: 1}}
	if err := collector.SaveSnapshot(fixture.snapshotPath, &collector.Snapshot{
		Generation: 2, Families: priorDevlink.ExportMetrics(),
	}); err != nil {
		t.Fatal(err)
	}
	engine := restoreRecoveryEngine(t, fixture)
	if err := engine.CollectAndCommit(context.Background()); err != nil {
		t.Fatalf("collect newly registered table: %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	state, err := collector.LoadState(config.StateDir)
	if err != nil || state.Generation != 3 || state.Tables["hip08_pcie_local_event_v2"] != 1 || state.Tables["devlink_event"] != 1 {
		t.Fatalf("state after enrollment = (%#v, %v)", state, err)
	}
	engine = restoreRecoveryEngine(t, fixture)
	if err := engine.CollectAndCommit(context.Background()); err != nil {
		t.Fatalf("idle collection after restart: %v", err)
	}
	state, err = collector.LoadState(config.StateDir)
	if err != nil || state.Generation != 3 || state.Tables["hip08_pcie_local_event_v2"] != 1 {
		t.Fatalf("state after restart = (%#v, %v), want unchanged generation and cursor", state, err)
	}
}
