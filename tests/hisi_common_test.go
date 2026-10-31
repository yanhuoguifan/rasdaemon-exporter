package collector_test

import (
	"context"
	"database/sql"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"rasdaemon-exporter/internal/collector"
)

const hisiCommonTestSchema = `CREATE TABLE hisi_common_section_v2 (
	id INTEGER PRIMARY KEY,
	timestamp TEXT,
	version INTEGER,
	soc_id INTEGER,
	socket_id INTEGER,
	totem_id INTEGER,
	nimbus_id INTEGER,
	sub_system_id INTEGER,
	module_id TEXT,
	sub_module_id INTEGER,
	core_id INTEGER,
	port_id INTEGER,
	err_type INTEGER,
	pcie_info TEXT,
	err_severity TEXT,
	regs_dump TEXT
)`

type hisiCommonTestRow struct {
	id          any
	timestamp   any
	version     any
	socID       any
	socketID    any
	totemID     any
	nimbusID    any
	subSystemID any
	moduleID    any
	subModuleID any
	coreID      any
	portID      any
	errType     any
	pcieInfo    any
	errSeverity any
	regsDump    any
}

func createHisiCommonDatabase(t *testing.T, path string, rows ...hisiCommonTestRow) {
	t.Helper()
	createSQLite(t, path, hisiCommonTestSchema)
	insertHisiCommonRows(t, path, rows...)
}

func insertHisiCommonRows(t *testing.T, path string, rows ...hisiCommonTestRow) {
	t.Helper()
	if len(rows) == 0 {
		return
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open synthetic hisi_common_section_v2 database: %v", err)
	}
	db.SetMaxOpenConns(1)
	defer db.Close()
	const query = `INSERT INTO hisi_common_section_v2 (
		id, timestamp, version, soc_id, socket_id, totem_id, nimbus_id, sub_system_id,
		module_id, sub_module_id, core_id, port_id, err_type, pcie_info,
		err_severity, regs_dump
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	for _, row := range rows {
		if _, err := db.ExecContext(context.Background(), query,
			row.id, row.timestamp, row.version, row.socID, row.socketID, row.totemID,
			row.nimbusID, row.subSystemID, row.moduleID, row.subModuleID,
			row.coreID, row.portID, row.errType, row.pcieInfo, row.errSeverity,
			row.regsDump,
		); err != nil {
			t.Fatalf("insert synthetic hisi_common_section_v2 row %v: %v", row.id, err)
		}
	}
}

func openHisiCommonDatabase(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open synthetic hisi_common_section_v2 database: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close synthetic hisi_common_section_v2 database: %v", err)
		}
	})
	return db
}

func TestHisiCommonCollectorPreservesFieldsAndCountsRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hisi-common.db")
	createHisiCommonDatabase(t, path,
		hisiCommonTestRow{
			id: 1, version: 2, socID: 1, socketID: 0, totemID: 3, nimbusID: 4,
			subSystemID: 5, moduleID: "PCIe Local", subModuleID: 6,
			coreID: 7, portID: 8, errType: 0x21,
			pcieInfo: "0000:03:00.0", errSeverity: "Fatal", regsDump: "secret register dump",
		},
		hisiCommonTestRow{
			id: 2, version: 2, socID: 1, socketID: 0, totemID: 3, nimbusID: 4,
			subSystemID: 5, moduleID: "PCIe Local", subModuleID: 6,
			coreID: 7, portID: 8, errType: 0x21,
			pcieInfo: "0000:03:00.0", errSeverity: "Fatal", regsDump: "different register dump",
		},
		hisiCommonTestRow{
			id: 3, version: 99, socID: -1, socketID: -2, totemID: 0,
			nimbusID: 1, subSystemID: 2, moduleID: "future module",
			subModuleID: 3, coreID: 4, portID: 5, errType: 0x1234,
			pcieInfo: "unfamiliar but preserved", errSeverity: "Future Severity",
		},
		hisiCommonTestRow{
			id: 4, version: nil, socID: nil, socketID: nil, totemID: nil,
			nimbusID: nil, subSystemID: nil, moduleID: nil, subModuleID: nil,
			coreID: nil, portID: nil, errType: nil, pcieInfo: nil, errSeverity: nil,
		},
	)
	digest := fileDigest(t, path)
	db := openHisiCommonDatabase(t, path)
	common := collector.HisiCommonCollector{}
	rows, err := common.CollectBatch(context.Background(), db, 500)
	if err != nil || rows != 4 {
		t.Fatalf("CollectBatch() = (%d, %v), want (4, nil)", rows, err)
	}
	want := map[collector.HisiCommonLabels]uint64{
		{
			Version: "2", SoCID: "1", SocketID: "0", TotemID: "3", NimbusID: "4",
			SubSystemID: "5", ModuleID: "PCIe Local", SubModuleID: "6",
			CoreID: "7", PortID: "8", ErrType: "33",
			PCIeInfo: "0000:03:00.0", ErrSeverity: "Fatal", DeviceName: "unknown",
		}: 2,
		{
			Version: "99", SoCID: "-1", SocketID: "-2", TotemID: "0", NimbusID: "1",
			SubSystemID: "2", ModuleID: "future module", SubModuleID: "3",
			CoreID: "4", PortID: "5", ErrType: "4660",
			PCIeInfo: "unfamiliar but preserved", ErrSeverity: "Future Severity", DeviceName: "unknown",
		}: 1,
		{DeviceName: "unknown"}: 1,
	}
	if common.LastID != 4 || !reflect.DeepEqual(common.Counts, want) {
		t.Fatalf("collector = (cursor %d, %#v), want cursor 4 and %#v", common.LastID, common.Counts, want)
	}
	if got := fileDigest(t, path); got != digest {
		t.Fatal("collection modified the source SQLite database")
	}
}

func TestHisiCommonCollectorResolvesCurrentBootPCINameFreshly(t *testing.T) {
	boot := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	stamp := pciEventTime(boot.Add(time.Hour))
	bdf := "0000:03:00.0"
	sysfs := t.TempDir()
	addFakePCIDevice(t, sysfs, bdf, "0x1234", "0x5678")
	pciIDs := filepath.Join(t.TempDir(), "pci.ids")
	writeIDs := func(vendor, device, model string) {
		t.Helper()
		if err := os.WriteFile(pciIDs, []byte(vendor+" Example Vendor\n\t"+device+" "+model+"\n"), 0o644); err != nil {
			t.Fatalf("write fake PCI names: %v", err)
		}
	}
	writeIDs("1234", "5678", "First Model")
	path := filepath.Join(t.TempDir(), "hisi-common-pcie.db")
	createHisiCommonDatabase(t, path, hisiCommonTestRow{
		id: 1, timestamp: stamp, pcieInfo: bdf, errSeverity: "severity one",
	})
	common := collector.HisiCommonCollector{
		DeviceResolver: &collector.AERDeviceResolver{BootTime: boot, SysfsRoot: sysfs, PCIIDsPath: pciIDs},
	}
	if rows, err := common.CollectBatch(context.Background(), openHisiCommonDatabase(t, path), 1); err != nil || rows != 1 {
		t.Fatalf("first CollectBatch() = (%d, %v), want (1, nil)", rows, err)
	}
	if err := os.WriteFile(filepath.Join(sysfs, bdf, "vendor"), []byte("0xabcd\n"), 0o644); err != nil {
		t.Fatalf("update fake PCI vendor ID: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sysfs, bdf, "device"), []byte("0xef01\n"), 0o644); err != nil {
		t.Fatalf("update fake PCI device ID: %v", err)
	}
	writeIDs("abcd", "ef01", "Updated Model")
	insertHisiCommonRows(t, path, hisiCommonTestRow{
		id: 2, timestamp: stamp, pcieInfo: bdf, errSeverity: "severity two",
	})
	if rows, err := common.CollectBatch(context.Background(), openHisiCommonDatabase(t, path), 1); err != nil || rows != 1 {
		t.Fatalf("second CollectBatch() = (%d, %v), want (1, nil)", rows, err)
	}
	if common.Counts[collector.HisiCommonLabels{PCIeInfo: bdf, ErrSeverity: "severity one", DeviceName: "Example Vendor First Model"}] != 1 {
		t.Fatalf("first resolved PCI name was not retained in its series: %#v", common.Counts)
	}
	if common.Counts[collector.HisiCommonLabels{PCIeInfo: bdf, ErrSeverity: "severity two", DeviceName: "Example Vendor Updated Model"}] != 1 {
		t.Fatalf("resolver reused a cached PCI name: %#v", common.Counts)
	}
}

func TestHisiCommonCollectorKeepsRawPCIInfoWhenResolutionIsUnavailable(t *testing.T) {
	boot := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	bdf := "0000:03:00.0"
	sysfs := t.TempDir()
	addFakePCIDevice(t, sysfs, bdf, "0x1234", "0x5678")
	pciIDs := filepath.Join(t.TempDir(), "pci.ids")
	if err := os.WriteFile(pciIDs, []byte("1234 Example Vendor\n\t5678 Example PCI Device\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "hisi-common-unknown-pcie.db")
	createHisiCommonDatabase(t, path,
		hisiCommonTestRow{id: 1, timestamp: pciEventTime(boot.Add(-time.Minute)), pcieInfo: bdf},
		hisiCommonTestRow{id: 2, timestamp: pciEventTime(boot), pcieInfo: bdf},
		hisiCommonTestRow{id: 3, timestamp: "invalid timestamp", pcieInfo: bdf},
		hisiCommonTestRow{id: 4, timestamp: pciEventTime(boot.Add(time.Minute)), pcieInfo: "03:00.0"},
	)
	common := collector.HisiCommonCollector{
		DeviceResolver: &collector.AERDeviceResolver{BootTime: boot, SysfsRoot: sysfs, PCIIDsPath: pciIDs},
	}
	if rows, err := common.CollectBatch(context.Background(), openHisiCommonDatabase(t, path), 10); err != nil || rows != 4 {
		t.Fatalf("CollectBatch() = (%d, %v), want (4, nil)", rows, err)
	}
	if common.Counts[collector.HisiCommonLabels{PCIeInfo: bdf, DeviceName: "unknown"}] != 3 {
		t.Fatalf("pre-boot/equal-boot/invalid-time events were not retained with unknown name: %#v", common.Counts)
	}
	if common.Counts[collector.HisiCommonLabels{PCIeInfo: "03:00.0", DeviceName: "unknown"}] != 1 {
		t.Fatalf("raw short PCIe info was not preserved: %#v", common.Counts)
	}
}

func TestHisiCommonCollectorPagesAndPreservesTotals(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hisi-common-pages.db")
	const total = 1007
	rows := make([]hisiCommonTestRow, total)
	for i := range rows {
		rows[i] = hisiCommonTestRow{
			id: i + 1, version: 2, socID: 1, moduleID: "module",
			errType: 0x21, pcieInfo: "0000:03:00.0", errSeverity: "Corrected",
		}
	}
	createHisiCommonDatabase(t, path, rows...)
	labels := collector.HisiCommonLabels{
		Version: "2", SoCID: "1", ModuleID: "module", ErrType: "33",
		PCIeInfo: "0000:03:00.0", ErrSeverity: "Corrected", DeviceName: "unknown",
	}
	common := collector.HisiCommonCollector{Counts: map[collector.HisiCommonLabels]uint64{labels: 3}}
	db := openHisiCommonDatabase(t, path)
	for round, wantCursor := range []int64{500, 1000, total} {
		got, err := common.CollectBatch(context.Background(), db, 500)
		wantRows := 500
		if round == 2 {
			wantRows = 7
		}
		if err != nil || got != wantRows || common.LastID != wantCursor {
			t.Fatalf("round %d = (%d, %v) cursor %d, want (%d, nil) cursor %d",
				round+1, got, err, common.LastID, wantRows, wantCursor)
		}
	}
	if common.Counts[labels] != total+3 {
		t.Fatalf("cumulative count = %d, want %d", common.Counts[labels], total+3)
	}
}

func TestHisiCommonCollectorKeepsProgressAtomicOnFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hisi-common-invalid.db")
	createHisiCommonDatabase(t, path,
		hisiCommonTestRow{id: 1, moduleID: "valid", errType: 1},
		hisiCommonTestRow{id: 2, moduleID: "bad integer", errType: "not-an-integer"},
	)
	db := openHisiCommonDatabase(t, path)
	old := collector.HisiCommonLabels{ModuleID: "already accumulated"}
	common := collector.HisiCommonCollector{Counts: map[collector.HisiCommonLabels]uint64{old: 5}}
	if rows, err := common.CollectBatch(context.Background(), db, 10); err == nil || rows != 0 {
		t.Fatalf("CollectBatch() = (%d, %v), want zero-row error", rows, err)
	}
	if common.LastID != 0 || len(common.Counts) != 1 || common.Counts[old] != 5 {
		t.Fatalf("collector changed on scan failure: cursor=%d counts=%#v", common.LastID, common.Counts)
	}
	if _, err := db.ExecContext(context.Background(),
		"UPDATE hisi_common_section_v2 SET err_type = 2 WHERE id = 2"); err != nil {
		t.Fatalf("repair synthetic source row: %v", err)
	}
	if rows, err := common.CollectBatch(context.Background(), db, 10); err != nil || rows != 2 || common.LastID != 2 {
		t.Fatalf("retry CollectBatch() = (%d, %v) cursor %d, want (2, nil) cursor 2", rows, err, common.LastID)
	}
}

func TestHisiCommonCollectorCancellationAndIDRollbackAreAtomic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hisi-common-cancel.db")
	createHisiCommonDatabase(t, path, hisiCommonTestRow{id: 1, version: 2, errType: 1})
	old := collector.HisiCommonLabels{ModuleID: "already accumulated", DeviceName: "unknown"}
	db := openHisiCommonDatabase(t, path)
	common := collector.HisiCommonCollector{Counts: map[collector.HisiCommonLabels]uint64{old: 4}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if rows, err := common.CollectBatch(ctx, db, 1); err == nil || rows != 0 {
		t.Fatalf("canceled CollectBatch() = (%d, %v), want zero-row error", rows, err)
	}
	if common.LastID != 0 || len(common.Counts) != 1 || common.Counts[old] != 4 {
		t.Fatalf("collector changed after cancellation: cursor=%d counts=%#v", common.LastID, common.Counts)
	}
	common.LastID = 2
	if rows, err := common.CollectBatch(context.Background(), db, 1); err == nil || rows != 0 {
		t.Fatalf("rolled-back CollectBatch() = (%d, %v), want zero-row error", rows, err)
	}
	if common.LastID != 2 || len(common.Counts) != 1 || common.Counts[old] != 4 {
		t.Fatalf("collector changed after ID rollback: cursor=%d counts=%#v", common.LastID, common.Counts)
	}
}

func TestHisiCommonCollectorChecksOverflowAndSeriesLimitAtomically(t *testing.T) {
	t.Run("counter overflow", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "hisi-common-overflow.db")
		createHisiCommonDatabase(t, path, hisiCommonTestRow{
			id: 1, version: 2, moduleID: "overflow", errType: 1,
		})
		labels := collector.HisiCommonLabels{Version: "2", ModuleID: "overflow", ErrType: "1", DeviceName: "unknown"}
		common := collector.HisiCommonCollector{Counts: map[collector.HisiCommonLabels]uint64{labels: math.MaxUint64}}
		if rows, err := common.CollectBatch(context.Background(), openHisiCommonDatabase(t, path), 1); err == nil || rows != 0 {
			t.Fatalf("overflow CollectBatch() = (%d, %v), want an error", rows, err)
		}
		if common.LastID != 0 || common.Counts[labels] != math.MaxUint64 {
			t.Fatalf("collector changed on overflow: cursor=%d counts=%#v", common.LastID, common.Counts)
		}
	})

	t.Run("series limit", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "hisi-common-series.db")
		createHisiCommonDatabase(t, path, hisiCommonTestRow{id: 1, moduleID: "new", errType: 1})
		common := collector.HisiCommonCollector{Counts: make(map[collector.HisiCommonLabels]uint64, 10000)}
		for i := 0; i < 10000; i++ {
			common.Counts[collector.HisiCommonLabels{ModuleID: "old-" + strconv.Itoa(i)}] = 1
		}
		if rows, err := common.CollectBatch(context.Background(), openHisiCommonDatabase(t, path), 1); err == nil || rows != 0 {
			t.Fatalf("series-limit CollectBatch() = (%d, %v), want an error", rows, err)
		}
		if common.LastID != 0 || len(common.Counts) != 10000 {
			t.Fatalf("collector changed at series limit: cursor=%d series=%d", common.LastID, len(common.Counts))
		}
	})
}

func TestHisiCommonCollectorRestoresSnapshotWithoutReinterpretingLabels(t *testing.T) {
	labels := collector.HisiCommonLabels{
		Version: "9", SoCID: "-1", ModuleID: "Unknown module", SubModuleID: "42",
		ErrType: "65535", PCIeInfo: "vendor-formatted PCI address", ErrSeverity: "Future severity",
		DeviceName: "saved device name",
	}
	want := map[collector.HisiCommonLabels]uint64{labels: 12}
	original := collector.HisiCommonCollector{Counts: want}
	path := filepath.Join(t.TempDir(), "hisi-common.prom")
	if err := collector.SaveSnapshot(path, &collector.Snapshot{Generation: 7, Families: original.ExportMetrics()}); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}
	snapshot, err := collector.LoadSnapshot(path)
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	var restored collector.HisiCommonCollector
	if err := restored.RestoreMetrics(snapshot.Families); err != nil {
		t.Fatalf("restore snapshot: %v", err)
	}
	if !reflect.DeepEqual(restored.Counts, want) {
		t.Fatalf("restored labels = %#v, want %#v", restored.Counts, want)
	}
	if err := restored.RestoreMetrics(nil); err != nil || len(restored.Counts) != 0 {
		t.Fatalf("RestoreMetrics(nil) = (%v, %#v), want an empty count map", err, restored.Counts)
	}
}

func TestHisiCommonCollectorPreservesCountsWhenRestoreFails(t *testing.T) {
	existing := collector.HisiCommonLabels{ModuleID: "existing"}
	common := collector.HisiCommonCollector{Counts: map[collector.HisiCommonLabels]uint64{existing: 4}}
	replacement := collector.HisiCommonCollector{Counts: map[collector.HisiCommonLabels]uint64{{ModuleID: "replacement"}: 9}}
	family := replacement.ExportMetrics()
	delete(family[0].Samples[0].Labels, "pcie_info")
	if err := common.RestoreMetrics(family); err == nil {
		t.Fatal("RestoreMetrics accepted a sample missing pcie_info")
	}
	if len(common.Counts) != 1 || common.Counts[existing] != 4 {
		t.Fatalf("counts changed after failed restore: %#v", common.Counts)
	}
}

func TestEngineEnrollsHisiCommonWithoutReplayingExistingTables(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "rasdaemon.db")
	createSQLite(t, databasePath, devlinkSchema, hisiCommonTestSchema)
	insertDevlinkEvents(t, databasePath, recoveryEvent(1, "already consumed"))
	insertHisiCommonRows(t, databasePath, hisiCommonTestRow{
		id: 1, version: 2, socID: 1, socketID: 0, totemID: 3, nimbusID: 4,
		subSystemID: 5, moduleID: "PCIe Local", subModuleID: 6,
		coreID: 7, portID: 8, errType: 9, pcieInfo: "0000:03:00.0", errSeverity: "Fatal",
	})
	config := validatedConfig(t, databasePath)
	fixture := engineRecoveryFixture{databasePath: databasePath, snapshotPath: config.SnapshotPath, config: config}
	devlinkLabels := recoveryLabels("already consumed")
	saveRecoveryState(t, fixture, 2, 1, sourceIdentityForTest(t, databasePath), map[string]int64{"devlink_event": 1})
	devlink := collector.DevlinkCollector{Counts: map[collector.DevlinkLabels]uint64{devlinkLabels: 1}}
	if err := collector.SaveSnapshot(fixture.snapshotPath, &collector.Snapshot{Generation: 2, Families: devlink.ExportMetrics()}); err != nil {
		t.Fatalf("save old snapshot: %v", err)
	}
	engine := restoreRecoveryEngine(t, fixture)
	if err := engine.CollectAndCommit(context.Background()); err != nil {
		t.Fatalf("collect after Hisi common enrollment: %v", err)
	}
	assertHisiCommonPair(t, fixture, 3, devlinkLabels)
	if err := engine.Close(); err != nil {
		t.Fatalf("close engine: %v", err)
	}
	engine = restoreRecoveryEngine(t, fixture)
	if err := engine.CollectAndCommit(context.Background()); err != nil {
		t.Fatalf("collect after restart: %v", err)
	}
	assertHisiCommonPair(t, fixture, 3, devlinkLabels)
}

func assertHisiCommonPair(t *testing.T, fixture engineRecoveryFixture, generation uint64, devlinkLabels collector.DevlinkLabels) {
	t.Helper()
	state, err := collector.LoadState(fixture.config.StateDir)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if state.Generation != generation || state.Tables["hisi_common_section_v2"] != 1 || state.Tables["devlink_event"] != 1 {
		t.Fatalf("state = %#v, want generation %d with both cursors at 1", state, generation)
	}
	snapshot, err := collector.LoadSnapshot(fixture.snapshotPath)
	if err != nil || snapshot.Generation != generation {
		t.Fatalf("load snapshot = (%#v, %v), want generation %d", snapshot, err, generation)
	}
	var common collector.HisiCommonCollector
	var commonFamilies []collector.CounterFamily
	var devlink collector.DevlinkCollector
	var devlinkFamilies []collector.CounterFamily
	for _, family := range snapshot.Families {
		switch family.Name {
		case "rasdaemon_hisi_common_events_total":
			commonFamilies = append(commonFamilies, family)
		case "rasdaemon_devlink_health_events_total":
			devlinkFamilies = append(devlinkFamilies, family)
		default:
			t.Fatalf("unexpected metric family %q", family.Name)
		}
	}
	if err := common.RestoreMetrics(commonFamilies); err != nil {
		t.Fatalf("restore Hisi common family: %v", err)
	}
	wantCommon := map[collector.HisiCommonLabels]uint64{{
		Version: "2", SoCID: "1", SocketID: "0", TotemID: "3", NimbusID: "4",
		SubSystemID: "5", ModuleID: "PCIe Local", SubModuleID: "6", CoreID: "7", PortID: "8",
		ErrType: "9", PCIeInfo: "0000:03:00.0", ErrSeverity: "Fatal", DeviceName: "unknown",
	}: 1}
	if !reflect.DeepEqual(common.Counts, wantCommon) {
		t.Fatalf("Hisi common counts = %#v, want %#v", common.Counts, wantCommon)
	}
	if err := devlink.RestoreMetrics(devlinkFamilies); err != nil {
		t.Fatalf("restore existing devlink family: %v", err)
	}
	if !reflect.DeepEqual(devlink.Counts, map[collector.DevlinkLabels]uint64{devlinkLabels: 1}) {
		t.Fatalf("devlink counts changed after enrollment: %#v", devlink.Counts)
	}
}
