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

const mceTestSchema = `CREATE TABLE mce_record (
	id INTEGER PRIMARY KEY,
	timestamp TEXT,
	mcgcap INTEGER,
	mcgstatus INTEGER,
	status INTEGER,
	addr INTEGER,
	misc INTEGER,
	ip INTEGER,
	tsc INTEGER,
	walltime INTEGER,
	ppin INTEGER,
	cpu INTEGER,
	cpuid INTEGER,
	apicid INTEGER,
	socketid INTEGER,
	cs INTEGER,
	bank INTEGER,
	cpuvendor INTEGER,
	microcode INTEGER,
	bank_name TEXT,
	error_msg TEXT,
	mcgstatus_msg TEXT,
	mcistatus_msg TEXT,
	mcastatus_msg TEXT,
	user_action TEXT,
	mc_location TEXT
)`

type mceTestRow struct {
	id           int64
	cpuVendor    any
	cpuid        any
	socketID     any
	cpu          any
	bank         any
	bankName     any
	errorMsg     any
	mciStatusMsg any
	mcaStatusMsg any
	mcLocation   any
	addr         any
	status       any
	misc         any
	ip           any
	userAction   any
}

func createMCEDatabase(t *testing.T, path string, rows ...mceTestRow) {
	t.Helper()
	createSQLite(t, path, mceTestSchema)
	insertMCERows(t, path, rows...)
}

func insertMCERows(t *testing.T, path string, rows ...mceTestRow) {
	t.Helper()
	if len(rows) == 0 {
		return
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open synthetic mce_record database: %v", err)
	}
	db.SetMaxOpenConns(1)
	defer db.Close()
	const query = `INSERT INTO mce_record (
		id, cpuvendor, cpuid, socketid, cpu, bank, bank_name, error_msg,
		mcistatus_msg, mcastatus_msg, mc_location, addr, status, misc, ip,
		user_action
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	for _, row := range rows {
		if _, err := db.ExecContext(context.Background(), query,
			row.id, row.cpuVendor, row.cpuid, row.socketID, row.cpu, row.bank,
			row.bankName, row.errorMsg, row.mciStatusMsg, row.mcaStatusMsg,
			row.mcLocation, row.addr, row.status, row.misc, row.ip, row.userAction,
		); err != nil {
			t.Fatalf("insert synthetic MCE row %d: %v", row.id, err)
		}
	}
}

func openMCEDatabase(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open synthetic mce_record database: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close synthetic mce_record database: %v", err)
		}
	})
	return db
}

func TestMCECollectorGroupsReportsAndFormatsLabels(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mce-events.db")
	createMCEDatabase(t, path,
		mceTestRow{
			id: 1, cpuVendor: 1, cpuid: int64(0x906ea), socketID: 0, cpu: 7, bank: 4,
			bankName: "MC0", errorMsg: "Memory address=0x1234 row: 17 syndrome 0xA n_errors=2 channel 3",
			mciStatusMsg: "MCi_STATUS[AddrV]: 0xDEAD syndrome=0xFF",
			mcaStatusMsg: "MCA bank 4 address 4096 syndrome 12 n_errors 2",
			mcLocation:   "socket 0 row 17 column 2 address=0x1234",
			addr:         0x1234, status: int64(-8070450532247928831), misc: 0xABC, ip: 0xFFFF,
			userAction: "discarded action",
		},
		mceTestRow{
			id: 2, cpuVendor: 1, cpuid: int64(0x906ea), socketID: 0, cpu: 7, bank: 4,
			bankName: "MC0", errorMsg: "Memory address=0x9876 row: 29 syndrome 0xB n_errors=8 channel 3",
			mciStatusMsg: "MCi_STATUS[AddrV]: 0xDEAD syndrome=0xFF",
			mcaStatusMsg: "MCA bank 4 address 8192 syndrome 13 n_errors 9",
			mcLocation:   "socket 0 row 18 column 5 address=0x9876",
			addr:         0x9876, status: int64(-8070450532247928830), misc: 0xDEF, ip: 0xEEEE,
			userAction: "another discarded action",
		},
		mceTestRow{
			id: 3, cpuVendor: -1, cpuid: int64(0x100000000), socketID: -2, cpu: 8, bank: 5,
			bankName: "MC1", errorMsg: "uncategorized CPU 8 event",
			mciStatusMsg: "status unchanged", mcaStatusMsg: "", mcLocation: "",
		},
		mceTestRow{
			id: 4, cpuVendor: nil, cpuid: nil, socketID: nil, cpu: nil, bank: nil,
			bankName: nil, errorMsg: nil, mciStatusMsg: nil, mcaStatusMsg: nil, mcLocation: nil,
		},
		mceTestRow{
			id: 5, cpuVendor: -3, cpuid: int64(-1), socketID: -1, cpu: -2, bank: -4,
			bankName: "negative fixture", errorMsg: "negative source values",
		},
		mceTestRow{
			id: 6, cpuVendor: 1, cpuid: int64(0x906ea), socketID: 0, cpu: 7, bank: 4,
			bankName: "MC0", errorMsg: "ECC " + string([]byte{0xff, 0xfe}) + " observed",
		},
	)
	digest := fileDigest(t, path)
	db := openMCEDatabase(t, path)
	mce := collector.MCECollector{}
	rows, err := mce.CollectBatch(context.Background(), db, 500)
	if err != nil || rows != 6 {
		t.Fatalf("CollectBatch() = (%d,%v), want (6,nil)", rows, err)
	}
	want := map[collector.MCELabels]uint64{
		{
			CPUVendor: "1", CPUID: "0x000906ea", SocketID: "0", CPU: "7", Bank: "4", BankName: "MC0",
			ErrorMsg:     "Memory address=<omitted> row: <omitted> syndrome <omitted> n_errors=<omitted> channel 3",
			MCIStatusMsg: "MCi_STATUS[AddrV]: 0xDEAD syndrome=0xFF",
			MCAStatusMsg: "MCA bank 4 address <omitted> syndrome <omitted> n_errors <omitted>",
			MCLocation:   "socket 0 row <omitted> column <omitted> address=<omitted>",
		}: 2,
		{
			CPUVendor: "-1", CPUID: "0x100000000", SocketID: "-2", CPU: "8", Bank: "5", BankName: "MC1",
			ErrorMsg: "uncategorized CPU 8 event", MCIStatusMsg: "status unchanged",
		}: 1,
		{
			BankName: "negative fixture", CPUVendor: "-3", CPUID: "-1", SocketID: "-1", CPU: "-2", Bank: "-4",
			ErrorMsg: "negative source values",
		}: 1,
		{
			CPUVendor: "1", CPUID: "0x000906ea", SocketID: "0", CPU: "7", Bank: "4", BankName: "MC0",
			ErrorMsg: "ECC <invalid> observed",
		}: 1,
		{}: 1,
	}
	if mce.LastID != 6 || !reflect.DeepEqual(mce.Counts, want) {
		t.Fatalf("MCE collector = (cursor %d, counts %#v), want cursor 6 and %#v", mce.LastID, mce.Counts, want)
	}
	if got := fileDigest(t, path); got != digest {
		t.Fatal("collection modified the SQLite source")
	}
}

func TestMCECollectorReadsPagesAndCreatesNewLabelGroups(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mce-pages.db")
	const total = 1007
	rows := make([]mceTestRow, total)
	for i := range rows {
		id := int64(i + 1)
		cpuid := int64(0x906ea)
		msg := "same MCE report"
		if id == 1001 {
			cpuid = int64(0x106ca)
			msg = "new MCE report"
		}
		rows[i] = mceTestRow{
			id: id, cpuVendor: 1, cpuid: cpuid, socketID: 0, cpu: 7, bank: 4,
			bankName: "MC0", errorMsg: msg, mciStatusMsg: "status",
		}
	}
	createMCEDatabase(t, path, rows...)
	db := openMCEDatabase(t, path)
	mce := collector.MCECollector{}
	var collected int
	for collected < total {
		count, err := mce.CollectBatch(context.Background(), db, 500)
		if err != nil {
			t.Fatalf("collect page after %d rows: %v", collected, err)
		}
		if count < 1 || count > 500 {
			t.Fatalf("page returned %d rows before end; want 1..500", count)
		}
		collected += count
		if mce.LastID != int64(collected) {
			t.Fatalf("cursor after %d rows = %d", collected, mce.LastID)
		}
	}
	same := collector.MCELabels{CPUVendor: "1", CPUID: "0x000906ea", SocketID: "0", CPU: "7", Bank: "4", BankName: "MC0", ErrorMsg: "same MCE report", MCIStatusMsg: "status"}
	newGroup := same
	newGroup.CPUID = "0x000106ca"
	newGroup.ErrorMsg = "new MCE report"
	if mce.Counts[same] != total-1 || mce.Counts[newGroup] != 1 || len(mce.Counts) != 2 {
		t.Fatalf("page grouping = %#v, want old group %d and new group 1", mce.Counts, total-1)
	}
	if count, err := mce.CollectBatch(context.Background(), db, 500); err != nil || count != 0 || mce.LastID != total {
		t.Fatalf("empty page = (%d rows, cursor %d, %v), want unchanged", count, mce.LastID, err)
	}
}

func TestMCECollectorKeepsCountsAndCursorOnFailure(t *testing.T) {
	t.Run("counter overflow", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "mce-overflow.db")
		createMCEDatabase(t, path, mceTestRow{id: 1, cpuVendor: 1, cpuid: int64(0x906ea), errorMsg: "overflow"})
		labels := collector.MCELabels{CPUVendor: "1", CPUID: "0x000906ea", ErrorMsg: "overflow"}
		mce := collector.MCECollector{Counts: map[collector.MCELabels]uint64{labels: math.MaxUint64}}
		if rows, err := mce.CollectBatch(context.Background(), openMCEDatabase(t, path), 1); err == nil || rows != 0 {
			t.Fatalf("overflow CollectBatch() = (%d,%v), want error and zero rows", rows, err)
		}
		if mce.LastID != 0 || len(mce.Counts) != 1 || mce.Counts[labels] != math.MaxUint64 {
			t.Fatalf("collector changed on overflow: cursor=%d counts=%#v", mce.LastID, mce.Counts)
		}
	})

	t.Run("canceled context", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "mce-canceled.db")
		createMCEDatabase(t, path, mceTestRow{id: 1, cpuVendor: 1, cpuid: int64(0x906ea), errorMsg: "cancel"})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		mce := collector.MCECollector{}
		if rows, err := mce.CollectBatch(ctx, openMCEDatabase(t, path), 1); err == nil || rows != 0 {
			t.Fatalf("canceled CollectBatch() = (%d,%v), want error and zero rows", rows, err)
		}
		if mce.LastID != 0 || len(mce.Counts) != 0 {
			t.Fatalf("collector changed after cancellation: cursor=%d counts=%#v", mce.LastID, mce.Counts)
		}
	})

	t.Run("source ID rollback", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "mce-rollback.db")
		createMCEDatabase(t, path, mceTestRow{id: 2, cpuVendor: 1, cpuid: int64(0x906ea)})
		mce := collector.MCECollector{LastID: 3}
		if rows, err := mce.CollectBatch(context.Background(), openMCEDatabase(t, path), 1); err == nil || rows != 0 {
			t.Fatalf("rollback CollectBatch() = (%d,%v), want error and zero rows", rows, err)
		}
		if mce.LastID != 3 || len(mce.Counts) != 0 {
			t.Fatalf("collector changed after source rollback: cursor=%d counts=%#v", mce.LastID, mce.Counts)
		}
	})
}

func TestMCECollectorSeriesLimitFailureIsAtomic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mce-series-limit.db")
	createMCEDatabase(t, path, mceTestRow{
		id: 1, cpuVendor: 1, cpuid: int64(0x906ea), errorMsg: "new event",
	})
	mce := collector.MCECollector{Counts: make(map[collector.MCELabels]uint64, 10000)}
	for i := 0; i < 10000; i++ {
		labels := collector.MCELabels{CPUVendor: "old", CPUID: "0x00000001", ErrorMsg: "event" + strconv.Itoa(i)}
		mce.Counts[labels] = 1
	}
	if rows, err := mce.CollectBatch(context.Background(), openMCEDatabase(t, path), 1); err == nil || rows != 0 {
		t.Fatalf("series-limit CollectBatch() = (%d,%v), want error and zero rows", rows, err)
	}
	if mce.LastID != 0 || len(mce.Counts) != 10000 {
		t.Fatalf("collector changed after series-limit error: cursor=%d series=%d", mce.LastID, len(mce.Counts))
	}
}

func TestMCECollectorRestoresSnapshotWithoutReinterpretingLabels(t *testing.T) {
	labels := collector.MCELabels{
		CPUVendor: "1", CPUID: "0x000906ea", SocketID: "0", CPU: "7", Bank: "4", BankName: "MC0",
		ErrorMsg: "error address <omitted>", MCIStatusMsg: "raw status 0x1234",
		MCAStatusMsg: "MCA <omitted>", MCLocation: "socket 0",
	}
	want := map[collector.MCELabels]uint64{labels: 12}
	original := collector.MCECollector{Counts: want}
	path := filepath.Join(t.TempDir(), "mce.prom")
	if err := collector.SaveSnapshot(path, &collector.Snapshot{Generation: 7, Families: original.ExportMetrics()}); err != nil {
		t.Fatalf("save MCE snapshot: %v", err)
	}
	snapshot, err := collector.LoadSnapshot(path)
	if err != nil {
		t.Fatalf("load MCE snapshot: %v", err)
	}
	var restored collector.MCECollector
	if err := restored.RestoreMetrics(snapshot.Families); err != nil {
		t.Fatalf("restore MCE snapshot: %v", err)
	}
	if !reflect.DeepEqual(restored.Counts, want) {
		t.Fatalf("restored MCE counts = %#v, want %#v", restored.Counts, want)
	}
	if err := restored.RestoreMetrics(nil); err != nil || len(restored.Counts) != 0 {
		t.Fatalf("RestoreMetrics(nil) = (%v,%#v), want empty count map", err, restored.Counts)
	}
}

func TestEngineEnrollsMCEWithoutReplayingExistingTables(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "rasdaemon.db")
	createSQLite(t, databasePath, devlinkSchema, mceTestSchema)
	insertDevlinkEvents(t, databasePath, recoveryEvent(1, "already consumed"))
	insertMCERows(t, databasePath, mceTestRow{
		id: 1, cpuVendor: 1, cpuid: int64(0x906ea), socketID: 0, cpu: 7, bank: 4,
		bankName: "MC0", errorMsg: "Machine Check report",
	})
	config := validatedConfig(t, databasePath)
	fixture := engineRecoveryFixture{databasePath: databasePath, snapshotPath: config.SnapshotPath, config: config}
	saveRecoveryState(t, fixture, 2, 1, sourceIdentityForTest(t, databasePath), map[string]int64{"devlink_event": 1})
	devlink := collector.DevlinkCollector{Counts: map[collector.DevlinkLabels]uint64{
		recoveryLabels("already consumed"): 1,
	}}
	if err := collector.SaveSnapshot(fixture.snapshotPath, &collector.Snapshot{Generation: 2, Families: devlink.ExportMetrics()}); err != nil {
		t.Fatalf("save legacy snapshot: %v", err)
	}
	engine := restoreRecoveryEngine(t, fixture)
	if err := engine.CollectAndCommit(context.Background()); err != nil {
		t.Fatalf("collect after MCE enrollment: %v", err)
	}
	assertEnrolledMCEPair(t, fixture, 3, 1)
	if err := engine.Close(); err != nil {
		t.Fatalf("close engine after MCE enrollment: %v", err)
	}

	engine = restoreRecoveryEngine(t, fixture)
	if err := engine.CollectAndCommit(context.Background()); err != nil {
		t.Fatalf("idle collect after MCE restart: %v", err)
	}
	assertEnrolledMCEPair(t, fixture, 3, 1)
}

func assertEnrolledMCEPair(t *testing.T, fixture engineRecoveryFixture, generation uint64, cursor int64) {
	t.Helper()
	state, err := collector.LoadState(fixture.config.StateDir)
	if err != nil {
		t.Fatalf("LoadState() error = %v", err)
	}
	if state.Generation != generation || state.Tables["mce_record"] != cursor || state.Tables["devlink_event"] != 1 {
		t.Fatalf("state after MCE enrollment = %#v, want generation %d and both cursors", state, generation)
	}
	snapshot, err := collector.LoadSnapshot(fixture.snapshotPath)
	if err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	var mce collector.MCECollector
	var families []collector.CounterFamily
	for _, family := range snapshot.Families {
		if family.Name == "rasdaemon_mce_events_total" {
			families = append(families, family)
		}
	}
	if err := mce.RestoreMetrics(families); err != nil {
		t.Fatalf("restore MCE family: %v", err)
	}
	want := map[collector.MCELabels]uint64{composeMCELabels(): 1}
	if !reflect.DeepEqual(mce.Counts, want) {
		t.Fatalf("MCE snapshot counts = %#v, want %#v", mce.Counts, want)
	}
	var restoredDevlink collector.DevlinkCollector
	var devlinkFamilies []collector.CounterFamily
	for _, family := range snapshot.Families {
		if family.Name == "rasdaemon_devlink_health_events_total" {
			devlinkFamilies = append(devlinkFamilies, family)
		}
	}
	if err := restoredDevlink.RestoreMetrics(devlinkFamilies); err != nil {
		t.Fatalf("restore devlink family: %v", err)
	}
	if !reflect.DeepEqual(restoredDevlink.Counts, map[collector.DevlinkLabels]uint64{recoveryLabels("already consumed"): 1}) {
		t.Fatalf("existing devlink counts changed: %#v", restoredDevlink.Counts)
	}
}

func composeMCELabels() collector.MCELabels {
	return collector.MCELabels{
		CPUVendor: "1", CPUID: "0x000906ea", SocketID: "0", CPU: "7", Bank: "4", BankName: "MC0",
		ErrorMsg: "Machine Check report",
	}
}
