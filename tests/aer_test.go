package collector_test

import (
	"context"
	"database/sql"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"rasdaemon-exporter/internal/collector"
)

const aerTestSchema = `CREATE TABLE aer_event (
	id INTEGER PRIMARY KEY,
	timestamp TEXT,
	dev_name TEXT,
	err_type TEXT,
	err_msg TEXT
)`

type aerTestRow struct {
	id        any
	timestamp any
	devName   any
	errType   any
	errMsg    any
}

func createAERDatabase(t *testing.T, path string, rows ...aerTestRow) {
	t.Helper()
	createSQLite(t, path, aerTestSchema)
	insertAERRows(t, path, rows...)
}

func insertAERRows(t *testing.T, path string, rows ...aerTestRow) {
	t.Helper()
	if len(rows) == 0 {
		return
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open synthetic aer_event database for insertion: %v", err)
	}
	db.SetMaxOpenConns(1)
	defer db.Close()
	for _, row := range rows {
		if _, err := db.ExecContext(context.Background(), `INSERT INTO aer_event
			(id, timestamp, dev_name, err_type, err_msg) VALUES (?, ?, ?, ?, ?)`,
			row.id, row.timestamp, row.devName, row.errType, row.errMsg); err != nil {
			t.Fatalf("insert synthetic aer_event row %v: %v", row.id, err)
		}
	}
}

func openAERDatabase(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open synthetic aer_event database: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close synthetic aer_event database: %v", err)
		}
	})
	return db
}

func pciEventTime(t time.Time) string {
	return t.Format("2006-01-02 15:04:05 -0700")
}

func addFakePCIDevice(t *testing.T, root, bdf, vendor, device string) {
	t.Helper()
	directory := filepath.Join(root, bdf)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		t.Fatalf("create fake PCI sysfs directory: %v", err)
	}
	for name, value := range map[string]string{"vendor": vendor, "device": device} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte(value+"\n"), 0o644); err != nil {
			t.Fatalf("write fake PCI sysfs %s: %v", name, err)
		}
	}
}

func TestAERCollectorAccumulatesRawLabelsAndCompactsMessages(t *testing.T) {
	path := filepath.Join(t.TempDir(), "aer-events.db")
	boot := time.Date(2026, 10, 25, 0, 0, 0, 0, time.UTC)
	bdf := "0000:03:00.0"
	sysfs := t.TempDir()
	addFakePCIDevice(t, sysfs, bdf, "0x1234", "0x5678")
	pciIDs := filepath.Join(t.TempDir(), "pci.ids")
	if err := os.WriteFile(pciIDs, []byte("1234 Example Vendor\n\t5678 Example PCI Device\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	createAERDatabase(t, path,
		aerTestRow{id: 1, timestamp: pciEventTime(boot.Add(time.Minute)), devName: bdf,
			errType: "Uncorrected (Non-Fatal)", errMsg: "  Completion Timeout address=0x1234 row 17 TLP Header: 01 02 03 04  "},
		aerTestRow{id: 2, timestamp: pciEventTime(boot.Add(2 * time.Minute)), devName: bdf,
			errType: "Uncorrected (Non-Fatal)", errMsg: "  Completion Timeout address=0x9876 row 29 TLP Header: 05 06 07 08  "},
		aerTestRow{id: 3, timestamp: pciEventTime(boot.Add(3 * time.Minute)), devName: bdf,
			errType: "Corrected", errMsg: "Receiver Error"},
		aerTestRow{id: 4, timestamp: pciEventTime(boot.Add(4 * time.Minute)), devName: nil,
			errType: nil, errMsg: nil},
		aerTestRow{id: 5, timestamp: pciEventTime(boot.Add(5 * time.Minute)), devName: "03:00.0",
			errType: "Corrected", errMsg: "Short BDF"},
	)
	db := openAERDatabase(t, path)
	aer := collector.AERCollector{DeviceResolver: &collector.AERDeviceResolver{
		BootTime: boot, SysfsRoot: sysfs, PCIIDsPath: pciIDs,
	}}
	rows, err := aer.CollectBatch(context.Background(), db, 3)
	if err != nil || rows != 3 {
		t.Fatalf("first AER batch = (%d, %v), want (3, nil)", rows, err)
	}
	if aer.LastID != 3 {
		t.Fatalf("cursor after first batch = %d, want 3", aer.LastID)
	}
	want := map[collector.AERLabels]uint64{
		{DevName: bdf, ErrType: "Uncorrected (Non-Fatal)", ErrMsg: "  Completion Timeout address=<omitted> row <omitted>", DeviceName: "Example Vendor Example PCI Device"}: 2,
		{DevName: bdf, ErrType: "Corrected", ErrMsg: "Receiver Error", DeviceName: "Example Vendor Example PCI Device"}:                                                     1,
	}
	if !reflect.DeepEqual(aer.Counts, want) {
		t.Fatalf("first batch counts = %#v, want %#v", aer.Counts, want)
	}
	rows, err = aer.CollectBatch(context.Background(), db, 3)
	if err != nil || rows != 2 {
		t.Fatalf("second AER batch = (%d, %v), want (2, nil)", rows, err)
	}
	want[collector.AERLabels{DeviceName: "unknown"}] = 1
	want[collector.AERLabels{DevName: "03:00.0", ErrType: "Corrected", ErrMsg: "Short BDF", DeviceName: "unknown"}] = 1
	if aer.LastID != 5 || !reflect.DeepEqual(aer.Counts, want) {
		t.Fatalf("collector after second batch = (%d, %#v), want cursor 5 and %#v", aer.LastID, aer.Counts, want)
	}
}

func TestAERDeviceResolverUsesFreshSysfsIDsAndNeverConfig(t *testing.T) {
	boot := time.Date(2026, 10, 25, 0, 0, 0, 0, time.UTC)
	bdf := "0000:03:00.0"
	sysfs := t.TempDir()
	addFakePCIDevice(t, sysfs, bdf, "0x1234", "0x5678")
	// A config file with a different identity must not affect name resolution.
	if err := os.WriteFile(filepath.Join(sysfs, bdf, "config"), []byte("0xabcd 0xef01"), 0o644); err != nil {
		t.Fatal(err)
	}
	pciIDs := filepath.Join(t.TempDir(), "pci.ids")
	if err := os.WriteFile(pciIDs, []byte("1234 First Vendor\n\t5678 First Device\n\tabcd\tAlternate Device\nabcd Other Vendor\n\tef01 Config Device\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	resolver := collector.AERDeviceResolver{BootTime: boot, SysfsRoot: sysfs, PCIIDsPath: pciIDs}
	stamp := pciEventTime(boot.Add(time.Minute))
	if got := resolver.Resolve(stamp, bdf); got != "First Vendor First Device" {
		t.Fatalf("Resolve()=%q, want sysfs-selected name", got)
	}
	if got := resolver.Resolve(stamp, "03:00.0"); got != "unknown" {
		t.Fatalf("Resolve() for BDF without domain=%q, want unknown", got)
	}

	// No BDF cache: a hotplug/re-enumeration is reflected by the next lookup.
	addFakePCIDevice(t, sysfs, bdf, "0xabcd", "0xef01")
	if got := resolver.Resolve(stamp, bdf); got != "Other Vendor Config Device" {
		t.Fatalf("Resolve() after sysfs identity changed=%q, want updated name", got)
	}

	if err := os.WriteFile(pciIDs, []byte("abcd Updated Vendor\n\tef01 Updated PCI Device\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := resolver.Resolve(stamp, bdf); got != "Updated Vendor Updated PCI Device" {
		t.Fatalf("Resolve() after pci.ids changed=%q, want updated local name", got)
	}
}

func TestAERDeviceResolverSkipsHistoricalOrUncertainEvents(t *testing.T) {
	boot := time.Date(2026, 10, 25, 0, 0, 0, 0, time.UTC)
	bdf := "0000:03:00.0"
	sysfs := t.TempDir()
	addFakePCIDevice(t, sysfs, bdf, "0x1234", "0x5678")
	pciIDs := filepath.Join(t.TempDir(), "pci.ids")
	if err := os.WriteFile(pciIDs, []byte("1234 Example Vendor\n\t5678 Example PCI Device\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	resolver := collector.AERDeviceResolver{BootTime: boot, SysfsRoot: sysfs, PCIIDsPath: pciIDs}
	if got := resolver.Resolve(pciEventTime(boot.Add(time.Minute)), bdf); got != "Example Vendor Example PCI Device" {
		t.Fatalf("current-boot event Resolve()=%q, want valid name fixture", got)
	}
	cases := []struct {
		name, timestamp string
		boot            time.Time
	}{
		{name: "before host boot", timestamp: pciEventTime(boot.Add(-time.Hour)), boot: boot},
		{name: "same second as host boot", timestamp: pciEventTime(boot), boot: boot},
		{name: "invalid timestamp", timestamp: "not-a-rasdaemon-timestamp", boot: boot},
		{name: "unknown host boot", timestamp: pciEventTime(boot.Add(time.Minute))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resolver.BootTime = tc.boot
			if got := resolver.Resolve(tc.timestamp, bdf); got != "unknown" {
				t.Fatalf("Resolve()=%q, want unknown", got)
			}
		})
	}
}

func TestAERDeviceResolverReturnsUnknownWhenDeviceOrNameIsUnavailable(t *testing.T) {
	boot := time.Date(2026, 10, 25, 0, 0, 0, 0, time.UTC)
	stamp := pciEventTime(boot.Add(time.Minute))
	sysfs := t.TempDir()
	pciIDs := filepath.Join(t.TempDir(), "pci.ids")
	resolver := collector.AERDeviceResolver{BootTime: boot, SysfsRoot: sysfs, PCIIDsPath: pciIDs}
	bdf := "0000:03:00.0"
	addFakePCIDevice(t, sysfs, bdf, "0x1234", "0x5678")
	for _, tc := range []struct {
		name string
		ids  string
	}{
		{name: "unknown vendor/device IDs", ids: "9999 Different Vendor\n\t9999 Different Device\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(pciIDs, []byte(tc.ids), 0o644); err != nil {
				t.Fatal(err)
			}
			if got := resolver.Resolve(stamp, bdf); got != "unknown" {
				t.Fatalf("Resolve()=%q, want unknown", got)
			}
		})
	}
	if err := os.Remove(pciIDs); err != nil {
		t.Fatal(err)
	}
	if got := resolver.Resolve(stamp, bdf); got != "unknown" {
		t.Fatalf("Resolve() with missing pci.ids=%q, want unknown", got)
	}
	if got := resolver.Resolve(stamp, "0000:04:00.0"); got != "unknown" {
		t.Fatalf("removed device Resolve()=%q, want unknown", got)
	}
}

func TestAERCollectorKeepsCursorAndCountsOnFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "aer-events.db")
	boot := time.Date(2026, 10, 25, 0, 0, 0, 0, time.UTC)
	resolver := &collector.AERDeviceResolver{BootTime: boot, SysfsRoot: filepath.Join(t.TempDir(), "missing-sysfs")}
	createAERDatabase(t, path,
		aerTestRow{id: 1, timestamp: pciEventTime(boot.Add(time.Minute)), devName: "0000:03:00.0", errType: "Corrected", errMsg: "first"},
		aerTestRow{id: 2, timestamp: pciEventTime(boot.Add(time.Minute)), devName: "0000:03:00.0", errType: "Corrected", errMsg: "second"},
	)
	db := openAERDatabase(t, path)
	aer := collector.AERCollector{DeviceResolver: resolver, Counts: map[collector.AERLabels]uint64{{
		DevName: "0000:03:00.0", ErrType: "Corrected", ErrMsg: "first", DeviceName: "unknown",
	}: math.MaxUint64}}
	if rows, err := aer.CollectBatch(context.Background(), db, 1); err == nil || rows != 0 {
		t.Fatalf("overflowing batch = (%d, %v), want error", rows, err)
	}
	if aer.LastID != 0 || aer.Counts[collector.AERLabels{
		DevName: "0000:03:00.0", ErrType: "Corrected", ErrMsg: "first", DeviceName: "unknown",
	}] != math.MaxUint64 || len(aer.Counts) != 1 {
		t.Fatalf("failed batch changed collector: cursor=%d counts=%v", aer.LastID, aer.Counts)
	}

	if rows, err := aer.CollectBatch(context.Background(), db, 0); err == nil || rows != 0 {
		t.Fatalf("zero-limit collection = (%d, %v), want error", rows, err)
	}
	if aer.LastID != 0 {
		t.Fatalf("invalid collection advanced cursor to %d", aer.LastID)
	}
}

func TestAERRestoreRoundTripPreservesEnrichedNames(t *testing.T) {
	labels := collector.AERLabels{
		DevName: "0000:03:00.0", ErrType: "Future Severity", ErrMsg: "Unsupported Request",
		DeviceName: "Example Vendor Example PCI Device",
	}
	original := collector.AERCollector{LastID: 7, Counts: map[collector.AERLabels]uint64{labels: 12}}
	families := original.ExportMetrics()
	var restored collector.AERCollector
	if err := restored.RestoreMetrics(families); err != nil {
		t.Fatal(err)
	}
	if restored.LastID != 0 || !reflect.DeepEqual(restored.Counts, original.Counts) {
		t.Fatalf("restored collector = (%d, %#v), want cursor unchanged and labels/counts preserved", restored.LastID, restored.Counts)
	}
	if err := restored.RestoreMetrics(nil); err != nil || len(restored.Counts) != 0 {
		t.Fatalf("restoring absent family = (%v, %v), want empty counts", restored.Counts, err)
	}
	if err := restored.RestoreMetrics([]collector.CounterFamily{{Name: "unexpected"}}); err == nil {
		t.Fatal("RestoreMetrics accepted an unknown family")
	}
}

func TestEngineEnrollsAERAndLeavesPreBootEventsUnresolved(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "rasdaemon.db")
	createSQLite(t, databasePath, devlinkSchema, aerTestSchema)
	insertDevlinkEvents(t, databasePath, recoveryEvent(1, "previously consumed"))
	insertAERRows(t, databasePath, aerTestRow{
		id: 1, timestamp: "2000-01-01 00:00:00 +0000", devName: "0000:03:00.0",
		errType: "Corrected", errMsg: "Receiver Error",
	})
	config := validatedConfig(t, databasePath)
	fixture := engineRecoveryFixture{databasePath: databasePath, snapshotPath: config.SnapshotPath, config: config}
	identity := sourceIdentityForTest(t, databasePath)
	saveRecoveryState(t, fixture, 2, 1, identity, map[string]int64{"devlink_event": 1})
	previous := collector.DevlinkCollector{Counts: map[collector.DevlinkLabels]uint64{
		recoveryLabels("previously consumed"): 1,
	}}
	if err := collector.SaveSnapshot(fixture.snapshotPath, &collector.Snapshot{
		Generation: 2, Families: previous.ExportMetrics(),
	}); err != nil {
		t.Fatal(err)
	}

	engine := restoreRecoveryEngine(t, fixture)
	if err := engine.CollectAndCommit(context.Background()); err != nil {
		t.Fatalf("collect newly registered AER table: %v", err)
	}
	state, err := collector.LoadState(config.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	if state.Generation != 3 || state.Tables["devlink_event"] != 1 || state.Tables["aer_event"] != 1 {
		t.Fatalf("state after AER enrollment = %#v, want generation 3 and both cursors at 1", state)
	}
	snapshot, err := collector.LoadSnapshot(fixture.snapshotPath)
	if err != nil {
		t.Fatal(err)
	}
	var emptyAER collector.AERCollector
	var emptyDevlink collector.DevlinkCollector
	aerName := emptyAER.ExportMetrics()[0].Name
	devlinkName := emptyDevlink.ExportMetrics()[0].Name
	var aerFamilies, devlinkFamilies []collector.CounterFamily
	for _, family := range snapshot.Families {
		switch family.Name {
		case aerName:
			aerFamilies = append(aerFamilies, family)
		case devlinkName:
			devlinkFamilies = append(devlinkFamilies, family)
		}
	}
	var aer collector.AERCollector
	if err := aer.RestoreMetrics(aerFamilies); err != nil {
		t.Fatal(err)
	}
	want := map[collector.AERLabels]uint64{{
		DevName: "0000:03:00.0", ErrType: "Corrected", ErrMsg: "Receiver Error", DeviceName: "unknown",
	}: 1}
	if !reflect.DeepEqual(aer.Counts, want) {
		t.Fatalf("pre-boot AER event counts = %#v, want %#v", aer.Counts, want)
	}
	var devlink collector.DevlinkCollector
	if err := devlink.RestoreMetrics(devlinkFamilies); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(devlink.Counts, previous.Counts) {
		t.Fatalf("legacy devlink count changed during AER enrollment: %#v", devlink.Counts)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	engine = restoreRecoveryEngine(t, fixture)
	if err := engine.CollectAndCommit(context.Background()); err != nil {
		t.Fatalf("collect after restart: %v", err)
	}
	state, err = collector.LoadState(config.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	if state.Generation != 3 || state.Tables["aer_event"] != 1 {
		t.Fatalf("restart changed committed AER progress: %#v", state)
	}
	snapshot, err = collector.LoadSnapshot(fixture.snapshotPath)
	if err != nil {
		t.Fatal(err)
	}
	aerFamilies = aerFamilies[:0]
	for _, family := range snapshot.Families {
		if family.Name == aerName {
			aerFamilies = append(aerFamilies, family)
		}
	}
	var afterRestart collector.AERCollector
	if err := afterRestart.RestoreMetrics(aerFamilies); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(afterRestart.Counts, want) {
		t.Fatalf("AER count changed after restart: %#v, want %#v", afterRestart.Counts, want)
	}
}

func TestAERInvalidUTF8IsReplacedBeforeGrouping(t *testing.T) {
	path := filepath.Join(t.TempDir(), "aer-events.db")
	boot := time.Date(2026, 10, 25, 0, 0, 0, 0, time.UTC)
	bdf := "0000:03:00.0"
	messageOne := append([]byte("Receiver Error "), 0xff, 0xfe)
	messageOne = append(messageOne, []byte(" address 0x1234 TLP Header: AA BB")...)
	messageTwo := append([]byte("Receiver Error "), 0xfd)
	messageTwo = append(messageTwo, []byte(" address 0x9876 TLP Header: CC DD")...)
	createAERDatabase(t, path,
		aerTestRow{id: 1, timestamp: pciEventTime(boot.Add(time.Minute)), devName: bdf, errType: "Corrected", errMsg: string(messageOne)},
		aerTestRow{id: 2, timestamp: pciEventTime(boot.Add(time.Minute)), devName: bdf, errType: "Corrected", errMsg: string(messageTwo)},
	)
	db := openAERDatabase(t, path)
	aer := collector.AERCollector{DeviceResolver: &collector.AERDeviceResolver{
		BootTime: boot, SysfsRoot: filepath.Join(t.TempDir(), "missing-sysfs"),
	}}
	rows, err := aer.CollectBatch(context.Background(), db, 10)
	if err != nil || rows != 2 {
		t.Fatalf("CollectBatch() = (%d, %v), want (2, nil)", rows, err)
	}
	want := collector.AERLabels{DevName: bdf, ErrType: "Corrected", ErrMsg: "Receiver Error <invalid> address <omitted>", DeviceName: "unknown"}
	if got := aer.Counts[want]; got != 2 || len(aer.Counts) != 1 {
		t.Fatalf("sanitized message groups=%v, want one normalized series with count 2", aer.Counts)
	}
}
