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

const diskErrorsSchema = `CREATE TABLE disk_errors (
	id INTEGER PRIMARY KEY,
	timestamp TEXT,
	dev TEXT,
	sector INTEGER,
	nr_sector INTEGER,
	error TEXT,
	rwbs TEXT,
	cmd TEXT
)`

type diskErrorRow struct {
	id        int64
	timestamp any
	dev       any
	sector    any
	nrSector  any
	err       any
	rwbs      any
	cmd       any
}

func diskEventTime(value time.Time) string {
	return value.Format("2006-01-02 15:04:05 -0700")
}

func createDiskDatabase(t *testing.T, path string, rows ...diskErrorRow) {
	t.Helper()
	createSQLite(t, path, diskErrorsSchema)
	insertDiskRows(t, path, rows...)
}

func insertDiskRows(t *testing.T, path string, rows ...diskErrorRow) {
	t.Helper()
	if len(rows) == 0 {
		return
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open synthetic disk_errors database: %v", err)
	}
	db.SetMaxOpenConns(1)
	defer db.Close()
	for _, row := range rows {
		if _, err := db.ExecContext(context.Background(), `INSERT INTO disk_errors
			(id, timestamp, dev, sector, nr_sector, error, rwbs, cmd)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			row.id, row.timestamp, row.dev, row.sector, row.nrSector, row.err, row.rwbs, row.cmd); err != nil {
			t.Fatalf("insert synthetic disk error %d: %v", row.id, err)
		}
	}
}

func openDiskDatabase(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open synthetic disk_errors database: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close synthetic disk_errors database: %v", err)
		}
	})
	return db
}

type fakeDiskSysfs struct {
	root       string
	whole      string
	partition  string
	deviceInfo string
}

func newFakeDiskSysfs(t *testing.T) fakeDiskSysfs {
	t.Helper()
	root := filepath.Join(t.TempDir(), "dev", "block")
	whole := filepath.Join(t.TempDir(), "devices", "scsi", "sda")
	partition := filepath.Join(whole, "sda1")
	deviceInfo := filepath.Join(t.TempDir(), "devices", "target0")
	for _, dir := range []string{root, whole, partition, deviceInfo} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("create fake block sysfs directory: %v", err)
		}
	}
	if err := os.Symlink(deviceInfo, filepath.Join(whole, "device")); err != nil {
		t.Fatalf("link fake whole-disk device: %v", err)
	}
	if err := os.WriteFile(filepath.Join(deviceInfo, "model"), []byte("Example Disk Model\n"), 0o644); err != nil {
		t.Fatalf("write fake disk model: %v", err)
	}
	if err := os.WriteFile(filepath.Join(partition, "partition"), []byte("1\n"), 0o644); err != nil {
		t.Fatalf("mark fake partition: %v", err)
	}
	for dev, target := range map[string]string{"8:0": whole, "8:1": partition} {
		if err := os.Symlink(target, filepath.Join(root, dev)); err != nil {
			t.Fatalf("link fake block device %s: %v", dev, err)
		}
	}
	return fakeDiskSysfs{root: root, whole: whole, partition: partition, deviceInfo: deviceInfo}
}

func newDiskResolver(boot time.Time, sysfs fakeDiskSysfs) *collector.DiskDeviceResolver {
	return &collector.DiskDeviceResolver{BootTime: boot, SysfsRoot: sysfs.root}
}

func TestDiskErrorCollectorAccumulatesRowsAndResolvesSysfsLabels(t *testing.T) {
	boot := time.Date(2026, 10, 26, 8, 0, 0, 0, time.UTC)
	sysfs := newFakeDiskSysfs(t)
	path := filepath.Join(t.TempDir(), "rasdaemon.db")
	createDiskDatabase(t, path,
		diskErrorRow{id: 1, timestamp: diskEventTime(boot.Add(time.Minute)), dev: "8:0", sector: 12, nrSector: 8, err: "I/O error", rwbs: "R", cmd: "READ"},
		diskErrorRow{id: 2, timestamp: diskEventTime(boot.Add(2 * time.Minute)), dev: "8:0", sector: 999, nrSector: 1, err: "I/O error", rwbs: "R", cmd: "READ"},
		diskErrorRow{id: 3, timestamp: diskEventTime(boot.Add(3 * time.Minute)), dev: "8:1", sector: 4, nrSector: 1, err: "timeout error", rwbs: "W", cmd: "WRITE"},
		diskErrorRow{id: 4, timestamp: diskEventTime(boot.Add(4 * time.Minute)), dev: "not-a-device", sector: 1, nrSector: 1, err: "future error text", rwbs: "D", cmd: "DISCARD"},
	)
	collectorUnderTest := collector.DiskErrorCollector{DeviceResolver: newDiskResolver(boot, sysfs)}
	db := openDiskDatabase(t, path)

	rows, err := collectorUnderTest.CollectBatch(context.Background(), db, 2)
	if err != nil || rows != 2 || collectorUnderTest.LastID != 2 {
		t.Fatalf("first CollectBatch() = (%d, %v), LastID=%d; want (2,nil),2", rows, err, collectorUnderTest.LastID)
	}
	want := map[collector.DiskErrorLabels]uint64{{
		Dev: "8:0", Error: "I/O error", RWBS: "R", DeviceName: "sda", DeviceModel: "Example Disk Model",
	}: 2}
	if !reflect.DeepEqual(collectorUnderTest.Counts, want) {
		t.Fatalf("first page counts = %#v, want %#v", collectorUnderTest.Counts, want)
	}

	rows, err = collectorUnderTest.CollectBatch(context.Background(), db, 2)
	if err != nil || rows != 2 || collectorUnderTest.LastID != 4 {
		t.Fatalf("second CollectBatch() = (%d, %v), LastID=%d; want (2,nil),4", rows, err, collectorUnderTest.LastID)
	}
	want[collector.DiskErrorLabels{
		Dev: "8:1", Error: "timeout error", RWBS: "W", DeviceName: "sda1", DeviceModel: "Example Disk Model",
	}] = 1
	want[collector.DiskErrorLabels{
		Dev: "not-a-device", Error: "future error text", RWBS: "D",
		DeviceName: "unknown", DeviceModel: "unknown",
	}] = 1
	if !reflect.DeepEqual(collectorUnderTest.Counts, want) {
		t.Fatalf("second page counts = %#v, want %#v", collectorUnderTest.Counts, want)
	}
}

func TestDiskDeviceResolverDoesNotCacheAndDoesNotFollowLogicalSlaves(t *testing.T) {
	boot := time.Date(2026, 10, 26, 8, 0, 0, 0, time.UTC)
	stamp := diskEventTime(boot.Add(time.Minute))
	sysfs := newFakeDiskSysfs(t)
	resolver := newDiskResolver(boot, sysfs)
	name, model := resolver.Resolve(stamp, "8:0")
	if name != "sda" || model != "Example Disk Model" {
		t.Fatalf("whole disk Resolve() = (%q,%q), want (sda, Example Disk Model)", name, model)
	}
	if err := os.WriteFile(filepath.Join(sysfs.deviceInfo, "model"), []byte("Replaced Model\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	name, model = resolver.Resolve(stamp, "8:0")
	if name != "sda" || model != "Replaced Model" {
		t.Fatalf("Resolve() after model change = (%q,%q), want fresh sysfs values", name, model)
	}

	otherDisk := filepath.Join(t.TempDir(), "devices", "nvme0n1")
	otherInfo := filepath.Join(t.TempDir(), "devices", "nvme0")
	if err := os.MkdirAll(otherDisk, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(otherInfo, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(otherInfo, filepath.Join(otherDisk, "device")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(otherInfo, "model"), []byte("Re-enumerated Model\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(sysfs.root, "8:0")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(otherDisk, filepath.Join(sysfs.root, "8:0")); err != nil {
		t.Fatal(err)
	}
	name, model = resolver.Resolve(stamp, "8:0")
	if name != "nvme0n1" || model != "Re-enumerated Model" {
		t.Fatalf("Resolve() after device re-enumeration = (%q,%q), want fresh block mapping", name, model)
	}

	logical := filepath.Join(t.TempDir(), "dm-0")
	if err := os.MkdirAll(filepath.Join(logical, "slaves"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(sysfs.whole, filepath.Join(logical, "slaves", "sda")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(logical, filepath.Join(sysfs.root, "253:0")); err != nil {
		t.Fatal(err)
	}
	name, model = resolver.Resolve(stamp, "253:0")
	if name != "dm-0" || model != "unknown" {
		t.Fatalf("logical device Resolve() = (%q,%q), want (dm-0, unknown) without slave attribution", name, model)
	}
}

func TestDiskDeviceResolverSkipsOldOrUncertainEvents(t *testing.T) {
	boot := time.Date(2026, 10, 26, 8, 0, 0, 0, time.UTC)
	sysfs := newFakeDiskSysfs(t)
	cases := []struct {
		name  string
		boot  time.Time
		stamp string
	}{
		{name: "before boot", boot: boot, stamp: diskEventTime(boot.Add(-time.Minute))},
		{name: "same second", boot: boot, stamp: diskEventTime(boot)},
		{name: "invalid timestamp", boot: boot, stamp: "not-a-timestamp"},
		{name: "unknown boot", stamp: diskEventTime(boot.Add(time.Minute))},
		{name: "malformed dev", boot: boot, stamp: diskEventTime(boot.Add(time.Minute))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resolver := newDiskResolver(tc.boot, sysfs)
			dev := "8:0"
			if tc.name == "malformed dev" {
				dev = "8/0"
			}
			name, model := resolver.Resolve(tc.stamp, dev)
			if name != "unknown" || model != "unknown" {
				t.Fatalf("Resolve() = (%q,%q), want unknown pair", name, model)
			}
		})
	}
}

func TestDiskErrorCollectorKeepsRawTextAndUnknownsAndRestoresSnapshot(t *testing.T) {
	boot := time.Date(2026, 10, 26, 8, 0, 0, 0, time.UTC)
	sysfs := newFakeDiskSysfs(t)
	path := filepath.Join(t.TempDir(), "rasdaemon.db")
	createDiskDatabase(t, path,
		diskErrorRow{id: 1, timestamp: diskEventTime(boot.Add(time.Minute)), dev: "8:0", err: "timeout error", rwbs: "R"},
		diskErrorRow{id: 2, timestamp: diskEventTime(boot.Add(time.Minute)), dev: nil, err: nil, rwbs: nil},
		diskErrorRow{id: 3, timestamp: diskEventTime(boot.Add(time.Minute)), dev: "8:0", err: "bad\xfftext", rwbs: "R"},
	)
	original := collector.DiskErrorCollector{DeviceResolver: newDiskResolver(boot, sysfs)}
	if rows, err := original.CollectBatch(context.Background(), openDiskDatabase(t, path), 10); err != nil || rows != 3 {
		t.Fatalf("CollectBatch() = (%d,%v), want (3,nil)", rows, err)
	}
	want := map[collector.DiskErrorLabels]uint64{
		{Dev: "8:0", Error: "timeout error", RWBS: "R", DeviceName: "sda", DeviceModel: "Example Disk Model"}:    1,
		{Dev: "", Error: "", RWBS: "", DeviceName: "unknown", DeviceModel: "unknown"}:                            1,
		{Dev: "8:0", Error: "bad<invalid>text", RWBS: "R", DeviceName: "sda", DeviceModel: "Example Disk Model"}: 1,
	}
	if !reflect.DeepEqual(original.Counts, want) {
		t.Fatalf("counts = %#v, want %#v", original.Counts, want)
	}
	var restored collector.DiskErrorCollector
	if err := restored.RestoreMetrics(original.ExportMetrics()); err != nil {
		t.Fatalf("RestoreMetrics() error = %v", err)
	}
	if !reflect.DeepEqual(restored.Counts, want) {
		t.Fatalf("restored counts = %#v, want %#v", restored.Counts, want)
	}
	if err := restored.RestoreMetrics(nil); err != nil || len(restored.Counts) != 0 {
		t.Fatalf("RestoreMetrics(nil) = (%v,%v), want empty counts", err, restored.Counts)
	}
}

func TestDiskErrorCollectorFailureDoesNotAdvanceCursorOrPartiallyMerge(t *testing.T) {
	boot := time.Date(2026, 10, 26, 8, 0, 0, 0, time.UTC)
	sysfs := newFakeDiskSysfs(t)
	path := filepath.Join(t.TempDir(), "rasdaemon.db")
	createDiskDatabase(t, path,
		diskErrorRow{id: 1, timestamp: diskEventTime(boot.Add(time.Minute)), dev: "8:0", err: "I/O", rwbs: "R"},
		diskErrorRow{id: 2, timestamp: diskEventTime(boot.Add(time.Minute)), dev: "8:0", err: "I/O", rwbs: "R"},
	)
	labels := collector.DiskErrorLabels{Dev: "8:0", Error: "I/O", RWBS: "R", DeviceName: "sda", DeviceModel: "Example Disk Model"}
	c := collector.DiskErrorCollector{
		DeviceResolver: newDiskResolver(boot, sysfs),
		Counts:         map[collector.DiskErrorLabels]uint64{labels: math.MaxUint64},
	}
	if rows, err := c.CollectBatch(context.Background(), openDiskDatabase(t, path), 10); err == nil || rows != 0 {
		t.Fatalf("overflow CollectBatch() = (%d,%v), want error and zero rows", rows, err)
	}
	if c.LastID != 0 || c.Counts[labels] != math.MaxUint64 || len(c.Counts) != 1 {
		t.Fatalf("collector mutated on failed batch: LastID=%d Counts=%#v", c.LastID, c.Counts)
	}
}

func TestDiskErrorCollectorRejectsSourceCursorRollback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rasdaemon.db")
	createDiskDatabase(t, path, diskErrorRow{
		id: 2, timestamp: "2026-10-26 08:01:00 +0000", dev: "8:0", err: "I/O error", rwbs: "R",
	})
	c := collector.DiskErrorCollector{LastID: 3}
	if rows, err := c.CollectBatch(context.Background(), openDiskDatabase(t, path), 10); err == nil || rows != 0 {
		t.Fatalf("CollectBatch() after source rollback = (%d,%v), want error and zero rows", rows, err)
	}
	if c.LastID != 3 || len(c.Counts) != 0 {
		t.Fatalf("collector changed after source rollback: LastID=%d Counts=%#v", c.LastID, c.Counts)
	}
}

func TestDiskErrorCollectorCancellationLeavesProgressUnchanged(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rasdaemon.db")
	createDiskDatabase(t, path, diskErrorRow{
		id: 1, timestamp: "2026-10-26 08:01:00 +0000", dev: "8:0", err: "I/O error", rwbs: "R",
	})
	c := collector.DiskErrorCollector{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if rows, err := c.CollectBatch(ctx, openDiskDatabase(t, path), 1); err == nil || rows != 0 {
		t.Fatalf("canceled CollectBatch() = (%d,%v), want error and zero rows", rows, err)
	}
	if c.LastID != 0 || len(c.Counts) != 0 {
		t.Fatalf("collector changed after cancellation: LastID=%d Counts=%#v", c.LastID, c.Counts)
	}
}

func TestDiskErrorCollectorDoesNotInitializeResolverDuringCollection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rasdaemon.db")
	createDiskDatabase(t, path, diskErrorRow{
		id: 1, timestamp: "2000-01-01 00:00:00 +0000", dev: "8:0", err: "I/O error", rwbs: "R",
	})
	c := collector.DiskErrorCollector{}
	if rows, err := c.CollectBatch(context.Background(), openDiskDatabase(t, path), 1); err != nil || rows != 1 {
		t.Fatalf("CollectBatch() = (%d, %v), want (1, nil)", rows, err)
	}
	if c.DeviceResolver != nil {
		t.Fatal("CollectBatch initialized DeviceResolver; registry construction must do that")
	}
	want := map[collector.DiskErrorLabels]uint64{{
		Dev: "8:0", Error: "I/O error", RWBS: "R", DeviceName: "unknown", DeviceModel: "unknown",
	}: 1}
	if !reflect.DeepEqual(c.Counts, want) {
		t.Fatalf("counts = %#v, want %#v", c.Counts, want)
	}
}

func TestDiskErrorCollectorSeriesLimitFailureIsAtomic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rasdaemon.db")
	createDiskDatabase(t, path, diskErrorRow{
		id: 1, timestamp: "2000-01-01 00:00:00 +0000", dev: "8:0", err: "new error", rwbs: "R",
	})
	c := collector.DiskErrorCollector{
		DeviceResolver: &collector.DiskDeviceResolver{},
		Counts:         make(map[collector.DiskErrorLabels]uint64, 10000),
	}
	for i := 0; i < 10000; i++ {
		labels := collector.DiskErrorLabels{
			Dev: "old-" + strconv.Itoa(i), Error: "existing", RWBS: "R",
			DeviceName: "unknown", DeviceModel: "unknown",
		}
		c.Counts[labels] = 1
	}
	before := len(c.Counts)
	if rows, err := c.CollectBatch(context.Background(), openDiskDatabase(t, path), 1); err == nil || rows != 0 {
		t.Fatalf("series-limit CollectBatch() = (%d,%v), want error and zero rows", rows, err)
	}
	if c.LastID != 0 || len(c.Counts) != before {
		t.Fatalf("collector changed after series limit: LastID=%d series=%d, want cursor zero and %d series", c.LastID, len(c.Counts), before)
	}
}

func TestEngineEnrollsDiskErrorsAndDoesNotReplayAfterRestart(t *testing.T) {
	boot := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	databasePath := filepath.Join(t.TempDir(), "rasdaemon.db")
	createSQLite(t, databasePath, devlinkSchema, diskErrorsSchema)
	insertDevlinkEvents(t, databasePath, recoveryEvent(1, "already consumed"))
	insertDiskRows(t, databasePath, diskErrorRow{
		id: 1, timestamp: diskEventTime(boot), dev: "8:0", err: "I/O error", rwbs: "R",
	})
	config := validatedConfig(t, databasePath)
	fixture := engineRecoveryFixture{databasePath: databasePath, snapshotPath: config.SnapshotPath, config: config}
	identity := sourceIdentityForTest(t, databasePath)
	saveRecoveryState(t, fixture, 2, 1, identity, map[string]int64{"devlink_event": 1})
	previous := collector.DevlinkCollector{Counts: map[collector.DevlinkLabels]uint64{
		recoveryLabels("already consumed"): 1,
	}}
	if err := collector.SaveSnapshot(fixture.snapshotPath, &collector.Snapshot{
		Generation: 2, Families: previous.ExportMetrics(),
	}); err != nil {
		t.Fatal(err)
	}
	engine := restoreRecoveryEngine(t, fixture)
	if err := engine.CollectAndCommit(context.Background()); err != nil {
		t.Fatalf("CollectAndCommit() enrolling disk_errors: %v", err)
	}
	state, err := collector.LoadState(config.StateDir)
	if err != nil || state.Generation != 3 || state.Tables["devlink_event"] != 1 || state.Tables["disk_errors"] != 1 {
		t.Fatalf("state after enrollment = %#v (error %v)", state, err)
	}
	snapshot, err := collector.LoadSnapshot(fixture.snapshotPath)
	if err != nil {
		t.Fatal(err)
	}
	var disk collector.DiskErrorCollector
	var diskFamilies []collector.CounterFamily
	for _, family := range snapshot.Families {
		if family.Name == "rasdaemon_block_io_error_events_total" {
			diskFamilies = append(diskFamilies, family)
		}
	}
	if err := disk.RestoreMetrics(diskFamilies); err != nil {
		t.Fatalf("RestoreMetrics() for enrolled table: %v", err)
	}
	want := map[collector.DiskErrorLabels]uint64{{
		Dev: "8:0", Error: "I/O error", RWBS: "R", DeviceName: "unknown", DeviceModel: "unknown",
	}: 1}
	if !reflect.DeepEqual(disk.Counts, want) {
		t.Fatalf("enrolled disk counts = %#v, want %#v", disk.Counts, want)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	engine = restoreRecoveryEngine(t, fixture)
	if err := engine.CollectAndCommit(context.Background()); err != nil {
		t.Fatalf("CollectAndCommit() after restart: %v", err)
	}
	stateAfterRestart, err := collector.LoadState(config.StateDir)
	if err != nil || stateAfterRestart.Generation != 3 || stateAfterRestart.Tables["disk_errors"] != 1 {
		t.Fatalf("state after restart = %#v (error %v), want unchanged generation and cursor", stateAfterRestart, err)
	}
}
