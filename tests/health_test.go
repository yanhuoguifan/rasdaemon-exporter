package collector_test

import (
	"bytes"
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"rasdaemon-exporter/internal/collector"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

func TestHealthPathUsesSnapshotAsBaseName(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{collector.DefaultSnapshotPath, "/var/lib/node_exporter/textfile_collector/rasdaemon.health.prom"},
		{"/tmp/host-ras.prom", "/tmp/host-ras.health.prom"},
		{"/tmp/metrics", "/tmp/metrics.health.prom"},
		{"/tmp/metrics.txt", "/tmp/metrics.txt.health.prom"},
	}
	for _, test := range cases {
		cfg := collector.DefaultConfig()
		cfg.SnapshotPath = test.input
		if got := cfg.HealthPath(); got != test.want {
			t.Errorf("HealthPath(%q) = %q, want %q", test.input, got, test.want)
		}
	}
}

func TestConfigValidationKeepsHealthFileOutsideManagedInputs(t *testing.T) {
	root := t.TempDir()
	stateDir := filepath.Join(root, "state")
	cases := []struct {
		name         string
		databasePath string
		stateDir     string
		snapshotPath string
	}{
		{
			name:         "database",
			databasePath: filepath.Join(root, "health.health.prom"),
			stateDir:     stateDir,
			snapshotPath: filepath.Join(root, "health.prom"),
		},
		{
			name:         "state directory",
			databasePath: filepath.Join(root, "events.db"),
			stateDir:     filepath.Join(root, "health.health.prom"),
			snapshotPath: filepath.Join(root, "health.prom"),
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			cfg := collector.Config{
				DatabasePath: test.databasePath,
				StateDir:     test.stateDir,
				SnapshotPath: test.snapshotPath,
				Interval:     collector.DefaultInterval,
			}
			if err := cfg.Validate(); err == nil {
				t.Fatalf("Validate() accepted health path overlapping %s", test.name)
			}
		})
	}
}

func TestSaveHealthWritesExactlyTwoUnlabeledGauges(t *testing.T) {
	path := filepath.Join(t.TempDir(), "health.prom")
	lastSuccess := time.Date(2026, 10, 30, 12, 34, 56, 789000000, time.UTC)
	if err := collector.SaveHealth(path, collector.Health{LastSuccess: lastSuccess, SeriesCount: 17}); err != nil {
		t.Fatalf("SaveHealth() error = %v", err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read health file: %v", err)
	}
	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(bytes.NewReader(contents))
	if err != nil {
		t.Fatalf("parse health exposition: %v", err)
	}
	if len(families) != 2 {
		t.Fatalf("health exposition has %d families, want exactly two", len(families))
	}
	want := map[string]float64{
		"rasdaemon_collection_last_success_timestamp_seconds": float64(lastSuccess.Unix()),
		"rasdaemon_series_count":                              17,
	}
	for name, value := range want {
		family := families[name]
		if family == nil || family.GetType() != dto.MetricType_GAUGE {
			t.Fatalf("health family %q missing or not gauge: %#v", name, family)
		}
		if len(family.GetMetric()) != 1 {
			t.Fatalf("health family %q has %d samples, want one", name, len(family.GetMetric()))
		}
		metric := family.GetMetric()[0]
		if len(metric.GetLabel()) != 0 {
			t.Errorf("health family %q has labels %v, want none", name, metric.GetLabel())
		}
		if math.Abs(metric.GetGauge().GetValue()-value) > 1e-6 {
			t.Errorf("health family %q value = %.9f, want %.9f", name, metric.GetGauge().GetValue(), value)
		}
	}
}

func TestEngineIdleRoundsRefreshHealthOnly(t *testing.T) {
	fixture := newEngineRecoveryFixture(t)
	engine := restoreRecoveryEngine(t, fixture)
	healthPath := fixture.config.HealthPath()
	if _, err := os.Stat(healthPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Restore created a health file before a successful round: %v", err)
	}
	snapshotBefore, snapshotInfo := readCommittedFile(t, fixture.snapshotPath)
	statePath := filepath.Join(fixture.config.StateDir, "state.json")
	stateBefore, stateInfo := readCommittedFile(t, statePath)
	if err := engine.CollectAndCommit(context.Background()); err != nil {
		t.Fatalf("first empty collection round: %v", err)
	}
	first := readHealth(t, healthPath)
	_, firstInfo := readCommittedFile(t, healthPath)
	if first.seriesCount != 0 || first.lastSuccess.IsZero() {
		t.Fatalf("first health sample = (%v, %d), want successful time and zero series", first.lastSuccess, first.seriesCount)
	}
	assertCommittedFileUnchanged(t, fixture.snapshotPath, snapshotBefore, snapshotInfo)
	assertCommittedFileUnchanged(t, statePath, stateBefore, stateInfo)
	if err := engine.Close(); err != nil {
		t.Fatalf("close engine: %v", err)
	}

	engine = restoreRecoveryEngine(t, fixture)
	if err := engine.CollectAndCommit(context.Background()); err != nil {
		t.Fatalf("second empty collection round: %v", err)
	}
	second := readHealth(t, healthPath)
	_, secondInfo := readCommittedFile(t, healthPath)
	if second.lastSuccess.Before(first.lastSuccess) || second.seriesCount != 0 {
		t.Fatalf("idle health sample = (%v, %d), want nondecreasing success time and zero series", second.lastSuccess, second.seriesCount)
	}
	if os.SameFile(firstInfo, secondInfo) {
		t.Fatal("idle success did not replace the health file")
	}
	healthBeforeCancel, healthInfo := readCommittedFile(t, healthPath)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := engine.CollectAndCommit(canceled); err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled round error = %v, want context.Canceled", err)
	}
	assertCommittedFileUnchanged(t, healthPath, healthBeforeCancel, healthInfo)
	assertCommittedFileUnchanged(t, fixture.snapshotPath, snapshotBefore, snapshotInfo)
	assertCommittedFileUnchanged(t, statePath, stateBefore, stateInfo)
	assertStateLockReleased(t, fixture.config.StateDir)
	if err := engine.Close(); err != nil {
		t.Fatalf("close engine: %v", err)
	}
}

func TestEngineHealthCountsSeriesFromRestoredEventSnapshot(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "rasdaemon.db")
	createSQLite(t, databasePath, devlinkSchema, mcTestSchema)
	insertDevlinkEvents(t, databasePath, recoveryEvent(1, "devlink event"))
	insertMCTestRows(t, databasePath, mcTestRow{
		id: 1, errCount: 0, errType: "Corrected", errMsg: "memory event", dimm: "DIMM-A",
	})
	config := validatedConfig(t, databasePath)
	engine := restoreRecoveryEngine(t, engineRecoveryFixture{
		databasePath: databasePath, snapshotPath: config.SnapshotPath, config: config,
	})
	if err := engine.CollectAndCommit(context.Background()); err != nil {
		t.Fatalf("collect devlink and MC records: %v", err)
	}
	if got := readHealth(t, config.HealthPath()).seriesCount; got != 3 {
		t.Fatalf("series count = %d, want one devlink plus two MC counter samples", got)
	}
	if err := collector.SaveHealth(config.HealthPath(), collector.Health{
		LastSuccess: time.Unix(100, 0), SeriesCount: 99,
	}); err != nil {
		t.Fatalf("write stale health sample: %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("close committed engine: %v", err)
	}

	// A new engine derives the count from the event snapshot, not health data or
	// by rescanning all table collectors on every idle round.
	engine = restoreRecoveryEngine(t, engineRecoveryFixture{
		databasePath: databasePath, snapshotPath: config.SnapshotPath, config: config,
	})
	if err := engine.CollectAndCommit(context.Background()); err != nil {
		t.Fatalf("idle round after restart: %v", err)
	}
	if got := readHealth(t, config.HealthPath()).seriesCount; got != 3 {
		t.Fatalf("restored series count = %d, want 3", got)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("close restarted engine: %v", err)
	}
}

func TestBackupRecoverySeedsHealthSeriesCountFromMatchingSnapshot(t *testing.T) {
	fixture := newEngineRecoveryFixture(t)
	identity := sourceIdentityForTest(t, fixture.databasePath)
	saveRecoveryState(t, fixture, 2, 0, identity, map[string]int64{"devlink_event": 0})
	previous := collector.DevlinkCollector{Counts: map[collector.DevlinkLabels]uint64{
		recoveryLabels("committed one"): 1,
		recoveryLabels("committed two"): 2,
	}}
	if err := collector.SaveSnapshot(previousSnapshotPath(fixture), &collector.Snapshot{
		Generation: 2, Families: previous.ExportMetrics(),
	}); err != nil {
		t.Fatalf("save matching previous snapshot: %v", err)
	}
	ahead := collector.DevlinkCollector{Counts: map[collector.DevlinkLabels]uint64{
		recoveryLabels("uncommitted ahead value"): 99,
	}}
	if err := collector.SaveSnapshot(fixture.snapshotPath, &collector.Snapshot{
		Generation: 3, Families: ahead.ExportMetrics(),
	}); err != nil {
		t.Fatalf("save ahead current snapshot: %v", err)
	}
	engine := restoreRecoveryEngine(t, fixture)
	if err := engine.CollectAndCommit(context.Background()); err != nil {
		t.Fatalf("publish health after backup recovery: %v", err)
	}
	if health := readHealth(t, fixture.config.HealthPath()); health.seriesCount != 2 {
		t.Fatalf("health count after backup recovery = %d, want 2 from committed snapshot", health.seriesCount)
	}
	assertRecoveryPair(t, fixture, 2, 0, previous.Counts)
	if err := engine.Close(); err != nil {
		t.Fatalf("close recovered engine: %v", err)
	}
}

func TestFailedCollectionDoesNotRefreshHealthTimestamp(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "rasdaemon.db")
	createSQLite(t, databasePath, "CREATE TABLE devlink_event (id INTEGER PRIMARY KEY)")
	config := validatedConfig(t, databasePath)
	if err := os.MkdirAll(config.StateDir, 0o700); err != nil {
		t.Fatalf("create state directory: %v", err)
	}
	previous := collector.Health{LastSuccess: time.Unix(100, 0), SeriesCount: 9}
	if err := collector.SaveHealth(config.HealthPath(), previous); err != nil {
		t.Fatalf("write previous health sample: %v", err)
	}
	engine := restoreRecoveryEngine(t, engineRecoveryFixture{
		databasePath: databasePath, snapshotPath: config.SnapshotPath, config: config,
	})
	before, beforeInfo := readCommittedFile(t, config.HealthPath())
	if err := engine.CollectAndCommit(context.Background()); err == nil {
		t.Fatal("collection with an invalid SQL projection unexpectedly succeeded")
	}
	assertCommittedFileUnchanged(t, config.HealthPath(), before, beforeInfo)
	assertStateLockReleased(t, config.StateDir)
}

func TestEventCommitFailureDoesNotRefreshHealth(t *testing.T) {
	fixture := newEngineRecoveryFixture(t, recoveryEvent(1, "first event"))
	engine := restoreRecoveryEngine(t, fixture)
	if err := engine.CollectAndCommit(context.Background()); err != nil {
		t.Fatalf("initial successful round: %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("close after initial round: %v", err)
	}
	healthPath := fixture.config.HealthPath()
	healthBefore, healthInfo := readCommittedFile(t, healthPath)
	snapshotBefore, snapshotInfo := readCommittedFile(t, fixture.snapshotPath)
	statePath := filepath.Join(fixture.config.StateDir, "state.json")
	stateBefore, stateInfo := readCommittedFile(t, statePath)
	if err := os.Mkdir(fixture.snapshotPath+".prev", 0o700); err != nil {
		t.Fatalf("create backup path to force event commit failure: %v", err)
	}
	insertDevlinkEvents(t, fixture.databasePath, recoveryEvent(2, "uncommitted event"))
	engine = restoreRecoveryEngine(t, fixture)
	if err := engine.CollectAndCommit(context.Background()); err == nil {
		t.Fatal("event commit succeeded despite invalid backup path")
	}
	assertCommittedFileUnchanged(t, healthPath, healthBefore, healthInfo)
	assertCommittedFileUnchanged(t, fixture.snapshotPath, snapshotBefore, snapshotInfo)
	assertCommittedFileUnchanged(t, statePath, stateBefore, stateInfo)
	assertStateLockReleased(t, fixture.config.StateDir)
	if err := os.Remove(fixture.snapshotPath + ".prev"); err != nil {
		t.Fatalf("remove injected backup path: %v", err)
	}
	engine = restoreRecoveryEngine(t, fixture)
	if err := engine.CollectAndCommit(context.Background()); err != nil {
		t.Fatalf("retry uncommitted row after event commit failure: %v", err)
	}
	assertRecoveryPair(t, fixture, 3, 2, map[collector.DevlinkLabels]uint64{
		recoveryLabels("first event"):       1,
		recoveryLabels("uncommitted event"): 1,
	})
	if health := readHealth(t, healthPath); health.seriesCount != 2 {
		t.Fatalf("health series count after commit retry = %d, want 2", health.seriesCount)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("close engine after commit retry: %v", err)
	}
}

func TestHealthPublicationFailureKeepsCommittedEventPair(t *testing.T) {
	fixture := newEngineRecoveryFixture(t, recoveryEvent(1, "committed event"))
	if err := os.Mkdir(fixture.config.HealthPath(), 0o700); err != nil {
		t.Fatalf("create health-path directory to force publication failure: %v", err)
	}
	engine := restoreRecoveryEngine(t, fixture)
	if err := engine.CollectAndCommit(context.Background()); err == nil {
		t.Fatal("collection succeeded despite health-file publication failure")
	}
	assertRecoveryPair(t, fixture, 2, 1, map[collector.DevlinkLabels]uint64{
		recoveryLabels("committed event"): 1,
	})
	assertStateLockReleased(t, fixture.config.StateDir)
	if err := os.Remove(fixture.config.HealthPath()); err != nil {
		t.Fatalf("remove health-path test directory: %v", err)
	}
	engine = restoreRecoveryEngine(t, fixture)
	if err := engine.CollectAndCommit(context.Background()); err != nil {
		t.Fatalf("publish health after restart: %v", err)
	}
	if health := readHealth(t, fixture.config.HealthPath()); health.seriesCount != 1 {
		t.Fatalf("post-restart health series count = %d, want 1", health.seriesCount)
	}
	assertRecoveryPair(t, fixture, 2, 1, map[collector.DevlinkLabels]uint64{
		recoveryLabels("committed event"): 1,
	})
	if err := engine.Close(); err != nil {
		t.Fatalf("close engine: %v", err)
	}
}

type healthSample struct {
	lastSuccess time.Time
	seriesCount uint64
}

func readHealth(t *testing.T, path string) healthSample {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read health file: %v", err)
	}
	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(bytes.NewReader(contents))
	if err != nil {
		t.Fatalf("parse health file: %v", err)
	}
	if len(families) != 2 {
		t.Fatalf("health file contains %d families, want exactly 2", len(families))
	}
	lastSuccess := families["rasdaemon_collection_last_success_timestamp_seconds"]
	seriesCount := families["rasdaemon_series_count"]
	if lastSuccess == nil || lastSuccess.GetType() != dto.MetricType_GAUGE || seriesCount == nil || seriesCount.GetType() != dto.MetricType_GAUGE {
		t.Fatalf("health families have unexpected names or types: %#v", families)
	}
	if len(lastSuccess.GetMetric()) != 1 || len(seriesCount.GetMetric()) != 1 {
		t.Fatal("health gauges must each have exactly one sample")
	}
	if len(lastSuccess.GetMetric()[0].GetLabel()) != 0 || len(seriesCount.GetMetric()[0].GetLabel()) != 0 {
		t.Fatal("health gauges must not add labels")
	}
	return healthSample{
		lastSuccess: time.Unix(int64(lastSuccess.GetMetric()[0].GetGauge().GetValue()), 0),
		seriesCount: uint64(seriesCount.GetMetric()[0].GetGauge().GetValue()),
	}
}
