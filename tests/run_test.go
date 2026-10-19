package collector_test

import (
	"context"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"testing"
	"time"

	"rasdaemon-exporter/internal/collector"
)

func TestRunCollectsImmediatelyAndCancellationInterruptsLongWait(t *testing.T) {
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "events.db")
	createDevlinkDatabase(t, databasePath, recoveryEvent(1, "first event"))
	cfg := validatedConfig(t, databasePath)
	cfg.Interval = 10 * time.Second
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate test configuration: %v", err)
	}

	cancel, done := startRun(t, cfg)
	waitForCommitted(t, cfg.StateDir, cfg.SnapshotPath, 2, 1, map[collector.DevlinkLabels]uint64{
		recoveryLabels("first event"): 1,
	})

	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() after cancellation = %v, want nil or context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not stop promptly during the interval wait")
	}
	assertStateLockReleased(t, cfg.StateDir)
}

func TestRunCollectsAppendedRowsAndRestartsWithoutDoubleCounting(t *testing.T) {
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "events.db")
	createDevlinkDatabase(t, databasePath, recoveryEvent(1, "same event"))
	cfg := validatedConfig(t, databasePath)
	cfg.Interval = 40 * time.Millisecond
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate test configuration: %v", err)
	}

	cancel, done := startRun(t, cfg)
	waitForCommitted(t, cfg.StateDir, cfg.SnapshotPath, 2, 1, map[collector.DevlinkLabels]uint64{
		recoveryLabels("same event"): 1,
	})
	insertDevlinkEvents(t, databasePath,
		recoveryEvent(2, "same event"),
		recoveryEvent(3, "another event"),
	)
	want := map[collector.DevlinkLabels]uint64{
		recoveryLabels("same event"):    2,
		recoveryLabels("another event"): 1,
	}
	waitForCommitted(t, cfg.StateDir, cfg.SnapshotPath, 3, 3, want)
	cancel()
	if err := waitForRun(t, done); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() after cancellation = %v, want nil or context.Canceled", err)
	}
	assertStateLockReleased(t, cfg.StateDir)

	committed, err := collector.LoadState(cfg.StateDir)
	if err != nil {
		t.Fatalf("load committed state before restart: %v", err)
	}
	snapshotBefore, snapshotInfo := readCommittedFile(t, cfg.SnapshotPath)
	statePath := filepath.Join(cfg.StateDir, "state.json")
	stateBefore, stateInfo := readCommittedFile(t, statePath)
	cfg.Interval = 40 * time.Millisecond
	cancelRestart, doneRestart := startRun(t, cfg)
	waitForStateLock(t, cfg.StateDir)
	time.Sleep(4 * cfg.Interval)
	select {
	case err := <-doneRestart:
		t.Fatalf("Run() exited during idle rounds: %v", err)
	default:
	}
	assertCommittedFileUnchanged(t, cfg.SnapshotPath, snapshotBefore, snapshotInfo)
	assertCommittedFileUnchanged(t, statePath, stateBefore, stateInfo)
	if _, err := os.Lstat(cfg.SnapshotPath + ".prev"); !os.IsNotExist(err) {
		t.Fatalf("idle restart created previous snapshot: %v", err)
	}
	stateAfter, err := collector.LoadState(cfg.StateDir)
	if err != nil || stateAfter.Generation != committed.Generation {
		t.Fatalf("idle restart state generation = (%v, %v), want %d", stateAfter, err, committed.Generation)
	}
	cancelRestart()
	if err := waitForRun(t, doneRestart); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("restarted Run() after cancellation = %v, want nil or context.Canceled", err)
	}
	assertStateLockReleased(t, cfg.StateDir)
}

func TestRunStopsAfterCommitFailureAndReleasesLock(t *testing.T) {
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "events.db")
	createDevlinkDatabase(t, databasePath, recoveryEvent(1, "first event"))
	outputDirectory := filepath.Join(directory, "output")
	if err := os.Mkdir(outputDirectory, 0o700); err != nil {
		t.Fatalf("create output directory: %v", err)
	}
	cfg := validatedConfig(t, databasePath)
	cfg.SnapshotPath = filepath.Join(outputDirectory, "metrics.prom")
	cfg.Interval = 500 * time.Millisecond
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate test configuration: %v", err)
	}

	cancel, done := startRun(t, cfg)
	waitForCommitted(t, cfg.StateDir, cfg.SnapshotPath, 2, 1, map[collector.DevlinkLabels]uint64{
		recoveryLabels("first event"): 1,
	})
	if err := os.Remove(cfg.SnapshotPath); err != nil {
		cancel()
		t.Fatalf("remove published snapshot to simulate commit failure: %v", err)
	}
	if err := os.Mkdir(cfg.SnapshotPath, 0o700); err != nil {
		cancel()
		t.Fatalf("replace snapshot with a directory: %v", err)
	}
	insertDevlinkEvents(t, databasePath, recoveryEvent(2, "next event"))

	err := waitForRun(t, done)
	cancel()
	if err == nil || errors.Is(err, context.Canceled) {
		t.Fatalf("Run() after snapshot commit failure = %v, want a commit error", err)
	}
	state, err := collector.LoadState(cfg.StateDir)
	if err != nil {
		t.Fatalf("load state after failed commit: %v", err)
	}
	if state.Generation != 2 || state.Tables["devlink_event"] != 1 {
		t.Fatalf("failed commit advanced state: generation=%d cursor=%d", state.Generation, state.Tables["devlink_event"])
	}
	assertStateLockReleased(t, cfg.StateDir)
}

func TestRunStopsOnInvalidDevlinkProjectionAndReleasesLock(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "events.db")
	createSQLite(t, databasePath, "CREATE TABLE devlink_event (id INTEGER PRIMARY KEY)")
	cfg := validatedConfig(t, databasePath)
	started := time.Now()
	err := collector.Run(context.Background(), cfg)
	if err == nil {
		t.Fatal("Run() succeeded with an invalid devlink projection")
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("Run() took %s to return the collection error", elapsed)
	}
	state, stateErr := collector.LoadState(cfg.StateDir)
	if stateErr != nil {
		t.Fatalf("load initialized state after collection failure: %v", stateErr)
	}
	if state.Generation != 1 || state.Tables["devlink_event"] != 0 {
		t.Fatalf("collection failure advanced state: generation=%d cursor=%d", state.Generation, state.Tables["devlink_event"])
	}
	assertStateLockReleased(t, cfg.StateDir)
}

func waitForCommitted(t *testing.T, stateDir, snapshotPath string, minGeneration uint64, cursor int64, want map[collector.DevlinkLabels]uint64) uint64 {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var lastState *collector.State
	var lastSnapshot *collector.Snapshot
	var lastErr error
	for time.Now().Before(deadline) {
		state, stateErr := collector.LoadState(stateDir)
		snapshot, snapshotErr := collector.LoadSnapshot(snapshotPath)
		if stateErr == nil && snapshotErr == nil {
			lastState, lastSnapshot = state, snapshot
			if state.Generation >= minGeneration && snapshot.Generation == state.Generation && state.Tables["devlink_event"] == cursor {
				var devlink collector.DevlinkCollector
				if err := devlink.RestoreMetrics(snapshot.Families); err == nil && maps.Equal(devlink.Counts, want) {
					return state.Generation
				} else if err != nil {
					lastErr = err
				}
			}
		} else if stateErr != nil {
			lastErr = stateErr
		} else {
			lastErr = snapshotErr
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for generation >= %d, cursor %d, counts %v; last state=%+v snapshot=%+v err=%v",
		minGeneration, cursor, want, lastState, lastSnapshot, lastErr)
	return 0
}

func waitForRun(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("Run() did not return within three seconds")
		return nil
	}
}

func startRun(t *testing.T, cfg collector.Config) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func(ctx context.Context, cfg collector.Config, result chan<- error) {
		result <- collector.Run(ctx, cfg)
		close(result)
	}(ctx, cfg, done)
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("Run() goroutine did not stop after test cleanup cancellation")
		}
	})
	return cancel, done
}

func readCommittedFile(t *testing.T, path string) ([]byte, os.FileInfo) {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read committed file %q: %v", path, err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat committed file %q: %v", path, err)
	}
	return contents, info
}

func assertCommittedFileUnchanged(t *testing.T, path string, contents []byte, info os.FileInfo) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil || string(got) != string(contents) {
		t.Errorf("file %q contents changed: read error %v", path, err)
	}
	after, err := os.Stat(path)
	if err != nil || !os.SameFile(info, after) || !info.ModTime().Equal(after.ModTime()) {
		t.Errorf("file %q identity or modification time changed: stat error %v", path, err)
	}
}

func waitForStateLock(t *testing.T, stateDir string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		lock, err := collector.AcquireLock(stateDir)
		if errors.Is(err, collector.ErrLocked) {
			return
		}
		if err == nil {
			_ = lock.Close()
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("probe collector lock: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for Run() to hold the state lock")
}
