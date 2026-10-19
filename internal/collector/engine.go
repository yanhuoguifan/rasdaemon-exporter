package collector

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// Shared engine budgets; pollRows and pollTime bound one collection round.
const (
	batchRows = 500
	pollRows  = 6000
	pollTime  = 2 * time.Second
)

// ErrNoCollectors reports that the database has no table supported by this build.
var ErrNoCollectors = errors.New("source database has no supported tables")

// Engine owns the read-only source and the in-memory devlink counters.
// The caller serializes all method calls; this type does not start goroutines.
type Engine struct {
	db           *sql.DB
	lock         *os.File
	databasePath string
	stateDir     string
	snapshotPath string
	devlink      DevlinkCollector
	committed    *State
	closed       bool
}

// OpenEngine prepares the state lock and opens the supported SQLite source.
// The configuration must already have passed Config.Validate.
func OpenEngine(ctx context.Context, cfg Config) (_ *Engine, openErr error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stateDir, err := preparePrivateDir(cfg.StateDir)
	if err != nil {
		return nil, err
	}

	lock, err := AcquireLock(stateDir)
	if err != nil {
		return nil, err
	}
	var db *sql.DB
	defer func() {
		if openErr == nil {
			return
		}
		openErr = errors.Join(openErr, closeResources(db, lock))
	}()

	db, err = openSQLite(ctx, cfg.DatabasePath)
	if err != nil {
		return nil, err
	}
	tables, err := discoverTables(ctx, db)
	if err != nil {
		return nil, err
	}
	if len(matchingSupportedTables(tables)) == 0 {
		return nil, ErrNoCollectors
	}
	return &Engine{
		db:           db,
		lock:         lock,
		databasePath: cfg.DatabasePath,
		stateDir:     stateDir,
		snapshotPath: filepath.Clean(cfg.SnapshotPath),
	}, nil
}

// Restore pairs the cursor state and metrics snapshot before collection begins.
// A missing pair is initialized; an orphaned first snapshot completes initialization.
func (e *Engine) Restore(ctx context.Context) (restoreErr error) {
	if e == nil || e.closed || e.db == nil {
		return errors.New("collector engine is closed")
	}
	if e.committed != nil {
		return errors.New("collector engine is already restored")
	}
	defer func() {
		if restoreErr != nil {
			restoreErr = errors.Join(restoreErr, e.Close())
		}
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	state, stateErr := LoadState(e.stateDir)
	snapshot, snapshotErr := LoadSnapshot(e.snapshotPath)
	stateMissing := errors.Is(stateErr, os.ErrNotExist)
	snapshotMissing := errors.Is(snapshotErr, os.ErrNotExist)

	switch {
	case stateMissing && snapshotMissing:
		if _, err := os.Lstat(e.snapshotBackupPath()); err == nil {
			return errors.New("state and metrics snapshot are missing while a previous snapshot exists")
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("inspect previous metrics snapshot: %w", err)
		}
		return e.initialize(true)
	case stateMissing && snapshotErr == nil:
		if snapshot.Generation != 1 {
			return errors.New("state file is missing beyond the first generation")
		}
		return e.initialize(false)
	case stateErr != nil:
		return stateErr
	}

	identity, err := databaseIdentity(e.databasePath)
	if err != nil {
		return err
	}
	cursor, err := devlinkCursor(state)
	if err != nil {
		return err
	}
	if err := state.CheckSource(identity); err != nil {
		return err
	}
	for _, path := range []string{e.snapshotPath, e.snapshotBackupPath()} {
		var candidate *Snapshot
		var candidateErr error
		if path == e.snapshotPath {
			candidate, candidateErr = snapshot, snapshotErr
		} else {
			candidate, candidateErr = LoadSnapshot(path)
		}
		if candidateErr != nil || candidate.Generation != state.Generation {
			continue
		}
		var recovered DevlinkCollector
		if err := recovered.RestoreMetrics(candidate.Families); err != nil {
			if path == e.snapshotPath {
				snapshotErr = err
			}
			continue
		}
		if path != e.snapshotPath {
			if err := SaveSnapshot(e.snapshotPath, candidate); err != nil {
				return err
			}
		}
		recovered.LastID = cursor
		e.devlink = recovered
		e.committed = state
		removeRegularFile(e.snapshotBackupPath())
		return nil
	}
	if snapshotErr != nil {
		return snapshotErr
	}
	return fmt.Errorf("no current or previous metrics snapshot matches committed generation %d (current generation %d)",
		state.Generation, snapshot.Generation)
}

// initialize creates generation one, optionally writing its missing empty snapshot first.
func (e *Engine) initialize(snapshotMissing bool) error {
	identity, err := databaseIdentity(e.databasePath)
	if err != nil {
		return err
	}
	state := NewState(identity, map[string]int64{"devlink_event": 0})
	if snapshotMissing {
		if err := SaveSnapshot(e.snapshotPath, &Snapshot{Generation: state.Generation}); err != nil {
			return err
		}
	}
	if err := SaveState(e.stateDir, state); err != nil {
		return err
	}
	e.committed = state
	removeRegularFile(e.snapshotBackupPath())
	return nil
}

// poll applies complete batches until a row or time budget is reached.
func (e *Engine) poll(ctx context.Context) error {
	started := time.Now()
	rowsRead := 0
	for rowsRead < pollRows && time.Since(started) < pollTime {
		if err := ctx.Err(); err != nil {
			return err
		}
		rows, err := e.devlink.CollectBatch(ctx, e.db, batchRows)
		if err != nil {
			return err
		}
		rowsRead += rows
		if rows < batchRows {
			return nil
		}
	}
	return nil
}

// CollectAndCommit reads one round and publishes counters before their matching cursors.
func (e *Engine) CollectAndCommit(ctx context.Context) (collectErr error) {
	if e == nil || e.closed || e.db == nil {
		return errors.New("collector engine is closed")
	}
	if e.committed == nil {
		return errors.New("collector engine must be restored before collection")
	}
	defer func() {
		if collectErr != nil {
			collectErr = errors.Join(collectErr, e.Close())
		}
	}()
	if err := e.poll(ctx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if e.devlink.LastID == e.committed.Tables["devlink_event"] {
		return nil
	}
	candidate, err := e.committed.Advance(map[string]int64{"devlink_event": e.devlink.LastID})
	if err != nil {
		return err
	}
	return e.commit(candidate)
}

// commit writes the previous metrics generation, the next metrics generation,
// then its matching state; it removes the backup only after both new files save.
func (e *Engine) commit(candidate *State) error {
	snapshot := &Snapshot{Generation: candidate.Generation, Families: e.devlink.ExportMetrics()}
	if e.committed != nil {
		previous, err := LoadSnapshot(e.snapshotPath)
		if err != nil {
			return fmt.Errorf("load committed metrics snapshot before commit: %w", err)
		}
		if previous.Generation != e.committed.Generation {
			return fmt.Errorf("current metrics snapshot generation %d does not match committed generation %d",
				previous.Generation, e.committed.Generation)
		}
		if err := SaveSnapshot(e.snapshotBackupPath(), previous); err != nil {
			return fmt.Errorf("save previous metrics snapshot: %w", err)
		}
	}
	if err := SaveSnapshot(e.snapshotPath, snapshot); err != nil {
		return err
	}
	if err := SaveState(e.stateDir, candidate); err != nil {
		return err
	}
	e.committed = candidate
	removeRegularFile(e.snapshotBackupPath())
	return nil
}

func (e *Engine) snapshotBackupPath() string {
	return e.snapshotPath + ".prev"
}

// Close releases SQLite before releasing the state-directory lock.
func (e *Engine) Close() error {
	if e == nil || e.closed {
		return nil
	}
	e.closed = true
	return closeResources(e.db, e.lock)
}

// AcquireLock applies the lock-file convention to a prepared state directory.
func AcquireLock(stateDir string) (*os.File, error) {
	if !filepath.IsAbs(stateDir) || filepath.Clean(stateDir) == string(filepath.Separator) {
		return nil, errors.New("state directory must be an absolute non-root path")
	}
	return acquireFileLock(filepath.Join(stateDir, "collector.lock"))
}

func devlinkCursor(state *State) (int64, error) {
	if len(state.Tables) != 1 {
		return 0, errors.New("state must contain exactly the enabled devlink_event cursor")
	}
	cursor, exists := state.Tables["devlink_event"]
	if !exists {
		return 0, errors.New("state is missing the devlink_event cursor")
	}
	return cursor, nil
}

func databaseIdentity(path string) (SourceIdentity, error) {
	info, err := os.Stat(path)
	if err != nil {
		return SourceIdentity{}, fmt.Errorf("inspect SQLite source %q: %w", path, err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return SourceIdentity{}, errors.New("cannot read SQLite source file identity")
	}
	return SourceIdentity{Path: path, Device: uint64(stat.Dev), Inode: stat.Ino}, nil
}

func closeResources(db *sql.DB, lock *os.File) error {
	var closeErr error
	if db != nil {
		if err := db.Close(); err != nil {
			closeErr = errors.Join(closeErr, fmt.Errorf("close SQLite source: %w", err))
		}
	}
	if lock != nil {
		if err := lock.Close(); err != nil {
			closeErr = errors.Join(closeErr, fmt.Errorf("close collector lock: %w", err))
		}
	}
	return closeErr
}

// Run restores persisted data, collects immediately, then waits between rounds.
// The caller must validate cfg before calling Run.
func Run(ctx context.Context, cfg Config) (runErr error) {
	engine, err := OpenEngine(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() {
		runErr = errors.Join(runErr, engine.Close())
	}()
	if err := engine.Restore(ctx); err != nil {
		return err
	}
	slog.Info("Starting collection loop",
		"database", cfg.DatabasePath,
		"output", cfg.SnapshotPath,
		"interval", cfg.Interval.String(),
	)
	timer := time.NewTimer(cfg.Interval)
	defer timer.Stop()
	for {
		if err := engine.CollectAndCommit(ctx); err != nil {
			return err
		}
		timer.Reset(cfg.Interval)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
}
