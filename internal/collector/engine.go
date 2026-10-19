package collector

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Shared engine budgets; pollRows and pollTime bound a whole Poll round, not each table.
const (
	batchRows = 500
	pollRows  = 6000
	pollTime  = 2 * time.Second
)

// ErrPublicationUnavailable keeps the CLI from claiming to export metrics
// before the durable textfile publication path is implemented.
var ErrPublicationUnavailable = errors.New("metrics publication is not implemented")

// ErrNoCollectors reports that the database has no table supported by this build.
var ErrNoCollectors = errors.New("source database has no supported tables")

// Engine owns the read-only source and the in-memory devlink counters.
// The caller serializes all method calls; this type does not start goroutines.
type Engine struct {
	db      *sql.DB
	lock    *os.File
	devlink DevlinkCollector
	closed  bool
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
		db:   db,
		lock: lock,
	}, nil
}

// Poll reads a bounded round and applies only complete batches to memory.
// It deliberately does not advance durable state or publish a textfile.
func (e *Engine) Poll(ctx context.Context) error {
	if e == nil || e.closed || e.db == nil {
		return errors.New("collector engine is closed")
	}
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

// Run remains a startup check until metrics can be durably published.
func Run(ctx context.Context, cfg Config) error {
	engine, err := OpenEngine(ctx, cfg)
	if err != nil {
		return err
	}
	return errors.Join(ErrPublicationUnavailable, engine.Close())
}
