package collector

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// ErrNoCollectors reports that this build has no supported table readers.
var ErrNoCollectors = errors.New("no supported table collectors are registered")

// Run exercises the read-only startup path using an already validated config.
// Callers must call Config.Validate before Run; the CLI does so while parsing.
// This bootstrap intentionally does not collect events or create output files.
func Run(ctx context.Context, cfg Config) (runErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	stateDir, err := preparePrivateDir(cfg.StateDir)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	lock, err := AcquireLock(stateDir)
	if err != nil {
		return err
	}
	defer func() {
		if err := lock.Close(); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("release collector lock: %w", err))
		}
	}()

	db, err := openSQLite(ctx, cfg.DatabasePath)
	if err != nil {
		return err
	}
	defer func() {
		if err := db.Close(); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("close SQLite source: %w", err))
		}
	}()

	databaseTables, err := discoverTables(ctx, db)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(matchingSupportedTables(databaseTables)) == 0 {
		return ErrNoCollectors
	}

	// A registered table reader must be wired into the engine in the same
	// change that registers it. Until then, reporting success would be false.
	return errors.New("registered table collectors are not wired into the bootstrap")
}

// AcquireLock applies the collector's lock-file convention to a prepared directory.
// Keep the returned file open for the collector lifetime; never unlink it.
func AcquireLock(stateDir string) (*os.File, error) {
	if !filepath.IsAbs(stateDir) || filepath.Clean(stateDir) == string(filepath.Separator) {
		return nil, errors.New("state directory must be an absolute non-root path")
	}
	return acquireFileLock(filepath.Join(stateDir, "collector.lock"))
}
