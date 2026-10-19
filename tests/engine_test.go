package collector_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"database/sql"

	"rasdaemon-exporter/internal/collector"
)

func TestRunWithNoSupportedTablesReturnsDeliberateIncompleteError(t *testing.T) {
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "events.db")
	createSQLite(t, databasePath, "CREATE TABLE unrelated (value TEXT)")
	before := fileDigest(t, databasePath)
	entriesBefore := directoryEntries(t, directory)

	err := collector.Run(context.Background(), validatedConfig(t, databasePath))
	if !errors.Is(err, collector.ErrNoCollectors) {
		t.Fatalf("Run() error = %v, want ErrNoCollectors", err)
	}
	if got := fileDigest(t, databasePath); got != before {
		t.Fatal("read-only bootstrap changed the synthetic source database")
	}
	if got := directoryEntries(t, directory); !reflect.DeepEqual(got, entriesBefore) {
		t.Fatalf("Run() created files beside the database: got %v, want %v", got, entriesBefore)
	}
}

func TestRunRejectsMissingAndNonSQLiteSourcesWithoutCreatingFiles(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		directory := t.TempDir()
		path := filepath.Join(directory, "missing.db")
		err := collector.Run(context.Background(), validatedConfig(t, path))
		if err == nil || errors.Is(err, collector.ErrNoCollectors) {
			t.Fatalf("Run() error = %v, want source-open error", err)
		}
		if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("missing source was created or stat failed unexpectedly: %v", statErr)
		}
		if got := directoryEntries(t, directory); len(got) != 0 {
			t.Fatalf("Run() created files for missing source: %v", got)
		}
	})

	t.Run("invalid database", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "invalid.db")
		contents := []byte("this is not a SQLite database\n")
		if err := os.WriteFile(path, contents, 0o600); err != nil {
			t.Fatal(err)
		}
		before := fileDigest(t, path)
		err := collector.Run(context.Background(), validatedConfig(t, path))
		if err == nil || errors.Is(err, collector.ErrNoCollectors) {
			t.Fatalf("Run() error = %v, want invalid SQLite error", err)
		}
		if got := fileDigest(t, path); got != before {
			t.Fatal("failed read-only open changed the invalid source file")
		}
	})

	t.Run("directory is not a database file", func(t *testing.T) {
		path := t.TempDir()
		err := collector.Run(context.Background(), validatedConfig(t, path))
		if err == nil || errors.Is(err, collector.ErrNoCollectors) {
			t.Fatalf("Run() error = %v, want non-regular source error", err)
		}
	})

	t.Run("symbolic link is not accepted as the source file", func(t *testing.T) {
		directory := t.TempDir()
		target := filepath.Join(directory, "events.db")
		link := filepath.Join(directory, "events-link.db")
		createSQLite(t, target, "CREATE TABLE unrelated (value TEXT)")
		if err := os.Symlink(target, link); err != nil {
			t.Fatalf("create temporary source symlink: %v", err)
		}

		err := collector.Run(context.Background(), validatedConfig(t, link))
		if err == nil || errors.Is(err, collector.ErrNoCollectors) {
			t.Fatalf("Run() error = %v, want symlink rejection", err)
		}
		if got := directoryEntries(t, directory); !reflect.DeepEqual(got, []string{"events-link.db", "events.db"}) {
			t.Fatalf("unexpected files after symlink test: %v", got)
		}
	})
}

func TestRunEncodesDatabasePathAsAFilePath(t *testing.T) {
	directory := t.TempDir()
	plainPath := filepath.Join(directory, "plain.db")
	createSQLite(t, plainPath, "CREATE TABLE unrelated (value TEXT)")

	oddPath := filepath.Join(directory, "events ?#% %3f 雪.db")
	if err := os.Rename(plainPath, oddPath); err != nil {
		t.Fatalf("rename synthetic database: %v", err)
	}
	before := fileDigest(t, oddPath)
	err := collector.Run(context.Background(), validatedConfig(t, oddPath))
	if !errors.Is(err, collector.ErrNoCollectors) {
		t.Fatalf("Run() error = %v, want ErrNoCollectors for exact filename", err)
	}
	if got := fileDigest(t, oddPath); got != before {
		t.Fatal("read-only bootstrap changed the database with URI-reserved path characters")
	}
	if got := directoryEntries(t, directory); !reflect.DeepEqual(got, []string{filepath.Base(oddPath)}) {
		t.Fatalf("unexpected files after URI path test: %v", got)
	}
}

func TestRunReturnsCancellationAndDoesNotPublishFiles(t *testing.T) {
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "events.db")
	createSQLite(t, databasePath, "CREATE TABLE unrelated (value TEXT)")
	entriesBefore := directoryEntries(t, directory)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := collector.Run(ctx, validatedConfig(t, databasePath))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
	if got := directoryEntries(t, directory); !reflect.DeepEqual(got, entriesBefore) {
		t.Fatalf("canceled startup created files: %v", got)
	}
}

func TestRunClosesSourceBeforeReturning(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "events.db")
	createSQLite(t, databasePath, "CREATE TABLE unrelated (value TEXT)")

	for attempt := 0; attempt < 3; attempt++ {
		err := collector.Run(context.Background(), validatedConfig(t, databasePath))
		if !errors.Is(err, collector.ErrNoCollectors) {
			t.Fatalf("Run() attempt %d error = %v, want ErrNoCollectors", attempt, err)
		}
		assertExclusiveSQLiteAccess(t, databasePath)
	}
}

func TestRunHonorsBoundedSQLiteBusyTimeout(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "events.db")
	createSQLite(t, databasePath, "CREATE TABLE unrelated (value TEXT)")

	locker, err := sql.Open("sqlite3", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	locker.SetMaxOpenConns(1)
	connection, err := locker.Conn(context.Background())
	if err != nil {
		locker.Close()
		t.Fatal(err)
	}
	if _, err := connection.ExecContext(context.Background(), "BEGIN EXCLUSIVE"); err != nil {
		connection.Close()
		locker.Close()
		t.Fatalf("acquire synthetic exclusive lock: %v", err)
	}
	t.Cleanup(func() {
		_, _ = connection.ExecContext(context.Background(), "ROLLBACK")
		_ = connection.Close()
		_ = locker.Close()
	})

	started := time.Now()
	err = collector.Run(context.Background(), validatedConfig(t, databasePath))
	elapsed := time.Since(started)
	if err == nil || errors.Is(err, collector.ErrNoCollectors) {
		t.Fatalf("Run() error = %v, want bounded lock error", err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("Run() waited %s for the SQLite lock; want a bounded wait under 2s", elapsed)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	started = time.Now()
	err = collector.Run(ctx, validatedConfig(t, databasePath))
	elapsed = time.Since(started)
	if err == nil || errors.Is(err, collector.ErrNoCollectors) {
		t.Fatalf("Run() with a locked source and expired context returned %v", err)
	}
	if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("locked-read context error = %v, want deadline exceeded", ctx.Err())
	}
	if elapsed > 2*time.Second {
		t.Fatalf("Run() with canceled locked query waited %s; want a bounded return under 2s", elapsed)
	}
}

func directoryEntries(t *testing.T, directory string) []string {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatalf("read temporary directory: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	return names
}

func assertExclusiveSQLiteAccess(t *testing.T, path string) {
	t.Helper()
	writer, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	writer.SetMaxOpenConns(1)
	connection, err := writer.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(context.Background(), "BEGIN EXCLUSIVE"); err != nil {
		t.Fatalf("acquire exclusive lock after Run returned: %v", err)
	}
	if _, err := connection.ExecContext(context.Background(), "ROLLBACK"); err != nil {
		t.Fatalf("release exclusive lock after Run returned: %v", err)
	}
}
