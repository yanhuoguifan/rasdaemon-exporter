package collector

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const (
	databaseTimeout = 500 * time.Millisecond
	databaseBusyMS  = 250
)

// openSQLite opens an existing regular file without allowing SQLite to create
// or modify it. URI escaping keeps filename characters out of driver options.
func openSQLite(ctx context.Context, path string) (*sql.DB, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("inspect SQLite source %q: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("SQLite source must be a regular file, got %q", path)
	}

	dsn := sqliteDataSource(path)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("configure SQLite source %q: %w", path, err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	pingContext, cancel := context.WithTimeout(ctx, databaseTimeout)
	defer cancel()
	if err := db.PingContext(pingContext); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("open read-only SQLite source %q: %w", path, err)
	}
	return db, nil
}

// sqliteDataSource builds the driver's URI from a filesystem path, not input
// supplied as a SQLite connection string.
func sqliteDataSource(path string) string {
	uri := url.URL{Scheme: "file", Path: path}
	query := uri.Query()
	query.Set("mode", "ro")
	query.Set("_query_only", "1")
	query.Set("_busy_timeout", fmt.Sprint(databaseBusyMS))
	uri.RawQuery = query.Encode()
	return uri.String()
}

// discoverTables reads table names only; it does not inspect DDL or versions.
func discoverTables(ctx context.Context, db *sql.DB) ([]string, error) {
	queryContext, cancel := context.WithTimeout(ctx, databaseTimeout)
	defer cancel()
	rows, err := db.QueryContext(queryContext,
		"SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
	if err != nil {
		return nil, fmt.Errorf("discover SQLite tables: %w", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("read SQLite table name: %w", err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate SQLite table names: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close SQLite table query: %w", err)
	}
	return names, nil
}
