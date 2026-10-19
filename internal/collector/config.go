package collector

import (
	"errors"
	"fmt"
	"path/filepath"
	"time"
)

// DefaultDatabasePath is the rasdaemon database location on a standard host.
const DefaultDatabasePath = "/var/lib/rasdaemon/ras-mc_event.db"

// DefaultStateDir stores the exporter's lock and durable collection state.
const DefaultStateDir = "/var/lib/rasdaemon-exporter"

// DefaultSnapshotPath is the textfile collector output location.
const DefaultSnapshotPath = "/var/lib/node_exporter/textfile_collector/rasdaemon.prom"

// DefaultInterval is the delay between completed collection rounds.
const DefaultInterval = 5 * time.Second

// Config contains the source, snapshot, and scheduling settings for the collector.
type Config struct {
	DatabasePath string
	StateDir     string
	SnapshotPath string
	Interval     time.Duration
}

// DefaultConfig returns settings for the standard rasdaemon installation.
func DefaultConfig() Config {
	return Config{
		DatabasePath: DefaultDatabasePath,
		StateDir:     DefaultStateDir,
		SnapshotPath: DefaultSnapshotPath,
		Interval:     DefaultInterval,
	}
}

// Validate rejects invalid collection intervals and empty, relative, or conflicting paths.
func (c Config) Validate() error {
	if c.Interval <= 0 {
		return errors.New("collection interval must be greater than zero")
	}
	if c.DatabasePath == "" {
		return errors.New("database path must not be empty")
	}
	if !filepath.IsAbs(c.DatabasePath) {
		return fmt.Errorf("database path must be absolute: %q", c.DatabasePath)
	}
	if c.StateDir == "" {
		return errors.New("state directory must not be empty")
	}
	cleanStateDir := filepath.Clean(c.StateDir)
	if !filepath.IsAbs(cleanStateDir) || cleanStateDir == string(filepath.Separator) {
		return fmt.Errorf("state directory must be an absolute non-root path: %q", c.StateDir)
	}
	if c.SnapshotPath == "" {
		return errors.New("metrics snapshot path must not be empty")
	}
	cleanSnapshotPath := filepath.Clean(c.SnapshotPath)
	if !filepath.IsAbs(cleanSnapshotPath) {
		return fmt.Errorf("metrics snapshot path must be absolute: %q", c.SnapshotPath)
	}
	// Managed files must not overlap the source database or state files.
	source := filepath.Clean(c.DatabasePath)
	if source == cleanStateDir || source == filepath.Join(cleanStateDir, "collector.lock") ||
		source == filepath.Join(cleanStateDir, stateFileName) {
		return errors.New("database path must differ from the state directory, lock file and state file")
	}
	stateFiles := [...]string{
		cleanStateDir,
		filepath.Join(cleanStateDir, stateFileName),
		filepath.Join(cleanStateDir, "collector.lock"),
	}
	for _, outputPath := range [...]string{cleanSnapshotPath, cleanSnapshotPath + ".prev"} {
		if outputPath == source {
			return errors.New("metrics snapshot and backup paths must differ from the database")
		}
		for _, statePath := range stateFiles {
			if outputPath == statePath {
				return errors.New("metrics snapshot and backup paths must differ from the state files")
			}
		}
	}
	return nil
}
