package collector

import (
	"errors"
	"fmt"
	"path/filepath"
)

// DefaultDatabasePath is the rasdaemon database location on a standard host.
const DefaultDatabasePath = "/var/lib/rasdaemon/ras-mc_event.db"

// DefaultStateDir stores the exporter's lock and future durable state.
const DefaultStateDir = "/var/lib/rasdaemon-exporter"

// Config contains the source settings needed by the runtime bootstrap.
type Config struct {
	DatabasePath string
	StateDir     string
}

// DefaultConfig returns settings for the standard rasdaemon installation.
func DefaultConfig() Config {
	return Config{
		DatabasePath: DefaultDatabasePath,
		StateDir:     DefaultStateDir,
	}
}

// Validate rejects ambiguous paths before any filesystem or database access.
func (c Config) Validate() error {
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
	// Collector-owned paths must never overwrite the source database.
	source := filepath.Clean(c.DatabasePath)
	if source == cleanStateDir || source == filepath.Join(cleanStateDir, "collector.lock") ||
		source == filepath.Join(cleanStateDir, stateFileName) {
		return errors.New("database path must differ from the state directory, lock file and state file")
	}
	return nil
}
