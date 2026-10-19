package collector

import (
	"errors"
	"fmt"
	"path/filepath"
)

// DefaultDatabasePath is the rasdaemon database location on a standard host.
const DefaultDatabasePath = "/var/lib/rasdaemon/ras-mc_event.db"

// Config contains the source settings needed by the runtime bootstrap.
type Config struct {
	DatabasePath string
}

// DefaultConfig returns settings for the standard rasdaemon installation.
func DefaultConfig() Config {
	return Config{DatabasePath: DefaultDatabasePath}
}

// Validate rejects ambiguous paths before any filesystem or database access.
func (c Config) Validate() error {
	if c.DatabasePath == "" {
		return errors.New("database path must not be empty")
	}
	if !filepath.IsAbs(c.DatabasePath) {
		return fmt.Errorf("database path must be absolute: %q", c.DatabasePath)
	}
	return nil
}
