package collector_test

import (
	"path/filepath"
	"testing"

	"rasdaemon-exporter/internal/collector"
)

func TestDefaultConfigUsesDocumentedDatabasePath(t *testing.T) {
	config := collector.DefaultConfig()
	if config.DatabasePath != collector.DefaultDatabasePath {
		t.Fatalf("default database path = %q, want %q", config.DatabasePath, collector.DefaultDatabasePath)
	}
	if config.StateDir != collector.DefaultStateDir {
		t.Fatalf("default state directory = %q, want %q", config.StateDir, collector.DefaultStateDir)
	}
	if err := config.Validate(); err != nil {
		t.Fatalf("validate default config: %v", err)
	}
}

func TestConfigValidationRequiresAbsoluteNonemptyPath(t *testing.T) {
	validPath := filepath.Join(t.TempDir(), "not-created.db")
	cases := []struct {
		name string
		path string
		want bool
	}{
		{name: "empty", path: "", want: true},
		{name: "relative", path: "events.db", want: true},
		{name: "absolute but not yet present", path: validPath},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			config := collector.Config{DatabasePath: test.path, StateDir: validPath + ".state"}
			original := config
			err := config.Validate()
			if (err != nil) != test.want {
				t.Fatalf("Validate() error = %v, wantError %t", err, test.want)
			}
			if config != original {
				t.Fatalf("Validate() mutated config: got %+v, want %+v", config, original)
			}
		})
	}
}

func TestConfigValidationRequiresAbsoluteNonRootStateDirectory(t *testing.T) {
	validStateDir := filepath.Join(t.TempDir(), "state")
	cases := []struct {
		name string
		path string
		want bool
	}{
		{name: "empty", path: "", want: true},
		{name: "relative", path: "state", want: true},
		{name: "root", path: string(filepath.Separator), want: true},
		{name: "absolute but not yet present", path: validStateDir},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			config := collector.Config{
				DatabasePath: filepath.Join(t.TempDir(), "events.db"),
				StateDir:     test.path,
			}
			err := config.Validate()
			if (err != nil) != test.want {
				t.Fatalf("Validate() error = %v, wantError %t", err, test.want)
			}
		})
	}
}

func TestConfigValidationKeepsDatabaseOutsideStatePaths(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	for _, databasePath := range []string{
		stateDir,
		filepath.Join(stateDir, "collector.lock"),
	} {
		t.Run(filepath.Base(databasePath), func(t *testing.T) {
			config := collector.Config{DatabasePath: databasePath, StateDir: stateDir}
			if err := config.Validate(); err == nil {
				t.Fatalf("Validate() accepted database path overlapping state: %q", databasePath)
			}
		})
	}
}
