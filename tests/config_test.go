package collector_test

import (
	"errors"
	"os"
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
	if config.SnapshotPath != collector.DefaultSnapshotPath {
		t.Fatalf("default snapshot path = %q, want %q", config.SnapshotPath, collector.DefaultSnapshotPath)
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
			config := collector.Config{
				DatabasePath: test.path,
				StateDir:     validPath + ".state",
				SnapshotPath: collector.DefaultSnapshotPath,
			}
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
				SnapshotPath: collector.DefaultSnapshotPath,
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
	separator := string(filepath.Separator)
	cases := []struct {
		name string
		path string
	}{
		{name: "state directory", path: stateDir},
		{name: "lock file", path: filepath.Join(stateDir, "collector.lock")},
		{name: "state file", path: filepath.Join(stateDir, "state.json")},
		{name: "normalized state file", path: stateDir + separator + "." + separator + "state.json"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			config := collector.Config{
				DatabasePath: test.path,
				StateDir:     stateDir,
				SnapshotPath: filepath.Join(t.TempDir(), "metrics.prom"),
			}
			if err := config.Validate(); err == nil {
				t.Fatalf("Validate() accepted database path overlapping state: %q", test.path)
			}
		})
	}
}

func TestConfigValidationRequiresAbsoluteSnapshotPathWithoutCheckingFilesystem(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "events.db")
	stateDir := filepath.Join(t.TempDir(), "state")
	missingParentPath := filepath.Join(t.TempDir(), "missing-parent", "metrics.prom")
	cases := []struct {
		name string
		path string
		want bool
	}{
		{name: "empty", path: "", want: true},
		{name: "relative", path: "metrics.prom", want: true},
		{name: "absolute parent does not exist", path: missingParentPath},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			config := collector.Config{DatabasePath: databasePath, StateDir: stateDir, SnapshotPath: test.path}
			if err := config.Validate(); (err != nil) != test.want {
				t.Fatalf("Validate() error = %v, wantError %t", err, test.want)
			}
		})
	}
	if _, err := os.Stat(filepath.Dir(missingParentPath)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Validate() created or unexpectedly found missing parent: %v", err)
	}
}

func TestConfigValidationKeepsSnapshotOutsideSourceAndStatePaths(t *testing.T) {
	root := t.TempDir()
	databasePath := filepath.Join(root, "events.db")
	stateDir := filepath.Join(root, "state")
	cases := []struct {
		name string
		path string
	}{
		{name: "database", path: databasePath},
		{name: "state directory", path: stateDir},
		{name: "state file", path: filepath.Join(stateDir, "state.json")},
		{name: "normalized state file", path: stateDir + string(filepath.Separator) + "." + string(filepath.Separator) + "state.json"},
		{name: "lock file", path: filepath.Join(stateDir, "collector.lock")},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			config := collector.Config{DatabasePath: databasePath, StateDir: stateDir, SnapshotPath: test.path}
			if err := config.Validate(); err == nil {
				t.Fatalf("Validate() accepted conflicting snapshot path %q", test.path)
			}
		})
	}
}

func TestConfigValidationKeepsPreviousSnapshotOutsideSourceAndStatePaths(t *testing.T) {
	root := t.TempDir()
	cases := []struct {
		name         string
		databasePath string
		stateDir     string
		snapshotPath string
	}{
		{
			name:         "database",
			databasePath: filepath.Join(root, "metrics.prom.prev"),
			stateDir:     filepath.Join(root, "state"),
			snapshotPath: filepath.Join(root, "metrics.prom"),
		},
		{
			name:         "state directory",
			databasePath: filepath.Join(root, "events.db"),
			stateDir:     filepath.Join(root, "metrics.prom.prev"),
			snapshotPath: filepath.Join(root, "metrics.prom"),
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			config := collector.Config{
				DatabasePath: test.databasePath,
				StateDir:     test.stateDir,
				SnapshotPath: test.snapshotPath,
			}
			if err := config.Validate(); err == nil {
				t.Fatalf("Validate() accepted previous snapshot path %q overlapping %s", test.snapshotPath+".prev", test.name)
			}
		})
	}
}
