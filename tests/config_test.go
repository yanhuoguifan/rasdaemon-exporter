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
			config := collector.Config{DatabasePath: test.path}
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
