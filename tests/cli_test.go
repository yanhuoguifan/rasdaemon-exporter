package collector_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"rasdaemon-exporter/internal/collector"
)

func TestCLIHelpDoesNotNeedTheHostDatabase(t *testing.T) {
	output, code := runCLI(t, "--help")
	if code != 0 {
		t.Fatalf("--help exit code = %d; output:\n%s", code, output)
	}
	for _, expected := range []string{"--db", collector.DefaultDatabasePath, "no event collectors"} {
		if !strings.Contains(strings.ToLower(output), strings.ToLower(expected)) {
			t.Errorf("--help output does not contain %q:\n%s", expected, output)
		}
	}
}

func TestCLIConfigurationErrorsExitTwo(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{name: "unknown flag", args: []string{"--unknown"}},
		{name: "positional argument", args: []string{"unexpected"}},
		{name: "empty database path", args: []string{"--db="}},
		{name: "relative database path", args: []string{"--db=relative.db"}},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			output, code := runCLI(t, test.args...)
			if code != 2 {
				t.Fatalf("CLI exit code = %d, want 2; output:\n%s", code, output)
			}
			if output == "" {
				t.Fatal("CLI returned no diagnostic")
			}
		})
	}
}

func TestCLISourceFailuresExitOne(t *testing.T) {
	directory := t.TempDir()
	missingPath := filepath.Join(directory, "missing.db")
	invalidPath := filepath.Join(directory, "invalid.db")
	if err := os.WriteFile(invalidPath, []byte("not sqlite\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	validPath := filepath.Join(directory, "empty.db")
	createSQLite(t, validPath, "CREATE TABLE unrelated (value TEXT)")
	pathDirectory := filepath.Join(directory, "a-directory")
	if err := os.Mkdir(pathDirectory, 0o700); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		path string
	}{
		{name: "missing file", path: missingPath},
		{name: "invalid SQLite file", path: invalidPath},
		{name: "directory source", path: pathDirectory},
		{name: "no supported table collectors", path: validPath},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			output, code := runCLI(t, "--db", test.path)
			if code != 1 {
				t.Fatalf("CLI exit code = %d, want 1; output:\n%s", code, output)
			}
			if output == "" {
				t.Fatal("CLI returned no diagnostic")
			}
		})
	}
	if _, err := os.Stat(missingPath); !os.IsNotExist(err) {
		t.Fatalf("CLI created missing database: stat error = %v", err)
	}
}

func runCLI(t *testing.T, args ...string) (string, int) {
	t.Helper()
	command := exec.Command(buildCLI(t), args...)
	command.Dir = t.TempDir()
	output, err := command.CombinedOutput()
	if err == nil {
		return string(output), 0
	}
	if exitError, ok := err.(*exec.ExitError); ok {
		return string(output), exitError.ExitCode()
	}
	t.Fatalf("run CLI: %v", fmt.Errorf("%w\n%s", err, output))
	return "", -1
}
