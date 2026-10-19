package collector_test

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"rasdaemon-exporter/internal/collector"
)

func TestCLIHelpDoesNotNeedTheHostDatabase(t *testing.T) {
	output, code := runCLI(t, "--help")
	if code != 0 {
		t.Fatalf("--help exit code = %d; output:\n%s", code, output)
	}
	for _, expected := range []string{
		"--db", collector.DefaultDatabasePath,
		"--state-dir", collector.DefaultStateDir,
		"metrics publication is not implemented yet",
	} {
		if !strings.Contains(strings.ToLower(output), strings.ToLower(expected)) {
			t.Errorf("--help output does not contain %q:\n%s", expected, output)
		}
	}
}

func TestCLIWithDevlinkTableDoesNotPollOrPublish(t *testing.T) {
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "events.db")
	// Discovery succeeds, but any collection attempt fails on missing columns.
	createSQLite(t, databasePath, "CREATE TABLE devlink_event (id INTEGER PRIMARY KEY)")
	before := fileDigest(t, databasePath)
	stateDir := filepath.Join(directory, "state")
	if err := os.Mkdir(stateDir, 0o700); err != nil {
		t.Fatalf("create state directory: %v", err)
	}
	statePath := filepath.Join(stateDir, "state.json")
	marker := []byte("leave this state untouched\n")
	if err := os.WriteFile(statePath, marker, 0o600); err != nil {
		t.Fatalf("create state marker: %v", err)
	}

	command := exec.Command(buildCLI(t), "--state-dir", stateDir, "--db", databasePath)
	command.Dir = t.TempDir()
	output, err := command.CombinedOutput()
	if exitError, ok := err.(*exec.ExitError); !ok || exitError.ExitCode() != 1 {
		t.Fatalf("CLI exit = %v, want code 1; output:\n%s", err, output)
	}
	if !strings.Contains(strings.ToLower(string(output)), "metrics publication is not implemented") {
		t.Fatalf("CLI did not report the unpublished runtime boundary:\n%s", output)
	}
	if got, err := os.ReadFile(statePath); err != nil || !reflect.DeepEqual(got, marker) {
		t.Fatalf("CLI changed existing state marker: contents=%q err=%v", got, err)
	}
	if got := fileDigest(t, databasePath); got != before {
		t.Fatal("CLI changed the read-only SQLite source")
	}
	if entries := directoryEntries(t, stateDir); !reflect.DeepEqual(entries, []string{"collector.lock", "state.json"}) {
		t.Fatalf("CLI created unexpected state files: %v", entries)
	}
	if entries := directoryEntries(t, directory); strings.Contains(strings.Join(entries, " "), ".prom") {
		t.Fatalf("CLI unexpectedly published a textfile metric: %v", entries)
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
		{name: "empty state directory", args: []string{"--state-dir="}},
		{name: "relative state directory", args: []string{"--state-dir=relative-state"}},
		{name: "root state directory", args: []string{"--state-dir=/"}},
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

func TestCLIRejectsDatabasePathsThatOverlapState(t *testing.T) {
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
			output, code := runCLI(t, "--state-dir", stateDir, "--db", test.path)
			if code != 2 {
				t.Fatalf("CLI exit code = %d, want 2; output:\n%s", code, output)
			}
			if _, err := os.Lstat(stateDir); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("invalid overlapping paths created state directory: %v", err)
			}
		})
	}
}

func TestCLIRejectsDatabaseAtStateFileWithoutChangingSource(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	sourcePath := filepath.Join(stateDir, "state.json")
	createSQLite(t, sourcePath, "CREATE TABLE source_fixture (value TEXT)")
	before := fileDigest(t, sourcePath)

	command := exec.Command(buildCLI(t), "--state-dir", stateDir, "--db", sourcePath)
	output, err := command.CombinedOutput()
	if exitError, ok := err.(*exec.ExitError); !ok || exitError.ExitCode() != 2 {
		t.Fatalf("CLI exit error = %v, want exit code 2; output:\n%s", err, output)
	}
	if len(output) == 0 {
		t.Fatal("CLI returned no path-collision diagnostic")
	}
	if after := fileDigest(t, sourcePath); after != before {
		t.Fatal("CLI changed the SQLite source at the state-file path")
	}
	entries, err := os.ReadDir(stateDir)
	if err != nil {
		t.Fatalf("read state directory: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "state.json" {
		t.Fatalf("CLI created state files beside the source database: %v", entries)
	}
}

func TestCLIRejectsRunWhileAnotherProcessHoldsStateLock(t *testing.T) {
	stateDir := privateStateDirectory(t)
	child := startLockChild(t, stateDir, "hold", 8*time.Second)
	child.expectLine(t, "READY")
	missingDatabase := filepath.Join(t.TempDir(), "missing.db")

	output, code := runCLI(t, "--state-dir", stateDir, "--db", missingDatabase)
	if code != 1 {
		t.Fatalf("locked CLI exit code = %d; want 1; output:\n%s", code, output)
	}
	if !strings.Contains(strings.ToLower(output), "locked") {
		t.Fatalf("locked CLI diagnostic missing; output:\n%s", output)
	}
	if _, err := os.Lstat(missingDatabase); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("locked CLI unexpectedly touched the missing database: %v", err)
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
	// Isolate tests from the host default; later caller flags may override it.
	stateDir := filepath.Join(t.TempDir(), "state")
	commandArgs := append([]string{"--state-dir", stateDir}, args...)
	command := exec.Command(buildCLI(t), commandArgs...)
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
