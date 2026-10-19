package collector_test

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
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
		"--output", collector.DefaultSnapshotPath,
		"--interval", "5s",
	} {
		if !strings.Contains(strings.ToLower(output), strings.ToLower(expected)) {
			t.Errorf("--help output does not contain %q:\n%s", expected, output)
		}
	}
}

func TestCLICollectsAndPublishesDevlinkEvents(t *testing.T) {
	for _, test := range []struct {
		name   string
		signal os.Signal
	}{
		{name: "SIGINT", signal: os.Interrupt},
		{name: "SIGTERM", signal: syscall.SIGTERM},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			databasePath := filepath.Join(directory, "events.db")
			stateDir := filepath.Join(directory, "state")
			outputPath := filepath.Join(directory, "metrics.prom")
			createDevlinkDatabase(t, databasePath, recoveryEvent(1, "first event"))

			command, stdout, stderr := startRuntimeCLI(t, databasePath, stateDir, outputPath, "1h")
			waitForCommitted(t, stateDir, outputPath, 2, 1, map[collector.DevlinkLabels]uint64{
				recoveryLabels("first event"): 1,
			})
			stopCLI(t, command, stdout, stderr, test.signal)
			logs := stderr.String()
			if strings.Count(logs, "Starting collection loop") != 1 {
				t.Fatalf("startup log should appear once; stderr:\n%s", logs)
			}
			for _, expected := range []string{databasePath, outputPath, "interval=1h"} {
				if !strings.Contains(logs, expected) {
					t.Errorf("startup log does not contain %q; stderr:\n%s", expected, logs)
				}
			}
			assertStateLockReleased(t, stateDir)
		})
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
		{name: "zero interval", args: []string{"--interval=0"}},
		{name: "negative interval", args: []string{"--interval=-1s"}},
		{name: "invalid interval", args: []string{"--interval=soon"}},
		{name: "relative output", args: []string{"--output=relative.prom"}},
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

func TestCLIInvalidRuntimeOptionsDoNotCreateFiles(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
	}{
		{name: "zero interval", args: []string{"--interval=0"}},
		{name: "negative interval", args: []string{"--interval=-1s"}},
		{name: "invalid interval", args: []string{"--interval=soon"}},
		{name: "relative output", args: []string{"--output=relative.prom"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			stateDir := filepath.Join(directory, "state")
			outputPath := filepath.Join(directory, "metrics.prom")
			databasePath := filepath.Join(directory, "missing.db")
			args := []string{"--db", databasePath, "--state-dir", stateDir, "--output", outputPath}
			args = append(args, test.args...)
			command := exec.Command(buildCLI(t), args...)
			command.Dir = directory
			output, err := command.CombinedOutput()
			if exitError, ok := err.(*exec.ExitError); !ok || exitError.ExitCode() != 2 {
				t.Fatalf("CLI exit = %v, want 2; output:\n%s", err, output)
			}
			for _, path := range []string{stateDir, outputPath, databasePath, filepath.Join(directory, "relative.prom")} {
				if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
					t.Errorf("invalid configuration created %q: %v", path, err)
				}
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
	directory := t.TempDir()
	stateDir := filepath.Join(directory, "state")
	if err := os.Mkdir(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	sourcePath := filepath.Join(stateDir, "state.json")
	createSQLite(t, sourcePath, "CREATE TABLE source_fixture (value TEXT)")
	before := fileDigest(t, sourcePath)

	command := exec.Command(buildCLI(t), "--state-dir", stateDir,
		"--output", filepath.Join(directory, "metrics.prom"), "--db", sourcePath)
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
	// Override every writable default before caller flags, keeping subprocesses
	// isolated even when they fail after configuration parsing.
	directory := t.TempDir()
	stateDir := filepath.Join(directory, "state")
	outputPath := filepath.Join(directory, "metrics.prom")
	commandArgs := append([]string{"--state-dir", stateDir, "--output", outputPath}, args...)
	command := exec.Command(buildCLI(t), commandArgs...)
	command.Dir = directory
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

func startRuntimeCLI(t *testing.T, databasePath, stateDir, outputPath, interval string) (*exec.Cmd, *bytes.Buffer, *bytes.Buffer) {
	t.Helper()
	command := exec.Command(buildCLI(t),
		"--db", databasePath,
		"--state-dir", stateDir,
		"--output", outputPath,
		"--interval", interval,
	)
	command.Dir = t.TempDir()
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatalf("start CLI: %v", err)
	}
	t.Cleanup(func() {
		if command.ProcessState == nil {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	})
	return command, &stdout, &stderr
}

func stopCLI(t *testing.T, command *exec.Cmd, stdout, stderr *bytes.Buffer, signal os.Signal) {
	t.Helper()
	if err := command.Process.Signal(signal); err != nil {
		t.Fatalf("send %s to CLI: %v", signal, err)
	}
	waited := make(chan error, 1)
	go func() { waited <- command.Wait() }()
	select {
	case err := <-waited:
		if err != nil {
			t.Fatalf("CLI exit = %v; stdout:\n%s\nstderr:\n%s", err, stdout, stderr)
		}
	case <-time.After(3 * time.Second):
		_ = command.Process.Kill()
		<-waited
		t.Fatalf("CLI did not stop after %s; stdout:\n%s\nstderr:\n%s", signal, stdout, stderr)
	}
}
