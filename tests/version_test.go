package collector_test

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestCLIVersionExitsWithoutRuntimeConfiguration(t *testing.T) {
	directory := t.TempDir()
	database := filepath.Join(directory, "missing.db")
	state := filepath.Join(directory, "state")
	output := filepath.Join(directory, "events.prom")
	command := exec.Command(buildCLI(t), "--version", "--db", database,
		"--state-dir", state, "--output", output, "--interval=0s")
	command.Dir = directory
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		t.Fatalf("--version exited with error: %v; stderr: %s", err, stderr.String())
	}
	if stdout.String() != "rasdaemon-exporter dev\n" {
		t.Fatalf("--version stdout = %q, want the development version line", stdout.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("--version wrote diagnostics to stderr: %s", stderr.String())
	}
	for _, path := range []string{database, state, output, filepath.Join(directory, "events.health.prom")} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("--version touched runtime path %q: %v", path, err)
		}
	}
}

func TestCLIVersionPrintsInjectedReleaseVersion(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "rasdaemon-exporter")
	const testVersion = "v9.8.7-test"
	command := exec.Command("go", "build", "-buildvcs=false", "-ldflags", "-X main.version="+testVersion,
		"-o", binary, ".")
	command.Dir = moduleRoot(t)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build versioned CLI: %v\n%s", err, output)
	}
	command = exec.Command(binary, "--version")
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		t.Fatalf("versioned --version exited with error: %v; stderr: %s", err, stderr.String())
	}
	if stdout.String() != "rasdaemon-exporter "+testVersion+"\n" || stderr.Len() != 0 {
		t.Fatalf("versioned output = (%q, %q), want version line on stdout only", stdout.String(), stderr.String())
	}
}

func TestCLIRejectsInvalidVersionArguments(t *testing.T) {
	for _, args := range [][]string{
		{"--version", "unexpected"},
		{"--version", "--unknown"},
		{"--version", "--interval=soon"},
	} {
		output, code := runCLI(t, args...)
		if code != 2 {
			t.Errorf("arguments %v exited %d, want 2; output: %s", args, code, output)
		}
	}
}
