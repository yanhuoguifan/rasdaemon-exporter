package collector_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"rasdaemon-exporter/internal/collector"

	_ "github.com/mattn/go-sqlite3"
)

var (
	cliBuildOnce sync.Once
	cliBinary    string
	cliBuildErr  error
	cliTempDir   string
)

// validatedConfig builds the same validated input that the CLI passes to Run.
func validatedConfig(t *testing.T, databasePath string) collector.Config {
	t.Helper()
	cfg := collector.Config{
		DatabasePath: databasePath,
		StateDir:     filepath.Join(t.TempDir(), "state"),
		SnapshotPath: filepath.Join(filepath.Dir(databasePath), "metrics.prom"),
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate test configuration: %v", err)
	}
	return cfg
}

// TestMain keeps one compiled command available to subprocess tests and removes
// only the temporary directory created for that command.
func TestMain(m *testing.M) {
	var err error
	cliTempDir, err = os.MkdirTemp("", "rasdaemon-exporter-cli-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "create CLI test directory:", err)
		os.Exit(1)
	}

	code := m.Run()
	if err := os.RemoveAll(cliTempDir); err != nil {
		fmt.Fprintln(os.Stderr, "remove CLI test directory:", err)
		if code == 0 {
			code = 1
		}
	}
	os.Exit(code)
}

// moduleRoot locates the module from this test source instead of depending on
// the working directory chosen by the go test runner.
func moduleRoot(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate test source")
	}
	return filepath.Dir(filepath.Dir(source))
}

// createSQLite creates a disposable database using the same registered driver
// used by the application. Callers pass only synthetic schema statements.
func createSQLite(t *testing.T, path string, statements ...string) {
	t.Helper()
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open synthetic SQLite database: %v", err)
	}
	db.SetMaxOpenConns(1)
	defer db.Close()

	ctx := context.Background()
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			t.Fatalf("execute synthetic SQLite schema: %v", err)
		}
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("verify synthetic SQLite database: %v", err)
	}
}

func fileDigest(t *testing.T, path string) [sha256.Size]byte {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read synthetic database: %v", err)
	}
	return sha256.Sum256(contents)
}

func buildCLI(t *testing.T) string {
	t.Helper()
	cliBuildOnce.Do(func() {
		cliBinary = filepath.Join(cliTempDir, "rasdaemon-exporter")
		command := exec.Command("go", "build", "-buildvcs=false", "-o", cliBinary, ".")
		command.Dir = moduleRoot(t)
		output, err := command.CombinedOutput()
		if err != nil {
			cliBuildErr = fmt.Errorf("build CLI: %w\n%s", err, output)
		}
	})
	if cliBuildErr != nil {
		t.Fatal(cliBuildErr)
	}
	return cliBinary
}
