package collector_test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"rasdaemon-exporter/internal/collector"
)

const (
	lockHelperModeEnv = "RASDAEMON_EXPORTER_LOCK_TEST_MODE"
	lockHelperDirEnv  = "RASDAEMON_EXPORTER_LOCK_TEST_DIR"
)

// TestLockProcessHelper is re-executed as a child test process. It avoids
// timing-based synchronization by announcing readiness over stdout and then
// waiting for an explicit command over stdin.
func TestLockProcessHelper(t *testing.T) {
	mode := os.Getenv(lockHelperModeEnv)
	if mode == "" {
		return
	}
	directory := os.Getenv(lockHelperDirEnv)
	if directory == "" {
		t.Fatal("lock helper state directory is missing")
	}

	switch mode {
	case "hold":
		lock, err := collector.AcquireLock(directory)
		if err != nil {
			t.Fatalf("child AcquireLock(): %v", err)
		}
		defer lock.Close()
		fmt.Fprintln(os.Stdout, "READY")
		command, err := bufio.NewReader(os.Stdin).ReadString('\n')
		if err != nil {
			t.Fatalf("read parent release command: %v", err)
		}
		if strings.TrimSpace(command) != "release" {
			t.Fatalf("unexpected parent command %q", command)
		}
		if err := lock.Close(); err != nil {
			t.Fatalf("child Close(): %v", err)
		}
		fmt.Fprintln(os.Stdout, "RELEASED")
	case "try":
		lock, err := collector.AcquireLock(directory)
		if err != nil {
			fmt.Fprintln(os.Stdout, "REJECTED")
			return
		}
		_ = lock.Close()
		fmt.Fprintln(os.Stdout, "ACQUIRED")
	default:
		t.Fatalf("unknown lock helper mode %q", mode)
	}
}

func TestLockExcludesAnotherProcessAndAllowsGracefulReacquire(t *testing.T) {
	directory := privateStateDirectory(t)
	child := startLockChild(t, directory, "hold", 8*time.Second)
	child.expectLine(t, "READY")

	lock, err := collector.AcquireLock(directory)
	if lock != nil {
		_ = lock.Close()
		t.Fatal("parent unexpectedly acquired a lock held by a child process")
	}
	if !errors.Is(err, collector.ErrLocked) {
		t.Fatalf("parent AcquireLock() error = %v; want ErrLocked", err)
	}

	if _, err := io.WriteString(child.stdin, "release\n"); err != nil {
		t.Fatalf("send child release command: %v", err)
	}
	child.expectLine(t, "RELEASED")
	child.wait(t)

	lock, err = collector.AcquireLock(directory)
	if err != nil {
		t.Fatalf("acquire after child closed cleanly: %v", err)
	}
	if err := lock.Close(); err != nil {
		t.Fatalf("release reacquired lock: %v", err)
	}
}

func TestKernelReleasesLockAfterHolderProcessIsKilled(t *testing.T) {
	directory := privateStateDirectory(t)
	child := startLockChild(t, directory, "hold", 8*time.Second)
	child.expectLine(t, "READY")

	lock, err := collector.AcquireLock(directory)
	if lock != nil {
		_ = lock.Close()
		t.Fatal("parent unexpectedly acquired a lock held by a child process")
	}
	if !errors.Is(err, collector.ErrLocked) {
		t.Fatalf("parent AcquireLock() error = %v; want ErrLocked", err)
	}

	if err := child.command.Process.Kill(); err != nil {
		t.Fatalf("kill lock-holding child: %v", err)
	}
	if err := child.command.Wait(); err == nil {
		t.Fatal("killed child unexpectedly exited successfully")
	}
	child.waited = true

	lock, err = collector.AcquireLock(directory)
	if err != nil {
		t.Fatalf("kernel did not release lock after holder death: %v", err)
	}
	if err := lock.Close(); err != nil {
		t.Fatalf("release lock acquired after child death: %v", err)
	}
}

func TestAcquireLockRejectsFIFOWithoutBlocking(t *testing.T) {
	directory := privateStateDirectory(t)
	fifo := directory + "/collector.lock"
	if err := makeFIFO(fifo); err != nil {
		t.Fatalf("create FIFO lock path: %v", err)
	}

	child := startLockChild(t, directory, "try", 2*time.Second)
	child.expectLine(t, "REJECTED")
	child.wait(t)
}

type lockChild struct {
	command *exec.Cmd
	stdin   io.WriteCloser
	stdout  *bufio.Reader
	stderr  bytes.Buffer
	waited  bool
	cancel  context.CancelFunc
}

func startLockChild(t *testing.T, directory, mode string, timeout time.Duration) *lockChild {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestLockProcessHelper$")
	stdin, err := command.StdinPipe()
	if err != nil {
		cancel()
		t.Fatalf("create child stdin: %v", err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		cancel()
		t.Fatalf("create child stdout: %v", err)
	}
	child := &lockChild{
		command: command,
		stdin:   stdin,
		stdout:  bufio.NewReader(stdout),
		cancel:  cancel,
	}
	command.Stderr = &child.stderr
	command.Env = lockHelperEnvironment(os.Environ(), directory, mode, t.TempDir())
	if err := command.Start(); err != nil {
		_ = child.stdin.Close()
		_ = stdout.Close()
		cancel()
		t.Fatalf("start lock helper process: %v", err)
	}
	t.Cleanup(func() {
		_ = child.stdin.Close()
		if !child.waited {
			_ = child.command.Process.Kill()
			_ = child.command.Wait()
			child.waited = true
		}
		child.cancel()
	})
	return child
}

func (child *lockChild) expectLine(t *testing.T, want string) {
	t.Helper()
	line, err := child.stdout.ReadString('\n')
	if err != nil {
		t.Fatalf("read child handshake: %v", err)
	}
	if got := strings.TrimSpace(line); got != want {
		t.Fatalf("child handshake = %q; want %q", got, want)
	}
}

func (child *lockChild) wait(t *testing.T) {
	t.Helper()
	if err := child.stdin.Close(); err != nil {
		t.Fatalf("close child stdin: %v", err)
	}
	if err := child.command.Wait(); err != nil {
		t.Fatalf("wait for child process: %v; stderr: %s", err, child.stderr.String())
	}
	child.waited = true
}

func lockHelperEnvironment(environment []string, directory, mode, tempDir string) []string {
	filtered := make([]string, 0, len(environment)+3)
	for _, entry := range environment {
		if strings.HasPrefix(entry, "TMPDIR=") || strings.HasPrefix(entry, lockHelperModeEnv+"=") ||
			strings.HasPrefix(entry, lockHelperDirEnv+"=") {
			continue
		}
		filtered = append(filtered, entry)
	}
	return append(filtered,
		"TMPDIR="+tempDir,
		lockHelperModeEnv+"="+mode,
		lockHelperDirEnv+"="+directory,
	)
}

func makeFIFO(path string) error {
	return syscall.Mkfifo(path, 0o600)
}
