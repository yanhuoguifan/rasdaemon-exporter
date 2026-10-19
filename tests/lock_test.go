package collector_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"rasdaemon-exporter/internal/collector"
)

func TestAcquireLockCreatesPersistentPrivateFile(t *testing.T) {
	directory := privateStateDirectory(t)
	lock, err := collector.AcquireLock(directory)
	if err != nil {
		t.Fatalf("AcquireLock() error = %v", err)
	}
	defer lock.Close()

	path := filepath.Join(directory, "collector.lock")
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("stat lock file: %v", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		t.Fatalf("lock file mode = %v; want a regular 0600 file", info.Mode())
	}
	if info.Mode()&os.ModeSymlink != 0 {
		t.Fatal("lock file must not be a symbolic link")
	}
}

func TestAcquireLockExcludesIndependentOpensInSameProcess(t *testing.T) {
	directory := privateStateDirectory(t)
	first, err := collector.AcquireLock(directory)
	if err != nil {
		t.Fatalf("acquire first lock: %v", err)
	}

	second, err := collector.AcquireLock(directory)
	if second != nil {
		_ = second.Close()
		t.Fatal("second independent open unexpectedly acquired the lock")
	}
	if !errors.Is(err, collector.ErrLocked) {
		t.Fatalf("second AcquireLock() error = %v; want ErrLocked", err)
	}

	if err := first.Close(); err != nil {
		t.Fatalf("release first lock: %v", err)
	}
	second, err = collector.AcquireLock(directory)
	if err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatalf("release reacquired lock: %v", err)
	}
}

func TestLockFileIsNotTruncatedOrUnlinked(t *testing.T) {
	directory := privateStateDirectory(t)
	path := filepath.Join(directory, "collector.lock")
	const contents = "persistent lock metadata is not owned by the exporter\n"
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("prepare existing lock file: %v", err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat existing lock file: %v", err)
	}

	lock, err := collector.AcquireLock(directory)
	if err != nil {
		t.Fatalf("AcquireLock() error = %v", err)
	}
	if err := lock.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	after, err := os.Stat(path)
	if err != nil {
		t.Fatalf("lock file was removed on Close(): %v", err)
	}
	if !os.SameFile(before, after) {
		t.Fatal("lock file inode changed; acquisition or Close() replaced the persistent file")
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read persistent lock file: %v", err)
	}
	if string(got) != contents {
		t.Fatalf("lock contents changed: got %q, want %q", got, contents)
	}
}

func TestAcquireLockValidatesBasicStateDirectoryPaths(t *testing.T) {
	parent := t.TempDir()
	private := filepath.Join(parent, "private")
	if err := os.Mkdir(private, 0o700); err != nil {
		t.Fatalf("create private directory: %v", err)
	}
	if err := os.Chmod(private, 0o700); err != nil {
		t.Fatalf("set private directory mode: %v", err)
	}
	link := filepath.Join(parent, "directory-link")
	if err := os.Symlink(private, link); err != nil {
		t.Fatalf("create state-directory symlink: %v", err)
	}
	trailingLink := link + string(filepath.Separator)

	missing := filepath.Join(parent, "missing")
	cases := []struct {
		name string
		path string
	}{
		{name: "empty", path: ""},
		{name: "relative", path: "relative-state"},
		{name: "root", path: string(filepath.Separator)},
		{name: "missing", path: missing},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			lock, err := collector.AcquireLock(test.path)
			if lock != nil {
				_ = lock.Close()
				t.Fatal("AcquireLock() unexpectedly returned a lock")
			}
			if err == nil {
				t.Fatal("AcquireLock() unexpectedly accepted an unsafe directory")
			}
		})
	}
	if _, err := os.Lstat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("AcquireLock() created the missing state directory: %v", err)
	}
	// The caller supplies a trusted existing directory. Lock acquisition does
	// not impose ownership, permission, or directory-symlink policy on it.
	for name, path := range map[string]string{"directory symlink": link, "trailing symlink": trailingLink} {
		t.Run(name, func(t *testing.T) {
			lock, err := collector.AcquireLock(path)
			if err != nil {
				t.Fatalf("AcquireLock() for caller-trusted path: %v", err)
			}
			if err := lock.Close(); err != nil {
				t.Fatalf("release lock: %v", err)
			}
		})
	}
}

func TestAcquireLockDoesNotRequirePrivateDirectoryMode(t *testing.T) {
	parent := t.TempDir()
	shared := filepath.Join(parent, "shared")
	if err := os.Mkdir(shared, 0o755); err != nil {
		t.Fatalf("create shared directory: %v", err)
	}
	lock, err := collector.AcquireLock(shared)
	if err != nil {
		t.Fatalf("AcquireLock() checked caller-owned directory policy: %v", err)
	}
	if err := lock.Close(); err != nil {
		t.Fatalf("release lock: %v", err)
	}
}

func TestAcquireLockRejectsUnsafeExistingLockFiles(t *testing.T) {
	t.Run("symbolic link", func(t *testing.T) {
		directory := privateStateDirectory(t)
		target := filepath.Join(directory, "target")
		if err := os.WriteFile(target, []byte("target"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, filepath.Join(directory, "collector.lock")); err != nil {
			t.Fatalf("create lock symlink: %v", err)
		}
		assertLockRejected(t, directory)
	})

	t.Run("hard link", func(t *testing.T) {
		directory := privateStateDirectory(t)
		lockPath := filepath.Join(directory, "collector.lock")
		otherPath := filepath.Join(directory, "other")
		if err := os.WriteFile(lockPath, []byte("shared"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(lockPath, otherPath); err != nil {
			t.Fatalf("create hard link: %v", err)
		}
		assertLockRejected(t, directory)
	})

	t.Run("non-regular file", func(t *testing.T) {
		directory := privateStateDirectory(t)
		if err := os.Mkdir(filepath.Join(directory, "collector.lock"), 0o700); err != nil {
			t.Fatalf("create directory at lock path: %v", err)
		}
		assertLockRejected(t, directory)
	})
}

func privateStateDirectory(t *testing.T) string {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatalf("set private state-directory mode: %v", err)
	}
	return directory
}

func assertLockRejected(t *testing.T, directory string) {
	t.Helper()
	lock, err := collector.AcquireLock(directory)
	if lock != nil {
		_ = lock.Close()
		t.Fatal("AcquireLock() unexpectedly accepted an unsafe lock file")
	}
	if err == nil {
		t.Fatal("AcquireLock() unexpectedly accepted an unsafe lock file")
	}
}
