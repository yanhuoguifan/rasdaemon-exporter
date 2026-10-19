package collector

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// ErrLocked means the state directory already has a lock holder.
var ErrLocked = errors.New("collector state directory is already locked")

// prepareStateDir creates only the final directory and accepts only a private,
// caller-owned real directory; existing permissions are never changed.
func prepareStateDir(path string) (string, error) {
	path = filepath.Clean(path)
	if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return "", fmt.Errorf("create state directory %q: %w", path, err)
	}

	info, err := os.Lstat(path)
	if err != nil {
		return "", fmt.Errorf("inspect state directory %q: %w", path, err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("state path is not a real directory: %q", path)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return "", fmt.Errorf("state directory %q must be owned by the effective user", path)
	}
	if info.Mode().Perm() != 0o700 {
		return "", fmt.Errorf("state directory %q must have mode 0700", path)
	}
	return path, nil
}

// AcquireLock requires an existing, trusted state directory prepared by the caller.
// Close the returned file to release the lock; process exit also releases it.
// Never remove or replace the lock file while held: flock protects its inode.
func AcquireLock(stateDir string) (*os.File, error) {
	if !filepath.IsAbs(stateDir) || filepath.Clean(stateDir) == string(filepath.Separator) {
		return nil, errors.New("state directory must be an absolute non-root path")
	}
	path := filepath.Join(stateDir, "collector.lock")
	// OpenFile sets close-on-exec. NONBLOCK prevents a FIFO from hanging startup;
	// NOFOLLOW rejects a symbolic link at the lock file, not in ancestor paths.
	flags := os.O_RDWR | os.O_CREATE | syscall.O_NOFOLLOW | syscall.O_NONBLOCK
	file, err := os.OpenFile(path, flags, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open collector lock %q: %w", path, err)
	}

	info, err := file.Stat()
	if err == nil && (!info.Mode().IsRegular() || info.Sys().(*syscall.Stat_t).Nlink != 1) {
		err = errors.New("lock file must be a regular, singly linked file")
	}
	if err == nil {
		err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	}
	if err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			err = ErrLocked
		}
		// Do not leak a descriptor when inspection or nonblocking locking fails.
		closeErr := file.Close()
		return nil, errors.Join(fmt.Errorf("acquire collector lock %q: %w", path, err), closeErr)
	}
	return file, nil
}
