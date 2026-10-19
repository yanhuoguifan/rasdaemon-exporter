package collector

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

// ErrLocked reports that the requested file already has a lock holder.
var ErrLocked = errors.New("file is already locked")

// errFileDurabilityUncertain means replacement is visible but directory sync failed.
var errFileDurabilityUncertain = errors.New("file replacement durability is uncertain")

// preparePrivateDir creates the final directory without changing existing modes.
func preparePrivateDir(path string) (string, error) {
	path = filepath.Clean(path)
	if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return "", fmt.Errorf("create private directory %q: %w", path, err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		return "", fmt.Errorf("inspect private directory %q: %w", path, err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !info.IsDir() || !ok || stat.Uid != uint32(os.Geteuid()) || info.Mode().Perm() != 0o700 {
		return "", fmt.Errorf("path %q must be a mode-0700 directory owned by the effective user", path)
	}
	return path, nil
}

// acquireFileLock opens and locks a caller-selected path without truncating it.
// Keep the returned file open for the lock lifetime; never unlink the path.
func acquireFileLock(path string) (*os.File, error) {
	flags := os.O_RDWR | os.O_CREATE | syscall.O_NOFOLLOW | syscall.O_NONBLOCK
	file, err := os.OpenFile(path, flags, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open lock file %q: %w", path, err)
	}
	info, err := file.Stat()
	if err == nil {
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !info.Mode().IsRegular() || !ok || stat.Nlink != 1 {
			err = errors.New("lock path must be a singly linked regular file")
		}
	}
	if err == nil {
		err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	}
	if err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			err = ErrLocked
		}
		return nil, errors.Join(fmt.Errorf("acquire lock file %q: %w", path, err), file.Close())
	}
	return file, nil
}

// readRegularFile rejects symbolic links and special files. A nonnegative limit
// bounds the read; a negative limit reads the complete file.
func readRegularFile(path string, limit int64) ([]byte, error) {
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, fmt.Errorf("open file %q: %w", path, err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("inspect file %q: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("path %q must be a regular file", path)
	}
	var reader io.Reader = file
	if limit >= 0 {
		reader = io.LimitReader(file, limit+1)
	}
	contents, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("read file %q: %w", path, err)
	}
	if limit >= 0 && int64(len(contents)) > limit {
		return nil, fmt.Errorf("file %q exceeds %d bytes", path, limit)
	}
	return contents, nil
}

// atomicWriteFile replaces a regular file in a trusted directory held by the caller.
// A same-directory temporary file keeps readers on complete old or new contents.
func atomicWriteFile(path string, contents []byte, mode os.FileMode) error {
	directoryPath := filepath.Dir(path)
	directory, err := os.Open(directoryPath)
	if err != nil {
		return fmt.Errorf("open parent directory %q: %w", directoryPath, err)
	}
	defer directory.Close()

	info, err := os.Lstat(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect file target %q: %w", path, err)
	}
	if err == nil && !info.Mode().IsRegular() {
		return fmt.Errorf("file target %q must be a regular file", path)
	}

	temporary, err := os.CreateTemp(directoryPath, ".write-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary file in %q: %w", directoryPath, err)
	}
	// Best-effort cleanup does not replace the primary error. After rename,
	// this temporary name no longer exists; the destination is never removed.
	defer os.Remove(temporary.Name())
	defer temporary.Close()

	if _, err := temporary.Write(contents); err != nil {
		return fmt.Errorf("write temporary file: %w", err)
	}
	if err := temporary.Chmod(mode.Perm()); err != nil {
		return fmt.Errorf("set temporary file permissions: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync temporary file: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary file: %w", err)
	}
	if err := os.Rename(temporary.Name(), path); err != nil {
		return fmt.Errorf("replace file %q: %w", path, err)
	}
	// Replacement is already visible. A sync failure must not imply rollback.
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("%w: %q: %w", errFileDurabilityUncertain, path, err)
	}
	return nil
}
