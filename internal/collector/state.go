package collector

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path/filepath"
)

const (
	stateFileName = "state.json"
	maxStateSize  = 2 << 20
)

// State records the cursors paired with one published metrics snapshot.
// Callers supply consumed IDs; this layer never queries the source database.
type State struct {
	Generation uint64           `json:"generation"`
	Source     SourceIdentity   `json:"source"`
	Tables     map[string]int64 `json:"tables"`
}

// SourceIdentity binds progress to a database file, not a rasdaemon version.
// The caller obtains this identity; state persistence does not inspect the source.
type SourceIdentity struct {
	Path   string `json:"path"`
	Device uint64 `json:"device"`
	Inode  uint64 `json:"inode"`
}

// ErrStateDurabilityUncertain means the new state name is visible, but its
// directory entry could not be confirmed durable. The caller must stop.
var ErrStateDurabilityUncertain = errFileDurabilityUncertain

// validateState checks durable invariants without deriving values from SQL.
func validateState(s *State) error {
	if s == nil {
		return errors.New("state must not be nil")
	}
	if s.Generation == 0 {
		return errors.New("state generation must be greater than zero")
	}
	if !filepath.IsAbs(s.Source.Path) {
		return errors.New("state source path must be absolute")
	}
	if s.Source.Inode == 0 {
		return errors.New("state source inode must be greater than zero")
	}
	if len(s.Tables) == 0 {
		return errors.New("state must contain at least one active table")
	}
	for name, lastID := range s.Tables {
		if name == "" {
			return errors.New("state table name must not be empty")
		}
		if lastID < 0 {
			return fmt.Errorf("state table %q has a negative last ID", name)
		}
	}
	return nil
}

func decodeState(contents []byte) (*State, error) {
	var state State
	if err := json.Unmarshal(contents, &state); err != nil {
		return nil, fmt.Errorf("decode state JSON: %w", err)
	}
	if err := validateState(&state); err != nil {
		return nil, err
	}
	return &state, nil
}

// Advance returns a candidate with the supplied consumed IDs and next generation.
// It leaves the committed state unchanged; callers persist the candidate only
// when its matching metrics snapshot is ready to publish.
func (s *State) Advance(cursors map[string]int64) (*State, error) {
	if err := validateState(s); err != nil {
		return nil, err
	}
	if s.Generation == math.MaxUint64 {
		return nil, errors.New("state generation exhausted")
	}
	next := &State{
		Generation: s.Generation + 1,
		Source:     s.Source,
		Tables:     make(map[string]int64, len(s.Tables)),
	}
	for table, id := range s.Tables {
		next.Tables[table] = id
	}
	for table, id := range cursors {
		previous, exists := s.Tables[table]
		if !exists {
			return nil, fmt.Errorf("cannot advance unregistered table %q", table)
		}
		if id < previous {
			return nil, fmt.Errorf("cursor for table %q moved backwards: %d to %d", table, previous, id)
		}
		next.Tables[table] = id
	}
	return next, nil
}

// Check compares caller-provided database identity and snapshot generation.
// A metrics snapshot exactly one generation ahead means recovery must rebuild
// retained history. The caller owns that recovery and reuses the pending generation.
func (s *State) Check(source SourceIdentity, snapshotGeneration uint64) (needsRebuild bool, err error) {
	if err := validateState(s); err != nil {
		return false, err
	}
	if source != s.Source {
		return false, errors.New("source database identity does not match state")
	}
	if snapshotGeneration == s.Generation {
		return false, nil
	}
	// Subtract only after comparing, so uint64 overflow cannot look like a match.
	if snapshotGeneration > s.Generation && snapshotGeneration-s.Generation == 1 {
		return true, nil
	}
	return false, fmt.Errorf("snapshot generation %d does not match state generation %d", snapshotGeneration, s.Generation)
}

// LoadState reads and validates the durable cursor snapshot while the caller
// holds the state-directory lock. Missing files remain distinguishable.
func LoadState(stateDir string) (*State, error) {
	path, err := stateFilePath(stateDir)
	if err != nil {
		return nil, err
	}
	contents, err := readRegularFile(path, maxStateSize)
	if err != nil {
		return nil, fmt.Errorf("read state file %q: %w", path, err)
	}
	state, err := decodeState(contents)
	if err != nil {
		return nil, fmt.Errorf("load state file %q: %w", path, err)
	}
	return state, nil
}

// SaveState atomically replaces the state snapshot. The caller validates runtime
// paths, holds the directory lock and coordinates generation with publication.
func SaveState(stateDir string, state *State) error {
	if err := validateState(state); err != nil {
		return fmt.Errorf("validate state: %w", err)
	}
	path, err := stateFilePath(stateDir)
	if err != nil {
		return err
	}
	contents, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("encode state JSON: %w", err)
	}
	contents = append(contents, '\n')
	if len(contents) > maxStateSize {
		return fmt.Errorf("state JSON exceeds %d bytes", maxStateSize)
	}
	if err := atomicWriteFile(path, contents, 0o600); err != nil {
		return fmt.Errorf("save state file %q: %w", path, err)
	}
	return nil
}

func stateFilePath(directory string) (string, error) {
	cleanPath := filepath.Clean(directory)
	if !filepath.IsAbs(cleanPath) || cleanPath == string(filepath.Separator) {
		return "", errors.New("state directory must be an absolute non-root path")
	}
	return filepath.Join(cleanPath, stateFileName), nil
}
