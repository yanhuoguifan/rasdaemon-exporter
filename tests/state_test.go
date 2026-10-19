package collector_test

import (
	"bytes"
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"syscall"
	"testing"

	"rasdaemon-exporter/internal/collector"
)

func TestLoadStateReportsMissingFile(t *testing.T) {
	directory, _ := lockedStateDirectory(t)
	state, err := collector.LoadState(directory)
	if state != nil || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("LoadState() = (%#v, %v), want (nil, os.ErrNotExist)", state, err)
	}
}

func TestStateRoundTripPreservesCursorsAndExactIntegerValues(t *testing.T) {
	directory, _ := lockedStateDirectory(t)
	state := &collector.State{
		Generation: math.MaxUint64,
		Source: collector.SourceIdentity{
			Path:   filepath.Join(directory, "source that is deliberately absent.db"),
			Device: 0,
			Inode:  math.MaxUint64,
		},
		Tables: map[string]int64{
			"devlink_event": math.MaxInt64,
			"事件表":           0,
		},
	}
	if err := collector.SaveState(directory, state); err != nil {
		t.Fatalf("SaveState() error = %v", err)
	}
	got, err := collector.LoadState(directory)
	if err != nil {
		t.Fatalf("LoadState() error = %v", err)
	}
	if !reflect.DeepEqual(got, state) {
		t.Fatalf("LoadState() = %#v, want %#v", got, state)
	}
	if _, err := os.Lstat(state.Source.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("state persistence unexpectedly inspected or created source: %v", err)
	}

	info, err := os.Stat(filepath.Join(directory, "state.json"))
	if err != nil {
		t.Fatalf("stat state file: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("state file mode = %04o, want 0600", info.Mode().Perm())
	}
}

func TestSaveStateRejectsInvalidCandidateWithoutChangingCommittedFile(t *testing.T) {
	directory, _ := lockedStateDirectory(t)
	committed := validTestState()
	if err := collector.SaveState(directory, committed); err != nil {
		t.Fatalf("save initial state: %v", err)
	}
	path := filepath.Join(directory, "state.json")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read initial state: %v", err)
	}

	candidate := validTestState()
	candidate.Generation++
	candidate.Tables["devlink_event"] = -1
	if err := collector.SaveState(directory, candidate); err == nil {
		t.Fatal("SaveState() accepted a negative table cursor")
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read committed state after rejected save: %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("rejected state save changed the existing committed file")
	}
}

func TestSaveStateDoesNotMutateInputAndLeavesNoTemporaryFiles(t *testing.T) {
	directory, _ := lockedStateDirectory(t)
	state := validTestState()
	before := cloneState(state)
	if err := collector.SaveState(directory, state); err != nil {
		t.Fatalf("SaveState() error = %v", err)
	}
	if !reflect.DeepEqual(state, before) {
		t.Fatalf("SaveState() mutated input: got %#v, want %#v", state, before)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatalf("read state directory: %v", err)
	}
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	want := []string{"collector.lock", "state.json"}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("state directory entries = %v, want %v", names, want)
	}
}

func TestStateReadersObserveWholeSnapshotsDuringAtomicReplacement(t *testing.T) {
	directory, _ := lockedStateDirectory(t)
	initial := validTestState()
	initial.Generation = 1
	initial.Tables["devlink_event"] = 1
	if err := collector.SaveState(directory, initial); err != nil {
		t.Fatalf("save initial state: %v", err)
	}

	stop := make(chan struct{})
	errorsFound := make(chan error, 3)
	var readers sync.WaitGroup
	for range 3 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				state, err := collector.LoadState(directory)
				if err != nil {
					errorsFound <- err
					return
				}
				if state.Generation != uint64(state.Tables["devlink_event"]) {
					errorsFound <- errors.New("reader observed a mixed state snapshot")
					return
				}
			}
		}()
	}

	for generation := uint64(2); generation <= 80; generation++ {
		candidate := validTestState()
		candidate.Generation = generation
		candidate.Tables["devlink_event"] = int64(generation)
		if err := collector.SaveState(directory, candidate); err != nil {
			close(stop)
			readers.Wait()
			t.Fatalf("save generation %d: %v", generation, err)
		}
	}
	close(stop)
	readers.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Fatalf("atomic snapshot reader: %v", err)
	}
}

func TestLoadAndSaveRejectUnsafeStateFileTargets(t *testing.T) {
	tests := []struct {
		name string
		make func(t *testing.T, directory string, target string)
	}{
		{
			name: "symbolic link",
			make: func(t *testing.T, directory, target string) {
				external := filepath.Join(t.TempDir(), "external-state.json")
				if err := os.WriteFile(external, []byte("keep this target"), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(external, target); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "directory",
			make: func(t *testing.T, directory, target string) {
				if err := os.Mkdir(target, 0o700); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "FIFO",
			make: func(t *testing.T, directory, target string) {
				if err := syscall.Mkfifo(target, 0o600); err != nil {
					t.Fatal(err)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			directory, _ := lockedStateDirectory(t)
			target := filepath.Join(directory, "state.json")
			test.make(t, directory, target)
			beforeInfo, err := os.Lstat(target)
			if err != nil {
				t.Fatalf("inspect unsafe target before operations: %v", err)
			}
			beforeEntries := directoryEntries(t, directory)
			var beforeContents []byte
			if beforeInfo.Mode().IsRegular() || beforeInfo.Mode()&os.ModeSymlink != 0 {
				beforeContents, err = os.ReadFile(target)
				if err != nil {
					t.Fatalf("read unsafe target before operations: %v", err)
				}
			}
			var beforeLink string
			if beforeInfo.Mode()&os.ModeSymlink != 0 {
				beforeLink, err = os.Readlink(target)
				if err != nil {
					t.Fatalf("read symbolic link before operations: %v", err)
				}
			}
			if state, err := collector.LoadState(directory); err == nil || state != nil {
				t.Fatalf("LoadState() = (%#v, %v), want target rejection", state, err)
			}
			if err := collector.SaveState(directory, validTestState()); err == nil {
				t.Fatal("SaveState() accepted an unsafe state target")
			}
			afterInfo, err := os.Lstat(target)
			if err != nil {
				t.Fatalf("inspect unsafe target after operations: %v", err)
			}
			if !os.SameFile(beforeInfo, afterInfo) || beforeInfo.Mode() != afterInfo.Mode() {
				t.Fatal("unsafe state target was replaced or changed type")
			}
			if beforeInfo.Mode().IsRegular() || beforeInfo.Mode()&os.ModeSymlink != 0 {
				afterContents, err := os.ReadFile(target)
				if err != nil || !bytes.Equal(afterContents, beforeContents) {
					t.Fatalf("unsafe target contents changed: read error %v", err)
				}
			}
			if beforeInfo.Mode()&os.ModeSymlink != 0 {
				afterLink, err := os.Readlink(target)
				if err != nil || afterLink != beforeLink {
					t.Fatalf("symbolic link changed from %q to %q (error %v)", beforeLink, afterLink, err)
				}
			}
			if entries := directoryEntries(t, directory); !reflect.DeepEqual(entries, beforeEntries) {
				t.Fatalf("unsafe target operations changed directory entries: got %v, want %v", entries, beforeEntries)
			}
		})
	}
}

func TestSaveStateAtomicallyReplacesHardlinkNameOnly(t *testing.T) {
	directory, _ := lockedStateDirectory(t)
	committed := validTestState()
	if err := collector.SaveState(directory, committed); err != nil {
		t.Fatalf("save initial state: %v", err)
	}

	statePath := filepath.Join(directory, "state.json")
	otherPath := filepath.Join(directory, "other-name")
	if err := os.Link(statePath, otherPath); err != nil {
		t.Fatalf("create state hard link: %v", err)
	}
	beforeInfo, err := os.Stat(statePath)
	if err != nil {
		t.Fatalf("stat linked state before save: %v", err)
	}
	beforeContents, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatalf("read linked state before operations: %v", err)
	}

	loaded, err := collector.LoadState(directory)
	if err != nil {
		t.Fatalf("LoadState() rejected a hard-linked regular file: %v", err)
	}
	if !reflect.DeepEqual(loaded, committed) {
		t.Fatalf("LoadState() = %#v, want %#v", loaded, committed)
	}

	candidate := validTestState()
	candidate.Generation++
	if err := collector.SaveState(directory, candidate); err != nil {
		t.Fatalf("SaveState() rejected a hard-linked regular target: %v", err)
	}
	otherContents, err := os.ReadFile(otherPath)
	if err != nil || !bytes.Equal(otherContents, beforeContents) {
		t.Fatalf("other hard link changed: read error %v", err)
	}
	stateContents, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatalf("read replaced state: %v", err)
	}
	if bytes.Equal(stateContents, beforeContents) {
		t.Fatal("state path still contains the old snapshot")
	}
	loaded, err = collector.LoadState(directory)
	if err != nil {
		t.Fatalf("load replaced state: %v", err)
	}
	if !reflect.DeepEqual(loaded, candidate) {
		t.Fatalf("LoadState() = %#v, want replacement %#v", loaded, candidate)
	}
	stateInfo, err := os.Stat(statePath)
	if err != nil {
		t.Fatalf("stat state path: %v", err)
	}
	otherInfo, err := os.Stat(otherPath)
	if err != nil {
		t.Fatalf("stat linked path: %v", err)
	}
	if os.SameFile(stateInfo, otherInfo) {
		t.Fatal("state path still refers to the old linked inode")
	}
	if !os.SameFile(beforeInfo, otherInfo) {
		t.Fatal("other hard link no longer refers to the original inode")
	}
	if entries := directoryEntries(t, directory); !reflect.DeepEqual(entries, []string{"collector.lock", "other-name", "state.json"}) {
		t.Fatalf("atomic replacement left unexpected directory entries: %v", entries)
	}
}

func TestSaveStateDoesNotCreateMissingParentDirectory(t *testing.T) {
	parent := t.TempDir()
	stateDir := filepath.Join(parent, "missing", "state")
	if err := collector.SaveState(stateDir, validTestState()); err == nil {
		t.Fatal("SaveState() succeeded with a missing parent directory")
	}
	if _, err := os.Lstat(stateDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("SaveState() created missing state directory: %v", err)
	}
	if entries := directoryEntries(t, parent); len(entries) != 0 {
		t.Fatalf("failed save left files in parent directory: %v", entries)
	}
}

func TestLoadStateRejectsOversizedFile(t *testing.T) {
	directory, _ := lockedStateDirectory(t)
	path := filepath.Join(directory, "state.json")
	contents := bytes.Repeat([]byte("x"), (2<<20)+1)
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatalf("write oversized state fixture: %v", err)
	}
	if state, err := collector.LoadState(directory); err == nil || state != nil {
		t.Fatalf("LoadState() = (%#v, %v), want oversized-file error", state, err)
	}
}

func TestLoadStateAcceptsFileExactlyAtSizeLimit(t *testing.T) {
	directory, _ := lockedStateDirectory(t)
	path := filepath.Join(directory, "state.json")
	contents := []byte(`{"generation":1,"source":{"path":"/not-read/ras-mc_event.db","device":0,"inode":1},"tables":{"devlink_event":0}}`)
	const maxStateSize = 2 << 20
	contents = append(contents, bytes.Repeat([]byte{' '}, maxStateSize-len(contents))...)
	if len(contents) != maxStateSize {
		t.Fatalf("state fixture size = %d, want %d", len(contents), maxStateSize)
	}
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatalf("write boundary-sized state: %v", err)
	}
	state, err := collector.LoadState(directory)
	if err != nil {
		t.Fatalf("LoadState() rejected a valid file at the size limit: %v", err)
	}
	if !reflect.DeepEqual(state, validTestState()) {
		t.Fatalf("LoadState() = %#v, want %#v", state, validTestState())
	}
}

func TestLoadStateRejectsMalformedOrInvalidJSON(t *testing.T) {
	valid := `{"generation":1,"source":{"path":"/not-read.db","device":0,"inode":1},"tables":{"devlink_event":0}}`
	fixtures := map[string]string{
		"malformed":                  `{`,
		"null root":                  `null`,
		"wrong top-level type":       `[]`,
		"string root":                `"null"`,
		"missing generation":         `{"source":{"path":"/not-read.db","device":0,"inode":1},"tables":{"devlink_event":0}}`,
		"null generation":            `{"generation":null,"source":{"path":"/not-read.db","device":0,"inode":1},"tables":{"devlink_event":0}}`,
		"string generation":          `{"generation":"1","source":{"path":"/not-read.db","device":0,"inode":1},"tables":{"devlink_event":0}}`,
		"fractional generation":      `{"generation":1.0,"source":{"path":"/not-read.db","device":0,"inode":1},"tables":{"devlink_event":0}}`,
		"exponent generation":        `{"generation":1e0,"source":{"path":"/not-read.db","device":0,"inode":1},"tables":{"devlink_event":0}}`,
		"missing source":             `{"generation":1,"tables":{"devlink_event":0}}`,
		"missing tables":             `{"generation":1,"source":{"path":"/not-read.db","device":0,"inode":1}}`,
		"null source":                `{"generation":1,"source":null,"tables":{"devlink_event":0}}`,
		"wrong source type":          `{"generation":1,"source":"null","tables":{"devlink_event":0}}`,
		"null tables":                `{"generation":1,"source":{"path":"/not-read.db","device":0,"inode":1},"tables":null}`,
		"array tables":               `{"generation":1,"source":{"path":"/not-read.db","device":0,"inode":1},"tables":[]}`,
		"string tables":              `{"generation":1,"source":{"path":"/not-read.db","device":0,"inode":1},"tables":"null"}`,
		"missing source path":        `{"generation":1,"source":{"device":0,"inode":1},"tables":{"devlink_event":0}}`,
		"wrong source path type":     `{"generation":1,"source":{"path":1,"device":0,"inode":1},"tables":{"devlink_event":0}}`,
		"wrong source device type":   `{"generation":1,"source":{"path":"/not-read.db","device":"0","inode":1},"tables":{"devlink_event":0}}`,
		"missing source inode":       `{"generation":1,"source":{"path":"/not-read.db","device":0},"tables":{"devlink_event":0}}`,
		"wrong source inode type":    `{"generation":1,"source":{"path":"/not-read.db","device":0,"inode":[]},"tables":{"devlink_event":0}}`,
		"null source path":           `{"generation":1,"source":{"path":null,"device":0,"inode":1},"tables":{"devlink_event":0}}`,
		"null source inode":          `{"generation":1,"source":{"path":"/not-read.db","device":0,"inode":null},"tables":{"devlink_event":0}}`,
		"zero generation":            `{"generation":0,"source":{"path":"/not-read.db","device":0,"inode":1},"tables":{"devlink_event":0}}`,
		"relative source":            `{"generation":1,"source":{"path":"relative.db","device":0,"inode":1},"tables":{"devlink_event":0}}`,
		"zero inode":                 `{"generation":1,"source":{"path":"/not-read.db","device":0,"inode":0},"tables":{"devlink_event":0}}`,
		"empty table set":            `{"generation":1,"source":{"path":"/not-read.db","device":0,"inode":1},"tables":{}}`,
		"empty table name":           `{"generation":1,"source":{"path":"/not-read.db","device":0,"inode":1},"tables":{"":0}}`,
		"string cursor":              `{"generation":1,"source":{"path":"/not-read.db","device":0,"inode":1},"tables":{"devlink_event":"0"}}`,
		"fractional cursor":          `{"generation":1,"source":{"path":"/not-read.db","device":0,"inode":1},"tables":{"devlink_event":0.0}}`,
		"exponent cursor":            `{"generation":1,"source":{"path":"/not-read.db","device":0,"inode":1},"tables":{"devlink_event":0e0}}`,
		"negative cursor":            `{"generation":1,"source":{"path":"/not-read.db","device":0,"inode":1},"tables":{"devlink_event":-1}}`,
		"int64 cursor overflow":      `{"generation":1,"source":{"path":"/not-read.db","device":0,"inode":1},"tables":{"devlink_event":9223372036854775808}}`,
		"uint64 generation overflow": `{"generation":18446744073709551616,"source":{"path":"/not-read.db","device":0,"inode":1},"tables":{"devlink_event":0}}`,
		"trailing object":            valid + ` {}`,
		"trailing primitive":         valid + ` false`,
	}
	for name, contents := range fixtures {
		t.Run(name, func(t *testing.T) {
			directory, _ := lockedStateDirectory(t)
			path := filepath.Join(directory, "state.json")
			if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
				t.Fatalf("write malformed state fixture: %v", err)
			}
			state, err := collector.LoadState(directory)
			if err == nil || state != nil {
				t.Fatalf("LoadState() = (%#v, %v), want validation error", state, err)
			}
		})
	}

	t.Run("escaped keys and strings preserve valid zero values", func(t *testing.T) {
		directory, _ := lockedStateDirectory(t)
		path := filepath.Join(directory, "state.json")
		contents := `{"\u0067eneration":1,"\u0073ource":{"\u0070ath":"/null/{source}\n雪.db","device":0,"inode":1},"tables":{"null {table}\n":0}}` + " \n\t"
		if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
			t.Fatalf("write escaped valid state fixture: %v", err)
		}
		state, err := collector.LoadState(directory)
		if err != nil {
			t.Fatalf("LoadState() rejected valid escaped JSON: %v", err)
		}
		if state.Source.Path != "/null/{source}\n雪.db" || state.Source.Device != 0 || state.Tables["null {table}\n"] != 0 {
			t.Fatalf("LoadState() changed escaped values: %#v", state)
		}
	})

}

func TestLoadStateUsesStandardJSONDecoding(t *testing.T) {
	tests := []struct {
		name     string
		contents []byte
		want     *collector.State
	}{
		{
			name:     "unknown fields are ignored",
			contents: []byte(`{"generation":1,"source":{"path":"/db","device":0,"inode":1,"extra":true},"tables":{"devlink_event":2},"extra":true}`),
			want: &collector.State{
				Generation: 1,
				Source:     collector.SourceIdentity{Path: "/db", Device: 0, Inode: 1},
				Tables:     map[string]int64{"devlink_event": 2},
			},
		},
		{
			name:     "null scalar values keep their zero values",
			contents: []byte(`{"generation":1,"source":{"path":"/db","device":null,"inode":1},"tables":{"devlink_event":null}}`),
			want: &collector.State{
				Generation: 1,
				Source:     collector.SourceIdentity{Path: "/db", Device: 0, Inode: 1},
				Tables:     map[string]int64{"devlink_event": 0},
			},
		},
		{
			name:     "duplicate object fields use standard decoder behavior",
			contents: []byte(`{"generation":1,"generation":2,"source":{"path":"/db","device":0,"inode":1},"tables":{"first":1},"tables":{"second":2}}`),
			want: &collector.State{
				Generation: 2,
				Source:     collector.SourceIdentity{Path: "/db", Device: 0, Inode: 1},
				Tables:     map[string]int64{"first": 1, "second": 2},
			},
		},
		{
			name:     "invalid UTF-8 is replaced",
			contents: []byte("{\"generation\":1,\"source\":{\"path\":\"/db\xff\",\"device\":0,\"inode\":1},\"tables\":{\"devlink_event\":0}}"),
			want: &collector.State{
				Generation: 1,
				Source:     collector.SourceIdentity{Path: "/db�", Device: 0, Inode: 1},
				Tables:     map[string]int64{"devlink_event": 0},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			directory, _ := lockedStateDirectory(t)
			path := filepath.Join(directory, "state.json")
			if err := os.WriteFile(path, test.contents, 0o600); err != nil {
				t.Fatalf("write state fixture: %v", err)
			}
			got, err := collector.LoadState(directory)
			if err != nil {
				t.Fatalf("LoadState() error = %v", err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("LoadState() = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestSaveStateRejectsOversizedCandidateWithoutReplacingCurrentState(t *testing.T) {
	directory, _ := lockedStateDirectory(t)
	committed := validTestState()
	if err := collector.SaveState(directory, committed); err != nil {
		t.Fatalf("save initial state: %v", err)
	}
	path := filepath.Join(directory, "state.json")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read initial state: %v", err)
	}

	oversized := validTestState()
	oversized.Tables[string(bytes.Repeat([]byte("x"), 2<<20))] = 0
	if err := collector.SaveState(directory, oversized); err == nil {
		t.Fatal("SaveState() accepted a state larger than the size limit")
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read state after oversized save: %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("oversized candidate replaced the committed state")
	}
}

func validTestState() *collector.State {
	return &collector.State{
		Generation: 1,
		Source: collector.SourceIdentity{
			Path:   "/not-read/ras-mc_event.db",
			Device: 0,
			Inode:  1,
		},
		Tables: map[string]int64{"devlink_event": 0},
	}
}

func lockedStateDirectory(t *testing.T) (string, *os.File) {
	t.Helper()
	directory := privateStateDirectory(t)
	lock, err := collector.AcquireLock(directory)
	if err != nil {
		t.Fatalf("acquire state lock: %v", err)
	}
	t.Cleanup(func() {
		if err := lock.Close(); err != nil {
			t.Errorf("release state lock: %v", err)
		}
	})
	return directory, lock
}
