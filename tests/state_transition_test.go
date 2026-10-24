package collector_test

import (
	"math"
	"reflect"
	"testing"

	"rasdaemon-exporter/internal/collector"
)

func stateForTransitions() *collector.State {
	return &collector.State{
		Generation: 8,
		Source: collector.SourceIdentity{
			Path:   "/not-read/ras-mc_event.db",
			Device: 0,
			Inode:  91,
		},
		Tables: map[string]int64{"devlink_event": 12, "mc_event": 34},
	}
}

func TestStateAdvanceCopiesAndAppliesPartialUpdates(t *testing.T) {
	current := stateForTransitions()
	before := cloneState(current)

	next, err := current.Advance(map[string]int64{"devlink_event": 15})
	if err != nil {
		t.Fatalf("Advance() error = %v", err)
	}
	want := &collector.State{
		Generation: 9,
		Source:     current.Source,
		Tables:     map[string]int64{"devlink_event": 15, "mc_event": 34},
	}
	if !reflect.DeepEqual(next, want) {
		t.Fatalf("Advance() = %#v, want %#v", next, want)
	}
	if !reflect.DeepEqual(current, before) {
		t.Fatalf("Advance() mutated committed state: got %#v, want %#v", current, before)
	}

	next.Tables["mc_event"] = 99
	if current.Tables["mc_event"] != 34 {
		t.Fatal("candidate table map aliases the committed table map")
	}
}

func TestStateAdvanceAcceptsEmptyHeartbeatAndMaximumID(t *testing.T) {
	current := stateForTransitions()
	next, err := current.Advance(nil)
	if err != nil {
		t.Fatalf("Advance(nil) error = %v", err)
	}
	if next.Generation != current.Generation+1 || !reflect.DeepEqual(next.Tables, current.Tables) {
		t.Fatalf("empty Advance() = %#v; want next generation with unchanged cursors", next)
	}

	maximumID := int64(math.MaxInt64)
	next, err = current.Advance(map[string]int64{"mc_event": maximumID})
	if err != nil {
		t.Fatalf("Advance() rejected maximum SQLite ID: %v", err)
	}
	if next.Tables["mc_event"] != maximumID {
		t.Fatalf("cursor = %d, want %d", next.Tables["mc_event"], maximumID)
	}
}

func TestStateAdvanceAddsRegisteredTableFromZero(t *testing.T) {
	current := stateForTransitions()
	delete(current.Tables, "mc_event")
	before := cloneState(current)

	first, err := current.Advance(map[string]int64{"mc_event": 0})
	if err != nil {
		t.Fatalf("Advance() with new table at zero: %v", err)
	}
	if first.Tables["mc_event"] != 0 || first.Generation != current.Generation+1 {
		t.Fatalf("first state with new table = %#v; want mc_event=0 and next generation", first)
	}
	if !reflect.DeepEqual(current, before) {
		t.Fatalf("Advance() mutated old state: got %#v, want %#v", current, before)
	}

	next, err := first.Advance(map[string]int64{"mc_event": 3})
	if err != nil || next.Tables["mc_event"] != 3 {
		t.Fatalf("Advance() after new table enrollment = (%#v, %v); want cursor 3", next, err)
	}
}

func TestStateAdvanceRejectsInvalidUpdatesWithoutMutation(t *testing.T) {
	tests := []struct {
		name    string
		state   func() *collector.State
		updates map[string]int64
	}{
		{name: "empty table name", state: stateForTransitions, updates: map[string]int64{"": 1}},
		{name: "negative cursor", state: stateForTransitions, updates: map[string]int64{"devlink_event": -1}},
		{name: "backwards cursor", state: stateForTransitions, updates: map[string]int64{"devlink_event": 11}},
		{
			name: "generation overflow",
			state: func() *collector.State {
				state := stateForTransitions()
				state.Generation = math.MaxUint64
				return state
			},
			updates: map[string]int64{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			current := test.state()
			before := cloneState(current)
			next, err := current.Advance(test.updates)
			if err == nil {
				t.Fatalf("Advance() = %#v, want error", next)
			}
			if next != nil {
				t.Fatalf("Advance() state = %#v on error; want nil", next)
			}
			if !reflect.DeepEqual(current, before) {
				t.Fatalf("failed Advance() mutated state: got %#v, want %#v", current, before)
			}
		})
	}
}

func TestStateCheckSourceAcceptsMatchingIdentity(t *testing.T) {
	state := stateForTransitions()
	if err := state.CheckSource(state.Source); err != nil {
		t.Fatalf("CheckSource() error = %v", err)
	}
}

func TestStateCheckSourceRejectsInvalidStateAndMismatchedIdentity(t *testing.T) {
	state := stateForTransitions()
	wrongSources := []collector.SourceIdentity{
		{Path: "/other/ras-mc_event.db", Device: state.Source.Device, Inode: state.Source.Inode},
		{Path: state.Source.Path, Device: state.Source.Device + 1, Inode: state.Source.Inode},
		{Path: state.Source.Path, Device: state.Source.Device, Inode: state.Source.Inode + 1},
	}
	for index, source := range wrongSources {
		if err := state.CheckSource(source); err == nil {
			t.Fatalf("CheckSource() accepted mismatched source identity %d", index)
		}
	}

	for _, invalid := range []*collector.State{
		{Source: state.Source, Tables: state.Tables},
		{Generation: state.Generation, Source: collector.SourceIdentity{Path: "relative"}, Tables: state.Tables},
		{Generation: state.Generation, Source: state.Source},
	} {
		if err := invalid.CheckSource(state.Source); err == nil {
			t.Fatalf("CheckSource() accepted invalid state %#v", invalid)
		}
	}
}

func TestStateTransitionsHandleGenerationBoundary(t *testing.T) {
	state := stateForTransitions()
	state.Generation = math.MaxUint64 - 1
	next, err := state.Advance(nil)
	if err != nil || next.Generation != math.MaxUint64 {
		t.Fatalf("Advance() at generation boundary = (%#v, %v)", next, err)
	}
	if _, err := next.Advance(nil); err == nil {
		t.Fatal("Advance() wrapped maximum generation")
	}
}

func cloneState(source *collector.State) *collector.State {
	clone := *source
	clone.Tables = make(map[string]int64, len(source.Tables))
	for table, cursor := range source.Tables {
		clone.Tables[table] = cursor
	}
	return &clone
}
