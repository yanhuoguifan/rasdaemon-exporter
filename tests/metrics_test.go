package collector_test

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"rasdaemon-exporter/internal/collector"
)

const (
	devlinkFamilyName = "rasdaemon_devlink_health_events_total"
	devlinkFamilyHelp = "Number of collected devlink event records."
	blockFamilyName   = "rasdaemon_block_io_error_events_total"
	blockFamilyHelp   = "Synthetic block I/O events.\nPath: C:\\rasdaemon"
)

func TestSnapshotRoundTripPreservesGenericFamiliesAndCountersWithinFloat64Precision(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metrics.prom")
	want := &collector.Snapshot{
		Generation: 17,
		Families: []collector.CounterFamily{
			{
				Name: devlinkFamilyName,
				Help: devlinkFamilyHelp,
				Samples: []collector.CounterSample{
					{
						Labels: map[string]string{
							"bus_name":      `pci\\0000`,
							"dev_name":      `eth"0`,
							"driver_name":   "mlx5_core",
							"reporter_name": "tx\nrx",
							"msg":           "timeout \"reported\" at /dev/网卡\n第二行",
						},
						Value: 1 << 53,
					},
					{Labels: map[string]string{}, Value: (1 << 53) - 1},
				},
			},
			{
				Name: blockFamilyName,
				Help: blockFamilyHelp,
				Samples: []collector.CounterSample{
					{
						Labels: map[string]string{"dev": "/dev/nvme0n1", "error": "I/O timeout"},
						Value:  42,
					},
				},
			},
		},
	}

	if err := collector.SaveSnapshot(path, want); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read saved metrics snapshot: %v", err)
	}
	text := string(contents)
	for _, encoded := range []string{
		"# rasdaemon-exporter generation=17\n",
		"# HELP " + blockFamilyName + " Synthetic block I/O events.\\nPath: C:\\\\rasdaemon\n",
		blockFamilyName + `{dev="/dev/nvme0n1",error="I/O timeout"} 42` + "\n",
	} {
		if !strings.Contains(text, encoded) {
			t.Errorf("saved snapshot does not contain %q", encoded)
		}
	}
	if strings.Contains(text, `node=`) || strings.Contains(text, `scope=`) {
		t.Fatalf("saved metrics contain a non-event label: %s", text)
	}

	got, err := collector.LoadSnapshot(path)
	if err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	if !sameSnapshot(got, want) {
		t.Fatalf("LoadSnapshot() = %#v, want semantically equal to %#v", got, want)
	}
}

func TestSnapshotCounterPrecisionBoundary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metrics.prom")
	withinLimit := &collector.Snapshot{Generation: 1, Families: []collector.CounterFamily{{
		Name: devlinkFamilyName,
		Help: devlinkFamilyHelp,
		Samples: []collector.CounterSample{{
			Labels: map[string]string{"msg": "limit"},
			Value:  1 << 53,
		}},
	}}}
	if err := collector.SaveSnapshot(path, withinLimit); err != nil {
		t.Fatalf("SaveSnapshot() rejected the inclusive 2^53 boundary: %v", err)
	}
	loaded, err := collector.LoadSnapshot(path)
	if err != nil {
		t.Fatalf("LoadSnapshot() at the 2^53 boundary: %v", err)
	}
	if !sameSnapshot(loaded, withinLimit) {
		t.Fatalf("boundary snapshot = %#v, want %#v", loaded, withinLimit)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read boundary snapshot: %v", err)
	}

	overLimit := &collector.Snapshot{Generation: 2, Families: []collector.CounterFamily{{
		Name: devlinkFamilyName,
		Help: devlinkFamilyHelp,
		Samples: []collector.CounterSample{{
			Labels: map[string]string{"msg": "rounded if converted"},
			Value:  (1 << 53) + 1,
		}},
	}}}
	if err := collector.SaveSnapshot(path, overLimit); err == nil {
		t.Fatal("SaveSnapshot() accepted a counter that float64 would round")
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read snapshot after rejected counter: %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("counter above the precision boundary changed the saved snapshot")
	}
}

func TestEmptyFamiliesAreOmittedAndGenerationRoundTrips(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metrics.prom")
	want := &collector.Snapshot{
		Generation: 42,
		Families: []collector.CounterFamily{
			{Name: "rasdaemon_empty_events_total", Help: "No events yet."},
		},
	}
	if err := collector.SaveSnapshot(path, want); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}

	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read empty-family snapshot: %v", err)
	}
	text := string(contents)
	if !strings.HasPrefix(text, "# rasdaemon-exporter generation=42\n") {
		t.Fatalf("empty snapshot lost its generation header: %s", text)
	}
	if text != "# rasdaemon-exporter generation=42\n" {
		t.Fatalf("empty family produced metric text: %q", text)
	}

	got, err := collector.LoadSnapshot(path)
	if err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	if got.Generation != want.Generation || len(got.Families) != 0 {
		t.Fatalf("LoadSnapshot() = %#v, want generation %d and no families", got, want.Generation)
	}

	// Older snapshots could contain HELP/TYPE without samples; expfmt drops
	// that family, which has the same restore meaning as no family at all.
	legacyPath := writeSnapshotFixture(t, "# rasdaemon-exporter generation=42\n"+
		"# HELP rasdaemon_empty_events_total No events yet.\n"+
		"# TYPE rasdaemon_empty_events_total counter\n")
	legacy, err := collector.LoadSnapshot(legacyPath)
	if err != nil || legacy.Generation != 42 || len(legacy.Families) != 0 {
		t.Fatalf("LoadSnapshot(metadata-only family) = (%#v, %v), want generation 42 and no families", legacy, err)
	}

	// A snapshot can also contain no metric families at all.
	emptyPath := filepath.Join(t.TempDir(), "empty.prom")
	empty := &collector.Snapshot{Generation: 43}
	if err := collector.SaveSnapshot(emptyPath, empty); err != nil {
		t.Fatalf("SaveSnapshot(empty families) error = %v", err)
	}
	got, err = collector.LoadSnapshot(emptyPath)
	if err != nil {
		t.Fatalf("LoadSnapshot(empty families) error = %v", err)
	}
	if !sameSnapshot(got, empty) {
		t.Fatalf("LoadSnapshot(empty families) = %#v, want %#v", got, empty)
	}
}

func TestZeroCounterSampleIsNotTreatedAsAnEmptyFamily(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metrics.prom")
	want := &collector.Snapshot{Generation: 8, Families: []collector.CounterFamily{{
		Name: devlinkFamilyName, Help: devlinkFamilyHelp,
		Samples: []collector.CounterSample{{Labels: map[string]string{"msg": "zero"}, Value: 0}},
	}}}
	if err := collector.SaveSnapshot(path, want); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read zero-counter snapshot: %v", err)
	}
	if !strings.Contains(string(contents), devlinkFamilyName+`{msg="zero"} 0`+"\n") {
		t.Fatalf("zero counter sample was omitted: %s", contents)
	}
	got, err := collector.LoadSnapshot(path)
	if err != nil || !sameSnapshot(got, want) {
		t.Fatalf("LoadSnapshot() = (%#v, %v), want zero-valued sample %#v", got, err, want)
	}
}

func TestSaveSnapshotSortsFamiliesAndLabelNamesButPreservesSampleOrder(t *testing.T) {
	snapshot := &collector.Snapshot{
		Generation: 7,
		Families: []collector.CounterFamily{
			{Name: "z_events_total", Help: "Last family.", Samples: []collector.CounterSample{
				{Labels: map[string]string{"b": "2", "a": "1"}, Value: 2},
				{Labels: map[string]string{"a": "0"}, Value: 1},
			}},
			{Name: "a_events_total", Help: "First family.", Samples: []collector.CounterSample{
				{Labels: map[string]string{}, Value: 3},
			}},
		},
	}
	path := filepath.Join(t.TempDir(), "snapshot.prom")
	if err := collector.SaveSnapshot(path, snapshot); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}

	text := string(contents)
	firstFamily := strings.Index(text, "# HELP a_events_total")
	lastFamily := strings.Index(text, "# HELP z_events_total")
	if firstFamily < 0 || lastFamily < 0 || firstFamily >= lastFamily {
		t.Fatalf("metric families are not sorted by name:\n%s", text)
	}
	firstSample := strings.Index(text, `z_events_total{a="1",b="2"} 2`)
	secondSample := strings.Index(text, `z_events_total{a="0"} 1`)
	if firstSample < 0 || secondSample < 0 || firstSample >= secondSample {
		t.Fatalf("sample order was not preserved or label names were not sorted:\n%s", text)
	}
	if snapshot.Families[0].Name != "z_events_total" || snapshot.Families[0].Samples[0].Value != 2 {
		t.Fatalf("SaveSnapshot mutated the input ordering: %#v", snapshot.Families)
	}
}

func TestLoadSnapshotAcceptsReorderedLabelsAndEmptyLabelSet(t *testing.T) {
	contents := snapshotDocument(
		"# rasdaemon-exporter generation=3\n",
		"# HELP synthetic_events_total Synthetic events.\n# TYPE synthetic_events_total counter\n"+
			`synthetic_events_total{z="last",a="first"} 9`+"\n"+
			"# HELP unlabeled_events_total Events without labels.\n# TYPE unlabeled_events_total counter\n"+
			"unlabeled_events_total 4\n",
	)
	path := writeSnapshotFixture(t, contents)
	got, err := collector.LoadSnapshot(path)
	if err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	want := &collector.Snapshot{
		Generation: 3,
		Families: []collector.CounterFamily{
			{Name: "synthetic_events_total", Help: "Synthetic events.", Samples: []collector.CounterSample{
				{Labels: map[string]string{"a": "first", "z": "last"}, Value: 9},
			}},
			{Name: "unlabeled_events_total", Help: "Events without labels.", Samples: []collector.CounterSample{
				{Value: 4},
			}},
		},
	}
	if !sameSnapshot(got, want) {
		t.Fatalf("LoadSnapshot() = %#v, want %#v", got, want)
	}
}

func TestLoadSnapshotDelegatesPrometheusTextCommentsAndOptionalHelpToExpfmt(t *testing.T) {
	contents := snapshotDocument(
		"# rasdaemon-exporter generation=11\n",
		"# an ordinary exposition comment\n"+
			"# TYPE synthetic_events_total counter\n"+
			`synthetic_events_total{device="one"} 7`+"\n",
	)
	path := writeSnapshotFixture(t, contents)
	got, err := collector.LoadSnapshot(path)
	if err != nil {
		t.Fatalf("LoadSnapshot() rejected standard comments or absent HELP: %v", err)
	}
	want := &collector.Snapshot{Generation: 11, Families: []collector.CounterFamily{{
		Name:    "synthetic_events_total",
		Samples: []collector.CounterSample{{Labels: map[string]string{"device": "one"}, Value: 7}},
	}}}
	if !sameSnapshot(got, want) {
		t.Fatalf("LoadSnapshot() = %#v, want %#v", got, want)
	}
}

func TestSaveSnapshotRejectsInvalidInputWithoutReplacingExistingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metrics.prom")
	committed := &collector.Snapshot{
		Generation: 1,
		Families: []collector.CounterFamily{{
			Name:    "synthetic_events_total",
			Help:    "Synthetic events.",
			Samples: []collector.CounterSample{{Labels: map[string]string{"kind": "old"}, Value: 4}},
		}},
	}
	if err := collector.SaveSnapshot(path, committed); err != nil {
		t.Fatalf("save initial snapshot: %v", err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read initial snapshot: %v", err)
	}

	invalid := []*collector.Snapshot{
		nil,
		{Generation: 0},
	}
	for _, candidate := range invalid {
		if err := collector.SaveSnapshot(path, candidate); err == nil {
			t.Errorf("SaveSnapshot(%#v) succeeded, want validation error", candidate)
		}
		after, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read committed snapshot after rejected save: %v", err)
		}
		if !bytes.Equal(after, before) {
			t.Fatal("rejected snapshot changed the previously committed file")
		}
	}
	if entries := snapshotDirectoryEntries(t, filepath.Dir(path)); len(entries) != 1 || entries[0] != "metrics.prom" {
		t.Fatalf("failed saves left temporary files: %v", entries)
	}
}

func TestSnapshotFileOperationsUseExpectedModeAndRejectUnsafeTargets(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "metrics.prom")
	snapshot := &collector.Snapshot{Generation: 1}
	if err := collector.SaveSnapshot(path, snapshot); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat saved snapshot: %v", err)
	}
	if info.Mode().Perm() != 0o644 {
		t.Fatalf("snapshot mode = %04o, want 0644", info.Mode().Perm())
	}
	if entries := snapshotDirectoryEntries(t, directory); len(entries) != 1 || entries[0] != "metrics.prom" {
		t.Fatalf("successful save left temporary files: %v", entries)
	}

	if _, err := collector.LoadSnapshot(directory); err == nil {
		t.Fatal("LoadSnapshot() accepted a directory")
	}
	if err := collector.SaveSnapshot(directory, snapshot); err == nil {
		t.Fatal("SaveSnapshot() replaced a directory")
	}

	external := filepath.Join(t.TempDir(), "external.prom")
	const externalContents = "keep this external snapshot"
	if err := os.WriteFile(external, []byte(externalContents), 0o600); err != nil {
		t.Fatalf("write external target: %v", err)
	}
	link := filepath.Join(directory, "link.prom")
	if err := os.Symlink(external, link); err != nil {
		t.Fatalf("create snapshot symlink: %v", err)
	}
	if _, err := collector.LoadSnapshot(link); err == nil {
		t.Fatal("LoadSnapshot() followed a symbolic link")
	}
	if err := collector.SaveSnapshot(link, snapshot); err == nil {
		t.Fatal("SaveSnapshot() replaced a symbolic link")
	}
	linkTarget, err := os.Readlink(link)
	if err != nil || linkTarget != external {
		t.Fatalf("snapshot symlink changed to %q (error %v)", linkTarget, err)
	}
	contents, err := os.ReadFile(external)
	if err != nil || string(contents) != externalContents {
		t.Fatalf("external target changed to %q (error %v)", contents, err)
	}
	entries := snapshotDirectoryEntries(t, directory)
	sort.Strings(entries)
	if len(entries) != 2 || entries[0] != "link.prom" || entries[1] != "metrics.prom" {
		t.Fatalf("unsafe-target operations left temporary files: %v", entries)
	}
}

func TestLoadSnapshotReportsMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.prom")
	if snapshot, err := collector.LoadSnapshot(path); snapshot != nil || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("LoadSnapshot() = (%#v, %v), want (nil, os.ErrNotExist)", snapshot, err)
	}
}

func TestLoadSnapshotRejectsMalformedText(t *testing.T) {
	validGeneration := "# rasdaemon-exporter generation=5\n"
	validHeader := "# HELP synthetic_events_total Synthetic events.\n# TYPE synthetic_events_total counter\n"
	validSample := "synthetic_events_total{device=\"one\"} 7"
	doc := func(generation, body string) string { return snapshotDocument(generation, validHeader+body+"\n") }

	fixtures := map[string]string{
		"missing generation":   doc("", validSample),
		"malformed generation": doc("# rasdaemon-exporter generation=zero\n", validSample),
		"zero generation":      doc("# rasdaemon-exporter generation=0\n", validSample),
		"generation overflow":  doc("# rasdaemon-exporter generation=18446744073709551616\n", validSample),
		"duplicate generation": doc(validGeneration+"# rasdaemon-exporter generation=6\n", validSample),
		"missing type": snapshotDocument(validGeneration,
			"# HELP synthetic_events_total Synthetic events.\n"+validSample+"\n"),
		"malformed type": snapshotDocument(validGeneration,
			"# TYPE synthetic_events_total counter extra\n"+validSample+"\n"),
		"wrong metric type": doc(validGeneration,
			"# HELP synthetic_events_total Synthetic events.\n# TYPE synthetic_events_total gauge\n"+validSample),
		"invalid metric syntax":              doc(validGeneration, "synthetic_events_total this is not a sample"),
		"bad count":                          doc(validGeneration, `synthetic_events_total{device="one"} many`),
		"negative count":                     doc(validGeneration, `synthetic_events_total{device="one"} -1`),
		"fractional count":                   doc(validGeneration, `synthetic_events_total{device="one"} 1.5`),
		"count above exact float64 boundary": doc(validGeneration, `synthetic_events_total{device="one"} 9007199254740994`),
		"not a number":                       doc(validGeneration, `synthetic_events_total{device="one"} NaN`),
		"infinite count":                     doc(validGeneration, `synthetic_events_total{device="one"} +Inf`),
		"count overflow":                     doc(validGeneration, `synthetic_events_total{device="one"} 18446744073709551616`),
		"timestamp":                          doc(validGeneration, `synthetic_events_total{device="one"} 7 12345`),
		"duplicate family": doc(validGeneration,
			validHeader+validSample+"\n"+validHeader+`synthetic_events_total{device="two"} 8`),
		"duplicate label name":       doc(validGeneration, `synthetic_events_total{device="one",device="two"} 7`),
		"reserved metric name label": doc(validGeneration, `synthetic_events_total{__name__="other_events_total"} 7`),
		"trailing malformed line":    doc(validGeneration, validSample+"\nnot a metric"),
	}

	for name, contents := range fixtures {
		t.Run(name, func(t *testing.T) {
			path := writeSnapshotFixture(t, contents)
			if snapshot, err := collector.LoadSnapshot(path); err == nil || snapshot != nil {
				t.Fatalf("LoadSnapshot() = (%#v, %v), want malformed-snapshot error", snapshot, err)
			}
		})
	}
}

func TestSnapshotAllowsMoreThanTenThousandSeries(t *testing.T) {
	const sampleCount = 10001
	want := &collector.Snapshot{Generation: 9, Families: []collector.CounterFamily{{
		Name: devlinkFamilyName, Help: devlinkFamilyHelp, Samples: make([]collector.CounterSample, sampleCount),
	}}}
	for i := range want.Families[0].Samples {
		want.Families[0].Samples[i] = collector.CounterSample{
			Labels: map[string]string{"device": fmt.Sprintf("device-%d", i)},
			Value:  1,
		}
	}

	path := filepath.Join(t.TempDir(), "many-series.prom")
	if err := collector.SaveSnapshot(path, want); err != nil {
		t.Fatalf("SaveSnapshot() rejected %d samples: %v", sampleCount, err)
	}
	got, err := collector.LoadSnapshot(path)
	if err != nil {
		t.Fatalf("LoadSnapshot() rejected %d samples: %v", sampleCount, err)
	}
	if !sameSnapshot(got, want) {
		t.Fatalf("snapshot did not preserve all %d samples", sampleCount)
	}
}

func TestSnapshotAllowsFilesLargerThanFourMiB(t *testing.T) {
	want := &collector.Snapshot{Generation: 2, Families: []collector.CounterFamily{{
		Name: "large_events_total", Help: "Large label.", Samples: []collector.CounterSample{{
			Labels: map[string]string{"value": strings.Repeat("x", (4<<20)+1)}, Value: 1,
		}},
	}}}
	path := filepath.Join(t.TempDir(), "large.prom")
	if err := collector.SaveSnapshot(path, want); err != nil {
		t.Fatalf("SaveSnapshot() rejected a file larger than 4 MiB: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat saved snapshot: %v", err)
	}
	if info.Size() <= 4<<20 {
		t.Fatalf("saved snapshot size = %d, want more than 4 MiB", info.Size())
	}
	got, err := collector.LoadSnapshot(path)
	if err != nil {
		t.Fatalf("LoadSnapshot() rejected a file larger than 4 MiB: %v", err)
	}
	if !sameSnapshot(got, want) {
		t.Fatal("large snapshot did not round-trip")
	}
}

func snapshotDocument(generation, body string) string {
	return generation + body
}

func writeSnapshotFixture(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "metrics.prom")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write snapshot fixture: %v", err)
	}
	return path
}

func snapshotDirectoryEntries(t *testing.T, directory string) []string {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatalf("read snapshot directory: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

func sameSnapshot(a, b *collector.Snapshot) bool {
	if a == nil || b == nil || a.Generation != b.Generation || len(a.Families) != len(b.Families) {
		return a == nil && b == nil
	}
	families := make(map[string]collector.CounterFamily, len(a.Families))
	for _, family := range a.Families {
		families[family.Name] = family
	}
	for _, expected := range b.Families {
		actual, ok := families[expected.Name]
		if !ok || actual.Help != expected.Help || len(actual.Samples) != len(expected.Samples) {
			return false
		}
		samples := make(map[string]uint64, len(actual.Samples))
		for _, sample := range actual.Samples {
			key := testLabelSetKey(sample.Labels)
			if _, duplicate := samples[key]; duplicate {
				return false
			}
			samples[key] = sample.Value
		}
		for _, sample := range expected.Samples {
			value, ok := samples[testLabelSetKey(sample.Labels)]
			if !ok || value != sample.Value {
				return false
			}
		}
	}
	return true
}

func testLabelSetKey(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for name := range labels {
		keys = append(keys, name)
	}
	sort.Strings(keys)
	var key strings.Builder
	for _, name := range keys {
		fmt.Fprintf(&key, "%q=%q;", name, labels[name])
	}
	return key.String()
}
