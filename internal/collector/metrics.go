package collector

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"google.golang.org/protobuf/proto"
)

const (
	maxExactCounter = uint64(1 << 53)
)

// Snapshot contains one generation of cumulative counter families.
// Database cursors are stored separately in state.json.
type Snapshot struct {
	Generation uint64
	Families   []CounterFamily
}

// CounterFamily describes one Prometheus counter family.
type CounterFamily struct {
	Name    string
	Help    string
	Samples []CounterSample
}

// CounterSample stores exported labels and its cumulative integer counter.
// Snapshot files preserve values exactly through maxExactCounter.
type CounterSample struct {
	Labels map[string]string
	Value  uint64
}

// MetricCodec translates one table's counters to and from generic families.
type MetricCodec interface {
	ExportMetrics() []CounterFamily
	RestoreMetrics([]CounterFamily) error
}

// ErrSnapshotDurabilityUncertain means the new metrics file is visible, but its
// directory entry could not be confirmed durable. The caller must stop.
var ErrSnapshotDurabilityUncertain = errFileDurabilityUncertain

// LoadSnapshot reads the generation header and Prometheus text families from
// path, returning their counters as generic families; for example, the sample
// events_total{device="eth0"} 12 becomes a CounterSample with value 12.
func LoadSnapshot(path string) (*Snapshot, error) {
	contents, err := readRegularFile(path, -1)
	if err != nil {
		return nil, fmt.Errorf("read metrics snapshot %q: %w", path, err)
	}
	snapshot, err := decodeSnapshot(contents)
	if err != nil {
		return nil, fmt.Errorf("load metrics snapshot %q: %w", path, err)
	}
	return snapshot, nil
}

// SaveSnapshot converts a Snapshot to Prometheus text and atomically replaces
// path; e.g. generation 3 and events_total{device="eth0"} 12 produce a
// generation header, HELP/TYPE metadata, and that sample line. Callers ensure
// family names and sample label sets are unique.
func SaveSnapshot(path string, snapshot *Snapshot) error {
	contents, err := encodeSnapshot(snapshot)
	if err != nil {
		return fmt.Errorf("encode metrics snapshot: %w", err)
	}
	if err := atomicWriteFile(path, contents, 0o644); err != nil {
		return fmt.Errorf("save metrics snapshot %q: %w", path, err)
	}
	return nil
}

// encodeSnapshot sorts metric families by name, preserves each family's sample
// order, and converts a snapshot to Prometheus text bytes, such as:
//
//	# rasdaemon-exporter generation=3
//	# HELP events_total Events.
//	# TYPE events_total counter
//	events_total{device="eth0"} 12
func encodeSnapshot(snapshot *Snapshot) ([]byte, error) {
	if snapshot == nil {
		return nil, errors.New("snapshot must not be nil")
	}
	if snapshot.Generation == 0 {
		return nil, errors.New("snapshot generation must be greater than zero")
	}

	families := slices.Clone(snapshot.Families)
	slices.SortFunc(families, func(a, b CounterFamily) int { return strings.Compare(a.Name, b.Name) })
	var output bytes.Buffer
	fmt.Fprintf(&output, "# rasdaemon-exporter generation=%d\n", snapshot.Generation)
	for _, family := range families {
		if len(family.Samples) == 0 {
			continue
		}
		if err := appendCounterFamily(&output, family); err != nil {
			return nil, fmt.Errorf("encode metric family %q: %w", family.Name, err)
		}
	}
	return output.Bytes(), nil
}

// appendCounterFamily converts a non-empty CounterFamily to HELP/TYPE and sample
// text, preserving sample order while sorting each sample's label names; for
// example, events_total{device="eth0"} 12. Empty families are omitted by
// encodeSnapshot, so this function never manufactures a sample.
func appendCounterFamily(output *bytes.Buffer, family CounterFamily) error {
	metric := &dto.MetricFamily{
		Name: proto.String(family.Name),
		Help: proto.String(family.Help),
		Type: dto.MetricType_COUNTER.Enum(),
	}
	metric.Metric = make([]*dto.Metric, 0, len(family.Samples))
	for _, sample := range family.Samples {
		if sample.Value > maxExactCounter {
			return fmt.Errorf("counter value %d exceeds the exact float64 limit %d", sample.Value, maxExactCounter)
		}
		item := &dto.Metric{
			Label:   make([]*dto.LabelPair, 0, len(sample.Labels)),
			Counter: &dto.Counter{Value: proto.Float64(float64(sample.Value))},
		}
		for _, name := range slices.Sorted(maps.Keys(sample.Labels)) {
			if name == string(model.MetricNameLabel) || !model.LegacyValidation.IsValidLabelName(name) {
				return fmt.Errorf("invalid Prometheus label name %q in metric %q", name, family.Name)
			}
			item.Label = append(item.Label, &dto.LabelPair{
				Name:  proto.String(name),
				Value: proto.String(sample.Labels[name]),
			})
		}
		metric.Metric = append(metric.Metric, item)
	}

	if _, err := expfmt.MetricFamilyToText(output, metric); err != nil {
		return err
	}
	return nil
}

// decodeSnapshot reads the exporter's generation comment and asks expfmt to
// parse Prometheus samples; events_total{device="eth0"} 12 becomes a CounterSample.
func decodeSnapshot(contents []byte) (*Snapshot, error) {
	generation, err := parseSnapshotGeneration(contents)
	if err != nil {
		return nil, err
	}
	parser := expfmt.NewTextParser(model.LegacyValidation)
	parsed, err := parser.TextToMetricFamilies(bytes.NewReader(contents))
	if err != nil {
		return nil, errors.New("invalid Prometheus text exposition")
	}

	names := slices.Sorted(maps.Keys(parsed))
	snapshot := &Snapshot{Generation: generation, Families: make([]CounterFamily, 0, len(names))}
	for _, name := range names {
		family := parsed[name]
		if family.GetType() != dto.MetricType_COUNTER {
			return nil, fmt.Errorf("metric family %q is not declared as a counter", name)
		}
		counterFamily := CounterFamily{Name: name, Help: family.GetHelp()}
		counterFamily.Samples = make([]CounterSample, 0, len(family.GetMetric()))
		for _, metric := range family.GetMetric() {
			if metric.TimestampMs != nil {
				return nil, fmt.Errorf("metric family %q contains a timestamped sample", name)
			}
			value := metric.GetCounter().GetValue()
			if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > float64(maxExactCounter) || math.Trunc(value) != value {
				return nil, fmt.Errorf("metric family %q contains a counter outside the exact float64 range [0, %d]", name, maxExactCounter)
			}
			labels := make(map[string]string, len(metric.GetLabel()))
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			counterFamily.Samples = append(counterFamily.Samples, CounterSample{Labels: labels, Value: uint64(value)})
		}
		snapshot.Families = append(snapshot.Families, counterFamily)
	}
	return snapshot, nil
}

// parseSnapshotGeneration extracts the exporter's positive generation comment,
// such as "# rasdaemon-exporter generation=3"; expfmt parses every other line.
func parseSnapshotGeneration(contents []byte) (uint64, error) {
	const generationPrefix = "# rasdaemon-exporter generation="
	var generation uint64
	var generationCount int
	for _, sourceLine := range bytes.Split(contents, []byte{'\n'}) {
		line := bytes.TrimSpace(sourceLine)
		if !bytes.HasPrefix(line, []byte(generationPrefix)) {
			continue
		}
		generationCount++
		text := string(bytes.TrimPrefix(line, []byte(generationPrefix)))
		parsed, err := strconv.ParseUint(text, 10, 64)
		if err != nil || parsed == 0 || strconv.FormatUint(parsed, 10) != text {
			return 0, errors.New("metrics snapshot has an invalid generation")
		}
		generation = parsed
	}
	if generationCount != 1 {
		return 0, errors.New("metrics snapshot must contain exactly one generation")
	}
	return generation, nil
}
