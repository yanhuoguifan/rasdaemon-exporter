package collector

import (
	"bytes"
	"fmt"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"google.golang.org/protobuf/proto"
)

const (
	lastSuccessMetricName = "rasdaemon_collection_last_success_timestamp_seconds"
	lastSuccessMetricHelp = "Unix timestamp of the latest complete successful collection round."
	seriesCountMetricName = "rasdaemon_series_count"
	seriesCountMetricHelp = "Number of event-counter series in the published snapshot."
)

// Health holds gauges published separately from the recoverable event snapshot.
type Health struct {
	LastSuccess time.Time
	SeriesCount uint64
}

// SaveHealth atomically writes the last-success and event-series gauges.
func SaveHealth(path string, health Health) error {
	families := []*dto.MetricFamily{
		{
			Name: proto.String(lastSuccessMetricName),
			Help: proto.String(lastSuccessMetricHelp),
			Type: dto.MetricType_GAUGE.Enum(),
			Metric: []*dto.Metric{{
				Gauge: &dto.Gauge{Value: proto.Float64(float64(health.LastSuccess.Unix()))},
			}},
		},
		{
			Name: proto.String(seriesCountMetricName),
			Help: proto.String(seriesCountMetricHelp),
			Type: dto.MetricType_GAUGE.Enum(),
			Metric: []*dto.Metric{{
				Gauge: &dto.Gauge{Value: proto.Float64(float64(health.SeriesCount))},
			}},
		},
	}
	var output bytes.Buffer
	for _, family := range families {
		if _, err := expfmt.MetricFamilyToText(&output, family); err != nil {
			return fmt.Errorf("encode health metric %q: %w", family.GetName(), err)
		}
	}
	if err := atomicWriteFile(path, output.Bytes(), 0o644); err != nil {
		return fmt.Errorf("write health file %q: %w", path, err)
	}
	return nil
}
