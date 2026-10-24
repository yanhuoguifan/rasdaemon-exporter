package collector

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
)

// MCLabels groups memory-controller records by the selected mc_event fields.
type MCLabels struct {
	ErrorType   string
	MC          string
	TopLayer    string
	MiddleLayer string
	LowerLayer  string
	DIMM        string
	ErrMsg      string
}

// MCCounters distinguishes recorded rows from their reported error counts.
type MCCounters struct {
	Events uint64
	Errors uint64
}

// MCCollector keeps one in-memory cursor and cumulative counters for mc_event.
// Serialize collection and field access.
type MCCollector struct {
	LastID int64
	Counts map[MCLabels]MCCounters
}

type mcBatch struct {
	lastID int64
	rows   int
	counts map[MCLabels]MCCounters
}

const (
	mcMaxSeries = 10000

	mcEventsMetricName = "rasdaemon_memory_events_total"
	mcErrorsMetricName = "rasdaemon_memory_errors_total"
	mcEventsMetricHelp = "Number of mc_event records reported by rasdaemon."
	mcErrorsMetricHelp = "Sum of mc_event error counts reported by rasdaemon."
)

// mcInternalLocationValue omits numeric internal locations without changing
// the surrounding rasdaemon description.
var mcInternalLocationValue = regexp.MustCompile(`(?i)(\b(?:address|addr|offset|row|column|col|bank_addr|bank_address|bank_group|bankgroup|bank)\b(?:[ \t]*[:=][ \t]*|[ \t]+))(?:0x[0-9a-f]+|[0-9]+)\b`)

// ExportMetrics converts each grouped mc_event row into event and error
// counters with the same seven source-derived labels.
func (c *MCCollector) ExportMetrics() []CounterFamily {
	events := CounterFamily{Name: mcEventsMetricName, Help: mcEventsMetricHelp}
	errorsFamily := CounterFamily{Name: mcErrorsMetricName, Help: mcErrorsMetricHelp}
	for labels, counters := range c.Counts {
		metricLabels := mcMetricLabels(labels)
		events.Samples = append(events.Samples, CounterSample{Labels: metricLabels, Value: counters.Events})
		errorsFamily.Samples = append(errorsFamily.Samples, CounterSample{Labels: mcMetricLabels(labels), Value: counters.Errors})
	}
	return []CounterFamily{events, errorsFamily}
}

// RestoreMetrics replaces counts from the paired event/error families. Both
// absent families mean an empty table; partial or mismatched pairs are invalid.
func (c *MCCollector) RestoreMetrics(families []CounterFamily) error {
	if len(families) == 0 {
		c.Counts = make(map[MCLabels]MCCounters)
		return nil
	}
	if len(families) != 2 {
		return errors.New("snapshot must contain both mc_event metric families")
	}
	var events, errorsFamily *CounterFamily
	for i := range families {
		switch families[i].Name {
		case mcEventsMetricName:
			events = &families[i]
		case mcErrorsMetricName:
			errorsFamily = &families[i]
		default:
			return fmt.Errorf("unexpected mc_event metric family %q", families[i].Name)
		}
	}
	if events == nil || errorsFamily == nil {
		return errors.New("snapshot must contain both mc_event metric families")
	}
	if len(events.Samples) > mcMaxSeries || len(errorsFamily.Samples) > mcMaxSeries {
		return fmt.Errorf("mc_event series limit %d exceeded", mcMaxSeries)
	}
	if len(events.Samples) != len(errorsFamily.Samples) {
		return errors.New("mc_event metric families have different label sets")
	}

	counts := make(map[MCLabels]MCCounters, len(events.Samples))
	for _, sample := range events.Samples {
		labels, err := mcLabelsFromMap(sample.Labels)
		if err != nil {
			return err
		}
		counts[labels] = MCCounters{Events: sample.Value}
	}
	for _, sample := range errorsFamily.Samples {
		labels, err := mcLabelsFromMap(sample.Labels)
		if err != nil {
			return err
		}
		counters, exists := counts[labels]
		if !exists {
			return errors.New("mc_event metric families have different label sets")
		}
		counters.Errors = sample.Value
		counts[labels] = counters
	}
	c.Counts = counts
	return nil
}

// CollectBatch reads one complete mc_event page and advances only after both
// cumulative counters pass overflow and series checks.
func (c *MCCollector) CollectBatch(ctx context.Context, db *sql.DB, limit int) (int, error) {
	if db == nil {
		return 0, errors.New("SQLite source is nil")
	}
	if limit <= 0 {
		return 0, errors.New("mc_event batch limit must be positive")
	}
	if c.LastID < 0 {
		return 0, errors.New("mc_event cursor must be non-negative")
	}

	batch, err := readMCBatch(ctx, db, c.LastID, limit)
	if err != nil {
		return 0, err
	}
	totals := c.Counts
	if totals == nil {
		totals = make(map[MCLabels]MCCounters)
	}
	if err := mergeMCBatch(totals, batch); err != nil {
		return 0, err
	}
	c.Counts = totals
	c.LastID = batch.lastID
	return batch.rows, nil
}

// readMCBatch scans one ordered, read-only page and aggregates it separately
// so a failed query cannot partially update the collector.
func readMCBatch(ctx context.Context, db *sql.DB, afterID int64, limit int) (mcBatch, error) {
	batch := mcBatch{lastID: afterID, counts: make(map[MCLabels]MCCounters)}
	queryCtx, cancel := context.WithTimeout(ctx, databaseTimeout)
	defer cancel()
	tx, err := db.BeginTx(queryCtx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return mcBatch{}, fmt.Errorf("begin mc_event read transaction: %w", err)
	}
	defer tx.Rollback()

	var latestID sql.NullInt64
	err = tx.QueryRowContext(queryCtx,
		"SELECT id FROM mc_event ORDER BY id DESC LIMIT 1").Scan(&latestID)
	var maxID int64
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return mcBatch{}, fmt.Errorf("read latest mc_event ID: %w", err)
	}
	if err == nil {
		if !latestID.Valid || latestID.Int64 <= 0 {
			return mcBatch{}, errors.New("mc_event IDs must be positive integers")
		}
		maxID = latestID.Int64
	}
	if maxID < afterID {
		return mcBatch{}, fmt.Errorf("mc_event maximum ID moved backwards from %d to %d", afterID, maxID)
	}

	const query = `SELECT id, err_count, err_type, mc, top_layer, middle_layer, lower_layer, label, err_msg
		FROM mc_event WHERE id > ? ORDER BY id LIMIT ?`
	rows, err := tx.QueryContext(queryCtx, query, afterID, limit)
	if err != nil {
		return mcBatch{}, fmt.Errorf("query mc_event after ID %d: %w", afterID, err)
	}
	defer rows.Close()
	lastID := afterID
	for rows.Next() {
		var id int64
		var errCount uint64
		var errType, dimm, errMsg sql.NullString
		var mc, topLayer, middleLayer, lowerLayer sql.NullInt64
		if err := rows.Scan(&id, &errCount, &errType, &mc, &topLayer, &middleLayer, &lowerLayer, &dimm, &errMsg); err != nil {
			return mcBatch{}, fmt.Errorf("scan mc_event: %w", err)
		}
		if id <= lastID {
			return mcBatch{}, fmt.Errorf("mc_event IDs are not strictly increasing: %d after %d", id, lastID)
		}
		labels := MCLabels{
			ErrorType:   errType.String,
			MC:          mcPosition(mc),
			TopLayer:    mcPosition(topLayer),
			MiddleLayer: mcPosition(middleLayer),
			LowerLayer:  mcPosition(lowerLayer),
			DIMM:        dimm.String,
			ErrMsg: mcInternalLocationValue.ReplaceAllString(
				strings.ToValidUTF8(errMsg.String, "<invalid>"), "${1}<omitted>"),
		}
		delta := batch.counts[labels]
		if errCount > math.MaxUint64-delta.Errors {
			return mcBatch{}, errors.New("mc_event batch error counter overflow")
		}
		delta.Events++
		delta.Errors += errCount
		batch.counts[labels] = delta
		batch.rows++
		lastID = id
	}
	if err := rows.Err(); err != nil {
		return mcBatch{}, fmt.Errorf("iterate mc_event: %w", err)
	}
	if err := rows.Close(); err != nil {
		return mcBatch{}, fmt.Errorf("close mc_event query: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return mcBatch{}, fmt.Errorf("finish mc_event read transaction: %w", err)
	}
	batch.lastID = lastID
	return batch, nil
}

// mergeMCBatch checks both totals before updating the caller-owned map.
func mergeMCBatch(totals map[MCLabels]MCCounters, batch mcBatch) error {
	newSeries := 0
	for labels, delta := range batch.counts {
		current, exists := totals[labels]
		if !exists {
			newSeries++
		}
		if delta.Events > math.MaxUint64-current.Events {
			return errors.New("mc_event event counter overflow")
		}
		if delta.Errors > math.MaxUint64-current.Errors {
			return errors.New("mc_event error counter overflow")
		}
	}
	if newSeries > mcMaxSeries-len(totals) {
		return fmt.Errorf("mc_event series limit %d exceeded", mcMaxSeries)
	}
	for labels, delta := range batch.counts {
		current := totals[labels]
		current.Events += delta.Events
		current.Errors += delta.Errors
		totals[labels] = current
	}
	return nil
}

func mcPosition(value sql.NullInt64) string {
	if !value.Valid || value.Int64 < 0 {
		return "unknown"
	}
	return strconv.FormatInt(value.Int64, 10)
}

func mcMetricLabels(labels MCLabels) map[string]string {
	return map[string]string{
		"error_type":   labels.ErrorType,
		"mc":           labels.MC,
		"top_layer":    labels.TopLayer,
		"middle_layer": labels.MiddleLayer,
		"lower_layer":  labels.LowerLayer,
		"dimm":         labels.DIMM,
		"err_msg":      labels.ErrMsg,
	}
}

func mcLabelsFromMap(values map[string]string) (MCLabels, error) {
	if len(values) != 7 {
		return MCLabels{}, errors.New("mc_event metric must contain exactly seven source labels")
	}
	var labels MCLabels
	var ok bool
	if labels.ErrorType, ok = values["error_type"]; !ok {
		return MCLabels{}, errors.New("mc_event metric is missing error_type label")
	}
	if labels.MC, ok = values["mc"]; !ok {
		return MCLabels{}, errors.New("mc_event metric is missing mc label")
	}
	if labels.TopLayer, ok = values["top_layer"]; !ok {
		return MCLabels{}, errors.New("mc_event metric is missing top_layer label")
	}
	if labels.MiddleLayer, ok = values["middle_layer"]; !ok {
		return MCLabels{}, errors.New("mc_event metric is missing middle_layer label")
	}
	if labels.LowerLayer, ok = values["lower_layer"]; !ok {
		return MCLabels{}, errors.New("mc_event metric is missing lower_layer label")
	}
	if labels.DIMM, ok = values["dimm"]; !ok {
		return MCLabels{}, errors.New("mc_event metric is missing dimm label")
	}
	if labels.ErrMsg, ok = values["err_msg"]; !ok {
		return MCLabels{}, errors.New("mc_event metric is missing err_msg label")
	}
	return labels, nil
}
