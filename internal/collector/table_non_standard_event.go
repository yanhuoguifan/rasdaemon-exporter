package collector

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
)

// NonStandardLabels groups non-standard firmware reports by source fields.
type NonStandardLabels struct {
	SecType  string
	FRUID    string
	FRUText  string
	Severity string
}

// NonStandardCollector stores the consumed cursor and cumulative report counts.
// Serialize collection and field access.
type NonStandardCollector struct {
	LastID int64
	Counts map[NonStandardLabels]uint64
}

type nonStandardBatch struct {
	lastID int64
	rows   int
	counts map[NonStandardLabels]uint64
}

const (
	nonStandardMaxSeries  = 10000
	nonStandardMetricName = "rasdaemon_non_standard_events_total"
	nonStandardMetricHelp = "Number of non-standard event records reported by rasdaemon."
)

// ExportMetrics returns cumulative report counts as one counter family.
func (c *NonStandardCollector) ExportMetrics() []CounterFamily {
	samples := make([]CounterSample, 0, len(c.Counts))
	for labels, value := range c.Counts {
		samples = append(samples, CounterSample{
			Labels: map[string]string{
				"sec_type": labels.SecType,
				"fru_id":   labels.FRUID,
				"fru_text": labels.FRUText,
				"severity": labels.Severity,
			},
			Value: value,
		})
	}
	return []CounterFamily{{Name: nonStandardMetricName, Help: nonStandardMetricHelp, Samples: samples}}
}

// RestoreMetrics replaces counts from the snapshot without reformatting labels.
func (c *NonStandardCollector) RestoreMetrics(families []CounterFamily) error {
	if len(families) == 0 {
		c.Counts = make(map[NonStandardLabels]uint64)
		return nil
	}
	if len(families) != 1 || families[0].Name != nonStandardMetricName {
		return errors.New("snapshot must contain exactly one non_standard_event metric family")
	}
	family := families[0]
	if len(family.Samples) > nonStandardMaxSeries {
		return fmt.Errorf("non_standard_event series limit %d exceeded", nonStandardMaxSeries)
	}
	counts := make(map[NonStandardLabels]uint64, len(family.Samples))
	for _, sample := range family.Samples {
		labels, err := nonStandardLabelsFromMap(sample.Labels)
		if err != nil {
			return err
		}
		counts[labels] = sample.Value
	}
	c.Counts = counts
	return nil
}

// CollectBatch counts each report once and advances only after a complete page.
func (c *NonStandardCollector) CollectBatch(ctx context.Context, db *sql.DB, limit int) (int, error) {
	if db == nil {
		return 0, errors.New("SQLite source is nil")
	}
	if limit <= 0 {
		return 0, errors.New("non_standard_event batch limit must be positive")
	}
	if c.LastID < 0 {
		return 0, errors.New("non_standard_event cursor must be non-negative")
	}
	batch, err := readNonStandardBatch(ctx, db, c.LastID, limit)
	if err != nil {
		return 0, err
	}
	totals := c.Counts
	if totals == nil {
		totals = make(map[NonStandardLabels]uint64)
	}
	if err := mergeNonStandardBatch(totals, batch.counts); err != nil {
		return 0, err
	}
	c.Counts = totals
	c.LastID = batch.lastID
	return batch.rows, nil
}

// readNonStandardBatch groups one ordered page inside a read-only transaction.
func readNonStandardBatch(ctx context.Context, db *sql.DB, afterID int64, limit int) (nonStandardBatch, error) {
	batch := nonStandardBatch{lastID: afterID, counts: make(map[NonStandardLabels]uint64)}
	queryCtx, cancel := context.WithTimeout(ctx, databaseTimeout)
	defer cancel()
	tx, err := db.BeginTx(queryCtx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nonStandardBatch{}, fmt.Errorf("begin non_standard_event read transaction: %w", err)
	}
	defer tx.Rollback()

	var latestID sql.NullInt64
	err = tx.QueryRowContext(queryCtx, "SELECT id FROM non_standard_event ORDER BY id DESC LIMIT 1").Scan(&latestID)
	var maxID int64
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nonStandardBatch{}, fmt.Errorf("read latest non_standard_event ID: %w", err)
	}
	if err == nil {
		if !latestID.Valid || latestID.Int64 <= 0 {
			return nonStandardBatch{}, errors.New("non_standard_event IDs must be positive integers")
		}
		maxID = latestID.Int64
	}
	if maxID < afterID {
		return nonStandardBatch{}, fmt.Errorf("non_standard_event maximum ID moved backwards from %d to %d", afterID, maxID)
	}
	const query = `SELECT id, sec_type, fru_id, fru_text, severity
		FROM non_standard_event WHERE id > ? ORDER BY id LIMIT ?`
	rows, err := tx.QueryContext(queryCtx, query, afterID, limit)
	if err != nil {
		return nonStandardBatch{}, fmt.Errorf("query non_standard_event after ID %d: %w", afterID, err)
	}
	defer rows.Close()
	lastID := afterID
	for rows.Next() {
		var id int64
		var secType, fruID sql.Null[[]byte]
		var fruText, severity sql.NullString
		if err := rows.Scan(&id, &secType, &fruID, &fruText, &severity); err != nil {
			return nonStandardBatch{}, fmt.Errorf("scan non_standard_event: %w", err)
		}
		if id <= lastID {
			return nonStandardBatch{}, fmt.Errorf("non_standard_event IDs are not strictly increasing: %d after %d", id, lastID)
		}
		secTypeGUID, err := formatGUID(secType)
		if err != nil {
			return nonStandardBatch{}, fmt.Errorf("non_standard_event ID %d sec_type: %w", id, err)
		}
		fruIDGUID, err := formatGUID(fruID)
		if err != nil {
			return nonStandardBatch{}, fmt.Errorf("non_standard_event ID %d fru_id: %w", id, err)
		}
		labels := NonStandardLabels{
			SecType:  secTypeGUID,
			FRUID:    fruIDGUID,
			FRUText:  strings.ToValidUTF8(fruText.String, "<invalid>"),
			Severity: severity.String,
		}
		batch.counts[labels]++
		batch.rows++
		lastID = id
	}
	if err := rows.Err(); err != nil {
		return nonStandardBatch{}, fmt.Errorf("iterate non_standard_event: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nonStandardBatch{}, fmt.Errorf("close non_standard_event query: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nonStandardBatch{}, fmt.Errorf("finish non_standard_event read transaction: %w", err)
	}
	batch.lastID = lastID
	return batch, nil
}

// mergeNonStandardBatch checks overflow and series cardinality before mutation.
func mergeNonStandardBatch(totals, delta map[NonStandardLabels]uint64) error {
	newSeries := 0
	for labels, count := range delta {
		current, exists := totals[labels]
		if !exists {
			newSeries++
		}
		if count > math.MaxUint64-current {
			return errors.New("non_standard_event counter overflow")
		}
	}
	if newSeries > nonStandardMaxSeries-len(totals) {
		return fmt.Errorf("non_standard_event series limit %d exceeded", nonStandardMaxSeries)
	}
	for labels, count := range delta {
		totals[labels] += count
	}
	return nil
}

func nonStandardLabelsFromMap(values map[string]string) (NonStandardLabels, error) {
	if len(values) != 4 {
		return NonStandardLabels{}, errors.New("non_standard_event metric must contain exactly four labels")
	}
	var labels NonStandardLabels
	var ok bool
	if labels.SecType, ok = values["sec_type"]; !ok {
		return NonStandardLabels{}, errors.New("non_standard_event metric is missing sec_type label")
	}
	if labels.FRUID, ok = values["fru_id"]; !ok {
		return NonStandardLabels{}, errors.New("non_standard_event metric is missing fru_id label")
	}
	if labels.FRUText, ok = values["fru_text"]; !ok {
		return NonStandardLabels{}, errors.New("non_standard_event metric is missing fru_text label")
	}
	if labels.Severity, ok = values["severity"]; !ok {
		return NonStandardLabels{}, errors.New("non_standard_event metric is missing severity label")
	}
	return labels, nil
}
