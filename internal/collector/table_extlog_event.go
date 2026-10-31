package collector

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
)

// ExtlogLabels groups firmware memory reports by their selected source fields.
type ExtlogLabels struct {
	EType    string
	Severity string
	FRUID    string
	FRUText  string
}

// ExtlogCollector stores the consumed cursor and cumulative report counts.
// Serialize collection and field access.
type ExtlogCollector struct {
	LastID int64
	Counts map[ExtlogLabels]uint64
}

type extlogBatch struct {
	lastID int64
	rows   int
	counts map[ExtlogLabels]uint64
}

const (
	extlogMaxSeries  = 10000
	extlogMetricName = "rasdaemon_extlog_memory_events_total"
	extlogMetricHelp = "Number of extlog memory event records reported by rasdaemon."
)

// ExportMetrics returns extlog record counts as one counter family.
func (c *ExtlogCollector) ExportMetrics() []CounterFamily {
	samples := make([]CounterSample, 0, len(c.Counts))
	for labels, value := range c.Counts {
		samples = append(samples, CounterSample{
			Labels: map[string]string{
				"etype":    labels.EType,
				"severity": labels.Severity,
				"fru_id":   labels.FRUID,
				"fru_text": labels.FRUText,
			},
			Value: value,
		})
	}
	return []CounterFamily{{Name: extlogMetricName, Help: extlogMetricHelp, Samples: samples}}
}

// RestoreMetrics replaces counts from the snapshot family without reformatting labels.
func (c *ExtlogCollector) RestoreMetrics(families []CounterFamily) error {
	if len(families) == 0 {
		c.Counts = make(map[ExtlogLabels]uint64)
		return nil
	}
	if len(families) != 1 || families[0].Name != extlogMetricName {
		return errors.New("snapshot must contain exactly one extlog_event metric family")
	}
	family := families[0]
	if len(family.Samples) > extlogMaxSeries {
		return fmt.Errorf("extlog_event series limit %d exceeded", extlogMaxSeries)
	}
	counts := make(map[ExtlogLabels]uint64, len(family.Samples))
	for _, sample := range family.Samples {
		labels, err := extlogLabelsFromMap(sample.Labels)
		if err != nil {
			return err
		}
		counts[labels] = sample.Value
	}
	c.Counts = counts
	return nil
}

// CollectBatch merges one page only after all rows and cumulative checks pass.
func (c *ExtlogCollector) CollectBatch(ctx context.Context, db *sql.DB, limit int) (int, error) {
	if db == nil {
		return 0, errors.New("SQLite source is nil")
	}
	if limit <= 0 {
		return 0, errors.New("extlog_event batch limit must be positive")
	}
	if c.LastID < 0 {
		return 0, errors.New("extlog_event cursor must be non-negative")
	}
	batch, err := readExtlogBatch(ctx, db, c.LastID, limit)
	if err != nil {
		return 0, err
	}
	totals := c.Counts
	if totals == nil {
		totals = make(map[ExtlogLabels]uint64)
	}
	if err := mergeExtlogBatch(totals, batch.counts); err != nil {
		return 0, err
	}
	c.Counts = totals
	c.LastID = batch.lastID
	return batch.rows, nil
}

// readExtlogBatch reads and groups one ordered page in a read-only transaction.
func readExtlogBatch(ctx context.Context, db *sql.DB, afterID int64, limit int) (extlogBatch, error) {
	batch := extlogBatch{lastID: afterID, counts: make(map[ExtlogLabels]uint64)}
	queryCtx, cancel := context.WithTimeout(ctx, databaseTimeout)
	defer cancel()
	tx, err := db.BeginTx(queryCtx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return extlogBatch{}, fmt.Errorf("begin extlog_event read transaction: %w", err)
	}
	defer tx.Rollback()

	var latestID sql.NullInt64
	err = tx.QueryRowContext(queryCtx, "SELECT id FROM extlog_event ORDER BY id DESC LIMIT 1").Scan(&latestID)
	var maxID int64
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return extlogBatch{}, fmt.Errorf("read latest extlog_event ID: %w", err)
	}
	if err == nil {
		if !latestID.Valid || latestID.Int64 <= 0 {
			return extlogBatch{}, errors.New("extlog_event IDs must be positive integers")
		}
		maxID = latestID.Int64
	}
	if maxID < afterID {
		return extlogBatch{}, fmt.Errorf("extlog_event maximum ID moved backwards from %d to %d", afterID, maxID)
	}
	const query = `SELECT id, etype, severity, fru_id, fru_text
		FROM extlog_event WHERE id > ? ORDER BY id LIMIT ?`
	rows, err := tx.QueryContext(queryCtx, query, afterID, limit)
	if err != nil {
		return extlogBatch{}, fmt.Errorf("query extlog_event after ID %d: %w", afterID, err)
	}
	defer rows.Close()
	lastID := afterID
	for rows.Next() {
		var id int64
		var etype, severity sql.NullInt64
		var fruID sql.Null[[]byte]
		var fruText sql.NullString
		if err := rows.Scan(&id, &etype, &severity, &fruID, &fruText); err != nil {
			return extlogBatch{}, fmt.Errorf("scan extlog_event: %w", err)
		}
		if id <= lastID {
			return extlogBatch{}, fmt.Errorf("extlog_event IDs are not strictly increasing: %d after %d", id, lastID)
		}
		guid, err := formatGUID(fruID)
		if err != nil {
			return extlogBatch{}, fmt.Errorf("extlog_event ID %d fru_id: %w", id, err)
		}
		labels := ExtlogLabels{
			EType:    extlogDecimal(etype),
			Severity: extlogDecimal(severity),
			FRUID:    guid,
			FRUText:  strings.ToValidUTF8(fruText.String, "<invalid>"),
		}
		batch.counts[labels]++
		batch.rows++
		lastID = id
	}
	if err := rows.Err(); err != nil {
		return extlogBatch{}, fmt.Errorf("iterate extlog_event: %w", err)
	}
	if err := rows.Close(); err != nil {
		return extlogBatch{}, fmt.Errorf("close extlog_event query: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return extlogBatch{}, fmt.Errorf("finish extlog_event read transaction: %w", err)
	}
	batch.lastID = lastID
	return batch, nil
}

func extlogDecimal(value sql.NullInt64) string {
	if !value.Valid {
		return ""
	}
	return fmt.Sprintf("%d", value.Int64)
}

// formatGUID formats the first 4/2/2 bytes as little-endian GUID fields.
// 33221100554477668899aabbccddeeff becomes 00112233-4455-6677-8899-aabbccddeeff.
func formatGUID(value sql.Null[[]byte]) (string, error) {
	if !value.Valid {
		return "", nil
	}
	if len(value.V) != 16 {
		return "", fmt.Errorf("GUID must be 16 bytes, got %d", len(value.V))
	}
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		binary.LittleEndian.Uint32(value.V[0:4]),
		binary.LittleEndian.Uint16(value.V[4:6]),
		binary.LittleEndian.Uint16(value.V[6:8]),
		value.V[8:10], value.V[10:16]), nil
}

func mergeExtlogBatch(totals, delta map[ExtlogLabels]uint64) error {
	newSeries := 0
	for labels, count := range delta {
		current, exists := totals[labels]
		if !exists {
			newSeries++
		}
		if count > math.MaxUint64-current {
			return errors.New("extlog_event counter overflow")
		}
	}
	if newSeries > extlogMaxSeries-len(totals) {
		return fmt.Errorf("extlog_event series limit %d exceeded", extlogMaxSeries)
	}
	for labels, count := range delta {
		totals[labels] += count
	}
	return nil
}

func extlogLabelsFromMap(values map[string]string) (ExtlogLabels, error) {
	if len(values) != 4 {
		return ExtlogLabels{}, errors.New("extlog_event metric must contain exactly four labels")
	}
	var labels ExtlogLabels
	var ok bool
	if labels.EType, ok = values["etype"]; !ok {
		return ExtlogLabels{}, errors.New("extlog_event metric is missing etype label")
	}
	if labels.Severity, ok = values["severity"]; !ok {
		return ExtlogLabels{}, errors.New("extlog_event metric is missing severity label")
	}
	if labels.FRUID, ok = values["fru_id"]; !ok {
		return ExtlogLabels{}, errors.New("extlog_event metric is missing fru_id label")
	}
	if labels.FRUText, ok = values["fru_text"]; !ok {
		return ExtlogLabels{}, errors.New("extlog_event metric is missing fru_text label")
	}
	return labels, nil
}
