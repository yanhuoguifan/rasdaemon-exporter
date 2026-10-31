package collector

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strconv"
)

// ARMLabels identifies one ARM processor error report series.
type ARMLabels struct {
	MPIDR        string
	Affinity     string
	ErrorType    string
	ErrorFlags   string
	RunningState string
	PSCIState    string
}

// ARMCollector stores the consumed cursor and cumulative arm_event counts.
// Serialize collection and field access.
type ARMCollector struct {
	LastID int64
	Counts map[ARMLabels]uint64
}

type armBatch struct {
	lastID int64
	rows   int
	counts map[ARMLabels]uint64
}

const (
	armMaxSeries  = 10000
	armMetricName = "rasdaemon_arm_processor_events_total"
	armMetricHelp = "Number of ARM processor error records reported by rasdaemon."
)

// ExportMetrics returns cumulative ARM report counts as one counter family.
func (c *ARMCollector) ExportMetrics() []CounterFamily {
	samples := make([]CounterSample, 0, len(c.Counts))
	for labels, value := range c.Counts {
		samples = append(samples, CounterSample{
			Labels: map[string]string{
				"mpidr":         labels.MPIDR,
				"affinity":      labels.Affinity,
				"error_type":    labels.ErrorType,
				"error_flags":   labels.ErrorFlags,
				"running_state": labels.RunningState,
				"psci_state":    labels.PSCIState,
			},
			Value: value,
		})
	}
	return []CounterFamily{{Name: armMetricName, Help: armMetricHelp, Samples: samples}}
}

// RestoreMetrics replaces ARM counts from the snapshot without reinterpreting labels.
func (c *ARMCollector) RestoreMetrics(families []CounterFamily) error {
	if len(families) == 0 {
		c.Counts = make(map[ARMLabels]uint64)
		return nil
	}
	if len(families) != 1 || families[0].Name != armMetricName {
		return errors.New("snapshot must contain exactly one arm_event metric family")
	}
	family := families[0]
	if len(family.Samples) > armMaxSeries {
		return fmt.Errorf("arm_event series limit %d exceeded", armMaxSeries)
	}
	counts := make(map[ARMLabels]uint64, len(family.Samples))
	for _, sample := range family.Samples {
		labels, err := armLabelsFromMap(sample.Labels)
		if err != nil {
			return err
		}
		counts[labels] = sample.Value
	}
	c.Counts = counts
	return nil
}

// CollectBatch merges one page only after all rows and cumulative checks pass.
func (c *ARMCollector) CollectBatch(ctx context.Context, db *sql.DB, limit int) (int, error) {
	if db == nil {
		return 0, errors.New("SQLite source is nil")
	}
	if limit <= 0 {
		return 0, errors.New("arm_event batch limit must be positive")
	}
	if c.LastID < 0 {
		return 0, errors.New("arm_event cursor must be non-negative")
	}
	batch, err := readARMBatch(ctx, db, c.LastID, limit)
	if err != nil {
		return 0, err
	}
	totals := c.Counts
	if totals == nil {
		totals = make(map[ARMLabels]uint64)
	}
	if err := mergeARMBatch(totals, batch.counts); err != nil {
		return 0, err
	}
	c.Counts = totals
	c.LastID = batch.lastID
	return batch.rows, nil
}

// readARMBatch reads and groups one ordered page in a read-only transaction.
func readARMBatch(ctx context.Context, db *sql.DB, afterID int64, limit int) (armBatch, error) {
	batch := armBatch{lastID: afterID, counts: make(map[ARMLabels]uint64)}
	queryCtx, cancel := context.WithTimeout(ctx, databaseTimeout)
	defer cancel()
	tx, err := db.BeginTx(queryCtx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return armBatch{}, fmt.Errorf("begin arm_event read transaction: %w", err)
	}
	defer tx.Rollback()

	var latestID sql.NullInt64
	err = tx.QueryRowContext(queryCtx, "SELECT id FROM arm_event ORDER BY id DESC LIMIT 1").Scan(&latestID)
	var maxID int64
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return armBatch{}, fmt.Errorf("read latest arm_event ID: %w", err)
	}
	if err == nil {
		if !latestID.Valid || latestID.Int64 <= 0 {
			return armBatch{}, errors.New("arm_event IDs must be positive integers")
		}
		maxID = latestID.Int64
	}
	if maxID < afterID {
		return armBatch{}, fmt.Errorf("arm_event maximum ID moved backwards from %d to %d", afterID, maxID)
	}
	const query = `SELECT id, mpidr, affinity, error_type, error_flags, running_state, psci_state
		FROM arm_event WHERE id > ? ORDER BY id LIMIT ?`
	rows, err := tx.QueryContext(queryCtx, query, afterID, limit)
	if err != nil {
		return armBatch{}, fmt.Errorf("query arm_event after ID %d: %w", afterID, err)
	}
	defer rows.Close()
	lastID := afterID
	for rows.Next() {
		var id int64
		var mpidr, affinity, runningState, psciState sql.NullInt64
		var errorType, errorFlags sql.NullString
		if err := rows.Scan(&id, &mpidr, &affinity, &errorType, &errorFlags, &runningState, &psciState); err != nil {
			return armBatch{}, fmt.Errorf("scan arm_event: %w", err)
		}
		if id <= lastID {
			return armBatch{}, fmt.Errorf("arm_event IDs are not strictly increasing: %d after %d", id, lastID)
		}
		labels := ARMLabels{
			MPIDR:        armMPIDR(mpidr),
			Affinity:     armDecimal(affinity),
			ErrorType:    errorType.String,
			ErrorFlags:   errorFlags.String,
			RunningState: armDecimal(runningState),
			PSCIState:    armDecimal(psciState),
		}
		batch.counts[labels]++
		batch.rows++
		lastID = id
	}
	if err := rows.Err(); err != nil {
		return armBatch{}, fmt.Errorf("iterate arm_event: %w", err)
	}
	if err := rows.Close(); err != nil {
		return armBatch{}, fmt.Errorf("close arm_event query: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return armBatch{}, fmt.Errorf("finish arm_event read transaction: %w", err)
	}
	batch.lastID = lastID
	return batch, nil
}

// armMPIDR formats valid non-negative source values as 64-bit hexadecimal.
// A negative SQL value is preserved as signed decimal rather than reinterpreted.
func armMPIDR(value sql.NullInt64) string {
	if !value.Valid {
		return ""
	}
	if value.Int64 < 0 {
		return strconv.FormatInt(value.Int64, 10)
	}
	return fmt.Sprintf("0x%016x", uint64(value.Int64))
}

func armDecimal(value sql.NullInt64) string {
	if !value.Valid {
		return ""
	}
	return strconv.FormatInt(value.Int64, 10)
}

// mergeARMBatch checks overflow and series cardinality before mutating totals.
func mergeARMBatch(totals, delta map[ARMLabels]uint64) error {
	newSeries := 0
	for labels, count := range delta {
		current, exists := totals[labels]
		if !exists {
			newSeries++
		}
		if count > math.MaxUint64-current {
			return errors.New("arm_event counter overflow")
		}
	}
	if newSeries > armMaxSeries-len(totals) {
		return fmt.Errorf("arm_event series limit %d exceeded", armMaxSeries)
	}
	for labels, count := range delta {
		totals[labels] += count
	}
	return nil
}

func armLabelsFromMap(values map[string]string) (ARMLabels, error) {
	if len(values) != 6 {
		return ARMLabels{}, errors.New("ARM metric must contain exactly six labels")
	}
	var labels ARMLabels
	var ok bool
	if labels.MPIDR, ok = values["mpidr"]; !ok {
		return ARMLabels{}, errors.New("ARM metric is missing mpidr label")
	}
	if labels.Affinity, ok = values["affinity"]; !ok {
		return ARMLabels{}, errors.New("ARM metric is missing affinity label")
	}
	if labels.ErrorType, ok = values["error_type"]; !ok {
		return ARMLabels{}, errors.New("ARM metric is missing error_type label")
	}
	if labels.ErrorFlags, ok = values["error_flags"]; !ok {
		return ARMLabels{}, errors.New("ARM metric is missing error_flags label")
	}
	if labels.RunningState, ok = values["running_state"]; !ok {
		return ARMLabels{}, errors.New("ARM metric is missing running_state label")
	}
	if labels.PSCIState, ok = values["psci_state"]; !ok {
		return ARMLabels{}, errors.New("ARM metric is missing psci_state label")
	}
	return labels, nil
}
