package collector

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
)

const (
	hip08OEMType1Table  = "hip08_oem_type1_event_v2"
	hip08OEMType2Table  = "hip08_oem_type2_event_v2"
	hip08OEMType1Metric = "rasdaemon_hisi_oem_type1_events_total"
	hip08OEMType2Metric = "rasdaemon_hisi_oem_type2_events_total"
	hip08OEMMaxSeries   = 10000
)

// HIP08OEMLabels holds the seven source-derived OEM event labels.
type HIP08OEMLabels struct {
	Version     string
	SoCID       string
	SocketID    string
	NimbusID    string
	ModuleID    string
	SubModuleID string
	ErrSeverity string
}

// HIP08OEMCollector owns one OEM table's cursor and counts. Use one of the
// constructors so each instance remains bound to its fixed source table.
type HIP08OEMCollector struct {
	LastID int64
	Counts map[HIP08OEMLabels]uint64

	tableName  string
	metricName string
	help       string
}

type hip08OEMBatch struct {
	lastID int64
	rows   int
	counts map[HIP08OEMLabels]uint64
}

// NewHIP08OEMType1Collector collects the HIP08 type1 OEM table.
func NewHIP08OEMType1Collector() *HIP08OEMCollector {
	return &HIP08OEMCollector{
		tableName:  hip08OEMType1Table,
		metricName: hip08OEMType1Metric,
		help:       "Number of HIP08 OEM type1 event records reported by rasdaemon.",
	}
}

// NewHIP08OEMType2Collector collects the HIP08 type2 OEM table.
func NewHIP08OEMType2Collector() *HIP08OEMCollector {
	return &HIP08OEMCollector{
		tableName:  hip08OEMType2Table,
		metricName: hip08OEMType2Metric,
		help:       "Number of HIP08 OEM type2 event records reported by rasdaemon.",
	}
}

// ExportMetrics returns this source table's cumulative records as one family.
func (c *HIP08OEMCollector) ExportMetrics() []CounterFamily {
	samples := make([]CounterSample, 0, len(c.Counts))
	for labels, value := range c.Counts {
		samples = append(samples, CounterSample{
			Labels: map[string]string{
				"version":       labels.Version,
				"soc_id":        labels.SoCID,
				"socket_id":     labels.SocketID,
				"nimbus_id":     labels.NimbusID,
				"module_id":     labels.ModuleID,
				"sub_module_id": labels.SubModuleID,
				"err_severity":  labels.ErrSeverity,
			},
			Value: value,
		})
	}
	return []CounterFamily{{Name: c.metricName, Help: c.help, Samples: samples}}
}

// RestoreMetrics replaces this table's counts without reinterpreting labels.
func (c *HIP08OEMCollector) RestoreMetrics(families []CounterFamily) error {
	if len(families) == 0 {
		c.Counts = make(map[HIP08OEMLabels]uint64)
		return nil
	}
	if len(families) != 1 || families[0].Name != c.metricName {
		return fmt.Errorf("snapshot must contain exactly one %s metric family", c.tableName)
	}
	family := families[0]
	if len(family.Samples) > hip08OEMMaxSeries {
		return fmt.Errorf("%s series limit %d exceeded", c.tableName, hip08OEMMaxSeries)
	}
	counts := make(map[HIP08OEMLabels]uint64, len(family.Samples))
	for _, sample := range family.Samples {
		labels, err := hip08OEMLabelsFromMap(sample.Labels)
		if err != nil {
			return err
		}
		counts[labels] = sample.Value
	}
	c.Counts = counts
	return nil
}

// CollectBatch counts each row once and advances only after the page succeeds.
func (c *HIP08OEMCollector) CollectBatch(ctx context.Context, db *sql.DB, limit int) (int, error) {
	if db == nil {
		return 0, errors.New("SQLite source is nil")
	}
	if limit <= 0 {
		return 0, fmt.Errorf("%s batch limit must be positive", c.tableName)
	}
	if c.LastID < 0 {
		return 0, fmt.Errorf("%s cursor must be non-negative", c.tableName)
	}
	batch, err := readHIP08OEMBatch(ctx, db, c.tableName, c.LastID, limit)
	if err != nil {
		return 0, err
	}
	totals := c.Counts
	if totals == nil {
		totals = make(map[HIP08OEMLabels]uint64)
	}
	if err := mergeHIP08OEMBatch(totals, batch.counts, c.tableName); err != nil {
		return 0, err
	}
	c.Counts = totals
	c.LastID = batch.lastID
	return batch.rows, nil
}

// readHIP08OEMBatch reads and groups one fixed-table page in a read-only transaction.
func readHIP08OEMBatch(ctx context.Context, db *sql.DB, table string, afterID int64, limit int) (hip08OEMBatch, error) {
	batch := hip08OEMBatch{lastID: afterID, counts: make(map[HIP08OEMLabels]uint64)}
	queryCtx, cancel := context.WithTimeout(ctx, databaseTimeout)
	defer cancel()
	tx, err := db.BeginTx(queryCtx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return hip08OEMBatch{}, fmt.Errorf("begin %s read transaction: %w", table, err)
	}
	defer tx.Rollback()

	var latestID sql.NullInt64
	err = tx.QueryRowContext(queryCtx, "SELECT id FROM "+table+" ORDER BY id DESC LIMIT 1").Scan(&latestID)
	var maxID int64
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return hip08OEMBatch{}, fmt.Errorf("read %s latest ID: %w", table, err)
	}
	if err == nil {
		if !latestID.Valid || latestID.Int64 <= 0 {
			return hip08OEMBatch{}, fmt.Errorf("%s IDs must be positive integers", table)
		}
		maxID = latestID.Int64
	}
	if maxID < afterID {
		return hip08OEMBatch{}, fmt.Errorf("%s maximum ID moved backwards from %d to %d", table, afterID, maxID)
	}
	query := "SELECT id, version, soc_id, socket_id, nimbus_id, module_id, sub_module_id, err_severity " +
		"FROM " + table + " WHERE id > ? ORDER BY id LIMIT ?"
	rows, err := tx.QueryContext(queryCtx, query, afterID, limit)
	if err != nil {
		return hip08OEMBatch{}, fmt.Errorf("query %s after ID %d: %w", table, afterID, err)
	}
	defer rows.Close()
	lastID := afterID
	for rows.Next() {
		var id int64
		var version, socID, socketID, nimbusID sql.NullInt64
		var moduleID, subModuleID, errSeverity sql.NullString
		if err := rows.Scan(&id, &version, &socID, &socketID, &nimbusID, &moduleID, &subModuleID, &errSeverity); err != nil {
			return hip08OEMBatch{}, fmt.Errorf("scan %s: %w", table, err)
		}
		if id <= lastID {
			return hip08OEMBatch{}, fmt.Errorf("%s IDs are not strictly increasing: %d after %d", table, id, lastID)
		}
		labels := HIP08OEMLabels{
			Version:     hip08Integer(version, false),
			SoCID:       hip08Integer(socID, false),
			SocketID:    hip08Integer(socketID, false),
			NimbusID:    hip08Integer(nimbusID, false),
			ModuleID:    moduleID.String,
			SubModuleID: subModuleID.String,
			ErrSeverity: errSeverity.String,
		}
		batch.counts[labels]++
		batch.rows++
		lastID = id
	}
	if err := rows.Err(); err != nil {
		return hip08OEMBatch{}, fmt.Errorf("iterate %s: %w", table, err)
	}
	if err := rows.Close(); err != nil {
		return hip08OEMBatch{}, fmt.Errorf("close %s query: %w", table, err)
	}
	if err := tx.Commit(); err != nil {
		return hip08OEMBatch{}, fmt.Errorf("finish %s read transaction: %w", table, err)
	}
	batch.lastID = lastID
	return batch, nil
}

// mergeHIP08OEMBatch checks overflow and series limits before mutating totals.
func mergeHIP08OEMBatch(totals, delta map[HIP08OEMLabels]uint64, table string) error {
	newSeries := 0
	for labels, count := range delta {
		current, exists := totals[labels]
		if !exists {
			newSeries++
		}
		if count > math.MaxUint64-current {
			return fmt.Errorf("%s counter overflow", table)
		}
	}
	if newSeries > hip08OEMMaxSeries-len(totals) {
		return fmt.Errorf("%s series limit %d exceeded", table, hip08OEMMaxSeries)
	}
	for labels, count := range delta {
		totals[labels] += count
	}
	return nil
}

func hip08OEMLabelsFromMap(values map[string]string) (HIP08OEMLabels, error) {
	if len(values) != 7 {
		return HIP08OEMLabels{}, errors.New("HIP08 OEM metric must contain exactly seven labels")
	}
	var labels HIP08OEMLabels
	var ok bool
	if labels.Version, ok = values["version"]; !ok {
		return HIP08OEMLabels{}, errors.New("HIP08 OEM metric is missing version label")
	}
	if labels.SoCID, ok = values["soc_id"]; !ok {
		return HIP08OEMLabels{}, errors.New("HIP08 OEM metric is missing soc_id label")
	}
	if labels.SocketID, ok = values["socket_id"]; !ok {
		return HIP08OEMLabels{}, errors.New("HIP08 OEM metric is missing socket_id label")
	}
	if labels.NimbusID, ok = values["nimbus_id"]; !ok {
		return HIP08OEMLabels{}, errors.New("HIP08 OEM metric is missing nimbus_id label")
	}
	if labels.ModuleID, ok = values["module_id"]; !ok {
		return HIP08OEMLabels{}, errors.New("HIP08 OEM metric is missing module_id label")
	}
	if labels.SubModuleID, ok = values["sub_module_id"]; !ok {
		return HIP08OEMLabels{}, errors.New("HIP08 OEM metric is missing sub_module_id label")
	}
	if labels.ErrSeverity, ok = values["err_severity"]; !ok {
		return HIP08OEMLabels{}, errors.New("HIP08 OEM metric is missing err_severity label")
	}
	return labels, nil
}
