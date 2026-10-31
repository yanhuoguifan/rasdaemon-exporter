package collector

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strconv"
)

// HIP08PCIeLocalLabels identifies one PCIe local report series.
type HIP08PCIeLocalLabels struct {
	Version     string
	SoCID       string
	SocketID    string
	NimbusID    string
	SubModuleID string
	CoreID      string
	PortID      string
	ErrType     string
	ErrSeverity string
}

// HIP08PCIeLocalCollector stores the consumed cursor and cumulative counts.
// Serialize collection and field access.
type HIP08PCIeLocalCollector struct {
	LastID int64
	Counts map[HIP08PCIeLocalLabels]uint64
}

type hip08PCIeLocalBatch struct {
	lastID int64
	rows   int
	counts map[HIP08PCIeLocalLabels]uint64
}

const (
	hip08PCIeLocalMaxSeries  = 10000
	hip08PCIeLocalMetricName = "rasdaemon_hisi_pcie_local_events_total"
	hip08PCIeLocalMetricHelp = "Number of HiSilicon HIP08 PCIe local event records reported by rasdaemon."
)

// ExportMetrics returns cumulative PCIe local report counts as one family.
func (c *HIP08PCIeLocalCollector) ExportMetrics() []CounterFamily {
	samples := make([]CounterSample, 0, len(c.Counts))
	for labels, value := range c.Counts {
		samples = append(samples, CounterSample{
			Labels: map[string]string{
				"version":       labels.Version,
				"soc_id":        labels.SoCID,
				"socket_id":     labels.SocketID,
				"nimbus_id":     labels.NimbusID,
				"sub_module_id": labels.SubModuleID,
				"core_id":       labels.CoreID,
				"port_id":       labels.PortID,
				"err_type":      labels.ErrType,
				"err_severity":  labels.ErrSeverity,
			},
			Value: value,
		})
	}
	return []CounterFamily{{Name: hip08PCIeLocalMetricName, Help: hip08PCIeLocalMetricHelp, Samples: samples}}
}

// RestoreMetrics replaces counts from the snapshot without reinterpreting labels.
func (c *HIP08PCIeLocalCollector) RestoreMetrics(families []CounterFamily) error {
	if len(families) == 0 {
		c.Counts = make(map[HIP08PCIeLocalLabels]uint64)
		return nil
	}
	if len(families) != 1 || families[0].Name != hip08PCIeLocalMetricName {
		return errors.New("snapshot must contain exactly one HIP08 PCIe local metric family")
	}
	family := families[0]
	if len(family.Samples) > hip08PCIeLocalMaxSeries {
		return fmt.Errorf("hip08_pcie_local_event_v2 series limit %d exceeded", hip08PCIeLocalMaxSeries)
	}
	counts := make(map[HIP08PCIeLocalLabels]uint64, len(family.Samples))
	for _, sample := range family.Samples {
		labels, err := hip08PCIeLocalLabelsFromMap(sample.Labels)
		if err != nil {
			return err
		}
		counts[labels] = sample.Value
	}
	c.Counts = counts
	return nil
}

// CollectBatch merges one page only after all rows and cumulative checks pass.
func (c *HIP08PCIeLocalCollector) CollectBatch(ctx context.Context, db *sql.DB, limit int) (int, error) {
	if db == nil {
		return 0, errors.New("SQLite source is nil")
	}
	if limit <= 0 {
		return 0, errors.New("hip08_pcie_local_event_v2 batch limit must be positive")
	}
	if c.LastID < 0 {
		return 0, errors.New("hip08_pcie_local_event_v2 cursor must be non-negative")
	}
	batch, err := readHIP08PCIeLocalBatch(ctx, db, c.LastID, limit)
	if err != nil {
		return 0, err
	}
	totals := c.Counts
	if totals == nil {
		totals = make(map[HIP08PCIeLocalLabels]uint64)
	}
	if err := mergeHIP08PCIeLocalBatch(totals, batch.counts); err != nil {
		return 0, err
	}
	c.Counts = totals
	c.LastID = batch.lastID
	return batch.rows, nil
}

// readHIP08PCIeLocalBatch groups one ordered page in a read-only transaction.
func readHIP08PCIeLocalBatch(ctx context.Context, db *sql.DB, afterID int64, limit int) (hip08PCIeLocalBatch, error) {
	batch := hip08PCIeLocalBatch{lastID: afterID, counts: make(map[HIP08PCIeLocalLabels]uint64)}
	queryCtx, cancel := context.WithTimeout(ctx, databaseTimeout)
	defer cancel()
	tx, err := db.BeginTx(queryCtx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return hip08PCIeLocalBatch{}, fmt.Errorf("begin hip08_pcie_local_event_v2 read transaction: %w", err)
	}
	defer tx.Rollback()

	var latestID sql.NullInt64
	err = tx.QueryRowContext(queryCtx, "SELECT id FROM hip08_pcie_local_event_v2 ORDER BY id DESC LIMIT 1").Scan(&latestID)
	var maxID int64
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return hip08PCIeLocalBatch{}, fmt.Errorf("read hip08_pcie_local_event_v2 latest ID: %w", err)
	}
	if err == nil {
		if !latestID.Valid || latestID.Int64 <= 0 {
			return hip08PCIeLocalBatch{}, errors.New("hip08_pcie_local_event_v2 IDs must be positive integers")
		}
		maxID = latestID.Int64
	}
	if maxID < afterID {
		return hip08PCIeLocalBatch{}, fmt.Errorf("hip08_pcie_local_event_v2 maximum ID moved backwards from %d to %d", afterID, maxID)
	}
	const query = `SELECT id, version, soc_id, socket_id, nimbus_id, sub_module_id,
		core_id, port_id, err_type, err_severity
		FROM hip08_pcie_local_event_v2 WHERE id > ? ORDER BY id LIMIT ?`
	rows, err := tx.QueryContext(queryCtx, query, afterID, limit)
	if err != nil {
		return hip08PCIeLocalBatch{}, fmt.Errorf("query hip08_pcie_local_event_v2 after ID %d: %w", afterID, err)
	}
	defer rows.Close()
	lastID := afterID
	for rows.Next() {
		var id int64
		var version, socID, socketID, nimbusID, coreID, portID, errType sql.NullInt64
		var subModuleID, errSeverity sql.NullString
		if err := rows.Scan(&id, &version, &socID, &socketID, &nimbusID, &subModuleID, &coreID, &portID, &errType, &errSeverity); err != nil {
			return hip08PCIeLocalBatch{}, fmt.Errorf("scan hip08_pcie_local_event_v2: %w", err)
		}
		if id <= lastID {
			return hip08PCIeLocalBatch{}, fmt.Errorf("hip08_pcie_local_event_v2 IDs are not strictly increasing: %d after %d", id, lastID)
		}
		labels := HIP08PCIeLocalLabels{
			Version:     hip08Integer(version, false),
			SoCID:       hip08Integer(socID, false),
			SocketID:    hip08Integer(socketID, false),
			NimbusID:    hip08Integer(nimbusID, false),
			SubModuleID: subModuleID.String,
			CoreID:      hip08Integer(coreID, false),
			PortID:      hip08Integer(portID, false),
			ErrType:     hip08Integer(errType, true),
			ErrSeverity: errSeverity.String,
		}
		batch.counts[labels]++
		batch.rows++
		lastID = id
	}
	if err := rows.Err(); err != nil {
		return hip08PCIeLocalBatch{}, fmt.Errorf("iterate hip08_pcie_local_event_v2: %w", err)
	}
	if err := rows.Close(); err != nil {
		return hip08PCIeLocalBatch{}, fmt.Errorf("close hip08_pcie_local_event_v2 query: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return hip08PCIeLocalBatch{}, fmt.Errorf("finish hip08_pcie_local_event_v2 read transaction: %w", err)
	}
	batch.lastID = lastID
	return batch, nil
}

// hip08Integer formats a nullable integer, keeping negative values decimal.
func hip08Integer(value sql.NullInt64, hexadecimal bool) string {
	if !value.Valid {
		return ""
	}
	if hexadecimal && value.Int64 >= 0 {
		return fmt.Sprintf("0x%x", uint64(value.Int64))
	}
	return strconv.FormatInt(value.Int64, 10)
}

func mergeHIP08PCIeLocalBatch(totals, delta map[HIP08PCIeLocalLabels]uint64) error {
	newSeries := 0
	for labels, count := range delta {
		current, exists := totals[labels]
		if !exists {
			newSeries++
		}
		if count > math.MaxUint64-current {
			return errors.New("hip08_pcie_local_event_v2 counter overflow")
		}
	}
	if newSeries > hip08PCIeLocalMaxSeries-len(totals) {
		return fmt.Errorf("hip08_pcie_local_event_v2 series limit %d exceeded", hip08PCIeLocalMaxSeries)
	}
	for labels, count := range delta {
		totals[labels] += count
	}
	return nil
}

func hip08PCIeLocalLabelsFromMap(values map[string]string) (HIP08PCIeLocalLabels, error) {
	if len(values) != 9 {
		return HIP08PCIeLocalLabels{}, errors.New("HIP08 PCIe local metric must contain exactly nine labels")
	}
	var labels HIP08PCIeLocalLabels
	var ok bool
	if labels.Version, ok = values["version"]; !ok {
		return HIP08PCIeLocalLabels{}, errors.New("HIP08 PCIe local metric is missing version label")
	}
	if labels.SoCID, ok = values["soc_id"]; !ok {
		return HIP08PCIeLocalLabels{}, errors.New("HIP08 PCIe local metric is missing soc_id label")
	}
	if labels.SocketID, ok = values["socket_id"]; !ok {
		return HIP08PCIeLocalLabels{}, errors.New("HIP08 PCIe local metric is missing socket_id label")
	}
	if labels.NimbusID, ok = values["nimbus_id"]; !ok {
		return HIP08PCIeLocalLabels{}, errors.New("HIP08 PCIe local metric is missing nimbus_id label")
	}
	if labels.SubModuleID, ok = values["sub_module_id"]; !ok {
		return HIP08PCIeLocalLabels{}, errors.New("HIP08 PCIe local metric is missing sub_module_id label")
	}
	if labels.CoreID, ok = values["core_id"]; !ok {
		return HIP08PCIeLocalLabels{}, errors.New("HIP08 PCIe local metric is missing core_id label")
	}
	if labels.PortID, ok = values["port_id"]; !ok {
		return HIP08PCIeLocalLabels{}, errors.New("HIP08 PCIe local metric is missing port_id label")
	}
	if labels.ErrType, ok = values["err_type"]; !ok {
		return HIP08PCIeLocalLabels{}, errors.New("HIP08 PCIe local metric is missing err_type label")
	}
	if labels.ErrSeverity, ok = values["err_severity"]; !ok {
		return HIP08PCIeLocalLabels{}, errors.New("HIP08 PCIe local metric is missing err_severity label")
	}
	return labels, nil
}
