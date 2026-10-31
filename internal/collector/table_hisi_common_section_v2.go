package collector

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
)

// HisiCommonLabels identifies one HiSilicon common-section report series.
type HisiCommonLabels struct {
	Version     string
	SoCID       string
	SocketID    string
	TotemID     string
	NimbusID    string
	SubSystemID string
	ModuleID    string
	SubModuleID string
	CoreID      string
	PortID      string
	ErrType     string
	PCIeInfo    string
	ErrSeverity string
	DeviceName  string
}

// HisiCommonCollector stores the consumed cursor and cumulative report counts.
// Serialize collection and field access.
type HisiCommonCollector struct {
	LastID         int64
	Counts         map[HisiCommonLabels]uint64
	DeviceResolver *AERDeviceResolver
}

type hisiCommonRecord struct {
	timestamp string
	labels    HisiCommonLabels
}

const (
	hisiCommonMaxSeries  = 10000
	hisiCommonMetricName = "rasdaemon_hisi_common_events_total"
	hisiCommonMetricHelp = "Number of HiSilicon common section event records reported by rasdaemon."
)

// ExportMetrics returns cumulative common-section report counts as one family.
func (c *HisiCommonCollector) ExportMetrics() []CounterFamily {
	samples := make([]CounterSample, 0, len(c.Counts))
	for labels, value := range c.Counts {
		samples = append(samples, CounterSample{
			Labels: map[string]string{
				"version":       labels.Version,
				"soc_id":        labels.SoCID,
				"socket_id":     labels.SocketID,
				"totem_id":      labels.TotemID,
				"nimbus_id":     labels.NimbusID,
				"sub_system_id": labels.SubSystemID,
				"module_id":     labels.ModuleID,
				"sub_module_id": labels.SubModuleID,
				"core_id":       labels.CoreID,
				"port_id":       labels.PortID,
				"err_type":      labels.ErrType,
				"pcie_info":     labels.PCIeInfo,
				"err_severity":  labels.ErrSeverity,
				"device_name":   labels.DeviceName,
			},
			Value: value,
		})
	}
	return []CounterFamily{{Name: hisiCommonMetricName, Help: hisiCommonMetricHelp, Samples: samples}}
}

// RestoreMetrics replaces counts from the snapshot without re-resolving PCI names.
func (c *HisiCommonCollector) RestoreMetrics(families []CounterFamily) error {
	if len(families) == 0 {
		c.Counts = make(map[HisiCommonLabels]uint64)
		return nil
	}
	if len(families) != 1 || families[0].Name != hisiCommonMetricName {
		return errors.New("snapshot must contain exactly one hisi_common_section_v2 metric family")
	}
	family := families[0]
	if len(family.Samples) > hisiCommonMaxSeries {
		return fmt.Errorf("hisi_common_section_v2 series limit %d exceeded", hisiCommonMaxSeries)
	}
	counts := make(map[HisiCommonLabels]uint64, len(family.Samples))
	for _, sample := range family.Samples {
		labels, err := hisiCommonLabelsFromMap(sample.Labels)
		if err != nil {
			return err
		}
		counts[labels] = sample.Value
	}
	c.Counts = counts
	return nil
}

// CollectBatch resolves PCI labels after the SQLite transaction and merges only
// after the whole page and cumulative checks succeed.
func (c *HisiCommonCollector) CollectBatch(ctx context.Context, db *sql.DB, limit int) (int, error) {
	if db == nil {
		return 0, errors.New("SQLite source is nil")
	}
	if limit <= 0 {
		return 0, errors.New("hisi_common_section_v2 batch limit must be positive")
	}
	if c.LastID < 0 {
		return 0, errors.New("hisi_common_section_v2 cursor must be non-negative")
	}
	records, lastID, err := readHisiCommonRecords(ctx, db, c.LastID, limit)
	if err != nil {
		return 0, err
	}
	if len(records) == 0 {
		return 0, nil
	}
	delta := make(map[HisiCommonLabels]uint64)
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		record.labels.DeviceName = c.DeviceResolver.Resolve(record.timestamp, record.labels.PCIeInfo)
		delta[record.labels]++
	}
	totals := c.Counts
	if totals == nil {
		totals = make(map[HisiCommonLabels]uint64)
	}
	if err := mergeHisiCommonCounts(totals, delta); err != nil {
		return 0, err
	}
	c.Counts = totals
	c.LastID = lastID
	return len(records), nil
}

// readHisiCommonRecords reads an ordered page in a short read-only transaction.
func readHisiCommonRecords(ctx context.Context, db *sql.DB, afterID int64, limit int) ([]hisiCommonRecord, int64, error) {
	queryCtx, cancel := context.WithTimeout(ctx, databaseTimeout)
	defer cancel()
	tx, err := db.BeginTx(queryCtx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, afterID, fmt.Errorf("begin hisi_common_section_v2 read transaction: %w", err)
	}
	defer tx.Rollback()

	var latestID sql.NullInt64
	err = tx.QueryRowContext(queryCtx, "SELECT id FROM hisi_common_section_v2 ORDER BY id DESC LIMIT 1").Scan(&latestID)
	var maxID int64
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, afterID, fmt.Errorf("read hisi_common_section_v2 latest ID: %w", err)
	}
	if err == nil {
		if !latestID.Valid || latestID.Int64 <= 0 {
			return nil, afterID, errors.New("hisi_common_section_v2 IDs must be positive integers")
		}
		maxID = latestID.Int64
	}
	if maxID < afterID {
		return nil, afterID, fmt.Errorf("hisi_common_section_v2 maximum ID moved backwards from %d to %d", afterID, maxID)
	}
	const query = `SELECT id, timestamp, version, soc_id, socket_id, totem_id,
		nimbus_id, sub_system_id, module_id, sub_module_id, core_id, port_id,
		err_type, pcie_info, err_severity
		FROM hisi_common_section_v2 WHERE id > ? ORDER BY id LIMIT ?`
	rows, err := tx.QueryContext(queryCtx, query, afterID, limit)
	if err != nil {
		return nil, afterID, fmt.Errorf("query hisi_common_section_v2 after ID %d: %w", afterID, err)
	}
	defer rows.Close()
	records := make([]hisiCommonRecord, 0, limit)
	lastID := afterID
	for rows.Next() {
		var id int64
		var timestamp, moduleID, pcieInfo, errSeverity sql.NullString
		var version, socID, socketID, totemID, nimbusID, subSystemID sql.NullInt64
		var subModuleID, coreID, portID, errType sql.NullInt64
		if err := rows.Scan(&id, &timestamp, &version, &socID, &socketID, &totemID,
			&nimbusID, &subSystemID, &moduleID, &subModuleID, &coreID, &portID,
			&errType, &pcieInfo, &errSeverity); err != nil {
			return nil, afterID, fmt.Errorf("scan hisi_common_section_v2: %w", err)
		}
		if id <= lastID {
			return nil, afterID, fmt.Errorf("hisi_common_section_v2 IDs are not strictly increasing: %d after %d", id, lastID)
		}
		records = append(records, hisiCommonRecord{
			timestamp: timestamp.String,
			labels: HisiCommonLabels{
				Version:     hip08Integer(version, false),
				SoCID:       hip08Integer(socID, false),
				SocketID:    hip08Integer(socketID, false),
				TotemID:     hip08Integer(totemID, false),
				NimbusID:    hip08Integer(nimbusID, false),
				SubSystemID: hip08Integer(subSystemID, false),
				ModuleID:    moduleID.String,
				SubModuleID: hip08Integer(subModuleID, false),
				CoreID:      hip08Integer(coreID, false),
				PortID:      hip08Integer(portID, false),
				ErrType:     hip08Integer(errType, false),
				PCIeInfo:    pcieInfo.String,
				ErrSeverity: errSeverity.String,
			},
		})
		lastID = id
	}
	if err := rows.Err(); err != nil {
		return nil, afterID, fmt.Errorf("iterate hisi_common_section_v2: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, afterID, fmt.Errorf("close hisi_common_section_v2 query: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, afterID, fmt.Errorf("finish hisi_common_section_v2 read transaction: %w", err)
	}
	return records, lastID, nil
}

// mergeHisiCommonCounts checks overflow and series cardinality before mutation.
func mergeHisiCommonCounts(totals, delta map[HisiCommonLabels]uint64) error {
	newSeries := 0
	for labels, count := range delta {
		current, exists := totals[labels]
		if !exists {
			newSeries++
		}
		if count > math.MaxUint64-current {
			return errors.New("hisi_common_section_v2 counter overflow")
		}
	}
	if newSeries > hisiCommonMaxSeries-len(totals) {
		return fmt.Errorf("hisi_common_section_v2 series limit %d exceeded", hisiCommonMaxSeries)
	}
	for labels, count := range delta {
		totals[labels] += count
	}
	return nil
}

func hisiCommonLabelsFromMap(values map[string]string) (HisiCommonLabels, error) {
	if len(values) != 14 {
		return HisiCommonLabels{}, errors.New("hisi_common_section_v2 metric must contain exactly fourteen labels")
	}
	var labels HisiCommonLabels
	var ok bool
	if labels.Version, ok = values["version"]; !ok {
		return HisiCommonLabels{}, errors.New("hisi_common_section_v2 metric is missing version label")
	}
	if labels.SoCID, ok = values["soc_id"]; !ok {
		return HisiCommonLabels{}, errors.New("hisi_common_section_v2 metric is missing soc_id label")
	}
	if labels.SocketID, ok = values["socket_id"]; !ok {
		return HisiCommonLabels{}, errors.New("hisi_common_section_v2 metric is missing socket_id label")
	}
	if labels.TotemID, ok = values["totem_id"]; !ok {
		return HisiCommonLabels{}, errors.New("hisi_common_section_v2 metric is missing totem_id label")
	}
	if labels.NimbusID, ok = values["nimbus_id"]; !ok {
		return HisiCommonLabels{}, errors.New("hisi_common_section_v2 metric is missing nimbus_id label")
	}
	if labels.SubSystemID, ok = values["sub_system_id"]; !ok {
		return HisiCommonLabels{}, errors.New("hisi_common_section_v2 metric is missing sub_system_id label")
	}
	if labels.ModuleID, ok = values["module_id"]; !ok {
		return HisiCommonLabels{}, errors.New("hisi_common_section_v2 metric is missing module_id label")
	}
	if labels.SubModuleID, ok = values["sub_module_id"]; !ok {
		return HisiCommonLabels{}, errors.New("hisi_common_section_v2 metric is missing sub_module_id label")
	}
	if labels.CoreID, ok = values["core_id"]; !ok {
		return HisiCommonLabels{}, errors.New("hisi_common_section_v2 metric is missing core_id label")
	}
	if labels.PortID, ok = values["port_id"]; !ok {
		return HisiCommonLabels{}, errors.New("hisi_common_section_v2 metric is missing port_id label")
	}
	if labels.ErrType, ok = values["err_type"]; !ok {
		return HisiCommonLabels{}, errors.New("hisi_common_section_v2 metric is missing err_type label")
	}
	if labels.PCIeInfo, ok = values["pcie_info"]; !ok {
		return HisiCommonLabels{}, errors.New("hisi_common_section_v2 metric is missing pcie_info label")
	}
	if labels.ErrSeverity, ok = values["err_severity"]; !ok {
		return HisiCommonLabels{}, errors.New("hisi_common_section_v2 metric is missing err_severity label")
	}
	if labels.DeviceName, ok = values["device_name"]; !ok {
		return HisiCommonLabels{}, errors.New("hisi_common_section_v2 metric is missing device_name label")
	}
	return labels, nil
}
