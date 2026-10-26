package collector

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// DiskErrorLabels identifies one exported disk error series.
type DiskErrorLabels struct {
	Dev         string
	Error       string
	RWBS        string
	DeviceName  string
	DeviceModel string
}

// DiskErrorCollector stores the consumed cursor and cumulative disk_errors.
// Register construction supplies DeviceResolver before collection starts.
// Serialize collection and field access.
type DiskErrorCollector struct {
	LastID         int64
	Counts         map[DiskErrorLabels]uint64
	DeviceResolver *DiskDeviceResolver
}

type diskErrorRecord struct {
	timestamp string
	labels    DiskErrorLabels
}

const (
	diskErrorMaxSeries    = 10000
	diskErrorMetricName   = "rasdaemon_block_io_error_events_total"
	diskErrorMetricHelp   = "Number of block I/O error records reported by rasdaemon."
	diskUnknownDevice     = "unknown"
	defaultBlockSysfsRoot = "/sys/dev/block"
)

// ExportMetrics converts grouped disk error rows to one counter family.
func (c *DiskErrorCollector) ExportMetrics() []CounterFamily {
	samples := make([]CounterSample, 0, len(c.Counts))
	for labels, value := range c.Counts {
		samples = append(samples, CounterSample{
			Labels: map[string]string{
				"dev":          labels.Dev,
				"error":        labels.Error,
				"rwbs":         labels.RWBS,
				"device_name":  labels.DeviceName,
				"device_model": labels.DeviceModel,
			},
			Value: value,
		})
	}
	return []CounterFamily{{Name: diskErrorMetricName, Help: diskErrorMetricHelp, Samples: samples}}
}

// RestoreMetrics replaces counts from the paired snapshot labels without
// resolving saved device names against current hardware.
func (c *DiskErrorCollector) RestoreMetrics(families []CounterFamily) error {
	if len(families) == 0 {
		c.Counts = make(map[DiskErrorLabels]uint64)
		return nil
	}
	if len(families) != 1 || families[0].Name != diskErrorMetricName {
		return errors.New("snapshot must contain exactly one disk_errors metric family")
	}
	family := families[0]
	if len(family.Samples) > diskErrorMaxSeries {
		return fmt.Errorf("disk_errors series limit %d exceeded", diskErrorMaxSeries)
	}
	counts := make(map[DiskErrorLabels]uint64, len(family.Samples))
	for _, sample := range family.Samples {
		labels, err := diskErrorLabelsFromMap(sample.Labels)
		if err != nil {
			return err
		}
		counts[labels] = sample.Value
	}
	c.Counts = counts
	return nil
}

// CollectBatch reads a page, resolves device labels after the SQLite
// transaction ends, and merges the page before advancing the cursor.
func (c *DiskErrorCollector) CollectBatch(ctx context.Context, db *sql.DB, limit int) (int, error) {
	if db == nil {
		return 0, errors.New("SQLite source is nil")
	}
	if limit <= 0 {
		return 0, errors.New("disk_errors batch limit must be positive")
	}
	if c.LastID < 0 {
		return 0, errors.New("disk_errors cursor must be non-negative")
	}
	records, lastID, err := readDiskErrorRecords(ctx, db, c.LastID, limit)
	if err != nil {
		return 0, err
	}
	if len(records) == 0 {
		return 0, nil
	}
	delta := make(map[DiskErrorLabels]uint64)
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		record.labels.DeviceName, record.labels.DeviceModel = c.DeviceResolver.Resolve(
			record.timestamp, record.labels.Dev,
		)
		delta[record.labels]++
	}
	totals := c.Counts
	if totals == nil {
		totals = make(map[DiskErrorLabels]uint64)
	}
	if err := mergeDiskErrorCounts(totals, delta); err != nil {
		return 0, err
	}
	c.Counts = totals
	c.LastID = lastID
	return len(records), nil
}

// readDiskErrorRecords loads one ordered page in a short read-only transaction.
func readDiskErrorRecords(ctx context.Context, db *sql.DB, afterID int64, limit int) ([]diskErrorRecord, int64, error) {
	queryCtx, cancel := context.WithTimeout(ctx, databaseTimeout)
	defer cancel()
	tx, err := db.BeginTx(queryCtx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, afterID, fmt.Errorf("begin disk_errors read transaction: %w", err)
	}
	defer tx.Rollback()

	var latestID sql.NullInt64
	err = tx.QueryRowContext(queryCtx,
		"SELECT id FROM disk_errors ORDER BY id DESC LIMIT 1").Scan(&latestID)
	var maxID int64
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, afterID, fmt.Errorf("read latest disk_errors ID: %w", err)
	}
	if err == nil {
		if !latestID.Valid || latestID.Int64 <= 0 {
			return nil, afterID, errors.New("disk_errors IDs must be positive integers")
		}
		maxID = latestID.Int64
	}
	if maxID < afterID {
		return nil, afterID, fmt.Errorf("disk_errors maximum ID moved backwards from %d to %d", afterID, maxID)
	}
	const query = `SELECT id, timestamp, dev, error, rwbs
		FROM disk_errors WHERE id > ? ORDER BY id LIMIT ?`
	rows, err := tx.QueryContext(queryCtx, query, afterID, limit)
	if err != nil {
		return nil, afterID, fmt.Errorf("query disk_errors after ID %d: %w", afterID, err)
	}
	defer rows.Close()
	records := make([]diskErrorRecord, 0, limit)
	lastID := afterID
	for rows.Next() {
		var id int64
		var timestamp, dev, errorText, rwbs sql.NullString
		if err := rows.Scan(&id, &timestamp, &dev, &errorText, &rwbs); err != nil {
			return nil, afterID, fmt.Errorf("scan disk_errors: %w", err)
		}
		if id <= lastID {
			return nil, afterID, fmt.Errorf("disk_errors IDs are not strictly increasing: %d after %d", id, lastID)
		}
		records = append(records, diskErrorRecord{
			timestamp: timestamp.String,
			labels: DiskErrorLabels{
				Dev:   dev.String,
				Error: strings.ToValidUTF8(errorText.String, "<invalid>"),
				RWBS:  rwbs.String,
			},
		})
		lastID = id
	}
	if err := rows.Err(); err != nil {
		return nil, afterID, fmt.Errorf("iterate disk_errors: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, afterID, fmt.Errorf("close disk_errors query: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, afterID, fmt.Errorf("finish disk_errors read transaction: %w", err)
	}
	return records, lastID, nil
}

// mergeDiskErrorCounts validates limits and overflow before changing totals.
func mergeDiskErrorCounts(totals, delta map[DiskErrorLabels]uint64) error {
	newSeries := 0
	for labels, count := range delta {
		current, exists := totals[labels]
		if !exists {
			newSeries++
		}
		if count > math.MaxUint64-current {
			return errors.New("disk_errors counter overflow")
		}
	}
	if newSeries > diskErrorMaxSeries-len(totals) {
		return fmt.Errorf("disk_errors series limit %d exceeded", diskErrorMaxSeries)
	}
	for labels, count := range delta {
		totals[labels] += count
	}
	return nil
}

// DiskDeviceResolver reads current block sysfs names and model without keeping
// a device-number cache or opening device nodes.
type DiskDeviceResolver struct {
	BootTime  time.Time
	SysfsRoot string
}

// Resolve returns current device labels only for events newer than this boot.
func (r *DiskDeviceResolver) Resolve(timestamp, dev string) (string, string) {
	if r == nil || r.BootTime.IsZero() {
		return diskUnknownDevice, diskUnknownDevice
	}
	eventTime, err := time.Parse(rasdaemonTimeLayout, timestamp)
	if err != nil || !eventTime.After(r.BootTime) {
		return diskUnknownDevice, diskUnknownDevice
	}
	major, minor, ok := parseBlockDeviceNumber(dev)
	if !ok {
		return diskUnknownDevice, diskUnknownDevice
	}
	root := r.SysfsRoot
	if root == "" {
		root = defaultBlockSysfsRoot
	}
	link := filepath.Join(root, fmt.Sprintf("%d:%d", major, minor))
	blockPath, err := filepath.EvalSymlinks(link)
	if err != nil {
		return diskUnknownDevice, diskUnknownDevice
	}
	deviceName := filepath.Base(blockPath)
	modelBlockPath := blockPath
	if _, err := os.Stat(filepath.Join(blockPath, "partition")); err == nil {
		modelBlockPath = filepath.Dir(blockPath)
	}
	model, err := os.ReadFile(filepath.Join(modelBlockPath, "device", "model"))
	if err != nil {
		return deviceName, diskUnknownDevice
	}
	modelName := strings.TrimSpace(string(model))
	if modelName == "" {
		return deviceName, diskUnknownDevice
	}
	return deviceName, modelName
}

// parseBlockDeviceNumber validates rasdaemon's decimal major:minor form.
func parseBlockDeviceNumber(value string) (uint64, uint64, bool) {
	majorText, minorText, ok := strings.Cut(value, ":")
	if !ok || majorText == "" || minorText == "" || strings.Contains(minorText, ":") {
		return 0, 0, false
	}
	major, err := strconv.ParseUint(majorText, 10, 32)
	if err != nil {
		return 0, 0, false
	}
	minor, err := strconv.ParseUint(minorText, 10, 32)
	if err != nil {
		return 0, 0, false
	}
	return major, minor, true
}

func diskErrorLabelsFromMap(values map[string]string) (DiskErrorLabels, error) {
	if len(values) != 5 {
		return DiskErrorLabels{}, errors.New("disk_errors metric must contain exactly five labels")
	}
	var labels DiskErrorLabels
	var ok bool
	if labels.Dev, ok = values["dev"]; !ok {
		return DiskErrorLabels{}, errors.New("disk_errors metric is missing dev label")
	}
	if labels.Error, ok = values["error"]; !ok {
		return DiskErrorLabels{}, errors.New("disk_errors metric is missing error label")
	}
	if labels.RWBS, ok = values["rwbs"]; !ok {
		return DiskErrorLabels{}, errors.New("disk_errors metric is missing rwbs label")
	}
	if labels.DeviceName, ok = values["device_name"]; !ok {
		return DiskErrorLabels{}, errors.New("disk_errors metric is missing device_name label")
	}
	if labels.DeviceModel, ok = values["device_model"]; !ok {
		return DiskErrorLabels{}, errors.New("disk_errors metric is missing device_model label")
	}
	return labels, nil
}
