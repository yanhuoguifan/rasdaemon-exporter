package collector

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// AERLabels identifies one PCIe AER counter series.
type AERLabels struct {
	DevName    string
	ErrType    string
	ErrMsg     string
	DeviceName string
}

// AERCollector stores the consumed cursor and cumulative aer_event counts.
// Register construction supplies DeviceResolver before collection starts.
// Serialize collection and field access.
type AERCollector struct {
	LastID         int64
	Counts         map[AERLabels]uint64
	DeviceResolver *AERDeviceResolver
}

type aerRecord struct {
	timestamp string
	labels    AERLabels
}

const (
	aerMaxSeries         = 10000
	aerMetricName        = "rasdaemon_pcie_aer_events_total"
	aerMetricHelp        = "Number of PCIe AER event records reported by rasdaemon."
	aerUnknownDeviceName = "unknown"
	rasdaemonTimeLayout  = "2006-01-02 15:04:05 -0700"
	defaultPCISysfsRoot  = "/sys/bus/pci/devices"
	kernelStatPath       = "/proc/stat"
)

var (
	aerBDFPattern = regexp.MustCompile(`^[[:xdigit:]]{4}:[[:xdigit:]]{2}:[[:xdigit:]]{2}\.[0-7]$`)
	aerTLPHeader  = regexp.MustCompile(`(?i)[ \t\r\n]*TLP[ \t]+Header[ \t]*[:=]`)
	aerLocation   = regexp.MustCompile(`(?i)(\b(?:address|addr|offset|row|column|col)\b(?:[ \t]*[:=][ \t]*|[ \t]+))(?:0x[0-9a-f]+|[0-9]+)\b`)
)

// ExportMetrics returns the cumulative AER report count by source labels.
func (c *AERCollector) ExportMetrics() []CounterFamily {
	samples := make([]CounterSample, 0, len(c.Counts))
	for labels, value := range c.Counts {
		samples = append(samples, CounterSample{
			Labels: map[string]string{
				"dev_name":    labels.DevName,
				"err_type":    labels.ErrType,
				"err_msg":     labels.ErrMsg,
				"device_name": labels.DeviceName,
			},
			Value: value,
		})
	}
	return []CounterFamily{{Name: aerMetricName, Help: aerMetricHelp, Samples: samples}}
}

// RestoreMetrics replaces AER counts from its metric family and preserves each
// saved device name; restored rows are not re-resolved against current devices.
func (c *AERCollector) RestoreMetrics(families []CounterFamily) error {
	if len(families) == 0 {
		c.Counts = make(map[AERLabels]uint64)
		return nil
	}
	if len(families) != 1 || families[0].Name != aerMetricName {
		return errors.New("snapshot must contain exactly one AER metric family")
	}
	family := families[0]
	if len(family.Samples) > aerMaxSeries {
		return fmt.Errorf("AER series limit %d exceeded", aerMaxSeries)
	}
	counts := make(map[AERLabels]uint64, len(family.Samples))
	for _, sample := range family.Samples {
		labels, err := aerLabelsFromMap(sample.Labels)
		if err != nil {
			return err
		}
		counts[labels] = sample.Value
	}
	c.Counts = counts
	return nil
}

// CollectBatch reads one bounded page, resolves only current-boot rows, then
// merges its counts and advances LastID after the complete page succeeds.
func (c *AERCollector) CollectBatch(ctx context.Context, db *sql.DB, limit int) (int, error) {
	if db == nil {
		return 0, errors.New("SQLite source is nil")
	}
	if limit <= 0 {
		return 0, errors.New("AER batch limit must be positive")
	}
	if c.LastID < 0 {
		return 0, errors.New("AER cursor must be non-negative")
	}
	records, lastID, err := readAERRecords(ctx, db, c.LastID, limit)
	if err != nil {
		return 0, err
	}
	if len(records) == 0 {
		return 0, nil
	}
	delta := make(map[AERLabels]uint64)
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		record.labels.DeviceName = c.DeviceResolver.Resolve(record.timestamp, record.labels.DevName)
		delta[record.labels]++
	}
	totals := c.Counts
	if totals == nil {
		totals = make(map[AERLabels]uint64)
	}
	if err := mergeAERBatch(totals, delta); err != nil {
		return 0, err
	}
	c.Counts = totals
	c.LastID = lastID
	return len(records), nil
}

// readAERRecords reads an ordered page in a short read-only SQLite transaction.
func readAERRecords(ctx context.Context, db *sql.DB, afterID int64, limit int) ([]aerRecord, int64, error) {
	queryCtx, cancel := context.WithTimeout(ctx, databaseTimeout)
	defer cancel()
	tx, err := db.BeginTx(queryCtx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, afterID, fmt.Errorf("begin aer_event read transaction: %w", err)
	}
	defer tx.Rollback()

	var latestID sql.NullInt64
	err = tx.QueryRowContext(queryCtx, "SELECT id FROM aer_event ORDER BY id DESC LIMIT 1").Scan(&latestID)
	var maxID int64
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, afterID, fmt.Errorf("read latest AER event ID: %w", err)
	}
	if err == nil {
		if !latestID.Valid || latestID.Int64 <= 0 {
			return nil, afterID, errors.New("aer_event IDs must be positive integers")
		}
		maxID = latestID.Int64
	}
	if maxID < afterID {
		return nil, afterID, fmt.Errorf("aer_event maximum ID moved backwards from %d to %d", afterID, maxID)
	}
	const query = `SELECT id, timestamp, dev_name, err_type, err_msg
		FROM aer_event WHERE id > ? ORDER BY id LIMIT ?`
	rows, err := tx.QueryContext(queryCtx, query, afterID, limit)
	if err != nil {
		return nil, afterID, fmt.Errorf("query AER events after ID %d: %w", afterID, err)
	}
	defer rows.Close()
	records := make([]aerRecord, 0, limit)
	lastID := afterID
	for rows.Next() {
		var record aerRecord
		var id int64
		var timestamp, devName, errType, errMsg sql.NullString
		if err := rows.Scan(&id, &timestamp, &devName, &errType, &errMsg); err != nil {
			return nil, afterID, fmt.Errorf("scan aer_event: %w", err)
		}
		if id <= lastID {
			return nil, afterID, fmt.Errorf("aer_event IDs are not strictly increasing: %d after %d", id, lastID)
		}
		record.labels = AERLabels{
			DevName: devName.String,
			ErrType: errType.String,
			ErrMsg:  compactAERMessage(errMsg.String),
		}
		record.timestamp = timestamp.String
		records = append(records, record)
		lastID = id
	}
	if err := rows.Err(); err != nil {
		return nil, afterID, fmt.Errorf("iterate aer_event: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, afterID, fmt.Errorf("close aer_event query: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, afterID, fmt.Errorf("finish aer_event read transaction: %w", err)
	}
	return records, lastID, nil
}

// compactAERMessage preserves the report text while omitting PCI payload and
// numeric internal locations that make otherwise similar events ungroupable.
func compactAERMessage(message string) string {
	message = strings.ToValidUTF8(message, "<invalid>")
	if location := aerTLPHeader.FindStringIndex(message); location != nil {
		message = message[:location[0]]
	}
	return aerLocation.ReplaceAllString(message, "${1}<omitted>")
}

// mergeAERBatch checks cardinality and overflow before mutating cumulative counts.
func mergeAERBatch(totals, delta map[AERLabels]uint64) error {
	newSeries := 0
	for labels, count := range delta {
		current, exists := totals[labels]
		if !exists {
			newSeries++
		}
		if count > math.MaxUint64-current {
			return errors.New("AER counter overflow")
		}
	}
	if newSeries > aerMaxSeries-len(totals) {
		return fmt.Errorf("AER series limit %d exceeded", aerMaxSeries)
	}
	for labels, count := range delta {
		totals[labels] += count
	}
	return nil
}

// AERDeviceResolver reads current PCI IDs from sysfs and resolves their names
// from the local pci.ids database without caching device names or BDFs.
type AERDeviceResolver struct {
	BootTime   time.Time
	SysfsRoot  string
	PCIIDsPath string
}

// Resolve returns unknown unless the event is strictly newer than host boot
// and its BDF, sysfs IDs, and local pci.ids entry can all be read.
func (r *AERDeviceResolver) Resolve(timestamp, bdf string) string {
	if r == nil || r.BootTime.IsZero() {
		return aerUnknownDeviceName
	}
	eventTime, err := time.Parse(rasdaemonTimeLayout, timestamp)
	if err != nil || !eventTime.After(r.BootTime) {
		return aerUnknownDeviceName
	}
	if !aerBDFPattern.MatchString(bdf) {
		return aerUnknownDeviceName
	}
	root := r.SysfsRoot
	if root == "" {
		root = defaultPCISysfsRoot
	}
	devicePath := filepath.Join(root, strings.ToLower(bdf))
	vendor, err := readPCIID(filepath.Join(devicePath, "vendor"))
	if err != nil {
		return aerUnknownDeviceName
	}
	device, err := readPCIID(filepath.Join(devicePath, "device"))
	if err != nil {
		return aerUnknownDeviceName
	}
	vendorName, name := localPCIDeviceName(r.PCIIDsPath, vendor, device)
	if name == "" {
		return aerUnknownDeviceName
	}
	if vendorName != "" {
		return vendorName + " " + name
	}
	return name
}

// readKernelBootTime reads the kernel's boot epoch without querying PCI config.
func readKernelBootTime() time.Time {
	file, err := os.Open(kernelStatPath)
	if err != nil {
		return time.Time{}
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) != 2 || fields[0] != "btime" {
			continue
		}
		seconds, err := strconv.ParseInt(fields[1], 10, 64)
		if err == nil && seconds > 0 {
			return time.Unix(seconds, 0)
		}
		return time.Time{}
	}
	if err := scanner.Err(); err != nil {
		return time.Time{}
	}
	return time.Time{}
}

// readPCIID reads the kernel-maintained vendor/device property; it does not
// open the PCI config attribute or access PCIe configuration space.
func readPCIID(path string) (uint16, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	value, err := strconv.ParseUint(strings.TrimSpace(string(data)), 0, 16)
	if err != nil {
		return 0, err
	}
	return uint16(value), nil
}

// localPCIDeviceName scans the local pci.ids file for one fresh sysfs ID pair.
func localPCIDeviceName(path string, wantVendor, wantDevice uint16) (string, string) {
	paths := []string{path}
	if path == "" {
		paths = []string{"/usr/share/hwdata/pci.ids", "/usr/share/misc/pci.ids"}
	}
	var file *os.File
	for _, path := range paths {
		if path == "" {
			continue
		}
		opened, err := os.Open(path)
		if err == nil {
			file = opened
			break
		}
	}
	if file == nil {
		return "", ""
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	var currentVendor uint16
	var currentVendorName string
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !strings.HasPrefix(line, "\t") {
			fields := strings.Fields(line)
			if len(fields) < 2 || len(fields[0]) != 4 {
				currentVendor = 0
				currentVendorName = ""
				continue
			}
			id, err := strconv.ParseUint(fields[0], 16, 16)
			if err != nil {
				currentVendor = 0
				currentVendorName = ""
				continue
			}
			currentVendor = uint16(id)
			currentVendorName = strings.Join(fields[1:], " ")
			continue
		}
		if currentVendor != wantVendor || strings.HasPrefix(line, "\t\t") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 || len(fields[0]) != 4 {
			continue
		}
		id, err := strconv.ParseUint(fields[0], 16, 16)
		if err != nil {
			continue
		}
		if uint16(id) == wantDevice {
			return currentVendorName, strings.Join(fields[1:], " ")
		}
	}
	if err := scanner.Err(); err != nil {
		return "", ""
	}
	return "", ""
}

func aerLabelsFromMap(values map[string]string) (AERLabels, error) {
	if len(values) != 4 {
		return AERLabels{}, errors.New("AER metric must contain exactly four labels")
	}
	var labels AERLabels
	var ok bool
	if labels.DevName, ok = values["dev_name"]; !ok {
		return AERLabels{}, errors.New("AER metric is missing dev_name label")
	}
	if labels.ErrType, ok = values["err_type"]; !ok {
		return AERLabels{}, errors.New("AER metric is missing err_type label")
	}
	if labels.ErrMsg, ok = values["err_msg"]; !ok {
		return AERLabels{}, errors.New("AER metric is missing err_msg label")
	}
	if labels.DeviceName, ok = values["device_name"]; !ok {
		return AERLabels{}, errors.New("AER metric is missing device_name label")
	}
	return labels, nil
}
