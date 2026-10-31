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

// MCELabels groups Machine Check records by the selected source fields.
type MCELabels struct {
	CPUVendor    string
	CPUID        string
	SocketID     string
	CPU          string
	Bank         string
	BankName     string
	ErrorMsg     string
	MCIStatusMsg string
	MCAStatusMsg string
	MCLocation   string
}

// MCECollector stores the consumed cursor and cumulative mce_record counts.
// Serialize collection and field access.
type MCECollector struct {
	LastID int64
	Counts map[MCELabels]uint64
}

type mceBatch struct {
	lastID int64
	rows   int
	counts map[MCELabels]uint64
}

const (
	mceMaxSeries  = 10000
	mceMetricName = "rasdaemon_mce_events_total"
	mceMetricHelp = "Number of Machine Check records reported by rasdaemon."
)

var (
	mceLocationValue = regexp.MustCompile(`(?i)(\b(?:address|addr|offset|row|column|col)\b(?:[ \t]*[:=][ \t]*|[ \t]+))(?:0x[0-9a-f]+|[0-9]+)\b`)
	mceSyndromeValue = regexp.MustCompile(`(?i)(\b(?:syndrome|synd)\b(?:[ \t]*[:=][ \t]*|[ \t]+))(?:0x[0-9a-f]+|[0-9a-f]+)\b`)
	mceErrorCount    = regexp.MustCompile(`(?i)(\bn_errors\b(?:[ \t]*[:=][ \t]*|[ \t]+))[0-9]+\b`)
)

// ExportMetrics returns cumulative MCE report counts as one counter family.
func (c *MCECollector) ExportMetrics() []CounterFamily {
	samples := make([]CounterSample, 0, len(c.Counts))
	for labels, value := range c.Counts {
		samples = append(samples, CounterSample{
			Labels: map[string]string{
				"cpuvendor":     labels.CPUVendor,
				"cpuid":         labels.CPUID,
				"socketid":      labels.SocketID,
				"cpu":           labels.CPU,
				"bank":          labels.Bank,
				"bank_name":     labels.BankName,
				"error_msg":     labels.ErrorMsg,
				"mcistatus_msg": labels.MCIStatusMsg,
				"mcastatus_msg": labels.MCAStatusMsg,
				"mc_location":   labels.MCLocation,
			},
			Value: value,
		})
	}
	return []CounterFamily{{Name: mceMetricName, Help: mceMetricHelp, Samples: samples}}
}

// RestoreMetrics replaces MCE counts from its snapshot family and preserves
// the saved label text without reinterpreting messages.
func (c *MCECollector) RestoreMetrics(families []CounterFamily) error {
	if len(families) == 0 {
		c.Counts = make(map[MCELabels]uint64)
		return nil
	}
	if len(families) != 1 || families[0].Name != mceMetricName {
		return errors.New("snapshot must contain exactly one MCE metric family")
	}
	family := families[0]
	if len(family.Samples) > mceMaxSeries {
		return fmt.Errorf("mce_record series limit %d exceeded", mceMaxSeries)
	}
	counts := make(map[MCELabels]uint64, len(family.Samples))
	for _, sample := range family.Samples {
		labels, err := mceLabelsFromMap(sample.Labels)
		if err != nil {
			return err
		}
		counts[labels] = sample.Value
	}
	c.Counts = counts
	return nil
}

// CollectBatch reads one bounded page and merges only after the full page and
// cumulative checks succeed, so errors never partially advance counts.
func (c *MCECollector) CollectBatch(ctx context.Context, db *sql.DB, limit int) (int, error) {
	if db == nil {
		return 0, errors.New("SQLite source is nil")
	}
	if limit <= 0 {
		return 0, errors.New("mce_record batch limit must be positive")
	}
	if c.LastID < 0 {
		return 0, errors.New("mce_record cursor must be non-negative")
	}
	batch, err := readMCEBatch(ctx, db, c.LastID, limit)
	if err != nil {
		return 0, err
	}
	totals := c.Counts
	if totals == nil {
		totals = make(map[MCELabels]uint64)
	}
	if err := mergeMCEBatch(totals, batch); err != nil {
		return 0, err
	}
	c.Counts = totals
	c.LastID = batch.lastID
	return batch.rows, nil
}

// readMCEBatch scans one ordered page in a read-only transaction and groups
// records before touching the collector's cumulative map.
func readMCEBatch(ctx context.Context, db *sql.DB, afterID int64, limit int) (mceBatch, error) {
	batch := mceBatch{lastID: afterID, counts: make(map[MCELabels]uint64)}
	queryCtx, cancel := context.WithTimeout(ctx, databaseTimeout)
	defer cancel()
	tx, err := db.BeginTx(queryCtx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return mceBatch{}, fmt.Errorf("begin mce_record read transaction: %w", err)
	}
	defer tx.Rollback()

	var latestID sql.NullInt64
	err = tx.QueryRowContext(queryCtx, "SELECT id FROM mce_record ORDER BY id DESC LIMIT 1").Scan(&latestID)
	var maxID int64
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return mceBatch{}, fmt.Errorf("read latest mce_record ID: %w", err)
	}
	if err == nil {
		if !latestID.Valid || latestID.Int64 <= 0 {
			return mceBatch{}, errors.New("mce_record IDs must be positive integers")
		}
		maxID = latestID.Int64
	}
	if maxID < afterID {
		return mceBatch{}, fmt.Errorf("mce_record maximum ID moved backwards from %d to %d", afterID, maxID)
	}
	const query = `SELECT id, cpuvendor, cpuid, socketid, cpu, bank, bank_name,
		error_msg, mcistatus_msg, mcastatus_msg, mc_location
		FROM mce_record WHERE id > ? ORDER BY id LIMIT ?`
	rows, err := tx.QueryContext(queryCtx, query, afterID, limit)
	if err != nil {
		return mceBatch{}, fmt.Errorf("query mce_record after ID %d: %w", afterID, err)
	}
	defer rows.Close()
	lastID := afterID
	for rows.Next() {
		var id int64
		var cpuVendor, cpuid, socketID, cpu, bank sql.NullInt64
		var bankName, errorMsg, mciStatusMsg, mcaStatusMsg, mcLocation sql.NullString
		if err := rows.Scan(
			&id, &cpuVendor, &cpuid, &socketID, &cpu, &bank, &bankName,
			&errorMsg, &mciStatusMsg, &mcaStatusMsg, &mcLocation,
		); err != nil {
			return mceBatch{}, fmt.Errorf("scan mce_record: %w", err)
		}
		if id <= lastID {
			return mceBatch{}, fmt.Errorf("mce_record IDs are not strictly increasing: %d after %d", id, lastID)
		}
		labels := MCELabels{
			CPUVendor:    mceDecimal(cpuVendor),
			CPUID:        mceCPUID(cpuid),
			SocketID:     mceDecimal(socketID),
			CPU:          mceDecimal(cpu),
			Bank:         mceDecimal(bank),
			BankName:     bankName.String,
			ErrorMsg:     compactMCEMessage(errorMsg.String),
			MCIStatusMsg: strings.ToValidUTF8(mciStatusMsg.String, "<invalid>"),
			MCAStatusMsg: compactMCEMessage(mcaStatusMsg.String),
			MCLocation:   compactMCEMessage(mcLocation.String),
		}
		batch.counts[labels]++
		batch.rows++
		lastID = id
	}
	if err := rows.Err(); err != nil {
		return mceBatch{}, fmt.Errorf("iterate mce_record: %w", err)
	}
	if err := rows.Close(); err != nil {
		return mceBatch{}, fmt.Errorf("close mce_record query: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return mceBatch{}, fmt.Errorf("finish mce_record read transaction: %w", err)
	}
	batch.lastID = lastID
	return batch, nil
}

func mceDecimal(value sql.NullInt64) string {
	if !value.Valid {
		return ""
	}
	return strconv.FormatInt(value.Int64, 10)
}

// mceCPUID uses conventional hexadecimal for non-negative CPUID values while
// leaving a negative SQLite value visible as signed decimal.
func mceCPUID(value sql.NullInt64) string {
	if !value.Valid {
		return ""
	}
	if value.Int64 < 0 {
		return strconv.FormatInt(value.Int64, 10)
	}
	return fmt.Sprintf("0x%08x", uint64(value.Int64))
}

// compactMCEMessage keeps the description while removing only the reviewed
// address, syndrome, and per-message n_errors values.
func compactMCEMessage(message string) string {
	message = strings.ToValidUTF8(message, "<invalid>")
	message = mceLocationValue.ReplaceAllString(message, "${1}<omitted>")
	message = mceSyndromeValue.ReplaceAllString(message, "${1}<omitted>")
	return mceErrorCount.ReplaceAllString(message, "${1}<omitted>")
}

// mergeMCEBatch checks overflow and series cardinality before mutating totals.
func mergeMCEBatch(totals map[MCELabels]uint64, batch mceBatch) error {
	newSeries := 0
	for labels, count := range batch.counts {
		current, exists := totals[labels]
		if !exists {
			newSeries++
		}
		if count > math.MaxUint64-current {
			return errors.New("mce_record counter overflow")
		}
	}
	if newSeries > mceMaxSeries-len(totals) {
		return fmt.Errorf("mce_record series limit %d exceeded", mceMaxSeries)
	}
	for labels, count := range batch.counts {
		totals[labels] += count
	}
	return nil
}

func mceLabelsFromMap(values map[string]string) (MCELabels, error) {
	if len(values) != 10 {
		return MCELabels{}, errors.New("MCE metric must contain exactly ten labels")
	}
	var labels MCELabels
	var ok bool
	if labels.CPUVendor, ok = values["cpuvendor"]; !ok {
		return MCELabels{}, errors.New("MCE metric is missing cpuvendor label")
	}
	if labels.CPUID, ok = values["cpuid"]; !ok {
		return MCELabels{}, errors.New("MCE metric is missing cpuid label")
	}
	if labels.SocketID, ok = values["socketid"]; !ok {
		return MCELabels{}, errors.New("MCE metric is missing socketid label")
	}
	if labels.CPU, ok = values["cpu"]; !ok {
		return MCELabels{}, errors.New("MCE metric is missing cpu label")
	}
	if labels.Bank, ok = values["bank"]; !ok {
		return MCELabels{}, errors.New("MCE metric is missing bank label")
	}
	if labels.BankName, ok = values["bank_name"]; !ok {
		return MCELabels{}, errors.New("MCE metric is missing bank_name label")
	}
	if labels.ErrorMsg, ok = values["error_msg"]; !ok {
		return MCELabels{}, errors.New("MCE metric is missing error_msg label")
	}
	if labels.MCIStatusMsg, ok = values["mcistatus_msg"]; !ok {
		return MCELabels{}, errors.New("MCE metric is missing mcistatus_msg label")
	}
	if labels.MCAStatusMsg, ok = values["mcastatus_msg"]; !ok {
		return MCELabels{}, errors.New("MCE metric is missing mcastatus_msg label")
	}
	if labels.MCLocation, ok = values["mc_location"]; !ok {
		return MCELabels{}, errors.New("MCE metric is missing mc_location label")
	}
	return labels, nil
}
