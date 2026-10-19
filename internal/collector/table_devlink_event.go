package collector

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
)

// DevlinkLabels preserves the five source fields used to group devlink events.
type DevlinkLabels struct {
	BusName      string
	DevName      string
	DriverName   string
	ReporterName string
	Msg          string
}

// DevlinkCollector keeps the in-memory cursor and cumulative counts for one
// devlink_event table. Callers may inspect or restore its fields; CollectBatch
// maintains them after successful reads. Serialize collection and field access.
type DevlinkCollector struct {
	LastID int64
	Counts map[DevlinkLabels]uint64
}

type devlinkBatch struct {
	lastID int64
	rows   int
	counts map[DevlinkLabels]uint64
}

const (
	devlinkMaxSeries = 10000
)

// CollectBatch reads one bounded batch and applies it only after the complete
// read and counter checks succeed. Limit must be positive.
func (c *DevlinkCollector) CollectBatch(ctx context.Context, db *sql.DB, limit int) (int, error) {
	if db == nil {
		return 0, errors.New("SQLite source is nil")
	}
	if limit <= 0 {
		return 0, errors.New("devlink batch limit must be positive")
	}
	if c.LastID < 0 {
		return 0, errors.New("devlink cursor must be non-negative")
	}

	batch, err := readDevlinkBatch(ctx, db, c.LastID, limit)
	if err != nil {
		return 0, err
	}
	totals := c.Counts
	if totals == nil {
		totals = make(map[DevlinkLabels]uint64)
	}
	if err := mergeDevlinkBatch(totals, batch, devlinkMaxSeries); err != nil {
		return 0, err
	}
	c.Counts = totals
	c.LastID = batch.lastID
	return batch.rows, nil
}

// readDevlinkBatch reads one consistent, bounded page without modifying SQLite.
// The collector supplies its non-negative cursor and positive batch limit.
func readDevlinkBatch(ctx context.Context, db *sql.DB, afterID int64, limit int) (devlinkBatch, error) {
	batch := devlinkBatch{lastID: afterID, counts: make(map[DevlinkLabels]uint64)}

	queryCtx, cancel := context.WithTimeout(ctx, databaseTimeout)
	defer cancel()
	tx, err := db.BeginTx(queryCtx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return devlinkBatch{}, fmt.Errorf("begin devlink read transaction: %w", err)
	}
	defer tx.Rollback()

	var latestID sql.NullInt64
	err = tx.QueryRowContext(queryCtx,
		"SELECT id FROM devlink_event ORDER BY id DESC LIMIT 1").Scan(&latestID)
	var maxID int64
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return devlinkBatch{}, fmt.Errorf("read latest devlink event ID: %w", err)
	}
	if err == nil {
		if !latestID.Valid || latestID.Int64 <= 0 {
			return devlinkBatch{}, errors.New("devlink_event IDs must be positive integers")
		}
		maxID = latestID.Int64
	}
	if maxID < afterID {
		return devlinkBatch{}, fmt.Errorf("devlink_event maximum ID moved backwards from %d to %d", afterID, maxID)
	}

	rows, err := tx.QueryContext(queryCtx,
		`SELECT id, bus_name, dev_name, driver_name, reporter_name, msg
		 FROM devlink_event WHERE id > ? ORDER BY id LIMIT ?`, afterID, limit)
	if err != nil {
		return devlinkBatch{}, fmt.Errorf("query devlink events after ID %d: %w", afterID, err)
	}
	defer rows.Close()
	lastID := afterID
	for rows.Next() {
		var id int64
		var busName, devName, driverName, reporterName, msg sql.NullString
		if err := rows.Scan(&id, &busName, &devName, &driverName, &reporterName, &msg); err != nil {
			return devlinkBatch{}, fmt.Errorf("scan devlink event: %w", err)
		}
		if id <= lastID {
			return devlinkBatch{}, fmt.Errorf("devlink_event IDs are not strictly increasing: %d after %d", id, lastID)
		}
		labels := DevlinkLabels{
			BusName:      busName.String,
			DevName:      devName.String,
			DriverName:   driverName.String,
			ReporterName: reporterName.String,
			Msg:          msg.String,
		}
		batch.counts[labels]++
		batch.rows++
		lastID = id
	}
	if err := rows.Err(); err != nil {
		return devlinkBatch{}, fmt.Errorf("iterate devlink events: %w", err)
	}
	if err := rows.Close(); err != nil {
		return devlinkBatch{}, fmt.Errorf("close devlink event query: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return devlinkBatch{}, fmt.Errorf("finish devlink read transaction: %w", err)
	}
	batch.lastID = lastID
	return batch, nil
}

// mergeDevlinkBatch applies deltas to the collector's counts only after checking
// counter overflow and the series limit.
func mergeDevlinkBatch(totals map[DevlinkLabels]uint64, batch devlinkBatch, maxSeries int) error {
	newSeries := 0
	for labels, delta := range batch.counts {
		current, exists := totals[labels]
		if !exists {
			newSeries++
		}
		if delta > math.MaxUint64-current {
			return errors.New("devlink counter overflow")
		}
	}
	if newSeries > maxSeries-len(totals) {
		return fmt.Errorf("devlink series limit %d exceeded", maxSeries)
	}
	for labels, delta := range batch.counts {
		totals[labels] += delta
	}
	return nil
}
