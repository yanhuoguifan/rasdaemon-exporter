package collector

import (
	"context"
	"database/sql"
	"slices"
)

// tableCollector combines one table's batch reader with its metric codec.
type tableCollector interface {
	MetricCodec
	CollectBatch(context.Context, *sql.DB, int) (int, error)
}

// tableEntry binds a registered table name to its collector-owned cursor.
type tableEntry struct {
	Name      string
	Cursor    *int64
	Collector tableCollector
}

// newTableEntries constructs registered collectors whose tables were discovered.
func newTableEntries(discovered []string) []tableEntry {
	devlink := &DevlinkCollector{}
	mc := &MCCollector{}
	aer := &AERCollector{}
	registered := []tableEntry{
		{Name: "devlink_event", Cursor: &devlink.LastID, Collector: devlink},
		{Name: "mc_event", Cursor: &mc.LastID, Collector: mc},
		{Name: "aer_event", Cursor: &aer.LastID, Collector: aer},
	}
	var entries []tableEntry
	for _, table := range registered {
		if slices.Contains(discovered, table.Name) {
			entries = append(entries, table)
		}
	}
	return entries
}

// metricFamilyOwners maps each uniquely registered metric family to its table.
func metricFamilyOwners(entries []tableEntry) map[string]int {
	owners := make(map[string]int)
	for index, entry := range entries {
		for _, family := range entry.Collector.ExportMetrics() {
			owners[family.Name] = index
		}
	}
	return owners
}
