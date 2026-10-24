package collector_test

import (
	"context"
	"database/sql"
	"errors"
	"maps"
	"math"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"rasdaemon-exporter/internal/collector"
)

const mcTestSchema = `CREATE TABLE mc_event (
	id INTEGER PRIMARY KEY,
	timestamp TEXT,
	err_count INTEGER,
	err_type TEXT,
	err_msg TEXT,
	label TEXT,
	mc INTEGER,
	top_layer INTEGER,
	middle_layer INTEGER,
	lower_layer INTEGER,
	driver_detail TEXT
)`

var _ collector.MetricCodec = (*collector.MCCollector)(nil)

type mcTestRow struct {
	id           any
	timestamp    any
	errCount     any
	errType      any
	errMsg       any
	dimm         any
	mc           any
	topLayer     any
	middleLayer  any
	lowerLayer   any
	driverDetail any
}

func createMCTestDatabase(t *testing.T, path string, rows ...mcTestRow) {
	t.Helper()
	createSQLite(t, path, mcTestSchema)
	insertMCTestRows(t, path, rows...)
}

func insertMCTestRows(t *testing.T, path string, rows ...mcTestRow) {
	t.Helper()
	if len(rows) == 0 {
		return
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open synthetic mc_event database for insertion: %v", err)
	}
	db.SetMaxOpenConns(1)
	defer db.Close()

	statement := `INSERT INTO mc_event
		(id, timestamp, err_count, err_type, err_msg, label, mc,
		 top_layer, middle_layer, lower_layer, driver_detail)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	for _, row := range rows {
		if _, err := db.ExecContext(context.Background(), statement,
			row.id, row.timestamp, row.errCount, row.errType, row.errMsg, row.dimm,
			row.mc, row.topLayer, row.middleLayer, row.lowerLayer, row.driverDetail); err != nil {
			t.Fatalf("insert synthetic mc_event row %v: %v", row.id, err)
		}
	}
}

func openMCTestDatabase(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open synthetic mc_event database: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close synthetic mc_event database: %v", err)
		}
	})
	return db
}

func TestMCCollectorMapsRowsAndAccumulatesBothCounters(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mc-events.db")
	dimm := `  DIMM A1 "quoted" 雪  `
	messageOne := "ECC at address=0x1234, row 17, bank: 3; addr/cmd error, bank error; GPU 0xDEAD"
	messageTwo := "ECC at address=0x9876, row 29, bank: 9; addr/cmd error, bank error; GPU 0xDEAD"
	createMCTestDatabase(t, path,
		mcTestRow{id: 1, timestamp: "one", errCount: 4, errType: "Corrected", errMsg: messageOne,
			dimm: dimm, mc: 0, topLayer: 1, middleLayer: 2, lowerLayer: 3, driverDetail: "ignored one"},
		mcTestRow{id: 2, timestamp: "two", errCount: 0, errType: "Corrected", errMsg: messageTwo,
			dimm: dimm, mc: 0, topLayer: 1, middleLayer: 2, lowerLayer: 3, driverDetail: "ignored two"},
		mcTestRow{id: 3, errCount: 2, errType: "Uncorrected", errMsg: "UE address 0x1234", dimm: dimm,
			mc: 0, topLayer: 1, middleLayer: 2, lowerLayer: 3},
		mcTestRow{id: 4, errCount: 1, errType: "Fatal", errMsg: "fatal bank 7", dimm: "DIMM-B", mc: 1},
		mcTestRow{id: 5, errCount: 0, errType: "Deferred", errMsg: "deferred event"},
		mcTestRow{id: 6, errCount: 1, errType: "Info", errMsg: "information"},
		mcTestRow{id: 7, errCount: 3, errType: "FutureType", errMsg: "future type"},
		mcTestRow{id: 8, errCount: 1, errType: nil, errMsg: nil, dimm: nil,
			mc: nil, topLayer: nil, middleLayer: nil, lowerLayer: nil},
		mcTestRow{id: 9, errCount: 5, errType: "", errMsg: "", dimm: "",
			mc: -1, topLayer: -1, middleLayer: -2, lowerLayer: -3},
		mcTestRow{id: 10, errCount: 2, errType: "Corrected", errMsg: "addr/cmd error; bank error", dimm: dimm,
			mc: nil, topLayer: 0, middleLayer: -1, lowerLayer: 5},
	)
	digest := fileDigest(t, path)
	db := openMCTestDatabase(t, path)
	mc := collector.MCCollector{}
	if mc.LastID != 0 || len(mc.Counts) != 0 {
		t.Fatalf("zero-value MC collector = (%d, %v), want empty state", mc.LastID, mc.Counts)
	}

	rows, err := mc.CollectBatch(context.Background(), db, 500)
	if err != nil || rows != 10 {
		t.Fatalf("collect mc_event rows = (%d, %v), want (10, nil)", rows, err)
	}
	want := map[collector.MCLabels]collector.MCCounters{
		{ErrorType: "Corrected", MC: "0", TopLayer: "1", MiddleLayer: "2", LowerLayer: "3", DIMM: dimm,
			ErrMsg: "ECC at address=<omitted>, row <omitted>, bank: <omitted>; addr/cmd error, bank error; GPU 0xDEAD"}: {Events: 2, Errors: 4},
		{ErrorType: "Uncorrected", MC: "0", TopLayer: "1", MiddleLayer: "2", LowerLayer: "3", DIMM: dimm,
			ErrMsg: "UE address <omitted>"}: {Events: 1, Errors: 2},
		{ErrorType: "Fatal", MC: "1", TopLayer: "unknown", MiddleLayer: "unknown", LowerLayer: "unknown", DIMM: "DIMM-B",
			ErrMsg: "fatal bank <omitted>"}: {Events: 1, Errors: 1},
		{ErrorType: "Deferred", MC: "unknown", TopLayer: "unknown", MiddleLayer: "unknown", LowerLayer: "unknown",
			ErrMsg: "deferred event"}: {Events: 1, Errors: 0},
		{ErrorType: "Info", MC: "unknown", TopLayer: "unknown", MiddleLayer: "unknown", LowerLayer: "unknown",
			ErrMsg: "information"}: {Events: 1, Errors: 1},
		{ErrorType: "FutureType", MC: "unknown", TopLayer: "unknown", MiddleLayer: "unknown", LowerLayer: "unknown",
			ErrMsg: "future type"}: {Events: 1, Errors: 3},
		{ErrorType: "", MC: "unknown", TopLayer: "unknown", MiddleLayer: "unknown", LowerLayer: "unknown",
			ErrMsg: ""}: {Events: 2, Errors: 6},
		{ErrorType: "Corrected", MC: "unknown", TopLayer: "0", MiddleLayer: "unknown", LowerLayer: "5", DIMM: dimm,
			ErrMsg: "addr/cmd error; bank error"}: {Events: 1, Errors: 2},
	}
	if mc.LastID != 10 || !reflect.DeepEqual(mc.Counts, want) {
		t.Fatalf("MC collector = (%d, %v), want (%d, %v)", mc.LastID, mc.Counts, 10, want)
	}
	if got := fileDigest(t, path); got != digest {
		t.Fatal("collection modified the SQLite source")
	}
}

func TestMCCollectorReplacesInvalidUTF8InMessagesBeforeGrouping(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mc-invalid-utf8.db")
	messageOne := "ECC 雪\n" + string([]byte{0xff, 0xfe}) + " slot " + string([]byte{0x80, 0x81})
	messageTwo := "ECC 雪\n" + string([]byte{0x80}) + " slot " + string([]byte{0xff})
	createMCTestDatabase(t, path,
		mcTestRow{id: 1, errCount: 1, errType: "Corrected", errMsg: messageOne},
		mcTestRow{id: 2, errCount: 2, errType: "Corrected", errMsg: messageTwo},
	)
	db := openMCTestDatabase(t, path)
	mc := collector.MCCollector{}
	if rows, err := mc.CollectBatch(context.Background(), db, 500); err != nil || rows != 2 {
		t.Fatalf("collect invalid UTF-8 messages = (%d, %v), want (2, nil)", rows, err)
	}
	labels := collector.MCLabels{
		ErrorType: "Corrected", MC: "unknown", TopLayer: "unknown", MiddleLayer: "unknown",
		LowerLayer: "unknown", ErrMsg: "ECC 雪\n<invalid> slot <invalid>",
	}
	want := map[collector.MCLabels]collector.MCCounters{labels: {Events: 2, Errors: 3}}
	if !reflect.DeepEqual(mc.Counts, want) {
		t.Fatalf("normalized MC messages were not grouped: got %v, want %v", mc.Counts, want)
	}

	path = filepath.Join(t.TempDir(), "mc-invalid-utf8.prom")
	if err := collector.SaveSnapshot(path, &collector.Snapshot{Generation: 1, Families: mc.ExportMetrics()}); err != nil {
		t.Fatalf("save normalized MC snapshot: %v", err)
	}
	snapshot, err := collector.LoadSnapshot(path)
	if err != nil {
		t.Fatalf("load normalized MC snapshot: %v", err)
	}
	var restored collector.MCCollector
	if err := restored.RestoreMetrics(snapshot.Families); err != nil {
		t.Fatalf("restore normalized MC snapshot: %v", err)
	}
	if !reflect.DeepEqual(restored.Counts, want) {
		t.Fatalf("restored MC counts = %v, want %v", restored.Counts, want)
	}
}

func TestMCCollectorReadsPagesAndAddsNewLabelGroups(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mc-pages.db")
	const total = 1007
	rows := make([]mcTestRow, total)
	for index := range rows {
		id := index + 1
		if id == 1001 {
			rows[index] = mcTestRow{id: id, errCount: 9, errType: "Corrected", errMsg: "new group", dimm: "DIMM-Z"}
			continue
		}
		rows[index] = mcTestRow{id: id, errCount: id % 2, errType: "Corrected", errMsg: "same event", dimm: "DIMM-A"}
	}
	createMCTestDatabase(t, path, rows...)
	db := openMCTestDatabase(t, path)
	mc := collector.MCCollector{}
	same := collector.MCLabels{ErrorType: "Corrected", MC: "unknown", TopLayer: "unknown", MiddleLayer: "unknown",
		LowerLayer: "unknown", DIMM: "DIMM-A", ErrMsg: "same event"}
	newGroup := same
	newGroup.DIMM = "DIMM-Z"
	newGroup.ErrMsg = "new group"

	var collected int
	for collected < total {
		count, err := mc.CollectBatch(context.Background(), db, 500)
		if err != nil {
			t.Fatalf("collect page after %d rows: %v", collected, err)
		}
		if count < 1 || count > 500 {
			t.Fatalf("page returned %d rows before end; want 1..500", count)
		}
		collected += count
		if mc.LastID != int64(collected) {
			t.Fatalf("cursor after %d rows = %d", collected, mc.LastID)
		}
	}
	if mc.Counts[same] != (collector.MCCounters{Events: uint64(total - 1), Errors: uint64((total - 1) / 2)}) {
		t.Fatalf("same-label totals = %+v", mc.Counts[same])
	}
	if mc.Counts[newGroup] != (collector.MCCounters{Events: 1, Errors: 9}) {
		t.Fatalf("new-label totals = %+v", mc.Counts[newGroup])
	}
	if count, err := mc.CollectBatch(context.Background(), db, 500); err != nil || count != 0 || mc.LastID != total {
		t.Fatalf("empty page = (%d rows, cursor %d, %v), want unchanged", count, mc.LastID, err)
	}
}

func TestMCCollectorRejectsInvalidRowsWithoutApplyingBatch(t *testing.T) {
	tests := []struct {
		name string
		bad  any
	}{
		{name: "null error count", bad: nil},
		{name: "negative error count", bad: int64(-1)},
		{name: "non-integer error count", bad: "not-an-integer"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "mc-invalid.db")
			createMCTestDatabase(t, path,
				mcTestRow{id: 1, errCount: 2, errType: "Corrected", errMsg: "valid before failure"},
				mcTestRow{id: 2, errCount: test.bad, errType: "Corrected", errMsg: "invalid count"},
				mcTestRow{id: 3, errCount: 1, errType: "Corrected", errMsg: "later valid"},
			)
			db := openMCTestDatabase(t, path)
			oldLabels := collector.MCLabels{ErrMsg: "already collected"}
			mc := collector.MCCollector{LastID: 0, Counts: map[collector.MCLabels]collector.MCCounters{
				oldLabels: {Events: 7, Errors: 11},
			}}
			before := maps.Clone(mc.Counts)
			if rows, err := mc.CollectBatch(context.Background(), db, 500); err == nil || rows != 0 {
				t.Fatalf("invalid batch = (%d, %v), want scan error and no rows", rows, err)
			}
			if mc.LastID != 0 || !reflect.DeepEqual(mc.Counts, before) {
				t.Fatalf("failed batch changed collector: (%d, %v)", mc.LastID, mc.Counts)
			}

			if _, err := db.ExecContext(context.Background(), "DELETE FROM mc_event WHERE id = 2"); err != nil {
				t.Fatalf("remove invalid fixture row: %v", err)
			}
			rows, err := mc.CollectBatch(context.Background(), db, 500)
			if err != nil || rows != 2 || mc.LastID != 3 {
				t.Fatalf("retry after fixture repair = (%d, %d, %v), want two rows through ID 3", rows, mc.LastID, err)
			}
		})
	}
}

func TestMCCollectorRejectsMissingColumnsAndCancellation(t *testing.T) {
	t.Run("missing column", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "mc-missing-column.db")
		createSQLite(t, path, `CREATE TABLE mc_event (
			id INTEGER PRIMARY KEY, err_count INTEGER, err_type TEXT,
			mc INTEGER, top_layer INTEGER, middle_layer INTEGER,
			lower_layer INTEGER, label TEXT)`)
		db := openMCTestDatabase(t, path)
		mc := collector.MCCollector{}
		if rows, err := mc.CollectBatch(context.Background(), db, 500); err == nil || rows != 0 {
			t.Fatalf("missing-column query = (%d, %v), want error", rows, err)
		}
		if mc.LastID != 0 || len(mc.Counts) != 0 {
			t.Fatalf("missing-column failure changed collector: (%d, %v)", mc.LastID, mc.Counts)
		}
	})

	t.Run("canceled", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "mc-canceled.db")
		createMCTestDatabase(t, path, mcTestRow{id: 1, errCount: 1, errType: "Corrected"})
		db := openMCTestDatabase(t, path)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		mc := collector.MCCollector{}
		if rows, err := mc.CollectBatch(ctx, db, 500); !errors.Is(err, context.Canceled) || rows != 0 {
			t.Fatalf("canceled collection = (%d, %v), want context.Canceled", rows, err)
		}
		if mc.LastID != 0 || len(mc.Counts) != 0 {
			t.Fatalf("canceled collection changed collector: (%d, %v)", mc.LastID, mc.Counts)
		}
	})
}

func TestMCCollectorRejectsIDRollbackWithoutChangingCounts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mc-rollback.db")
	createMCTestDatabase(t, path, mcTestRow{id: 8, errCount: 1, errType: "Corrected", errMsg: "event"})
	db := openMCTestDatabase(t, path)
	mc := collector.MCCollector{}
	if rows, err := mc.CollectBatch(context.Background(), db, 500); err != nil || rows != 1 {
		t.Fatalf("initial collection = (%d, %v), want one row", rows, err)
	}
	before := maps.Clone(mc.Counts)
	if _, err := db.ExecContext(context.Background(), "DELETE FROM mc_event"); err != nil {
		t.Fatalf("delete rows to simulate ID rollback: %v", err)
	}
	if rows, err := mc.CollectBatch(context.Background(), db, 500); err == nil || rows != 0 {
		t.Fatalf("collection after ID rollback = (%d, %v), want error", rows, err)
	}
	if mc.LastID != 8 || !reflect.DeepEqual(mc.Counts, before) {
		t.Fatalf("ID rollback changed collector: (%d, %v)", mc.LastID, mc.Counts)
	}
}

func TestMCCollectorCounterOverflowRejectsWholeBatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mc-overflow.db")
	createMCTestDatabase(t, path, mcTestRow{id: 1, errCount: 1, errType: "Corrected", errMsg: "event"})
	db := openMCTestDatabase(t, path)
	labels := collector.MCLabels{ErrorType: "Corrected", MC: "unknown", TopLayer: "unknown", MiddleLayer: "unknown",
		LowerLayer: "unknown", ErrMsg: "event"}
	for _, initial := range []collector.MCCounters{
		{Events: math.MaxUint64, Errors: 4},
		{Events: 3, Errors: math.MaxUint64},
	} {
		mc := collector.MCCollector{Counts: map[collector.MCLabels]collector.MCCounters{labels: initial}}
		if rows, err := mc.CollectBatch(context.Background(), db, 500); err == nil || rows != 0 {
			t.Fatalf("overflowing collection from %+v = (%d, %v), want error", initial, rows, err)
		}
		if mc.LastID != 0 || !reflect.DeepEqual(mc.Counts, map[collector.MCLabels]collector.MCCounters{labels: initial}) {
			t.Fatalf("overflow partially changed collector: (%d, %v)", mc.LastID, mc.Counts)
		}
	}
}

func TestMCCollectorRejectsBatchErrorTotalOverflow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mc-batch-overflow.db")
	maximum := int64(math.MaxInt64)
	createMCTestDatabase(t, path,
		mcTestRow{id: 1, errCount: maximum, errType: "Corrected", errMsg: "same"},
		mcTestRow{id: 2, errCount: maximum, errType: "Corrected", errMsg: "same"},
		mcTestRow{id: 3, errCount: maximum, errType: "Corrected", errMsg: "same"},
	)
	db := openMCTestDatabase(t, path)
	mc := collector.MCCollector{}
	if rows, err := mc.CollectBatch(context.Background(), db, 500); err == nil || rows != 0 {
		t.Fatalf("batch with overflowing error delta = (%d, %v), want error", rows, err)
	}
	if mc.LastID != 0 || len(mc.Counts) != 0 {
		t.Fatalf("batch-delta overflow changed collector: (%d, %v)", mc.LastID, mc.Counts)
	}
}

func TestMCCollectorSeriesLimitRejectsWholeBatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mc-series-limit.db")
	createMCTestDatabase(t, path, mcTestRow{id: 10001, errCount: 1, errType: "Corrected", errMsg: "beyond limit"})
	db := openMCTestDatabase(t, path)
	counts := make(map[collector.MCLabels]collector.MCCounters, 10000)
	for index := 0; index < 10000; index++ {
		counts[collector.MCLabels{ErrMsg: "event " + strconv.Itoa(index)}] = collector.MCCounters{Events: 1}
	}
	mc := collector.MCCollector{LastID: 10000, Counts: counts}
	before := maps.Clone(mc.Counts)
	if rows, err := mc.CollectBatch(context.Background(), db, 500); err == nil || rows != 0 {
		t.Fatalf("collection beyond series limit = (%d, %v), want error", rows, err)
	}
	if mc.LastID != 10000 || !reflect.DeepEqual(mc.Counts, before) {
		t.Fatal("series-limit error partially applied the batch")
	}
}

func TestMCMetricExportAndSnapshotRestorePreserveCounterPair(t *testing.T) {
	first := collector.MCLabels{ErrorType: "Corrected", MC: "0", TopLayer: "1", MiddleLayer: "2", LowerLayer: "3",
		DIMM: "DIMM A1 雪", ErrMsg: "address <omitted>"}
	second := collector.MCLabels{ErrorType: "Uncorrected", MC: "unknown", TopLayer: "unknown", MiddleLayer: "unknown",
		LowerLayer: "unknown", DIMM: "", ErrMsg: "event"}
	mc := collector.MCCollector{LastID: 72, Counts: map[collector.MCLabels]collector.MCCounters{
		first:  {Events: 1 << 53, Errors: 0},
		second: {Events: 9, Errors: 1 << 53},
	}}
	families := mc.ExportMetrics()
	if len(families) != 2 || families[0].Name == families[1].Name {
		t.Fatalf("MC metric families = %#v, want two distinct families", families)
	}
	byName := make(map[string]collector.CounterFamily, len(families))
	for _, family := range families {
		byName[family.Name] = family
	}
	eventFamily, hasEvents := byName["rasdaemon_memory_events_total"]
	errorFamily, hasErrors := byName["rasdaemon_memory_errors_total"]
	if !hasEvents || !hasErrors || eventFamily.Help == "" || errorFamily.Help == "" {
		t.Fatalf("unexpected MC metric family names or HELP: %#v", families)
	}
	if len(eventFamily.Samples) != 2 || len(errorFamily.Samples) != 2 {
		t.Fatalf("MC samples = (%d events, %d errors), want two each", len(eventFamily.Samples), len(errorFamily.Samples))
	}
	assertMCSample(t, eventFamily, first, 1<<53)
	assertMCSample(t, eventFamily, second, 9)
	assertMCSample(t, errorFamily, first, 0)
	assertMCSample(t, errorFamily, second, 1<<53)

	// The returned samples own their label maps and cannot mutate collector state.
	eventFamily.Samples[0].Labels["dimm"] = "changed"
	eventFamily.Samples[0].Value = 4
	if !reflect.DeepEqual(mc.Counts, map[collector.MCLabels]collector.MCCounters{
		first: {Events: 1 << 53}, second: {Events: 9, Errors: 1 << 53},
	}) {
		t.Fatalf("mutating exported samples changed collector counts: %v", mc.Counts)
	}
	families = mc.ExportMetrics()

	path := filepath.Join(t.TempDir(), "mc-snapshot.prom")
	if err := collector.SaveSnapshot(path, &collector.Snapshot{Generation: 5, Families: families}); err != nil {
		t.Fatalf("save MC metric snapshot: %v", err)
	}
	snapshot, err := collector.LoadSnapshot(path)
	if err != nil {
		t.Fatalf("load MC metric snapshot: %v", err)
	}
	if snapshot.Generation != 5 {
		t.Fatalf("snapshot generation = %d, want 5", snapshot.Generation)
	}
	restored := collector.MCCollector{LastID: 91, Counts: map[collector.MCLabels]collector.MCCounters{{ErrMsg: "stale"}: {Events: 2}}}
	if err := restored.RestoreMetrics(snapshot.Families); err != nil {
		t.Fatalf("restore MC metric families: %v", err)
	}
	want := map[collector.MCLabels]collector.MCCounters{
		first: {Events: 1 << 53}, second: {Events: 9, Errors: 1 << 53},
	}
	if restored.LastID != 91 || !reflect.DeepEqual(restored.Counts, want) {
		t.Fatalf("restored MC collector = (%d, %v), want cursor 91 and %v", restored.LastID, restored.Counts, want)
	}
}

func assertMCSample(t *testing.T, family collector.CounterFamily, labels collector.MCLabels, value uint64) {
	t.Helper()
	wantLabels := map[string]string{
		"error_type": labels.ErrorType, "mc": labels.MC, "top_layer": labels.TopLayer,
		"middle_layer": labels.MiddleLayer, "lower_layer": labels.LowerLayer,
		"dimm": labels.DIMM, "err_msg": labels.ErrMsg,
	}
	for _, sample := range family.Samples {
		if reflect.DeepEqual(sample.Labels, wantLabels) {
			if sample.Value != value {
				t.Fatalf("family %s value for %#v = %d, want %d", family.Name, labels, sample.Value, value)
			}
			return
		}
	}
	t.Fatalf("family %s does not contain labels %#v", family.Name, wantLabels)
}

func TestMCRestoreValidatesFamilyPairAndLabelsAtomically(t *testing.T) {
	labels := collector.MCLabels{ErrorType: "Corrected", MC: "0", TopLayer: "1", MiddleLayer: "2", LowerLayer: "3",
		DIMM: "DIMM-A", ErrMsg: "event"}
	validLabels := map[string]string{
		"error_type": labels.ErrorType, "mc": labels.MC, "top_layer": labels.TopLayer,
		"middle_layer": labels.MiddleLayer, "lower_layer": labels.LowerLayer,
		"dimm": labels.DIMM, "err_msg": labels.ErrMsg,
	}
	events := collector.CounterFamily{Name: "rasdaemon_memory_events_total", Samples: []collector.CounterSample{{Labels: validLabels, Value: 4}}}
	errorsFamily := collector.CounterFamily{Name: "rasdaemon_memory_errors_total", Samples: []collector.CounterSample{{Labels: maps.Clone(validLabels), Value: 7}}}
	invalid := []struct {
		name     string
		families []collector.CounterFamily
	}{
		{name: "only events family", families: []collector.CounterFamily{events}},
		{name: "only errors family", families: []collector.CounterFamily{errorsFamily}},
		{name: "unexpected family", families: []collector.CounterFamily{events, {Name: "other_total"}}},
		{name: "missing label", families: []collector.CounterFamily{{Name: events.Name, Samples: []collector.CounterSample{{Labels: map[string]string{"error_type": "ce"}}}}, errorsFamily}},
		{name: "extra label", families: []collector.CounterFamily{{Name: events.Name, Samples: []collector.CounterSample{{Labels: map[string]string{
			"error_type": "ce", "mc": "0", "top_layer": "1", "middle_layer": "2", "lower_layer": "3",
			"dimm": "DIMM-A", "err_msg": "event", "node": "host",
		}}}}, errorsFamily}},
		{name: "mismatched label keys", families: []collector.CounterFamily{events, {
			Name: errorsFamily.Name,
			Samples: []collector.CounterSample{{Labels: map[string]string{
				"error_type": "ue", "mc": "0", "top_layer": "1", "middle_layer": "2", "lower_layer": "3",
				"dimm": "DIMM-A", "err_msg": "event",
			}}},
		}}},
		{name: "mismatched label counts", families: []collector.CounterFamily{events, {
			Name: errorsFamily.Name,
			Samples: []collector.CounterSample{
				{Labels: maps.Clone(validLabels), Value: 7},
				{Labels: map[string]string{
					"error_type": "ue", "mc": "0", "top_layer": "1", "middle_layer": "2", "lower_layer": "3",
					"dimm": "DIMM-A", "err_msg": "another event",
				}},
			},
		}}},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			initial := collector.MCLabels{ErrMsg: "stale"}
			mc := collector.MCCollector{LastID: 33, Counts: map[collector.MCLabels]collector.MCCounters{initial: {Events: 2, Errors: 3}}}
			before := maps.Clone(mc.Counts)
			if err := mc.RestoreMetrics(test.families); err == nil {
				t.Fatal("RestoreMetrics succeeded; want validation error")
			}
			if mc.LastID != 33 || !reflect.DeepEqual(mc.Counts, before) {
				t.Fatalf("failed restore changed collector: (%d, %v)", mc.LastID, mc.Counts)
			}
		})
	}

	t.Run("both families absent clears counts and preserves cursor", func(t *testing.T) {
		mc := collector.MCCollector{LastID: 33, Counts: map[collector.MCLabels]collector.MCCounters{{ErrMsg: "stale"}: {Events: 2}}}
		if err := mc.RestoreMetrics(nil); err != nil {
			t.Fatalf("restore absent MC families: %v", err)
		}
		if mc.LastID != 33 || len(mc.Counts) != 0 {
			t.Fatalf("restore absent families = (%d, %v), want unchanged cursor and empty counts", mc.LastID, mc.Counts)
		}
	})

	t.Run("valid pair replaces counts without changing cursor", func(t *testing.T) {
		mc := collector.MCCollector{LastID: 33, Counts: map[collector.MCLabels]collector.MCCounters{{ErrMsg: "stale"}: {Events: 2}}}
		if err := mc.RestoreMetrics([]collector.CounterFamily{errorsFamily, events}); err != nil {
			t.Fatalf("restore valid MC pair: %v", err)
		}
		if mc.LastID != 33 || !reflect.DeepEqual(mc.Counts, map[collector.MCLabels]collector.MCCounters{labels: {Events: 4, Errors: 7}}) {
			t.Fatalf("restored valid pair = (%d, %v), want cursor 33 and one paired count", mc.LastID, mc.Counts)
		}
	})
}

func TestMCRestoreEnforcesPerTableSeriesLimit(t *testing.T) {
	const seriesLimit = 10000
	events := collector.CounterFamily{Name: "rasdaemon_memory_events_total", Samples: make([]collector.CounterSample, seriesLimit+1)}
	for index := range events.Samples {
		events.Samples[index] = collector.CounterSample{Labels: map[string]string{
			"error_type": "unknown", "mc": "unknown", "top_layer": "unknown", "middle_layer": "unknown",
			"lower_layer": "unknown", "dimm": "", "err_msg": strconv.Itoa(index),
		}}
	}
	errorsFamily := collector.CounterFamily{Name: "rasdaemon_memory_errors_total"}
	mc := collector.MCCollector{LastID: 10, Counts: map[collector.MCLabels]collector.MCCounters{{ErrMsg: "stale"}: {Events: 1}}}
	before := maps.Clone(mc.Counts)
	if err := mc.RestoreMetrics([]collector.CounterFamily{events, errorsFamily}); err == nil || !strings.Contains(err.Error(), "series limit") {
		t.Fatalf("restore %d MC series = %v, want series-limit error", seriesLimit+1, err)
	}
	if mc.LastID != 10 || !reflect.DeepEqual(mc.Counts, before) {
		t.Fatal("series-limit restore changed collector")
	}
}
