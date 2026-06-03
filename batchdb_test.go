package batchdb

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- Mock Writer ---

type mockWriter struct {
	mu       sync.Mutex
	rows     map[string]int
	failNext atomic.Bool
	pingFail atomic.Bool
}

func newMockWriter() *mockWriter {
	return &mockWriter{rows: make(map[string]int)}
}

func (m *mockWriter) Write(_ context.Context, tableName string, _ []string, rows [][]any) error {
	if m.failNext.Load() {
		return errors.New("mock: write failed")
	}
	m.mu.Lock()
	m.rows[tableName] += len(rows)
	m.mu.Unlock()
	return nil
}

func (m *mockWriter) Ping(_ context.Context) error {
	if m.pingFail.Load() {
		return errors.New("mock: ping failed")
	}
	return nil
}

func (m *mockWriter) totalRows() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	total := 0
	for _, v := range m.rows {
		total += v
	}
	return total
}

// --- Tests ---

func TestBasicAddAndFlush(t *testing.T) {
	w := newMockWriter()
	cfg := Config{
		BatchSize:     5,
		FlushInterval: 10 * time.Millisecond,
		Workers:       2,
		WALDir:        t.TempDir(),
	}

	db, err := New(w, cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	for i := 0; i < 20; i++ {
		err := db.AddRecord(&RawRecord{
			Table:   "test_table",
			Columns: []string{"id", "name"},
			Values:  []any{i, "hello"},
		})
		if err != nil {
			t.Fatalf("AddRecord failed: %v", err)
		}
	}

	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.Close(ctx); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if w.totalRows() != 20 {
		t.Errorf("expected 20 rows written, got %d", w.totalRows())
	}
}

func TestManualFlush(t *testing.T) {
	w := newMockWriter()
	cfg := Config{
		BatchSize:     1000,
		FlushInterval: 1 * time.Hour,
		Workers:       2,
		WALDir:        t.TempDir(),
	}

	db, err := New(w, cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	for i := 0; i < 10; i++ {
		_ = db.AddRecord(&RawRecord{
			Table:   "flush_test",
			Columns: []string{"x"},
			Values:  []any{i},
		})
	}

	ctx := context.Background()
	if err := db.Flush(ctx); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	if w.totalRows() != 10 {
		t.Errorf("expected 10 rows after manual flush, got %d", w.totalRows())
	}

	closeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = db.Close(closeCtx)
}

func TestWriteFailureFallbackToWAL(t *testing.T) {
	w := newMockWriter()
	w.failNext.Store(true)

	cfg := Config{
		BatchSize:               5,
		FlushInterval:           10 * time.Millisecond,
		Workers:                 2,
		MaxRetries:              1,
		CircuitBreakerThreshold: 100,
		WALDir:                  t.TempDir(),
	}

	db, err := New(w, cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	for i := 0; i < 10; i++ {
		_ = db.AddRecord(&RawRecord{
			Table:   "wal_test",
			Columns: []string{"id"},
			Values:  []any{i},
		})
	}

	time.Sleep(200 * time.Millisecond)

	stats := db.Stats()
	if stats.TotalWALWrites == 0 {
		t.Error("expected WAL writes when DB fails")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = db.Close(ctx)
}

func TestCircuitBreaker(t *testing.T) {
	w := newMockWriter()
	w.failNext.Store(true)

	cfg := Config{
		BatchSize:               2,
		FlushInterval:           5 * time.Millisecond,
		Workers:                 1,
		MaxRetries:              3,
		RetryBaseDelay:          1 * time.Millisecond,
		CircuitBreakerThreshold: 3,
		WALDir:                  t.TempDir(),
	}

	db, err := New(w, cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	for i := 0; i < 4; i++ {
		_ = db.AddRecord(&RawRecord{
			Table:   "cb_test",
			Columns: []string{"id"},
			Values:  []any{i},
		})
	}

	time.Sleep(200 * time.Millisecond)

	if db.cb.State() != circuitOpen {
		t.Errorf("expected circuit breaker to be open after repeated failures, state=%d", db.cb.State())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = db.Close(ctx)
}

func TestClosedDBRejectsWrites(t *testing.T) {
	w := newMockWriter()
	cfg := Config{
		WALDir: t.TempDir(),
	}

	db, err := New(w, cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = db.Close(ctx)

	err = db.AddRecord(&RawRecord{
		Table:   "closed_test",
		Columns: []string{"id"},
		Values:  []any{1},
	})
	if !errors.Is(err, ErrClosed) {
		t.Errorf("expected ErrClosed, got %v", err)
	}
}

func TestMultiTableWrites(t *testing.T) {
	w := newMockWriter()
	cfg := Config{
		BatchSize:     3,
		FlushInterval: 10 * time.Millisecond,
		Workers:       4,
		WALDir:        t.TempDir(),
	}

	db, err := New(w, cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	tables := []string{"table_a", "table_b", "table_c"}
	for _, tbl := range tables {
		for i := 0; i < 10; i++ {
			_ = db.AddRecord(&RawRecord{
				Table:   tbl,
				Columns: []string{"val"},
				Values:  []any{i},
			})
		}
	}

	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = db.Close(ctx)

	if w.totalRows() != 30 {
		t.Errorf("expected 30 rows across 3 tables, got %d", w.totalRows())
	}
}

func TestAddRecordsBatch(t *testing.T) {
	w := newMockWriter()
	cfg := Config{
		BatchSize:     100,
		FlushInterval: 10 * time.Millisecond,
		Workers:       2,
		WALDir:        t.TempDir(),
	}

	db, err := New(w, cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	records := make([]Record, 50)
	for i := 0; i < 50; i++ {
		records[i] = &RawRecord{
			Table:   "batch_test",
			Columns: []string{"id", "val"},
			Values:  []any{i, "data"},
		}
	}

	if err := db.AddRecords(records); err != nil {
		t.Fatalf("AddRecords failed: %v", err)
	}

	ctx := context.Background()
	if err := db.Flush(ctx); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	if w.totalRows() != 50 {
		t.Errorf("expected 50 rows, got %d", w.totalRows())
	}

	closeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = db.Close(closeCtx)
}

func TestStats(t *testing.T) {
	w := newMockWriter()
	cfg := Config{
		BatchSize:     5,
		FlushInterval: 10 * time.Millisecond,
		Workers:       2,
		WALDir:        t.TempDir(),
	}

	db, err := New(w, cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	for i := 0; i < 10; i++ {
		_ = db.AddRecord(&RawRecord{
			Table:   "stats_test",
			Columns: []string{"x"},
			Values:  []any{i},
		})
	}

	time.Sleep(100 * time.Millisecond)

	stats := db.Stats()
	if stats.TotalFlushed != 10 {
		t.Errorf("expected TotalFlushed=10, got %d", stats.TotalFlushed)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = db.Close(ctx)
}
