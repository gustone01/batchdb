package batchdb

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gorm.io/gorm"
)

type DB struct {
	cfg       Config
	writer    Writer
	bufMgr    *bufferManager
	walMgr    *walManager
	cb        *circuitBreaker
	stats     *statsCollector
	wp        *workerPool
	ticker    *time.Ticker
	tickerDone chan struct{}
	closed    atomic.Bool
	closeOnce atomic.Bool
	inFlight  atomic.Int64
	ctx       context.Context
	cancel    context.CancelFunc
}

func New(writer Writer, cfg Config) (*DB, error) {
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	stats := &statsCollector{}
	cb := &circuitBreaker{threshold: cfg.CircuitBreakerThreshold}

	bufMgr := &bufferManager{batchSize: cfg.BatchSize}
	walMgr := newWALManager(&cfg, writer, stats, cfg.Hooks)

	wp := newWorkerPool(ctx, cancel, &cfg, bufMgr, writer, walMgr, cb, stats, cfg.Hooks)
	wp.start()

	db := &DB{
		cfg:        cfg,
		writer:     writer,
		bufMgr:     bufMgr,
		walMgr:     walMgr,
		cb:         cb,
		stats:      stats,
		wp:         wp,
		tickerDone: make(chan struct{}),
		ctx:        ctx,
		cancel:     cancel,
	}

	db.startTicker()
	walMgr.startReplayLoop()

	return db, nil
}

func NewGormDB(gormDB *gorm.DB, cfg Config) (*DB, error) {
	return New(NewGormWriter(gormDB), cfg)
}

func (db *DB) AddRecord(record Record) error {
	if db.closed.Load() {
		return ErrClosed
	}
	db.inFlight.Add(1)
	defer db.inFlight.Add(-1)

	cols, vals := record.ColumnValues()
	tableName := record.TableName()
	data := recordData{columns: cols, values: vals}

	count := db.bufMgr.append(tableName, data)

	if count >= db.cfg.MaxBufferSize {
		overflow := db.bufMgr.drain(tableName)
		if len(overflow) > 0 {
			if err := db.walMgr.Write(tableName, overflow); err != nil {
				return err
			}
		}
	} else if count >= db.cfg.BatchSize {
		db.wp.submitFlush(tableName)
	}

	return nil
}

func (db *DB) AddRecords(records []Record) error {
	if db.closed.Load() {
		return ErrClosed
	}
	db.inFlight.Add(1)
	defer db.inFlight.Add(-1)

	grouped := make(map[string][]recordData)
	for _, r := range records {
		cols, vals := r.ColumnValues()
		tableName := r.TableName()
		grouped[tableName] = append(grouped[tableName], recordData{columns: cols, values: vals})
	}

	for tableName, dataList := range grouped {
		for _, data := range dataList {
			count := db.bufMgr.append(tableName, data)
			if count >= db.cfg.MaxBufferSize {
				overflow := db.bufMgr.drain(tableName)
				if len(overflow) > 0 {
					if err := db.walMgr.Write(tableName, overflow); err != nil {
						return err
					}
				}
			} else if count >= db.cfg.BatchSize {
				db.wp.submitFlush(tableName)
			}
		}
	}
	return nil
}

func (db *DB) Flush(ctx context.Context) error {
	if db.closed.Load() {
		return ErrClosed
	}

	var tables []string
	db.bufMgr.buffers.Range(func(key, _ any) bool {
		tables = append(tables, key.(string))
		return true
	})

	var wg sync.WaitGroup
	errCh := make(chan error, len(tables))

	for _, tableName := range tables {
		records := db.bufMgr.drain(tableName)
		if len(records) == 0 {
			continue
		}

		wg.Add(1)
		go func(table string, recs []recordData) {
			defer wg.Done()
			grouped := groupRecordsByColumns(recs)
			for cols, rows := range grouped {
				columns := splitColumns(cols)
				valRows := make([][]any, len(rows))
				for i, r := range rows {
					valRows[i] = r.values
				}
				if err := db.writer.Write(ctx, table, columns, valRows); err != nil {
					errCh <- err
					return
				}
				db.stats.totalFlushed.Add(int64(len(rows)))
			}
		}(tableName, records)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) Stats() Stats {
	bufRecords := make(map[string]int)
	db.bufMgr.buffers.Range(func(key, value any) bool {
		buf := value.(*buffer)
		buf.mu.Lock()
		bufRecords[key.(string)] = len(buf.records)
		buf.mu.Unlock()
		return true
	})

	return Stats{
		BufferRecords:  bufRecords,
		TotalFlushed:   db.stats.totalFlushed.Load(),
		TotalWALWrites: db.stats.totalWALWrites.Load(),
		WALDiskUsage:   db.walMgr.DiskUsage(),
		WALFileCount:   db.walMgr.FileCount(),
		WorkersBusy:    int(db.stats.workersBusy.Load()),
	}
}

func (db *DB) Close(ctx context.Context) error {
	if !db.closeOnce.CompareAndSwap(false, true) {
		return ErrClosed
	}
	db.closed.Store(true)

	// Wait for in-flight AddRecord calls to finish
	deadline := time.After(1 * time.Second)
	for db.inFlight.Load() > 0 {
		select {
		case <-deadline:
			slog.Warn("batchdb: timeout waiting for in-flight operations")
			goto proceed
		case <-time.After(1 * time.Millisecond):
		}
	}
proceed:

	db.stopTicker()

	// Submit flush signals for all non-empty buffers
	db.bufMgr.buffers.Range(func(key, _ any) bool {
		db.wp.submitFlush(key.(string))
		return true
	})

	db.wp.stop()

	// Check context timeout - cancel worker context if needed
	select {
	case <-ctx.Done():
		db.cancel()
	default:
	}

	// Defensive sweep: anything still in buffers goes to WAL
	db.bufMgr.buffers.Range(func(key, value any) bool {
		tableName := key.(string)
		records := db.bufMgr.drain(tableName)
		if len(records) > 0 {
			if err := db.walMgr.Write(tableName, records); err != nil {
				slog.Error("batchdb: close sweep WAL write failed",
					"table", tableName, "err", err)
			}
		}
		return true
	})

	db.walMgr.stopReplay()
	db.walMgr.closeAll()

	return nil
}

func (db *DB) startTicker() {
	db.ticker = time.NewTicker(db.cfg.FlushInterval)
	go func() {
		defer close(db.tickerDone)
		for {
			select {
			case <-db.ticker.C:
				db.bufMgr.buffers.Range(func(key, value any) bool {
					buf := value.(*buffer)
					buf.mu.Lock()
					hasData := len(buf.records) > 0
					buf.mu.Unlock()
					if hasData {
						db.wp.submitFlush(key.(string))
					}
					return true
				})
			case <-db.ctx.Done():
				return
			}
		}
	}()
}

func (db *DB) stopTicker() {
	db.ticker.Stop()
	db.cancel()
	<-db.tickerDone
}

// helpers

func groupRecordsByColumns(records []recordData) map[string][]recordData {
	groups := make(map[string][]recordData)
	for _, rec := range records {
		key := joinColumns(rec.columns)
		groups[key] = append(groups[key], rec)
	}
	return groups
}

func joinColumns(cols []string) string {
	var sb strings.Builder
	for i, c := range cols {
		if i > 0 {
			sb.WriteByte(0)
		}
		sb.WriteString(c)
	}
	return sb.String()
}

func splitColumns(key string) []string {
	return strings.Split(key, "\x00")
}
