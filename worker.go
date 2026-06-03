package batchdb

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"
)

type workerPool struct {
	cfg        *Config
	bufMgr     *bufferManager
	writer     Writer
	walMgr     *walManager
	cb         *circuitBreaker
	stats      *statsCollector
	hooks      *Hooks
	signalCh   chan string
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
}

func newWorkerPool(
	ctx context.Context,
	cancel context.CancelFunc,
	cfg *Config,
	bufMgr *bufferManager,
	writer Writer,
	walMgr *walManager,
	cb *circuitBreaker,
	stats *statsCollector,
	hooks *Hooks,
) *workerPool {
	wp := &workerPool{
		cfg:      cfg,
		bufMgr:   bufMgr,
		writer:   writer,
		walMgr:   walMgr,
		cb:       cb,
		stats:    stats,
		hooks:    hooks,
		signalCh: make(chan string, cfg.Workers*4),
		ctx:      ctx,
		cancel:   cancel,
	}
	return wp
}

func (wp *workerPool) start() {
	for i := 0; i < wp.cfg.Workers; i++ {
		wp.wg.Add(1)
		go wp.loop()
	}
}

func (wp *workerPool) submitFlush(tableName string) {
	select {
	case wp.signalCh <- tableName:
	default:
	}
}

func (wp *workerPool) stop() {
	close(wp.signalCh)
	wp.wg.Wait()
}

func (wp *workerPool) loop() {
	defer wp.wg.Done()
	for tableName := range wp.signalCh {
		wp.processTable(tableName)
	}
}

func (wp *workerPool) processTable(tableName string) {
	records := wp.bufMgr.drain(tableName)
	if len(records) == 0 {
		return
	}

	wp.stats.workersBusy.Add(1)
	defer wp.stats.workersBusy.Add(-1)

	grouped := wp.groupByColumns(records)

	for colKey, rows := range grouped {
		cols := strings.Split(colKey, "\x00")
		valRows := make([][]any, len(rows))
		for i, r := range rows {
			valRows[i] = r.values
		}
		wp.writeWithRetry(tableName, cols, valRows)
	}
}

func (wp *workerPool) groupByColumns(records []recordData) map[string][]recordData {
	groups := make(map[string][]recordData)
	for _, rec := range records {
		key := strings.Join(rec.columns, "\x00")
		groups[key] = append(groups[key], rec)
	}
	return groups
}

func (wp *workerPool) writeWithRetry(tableName string, cols []string, rows [][]any) {
	if wp.cb.State() == circuitOpen {
		wp.fallbackToWAL(tableName, cols, rows)
		return
	}

	var lastErr error
	for attempt := 0; attempt <= wp.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			if wp.cb.State() == circuitOpen {
				wp.fallbackToWAL(tableName, cols, rows)
				return
			}
			delay := wp.cfg.RetryBaseDelay * (1 << (attempt - 1))
			select {
			case <-wp.ctx.Done():
				wp.fallbackToWAL(tableName, cols, rows)
				return
			case <-time.After(delay):
			}
		}

		err := wp.writer.Write(wp.ctx, tableName, cols, rows)
		if err == nil {
			wp.cb.RecordSuccess()
			wp.stats.totalFlushed.Add(int64(len(rows)))
			if wp.hooks != nil && wp.hooks.OnFlush != nil {
				wp.hooks.OnFlush(tableName, len(rows), nil)
			}
			return
		}

		lastErr = err
		wp.cb.RecordFailure()
		slog.Warn("batchdb: write failed",
			"table", tableName, "attempt", attempt+1, "err", err)
	}

	slog.Error("batchdb: all retries exhausted, writing to WAL",
		"table", tableName, "rows", len(rows), "err", lastErr)
	wp.fallbackToWAL(tableName, cols, rows)

	if wp.hooks != nil && wp.hooks.OnFlush != nil {
		wp.hooks.OnFlush(tableName, len(rows), lastErr)
	}
}

func (wp *workerPool) fallbackToWAL(tableName string, cols []string, rows [][]any) {
	records := make([]recordData, len(rows))
	for i, row := range rows {
		records[i] = recordData{columns: cols, values: row}
	}
	if err := wp.walMgr.Write(tableName, records); err != nil {
		slog.Error("batchdb: WAL write also failed, data lost",
			"table", tableName, "rows", len(rows), "err", err)
	}
}
