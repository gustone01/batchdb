package batchdb

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// workerPool 是信号驱动的并发刷写引擎。
// 收到 tableName 信号后从 bufferManager 取出数据，分批写入数据库；
// 写入失败经重试后降级到 WAL。
type workerPool struct {
	cfg      *Config          // 运行配置引用
	bufMgr   *bufferManager   // 缓冲管理器，Worker 从中 drain 数据
	writer   Writer           // 数据库写入器
	walMgr   *walManager      // WAL 管理器，写入失败时降级使用
	cb       *circuitBreaker  // 熔断器，Open 时直接跳过写入
	stats    *statsCollector  // 统计指标收集器
	hooks    *Hooks           // 生命周期回调钩子
	signalCh chan string      // 携带 tableName 的刷写信号通道（带缓冲，容量=Workers*4）
	wg       sync.WaitGroup   // 等待所有 Worker goroutine 退出
	ctx      context.Context  // 生命周期上下文
	cancel   context.CancelFunc
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

// submitFlush 发送一个非阻塞的刷写信号。信号通道满时丢弃（因为已有信号排队）。
func (wp *workerPool) submitFlush(tableName string) {
	select {
	case wp.signalCh <- tableName:
	default:
	}
}

// stop 关闭信号通道并等待所有 Worker 处理完剩余信号后退出。
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

// processTable 从缓冲区取出指定表的全部数据，按列结构分组后逐组写入。
// 同一组内的记录具有相同的列列表，可以合并到一条 INSERT 语句中。
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

// groupByColumns 将记录按列名组合分组。
// 使用 \x00 作为列名分隔符生成 key，保证不同列结构的记录分开写入。
func (wp *workerPool) groupByColumns(records []recordData) map[string][]recordData {
	groups := make(map[string][]recordData)
	for _, rec := range records {
		key := strings.Join(rec.columns, "\x00")
		groups[key] = append(groups[key], rec)
	}
	return groups
}

// writeWithRetry 执行带指数退避重试的写入。全部重试失败后降级到 WAL。
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

// fallbackToWAL 将写入失败的数据持久化到 WAL 文件，防止数据丢失。
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
