// Package batchdb 提供高吞吐批量写入能力，适用于 Go 应用向 OLAP 数据库（Doris、ClickHouse 等）
// 高频写入场景。
//
// 核心设计：
//   - 每表独立缓冲区，按表名自动分组
//   - Worker 池并发刷写，信号驱动（非轮询）
//   - WAL 持久化保障数据不丢，DB 恢复后自动重放
//   - 全局熔断器，连续失败时自动降级写 WAL，探测恢复后切回
//   - 优雅关闭，残余缓冲数据兜底写入 WAL
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

// DB 是 batchdb 的核心实例，管理缓冲区、Worker 池、WAL 和熔断器的生命周期。
// 通过 New 或 NewGormDB 创建，使用完毕后必须调用 Close 释放资源。
type DB struct {
	cfg        Config           // 运行配置（创建后不可变）
	writer     Writer           // 数据写入目标（由用户注入）
	bufMgr     *bufferManager   // 按表分组的内存缓冲管理器
	walMgr     *walManager      // WAL 文件持久化管理器
	cb         *circuitBreaker  // 全局熔断器，防止持续写入不可用的数据库
	stats      *statsCollector  // 运行时统计指标收集器
	wp         *workerPool      // 并发刷写 Worker 池
	ticker     *time.Ticker     // 定时刷写定时器（FlushInterval）
	tickerDone chan struct{}     // ticker goroutine 退出信号
	closed     atomic.Bool      // 标记 DB 是否已关闭（拒绝新写入）
	closeOnce  atomic.Bool      // 保证 Close 只执行一次
	inFlight   atomic.Int64     // 当前正在执行的 AddRecord/AddRecords 调用数
	ctx        context.Context  // 生命周期上下文，Close 时取消
	cancel     context.CancelFunc
}

// New 创建一个新的 DB 实例。writer 定义数据写入目标，cfg 控制批量行为。
// 创建成功后会自动启动 Worker 池、定时刷写和 WAL 重放循环。
func New(writer Writer, cfg Config) (*DB, error) {
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	stats := &statsCollector{}
	cb := &circuitBreaker{threshold: cfg.CircuitBreakerThreshold}

	bufMgr := &bufferManager{batchSize: cfg.BatchSize}
	walMgr, err := newWALManager(&cfg, writer, stats, cfg.Hooks)
	if err != nil {
		cancel()
		return nil, err
	}

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

// NewGormDB 是一个便捷构造函数，内部使用 GormWriter 包装 gorm.DB。
func NewGormDB(gormDB *gorm.DB, cfg Config) (*DB, error) {
	return New(NewGormWriter(gormDB), cfg)
}

// AddRecord 向缓冲区追加单条记录。当缓冲区达到 BatchSize 时触发异步刷写；
// 当达到 MaxBufferSize 时同步溢写到 WAL 以防止内存无限增长。
// DB 已关闭时返回 ErrClosed。
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

// AddRecords 批量追加多条记录，内部按表名分组后逐条写入缓冲区。
// 语义与循环调用 AddRecord 相同，但减少了锁竞争开销。
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

// Flush 将所有缓冲区中的数据同步写入数据库。
// 每个表的刷写并发执行，任一表写入失败即返回错误。
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

// Stats 返回当前运行时统计快照，包括各表缓冲区大小、刷写总数、WAL 状态等。
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

// Close 优雅关闭 DB 实例。执行顺序：
//  1. 拒绝新写入
//  2. 等待正在执行的 AddRecord 完成（最多 1s）
//  3. 向 Worker 池发送所有非空缓冲区的刷写信号并等待完成
//  4. 将仍残留在缓冲区的数据兜底写入 WAL
//  5. 停止 WAL 重放循环并关闭所有 WAL 文件句柄
//
// ctx 超时会取消 Worker 上下文，未完成的写入将降级到 WAL。
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

// startTicker 启动定时刷写 goroutine。
// 每隔 FlushInterval 扫描所有非空缓冲区并提交刷写信号，保证低频写入场景下的数据时效性。
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

// stopTicker 停止定时器并等待 ticker goroutine 退出。
func (db *DB) stopTicker() {
	db.ticker.Stop()
	db.cancel()
	<-db.tickerDone
}

// helpers

// groupRecordsByColumns 将一批记录按列结构分组，相同列列表的记录会被合并成一条批量 INSERT。
func groupRecordsByColumns(records []recordData) map[string][]recordData {
	groups := make(map[string][]recordData)
	for _, rec := range records {
		key := joinColumns(rec.columns)
		groups[key] = append(groups[key], rec)
	}
	return groups
}

// joinColumns 使用 \x00 作为分隔符将列名列表拼接为单个字符串，作为分组 key。
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

// splitColumns 将 joinColumns 生成的 key 还原为列名切片。
func splitColumns(key string) []string {
	return strings.Split(key, "\x00")
}
