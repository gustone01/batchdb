package batchdb

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// walRecord 是 WAL 文件中单行 JSON 的序列化结构。
type walRecord struct {
	Columns []string `json:"columns"`
	Values  []any    `json:"values"`
}

// walWriter 管理单个表的 WAL 文件写入，支持自动轮转和 gzip 压缩。
type walWriter struct {
	mu       sync.Mutex    // 保护文件写入的并发安全
	dir      string        // 该表的 WAL 子目录路径
	file     *os.File      // 当前打开的 WAL 文件句柄
	gzWriter *gzip.Writer  // gzip 压缩写入器（WALCompress=true 时非 nil）
	bufW     *bufio.Writer // 缓冲写入器，减少系统调用次数
	rows     int           // 当前文件已写入的行数
	size     int64         // 当前文件已写入的字节数
	cfg      *Config       // 引用配置（用于判断轮转阈值）
}

// walManager 协调所有表的 WAL 写入和后台重放。
// WAL 目录结构：<WALDir>/<tableName>/<timestamp>.wal
// 损坏的记录会被移动到 <WALDir>/dead/<tableName>/ 目录。
type walManager struct {
	cfg        *Config          // 运行配置引用
	writers    sync.Map         // tableName -> *walWriter，每表一个写入器
	totalSize  atomic.Int64     // WAL 目录总大小（字节），用于容量限制判断
	replayStop chan struct{}    // 通知重放循环退出的信号
	replayDone chan struct{}    // 重放循环退出后关闭，用于等待 goroutine 结束
	probeStop  chan struct{}    // 探测循环退出信号（预留）
	probeDone  chan struct{}    // 探测循环完成信号（预留）
	writer     Writer           // 重放时使用的数据库写入器
	stats      *statsCollector  // 统计指标收集器
	hooks      *Hooks           // 生命周期回调钩子
	closed     atomic.Bool      // 标记是否已关闭
}

func newWALManager(cfg *Config, writer Writer, stats *statsCollector, hooks *Hooks) *walManager {
	wm := &walManager{
		cfg:        cfg,
		writer:     writer,
		stats:      stats,
		hooks:      hooks,
		replayStop: make(chan struct{}),
		replayDone: make(chan struct{}),
		probeStop:  make(chan struct{}),
		probeDone:  make(chan struct{}),
	}
	wm.totalSize.Store(wm.scanDiskUsage())
	return wm
}

func (wm *walManager) scanDiskUsage() int64 {
	var total int64
	_ = filepath.Walk(wm.cfg.WALDir, func(_ string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		total += info.Size()
		return nil
	})
	return total
}

// Write 将记录批量追加到对应表的 WAL 文件。
// 当 WAL 总大小超过 MaxWALSize 时返回 ErrWALFull。
func (wm *walManager) Write(tableName string, records []recordData) error {
	if len(records) == 0 {
		return nil
	}
	if wm.totalSize.Load() >= wm.cfg.MaxWALSize {
		return ErrWALFull
	}

	w := wm.getOrCreateWriter(tableName)
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, rec := range records {
		entry := walRecord{Columns: rec.columns, Values: rec.values}
		data, err := json.Marshal(entry)
		if err != nil {
			slog.Error("batchdb: WAL marshal failed", "table", tableName, "err", err)
			continue
		}

		if w.file == nil || w.rows >= wm.cfg.WALMaxFileRows || w.size >= wm.cfg.WALMaxFileSize {
			if err := w.rotate(); err != nil {
				slog.Error("batchdb: WAL rotate failed", "table", tableName, "err", err)
				return err
			}
		}

		data = append(data, '\n')
		n, err := w.bufW.Write(data)
		if err != nil {
			slog.Error("batchdb: WAL write failed", "table", tableName, "err", err)
			return err
		}
		w.rows++
		w.size += int64(n)
		wm.totalSize.Add(int64(n))
	}

	if err := w.flush(); err != nil {
		return err
	}

	wm.stats.totalWALWrites.Add(int64(len(records)))
	if wm.hooks != nil && wm.hooks.OnWALWrite != nil {
		wm.hooks.OnWALWrite(tableName, len(records))
	}
	return nil
}

func (wm *walManager) getOrCreateWriter(tableName string) *walWriter {
	if v, ok := wm.writers.Load(tableName); ok {
		return v.(*walWriter)
	}
	dir := filepath.Join(wm.cfg.WALDir, tableName)
	w := &walWriter{dir: dir, cfg: wm.cfg}
	actual, _ := wm.writers.LoadOrStore(tableName, w)
	return actual.(*walWriter)
}

// rotate 关闭当前 WAL 文件并创建新文件。文件名使用时间戳保证唯一性和顺序性。
func (w *walWriter) rotate() error {
	if err := w.closeFile(); err != nil {
		return err
	}

	if err := os.MkdirAll(w.dir, 0755); err != nil {
		return err
	}

	fname := fmt.Sprintf("%s.wal", time.Now().Format("20060102_150405.000"))
	path := filepath.Join(w.dir, fname)

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w.file = f
	w.rows = 0
	w.size = 0

	if w.cfg.WALCompress {
		w.gzWriter, _ = gzip.NewWriterLevel(f, gzip.BestSpeed)
		w.bufW = bufio.NewWriterSize(w.gzWriter, 64*1024)
	} else {
		w.gzWriter = nil
		w.bufW = bufio.NewWriterSize(f, 64*1024)
	}
	return nil
}

// flush 将缓冲数据刷入磁盘。对于 gzip 模式需要先关闭再重新打开 gzWriter，
// 以确保压缩流完整可读（支持后续重放时从任意文件断点读取）。
func (w *walWriter) flush() error {
	if w.bufW != nil {
		if err := w.bufW.Flush(); err != nil {
			return err
		}
	}
	if w.gzWriter != nil {
		if err := w.gzWriter.Close(); err != nil {
			return err
		}
		w.gzWriter, _ = gzip.NewWriterLevel(w.file, gzip.BestSpeed)
		w.bufW = bufio.NewWriterSize(w.gzWriter, 64*1024)
	}
	if w.file != nil {
		return w.file.Sync()
	}
	return nil
}

func (w *walWriter) closeFile() error {
	if w.file == nil {
		return nil
	}
	if w.bufW != nil {
		_ = w.bufW.Flush()
	}
	if w.gzWriter != nil {
		_ = w.gzWriter.Close()
	}
	err := w.file.Close()
	w.file = nil
	w.gzWriter = nil
	w.bufW = nil
	return err
}

func (wm *walManager) closeAll() {
	wm.closed.Store(true)
	wm.writers.Range(func(key, value any) bool {
		w := value.(*walWriter)
		w.mu.Lock()
		_ = w.closeFile()
		w.mu.Unlock()
		return true
	})
}

func (wm *walManager) DiskUsage() int64 {
	return wm.totalSize.Load()
}

func (wm *walManager) FileCount() int {
	var count int
	_ = filepath.Walk(wm.cfg.WALDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		if strings.HasSuffix(path, ".wal") {
			count++
		}
		return nil
	})
	return count
}

// --- Replay ---

// startReplayLoop 启动后台 WAL 重放循环。
// 首次立即尝试重放已有文件，之后按 WALProbeInterval 周期性探测 DB 可用性后重放。
func (wm *walManager) startReplayLoop() {
	go func() {
		defer close(wm.replayDone)
		wm.replayOnce()

		ticker := time.NewTicker(wm.cfg.WALProbeInterval)
		defer ticker.Stop()

		for {
			select {
			case <-wm.replayStop:
				return
			case <-ticker.C:
				if wm.hasWALFiles() {
					if err := wm.writer.Ping(context.Background()); err == nil {
						wm.replayOnce()
					}
				}
			}
		}
	}()
}

func (wm *walManager) stopReplay() {
	close(wm.replayStop)
	<-wm.replayDone
}

func (wm *walManager) hasWALFiles() bool {
	entries, err := os.ReadDir(wm.cfg.WALDir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if e.IsDir() && e.Name() != "dead" {
			subEntries, _ := os.ReadDir(filepath.Join(wm.cfg.WALDir, e.Name()))
			for _, se := range subEntries {
				if !se.IsDir() && strings.HasSuffix(se.Name(), ".wal") {
					return true
				}
			}
		}
	}
	return false
}

func (wm *walManager) replayOnce() {
	entries, err := os.ReadDir(wm.cfg.WALDir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if !entry.IsDir() || entry.Name() == "dead" {
			continue
		}
		tableName := entry.Name()
		tableDir := filepath.Join(wm.cfg.WALDir, tableName)
		wm.replayTable(tableName, tableDir)
	}
}

func (wm *walManager) replayTable(tableName, tableDir string) {
	files, err := os.ReadDir(tableDir)
	if err != nil {
		return
	}

	walFiles := make([]string, 0, len(files))
	for _, f := range files {
		if !f.IsDir() && strings.HasSuffix(f.Name(), ".wal") {
			walFiles = append(walFiles, f.Name())
		}
	}
	sort.Strings(walFiles)

	for _, fname := range walFiles {
		select {
		case <-wm.replayStop:
			return
		default:
		}

		path := filepath.Join(tableDir, fname)
		if err := wm.replayFile(tableName, path); err != nil {
			slog.Error("batchdb: WAL replay failed, will retry next round",
				"table", tableName, "file", fname, "err", err)
			return
		}

		if err := os.Remove(path); err != nil {
			slog.Error("batchdb: WAL remove failed", "file", path, "err", err)
		} else {
			info, _ := os.Stat(path)
			if info != nil {
				wm.totalSize.Add(-info.Size())
			}
		}
	}
}

// replayFile 读取并重放单个 WAL 文件中的所有记录。
// 损坏的 JSON 行会被移动到 dead-letter 目录，不阻塞其他正常记录的重放。
func (wm *walManager) replayFile(tableName, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	var reader io.Reader
	if wm.cfg.WALCompress {
		gzr, err := gzip.NewReader(f)
		if err != nil {
			slog.Warn("batchdb: WAL gzip corrupt, skipping", "file", path, "err", err)
			wm.moveToDeadLetter(tableName, path)
			return nil
		}
		defer gzr.Close()
		reader = gzr
	} else {
		reader = f
	}

	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	var batch []walRecord
	var totalReplayed int

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec walRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			slog.Warn("batchdb: WAL record corrupt, moving to dead-letter",
				"table", tableName, "err", err)
			wm.writeDeadLetterLine(tableName, line)
			continue
		}
		batch = append(batch, rec)

		if len(batch) >= wm.cfg.ReplayBatchSize {
			if err := wm.replayBatch(tableName, batch); err != nil {
				return err
			}
			totalReplayed += len(batch)
			batch = batch[:0]

			if wm.cfg.ReplayInterval > 0 {
				time.Sleep(wm.cfg.ReplayInterval)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		slog.Warn("batchdb: WAL file read error (possible truncation)",
			"file", path, "err", err)
	}

	if len(batch) > 0 {
		if err := wm.replayBatch(tableName, batch); err != nil {
			return err
		}
		totalReplayed += len(batch)
	}

	if wm.hooks != nil && wm.hooks.OnReplay != nil {
		wm.hooks.OnReplay(tableName, totalReplayed, nil)
	}
	return nil
}

// replayBatch 将一批 WAL 记录重新写入数据库。写入前会先 Ping 探测数据库可用性。
func (wm *walManager) replayBatch(tableName string, batch []walRecord) error {
	if err := wm.writer.Ping(context.Background()); err != nil {
		return fmt.Errorf("db unavailable during replay: %w", err)
	}

	grouped := make(map[string][][]any)
	colKey := make(map[string][]string)

	for _, rec := range batch {
		key := strings.Join(rec.Columns, "\x00")
		grouped[key] = append(grouped[key], rec.Values)
		if _, ok := colKey[key]; !ok {
			colKey[key] = rec.Columns
		}
	}

	for key, rows := range grouped {
		cols := colKey[key]
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := wm.writer.Write(ctx, tableName, cols, rows)
		cancel()
		if err != nil {
			return fmt.Errorf("replay write failed: %w", err)
		}
	}
	return nil
}

// moveToDeadLetter 将无法解析的 WAL 文件整体移动到 dead-letter 目录保留供人工排查。
func (wm *walManager) moveToDeadLetter(tableName, srcPath string) {
	deadDir := filepath.Join(wm.cfg.WALDir, "dead", tableName)
	_ = os.MkdirAll(deadDir, 0755)
	destPath := filepath.Join(deadDir, filepath.Base(srcPath))
	_ = os.Rename(srcPath, destPath)
}

// writeDeadLetterLine 将单条损坏的 JSON 行追加到 dead-letter 文件，避免阻塞正常重放流程。
func (wm *walManager) writeDeadLetterLine(tableName string, line []byte) {
	deadDir := filepath.Join(wm.cfg.WALDir, "dead", tableName)
	_ = os.MkdirAll(deadDir, 0755)
	deadFile := filepath.Join(deadDir, fmt.Sprintf("dead_%s.jsonl", time.Now().Format("20060102_150405")))
	f, err := os.OpenFile(deadFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = f.Write(append(line, '\n'))
}
