package batchdb

import "sync/atomic"

// Stats 是运行时统计快照，通过 DB.Stats() 获取。
type Stats struct {
	BufferRecords  map[string]int // 各表当前缓冲区中的记录数
	TotalFlushed   int64          // 累计成功刷写到数据库的记录数
	TotalWALWrites int64          // 累计写入 WAL 的记录数
	WALDiskUsage   int64          // WAL 目录当前占用的磁盘字节数
	WALFileCount   int            // WAL 目录中 .wal 文件的数量
	WorkersBusy    int            // 当前正在执行刷写的 Worker 数量
}

// statsCollector 使用原子操作收集运行时指标，线程安全。
type statsCollector struct {
	totalFlushed   atomic.Int64
	totalWALWrites atomic.Int64
	workersBusy    atomic.Int32
}
