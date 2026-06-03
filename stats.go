package batchdb

import "sync/atomic"

type Stats struct {
	BufferRecords  map[string]int
	TotalFlushed   int64
	TotalWALWrites int64
	WALDiskUsage   int64
	WALFileCount   int
	WorkersBusy    int
}

type statsCollector struct {
	totalFlushed   atomic.Int64
	totalWALWrites atomic.Int64
	workersBusy    atomic.Int32
}
