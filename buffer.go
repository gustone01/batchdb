package batchdb

import "sync"

// buffer 是单个表的记录缓冲区，受互斥锁保护。
type buffer struct {
	mu      sync.Mutex
	records []recordData
}

// bufferManager 管理所有表的缓冲区，使用 sync.Map 实现无锁的表级查找。
type bufferManager struct {
	buffers   sync.Map // map[string]*buffer
	batchSize int
}

// getOrCreate 获取或创建指定表的缓冲区（懒初始化）。
func (bm *bufferManager) getOrCreate(tableName string) *buffer {
	if v, ok := bm.buffers.Load(tableName); ok {
		return v.(*buffer)
	}
	v, _ := bm.buffers.LoadOrStore(tableName, &buffer{})
	return v.(*buffer)
}

// drain 原子性地取出并清空指定表的所有缓冲记录。
func (bm *bufferManager) drain(tableName string) []recordData {
	buf := bm.getOrCreate(tableName)
	buf.mu.Lock()
	defer buf.mu.Unlock()

	if len(buf.records) == 0 {
		return nil
	}

	out := make([]recordData, len(buf.records))
	copy(out, buf.records)
	buf.records = buf.records[:0]
	return out
}

// append 向指定表的缓冲区追加一条记录，返回追加后的缓冲区长度。
func (bm *bufferManager) append(tableName string, data recordData) int {
	buf := bm.getOrCreate(tableName)
	buf.mu.Lock()
	defer buf.mu.Unlock()

	buf.records = append(buf.records, data)
	return len(buf.records)
}
