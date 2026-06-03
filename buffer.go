package batchdb

import "sync"

type buffer struct {
	mu      sync.Mutex
	records []recordData
}

type bufferManager struct {
	buffers   sync.Map
	batchSize int
}

func (bm *bufferManager) getOrCreate(tableName string) *buffer {
	if v, ok := bm.buffers.Load(tableName); ok {
		return v.(*buffer)
	}
	v, _ := bm.buffers.LoadOrStore(tableName, &buffer{})
	return v.(*buffer)
}

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

func (bm *bufferManager) append(tableName string, data recordData) int {
	buf := bm.getOrCreate(tableName)
	buf.mu.Lock()
	defer buf.mu.Unlock()

	buf.records = append(buf.records, data)
	return len(buf.records)
}
