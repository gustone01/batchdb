package batchdb

// Hooks 提供生命周期回调，可用于监控、告警或日志采集。所有字段均可选。
type Hooks struct {
	// OnFlush 在每次刷写完成后调用。err 非 nil 表示写入失败（已降级到 WAL）。
	OnFlush func(tableName string, count int, err error)
	// OnWALWrite 在数据成功写入 WAL 文件后调用。
	OnWALWrite func(tableName string, count int)
	// OnReplay 在 WAL 文件重放完成后调用。err 非 nil 表示重放中断。
	OnReplay func(tableName string, count int, err error)
}
