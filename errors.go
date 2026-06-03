package batchdb

import "errors"

// 包级错误变量。
var (
	// ErrClosed 表示 DB 实例已关闭，不再接受新的写入操作。
	ErrClosed = errors.New("batchdb: closed")
	// ErrWALFull 表示 WAL 目录总大小已超过 Config.MaxWALSize 限制。
	ErrWALFull = errors.New("batchdb: WAL directory size exceeded limit")
)
