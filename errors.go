package batchdb

import "errors"

var (
	ErrClosed  = errors.New("batchdb: closed")
	ErrWALFull = errors.New("batchdb: WAL directory size exceeded limit")
)
