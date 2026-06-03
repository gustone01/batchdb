package batchdb

type Hooks struct {
	OnFlush    func(tableName string, count int, err error)
	OnWALWrite func(tableName string, count int)
	OnReplay   func(tableName string, count int, err error)
}
