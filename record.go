package batchdb

// Record 是写入记录的抽象接口。用户的业务结构体实现该接口即可投递到 batchdb。
type Record interface {
	// TableName 返回目标表名。
	TableName() string
	// ColumnValues 返回列名列表和对应的值列表（顺序一致）。
	ColumnValues() (columns []string, values []any)
}

// recordData 是 Record 的内部扁平表示，避免在缓冲区中持有用户对象引用。
type recordData struct {
	columns []string
	values  []any
}

// RawRecord 是 Record 接口的通用实现，适用于不想定义专用结构体的场景。
type RawRecord struct {
	Table   string
	Columns []string
	Values  []any
}

func (r *RawRecord) TableName() string                  { return r.Table }
func (r *RawRecord) ColumnValues() ([]string, []any) { return r.Columns, r.Values }

// NewRawRecord 创建一条原始记录。
func NewRawRecord(tableName string, columns []string, values []any) *RawRecord {
	return &RawRecord{
		Table:   tableName,
		Columns: columns,
		Values:  values,
	}
}
