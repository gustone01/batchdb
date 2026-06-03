package batchdb

type Record interface {
	TableName() string
	ColumnValues() (columns []string, values []any)
}

type recordData struct {
	columns []string
	values  []any
}

type RawRecord struct {
	Table   string
	Columns []string
	Values  []any
}

func (r *RawRecord) TableName() string                  { return r.Table }
func (r *RawRecord) ColumnValues() ([]string, []any) { return r.Columns, r.Values }

func NewRawRecord(tableName string, columns []string, values []any) *RawRecord {
	return &RawRecord{
		Table:   tableName,
		Columns: columns,
		Values:  values,
	}
}
