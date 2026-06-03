package batchdb

import (
	"context"
	"strings"

	"gorm.io/gorm"
)

// Writer 是数据写入的抽象接口。用户可以实现该接口对接任意数据库。
// Write 将一批同列结构的行写入指定表；Ping 用于探测数据库可用性（WAL 重放前调用）。
type Writer interface {
	Write(ctx context.Context, tableName string, columns []string, rows [][]any) error
	Ping(ctx context.Context) error
}

// GormWriter 是基于 gorm.DB 的 Writer 实现，使用拼接 INSERT 语句批量写入。
type GormWriter struct {
	db *gorm.DB
}

// NewGormWriter 创建一个 GormWriter 实例。
func NewGormWriter(db *gorm.DB) *GormWriter {
	return &GormWriter{db: db}
}

// Write 拼接 INSERT INTO table (cols) VALUES (?,?,...),(?,?,...)... 并通过 gorm 执行。
// 所有行合并到一条 SQL 中批量写入，减少网络往返。
func (w *GormWriter) Write(ctx context.Context, tableName string, columns []string, rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}

	colCount := len(columns)
	var sb strings.Builder

	sb.WriteString("INSERT INTO ")
	sb.WriteString(tableName)
	sb.WriteString(" (")
	for i, col := range columns {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(col)
	}
	sb.WriteString(") VALUES ")

	placeholder := buildPlaceholder(colCount)
	args := make([]any, 0, len(rows)*colCount)

	for i, row := range rows {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(placeholder)
		args = append(args, row...)
	}

	return w.db.WithContext(ctx).Exec(sb.String(), args...).Error
}

// Ping 通过 SELECT 1 探测数据库连接是否可用。
func (w *GormWriter) Ping(ctx context.Context) error {
	var dummy int
	return w.db.WithContext(ctx).Raw("SELECT 1").Row().Scan(&dummy)
}

// buildPlaceholder 生成 (?,?,?...) 格式的占位符字符串，n 为列数。
func buildPlaceholder(n int) string {
	if n == 0 {
		return "()"
	}
	var sb strings.Builder
	sb.WriteByte('(')
	for i := 0; i < n; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteByte('?')
	}
	sb.WriteByte(')')
	return sb.String()
}

var _ Writer = (*GormWriter)(nil)

