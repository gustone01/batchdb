package batchdb

import (
	"context"
	"strings"

	"gorm.io/gorm"
)

type Writer interface {
	Write(ctx context.Context, tableName string, columns []string, rows [][]any) error
	Ping(ctx context.Context) error
}

type GormWriter struct {
	db *gorm.DB
}

func NewGormWriter(db *gorm.DB) *GormWriter {
	return &GormWriter{db: db}
}

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

func (w *GormWriter) Ping(ctx context.Context) error {
	var dummy int
	return w.db.WithContext(ctx).Raw("SELECT 1").Row().Scan(&dummy)
}

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

