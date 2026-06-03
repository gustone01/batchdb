# batchdb

高吞吐批量写入库，适用于 Go 应用向 OLAP 数据库（Doris、ClickHouse 等）高频写入场景。

## 特性

- 每表独立缓冲区，按表名自动分组
- Worker 池并发写入，信号驱动
- WAL 持久化，DB 恢复后自动重放
- 全局熔断器，探测恢复后自动切回
- 优雅关闭，残余数据兜底写 WAL
- 监控钩子（OnFlush / OnWALWrite / OnReplay）

## 安装

```bash
go get github.com/gustone01/batchdb
```

## 快速开始

```go
db, err := batchdb.NewGormDB(gormDB, batchdb.Config{
    BatchSize: 1000,
    Workers:   8,
    WALDir:    "./wal",
})

db.AddRecord(&MyRecord{...})

db.Close(ctx)
```

## 文档

详细用法参见 [docs/guide.md](docs/guide.md)。

## License

MIT
