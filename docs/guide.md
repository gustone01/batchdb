# batchdb 使用指南

本文档详细介绍 batchdb 的架构设计、使用方法和最佳实践。

## 目录

- [架构概览](#架构概览)
- [安装与初始化](#安装与初始化)
- [基本用法](#基本用法)
- [实现 Record 接口](#实现-record-接口)
- [自定义 Writer](#自定义-writer)
- [WAL 机制详解](#wal-机制详解)
- [熔断器机制](#熔断器机制)
- [优雅关闭](#优雅关闭)
- [性能调优](#性能调优)
- [故障排查](#故障排查)

---

## 架构概览

```
 AddRecord()                       每表独立缓冲区
    │                              ┌──────────┐
    ├─ LoadOrStore ───────────────>│ Buffer A │──┐
    │   (懒初始化，原子操作)         └──────────┘  │
    │                              ┌──────────┐  │   ┌──────────────┐
    │                              │ Buffer B │──┼──>│  Flush 队列   │
    │                              └──────────┘  │   └──────┬───────┘
    │                              ┌──────────┐  │          │
    │                              │ Buffer C │──┘   ┌──────┼──────┐
    │                              └──────────┘      ▼      ▼      ▼
    │                                            Worker1 Worker2 Worker3
    │                                               │
    │  缓冲区满(>MaxBufferSize)                      │
    │      │                                    Writer.Write()
    │      ▼                                        │
    │   WAL 落盘  <────────────────────────  失败+重试耗尽
    │                                               │
    │                                            成功 → done
    │
    │  WAL 重放协程（后台）
    │      │
    │      ├─ 启动时：扫描 WAL 目录，有文件则重放
    │      └─ 运行期：定期 Ping DB，恢复后重放
    │             │
    │        重放成功 → 删除 WAL 文件
    │        重放仍失败 → 写入 dead-letter
```

### 数据流转

1. `AddRecord()` 将数据写入对应表的 Buffer
2. 满 BatchSize 或定时器触发时，向 flush 信号通道提交表名
3. Worker 收到信号后 drain Buffer，按列分组后写 DB
4. 写入失败则重试，重试耗尽则落盘 WAL
5. 后台协程探测 DB 恢复后自动重放 WAL

---

## 安装与初始化

### 安装

```bash
go get github.com/gustone01/batchdb
```

### 使用 Gorm 初始化

```go
import (
    "github.com/gustone01/batchdb"
    "gorm.io/driver/mysql"
    "gorm.io/gorm"
)

gormDB, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
if err != nil {
    panic(err)
}

db, err := batchdb.NewGormDB(gormDB, batchdb.Config{
    BatchSize:     1000,
    FlushInterval: 200 * time.Millisecond,
    Workers:       8,
    WALDir:        "./data/wal",
})
if err != nil {
    panic(err)
}
defer db.Close(context.Background())
```

### 使用自定义 Writer 初始化

```go
writer := NewMyCustomWriter(...)
db, err := batchdb.New(writer, batchdb.Config{
    BatchSize: 2000,
    Workers:   4,
})
```

---

## 基本用法

### 单条写入

```go
err := db.AddRecord(&MyRecord{
    Field1: "value1",
    Field2: 42,
})
if err != nil {
    // err == ErrClosed: 实例已关闭
    // err == ErrWALFull: WAL 磁盘超限
}
```

### 批量写入（支持混表）

```go
records := []batchdb.Record{
    &InstallLog{DeviceID: "a", Channel: "ch1"},
    &OrderLog{OrderID: "x", Amount: 100},    // 不同表
    &InstallLog{DeviceID: "b", Channel: "ch2"},
}
err := db.AddRecords(records)
```

`AddRecords` 内部自动按 `TableName()` 分组，分别放入对应 Buffer。

### 手动 Flush

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
err := db.Flush(ctx)
```

强制将所有 Buffer 中的数据立即写入 DB（绕过 Worker 池，直接写入）。

### 运行状态查看

```go
stats := db.Stats()
fmt.Printf("缓冲区: %v\n", stats.BufferRecords)
fmt.Printf("已写入: %d 条\n", stats.TotalFlushed)
fmt.Printf("WAL 写入: %d 条\n", stats.TotalWALWrites)
fmt.Printf("WAL 磁盘: %d bytes\n", stats.WALDiskUsage)
fmt.Printf("WAL 文件: %d 个\n", stats.WALFileCount)
fmt.Printf("忙碌 Worker: %d\n", stats.WorkersBusy)
```

---

## 实现 Record 接口

### 方式一：结构体实现

```go
type UserEvent struct {
    UserID    int64
    EventType string
    EventTime string
    Payload   string
}

func (r *UserEvent) TableName() string { return "user_events" }

func (r *UserEvent) ColumnValues() ([]string, []any) {
    return []string{"user_id", "event_type", "event_time", "payload"},
           []any{r.UserID, r.EventType, r.EventTime, r.Payload}
}
```

### 方式二：使用内置 RawRecord

适用于动态列场景（如 JSON 解析后直接写入）：

```go
record := batchdb.NewRawRecord(
    "raw_events",
    []string{"id", "data", "ts"},
    []any{12345, `{"key":"val"}`, "2026-06-03 16:00:00"},
)
db.AddRecord(record)
```

### 注意事项

- `ColumnValues()` 返回的列名顺序必须与 values 一一对应
- 同一张表的不同 Record 实例可以有不同列（库内部会按列分组拼 INSERT）
- 列名应使用数据库实际列名（不做转换）

---

## 自定义 Writer

实现 `Writer` 接口即可替换写入后端：

```go
type Writer interface {
    Write(ctx context.Context, tableName string, columns []string, rows [][]any) error
    Ping(ctx context.Context) error
}
```

### 示例：Doris Stream Load Writer

```go
type StreamLoadWriter struct {
    httpClient *http.Client
    feHost     string
    user       string
    password   string
}

func (w *StreamLoadWriter) Write(ctx context.Context, tableName string, columns []string, rows [][]any) error {
    // 将 rows 转为 CSV/JSON 格式
    // 发送 HTTP PUT 到 Doris FE Stream Load 接口
    return nil
}

func (w *StreamLoadWriter) Ping(ctx context.Context) error {
    resp, err := w.httpClient.Get(w.feHost + "/api/health")
    if err != nil {
        return err
    }
    resp.Body.Close()
    return nil
}
```

---

## WAL 机制详解

### 目录结构

```
{WALDir}/
├── install_log/
│   ├── 20260603_160405.000.wal    # gzip 压缩的 JSONL
│   └── 20260603_160410.123.wal
├── order_log/
│   └── 20260603_161000.456.wal
└── dead/                           # dead-letter 目录
    └── install_log/
        └── dead_20260603_162000.jsonl
```

### 触发写 WAL 的场景

| 场景 | 触发条件 |
|------|----------|
| Buffer 溢出 | 单表 Buffer 达到 MaxBufferSize |
| 写入失败 | Worker 重试耗尽后 |
| 熔断器 Open | 熔断期间所有 batch 直接写 WAL |
| 关闭超时 | Close ctx 超时后残余数据写 WAL |

### WAL 文件格式

采用 **gzip 拼接**（concatenated gzip streams）方式追加：

```jsonl
{"columns":["device_id","channel"],"values":["abc","toutiao"]}
{"columns":["device_id","channel"],"values":["def","gdt"]}
```

每次 flush 到 WAL 时创建独立 gzip 流并 fsync，保证进程崩溃后已写数据可恢复。

### 重放流程

1. 启动时自动扫描 WAL 目录，有文件则立即重放
2. 运行期定期 `Ping` DB，恢复后触发重放
3. 重放按文件时间顺序、每批 ReplayBatchSize 条、批间 sleep ReplayInterval
4. 成功 → 删除 WAL 文件
5. 数据格式问题 → 写入 dead-letter
6. 文件损坏（gzip 截断）→ 恢复已读部分，跳过损坏段

### 磁盘保护

- WAL 目录总大小上限 MaxWALSize（默认 5GB）
- 超限后 `AddRecord` 返回 `ErrWALFull`
- Worker 写 WAL 磁盘满时数据丢失，打 Error 日志 + 调用 OnFlush 钩子

---

## 熔断器机制

三状态有限状态机，所有 Worker 共享：

```
  ┌─────────┐  连续失败 >= threshold  ┌─────────┐
  │ Closed  │ ───────────────────────> │  Open   │
  │(正常写DB)│ <─────────── 成功 ────── │(直接WAL)│
  └─────────┘                          └────┬────┘
       ▲                                    │
       │                    Ping 成功        │
       │                 ┌──────────┐       │
       └──── 写入成功 ───│ HalfOpen │<──────┘
                         │(试写1批) │
                         └──────────┘
```

- **Closed**: 正常写 DB，任何一次成功 reset failures
- **Open**: 所有 batch 直接写 WAL，跳过 DB 写入和重试
- **HalfOpen**: 探测 Ping 成功后放 1 个 batch 试写，成功切 Closed，失败切回 Open

---

## 优雅关闭

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
err := db.Close(ctx)
```

### Close 流程

1. CAS 保证只执行一次（重复调用返回 `ErrClosed`）
2. 设置 closed 标志，后续 `AddRecord` 立即返回 `ErrClosed`
3. 等待 in-flight 的 `AddRecord` 完成
4. 停止定时器
5. 向所有非空 Buffer 提交 flush 信号
6. 关闭信号通道，Worker 处理完剩余信号后退出
7. 等待所有 Worker 完成或 ctx 超时
8. 超时时 cancel Worker context → Writer.Write 返回 → Worker 将数据写 WAL
9. 防御性扫描：Buffer 残留数据写 WAL
10. 停止 WAL 重放协程，关闭文件句柄

---

## 性能调优

### Workers 数量

```
推荐值 = min(写入表数量, DB MaxOpenConns / 2)
```

- 表少（1-2 张）：Workers=2~4 即可
- 表多（10+ 张）：Workers=8~16，每个 Worker 可服务不同表

### BatchSize

| 数据库 | 推荐 BatchSize |
|--------|---------------|
| Doris  | 1000 ~ 5000   |
| ClickHouse | 5000 ~ 50000 |
| MySQL  | 500 ~ 2000    |

过大的 BatchSize 会增加单次 INSERT 的内存占用和网络包大小。

### FlushInterval

- 实时性要求高：50ms ~ 200ms
- 吞吐优先：500ms ~ 2s
- 建议不低于 50ms，避免空转 CPU 开销

### WAL 压缩

WAL 默认开启 gzip level 1 压缩：
- 压缩比约 5:1（JSON 数据）
- CPU 开销极小（level 1）
- 可通过 `WALCompress: false` 关闭（适用于 SSD + 数据量小场景）

---

## 故障排查

### 常见问题

#### Q: AddRecord 返回 ErrWALFull

WAL 目录总大小超过 MaxWALSize。可能原因：
- DB 长时间不可用，WAL 持续堆积
- 重放速度跟不上写入速度

解决：
1. 检查 DB 连接是否正常
2. 增大 MaxWALSize
3. 检查 dead-letter 目录是否有大量文件（表结构变更导致重放失败）

#### Q: 日志出现 "data lost"

Worker 写 WAL 时磁盘也满了，数据无法持久化。

解决：
1. 清理磁盘空间
2. 增大 MaxWALSize 之前确保磁盘有足够容量
3. 通过 OnFlush 钩子接入告警

#### Q: 重放后出现少量数据重复

这是 at-least-once 语义的正常现象（DB 执行成功但响应超时）。

- Unique Key 表：数据库自动去重，无需处理
- Duplicate Key 表：接受极低概率重复，或在应用层去重

#### Q: dead-letter 目录有文件

WAL 中的记录无法重放到 DB（通常是表结构变更、列被删除等）。

处理方式：
1. 检查 dead-letter 文件内容（JSONL 格式）
2. 修复数据后手动导入
3. 确认无价值后删除

### 日志关键字

| 日志 | 级别 | 含义 |
|------|------|------|
| `write failed` | WARN | 单次写入失败，将重试 |
| `all retries exhausted, writing to WAL` | ERROR | 重试耗尽，降级到 WAL |
| `WAL write also failed, data lost` | ERROR | WAL 也写失败，数据丢失 |
| `WAL replay failed` | ERROR | 重放失败，下轮重试 |
| `WAL gzip corrupt` | WARN | WAL 文件损坏，移至 dead-letter |
