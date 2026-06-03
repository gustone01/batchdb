package batchdb

import (
	"fmt"
	"time"
)

// Config 定义 batchdb 的运行参数。零值字段会在初始化时自动填充为合理的默认值。
type Config struct {
	// Hooks 生命周期回调钩子（可选）。
	Hooks *Hooks

	// BatchSize 触发异步刷写的缓冲区阈值。
	// 当某张表的缓冲区记录数达到该值时，Worker 会被唤醒执行写入。
	BatchSize int // 默认 1000

	// MaxBufferSize 单表缓冲区的硬上限。
	// 超过此值时 AddRecord 会同步将溢出数据写入 WAL，防止 OOM。
	MaxBufferSize int // 默认 100000

	// FlushInterval 定时器周期：即使缓冲区未满也会触发刷写，保证数据时效性。
	FlushInterval time.Duration // 默认 200ms

	// Workers 并发刷写 Worker 的数量。
	// 推荐值：min(表数量, 数据库连接池大小/2)。
	Workers int // 默认 8

	// MaxRetries 单次批量写入的最大重试次数（不含首次尝试）。
	MaxRetries int // 默认 3

	// RetryBaseDelay 重试退避的基准延迟，实际延迟 = base * 2^(attempt-1)。
	RetryBaseDelay time.Duration // 默认 100ms

	// WALDir WAL 文件存储目录。每张表在该目录下有独立子目录。
	WALDir string // 默认 "./wal"

	// WALMaxFileSize 单个 WAL 文件的最大字节数，超过后自动轮转新文件。
	WALMaxFileSize int64 // 默认 64MB

	// WALMaxFileRows 单个 WAL 文件的最大行数，超过后自动轮转新文件。
	WALMaxFileRows int // 默认 100000

	// MaxWALSize WAL 目录的总容量上限。超过后 Write 返回 ErrWALFull。
	MaxWALSize int64 // 默认 5GB

	// WALCompress 是否启用 gzip 压缩（level=1，速度优先）。
	WALCompress bool // 默认 true

	// WALProbeInterval 后台重放循环探测数据库可用性的间隔。
	WALProbeInterval time.Duration // 默认 5s

	// ReplayBatchSize 重放时每批写入的记录数。
	ReplayBatchSize int // 默认 500

	// ReplayInterval 重放相邻批次之间的间隔，用于限速避免冲击数据库。
	ReplayInterval time.Duration // 默认 50ms

	// CircuitBreakerThreshold 触发熔断的连续失败次数。
	CircuitBreakerThreshold int // 默认 3
}

func (c *Config) applyDefaults() {
	if c.BatchSize == 0 {
		c.BatchSize = 1000
	}
	if c.MaxBufferSize == 0 {
		c.MaxBufferSize = 100000
	}
	if c.FlushInterval == 0 {
		c.FlushInterval = 200 * time.Millisecond
	}
	if c.Workers == 0 {
		c.Workers = 8
	}
	if c.MaxRetries == 0 {
		c.MaxRetries = 3
	}
	if c.RetryBaseDelay == 0 {
		c.RetryBaseDelay = 100 * time.Millisecond
	}
	if c.WALDir == "" {
		c.WALDir = "./wal"
	}
	if c.WALMaxFileSize == 0 {
		c.WALMaxFileSize = 64 << 20 // 64MB
	}
	if c.WALMaxFileRows == 0 {
		c.WALMaxFileRows = 100000
	}
	if c.MaxWALSize == 0 {
		c.MaxWALSize = 5 << 30 // 5GB
	}
	if c.WALProbeInterval == 0 {
		c.WALProbeInterval = 5 * time.Second
	}
	if c.ReplayBatchSize == 0 {
		c.ReplayBatchSize = 500
	}
	if c.ReplayInterval == 0 {
		c.ReplayInterval = 50 * time.Millisecond
	}
	if c.CircuitBreakerThreshold == 0 {
		c.CircuitBreakerThreshold = 3
	}
}

func (c *Config) validate() error {
	if c.BatchSize <= 0 {
		return fmt.Errorf("batchdb: BatchSize must be positive, got %d", c.BatchSize)
	}
	if c.MaxBufferSize <= 0 {
		return fmt.Errorf("batchdb: MaxBufferSize must be positive, got %d", c.MaxBufferSize)
	}
	if c.FlushInterval <= 0 {
		return fmt.Errorf("batchdb: FlushInterval must be positive, got %v", c.FlushInterval)
	}
	if c.Workers <= 0 {
		return fmt.Errorf("batchdb: Workers must be positive, got %d", c.Workers)
	}
	if c.MaxRetries < 0 {
		return fmt.Errorf("batchdb: MaxRetries must be non-negative, got %d", c.MaxRetries)
	}
	if c.RetryBaseDelay < 0 {
		return fmt.Errorf("batchdb: RetryBaseDelay must be non-negative, got %v", c.RetryBaseDelay)
	}
	if c.WALDir == "" {
		return fmt.Errorf("batchdb: WALDir must not be empty")
	}
	if c.WALMaxFileSize <= 0 {
		return fmt.Errorf("batchdb: WALMaxFileSize must be positive, got %d", c.WALMaxFileSize)
	}
	if c.WALMaxFileRows <= 0 {
		return fmt.Errorf("batchdb: WALMaxFileRows must be positive, got %d", c.WALMaxFileRows)
	}
	if c.MaxWALSize <= 0 {
		return fmt.Errorf("batchdb: MaxWALSize must be positive, got %d", c.MaxWALSize)
	}
	if c.WALProbeInterval <= 0 {
		return fmt.Errorf("batchdb: WALProbeInterval must be positive, got %v", c.WALProbeInterval)
	}
	if c.ReplayBatchSize <= 0 {
		return fmt.Errorf("batchdb: ReplayBatchSize must be positive, got %d", c.ReplayBatchSize)
	}
	if c.ReplayInterval < 0 {
		return fmt.Errorf("batchdb: ReplayInterval must be non-negative, got %v", c.ReplayInterval)
	}
	if c.CircuitBreakerThreshold <= 0 {
		return fmt.Errorf("batchdb: CircuitBreakerThreshold must be positive, got %d", c.CircuitBreakerThreshold)
	}
	return nil
}
