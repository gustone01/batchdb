package batchdb

import (
	"fmt"
	"time"
)

type Config struct {
	Hooks                   *Hooks
	BatchSize               int           // 默认 1000
	MaxBufferSize           int           // 默认 100000
	FlushInterval           time.Duration // 默认 200ms
	Workers                 int           // 默认 8；经验法则：min(表数量, MaxOpenConns/2)
	MaxRetries              int           // 默认 3
	RetryBaseDelay          time.Duration // 默认 100ms
	WALDir                  string        // 默认 "./wal"
	WALMaxFileSize          int64         // 默认 64MB
	WALMaxFileRows          int           // 默认 100000
	MaxWALSize              int64         // 默认 5GB
	WALCompress             bool          // 默认 true (gzip level 1)
	WALProbeInterval        time.Duration // 默认 5s
	ReplayBatchSize         int           // 默认 500
	ReplayInterval          time.Duration // 默认 50ms
	CircuitBreakerThreshold int           // 默认 3
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
