// Package engine exposes the top-level StorageEngine that ties all subsystems together.
package engine

import "time"

// Config holds the full engine configuration.
type Config struct {
	DataDir  string
	DataFile string
	// WALFile is the base path for WAL segment files.
	// Segments are named <WALFile>_000001.log, <WALFile>_000002.log, etc.
	WALFile            string
	BufferPoolSize     int
	WALBufferSize      int
	SyncWAL            bool
	WALMaxSegmentSize  int64  // rotate segment when it exceeds this size (0 = no rotation)
	WALArchiveCommand  string // shell command to run after rotation (%s = segment path); "" = delete
	CheckpointInterval time.Duration
	VacuumInterval     time.Duration
	VacuumThreshold    float64
	DefaultIsolation   string
	// API authentication
	APIKeys  []string
	WSBuffer int
	// Logging
	LogLevel  string // "debug" | "info" | "warn" | "error"
	LogFormat string // "text" | "json"
	// Shutdown
	IdleTxnTimeout      time.Duration
	RequestDrainTimeout time.Duration
	// Rate limiting
	RateLimitPerSec float64 // requests per second per IP (0 = disabled)
	RateLimitBurst  float64 // burst capacity per IP (0 = disabled)
	// Group commit
	GroupCommitDelay time.Duration // batch fsyncs within this window (0 = disabled, i.e. sync per commit)
}

// DefaultConfig returns production-ready defaults.
func DefaultConfig(dataDir string) Config {
	return Config{
		DataDir:             dataDir,
		DataFile:            dataDir + "/btree.db",
		WALFile:             dataDir + "/wal", // base path; segments get _NNNNNN.log suffix
		BufferPoolSize:      256,
		WALBufferSize:       65536,
		SyncWAL:             true,
		WALMaxSegmentSize:   256 * 1024 * 1024, // 256 MB
		WALArchiveCommand:   "",                // delete by default
		CheckpointInterval:  30 * time.Second,
		VacuumInterval:      60 * time.Second,
		VacuumThreshold:     0.20,
		DefaultIsolation:    "snapshot",
		WSBuffer:            512,
		LogLevel:            "info",
		LogFormat:           "text",
		IdleTxnTimeout:      5 * time.Minute,
		RequestDrainTimeout: 30 * time.Second,
		RateLimitPerSec:     100,
		RateLimitBurst:      200,
	}
}
