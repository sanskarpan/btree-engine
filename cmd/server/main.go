// Command server starts the B+Tree storage engine and its HTTP gateway.
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"btree-engine/gateway"
	"btree-engine/internal/engine"
	"btree-engine/internal/logging"

	"gopkg.in/yaml.v3"
)

// fileConfig mirrors the config.yaml structure for YAML unmarshalling.
type fileConfig struct {
	Engine struct {
		DataFile       string `yaml:"data_file"`
		BufferPoolSize int    `yaml:"buffer_pool_size"`
	} `yaml:"engine"`
	WAL struct {
		LogFile            string  `yaml:"log_file"`
		BufferSize         int     `yaml:"buffer_size"`
		SyncOnCommit       *bool   `yaml:"sync_on_commit"`
		MaxSegmentSize     int64   `yaml:"max_segment_size"`
		ArchiveCommand     string  `yaml:"archive_command"`
		GroupCommitDelayMs float64 `yaml:"group_commit_delay_ms"`
	} `yaml:"wal"`
	Recovery struct {
		CheckpointInterval string `yaml:"checkpoint_interval"`
	} `yaml:"recovery"`
	MVCC struct {
		DefaultIsolation    string  `yaml:"default_isolation"`
		VacuumInterval      string  `yaml:"vacuum_interval"`
		VacuumDeadThreshold float64 `yaml:"vacuum_dead_threshold"`
	} `yaml:"mvcc"`
	Gateway struct {
		Port                int      `yaml:"port"`
		WSBuffer            int      `yaml:"ws_buffer"`
		APIKeys             []string `yaml:"api_keys"`
		IdleTxnTimeout      string   `yaml:"idle_txn_timeout"`
		RequestDrainTimeout string   `yaml:"request_drain_timeout"`
		RateLimitPerSec     float64  `yaml:"rate_limit_per_sec"`
		RateLimitBurst      float64  `yaml:"rate_limit_burst"`
	} `yaml:"gateway"`
	LogLevel  string `yaml:"log_level"`
	LogFormat string `yaml:"log_format"`
}

func main() {
	cfgPath := flag.String("config", "", "path to config file")
	flag.Parse()

	// Handle the healthcheck subcommand for container liveness probes (distroless).
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		runHealthcheck()
		return
	}

	resolvedCfgPath := resolveConfigPath(*cfgPath)
	fc := defaultFileConfig()
	if err := loadFileConfig(&fc, resolvedCfgPath); err != nil {
		slog.Error("load config failed", "path", resolvedCfgPath, "err", err)
		os.Exit(1)
	}

	// Initialise global structured logger before anything else.
	logging.Init(fc.LogLevel, fc.LogFormat)

	// Build engine config from merged file config.
	dataDir := "./data"
	engCfg := engine.DefaultConfig(dataDir)
	if fc.Engine.DataFile != "" {
		engCfg.DataFile = fc.Engine.DataFile
	}
	if fc.WAL.LogFile != "" {
		engCfg.WALFile = fc.WAL.LogFile
	}
	if fc.Engine.BufferPoolSize > 0 {
		engCfg.BufferPoolSize = fc.Engine.BufferPoolSize
	}
	if fc.WAL.BufferSize > 0 {
		engCfg.WALBufferSize = fc.WAL.BufferSize
	}
	if fc.WAL.SyncOnCommit != nil {
		engCfg.SyncWAL = *fc.WAL.SyncOnCommit
	}
	if fc.WAL.MaxSegmentSize > 0 {
		engCfg.WALMaxSegmentSize = fc.WAL.MaxSegmentSize
	}
	engCfg.WALArchiveCommand = fc.WAL.ArchiveCommand
	if fc.WAL.GroupCommitDelayMs > 0 {
		engCfg.GroupCommitDelay = time.Duration(fc.WAL.GroupCommitDelayMs * float64(time.Millisecond))
	}
	if d, err := time.ParseDuration(fc.Recovery.CheckpointInterval); err == nil && d > 0 {
		engCfg.CheckpointInterval = d
	}
	if d, err := time.ParseDuration(fc.MVCC.VacuumInterval); err == nil && d > 0 {
		engCfg.VacuumInterval = d
	}
	if fc.MVCC.VacuumDeadThreshold > 0 {
		engCfg.VacuumThreshold = fc.MVCC.VacuumDeadThreshold
	}
	engCfg.DefaultIsolation = strings.TrimSpace(fc.MVCC.DefaultIsolation)
	engCfg.APIKeys = fc.Gateway.APIKeys
	if fc.Gateway.WSBuffer > 0 {
		engCfg.WSBuffer = fc.Gateway.WSBuffer
	}
	engCfg.LogLevel = fc.LogLevel
	engCfg.LogFormat = fc.LogFormat
	if d, err := time.ParseDuration(fc.Gateway.IdleTxnTimeout); err == nil && d > 0 {
		engCfg.IdleTxnTimeout = d
	}
	if d, err := time.ParseDuration(fc.Gateway.RequestDrainTimeout); err == nil && d > 0 {
		engCfg.RequestDrainTimeout = d
	}
	if fc.Gateway.RateLimitPerSec > 0 {
		engCfg.RateLimitPerSec = fc.Gateway.RateLimitPerSec
	}
	if fc.Gateway.RateLimitBurst > 0 {
		engCfg.RateLimitBurst = fc.Gateway.RateLimitBurst
	}

	if !engCfg.SyncWAL {
		slog.Warn("wal.sync_on_commit is false — durability reduced")
	}

	slog.Info("opening engine",
		"data_file", engCfg.DataFile,
		"wal_file", engCfg.WALFile,
		"buffer_pool_size", engCfg.BufferPoolSize,
		"checkpoint_interval", engCfg.CheckpointInterval,
	)

	if len(engCfg.APIKeys) == 0 {
		slog.Warn("gateway.api_keys is empty — all API routes are unauthenticated")
	}

	eng, err := engine.OpenEngine(engCfg)
	if err != nil {
		slog.Error("open engine failed", "err", err)
		os.Exit(1)
	}

	port := fc.Gateway.Port
	if port == 0 {
		port = 8080
	}
	srv := gateway.NewServer(eng, engCfg, port)

	slog.Info("gateway listening", "port", port)
	// Run blocks until SIGTERM/SIGINT, drains HTTP, then calls eng.Close().
	if err := srv.Run(eng); err != nil {
		slog.Error("server stopped with error", "err", err)
	}
}

func resolveConfigPath(flagValue string) string {
	if flagValue != "" {
		return flagValue
	}
	if envPath := strings.TrimSpace(os.Getenv("CONFIG_PATH")); envPath != "" {
		return envPath
	}
	return "config.yaml"
}

func loadFileConfig(dst *fileConfig, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var parsed fileConfig
	if err := yaml.Unmarshal(data, &parsed); err != nil {
		return err
	}
	mergeFileConfig(dst, &parsed)
	return nil
}

// runHealthcheck calls GET /health/live and exits 0 on HTTP 200, 1 otherwise.
// Used as HEALTHCHECK CMD in Dockerfile (distroless has no curl).
func runHealthcheck() {
	port := 8080 // default; healthcheck always targets localhost
	url := fmt.Sprintf("http://localhost:%d/health/live", port)
	resp, err := http.Get(url) //nolint:gosec,noctx
	if err != nil || resp.StatusCode != http.StatusOK {
		os.Exit(1)
	}
	os.Exit(0)
}

func defaultFileConfig() fileConfig {
	var c fileConfig
	c.Engine.DataFile = "./data/btree.db"
	c.Engine.BufferPoolSize = 256
	c.WAL.LogFile = "" // use engine DefaultConfig value
	c.WAL.BufferSize = 65536
	c.WAL.SyncOnCommit = boolPtr(true)
	c.WAL.MaxSegmentSize = 256 * 1024 * 1024
	c.WAL.GroupCommitDelayMs = 0 // disabled by default
	c.Recovery.CheckpointInterval = "30s"
	c.MVCC.DefaultIsolation = "snapshot"
	c.MVCC.VacuumInterval = "60s"
	c.MVCC.VacuumDeadThreshold = 0.20
	c.Gateway.Port = 8080
	c.Gateway.WSBuffer = 512
	c.Gateway.IdleTxnTimeout = "5m"
	c.Gateway.RequestDrainTimeout = "30s"
	c.Gateway.RateLimitPerSec = 100
	c.Gateway.RateLimitBurst = 200
	c.LogLevel = "info"
	c.LogFormat = "text"
	return c
}

// mergeFileConfig copies non-zero values from src into dst.
func mergeFileConfig(dst *fileConfig, src *fileConfig) {
	if src.Engine.DataFile != "" {
		dst.Engine.DataFile = src.Engine.DataFile
	}
	if src.Engine.BufferPoolSize > 0 {
		dst.Engine.BufferPoolSize = src.Engine.BufferPoolSize
	}
	if src.WAL.LogFile != "" {
		dst.WAL.LogFile = src.WAL.LogFile
	}
	if src.WAL.BufferSize > 0 {
		dst.WAL.BufferSize = src.WAL.BufferSize
	}
	if src.WAL.SyncOnCommit != nil {
		dst.WAL.SyncOnCommit = boolPtr(*src.WAL.SyncOnCommit)
	}
	if src.WAL.MaxSegmentSize > 0 {
		dst.WAL.MaxSegmentSize = src.WAL.MaxSegmentSize
	}
	if src.WAL.ArchiveCommand != "" {
		dst.WAL.ArchiveCommand = src.WAL.ArchiveCommand
	}
	if src.WAL.GroupCommitDelayMs > 0 {
		dst.WAL.GroupCommitDelayMs = src.WAL.GroupCommitDelayMs
	}
	if src.Recovery.CheckpointInterval != "" {
		dst.Recovery.CheckpointInterval = src.Recovery.CheckpointInterval
	}
	if src.MVCC.VacuumInterval != "" {
		dst.MVCC.VacuumInterval = src.MVCC.VacuumInterval
	}
	if src.MVCC.DefaultIsolation != "" {
		dst.MVCC.DefaultIsolation = src.MVCC.DefaultIsolation
	}
	if src.MVCC.VacuumDeadThreshold > 0 {
		dst.MVCC.VacuumDeadThreshold = src.MVCC.VacuumDeadThreshold
	}
	if src.Gateway.Port > 0 {
		dst.Gateway.Port = src.Gateway.Port
	}
	if src.Gateway.WSBuffer > 0 {
		dst.Gateway.WSBuffer = src.Gateway.WSBuffer
	}
	if len(src.Gateway.APIKeys) > 0 {
		dst.Gateway.APIKeys = src.Gateway.APIKeys
	}
	if src.Gateway.IdleTxnTimeout != "" {
		dst.Gateway.IdleTxnTimeout = src.Gateway.IdleTxnTimeout
	}
	if src.Gateway.RequestDrainTimeout != "" {
		dst.Gateway.RequestDrainTimeout = src.Gateway.RequestDrainTimeout
	}
	if src.Gateway.RateLimitPerSec > 0 {
		dst.Gateway.RateLimitPerSec = src.Gateway.RateLimitPerSec
	}
	if src.Gateway.RateLimitBurst > 0 {
		dst.Gateway.RateLimitBurst = src.Gateway.RateLimitBurst
	}
	if src.LogLevel != "" {
		dst.LogLevel = src.LogLevel
	}
	if src.LogFormat != "" {
		dst.LogFormat = src.LogFormat
	}
}

func boolPtr(v bool) *bool {
	return &v
}
