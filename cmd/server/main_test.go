package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveConfigPath(t *testing.T) {
	t.Setenv("CONFIG_PATH", "/env/config.yaml")

	if got := resolveConfigPath("/flag/config.yaml"); got != "/flag/config.yaml" {
		t.Fatalf("flag path should win, got %q", got)
	}
	if got := resolveConfigPath(""); got != "/env/config.yaml" {
		t.Fatalf("env path should be used when flag is empty, got %q", got)
	}
	os.Unsetenv("CONFIG_PATH")
	if got := resolveConfigPath(""); got != "config.yaml" {
		t.Fatalf("default config path mismatch: %q", got)
	}
}

func TestLoadFileConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("gateway:\n  port: 9090\nlog_level: debug\nmvcc:\n  default_isolation: serializable\n"), 0644); err != nil {
		t.Fatal(err)
	}
	cfg := defaultFileConfig()
	if err := loadFileConfig(&cfg, path); err != nil {
		t.Fatal(err)
	}
	if cfg.Gateway.Port != 9090 {
		t.Fatalf("expected gateway port 9090, got %d", cfg.Gateway.Port)
	}
	if cfg.LogLevel != "debug" {
		t.Fatalf("expected log level debug, got %q", cfg.LogLevel)
	}
	if cfg.MVCC.DefaultIsolation != "serializable" {
		t.Fatalf("expected default isolation serializable, got %q", cfg.MVCC.DefaultIsolation)
	}
	if cfg.WAL.SyncOnCommit == nil || !*cfg.WAL.SyncOnCommit {
		t.Fatalf("expected sync_on_commit to remain true when omitted")
	}
}
