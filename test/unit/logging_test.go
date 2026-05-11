package unit

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"btree-engine/internal/logging"
)

func TestLogging_LevelFilter(t *testing.T) {
	var buf bytes.Buffer
	opts := &slog.HandlerOptions{Level: slog.LevelWarn}
	handler := slog.NewTextHandler(&buf, opts)
	logger := slog.New(handler)
	slog.SetDefault(logger)
	t.Cleanup(func() { logging.Init("info", "text") })

	slog.Debug("this should not appear")
	slog.Info("this should also not appear")
	slog.Warn("this should appear")

	out := buf.String()
	if strings.Contains(out, "this should not appear") {
		t.Error("DEBUG message leaked through WARN filter")
	}
	if strings.Contains(out, "this should also not appear") {
		t.Error("INFO message leaked through WARN filter")
	}
	if !strings.Contains(out, "this should appear") {
		t.Error("WARN message not written")
	}
}

func TestLogging_JSONOutput(t *testing.T) {
	var buf bytes.Buffer
	opts := &slog.HandlerOptions{Level: slog.LevelInfo}
	handler := slog.NewJSONHandler(&buf, opts)
	logger := slog.New(handler)
	slog.SetDefault(logger)
	t.Cleanup(func() { logging.Init("info", "text") })

	slog.Info("test event", "key", "value", "count", 42)

	var record map[string]any
	if err := json.Unmarshal(buf.Bytes(), &record); err != nil {
		t.Fatalf("failed to parse JSON log output: %v\nraw: %s", err, buf.String())
	}
	if record["msg"] != "test event" {
		t.Errorf("expected msg=test event, got %v", record["msg"])
	}
	if record["key"] != "value" {
		t.Errorf("expected key=value, got %v", record["key"])
	}
}

func TestLogging_Init_TextFormat(t *testing.T) {
	logging.Init("info", "text")
	if logging.L == nil {
		t.Error("Init should set a non-nil logger")
	}
}

func TestLogging_Init_JSONFormat(t *testing.T) {
	logging.Init("debug", "json")
	if logging.L == nil {
		t.Error("Init with json format should set a non-nil logger")
	}
	t.Cleanup(func() { logging.Init("info", "text") })
}
