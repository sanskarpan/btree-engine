// Package logging provides a globally-configured slog logger for the engine.
package logging

import (
	"log/slog"
	"os"
)

// L is the global structured logger. Initialised to the default slog handler;
// call Init() once at startup to override level and format.
var L = slog.Default()

// Init configures the global logger. Must be called before starting any
// engine subsystem. Subsequent calls replace the previous configuration.
//
//   - level:  "debug" | "info" | "warn" | "error"  (default "info")
//   - format: "text"  | "json"                      (default "text")
func Init(level, format string) {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: lvl, AddSource: false}
	var handler slog.Handler
	if format == "json" {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}
	L = slog.New(handler)
	slog.SetDefault(L)
}
