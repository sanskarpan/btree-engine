package wal

import (
	"context"
	"log/slog"
	"os/exec"
	"time"
)

// archiveExec runs the given shell command (already-expanded archive command).
// Any error is logged but not propagated — archive failures must not block WAL truncation.
func archiveExec(cmd string) {
	if cmd == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "sh", "-c", cmd).CombinedOutput() //nolint:gosec
	if err != nil {
		slog.Error("wal archive command failed", "cmd", cmd, "err", err, "output", string(out))
	}
}
