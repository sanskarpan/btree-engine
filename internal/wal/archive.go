package wal

import (
	"log/slog"
	"os/exec"
)

// archiveExec runs the given shell command (already-expanded archive command).
// Any error is logged but not propagated — archive failures must not block WAL truncation.
func archiveExec(cmd string) {
	if cmd == "" {
		return
	}
	out, err := exec.Command("sh", "-c", cmd).CombinedOutput() //nolint:gosec
	if err != nil {
		slog.Error("wal archive command failed", "cmd", cmd, "err", err, "output", string(out))
	}
}
