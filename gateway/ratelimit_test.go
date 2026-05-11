package gateway

import (
	"net/http/httptest"
	"testing"
)

func TestExtractIP_UsesForwardedHeaders(t *testing.T) {
	req := httptest.NewRequest("GET", "http://example.com/api/v1/engine/stats", nil)
	req.RemoteAddr = "10.0.0.1:1234"
	req.Header.Set("X-Forwarded-For", "198.51.100.10, 10.0.0.1")
	if got := extractIP(req); got != "198.51.100.10" {
		t.Fatalf("expected forwarded client IP, got %q", got)
	}
}

func TestExtractIP_FallsBackToRealIP(t *testing.T) {
	req := httptest.NewRequest("GET", "http://example.com/api/v1/engine/stats", nil)
	req.RemoteAddr = "10.0.0.1:1234"
	req.Header.Set("X-Real-IP", "203.0.113.7")
	if got := extractIP(req); got != "203.0.113.7" {
		t.Fatalf("expected X-Real-IP fallback, got %q", got)
	}
}
