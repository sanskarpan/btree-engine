package unit

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"btree-engine/gateway"
)

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
}

func TestAuthMiddleware_MissingKey(t *testing.T) {
	keys := gateway.NewAPIKeySet([]string{"secret"})
	h := gateway.AuthMiddleware(keys, nil, okHandler())

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/api/v1/engine/stats", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
}

func TestAuthMiddleware_WrongScheme(t *testing.T) {
	keys := gateway.NewAPIKeySet([]string{"secret"})
	h := gateway.AuthMiddleware(keys, nil, okHandler())

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/api/v1/engine/stats", nil)
	req.Header.Set("Authorization", "Basic c2VjcmV0")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
}

func TestAuthMiddleware_InvalidKey(t *testing.T) {
	keys := gateway.NewAPIKeySet([]string{"secret"})
	h := gateway.AuthMiddleware(keys, nil, okHandler())

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/api/v1/engine/stats", nil)
	req.Header.Set("Authorization", "Bearer wrongkey")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
}

func TestAuthMiddleware_ValidKey(t *testing.T) {
	keys := gateway.NewAPIKeySet([]string{"secret"})
	h := gateway.AuthMiddleware(keys, nil, okHandler())

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/api/v1/engine/stats", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
}

func TestAuthMiddleware_ExemptPath(t *testing.T) {
	keys := gateway.NewAPIKeySet([]string{"secret"})
	exempt := []string{"/health/live"}
	h := gateway.AuthMiddleware(keys, exempt, okHandler())

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/health/live", nil)
	// No Authorization header — should pass through.
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 for exempt path, got %d", rr.Code)
	}
}

func TestAuthMiddleware_NonExemptPathRequiresAuth(t *testing.T) {
	keys := gateway.NewAPIKeySet([]string{"secret"})
	exempt := []string{"/health/live"}
	h := gateway.AuthMiddleware(keys, exempt, okHandler())

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/api/v1/engine/stats", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for non-exempt path without auth, got %d", rr.Code)
	}
}

func TestAPIKeySet_ConstantTime(t *testing.T) {
	// Verify valid() checks all keys even after a match, to prevent timing oracles.
	// We can only verify correctness here (not timing); the constant-time property
	// comes from the crypto/subtle implementation.
	keys := gateway.NewAPIKeySet([]string{"first", "second", "third"})

	if !keys.Valid("first") {
		t.Error("first key should be valid")
	}
	if !keys.Valid("second") {
		t.Error("second key should be valid")
	}
	if keys.Valid("fourth") {
		t.Error("unknown key should be invalid")
	}
	if keys.Valid("") {
		t.Error("empty key should be invalid")
	}
}

func TestNewAPIKeySet_TrimsWhitespace(t *testing.T) {
	keys := gateway.NewAPIKeySet([]string{"  spaced  ", "normal"})
	if !keys.Valid("spaced") {
		t.Error("key with surrounding whitespace should be trimmed and valid")
	}
	if keys.Valid("  spaced  ") {
		t.Error("key with whitespace should not match after trim")
	}
}
