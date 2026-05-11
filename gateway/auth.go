package gateway

import (
	"crypto/subtle"
	"log/slog"
	"net/http"
	"strings"
)

// APIKeySet is a set of valid API keys loaded at startup.
type APIKeySet map[string]struct{}

// NewAPIKeySet creates a key set from a slice of raw keys.
// Leading/trailing whitespace is trimmed; duplicates are silently de-duplicated.
func NewAPIKeySet(keys []string) APIKeySet {
	m := make(APIKeySet, len(keys))
	for _, k := range keys {
		k = strings.TrimSpace(k)
		if k != "" {
			m[k] = struct{}{}
		}
	}
	return m
}

// AuthMiddleware wraps handler h, requiring a valid Bearer token.
// Paths listed in exempt are passed through without authentication.
func AuthMiddleware(keys APIKeySet, exempt []string, h http.Handler) http.Handler {
	exemptSet := make(map[string]struct{}, len(exempt))
	for _, p := range exempt {
		exemptSet[p] = struct{}{}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := exemptSet[r.URL.Path]; ok {
			h.ServeHTTP(w, r)
			return
		}
		token := bearerToken(r)
		if !keys.Valid(token) {
			slog.Warn("auth failure",
				"path", r.URL.Path,
				"method", r.Method,
				"remote", r.RemoteAddr)
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		h.ServeHTTP(w, r)
	})
}

func bearerToken(r *http.Request) string {
	v := r.Header.Get("Authorization")
	if !strings.HasPrefix(v, "Bearer ") {
		return ""
	}
	return strings.TrimPrefix(v, "Bearer ")
}

// Valid returns true if key matches any key in the set using constant-time comparison.
// All keys are always compared to prevent timing oracles.
func (s APIKeySet) Valid(key string) bool {
	if key == "" {
		return false
	}
	ok := false
	for k := range s {
		if subtle.ConstantTimeCompare([]byte(k), []byte(key)) == 1 {
			ok = true
			// do NOT break — must compare all keys to prevent timing oracle
		}
	}
	return ok
}
