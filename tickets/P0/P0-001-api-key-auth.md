# P0-001 — API Key Authentication Middleware

**Priority:** P0 (Critical)
**Phase:** 0-A
**Effort:** M (1 week)
**Depends on:** none
**Blocks:** P0-002, P0-003

---

## Problem Statement

The B+Tree engine REST API has zero authentication. Any process reachable at port 8080 can:
- Read all stored data
- Write or delete any key
- Crash the engine (`POST /api/v1/engine/crash`)
- Run ARIES recovery (`POST /api/v1/engine/recover`)
- Open unlimited transactions, exhausting memory

This is a complete trust boundary failure for any deployment beyond localhost development.

---

## Context

- `gateway/server.go` registers all routes via `http.HandleFunc`
- No middleware chain exists
- No concept of caller identity in any handler
- `gateway/rest.go` handlers receive `(w http.ResponseWriter, r *http.Request)`

---

## Goals

1. All API routes require a valid API key in the `Authorization: Bearer <key>` header
2. Missing or invalid key → HTTP 401 `{"error": "unauthorized"}`
3. Health endpoints (`/health/live`, `/health/ready`) exempt from auth
4. At least one API key must be configured at startup or server refuses to start
5. Auth failure logs include: IP address, path, method (never the supplied key)

---

## Technical Design

### New File: `gateway/auth.go`

```go
package gateway

import (
    "crypto/subtle"
    "net/http"
    "strings"
)

// APIKeySet is a set of valid API keys loaded at startup.
type APIKeySet map[string]struct{}

// NewAPIKeySet creates a key set from a slice of raw keys.
// Keys are stored as-is; constant-time comparison used at check time.
func NewAPIKeySet(keys []string) APIKeySet {
    m := make(APIKeySet, len(keys))
    for _, k := range keys {
        m[k] = struct{}{}
    }
    return m
}

// AuthMiddleware wraps handler h, requiring a valid Bearer token.
// Exempt paths (e.g., /health/live) are passed through without auth.
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
        if !keys.valid(token) {
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

func (s APIKeySet) valid(key string) bool {
    if key == "" {
        return false
    }
    // Constant-time comparison: iterate all keys to prevent timing oracle.
    ok := false
    for k := range s {
        if subtle.ConstantTimeCompare([]byte(k), []byte(key)) == 1 {
            ok = true
            // do NOT break — must compare all keys
        }
    }
    return ok
}
```

### Config Changes: `internal/engine/config.go`

```go
type GatewayConfig struct {
    Port     int      `yaml:"port"`
    WSBuffer int      `yaml:"ws_buffer"`
    APIKeys  []string `yaml:"api_keys"` // NEW — fail startup if empty
}
```

### Config File: `config.yaml`

```yaml
gateway:
  port: 8080
  ws_buffer: 512
  api_keys:
    - "dev-key-change-me"
    # Add more keys for rotation
```

### Startup Validation: `cmd/server/main.go`

```go
if len(cfg.Gateway.APIKeys) == 0 {
    log.Fatal("fatal: gateway.api_keys must contain at least one key")
}
keys := gateway.NewAPIKeySet(cfg.Gateway.APIKeys)
```

### Server Registration: `gateway/server.go`

```go
exempt := []string{"/health/live", "/health/ready"}
mux := http.NewServeMux()
// ... register all routes on mux ...
handler := gateway.AuthMiddleware(keys, exempt, mux)
http.ListenAndServe(addr, handler)
```

---

## Database Changes

None. API keys are stored in `config.yaml` only (flat file, no DB changes required).

---

## API Changes

**All existing endpoints:** Now require `Authorization: Bearer <key>` header.

**New responses:**
```json
HTTP 401
{"error": "unauthorized"}
```

**Exempt endpoints (no auth required):**
- `GET /health/live`
- `GET /health/ready`

---

## Security Considerations

1. **Constant-time comparison** prevents timing oracle attacks (attacker cannot determine key length or prefix by measuring response time)
2. **Never log the supplied key** — only log path, method, remote IP on auth failure
3. **Keys in config.yaml** — file must be readable only by the service user (`chmod 600`)
4. Keys are plain strings; for production, use a secret manager (Vault, AWS Secrets Manager) to inject via environment variable:
   ```yaml
   api_keys:
     - "${API_KEY_1}"    # resolved from env at startup
   ```
5. No rate limiting on auth failures — add P0-019 (rate limiter) after this ticket

---

## Edge Cases & Failure Scenarios

| Scenario | Expected Behavior |
|----------|------------------|
| Empty `Authorization` header | 401 Unauthorized |
| `Authorization: Basic ...` | 401 Unauthorized (wrong scheme) |
| Valid key, wrong route | Normal route 404 |
| Valid key, engine crashed | Normal engine error response |
| Config has empty `api_keys` list | Server refuses to start |
| Config has duplicate keys | De-duplicated silently |
| Key contains whitespace | Leading/trailing whitespace trimmed at load time |

---

## Testing Plan

### Unit Tests (`test/unit/auth_test.go`)

```go
func TestAuthMiddleware_MissingKey(t *testing.T)   // 401
func TestAuthMiddleware_WrongScheme(t *testing.T)  // 401 for "Basic ..."
func TestAuthMiddleware_InvalidKey(t *testing.T)   // 401
func TestAuthMiddleware_ValidKey(t *testing.T)     // 200 pass-through
func TestAuthMiddleware_ExemptPath(t *testing.T)   // /health/live passes without key
func TestAPIKeySet_ConstantTime(t *testing.T)      // timing: all-keys scan regardless of match position
```

### Integration Tests (`test/integration/auth_test.go`)

```go
func TestAPI_AllRoutes_Require_Auth(t *testing.T)  // iterate all registered routes, verify 401 without key
func TestAPI_ValidKey_AllRoutes_Pass(t *testing.T) // verify key is accepted on every route
```

---

## Rollout Strategy

1. Deploy with `api_keys: ["dev-key-change-me"]` in non-production
2. Update all clients to include `Authorization: Bearer dev-key-change-me`
3. Verify all integration tests pass
4. Rotate to a randomly generated key before production deployment
5. Set up secret manager injection for production

---

## Monitoring Requirements

- `btree_auth_failures_total` counter (labeled by `path`, `remote_addr` — no key value)
- Log WARN for > 10 auth failures/minute from the same IP

---

## Definition of Done

- [ ] `gateway/auth.go` implemented with constant-time comparison
- [ ] All existing routes require auth
- [ ] `/health/live` and `/health/ready` exempt
- [ ] Startup fails if `api_keys` is empty
- [ ] Unit tests cover all auth failure modes
- [ ] Integration test verifies all routes are protected
- [ ] Config documentation updated
- [ ] `Authorization: Bearer <key>` documented in REST API reference
