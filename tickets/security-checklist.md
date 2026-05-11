# Security Validation Checklist

> Review this checklist before every production deployment and after every auth-related change.

---

## 1. Authentication & Authorization

- [ ] All API endpoints require `Authorization: Bearer <key>` header
- [ ] Missing header → 401 (not 200, not 403, not 500)
- [ ] Malformed header (not `Bearer ...`) → 401
- [ ] Invalid key → 401
- [ ] Key comparison uses constant-time algorithm (no timing oracle)
- [ ] `/health/live`, `/health/ready`, `/metrics` exempt from auth (intentional)
- [ ] Auth failure log entries contain: path, method, remote IP — never the supplied key
- [ ] API keys stored in config.yaml with file permissions `600` (owner-only read)
- [ ] In CI: API key from environment variable, not hardcoded
- [ ] After key rotation: old key invalid immediately (no grace period unless explicitly configured)

---

## 2. Input Validation

- [ ] Empty key (`""`) → HTTP 400 at gateway layer
- [ ] Empty key → rejected at engine layer (defense in depth)
- [ ] Key > max size (512 bytes default) → HTTP 400 "key exceeds maximum length"
- [ ] Value > max size (65535 bytes default) → HTTP 400 "value exceeds maximum length"
- [ ] Null bytes in key → handled correctly (no C-string truncation issues — Go strings are safe)
- [ ] Non-UTF8 bytes in key → handled correctly (binary keys allowed)
- [ ] Integer overflow in page IDs → validated before use
- [ ] Transaction ID `0` → rejected (reserved)
- [ ] Negative scan range (start > end) → returns empty result (not error, not panic)
- [ ] Very long scan range → bounded by cursor page limit
- [ ] JSON body too large (> configurable limit) → HTTP 413

---

## 3. Injection Prevention

- [ ] No SQL (N/A — no SQL layer)
- [ ] No shell execution in request handling path
- [ ] No `eval` or dynamic code execution
- [ ] WAL archive hook command (P0-017): command is a fixed template, archive path substituted safely
  - Template: `archive_command: "cp %s /backup/"` where `%s` is the segment filename only (no path traversal)
  - Filename validated: must match pattern `wal_[0-9]+\.log`

---

## 4. Data Confidentiality

- [ ] API responses never include config file contents (API keys must not appear in stats/debug endpoints)
- [ ] `/api/v1/engine/stats` does not expose file paths in production mode
- [ ] Error messages do not expose internal file paths, stack traces, or config
  - Development: full error details OK
  - Production: `{"error": "internal server error"}` for 500s; details in logs only
- [ ] WAL records do not log the value of keys/values at INFO level
  - Values may be sensitive data; logged only at DEBUG with explicit user consent

---

## 5. Denial of Service Prevention

- [ ] Rate limiter: max N requests/sec per IP (token bucket)
- [ ] Max concurrent open transactions: default 100; returns 429 when exceeded
- [ ] Request timeout: default 30s; returns 504 on exceed (prevents goroutine leak)
- [ ] WAL write: single concurrent writer (WAL lock prevents duplicate record injection)
- [ ] Buffer pool exhaustion: `FetchPage` returns error, not infinite block, when all frames pinned
- [ ] Cursor auto-close after idle timeout (prevents cursor leak holding page pins)
- [ ] Transaction auto-abort after idle timeout (prevents memory leak in statusTable)

---

## 6. Data Integrity

- [ ] Full-page CRC32 checksum verified on every `FetchPage()` (no bypass)
- [ ] WAL record CRC32 verified on every record during recovery (no bypass)
- [ ] Checksum mismatch → hard error returned, not silent corruption
- [ ] WAL file not writable → engine fails to open (not silently continues without WAL)
- [ ] Data file not writable → engine fails to open
- [ ] Partial page write (torn write) → detected by checksum on next read

---

## 7. Transaction Isolation

- [ ] Snapshot isolation: uncommitted writes from other transactions are invisible
- [ ] Snapshot isolation: committed writes from transactions starting after snapshot are invisible
- [ ] Write-write conflict: second writer gets 409 Conflict (not silent data loss)
- [ ] Aborted transaction: all writes invisible to all other transactions (MVCC visibility)
- [ ] No dirty reads possible under any isolation level
- [ ] Recovery: uncommitted (in-flight) transactions at crash time are rolled back

---

## 8. File System Security

- [ ] `data/` directory created with `0755` permissions
- [ ] Data file (`btree.db`) created with `0600` permissions (owner read/write only)
- [ ] WAL file created with `0600` permissions
- [ ] No world-readable database files
- [ ] Path traversal in `data_file` config: resolved to absolute path and validated (must be within configured data directory)
- [ ] symlink attacks: data path resolved before use (avoid following attacker-controlled symlinks in `data/`)

---

## 9. Logging Security

- [ ] No sensitive data in logs at INFO or above: no key values, no user data
- [ ] Log level DEBUG acceptable in development only
- [ ] Auth tokens/API keys never logged at any level
- [ ] Log rotation configured (prevent disk exhaustion from unbounded logging)
- [ ] Log output buffered but flushed before shutdown

---

## 10. Dependency Security

```bash
# Audit Go dependencies
go list -m all | govulncheck ./...

# Audit npm/bun dependencies
cd frontend && bun audit
```

- [ ] No critical CVEs in Go dependencies
- [ ] No critical CVEs in frontend dependencies
- [ ] `go.sum` checked into source control (dependency pinning)
- [ ] `bun.lockb` checked into source control

---

## 11. Container Security (when using Docker)

- [ ] Container runs as non-root user (`nonroot` in distroless)
- [ ] Container filesystem is read-only except `/app/data` volume
- [ ] No `SYS_ADMIN` or other excessive capabilities
- [ ] `--security-opt=no-new-privileges:true` in container run args
- [ ] Health check endpoint does not require auth (intentional exemption)
- [ ] Container image scanned for vulnerabilities before deployment:
  ```bash
  docker scout cves btree-engine:latest
  ```

---

## 12. Secrets Management

- [ ] API keys not hardcoded in source code (checked by `grep -r "api_keys" --include="*.go"`)
- [ ] API keys not in git history (checked by `git log -S "api_keys" -- config.yaml`)
- [ ] In production: API keys injected via environment variable or secrets manager
- [ ] Rotation procedure documented in `docs/operations.md`

---

## 13. Network Security

- [ ] Only necessary ports exposed (8080 for API, 3001 for frontend BFF)
- [ ] Firewall rules restricting access to trusted clients only
- [ ] TLS termination at reverse proxy (nginx/Caddy) for production
- [ ] CORS headers: `Access-Control-Allow-Origin: https://your-frontend-domain` (not `*` in production)
- [ ] WebSocket endpoint authenticated same as REST (Bearer token in first message or query param)

---

## Pre-Deployment Sign-off

| Check | Reviewer | Date | Status |
|-------|----------|------|--------|
| Auth coverage (all routes) | | | |
| Input validation (key/value bounds) | | | |
| No sensitive data in error responses | | | |
| Dependency audit clean | | | |
| Container security scan clean | | | |
| Rate limiting tested under load | | | |
| Transaction timeout configured | | | |
| WAL durable (fsync on commit) | | | |
| Checksum verification active | | | |
