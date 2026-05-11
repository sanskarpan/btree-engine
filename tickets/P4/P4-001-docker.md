# P4-001 to P4-005 — Docker & Container Support

**Priority:** P4 (DevOps)
**Phase:** 4-A
**Effort:** S (2 days)
**Depends on:** P0-012 (graceful shutdown), P1-012 (health checks)

---

## Problem Statement

There is no Docker support. Deploying the engine requires:
1. Installing Go 1.26+ locally
2. Installing Bun for the frontend BFF
3. Manually starting both services
4. No way to reproduce the exact running environment

This blocks: CI/CD, cloud deployment, contributor onboarding, demo sharing.

---

## Goals

1. `docker compose up` starts entire stack (engine + frontend) in < 30 seconds
2. Final image size < 30 MB
3. Container health check via `/health/ready`
4. Development compose with live reload
5. Optional Prometheus + Grafana for monitoring

---

## Dockerfile (Multi-Stage)

```dockerfile
# syntax=docker/dockerfile:1.7

# ── Stage 1: Go builder ──────────────────────────────────────────────
FROM golang:1.26-alpine AS go-builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -trimpath \
    -o /out/btree-server ./cmd/server

# ── Stage 2: Frontend BFF builder ───────────────────────────────────
FROM oven/bun:1.2-alpine AS bun-builder

WORKDIR /src/frontend
COPY frontend/package.json frontend/bun.lockb ./
RUN bun install --frozen-lockfile

COPY frontend/ .
RUN bun run build 2>/dev/null || true  # static build if applicable

# ── Stage 3: Runtime ─────────────────────────────────────────────────
FROM gcr.io/distroless/static-debian12:nonroot AS runtime

# Copy Go binary
COPY --from=go-builder /out/btree-server /app/btree-server

# Copy default config (overridable via bind mount)
COPY config.yaml /app/config.yaml

# Data directory (overridable via volume)
VOLUME ["/app/data"]

WORKDIR /app

# Default port
EXPOSE 8080

# Health check (requires /health/live to exist — P1-012)
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
    CMD ["/app/btree-server", "healthcheck"] || exit 1

# Note: distroless has no shell, so HEALTHCHECK uses the binary itself with a
# healthcheck subcommand that calls GET /health/live and exits 0/1.

ENTRYPOINT ["/app/btree-server"]
CMD ["--config=/app/config.yaml"]
```

**Image size analysis:**
- `distroless/static-debian12:nonroot` base: ~2 MB
- Go binary (stripped): ~8-10 MB
- Config: < 1 KB
- Total: **~12 MB** ✓

---

## docker-compose.yml (Production)

```yaml
version: "3.9"

services:
  engine:
    build:
      context: .
      dockerfile: Dockerfile
      target: runtime
    image: btree-engine:latest
    ports:
      - "8080:8080"
    volumes:
      - engine-data:/app/data
      - ./config.yaml:/app/config.yaml:ro
    environment:
      - LOG_LEVEL=info
      - LOG_FORMAT=json
    healthcheck:
      test: ["CMD", "/app/btree-server", "healthcheck"]
      interval: 10s
      timeout: 5s
      retries: 3
      start_period: 30s
    restart: unless-stopped
    deploy:
      resources:
        limits:
          memory: 512M
          cpus: "1.0"

  frontend:
    build:
      context: ./frontend
      dockerfile: Dockerfile.frontend
    ports:
      - "3001:3001"
    environment:
      - ENGINE_URL=http://engine:8080
    depends_on:
      engine:
        condition: service_healthy
    restart: unless-stopped

volumes:
  engine-data:
    driver: local
```

---

## docker-compose.dev.yml (Development)

```yaml
version: "3.9"

services:
  engine:
    build:
      context: .
      target: go-builder   # use builder stage for live reload
    command: ["go", "run", "./cmd/server", "--config=/app/config.yaml"]
    volumes:
      - .:/src:ro           # live code mount (read-only for safety)
      - engine-data:/app/data
      - ./config.yaml:/app/config.yaml
    ports:
      - "8080:8080"
    environment:
      - LOG_LEVEL=debug
      - LOG_FORMAT=text
    # No health check in dev — faster restart

  frontend:
    image: oven/bun:1.2
    working_dir: /app
    command: ["bun", "run", "dev"]
    volumes:
      - ./frontend:/app     # live frontend code
    ports:
      - "3001:3001"
    environment:
      - ENGINE_URL=http://engine:8080

volumes:
  engine-data:
```

---

## docker-compose.monitoring.yml (Optional)

```yaml
version: "3.9"

# Extend the main compose with monitoring
# Usage: docker compose -f docker-compose.yml -f docker-compose.monitoring.yml up

services:
  prometheus:
    image: prom/prometheus:v2.53.0
    ports:
      - "9090:9090"
    volumes:
      - ./docs/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - prometheus-data:/prometheus
    command:
      - "--config.file=/etc/prometheus/prometheus.yml"
      - "--storage.tsdb.retention.time=30d"

  grafana:
    image: grafana/grafana:11.0.0
    ports:
      - "3000:3000"
    volumes:
      - ./docs/grafana_dashboard.json:/etc/grafana/provisioning/dashboards/btree.json:ro
      - grafana-data:/var/lib/grafana
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
      - GF_AUTH_ANONYMOUS_ENABLED=true

volumes:
  prometheus-data:
  grafana-data:
```

---

## Prometheus Scrape Config (`docs/prometheus.yml`)

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: btree-engine
    static_configs:
      - targets: ["engine:8080"]
    metrics_path: /metrics
```

---

## Frontend Dockerfile (`frontend/Dockerfile.frontend`)

```dockerfile
FROM oven/bun:1.2-alpine AS builder
WORKDIR /app
COPY package.json bun.lockb ./
RUN bun install --frozen-lockfile
COPY . .
RUN bun run build

FROM oven/bun:1.2-alpine AS runtime
WORKDIR /app
COPY --from=builder /app .
ENV ENGINE_URL=http://localhost:8080
EXPOSE 3001
CMD ["bun", "run", "server/bff.ts"]
```

---

## Healthcheck Binary Subcommand

The `distroless` base has no `curl` or `wget`. Add a `healthcheck` subcommand to `cmd/server/main.go`:

```go
// cmd/server/main.go
if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
    resp, err := http.Get("http://localhost:8080/health/live")
    if err != nil || resp.StatusCode != 200 {
        os.Exit(1)
    }
    os.Exit(0)
}
```

---

## .dockerignore

```
data/
*.log
*.db
.git/
node_modules/
frontend/.bun/
bin/
```

---

## Testing Plan

```bash
# Build and verify image size
docker build -t btree-engine:test .
docker images btree-engine:test  # verify < 30 MB

# Functional test
docker compose up -d
sleep 10
curl -H "Authorization: Bearer dev-key-change-me" \
     http://localhost:8080/api/v1/engine/stats
# expect 200 JSON response

# Health check
curl http://localhost:8080/health/live  # expect 200

# Shutdown test
docker compose stop  # verify graceful flush (no data loss)
docker compose up -d
curl http://localhost:8080/api/v1/engine/stats  # data persists
```

---

## Definition of Done

- [ ] `Dockerfile` with multi-stage build (< 30 MB final image)
- [ ] `docker-compose.yml` for production
- [ ] `docker-compose.dev.yml` for development with volume mounts
- [ ] `docker-compose.monitoring.yml` for Prometheus + Grafana (optional)
- [ ] Frontend `Dockerfile.frontend`
- [ ] `healthcheck` subcommand in server binary
- [ ] `.dockerignore` configured
- [ ] `docker compose up` functional test passes
- [ ] Graceful shutdown: `docker stop` → clean flush → data persists on restart
- [ ] Image size < 30 MB verified in CI
