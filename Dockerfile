# ── Stage 1: build ───────────────────────────────────────────────────────────
FROM golang:1.26-alpine AS builder

WORKDIR /src

# Cache module downloads separately from source changes.
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build a statically-linked binary.
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags="-s -w" -o /btree-server ./cmd/server

# ── Stage 2: run ─────────────────────────────────────────────────────────────
FROM scratch

# Copy CA certificates for HTTPS (if the server ever calls external services).
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy the binary.
COPY --from=builder /btree-server /btree-server
COPY config.yaml /etc/btree/config.yaml

# Default config path; override with -e CONFIG_PATH or a volume mount.
ENV CONFIG_PATH=/etc/btree/config.yaml

# Data and WAL directories expected by config.yaml defaults.
VOLUME ["/data"]

EXPOSE 8080

ENTRYPOINT ["/btree-server"]
