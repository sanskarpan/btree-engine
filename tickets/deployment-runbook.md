# Deployment & Operations Runbook

> This document covers: deployment procedure, configuration, rollback, monitoring, and disaster recovery.

---

## 1. Prerequisites

| Requirement | Minimum | Recommended |
|-------------|---------|-------------|
| Go | 1.26 | 1.26+ |
| Bun | 1.2 | latest |
| OS | Linux (kernel ≥ 5.4) | Ubuntu 24.04 LTS |
| CPU | 1 core | 2+ cores |
| Memory | 256 MB | 1 GB |
| Disk (data) | 1 GB | SSD, 10+ GB |
| Disk (WAL) | 512 MB | SSD, same volume as data |

**Critical:** Data file and WAL file must be on the same filesystem to allow `rename(2)` for atomic catalog updates.

---

## 2. Build

```bash
# Production binary (stripped, trimmed)
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go build -ldflags="-s -w" -trimpath \
  -o bin/btree-server ./cmd/server

# Verify binary
./bin/btree-server --version  # should print version string
./bin/btree-server healthcheck || echo "server not running yet (expected)"
```

---

## 3. Configuration

### Minimum Production `config.yaml`

```yaml
engine:
  data_file: "/var/lib/btree/btree.db"

wal:
  log_file: "/var/lib/btree/wal"
  sync_on_commit: true     # CRITICAL: never set false in production
  max_segment_size: 268435456  # 256 MB

recovery:
  checkpoint_interval: "30s"

mvcc:
  default_isolation: "snapshot"
  vacuum_interval: "60s"
  vacuum_dead_threshold: 0.20

tree:
  fill_factor: 0.75
  min_fill_factor: 0.40

gateway:
  port: 8080
  api_keys:
    - "${BTREE_API_KEY}"   # injected from environment

log_level: "info"
log_format: "json"
```

### Environment Variables

```bash
BTREE_API_KEY=<randomly-generated-64-char-hex>
BTREE_LOG_LEVEL=info  # override log level without config edit
```

---

## 4. First-Time Deployment

```bash
# 1. Create data directory
sudo mkdir -p /var/lib/btree
sudo chown btree-user:btree-group /var/lib/btree
sudo chmod 750 /var/lib/btree

# 2. Copy binary and config
sudo cp bin/btree-server /usr/local/bin/
sudo cp config.yaml /etc/btree/config.yaml
sudo chmod 600 /etc/btree/config.yaml  # protect API keys

# 3. Create systemd unit
sudo tee /etc/systemd/system/btree-engine.service <<'EOF'
[Unit]
Description=B+Tree Storage Engine
After=network.target

[Service]
Type=simple
User=btree-user
Group=btree-group
ExecStart=/usr/local/bin/btree-server --config=/etc/btree/config.yaml
ExecStop=/bin/kill -TERM $MAINPID
TimeoutStopSec=30
Restart=on-failure
RestartSec=5
Environment="BTREE_API_KEY=change-me"
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF

# 4. Start
sudo systemctl daemon-reload
sudo systemctl enable btree-engine
sudo systemctl start btree-engine
sudo systemctl status btree-engine

# 5. Verify
sleep 5
curl http://localhost:8080/health/ready
# Expected: {"status":"ready","wal_flushed_lsn":0,"dirty_pages":0,"active_txns":0}
```

---

## 5. Rolling Update (Zero-Downtime)

This engine is **single-node** — there is no replica to route traffic to during update. The update procedure minimises downtime:

```bash
# 1. Build new binary
CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/btree-server-new ./cmd/server

# 2. Pre-flight: verify new binary starts on test data
./bin/btree-server-new --config=./test-config.yaml &
sleep 3
curl http://localhost:8082/health/ready  # test port
kill %1

# 3. Checkpoint + drain (minimises recovery time after restart)
curl -H "Authorization: Bearer $BTREE_API_KEY" \
     -X POST http://localhost:8080/api/v1/wal/checkpoint

# 4. Replace binary (atomic)
sudo cp bin/btree-server-new /usr/local/bin/btree-server.new
sudo mv /usr/local/bin/btree-server.new /usr/local/bin/btree-server  # atomic rename

# 5. Restart with SIGTERM (triggers graceful shutdown + flush)
sudo systemctl restart btree-engine

# 6. Verify recovery completed successfully
sleep 10
curl http://localhost:8080/health/ready
# Downtime window: ~2-5 seconds (graceful shutdown + ARIES recovery)
```

---

## 6. Rollback

```bash
# Keep previous binary at /usr/local/bin/btree-server.prev
sudo cp /usr/local/bin/btree-server /usr/local/bin/btree-server.prev
# (done automatically if using deployment script)

# To rollback:
sudo cp /usr/local/bin/btree-server.prev /usr/local/bin/btree-server
sudo systemctl restart btree-engine
curl http://localhost:8080/health/ready
```

**WAL compatibility:** The WAL format is backward-compatible within a minor version. Rolling back across a major version change that adds new WAL record types requires:
1. Stop engine
2. Run recovery once with new binary to apply redo
3. Checkpoint (makes new format WAL records unnecessary)
4. Roll back binary

---

## 7. Monitoring Checklist

### Essential Prometheus Alerts

```yaml
# docs/prometheus-alerts.yml

groups:
  - name: btree-engine
    rules:
      - alert: BtreeEngineDown
        expr: up{job="btree-engine"} == 0
        for: 1m
        severity: critical

      - alert: BtreeBufferHitRateLow
        expr: btree_buffer_hit_ratio < 0.60
        for: 5m
        severity: warning
        annotations:
          summary: "Buffer hit rate below 60% — consider increasing buffer_pool_size"

      - alert: BtreeWALSizeLarge
        expr: btree_wal_size_bytes > 2e9  # 2 GB
        for: 10m
        severity: warning
        annotations:
          summary: "WAL file larger than 2GB — checkpoint may be stuck"

      - alert: BtreeActiveTransactionsHigh
        expr: btree_txns_active > 80
        for: 2m
        severity: warning
        annotations:
          summary: "More than 80 active transactions — possible transaction leak"

      - alert: BtreeWriteConflictsSpike
        expr: rate(btree_txns_write_conflicts_total[5m]) > 10
        severity: warning
        annotations:
          summary: "High write conflict rate — application may need retry logic review"

      - alert: BtreeDiskWriteLatencyHigh
        expr: histogram_quantile(0.99, btree_disk_write_duration_seconds_bucket) > 0.1
        for: 5m
        severity: warning
        annotations:
          summary: "Disk write P99 latency > 100ms — possible disk pressure"

      - alert: BtreeRecoveryRunning
        expr: increase(btree_recovery_duration_seconds_count[5m]) > 0
        severity: info
        annotations:
          summary: "ARIES recovery was triggered — engine restarted after crash"
```

### Daily Operational Checks

```bash
# Buffer hit rate (should be > 80%)
curl -s http://localhost:8080/api/v1/engine/stats | jq .buffer_hit_rate

# WAL size
curl -s http://localhost:8080/api/v1/engine/stats | jq .wal_size_bytes

# Active transactions (should be near 0 at idle)
curl -s http://localhost:8080/api/v1/engine/stats | jq .active_txns

# Dirty pages (should not grow indefinitely — checkpoint clears them)
curl -s http://localhost:8080/api/v1/engine/stats | jq .dirty_pages

# Last checkpoint time
curl -s http://localhost:8080/health/ready | jq .last_checkpoint_ago_seconds
# Should be < checkpoint_interval (30s default)
```

---

## 8. Disaster Recovery

### Scenario 1: Clean Shutdown + Restart

```bash
sudo systemctl stop btree-engine   # SIGTERM → graceful flush
sudo systemctl start btree-engine  # ARIES recovery (should be instant — checkpoint was written on close)
curl http://localhost:8080/health/ready  # verify ready
```

**Expected recovery time:** < 1 second (checkpoint on close → recovery at checkpoint LSN).

---

### Scenario 2: Crash (Process Killed Without Flush)

```bash
# Simulate crash (for testing)
sudo kill -9 $(pidof btree-server)
sudo systemctl start btree-engine

# Monitor recovery in logs
journalctl -u btree-engine -f | grep -E "recovery|redo|undo"
```

**Expected recovery time:** Proportional to records since last checkpoint.
- With 30s checkpoint interval and 1000 commits/sec: ~30K records to replay
- At 1M records/sec replay speed: ~30ms recovery time

**Expected data state:**
- All data committed before crash: **present**
- Uncommitted transactions at crash time: **rolled back** (MVCC visibility — aborted Xmin = invisible)

---

### Scenario 3: Disk Full

**Symptoms:**
- `POST /api/v1/txn/:id/put` returns 500 with "no space left on device"
- `btree_disk_write_duration_seconds` spikes
- Engine logs ERROR: "WritePage failed: no space left on device"

**Recovery:**
```bash
# 1. Stop new writes (if possible — application-level circuit breaker)

# 2. Free space: truncate old WAL segments
curl -X POST -H "Authorization: Bearer $KEY" http://localhost:8080/api/v1/wal/checkpoint
# This triggers WAL truncation, freeing archived segments

# 3. If still full: delete old WAL segments manually
# ONLY delete segments BEFORE the checkpoint LSN
ls -lh /var/lib/btree/wal_*.log
# Check wal.catalog.json for oldest_required_lsn
# Delete segments entirely before that LSN

# 4. Resume engine
sudo systemctl restart btree-engine
```

---

### Scenario 4: Checksum Mismatch on Page Load

**Symptoms:**
- `FetchPage` returns error: "page N: checksum mismatch"
- Engine log ERROR with page ID

**Causes:**
- Torn write (partial page written before power failure)
- Disk corruption (hardware failure)
- Bug in page modification code (should not happen after Session 3 fixes)

**Recovery:**
```bash
# 1. Identify the affected page
grep "checksum mismatch" /var/log/btree/btree.log

# 2. If the page is in the WAL (i.e., a recent modification was WAL-logged):
#    ARIES redo will restore the correct version.
#    Stop the engine, restart — recovery should fix it.
sudo systemctl restart btree-engine

# 3. If recovery doesn't fix it: the page was modified before the last checkpoint
#    and the WAL records for it were truncated.
#    This means the data in that page is irrecoverably lost for that specific page.
#    Options:
#    a) Restore from backup up to the last known-good checkpoint
#    b) Surgically skip/zero the affected page (data loss for that page only)
```

**Prevention:**
- Full-page checksums (already implemented in Session 3) ✓
- UPS/battery-backed RAID for data directory
- WAL on a separate device from data file (survive partial disk failures)

---

### Scenario 5: Restore from Backup

```bash
# Assuming backup was taken with:
#   cp /var/lib/btree/btree.db /backup/btree.db.$(date +%Y%m%d)
#   cp /var/lib/btree/wal_*.log /backup/wal/
#   cp /var/lib/btree/wal.catalog.json /backup/

# 1. Stop engine
sudo systemctl stop btree-engine

# 2. Restore data file
sudo cp /backup/btree.db.20260509 /var/lib/btree/btree.db
sudo cp /backup/wal/* /var/lib/btree/
sudo cp /backup/wal.catalog.json /var/lib/btree/

# 3. Apply WAL records since backup (PITR — if WAL segments available)
#    With WAL archiving configured, segments since backup exist in archive.
#    Copy them to /var/lib/btree/ and update catalog.

# 4. Start engine — ARIES recovery applies WAL records
sudo systemctl start btree-engine
curl http://localhost:8080/health/ready
```

---

## 9. Backup Procedure

Until `P5-006` (btree-admin backup) is implemented:

```bash
#!/bin/bash
# Manual backup script (run as cron job)
set -euo pipefail

BACKUP_DIR="/backup/btree/$(date +%Y%m%d_%H%M%S)"
DATA_DIR="/var/lib/btree"
API_KEY="${BTREE_API_KEY}"

mkdir -p "$BACKUP_DIR"

# 1. Trigger checkpoint to minimise recovery work after restore
curl -s -X POST -H "Authorization: Bearer $API_KEY" \
     http://localhost:8080/api/v1/wal/checkpoint

# 2. Copy files (not perfectly consistent — may catch mid-write pages)
#    ARIES recovery on restore will fix any inconsistency.
cp "$DATA_DIR/btree.db" "$BACKUP_DIR/"
cp "$DATA_DIR"/wal_*.log "$BACKUP_DIR/" 2>/dev/null || true
cp "$DATA_DIR/wal.catalog.json" "$BACKUP_DIR/" 2>/dev/null || true

# 3. Record backup metadata
echo "{\"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\", \"data_dir\": \"$DATA_DIR\"}" \
     > "$BACKUP_DIR/backup.json"

echo "Backup complete: $BACKUP_DIR"
```

---

## 10. Capacity Planning

| Parameter | Formula | Example |
|-----------|---------|---------|
| Storage per key-value | `21 + len(key) + len(value)` bytes per version | key=32, value=64 → 117 bytes |
| Versions per key (with 10 updates) | 10 dead + 1 live | 11 × 117 = 1287 bytes |
| Pages needed (at 75% fill) | `total_data_bytes / (4096 × 0.75)` | 1M keys × 117 bytes / 3072 = ~38K pages |
| Storage (pages) | `pages × 4096` | 38K × 4096 ≈ 156 MB |
| WAL at 1000 commits/sec, 30s checkpoint | `1000 × 3_records × 50_bytes × 30` | ~4.5 MB per checkpoint interval |
| Buffer pool for 90% hit rate (Zipf) | `hot_set_pages × 4096` | top 10% of 38K pages = 3800 frames = 15 MB |

**Rule of thumb:** Provision buffer pool = `expected_working_set × 1.5`. If working set is 500 MB, set `buffer_pool_size: 200000` (800 MB pool).

---

## 11. Graceful Degradation

If the engine is under extreme memory or disk pressure, the following degradation order is expected:

1. **Write conflicts increase:** More concurrent writers → more ErrWriteConflict responses → application retries
2. **Buffer hit rate drops:** Pool exhaustion → more disk reads → increased latency
3. **Checkpoint falls behind:** Dirty pages accumulate → recovery time increases
4. **Vacuum falls behind:** Dead tuples accumulate → pages fill → ErrPageFull on insert → application error

**Mitigation at each level:**
1. Application retry with exponential backoff
2. Increase `buffer_pool_size` or reduce concurrent readers
3. Increase checkpoint interval or dedicated I/O path for WAL
4. Reduce `vacuum_interval` or increase `vacuum_dead_threshold`
