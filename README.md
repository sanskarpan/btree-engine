# B+Tree Storage Engine

A from-scratch B+Tree storage engine in Go with MVCC, WAL/ARIES crash recovery, a REST+WebSocket gateway, and an interactive D3.js visualization frontend.

## Architecture

```
┌─────────────────────────────────────────────────┐
│  Frontend (Elysia BFF :3001 → D3 panels)        │
├─────────────────────────────────────────────────┤
│  Gateway (HTTP REST + WebSocket :8080)           │
├──────────────────┬──────────────────────────────┤
│  StorageEngine   │  MVCC layer                   │
│  (engine.go)     │  (TransactionManager,         │
│                  │   Snapshot, VacuumWorker)      │
├──────────────────┼──────────────────────────────┤
│  B+Tree          │  WAL (ARIES)                  │
│  (btree/)        │  (wal/, recovery/)             │
├──────────────────┴──────────────────────────────┤
│  Buffer Pool (Clock eviction)                    │
├─────────────────────────────────────────────────┤
│  Disk Manager (slotted 4096-byte pages)          │
└─────────────────────────────────────────────────┘
```

## B+Tree vs LSM-Tree Comparison

| Property               | B+Tree (this engine)            | LSM-Tree (e.g. RocksDB)         |
|------------------------|---------------------------------|----------------------------------|
| **Write path**         | In-place update via buffer pool | Append to memtable → SSTables    |
| **Read path**          | O(log N) single tree traversal  | Check memtable + each SST level  |
| **Write amplification**| Low (1 WAL write + 1 page)     | High (compaction rewrites data)  |
| **Read amplification** | Low (height of tree)            | Can be high (multiple SSTables)  |
| **Space amplification**| Low (in-place)                  | High (multiple versions on disk) |
| **Range scans**        | Excellent (linked leaf pages)   | Good (sorted SSTables)           |
| **Crash recovery**     | ARIES WAL + redo/undo           | WAL + SSTable atomicity          |
| **Concurrent writes**  | MVCC (snapshot isolation)       | MVCC or lock-based               |
| **Best for**           | Read-heavy, random access       | Write-heavy, sequential inserts  |

## Setup

### Prerequisites

- Go 1.22+
- Bun 1.x (for frontend)
- golangci-lint (optional, for linting)

### Run backend

```bash
go run ./cmd/server --config config.yaml
# Backend starts on :8080
```

### Run frontend

```bash
cd frontend
bun install
BFF_PORT=3001 BACKEND_URL=http://localhost:8080 BACKEND_API_KEY=dev-key-change-me bun run dev
# Builds the UI bundle, starts the BFF on :3001, and serves the console at http://localhost:3001
```

### Run tests

```bash
go test ./... -race -count=1
```

### Run linter

```bash
golangci-lint run ./...
```

## Frontend Panels

The D3.js frontend has 7 interactive panels:

| Panel | Description |
|-------|-------------|
| **Tree View** | Live B+Tree structure with internal and leaf nodes; click a node to inspect its page |
| **Page Inspector** | Hex-level view of any page: fill factor bar, slot table with Xmin/Xmax/key/value/status |
| **MVCC Versions** | Per-key version history as horizontal timeline bars, color-coded by visibility |
| **Isolation Demo** | Side-by-side Read Committed vs Snapshot Isolation with write-skew banner |
| **Buffer Pool** | Grid visualization of all frames; color-coded empty/clean/dirty/pinned |
| **WAL Log** | Scrolling WAL tail with Crash/Recover/Checkpoint buttons |
| **Scenarios & Metrics** | Pre-built scenario runner + live metrics table + event count bar chart |

## Key Design Decisions

### MVCC Isolation Levels

- **Snapshot Isolation** — one snapshot taken at `BEGIN`; all reads see the same consistent snapshot. Write-skew is possible (demonstrable via the write-skew scenario).
- **Read Committed** — snapshot refreshed before each `Get()` call; non-repeatable reads are allowed.

### ARIES Recovery

Three phases:
1. **Analysis** — replay WAL from last checkpoint, rebuild DPT and ATT.
2. **Redo** — reapply all logged operations to bring pages forward.
3. **Undo** — roll back all uncommitted transactions using CLR records.

### Buffer Pool

Clock (second-chance) eviction. All dirty pages have a `recLSN` for ARIES's dirty page table. The buffer pool flushes pages respecting WAL-before-page ordering.

## Page Format

See [page format diagram](docs/page_format.md).

## MVCC Visibility Rules

See [visibility flowchart](docs/mvcc_visibility.md).
