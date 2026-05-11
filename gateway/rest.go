package gateway

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"btree-engine/internal/btree"
	"btree-engine/internal/engine"
	"btree-engine/internal/mvcc"
	"btree-engine/internal/simulation"
)

const (
	maxKeySize   = 512
	maxValueSize = 65535
)

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v) //nolint
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "GET only")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleHealthLive is a Kubernetes-style liveness probe: always 200 while process is up.
func (s *Server) handleHealthLive(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "GET only")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "live"})
}

// handleHealthReady returns 503 during shutdown so load balancers stop routing traffic.
func (s *Server) handleHealthReady(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "GET only")
		return
	}
	if s.isShuttingDown.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "shutting_down"})
		return
	}
	stats := s.eng.Stats()
	lastCkptNano := s.eng.LastCheckpointAt()
	var ckptAgo float64
	if lastCkptNano > 0 {
		ckptAgo = time.Since(time.Unix(0, lastCkptNano)).Seconds()
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":                      "ready",
		"dirty_pages":                 stats.DirtyPages,
		"active_txns":                 stats.ActiveTxns,
		"last_checkpoint_lsn":         s.eng.LastCheckpointLSN(),
		"last_checkpoint_ago_seconds": ckptAgo,
	})
}

// -------- Engine --------

func (s *Server) handleEngineStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "GET only")
		return
	}
	writeJSON(w, http.StatusOK, s.eng.Stats())
}

func (s *Server) handleCrash(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "POST only")
		return
	}
	s.mu.Lock()
	s.txns = make(map[uint64]*mvcc.Transaction)
	s.eng.CrashClose() // hold lock so concurrent requests don't race against a half-closed engine
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]string{"status": "crashed"})
}

func (s *Server) handleRecover(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "POST only")
		return
	}
	eng, err := engine.OpenEngine(s.cfg)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	var old *engine.StorageEngine
	s.mu.Lock()
	old = s.eng
	s.eng = eng
	s.txns = make(map[uint64]*mvcc.Transaction)
	s.txnLastUsed = make(map[uint64]time.Time)
	s.wsHub.updateBus(eng.EventBus())
	if s.metricsCollector != nil {
		s.metricsCollector.Subscribe(eng.EventBus())
	}
	s.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "recovered"})
}

// -------- Transactions --------

func (s *Server) handleTxnBegin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "POST only")
		return
	}
	var req struct {
		IsoLevel string `json:"iso_level"`
	}
	json.NewDecoder(r.Body).Decode(&req) //nolint

	isoName := strings.TrimSpace(req.IsoLevel)
	if isoName == "" {
		isoName = s.defaultIsolationName()
	}
	var iso mvcc.IsolationLevel
	switch isoName {
	case "read_committed":
		iso = mvcc.ReadCommitted
	case "serializable":
		iso = mvcc.Serializable
	case "snapshot":
		iso = mvcc.SnapshotIsolation
	default:
		iso = mvcc.SnapshotIsolation
		isoName = "snapshot"
	}

	s.mu.Lock()
	txn := s.eng.Begin(iso)
	id := uint64(txn.ID)
	s.txns[id] = txn
	s.txnLastUsed[id] = time.Now()
	s.mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{
		"txn_id":    id,
		"iso_level": isoName,
	})
}

// handleTxnRouter dispatches /api/v1/txn/:id/... routes.
func (s *Server) handleTxnRouter(w http.ResponseWriter, r *http.Request) {
	// path: /api/v1/txn/<id>/<op>
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/v1/txn/"), "/")
	if len(parts) < 2 {
		writeError(w, http.StatusBadRequest, "path: /api/v1/txn/<id>/<op>")
		return
	}
	idStr, op := parts[0], parts[1]
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid txn id")
		return
	}

	s.mu.Lock()
	txn := s.txns[id]
	if txn != nil {
		s.txnLastUsed[id] = time.Now()
	}
	s.mu.Unlock()
	if txn == nil {
		writeError(w, http.StatusNotFound, "transaction not found")
		return
	}

	switch op {
	case "commit":
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "POST only")
			return
		}
		if err := s.eng.Commit(txn); err != nil {
			if errors.Is(err, mvcc.ErrSerializationFailure) {
				writeError(w, http.StatusConflict, err.Error())
				return
			}
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		s.mu.Lock()
		delete(s.txns, id)
		delete(s.txnLastUsed, id)
		s.mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]string{"status": "committed"})

	case "abort":
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "POST only")
			return
		}
		if err := s.eng.Abort(txn); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		s.mu.Lock()
		delete(s.txns, id)
		delete(s.txnLastUsed, id)
		s.mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]string{"status": "aborted"})

	case "put":
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "POST only")
			return
		}
		var req struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if req.Key == "" {
			writeError(w, http.StatusBadRequest, "key must not be empty")
			return
		}
		if len(req.Key) > maxKeySize {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("key exceeds max size of %d bytes", maxKeySize))
			return
		}
		if len(req.Value) > maxValueSize {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("value exceeds max size of %d bytes", maxValueSize))
			return
		}
		if err := s.eng.Put(txn, []byte(req.Key), []byte(req.Value)); err != nil {
			if errors.Is(err, btree.ErrWriteConflict) {
				writeError(w, http.StatusConflict, err.Error())
				return
			}
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})

	case "get":
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "GET only")
			return
		}
		key := r.URL.Query().Get("key")
		if key == "" {
			writeError(w, http.StatusBadRequest, "key query param required")
			return
		}
		if len(key) > maxKeySize {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("key exceeds max size of %d bytes", maxKeySize))
			return
		}
		val, err := s.eng.Get(txn, []byte(key))
		if err != nil {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"key": key, "value": string(val)})

	case "delete":
		if r.Method != http.MethodDelete && r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "DELETE or POST only")
			return
		}
		var req struct {
			Key string `json:"key"`
		}
		if r.Method == http.MethodDelete {
			req.Key = r.URL.Query().Get("key")
		} else {
			json.NewDecoder(r.Body).Decode(&req) //nolint
		}
		if req.Key == "" {
			writeError(w, http.StatusBadRequest, "key must not be empty")
			return
		}
		if len(req.Key) > maxKeySize {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("key exceeds max size of %d bytes", maxKeySize))
			return
		}
		if err := s.eng.Delete(txn, []byte(req.Key)); err != nil {
			if errors.Is(err, btree.ErrWriteConflict) {
				writeError(w, http.StatusConflict, err.Error())
				return
			}
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})

	case "scan":
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "GET only")
			return
		}
		start := r.URL.Query().Get("start")
		end := r.URL.Query().Get("end")
		var startBytes, endBytes []byte
		if start != "" {
			startBytes = []byte(start)
		}
		if end != "" {
			endBytes = []byte(end)
		}
		cursor, err := s.eng.Scan(txn, startBytes, endBytes)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer cursor.Close()

		var results []map[string]string
		for {
			t, err := cursor.Next()
			if err != nil {
				break
			}
			results = append(results, map[string]string{
				"key":   string(t.Key),
				"value": string(t.Value),
			})
		}
		if results == nil {
			results = []map[string]string{}
		}
		writeJSON(w, http.StatusOK, map[string]any{"results": results, "count": len(results)})

	case "savepoint":
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "POST only")
			return
		}
		var req struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
			writeError(w, http.StatusBadRequest, "body: {\"name\": \"<savepoint_name>\"}")
			return
		}
		if err := s.eng.Savepoint(txn, req.Name); err != nil {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "savepoint_created", "name": req.Name})

	case "rollback_to_savepoint":
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "POST only")
			return
		}
		var req struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
			writeError(w, http.StatusBadRequest, "body: {\"name\": \"<savepoint_name>\"}")
			return
		}
		if err := s.eng.RollbackToSavepoint(txn, req.Name); err != nil {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "rolled_back", "name": req.Name})

	default:
		writeError(w, http.StatusNotFound, "unknown operation")
	}
}

func (s *Server) defaultIsolationName() string {
	switch strings.TrimSpace(s.cfg.DefaultIsolation) {
	case "read_committed", "serializable", "snapshot":
		return strings.TrimSpace(s.cfg.DefaultIsolation)
	default:
		return "snapshot"
	}
}

// -------- Tree Inspection --------

func (s *Server) handleTreeStructure(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "GET only")
		return
	}
	s.mu.RLock()
	node := s.eng.TreeStructure()
	s.mu.RUnlock()
	writeJSON(w, http.StatusOK, node)
}

func (s *Server) handleTreePage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "GET only")
		return
	}
	idStr := strings.TrimPrefix(r.URL.Path, "/api/v1/tree/page/")
	id, err := strconv.ParseUint(idStr, 10, 32)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid page id")
		return
	}
	s.mu.RLock()
	result, err2 := s.eng.PageContents(uint32(id))
	s.mu.RUnlock()
	if err2 != nil {
		writeError(w, http.StatusNotFound, fmt.Sprintf("page %d not found or unreadable: %s", id, err2.Error()))
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// -------- MVCC --------

func (s *Server) handleMVCCVersions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "GET only")
		return
	}
	key := r.URL.Query().Get("key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "key query param required")
		return
	}
	s.mu.RLock()
	versions := s.eng.MVCCVersions([]byte(key))
	s.mu.RUnlock()
	writeJSON(w, http.StatusOK, map[string]any{"key": key, "versions": versions})
}

func (s *Server) handleMVCCVisibility(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "GET only")
		return
	}
	key := r.URL.Query().Get("key")
	txnIDStr := r.URL.Query().Get("txnID")
	if key == "" || txnIDStr == "" {
		writeError(w, http.StatusBadRequest, "key and txnID query params required")
		return
	}
	id, err := strconv.ParseUint(txnIDStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid txn id")
		return
	}
	s.mu.Lock()
	txn := s.txns[id]
	if txn != nil {
		s.txnLastUsed[id] = time.Now()
		if txn.IsoLevel == mvcc.ReadCommitted {
			txn.Snapshot = s.eng.TakeSnapshot(txn.ID)
		}
	}
	s.mu.Unlock()
	if txn == nil {
		writeError(w, http.StatusNotFound, "transaction not found")
		return
	}

	s.mu.RLock()
	annotated := s.eng.MVCCVisibilityForTxn([]byte(key), txn)
	s.mu.RUnlock()
	writeJSON(w, http.StatusOK, map[string]any{
		"key":      key,
		"txn_id":   id,
		"versions": annotated,
	})
}

// -------- Buffer Pool --------

func (s *Server) handleBufferStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "GET only")
		return
	}
	stats := s.eng.Stats()
	writeJSON(w, http.StatusOK, stats)
}

func (s *Server) handleBufferFrames(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "GET only")
		return
	}
	frames := s.eng.Frames()
	writeJSON(w, http.StatusOK, map[string]any{"frames": frames, "count": len(frames)})
}

// -------- WAL --------

func (s *Server) handleWALTail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "GET only")
		return
	}
	nStr := r.URL.Query().Get("n")
	n := 50
	if nStr != "" {
		if v, err := strconv.Atoi(nStr); err == nil && v > 0 {
			n = v
		}
	}
	records := s.eng.WALTail(n)
	writeJSON(w, http.StatusOK, map[string]any{"records": records, "count": len(records)})
}

func (s *Server) handleCheckpoint(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "POST only")
		return
	}
	s.mu.RLock()
	lsn, err := s.eng.Checkpoint()
	s.mu.RUnlock()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "checkpoint_lsn": lsn})
}

// -------- Scenarios --------

func (s *Server) handleListScenarios(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "GET only")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"scenarios": simulation.ScenarioNames()})
}

func (s *Server) handleRunScenario(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "POST only")
		return
	}
	name := strings.TrimSuffix(
		strings.TrimPrefix(r.URL.Path, "/api/v1/scenarios/"),
		"/run",
	)
	s.mu.Lock()
	result, err := simulation.Run(name, s.eng)
	s.mu.Unlock()
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}
