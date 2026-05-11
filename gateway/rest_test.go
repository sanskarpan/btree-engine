package gateway

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"btree-engine/internal/engine"
	"btree-engine/internal/mvcc"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func openGatewayTestServer(t *testing.T) (*Server, *engine.StorageEngine, func()) {
	t.Helper()
	cfg := engine.DefaultConfig(t.TempDir())
	cfg.SyncWAL = true
	eng, err := engine.OpenEngine(cfg)
	require.NoError(t, err)
	srv := NewServer(eng, cfg, 0)
	return srv, eng, func() { _ = eng.Close() }
}

func TestHandleMVCCVisibility_UsesTransactionSnapshot(t *testing.T) {
	srv, eng, cleanup := openGatewayTestServer(t)
	defer cleanup()

	setup := eng.Begin(mvcc.SnapshotIsolation)
	require.NoError(t, eng.Put(setup, []byte("balance"), []byte("100")))
	require.NoError(t, eng.Commit(setup))

	snapshotTxn := eng.Begin(mvcc.SnapshotIsolation)

	upd := eng.Begin(mvcc.SnapshotIsolation)
	require.NoError(t, eng.Update(upd, []byte("balance"), []byte("200")))
	require.NoError(t, eng.Commit(upd))

	srv.mu.Lock()
	srv.txns[uint64(snapshotTxn.ID)] = snapshotTxn
	srv.txnLastUsed[uint64(snapshotTxn.ID)] = time.Now()
	srv.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet,
		fmt.Sprintf("/api/v1/mvcc/visibility?key=balance&txnID=%d", snapshotTxn.ID), nil)
	rr := httptest.NewRecorder()
	srv.handleMVCCVisibility(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	var resp struct {
		Versions []engine.VersionVisibilityInfo `json:"versions"`
	}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	require.Len(t, resp.Versions, 2)

	byValue := make(map[string]engine.VersionVisibilityInfo, len(resp.Versions))
	for _, v := range resp.Versions {
		byValue[v.Value] = v
	}
	assert.True(t, byValue["100"].Visible)
	assert.False(t, byValue["200"].Visible)
	assert.Contains(t, byValue["200"].Reason, "after this snapshot")

	require.NoError(t, eng.Abort(snapshotTxn))
}

func TestHandleMVCCVisibility_UnknownTxn(t *testing.T) {
	srv, _, cleanup := openGatewayTestServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/mvcc/visibility?key=k&txnID=999", nil)
	rr := httptest.NewRecorder()
	srv.handleMVCCVisibility(rr, req)

	require.Equal(t, http.StatusNotFound, rr.Code)
}

func TestHandleTxnBegin_UsesConfiguredDefaultIsolation(t *testing.T) {
	srv, _, cleanup := openGatewayTestServer(t)
	defer cleanup()

	srv.cfg.DefaultIsolation = "serializable"
	req := httptest.NewRequest(http.MethodPost, "/api/v1/txn/begin", nil)
	rr := httptest.NewRecorder()
	srv.handleTxnBegin(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	var resp struct {
		TxnID    uint64 `json:"txn_id"`
		IsoLevel string `json:"iso_level"`
	}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	require.Equal(t, "serializable", resp.IsoLevel)
}
