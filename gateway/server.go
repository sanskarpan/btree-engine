// Package gateway provides the HTTP REST API and WebSocket server for the storage engine.
package gateway

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"btree-engine/internal/engine"
	"btree-engine/internal/metrics"
	"btree-engine/internal/mvcc"
)

// Server wraps the storage engine and exposes it via HTTP.
type Server struct {
	eng              *engine.StorageEngine
	cfg              engine.Config
	port             int
	mu               sync.RWMutex
	txns             map[uint64]*mvcc.Transaction // active transactions by ID
	txnLastUsed      map[uint64]time.Time         // last operation time per txn
	wsHub            *wsHub
	isShuttingDown   atomic.Bool
	metricsHandler   http.Handler
	metricsCollector *metrics.Collector
}

// NewServer creates a gateway server backed by eng.
func NewServer(eng *engine.StorageEngine, cfg engine.Config, port int) *Server {
	s := &Server{
		eng:         eng,
		cfg:         cfg,
		port:        port,
		txns:        make(map[uint64]*mvcc.Transaction),
		txnLastUsed: make(map[uint64]time.Time),
	}
	s.wsHub = newWSHub(eng.EventBus())

	// Create Prometheus registry and wire up the metrics collector.
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	coll := metrics.NewCollector(reg)
	coll.Subscribe(eng.EventBus())
	s.metricsCollector = coll
	s.metricsHandler = promhttp.HandlerFor(reg, promhttp.HandlerOpts{})

	return s
}

// Run starts the HTTP server, blocks until SIGTERM/SIGINT, drains in-flight
// requests, then calls eng.Close(). It is the owner of the engine's lifecycle
// from this point forward.
func (s *Server) Run(eng *engine.StorageEngine) error {
	mux := http.NewServeMux()
	s.registerRoutes(mux)

	// Apply rate limiting (outermost — limits all traffic including auth failures).
	var handler http.Handler = mux
	if s.cfg.RateLimitPerSec > 0 {
		rl := NewRateLimiter(s.cfg.RateLimitPerSec, s.cfg.RateLimitBurst)
		handler = RateLimitMiddleware(rl, mux)
	}

	// Apply auth middleware (after rate limiting so rate limit applies pre-auth too).
	if len(s.cfg.APIKeys) > 0 {
		keys := NewAPIKeySet(s.cfg.APIKeys)
		exempt := []string{"/health", "/health/live", "/health/ready", "/metrics"}
		handler = AuthMiddleware(keys, exempt, handler)
	}

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", s.port),
		Handler: handler,
	}

	// Start idle transaction pruner.
	prunerDone := make(chan struct{})
	if s.cfg.IdleTxnTimeout > 0 {
		go s.runIdlePruner(prunerDone)
	}

	// Start HTTP listener in the background.
	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	// Block until OS signal or HTTP server error.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)

	select {
	case sig := <-quit:
		slog.Info("shutdown signal received", "signal", sig.String())
	case err := <-errCh:
		close(prunerDone)
		return err
	}

	// Signal health/ready to return 503 so load balancers stop routing.
	s.isShuttingDown.Store(true)
	close(prunerDone)

	drainTimeout := s.cfg.RequestDrainTimeout
	if drainTimeout <= 0 {
		drainTimeout = 30 * time.Second
	}
	slog.Info("draining in-flight requests", "timeout_s", drainTimeout.Seconds())

	ctx, cancel := context.WithTimeout(context.Background(), drainTimeout)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		slog.Warn("http drain timed out", "err", err)
	}

	// Abort any transactions that were not cleaned up during drain.
	s.mu.Lock()
	for id, txn := range s.txns {
		slog.Warn("aborting open transaction on shutdown", "txn_id", id)
		_ = eng.Abort(txn)
	}
	s.txns = make(map[uint64]*mvcc.Transaction)
	s.txnLastUsed = make(map[uint64]time.Time)
	s.mu.Unlock()

	slog.Info("closing engine")
	if err := eng.Close(); err != nil {
		return err
	}
	slog.Info("shutdown complete")
	return nil
}

// runIdlePruner periodically aborts transactions idle longer than IdleTxnTimeout.
func (s *Server) runIdlePruner(done <-chan struct{}) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.pruneIdleTxns()
		case <-done:
			return
		}
	}
}

func (s *Server) pruneIdleTxns() {
	if s.cfg.IdleTxnTimeout <= 0 {
		return
	}
	cutoff := time.Now().Add(-s.cfg.IdleTxnTimeout)
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, lastUsed := range s.txnLastUsed {
		if lastUsed.Before(cutoff) {
			if txn, ok := s.txns[id]; ok {
				slog.Warn("aborting idle transaction",
					"txn_id", id, "idle_s", time.Since(lastUsed).Seconds())
				_ = s.eng.Abort(txn)
				delete(s.txns, id)
			}
			delete(s.txnLastUsed, id)
		}
	}
}

func (s *Server) registerRoutes(mux *http.ServeMux) {
	// Health and metrics endpoints are exempt from auth.
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/health/live", s.handleHealthLive)
	mux.HandleFunc("/health/ready", s.handleHealthReady)
	if s.metricsHandler != nil {
		mux.Handle("/metrics", s.metricsHandler)
	}

	// Engine
	mux.HandleFunc("/api/v1/engine/stats", s.handleEngineStats)
	mux.HandleFunc("/api/v1/engine/crash", s.handleCrash)
	mux.HandleFunc("/api/v1/engine/recover", s.handleRecover)

	// Transactions
	mux.HandleFunc("/api/v1/txn/begin", s.handleTxnBegin)
	mux.HandleFunc("/api/v1/txn/", s.handleTxnRouter)

	// Tree inspection
	mux.HandleFunc("/api/v1/tree/structure", s.handleTreeStructure)
	mux.HandleFunc("/api/v1/tree/page/", s.handleTreePage)

	// MVCC inspection
	mux.HandleFunc("/api/v1/mvcc/versions", s.handleMVCCVersions)
	mux.HandleFunc("/api/v1/mvcc/visibility", s.handleMVCCVisibility)

	// Buffer pool
	mux.HandleFunc("/api/v1/buffer/stats", s.handleBufferStats)
	mux.HandleFunc("/api/v1/buffer/frames", s.handleBufferFrames)

	// WAL
	mux.HandleFunc("/api/v1/wal/tail", s.handleWALTail)
	mux.HandleFunc("/api/v1/wal/checkpoint", s.handleCheckpoint)

	// Scenarios
	mux.HandleFunc("/api/v1/scenarios", s.handleListScenarios)
	mux.HandleFunc("/api/v1/scenarios/", s.handleRunScenario)

	// WebSocket
	mux.HandleFunc("/ws", s.handleWebSocket)
}

func (s *Server) wsBufferSize() int {
	if s.cfg.WSBuffer > 0 {
		return s.cfg.WSBuffer
	}
	return 256
}
