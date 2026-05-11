package gateway

import (
	"context"
	"net/http/httptest"
	"testing"

	"btree-engine/internal/engine"

	"github.com/stretchr/testify/assert"
)

func TestAllowWebSocketOrigin(t *testing.T) {
	req := httptest.NewRequestWithContext(context.Background(), "GET", "http://127.0.0.1:8080/ws", nil)
	req.Host = "127.0.0.1:8080"
	req.Header.Set("Origin", "http://127.0.0.1:3001")
	assert.True(t, allowWebSocketOrigin(req))
}

func TestAllowWebSocketOrigin_SameHost(t *testing.T) {
	req := httptest.NewRequestWithContext(context.Background(), "GET", "https://example.com/ws", nil)
	req.Host = "example.com"
	req.Header.Set("Origin", "https://example.com")
	assert.True(t, allowWebSocketOrigin(req))
}

func TestAllowWebSocketOrigin_RejectsForeignOrigin(t *testing.T) {
	req := httptest.NewRequestWithContext(context.Background(), "GET", "https://example.com/ws", nil)
	req.Host = "example.com"
	req.Header.Set("Origin", "https://evil.example")
	assert.False(t, allowWebSocketOrigin(req))
}

func TestWSBufferSize_UsesConfig(t *testing.T) {
	srv := &Server{cfg: engine.Config{WSBuffer: 1024}}
	assert.Equal(t, 1024, srv.wsBufferSize())

	srv = &Server{cfg: engine.Config{}}
	assert.Equal(t, 256, srv.wsBufferSize())
}
