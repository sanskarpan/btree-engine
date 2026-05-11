package gateway

import (
	"encoding/json"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"btree-engine/internal/events"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: allowWebSocketOrigin,
}

// wsClient is one connected WebSocket client.
type wsClient struct {
	conn      *websocket.Conn
	send      chan []byte
	done      chan struct{}
	closeOnce sync.Once
}

// wsHub fans engine events out to all connected WebSocket clients.
type wsHub struct {
	mu      sync.RWMutex
	clients map[*wsClient]struct{}
	unsub   func()
}

func newWSHub(bus *events.EventBus) *wsHub {
	h := &wsHub{clients: make(map[*wsClient]struct{})}
	if bus != nil {
		h.unsub = bus.SubscribeAll(func(evt events.Event) {
			data, _ := json.Marshal(evt)
			h.broadcast(data)
		})
	}
	return h
}

func (h *wsHub) updateBus(bus *events.EventBus) {
	if h.unsub != nil {
		h.unsub()
	}
	if bus != nil {
		h.unsub = bus.SubscribeAll(func(evt events.Event) {
			data, _ := json.Marshal(evt)
			h.broadcast(data)
		})
	}
}

func (h *wsHub) add(c *wsClient) {
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
}

func (h *wsHub) remove(c *wsClient) {
	h.mu.Lock()
	delete(h.clients, c)
	h.mu.Unlock()
	c.closeOnce.Do(func() {
		close(c.done)
	})
}

func (h *wsHub) broadcast(data []byte) {
	h.mu.RLock()
	clients := make([]*wsClient, 0, len(h.clients))
	for c := range h.clients {
		clients = append(clients, c)
	}
	h.mu.RUnlock()
	for _, c := range clients {
		select {
		case <-c.done:
			continue
		default:
		}
		select {
		case c.send <- data:
		default: // drop if full
		}
	}
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	client := &wsClient{
		conn: conn,
		send: make(chan []byte, s.wsBufferSize()),
		done: make(chan struct{}),
	}
	s.wsHub.add(client)
	defer func() {
		s.wsHub.remove(client)
		_ = conn.Close()
	}()

	// Write pump
	go func() {
		for {
			select {
			case <-client.done:
				return
			case data := <-client.send:
				if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
					return
				}
			}
		}
	}()

	// Read pump (drain incoming messages; close on error)
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			break
		}
	}
}

func allowWebSocketOrigin(r *http.Request) bool {
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	host := canonicalHost(r.Host)
	originHost := canonicalHost(u.Host)
	if host != "" && originHost == host {
		return true
	}
	return isLoopbackHost(originHost)
}

func canonicalHost(hostport string) string {
	if hostport == "" {
		return ""
	}
	if host, _, err := net.SplitHostPort(hostport); err == nil {
		return strings.ToLower(host)
	}
	return strings.ToLower(hostport)
}

func isLoopbackHost(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	switch host {
	case "localhost", "127.0.0.1", "::1":
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
