package gateway

import (
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// ipBucket is a token bucket for a single IP address.
type ipBucket struct {
	tokens   float64
	lastSeen time.Time
}

// RateLimiter implements a per-IP token bucket rate limiter.
// Each IP starts with `burst` tokens and refills at `rate` tokens/second.
type RateLimiter struct {
	mu      sync.Mutex
	buckets map[string]*ipBucket
	rate    float64 // tokens per second
	burst   float64 // max tokens (burst capacity)
}

// NewRateLimiter creates a RateLimiter with the given rate and burst values.
func NewRateLimiter(ratePerSec float64, burst float64) *RateLimiter {
	rl := &RateLimiter{
		buckets: make(map[string]*ipBucket),
		rate:    ratePerSec,
		burst:   burst,
	}
	go rl.cleanup()
	return rl
}

// Allow returns true if the request from ip should be allowed.
func (rl *RateLimiter) Allow(ip string) bool {
	now := time.Now()
	rl.mu.Lock()
	defer rl.mu.Unlock()

	b, ok := rl.buckets[ip]
	if !ok {
		b = &ipBucket{tokens: rl.burst, lastSeen: now}
		rl.buckets[ip] = b
	}

	// Refill tokens based on elapsed time.
	elapsed := now.Sub(b.lastSeen).Seconds()
	b.tokens += elapsed * rl.rate
	if b.tokens > rl.burst {
		b.tokens = rl.burst
	}
	b.lastSeen = now

	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}

// cleanup periodically removes buckets for IPs that have been idle for >5 minutes.
func (rl *RateLimiter) cleanup() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		cutoff := time.Now().Add(-5 * time.Minute)
		rl.mu.Lock()
		for ip, b := range rl.buckets {
			if b.lastSeen.Before(cutoff) {
				delete(rl.buckets, ip)
			}
		}
		rl.mu.Unlock()
	}
}

// RateLimitMiddleware wraps h with per-IP rate limiting.
// Requests exceeding the limit receive HTTP 429.
func RateLimitMiddleware(rl *RateLimiter, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := extractIP(r)
		if !rl.Allow(ip) {
			slog.Warn("rate limit exceeded", "ip", ip, "path", r.URL.Path)
			writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
			return
		}
		h.ServeHTTP(w, r)
	})
}

// extractIP returns the client IP from the request, stripping the port.
func extractIP(r *http.Request) string {
	if forwarded := r.Header.Get("X-Forwarded-For"); forwarded != "" {
		for _, part := range strings.Split(forwarded, ",") {
			if ip := normalizeIPCandidate(part); ip != "" {
				return ip
			}
		}
	}
	if ip := normalizeIPCandidate(r.Header.Get("X-Real-IP")); ip != "" {
		return ip
	}
	return normalizeIPCandidate(r.RemoteAddr)
}

func normalizeIPCandidate(candidate string) string {
	candidate = strings.TrimSpace(candidate)
	if candidate == "" {
		return ""
	}
	if host, _, err := net.SplitHostPort(candidate); err == nil {
		candidate = host
	}
	if ip := net.ParseIP(candidate); ip != nil {
		return ip.String()
	}
	return ""
}
