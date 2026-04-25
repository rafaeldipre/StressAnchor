package stats

import (
	"encoding/json"
	"sync"
	"time"
)

type ProtocolStats struct {
	Connections       int64 `json:"connections"`
	ActiveConnections int64 `json:"active_connections"`
	BytesReceived     int64 `json:"bytes_received"`
	BytesSent         int64 `json:"bytes_sent"`
	Requests          int64 `json:"requests"`
	Errors            int64 `json:"errors"`
}

type ServerSnapshot struct {
	TotalConnections   int64                    `json:"total_connections"`
	ActiveConnections  int64                    `json:"active_connections"`
	BytesReceived      int64                    `json:"bytes_received"`
	BytesSent          int64                    `json:"bytes_sent"`
	RequestsPerSecond  float64                  `json:"requests_per_second"`
	Errors             int64                    `json:"errors"`
	UptimeSeconds      int64                    `json:"uptime_seconds"`
	PerProtocol        map[string]ProtocolStats `json:"per_protocol"`
	TotalRequests      int64                    `json:"total_requests"`
	LastUpdatedUnixNano int64                   `json:"last_updated_unix_ns"`
}

type ServerStats struct {
	mu       sync.Mutex
	started  time.Time
	lastTick time.Time
	lastReq  int64
	rps      float64

	totalConnections  int64
	activeConnections int64
	bytesReceived     int64
	bytesSent         int64
	totalRequests     int64
	errors            int64
	perProtocol       map[string]ProtocolStats
}

func NewServerStats() *ServerStats {
	return &ServerStats{
		started:  time.Now(),
		lastTick: time.Now(),
		perProtocol: map[string]ProtocolStats{
			"tcp":  {},
			"udp":  {},
			"http": {},
		},
	}
}

func (s *ServerStats) StartRPSCalculator(stop <-chan struct{}) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.RecalculateRPS(time.Now())
		case <-stop:
			return
		}
	}
}

func (s *ServerStats) RecalculateRPS(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	elapsed := now.Sub(s.lastTick).Seconds()
	if elapsed <= 0 {
		return
	}
	s.rps = float64(s.totalRequests-s.lastReq) / elapsed
	s.lastReq = s.totalRequests
	s.lastTick = now
}

func (s *ServerStats) OpenConnection(protocol string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.totalConnections++
	s.activeConnections++
	p := s.perProtocol[protocol]
	p.Connections++
	p.ActiveConnections++
	s.perProtocol[protocol] = p
}

func (s *ServerStats) CloseConnection(protocol string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activeConnections > 0 {
		s.activeConnections--
	}
	p := s.perProtocol[protocol]
	if p.ActiveConnections > 0 {
		p.ActiveConnections--
	}
	s.perProtocol[protocol] = p
}

func (s *ServerStats) AddRequest(protocol string, bytesReceived, bytesSent int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.totalRequests++
	s.bytesReceived += bytesReceived
	s.bytesSent += bytesSent
	p := s.perProtocol[protocol]
	p.Requests++
	p.BytesReceived += bytesReceived
	p.BytesSent += bytesSent
	s.perProtocol[protocol] = p
}

func (s *ServerStats) AddError(protocol string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errors++
	p := s.perProtocol[protocol]
	p.Errors++
	s.perProtocol[protocol] = p
}

func (s *ServerStats) Snapshot() ServerSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	perProtocol := make(map[string]ProtocolStats, len(s.perProtocol))
	for k, v := range s.perProtocol {
		perProtocol[k] = v
	}
	return ServerSnapshot{
		TotalConnections:   s.totalConnections,
		ActiveConnections:  s.activeConnections,
		BytesReceived:      s.bytesReceived,
		BytesSent:          s.bytesSent,
		RequestsPerSecond:  s.rps,
		Errors:             s.errors,
		UptimeSeconds:      int64(time.Since(s.started).Seconds()),
		PerProtocol:        perProtocol,
		TotalRequests:      s.totalRequests,
		LastUpdatedUnixNano: time.Now().UnixNano(),
	}
}

func (s ServerSnapshot) JSON() ([]byte, error) {
	return json.MarshalIndent(s, "", "  ")
}
