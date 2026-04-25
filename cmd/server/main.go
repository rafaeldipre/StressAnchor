package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"mesh-stress-test/internal/stats"
)

func main() {
	host := flag.String("host", "0.0.0.0", "host/interface to bind")
	portTCP := flag.Int("port-tcp", 9000, "TCP echo port")
	portUDP := flag.Int("port-udp", 9001, "UDP echo port")
	portHTTP := flag.Int("port-http", 8080, "HTTP port")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serverStats := stats.NewServerStats()
	rpsStop := make(chan struct{})
	go serverStats.StartRPSCalculator(rpsStop)
	defer close(rpsStop)

	errCh := make(chan error, 3)
	go func() { errCh <- serveTCP(ctx, *host, *portTCP, serverStats) }()
	go func() { errCh <- serveUDP(ctx, *host, *portUDP, serverStats) }()
	go func() { errCh <- serveHTTP(ctx, *host, *portHTTP, serverStats) }()

	log.Printf("stress-server listening: tcp=%s udp=%s http=%s",
		net.JoinHostPort(*host, strconv.Itoa(*portTCP)),
		net.JoinHostPort(*host, strconv.Itoa(*portUDP)),
		net.JoinHostPort(*host, strconv.Itoa(*portHTTP)),
	)

	select {
	case <-ctx.Done():
		log.Println("shutdown requested")
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
			log.Fatalf("server failed: %v", err)
		}
	}
	time.Sleep(250 * time.Millisecond)
}

func serveTCP(ctx context.Context, host string, port int, st *stats.ServerStats) error {
	ln, err := net.Listen("tcp", net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		return err
	}
	defer ln.Close()
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			st.AddError("tcp")
			continue
		}
		go handleTCPConn(conn, st)
	}
}

func handleTCPConn(conn net.Conn, st *stats.ServerStats) {
	st.OpenConnection("tcp")
	defer st.CloseConnection("tcp")
	defer conn.Close()

	buf := make([]byte, 64*1024)
	for {
		n, err := conn.Read(buf)
		if n > 0 {
			written, writeErr := conn.Write(buf[:n])
			st.AddRequest("tcp", int64(n), int64(written))
			if writeErr != nil {
				st.AddError("tcp")
				return
			}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				st.AddError("tcp")
			}
			return
		}
	}
}

func serveUDP(ctx context.Context, host string, port int, st *stats.ServerStats) error {
	addr, err := net.ResolveUDPAddr("udp", net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		return err
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	buf := make([]byte, 64*1024)
	for {
		n, remote, err := conn.ReadFromUDP(buf)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			st.AddError("udp")
			continue
		}
		st.OpenConnection("udp")
		written, writeErr := conn.WriteToUDP(buf[:n], remote)
		st.AddRequest("udp", int64(n), int64(written))
		st.CloseConnection("udp")
		if writeErr != nil {
			st.AddError("udp")
		}
	}
}

func serveHTTP(ctx context.Context, host string, port int, st *stats.ServerStats) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		writeHTTP(w, st, 0, int64(len(dashboardHTML)), func() {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = io.WriteString(w, dashboardHTML)
		})
	})
	mux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			st.AddError("http")
			return
		}
		body := fmt.Sprintf(`{"status":"ok","ts":%d}`+"\n", time.Now().UnixNano())
		writeHTTP(w, st, 0, int64(len(body)), func() {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, body)
		})
	})
	mux.HandleFunc("/data", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			st.AddError("http")
			return
		}
		n, err := io.Copy(io.Discard, r.Body)
		if err != nil {
			st.AddError("http")
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		body := fmt.Sprintf(`{"bytes":%d}`+"\n", n)
		writeHTTP(w, st, n, int64(len(body)), func() {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, body)
		})
	})
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		body, err := st.Snapshot().JSON()
		if err != nil {
			st.AddError("http")
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeHTTP(w, st, 0, int64(len(body)), func() {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(body)
		})
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		body := prometheusMetrics(st.Snapshot())
		writeHTTP(w, st, 0, int64(len(body)), func() {
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			_, _ = io.WriteString(w, body)
		})
	})

	server := &http.Server{
		Addr:              net.JoinHostPort(host, strconv.Itoa(port)),
		Handler:           loggingMiddleware(st, mux),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	return server.ListenAndServe()
}

func loggingMiddleware(st *stats.ServerStats, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		st.OpenConnection("http")
		defer st.CloseConnection("http")
		next.ServeHTTP(w, r)
	})
}

func writeHTTP(w http.ResponseWriter, st *stats.ServerStats, in, out int64, fn func()) {
	fn()
	st.AddRequest("http", in, out)
}

func prometheusMetrics(s stats.ServerSnapshot) string {
	out := ""
	out += "# HELP stress_server_total_connections Total accepted connections\n# TYPE stress_server_total_connections counter\n"
	out += fmt.Sprintf("stress_server_total_connections %d\n", s.TotalConnections)
	out += "# HELP stress_server_active_connections Current active connections\n# TYPE stress_server_active_connections gauge\n"
	out += fmt.Sprintf("stress_server_active_connections %d\n", s.ActiveConnections)
	out += "# HELP stress_server_bytes_received Bytes received\n# TYPE stress_server_bytes_received counter\n"
	out += fmt.Sprintf("stress_server_bytes_received %d\n", s.BytesReceived)
	out += "# HELP stress_server_bytes_sent Bytes sent\n# TYPE stress_server_bytes_sent counter\n"
	out += fmt.Sprintf("stress_server_bytes_sent %d\n", s.BytesSent)
	out += "# HELP stress_server_requests_per_second Requests per second\n# TYPE stress_server_requests_per_second gauge\n"
	out += fmt.Sprintf("stress_server_requests_per_second %.3f\n", s.RequestsPerSecond)
	out += "# HELP stress_server_errors Total errors\n# TYPE stress_server_errors counter\n"
	out += fmt.Sprintf("stress_server_errors %d\n", s.Errors)
	out += "# HELP stress_server_uptime_seconds Uptime seconds\n# TYPE stress_server_uptime_seconds gauge\n"
	out += fmt.Sprintf("stress_server_uptime_seconds %d\n", s.UptimeSeconds)
	for protocol, p := range s.PerProtocol {
		out += fmt.Sprintf("stress_server_protocol_requests{protocol=%q} %d\n", protocol, p.Requests)
		out += fmt.Sprintf("stress_server_protocol_bytes_received{protocol=%q} %d\n", protocol, p.BytesReceived)
		out += fmt.Sprintf("stress_server_protocol_bytes_sent{protocol=%q} %d\n", protocol, p.BytesSent)
		out += fmt.Sprintf("stress_server_protocol_errors{protocol=%q} %d\n", protocol, p.Errors)
	}
	return out
}

const dashboardHTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>StressAnchor Dashboard</title>
<style>
body{font-family:system-ui,-apple-system,Segoe UI,sans-serif;margin:0;background:#101418;color:#e9eef3}
main{max-width:980px;margin:32px auto;padding:0 20px}
h1{font-size:28px;margin:0 0 18px}
table{width:100%;border-collapse:collapse;background:#171d23;border:1px solid #2b333b}
th,td{padding:10px 12px;border-bottom:1px solid #2b333b;text-align:left}
th{color:#9fb0c0;font-size:12px;text-transform:uppercase}
.ok{color:#74d99f}.warn{color:#ffd166}
</style>
</head>
<body>
<main>
<h1>StressAnchor Server</h1>
<table id="metrics"><tbody></tbody></table>
</main>
<script>
const rows = [
["total_connections","Total connections"],["active_connections","Active connections"],
["bytes_received","Bytes received"],["bytes_sent","Bytes sent"],
["requests_per_second","Requests/sec"],["errors","Errors"],["uptime_seconds","Uptime seconds"]
];
async function refresh(){
  const r = await fetch('/stats', {cache:'no-store'});
  const s = await r.json();
  let html = '<tr><th>Metric</th><th>Value</th></tr>';
  rows.forEach(([k,l]) => html += `<tr><td>${l}</td><td class="${k==='errors'&&s[k]>0?'warn':'ok'}">${s[k]}</td></tr>`);
  Object.entries(s.per_protocol).forEach(([proto,v]) => {
    html += `<tr><th colspan="2">${proto.toUpperCase()}</th></tr>`;
    Object.entries(v).forEach(([k,val]) => html += `<tr><td>${k}</td><td>${val}</td></tr>`);
  });
  document.querySelector('#metrics tbody').innerHTML = html;
}
refresh(); setInterval(refresh, 1000);
</script>
</body>
</html>`
