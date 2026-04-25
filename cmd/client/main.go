package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"mesh-stress-test/internal/proto"
)

type config struct {
	Host           string `json:"host"`
	Workers        int    `json:"workers"`
	DurationSec    int    `json:"duration_seconds"`
	Mode           string `json:"mode"`
	PayloadSize    int    `json:"payload_size"`
	RampUpSec      int    `json:"ramp_up_seconds"`
	ReportInterval int    `json:"report_interval_seconds"`
}

type sample struct {
	Mode      string
	Latency   time.Duration
	SentBytes int64
	RecvBytes int64
	Err       error
	At        time.Time
}

type intervalReport struct {
	ElapsedSeconds   float64 `json:"elapsed_seconds"`
	RemainingSeconds float64 `json:"remaining_seconds"`
	Requests         int64   `json:"requests"`
	Errors           int64   `json:"errors"`
	ErrorPercent     float64 `json:"error_percent"`
	RequestsPerSec   float64 `json:"requests_per_second"`
	SentMBPerSec      float64 `json:"sent_mb_per_second"`
	RecvMBPerSec      float64 `json:"recv_mb_per_second"`
	ActiveConnections int64   `json:"active_connections"`
	P50MS             float64 `json:"p50_ms"`
	P95MS             float64 `json:"p95_ms"`
	P99MS             float64 `json:"p99_ms"`
	MinMS             float64 `json:"min_ms"`
	MaxMS             float64 `json:"max_ms"`
	Buckets           buckets `json:"latency_buckets"`
}

type finalReport struct {
	Config    config           `json:"config"`
	StartedAt time.Time        `json:"started_at"`
	EndedAt   time.Time        `json:"ended_at"`
	Summary   intervalReport   `json:"summary"`
	Intervals []intervalReport `json:"intervals"`
}

type buckets struct {
	LT10MS     int64 `json:"lt_10_ms"`
	MS10To50   int64 `json:"10_to_50_ms"`
	MS50To100  int64 `json:"50_to_100_ms"`
	MS100To500 int64 `json:"100_to_500_ms"`
	GE500MS    int64 `json:"ge_500_ms"`
}

var activeConnections int64

func main() {
	cfg := parseFlags()
	if cfg.Host == "" {
		fmt.Fprintln(os.Stderr, "--host is required")
		os.Exit(2)
	}

	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(signalCtx, time.Duration(cfg.DurationSec)*time.Second)
	defer cancel()

	payload := proto.Payload(cfg.PayloadSize)
	results := make(chan sample, cfg.Workers*4)
	startedAt := time.Now()

	var wg sync.WaitGroup
	go func() {
		launchWorkers(ctx, cfg, payload, results, &wg)
		wg.Wait()
		close(results)
	}()

	report := collect(cfg, startedAt, results)
	if err := saveReports(report); err != nil {
		fmt.Fprintf(os.Stderr, "failed to save report: %v\n", err)
		os.Exit(1)
	}
	printFinal(report)
}

func parseFlags() config {
	cfg := config{}
	flag.StringVar(&cfg.Host, "host", "", "server IP/host")
	flag.IntVar(&cfg.Workers, "workers", 50, "concurrent workers")
	flag.IntVar(&cfg.DurationSec, "duration", 60, "test duration in seconds")
	flag.StringVar(&cfg.Mode, "mode", "mixed", "tcp | udp | http | mixed")
	flag.IntVar(&cfg.PayloadSize, "payload-size", 1024, "payload size per request")
	flag.IntVar(&cfg.RampUpSec, "ramp-up", 5, "seconds to reach all workers")
	flag.IntVar(&cfg.ReportInterval, "report-interval", 2, "report interval seconds")
	flag.Parse()
	if cfg.Workers < 1 {
		cfg.Workers = 1
	}
	if cfg.DurationSec < 1 {
		cfg.DurationSec = 1
	}
	if cfg.PayloadSize < 1 {
		cfg.PayloadSize = 1
	}
	if cfg.ReportInterval < 1 {
		cfg.ReportInterval = 1
	}
	if cfg.RampUpSec < 0 {
		cfg.RampUpSec = 0
	}
	switch cfg.Mode {
	case "tcp", "udp", "http", "mixed":
	default:
		fmt.Fprintln(os.Stderr, "--mode must be tcp, udp, http, or mixed")
		os.Exit(2)
	}
	return cfg
}

func launchWorkers(ctx context.Context, cfg config, payload []byte, results chan<- sample, wg *sync.WaitGroup) {
	delay := time.Duration(0)
	if cfg.RampUpSec > 0 && cfg.Workers > 1 {
		delay = time.Duration(cfg.RampUpSec) * time.Second / time.Duration(cfg.Workers-1)
	}
	for i := 0; i < cfg.Workers; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}
		mode := workerMode(cfg.Mode, i, cfg.Workers)
		wg.Add(1)
		go func() {
			defer wg.Done()
			atomic.AddInt64(&activeConnections, 1)
			defer atomic.AddInt64(&activeConnections, -1)
			runWorker(ctx, cfg.Host, mode, payload, results)
		}()
		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}
}

func workerMode(mode string, idx, total int) string {
	if mode != "mixed" {
		return mode
	}
	tcpN := int(math.Round(float64(total) * 0.40))
	udpN := int(math.Round(float64(total) * 0.30))
	if idx < tcpN {
		return "tcp"
	}
	if idx < tcpN+udpN {
		return "udp"
	}
	return "http"
}

func runWorker(ctx context.Context, host, mode string, payload []byte, results chan<- sample) {
	switch mode {
	case "tcp":
		runTCPWorker(ctx, host, payload, results)
	case "udp":
		runUDPWorker(ctx, host, payload, results)
	case "http":
		runHTTPWorker(ctx, host, payload, results)
	}
}

func runTCPWorker(ctx context.Context, host string, payload []byte, results chan<- sample) {
	addr := net.JoinHostPort(host, "9000")
	dialer := net.Dialer{Timeout: 5 * time.Second}
	for ctx.Err() == nil {
		conn, err := dialer.DialContext(ctx, "tcp", addr)
		if err != nil {
			sendSample(ctx, results, sample{Mode: "tcp", Err: err, At: time.Now()})
			time.Sleep(200 * time.Millisecond)
			continue
		}
		for ctx.Err() == nil {
			start := time.Now()
			_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
			_, writeErr := conn.Write(payload)
			if writeErr != nil {
				_ = conn.Close()
				sendSample(ctx, results, sample{Mode: "tcp", Err: writeErr, At: time.Now()})
				break
			}
			buf := make([]byte, len(payload))
			_, readErr := io.ReadFull(conn, buf)
			latency := time.Since(start)
			if readErr != nil || !proto.Equal(payload, buf) {
				_ = conn.Close()
				if readErr == nil {
					readErr = errors.New("tcp echo mismatch")
				}
				sendSample(ctx, results, sample{Mode: "tcp", Latency: latency, SentBytes: int64(len(payload)), Err: readErr, At: time.Now()})
				break
			}
			sendSample(ctx, results, sample{Mode: "tcp", Latency: latency, SentBytes: int64(len(payload)), RecvBytes: int64(len(buf)), At: time.Now()})
		}
		_ = conn.Close()
	}
}

func runUDPWorker(ctx context.Context, host string, payload []byte, results chan<- sample) {
	addr := net.JoinHostPort(host, "9001")
	conn, err := net.Dial("udp", addr)
	if err != nil {
		sendSample(ctx, results, sample{Mode: "udp", Err: err, At: time.Now()})
		return
	}
	defer conn.Close()
	buf := make([]byte, len(payload))
	for ctx.Err() == nil {
		start := time.Now()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		_, writeErr := conn.Write(payload)
		if writeErr != nil {
			sendSample(ctx, results, sample{Mode: "udp", Err: writeErr, At: time.Now()})
			continue
		}
		n, readErr := conn.Read(buf)
		latency := time.Since(start)
		if readErr != nil || !proto.Equal(payload, buf[:n]) {
			if readErr == nil {
				readErr = errors.New("udp echo mismatch")
			}
			sendSample(ctx, results, sample{Mode: "udp", Latency: latency, SentBytes: int64(len(payload)), Err: readErr, At: time.Now()})
			continue
		}
		sendSample(ctx, results, sample{Mode: "udp", Latency: latency, SentBytes: int64(len(payload)), RecvBytes: int64(n), At: time.Now()})
	}
}

func runHTTPWorker(ctx context.Context, host string, payload []byte, results chan<- sample) {
	url := "http://" + net.JoinHostPort(host, "8080") + "/data"
	client := &http.Client{Timeout: 10 * time.Second}
	for ctx.Err() == nil {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
		if err != nil {
			sendSample(ctx, results, sample{Mode: "http", Err: err, At: time.Now()})
			continue
		}
		start := time.Now()
		resp, err := client.Do(req)
		latency := time.Since(start)
		if err != nil {
			sendSample(ctx, results, sample{Mode: "http", Latency: latency, SentBytes: int64(len(payload)), Err: err, At: time.Now()})
			continue
		}
		body, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil || resp.StatusCode >= 400 {
			if readErr == nil {
				readErr = fmt.Errorf("http status %d", resp.StatusCode)
			}
			sendSample(ctx, results, sample{Mode: "http", Latency: latency, SentBytes: int64(len(payload)), RecvBytes: int64(len(body)), Err: readErr, At: time.Now()})
			continue
		}
		sendSample(ctx, results, sample{Mode: "http", Latency: latency, SentBytes: int64(len(payload)), RecvBytes: int64(len(body)), At: time.Now()})
	}
}

func sendSample(ctx context.Context, results chan<- sample, s sample) {
	select {
	case results <- s:
	case <-ctx.Done():
	}
}

func collect(cfg config, startedAt time.Time, results <-chan sample) finalReport {
	ticker := time.NewTicker(time.Duration(cfg.ReportInterval) * time.Second)
	defer ticker.Stop()

	var allLatencies, intervalLatencies []time.Duration
	var totalReq, totalErr, totalSent, totalRecv int64
	var intervalReq, intervalErr, intervalSent, intervalRecv int64
	var intervals []intervalReport
	lastReport := startedAt

	for {
		select {
		case s, ok := <-results:
			if !ok {
				if intervalReq > 0 || intervalErr > 0 {
					intervals = append(intervals, makeInterval(cfg, startedAt, time.Now(), time.Since(lastReport), intervalLatencies, intervalReq, intervalErr, intervalSent, intervalRecv))
				}
				report := buildReport(cfg, startedAt, time.Now(), allLatencies, totalReq, totalErr, totalSent, totalRecv, intervals)
				fmt.Print("\033[H\033[2J")
				return report
			}
			totalReq++
			intervalReq++
			if s.Err != nil {
				totalErr++
				intervalErr++
			} else {
				allLatencies = append(allLatencies, s.Latency)
				intervalLatencies = append(intervalLatencies, s.Latency)
			}
			totalSent += s.SentBytes
			totalRecv += s.RecvBytes
			intervalSent += s.SentBytes
			intervalRecv += s.RecvBytes
		case now := <-ticker.C:
			interval := makeInterval(cfg, startedAt, now, now.Sub(lastReport), intervalLatencies, intervalReq, intervalErr, intervalSent, intervalRecv)
			intervals = append(intervals, interval)
			printInterval(interval, cfg)
			intervalLatencies = nil
			intervalReq, intervalErr, intervalSent, intervalRecv = 0, 0, 0, 0
			lastReport = now
		}
	}
}

func buildReport(cfg config, startedAt, endedAt time.Time, latencies []time.Duration, req, errs, sent, recv int64, intervals []intervalReport) finalReport {
	summary := makeInterval(cfg, startedAt, endedAt, endedAt.Sub(startedAt), latencies, req, errs, sent, recv)
	return finalReport{Config: cfg, StartedAt: startedAt, EndedAt: endedAt, Summary: summary, Intervals: intervals}
}

func makeInterval(cfg config, startedAt, now time.Time, window time.Duration, latencies []time.Duration, req, errs, sent, recv int64) intervalReport {
	if window <= 0 {
		window = time.Second
	}
	elapsed := now.Sub(startedAt).Seconds()
	remaining := math.Max(0, float64(cfg.DurationSec)-elapsed)
	stats := latencyStats(latencies)
	errPct := 0.0
	if req > 0 {
		errPct = float64(errs) * 100 / float64(req)
	}
	return intervalReport{
		ElapsedSeconds:   elapsed,
		RemainingSeconds: remaining,
		Requests:         req,
		Errors:           errs,
		ErrorPercent:     errPct,
		RequestsPerSec:   float64(req) / window.Seconds(),
		SentMBPerSec:     float64(sent) / 1024 / 1024 / window.Seconds(),
		RecvMBPerSec:     float64(recv) / 1024 / 1024 / window.Seconds(),
		ActiveConnections: atomic.LoadInt64(&activeConnections),
		P50MS:            stats.p50,
		P95MS:            stats.p95,
		P99MS:            stats.p99,
		MinMS:            stats.min,
		MaxMS:            stats.max,
		Buckets:          makeBuckets(latencies),
	}
}

type latencySummary struct {
	p50, p95, p99, min, max float64
}

func latencyStats(in []time.Duration) latencySummary {
	if len(in) == 0 {
		return latencySummary{}
	}
	values := append([]time.Duration(nil), in...)
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return latencySummary{
		p50: percentile(values, 0.50),
		p95: percentile(values, 0.95),
		p99: percentile(values, 0.99),
		min: durationMS(values[0]),
		max: durationMS(values[len(values)-1]),
	}
}

func percentile(values []time.Duration, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	idx := int(math.Ceil(p*float64(len(values)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(values) {
		idx = len(values) - 1
	}
	return durationMS(values[idx])
}

func durationMS(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}

func makeBuckets(latencies []time.Duration) buckets {
	var b buckets
	for _, d := range latencies {
		ms := durationMS(d)
		switch {
		case ms < 10:
			b.LT10MS++
		case ms < 50:
			b.MS10To50++
		case ms < 100:
			b.MS50To100++
		case ms < 500:
			b.MS100To500++
		default:
			b.GE500MS++
		}
	}
	return b
}

func printInterval(r intervalReport, cfg config) {
	color := "\033[32m"
	if r.P95MS > 500 || r.Errors > 0 {
		color = "\033[31m"
	} else if r.P95MS > 100 {
		color = "\033[33m"
	}
	reset := "\033[0m"
	fmt.Print("\033[H\033[2J")
	fmt.Printf("StressAnchor client -> %s | mode=%s workers=%d payload=%dB\n\n", cfg.Host, cfg.Mode, cfg.Workers, cfg.PayloadSize)
	fmt.Printf("%-12s %-12s %-12s %-12s %-12s %-12s\n", "elapsed", "remaining", "active", "req/s", "sent MB/s", "recv MB/s")
	fmt.Printf("%-12.1f %-12.1f %-12d %-12.2f %-12.2f %-12.2f\n\n", r.ElapsedSeconds, r.RemainingSeconds, r.ActiveConnections, r.RequestsPerSec, r.SentMBPerSec, r.RecvMBPerSec)
	fmt.Printf("%-10s %-10s %-10s %-10s %-10s %-10s %-10s\n", "p50", "p95", "p99", "min", "max", "errors", "err%")
	fmt.Printf("%s%-10.2f %-10.2f %-10.2f %-10.2f %-10.2f %-10d %-10.2f%s\n\n", color, r.P50MS, r.P95MS, r.P99MS, r.MinMS, r.MaxMS, r.Errors, r.ErrorPercent, reset)
	fmt.Printf("Latency buckets: <10ms=%d 10-50ms=%d 50-100ms=%d 100-500ms=%d >=500ms=%d\n",
		r.Buckets.LT10MS, r.Buckets.MS10To50, r.Buckets.MS50To100, r.Buckets.MS100To500, r.Buckets.GE500MS)
}

func printFinal(report finalReport) {
	s := report.Summary
	fmt.Println("StressAnchor summary")
	fmt.Printf("Duration: %.1fs | Requests: %d | Errors: %d (%.2f%%)\n", s.ElapsedSeconds, s.Requests, s.Errors, s.ErrorPercent)
	fmt.Printf("Throughput: %.2f req/s | sent %.2f MB/s | recv %.2f MB/s\n", s.RequestsPerSec, s.SentMBPerSec, s.RecvMBPerSec)
	fmt.Printf("Latency ms: p50 %.2f | p95 %.2f | p99 %.2f | min %.2f | max %.2f\n", s.P50MS, s.P95MS, s.P99MS, s.MinMS, s.MaxMS)
	fmt.Println("Reports written: stress_report_<timestamp>.json and stress_report_<timestamp>.csv")
}

func saveReports(report finalReport) error {
	ts := time.Now().Format("20060102_150405")
	jsonName := "stress_report_" + ts + ".json"
	csvName := "stress_report_" + ts + ".csv"

	jsonFile, err := os.Create(jsonName)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(jsonFile)
	enc.SetIndent("", "  ")
	if err := enc.Encode(report); err != nil {
		_ = jsonFile.Close()
		return err
	}
	if err := jsonFile.Close(); err != nil {
		return err
	}

	csvFile, err := os.Create(csvName)
	if err != nil {
		return err
	}
	w := csv.NewWriter(csvFile)
	header := []string{"elapsed_seconds", "remaining_seconds", "requests", "errors", "error_percent", "requests_per_second", "sent_mb_per_second", "recv_mb_per_second", "active_connections", "p50_ms", "p95_ms", "p99_ms", "min_ms", "max_ms", "lt_10_ms", "10_to_50_ms", "50_to_100_ms", "100_to_500_ms", "ge_500_ms"}
	if err := w.Write(header); err != nil {
		_ = csvFile.Close()
		return err
	}
	for _, row := range report.Intervals {
		if err := w.Write(csvRow(row)); err != nil {
			_ = csvFile.Close()
			return err
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		_ = csvFile.Close()
		return err
	}
	return csvFile.Close()
}

func csvRow(r intervalReport) []string {
	return []string{
		f(r.ElapsedSeconds), f(r.RemainingSeconds), strconv.FormatInt(r.Requests, 10),
		strconv.FormatInt(r.Errors, 10), f(r.ErrorPercent), f(r.RequestsPerSec), f(r.SentMBPerSec),
		f(r.RecvMBPerSec), strconv.FormatInt(r.ActiveConnections, 10), f(r.P50MS), f(r.P95MS),
		f(r.P99MS), f(r.MinMS), f(r.MaxMS), strconv.FormatInt(r.Buckets.LT10MS, 10),
		strconv.FormatInt(r.Buckets.MS10To50, 10), strconv.FormatInt(r.Buckets.MS50To100, 10),
		strconv.FormatInt(r.Buckets.MS100To500, 10), strconv.FormatInt(r.Buckets.GE500MS, 10),
	}
}

func f(v float64) string {
	return strconv.FormatFloat(v, 'f', 4, 64)
}
