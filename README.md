# StressAnchor

StressAnchor is a lightweight Go toolkit for stress testing site-to-site network paths, especially Cloudflare Mesh links between an OCI instance and an on-premise Orange Pi 5.

It provides two standalone binaries:

- `stress-server`: runs on the cloud side and exposes TCP, UDP, HTTP, dashboard, JSON stats, and Prometheus-compatible metrics.
- `stress-client`: runs on the on-premise side and continuously generates TCP, UDP, HTTP, or mixed traffic while measuring latency, throughput, errors, and active workers.

The project uses only the Go standard library and is designed for simple deployment on Linux without root privileges or external services.

## Build

```sh
make build-server
make build-client
```

Cross-compile for the target machines:

```sh
make cross-server  # linux/amd64 -> bin/stress-server-linux-amd64
make cross-client  # linux/arm64 -> bin/stress-client-linux-arm64
```

## Server

Run the server on the OCI host:

```sh
./bin/stress-server-linux-amd64 --host 0.0.0.0 --port-tcp 9000 --port-udp 9001 --port-http 8080
```

Server listeners and endpoints:

- TCP `:9000`: echo server
- UDP `:9001`: echo server
- HTTP `GET /ping`: returns `{"status":"ok","ts":<unix_ns>}`
- HTTP `POST /data`: accepts a request body and returns its byte size
- HTTP `GET /stats`: returns current server statistics as JSON
- HTTP `GET /metrics`: exports Prometheus-compatible text metrics
- HTTP `GET /`: serves an inline dashboard refreshed every second

## Client

Run the client on the Orange Pi 5 or another on-premise Linux host:

```sh
./bin/stress-client-linux-arm64 \
  --host 10.175.0.5 \
  --workers 50 \
  --duration 60 \
  --mode mixed \
  --payload-size 1024 \
  --ramp-up 5 \
  --report-interval 2
```

Traffic modes:

- `tcp`: TCP echo traffic to `:9000`
- `udp`: UDP echo traffic to `:9001`
- `http`: HTTP `POST /data` traffic to `:8080`
- `mixed`: distributes workers as 40% TCP, 30% UDP, and 30% HTTP

The client updates a terminal table in place, handles `SIGINT` and `SIGTERM`, prints a final summary, and writes:

- `stress_report_<timestamp>.json`
- `stress_report_<timestamp>.csv`

The CSV file contains one row per reporting interval.

## Metrics

The client reports:

- Latency percentiles: p50, p95, p99, min, and max
- Latency histogram buckets
- Requests per second
- Sent and received MB/s
- Total errors and error rate
- Active workers
- Elapsed and remaining time

The server tracks:

- Total and active connections
- Bytes received and sent
- Requests per second
- Error count
- Uptime
- Per-protocol counters for TCP, UDP, and HTTP

## Development

```sh
make fmt
make test
```
