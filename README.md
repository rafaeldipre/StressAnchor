# StressAnchor

StressAnchor is a Go network stress testing tool for validating a Cloudflare Mesh site-to-site path between an OCI server and an on-premise Orange Pi 5.

It builds two independent binaries:

- `stress-server`: runs on OCI and exposes TCP, UDP, HTTP, dashboard, stats, and Prometheus-compatible metrics.
- `stress-client`: runs on the Orange Pi 5 and drives TCP, UDP, HTTP, or mixed traffic while collecting latency and throughput reports.

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

```sh
./bin/stress-server-linux-amd64 --host 0.0.0.0 --port-tcp 9000 --port-udp 9001 --port-http 8080
```

Server endpoints and listeners:

- TCP `:9000`: echo server
- UDP `:9001`: echo server
- HTTP `GET /ping`: returns `{"status":"ok","ts":<unix_ns>}`
- HTTP `POST /data`: accepts a request body and returns its byte size
- HTTP `GET /stats`: JSON statistics
- HTTP `GET /metrics`: Prometheus text metrics
- HTTP `GET /`: inline dashboard refreshed every second

## Client

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

Modes:

- `tcp`: TCP echo traffic to `:9000`
- `udp`: UDP echo traffic to `:9001`
- `http`: HTTP `POST /data` traffic to `:8080`
- `mixed`: distributes workers as 40% TCP, 30% UDP, 30% HTTP

The client updates a terminal table in place, handles `SIGINT` and `SIGTERM`, prints a final summary, and always writes:

- `stress_report_<timestamp>.json`
- `stress_report_<timestamp>.csv`

The CSV contains one row per report interval.

## Development

```sh
make fmt
make test
```
