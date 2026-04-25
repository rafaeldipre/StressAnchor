BINARY_SERVER=stress-server
BINARY_CLIENT=stress-client
BIN_DIR=bin

.PHONY: all build build-server build-client cross-server cross-client clean test fmt

all: build

build: build-server build-client

build-server:
	mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/$(BINARY_SERVER) ./cmd/server

build-client:
	mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/$(BINARY_CLIENT) ./cmd/client

cross-server:
	mkdir -p $(BIN_DIR)
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o $(BIN_DIR)/stress-server-linux-amd64 ./cmd/server

cross-client:
	mkdir -p $(BIN_DIR)
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o $(BIN_DIR)/stress-client-linux-arm64 ./cmd/client

test:
	go test ./...

fmt:
	go fmt ./...

clean:
	rm -rf $(BIN_DIR)
