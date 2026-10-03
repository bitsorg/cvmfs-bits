.PHONY: build test lint clean run-sim

# Version stamped into the binary (cvmfs-prepub --version); empty outside git,
# where the binary falls back to the go tool's VCS stamp or "dev".
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null)
LDFLAGS := -X main.version=$(VERSION)

build:
	@echo "Building cvmfs-prepub..."
	@mkdir -p bin
	go build -v -ldflags "$(LDFLAGS)" -o bin/cvmfs-prepub  ./cmd/prepub

test:
	@echo "Running tests..."
	go test -v -race -cover ./...

lint:
	@echo "Running linters..."
	go fmt ./...
	go vet ./...

clean:
	@echo "Cleaning..."
	rm -rf bin/
	go clean -testcache ./...

run-sim:
	@echo "Running cluster simulator integration test..."
	go test -v -run TestCluster ./testutil/simulate/...

all: clean lint test build
