.PHONY: build test lint clean run-sim

build:
	@echo "Building cvmfs-prepub and cvmfs-prepubctl..."
	go build -v ./cmd/prepub
	go build -v ./cmd/prepubctl

test:
	@echo "Running tests..."
	go test -v -race -cover ./...

lint:
	@echo "Running linters..."
	go fmt ./...
	go vet ./...

clean:
	@echo "Cleaning..."
	rm -f prepub prepubctl
	go clean -testcache ./...

run-sim:
	@echo "Running cluster simulator integration test..."
	go test -v -run TestCluster ./testutil/simulate/...

all: clean lint test build
