.PHONY: all build test clean run-server run-worker run-example lint docker-build docker-up docker-down

# Binary names
BINARY_SERVER=dtq-server
BINARY_WORKER=dtq-worker
BINARY_EXAMPLE=dtq-example

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod

# Build directory
BUILD_DIR=bin

all: build

# Download dependencies
deps:
	$(GOMOD) download
	$(GOMOD) tidy

# Build all binaries
build: deps
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) -o $(BUILD_DIR)/$(BINARY_SERVER) ./cmd/server
	$(GOBUILD) -o $(BUILD_DIR)/$(BINARY_WORKER) ./cmd/worker
	$(GOBUILD) -o $(BUILD_DIR)/$(BINARY_EXAMPLE) ./cmd/example

# Build individual binaries
build-server: deps
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) -o $(BUILD_DIR)/$(BINARY_SERVER) ./cmd/server

build-worker: deps
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) -o $(BUILD_DIR)/$(BINARY_WORKER) ./cmd/worker

build-example: deps
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) -o $(BUILD_DIR)/$(BINARY_EXAMPLE) ./cmd/example

# Run applications
run-server:
	$(GOCMD) run ./cmd/server

run-worker:
	$(GOCMD) run ./cmd/worker

run-example:
	$(GOCMD) run ./cmd/example

# Run tests
test:
	$(GOTEST) -v ./...

test-short:
	$(GOTEST) -v -short ./...

test-coverage:
	$(GOTEST) -v -coverprofile=coverage.out ./...
	$(GOCMD) tool cover -html=coverage.out -o coverage.html

# Lint
lint:
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run ./...; \
	else \
		echo "golangci-lint not installed. Run: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest"; \
	fi

# Docker
docker-build:
	docker build -t distributed-task-queue:latest .

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down

docker-logs:
	docker-compose logs -f

# Start Redis locally (for development)
redis-start:
	docker run -d --name dtq-redis -p 6379:6379 redis:7-alpine

redis-stop:
	docker stop dtq-redis && docker rm dtq-redis

# Clean
clean:
	rm -rf $(BUILD_DIR)
	rm -f coverage.out coverage.html

# Install development tools
dev-tools:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

# Demo: Run a complete demonstration
demo: build
	@echo "=== Starting Demo ==="
	@echo ""
	@echo "1. Make sure Redis is running on localhost:6379"
	@echo "   Run 'make redis-start' if needed"
	@echo ""
	@echo "2. In one terminal, start the worker:"
	@echo "   make run-worker"
	@echo ""
	@echo "3. In another terminal, run the example producer:"
	@echo "   make run-example"
	@echo ""
	@echo "4. Optionally, start the HTTP server:"
	@echo "   make run-server"
	@echo ""

# Help
help:
	@echo "Available targets:"
	@echo "  make build        - Build all binaries"
	@echo "  make test         - Run all tests"
	@echo "  make run-server   - Run the HTTP API server"
	@echo "  make run-worker   - Run the worker pool"
	@echo "  make run-example  - Run the example producer"
	@echo "  make docker-up    - Start services with docker-compose"
	@echo "  make docker-down  - Stop docker-compose services"
	@echo "  make redis-start  - Start Redis container for development"
	@echo "  make redis-stop   - Stop Redis container"
	@echo "  make clean        - Remove build artifacts"
	@echo "  make help         - Show this help message"
