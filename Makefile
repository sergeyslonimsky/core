-include .local
export

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: gci
gci: ## Format code with gofumpt and gci
	@gofumpt -w -extra .
	@gci write --skip-vendor --skip-generated -s standard -s default -s "prefix(gitlab.com/golib3/core)" --custom-order .

.PHONY: lint
lint: ## Run golangci-lint
	@golangci-lint run

.PHONY: test
test: ## Run unit tests with race detector (excludes integration tests)
	@echo "Running unit tests..."
	@go test -race -count=1 -timeout=2m -short -v ./...

.PHONY: test-integration
test-integration: ## Run integration tests (requires Docker)
	@echo "Running integration tests..."
	@go test -race -count=1 -timeout=15m -tags=integration -v ./...

.PHONY: test-all
test-all: ## Run all tests (unit + integration)
	@echo "Running all tests..."
	@go test -race -count=1 -timeout=15m -tags=integration -v ./...

.PHONY: test-coverage
test-coverage: ## Run unit tests with coverage report
	@echo "Running unit tests with coverage..."
	@go test -race -count=1 -timeout=2m -short -coverprofile=coverage.out -covermode=atomic ./...
	@go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report saved to coverage.html"

.PHONY: bench
bench: ## Run benchmark tests
	@echo "Running benchmarks..."
	@go test -run=^$$ -bench=. -benchmem -timeout=10m ./...

.PHONY: test-quick
test-quick: ## Run tests quickly without race detector (for rapid development)
	@echo "Running quick tests..."
	@go test -count=1 -timeout=2m -short ./...

.PHONY: clean
clean: ## Clean test cache and coverage files
	@echo "Cleaning test cache and coverage files..."
	@go clean -testcache
	@rm -f coverage.out coverage.html
	@echo "Done"
