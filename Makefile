.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: format
format: ## Format code with golangci-lint
	@golangci-lint fmt

.PHONY: lint
lint: ## Run golangci-lint
	@golangci-lint run

.PHONY: test
test: ## Run unit tests with race detector (excludes integration tests)
	@go test -race -count=1 -timeout=2m -short -v ./...

.PHONY: test-integration
test-integration: ## Run integration tests (requires Docker)
	@cd internal/integration && go test -race -count=1 -timeout=15m -v ./...

.PHONY: test-all
test-all: test test-integration ## Run all tests (unit + integration)
