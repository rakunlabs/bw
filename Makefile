.DEFAULT_GOAL := help

.PHONY: test
test: ## Run unit tests
	@go test -v -count=1 ./...

.PHONY: bench
bench: ## # bench runs the full benchmark suite. Tune size with BW_BENCH_N.
	go test -bench=. -benchmem -benchtime=1s -run=^$$ .

.PHONY: vet
vet: ## Run go vet on all packages
	go vet ./...

.PHONY: check
check: vet test ## Run all checks (vet + test)

.PHONY: help
help: ## Display this help screen
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
