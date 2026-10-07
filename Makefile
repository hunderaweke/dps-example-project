# dps-audit-service Makefile. Run `make` or `make help` to list targets.
SHELL := /bin/bash
.DEFAULT_GOAL := help

MODULE      := $(shell go list -m)
DB_URL      ?= postgres://postgres:postgres@localhost:5432/audit?sslmode=disable
MIGRATIONS  := internal/const/migrations
BENCH_COUNT ?= 6
BENCH_PKGS  ?= ./internal/...
PKG         ?= ./internal/module
SQLC_IMAGE  := sqlc/sqlc:1.30.0
MIGRATE     := GOFLAGS=-tags=pgx5 go tool migrate

##@ Help
.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage: make \033[36m<target>\033[0m\n"} \
		/^[a-zA-Z0-9_-]+:.*?##/ { printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2 } \
		/^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) }' $(MAKEFILE_LIST)

##@ Setup
.PHONY: tools rename
tools: ## Download modules and build all pinned dev tools (go tool)
	go mod download
	go build -o /dev/null tool

rename: ## Rename the Go module for a new service: make rename MODULE_NEW=github.com/org/svc
	@test -n "$(MODULE_NEW)" || (echo "usage: make rename MODULE_NEW=github.com/org/new-service" && exit 1)
	go mod edit -module $(MODULE_NEW)
	grep -rl --exclude-dir=.git --exclude-dir=bin '$(MODULE)' . | xargs perl -pi -e 's#\Q$(MODULE)\E#$(MODULE_NEW)#g'
	go mod tidy
	@echo "module renamed to $(MODULE_NEW)"

##@ Run
.PHONY: run-api run-worker dev-up dev-down up down logs
run-api: ## Run the API on the host (needs `make dev-up`)
	go run ./cmd/api

run-worker: ## Run the worker on the host (needs `make dev-up`)
	go run ./cmd/worker

dev-up: ## Start local infrastructure (compose.dev.yml)
	docker compose -f compose.dev.yml up -d --wait

dev-down: ## Stop local infrastructure and remove volumes
	docker compose -f compose.dev.yml down -v

up: ## Build and start the full stack (compose.yml)
	docker compose up -d --build --wait

down: ## Stop the full stack and remove volumes
	docker compose down -v

logs: ## Tail api and worker logs
	docker compose logs -f api worker

##@ Codegen
.PHONY: generate sqlc proto mocks openapi
generate: sqlc proto mocks openapi ## Run all code generators

sqlc: ## Generate type-safe query code from internal/const/queries (docker)
	docker run --rm -v "$(CURDIR)":/src -w /src $(SQLC_IMAGE) generate

proto: ## Generate Go code for the dps-contracts envelope into pkg/dpsapi/gen (buf)
	go tool buf generate

mocks: ## Generate testify mocks for module ports (mockery)
	go tool mockery

openapi: ## Write the OpenAPI spec to docs/openapi.yaml
	@mkdir -p docs
	go run ./cmd/api -openapi > docs/openapi.yaml

##@ Database
.PHONY: migrate-new migrate-up migrate-down migrate-force
migrate-new: ## Create a migration pair: make migrate-new NAME=add_users
	@test -n "$(NAME)" || (echo "usage: make migrate-new NAME=<name>" && exit 1)
	$(MIGRATE) create -ext sql -dir $(MIGRATIONS) -seq $(NAME)

migrate-up: ## Apply all up migrations to DB_URL
	$(MIGRATE) -path $(MIGRATIONS) -database "$(subst postgres://,pgx5://,$(DB_URL))" up

migrate-down: ## Roll back the last migration on DB_URL
	$(MIGRATE) -path $(MIGRATIONS) -database "$(subst postgres://,pgx5://,$(DB_URL))" down 1

migrate-force: ## Force the schema version after a failed migration: make migrate-force V=1
	@test -n "$(V)" || (echo "usage: make migrate-force V=<version>" && exit 1)
	$(MIGRATE) -path $(MIGRATIONS) -database "$(subst postgres://,pgx5://,$(DB_URL))" force $(V)

##@ Quality
.PHONY: fmt vet lint test test-e2e test-integration test-all cover
fmt: ## Format code (gofmt + goimports via golangci-lint)
	go tool golangci-lint fmt

vet: ## Run go vet
	go vet ./...

lint: ## Run golangci-lint (includes architecture import rules)
	go tool golangci-lint run

test: ## Unit tests (no docker)
	go test -short -race ./...

test-e2e: ## Godog e2e features against testcontainers (needs docker)
	go test -count=1 -v ./tests/e2e/...

test-integration: ## Audit consumer against Redpanda + Postgres in testcontainers (needs docker)
	go test -count=1 -timeout 15m -v ./tests/integration

test-all: ## Unit, integration and e2e tests (needs docker)
	go test -race -count=1 ./...

cover: ## Unit test coverage report (opens browser)
	go test -short -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out

##@ Benchmarks
.PHONY: bench bench-baseline bench-compare bench-profile bench-storage bench-check bench-check-all bench-integration
bench: ## Run benchmarks into bench/current.txt (BENCH_PKGS, BENCH_COUNT)
	@mkdir -p bench
	go test -run='^$$' -bench=. -benchmem -count=$(BENCH_COUNT) $(BENCH_PKGS) | tee bench/current.txt

bench-baseline: bench ## Save the current run as the committed baseline
	cp bench/current.txt bench/baseline.txt

bench-compare: bench ## Compare a fresh run against bench/baseline.txt (benchstat)
	@test -f bench/baseline.txt || (echo "no baseline: run make bench-baseline first" && exit 1)
	go tool benchstat bench/baseline.txt bench/current.txt

bench-profile: ## CPU+memory profile one package and open pprof: make bench-profile PKG=./internal/router
	@mkdir -p bench
	go test -run='^$$' -bench=. -benchmem -cpuprofile=bench/cpu.out -memprofile=bench/mem.out -o bench/pkg.test $(PKG)
	go tool pprof -http=:0 bench/pkg.test bench/cpu.out

bench-check: ## Fail if any benchmark budget is broken (no docker)
	PERF_BUDGETS=1 go test -short -run=Budget -count=1 ./internal/...

bench-check-all: ## Fail if any budget is broken, incl. Postgres and Redpanda (docker)
	PERF_BUDGETS=1 go test -run=Budget -count=1 -timeout 30m ./internal/... ./tests/integration

bench-integration: ## End-to-end Redpanda -> Postgres throughput benchmarks (docker; BENCH_COUNT)
	@mkdir -p bench
	go test -run='^$$' -bench=. -benchmem -count=$(BENCH_COUNT) -timeout 30m ./tests/integration | tee bench/integration.txt

bench-storage: ## Audit storage benchmark, YSQL + S3 Object Lock + OpenSearch (docker; SB_PROFILE=quick|full)
	STORAGEBENCH=1 SB_PROFILE=$(or $(SB_PROFILE),quick) go test ./tests/storagebench -run TestStorageBench -count=1 -timeout 4h -v


##@ Build
.PHONY: build docker-build clean
build: ## Build api and worker binaries into bin/
	CGO_ENABLED=0 go build -trimpath -o bin/api ./cmd/api
	CGO_ENABLED=0 go build -trimpath -o bin/worker ./cmd/worker

docker-build: ## Build api and worker images
	docker build --target api -t $(notdir $(MODULE))-api .
	docker build --target worker -t $(notdir $(MODULE))-worker .

clean: ## Remove build, profile and coverage output
	rm -rf bin coverage.out bench/current.txt bench/*.out bench/*.test
