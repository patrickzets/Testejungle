.PHONY: up down deps migrate-up migrate-down test test-race test-race-local vet fmt test-integration test-e2e

DB_URL ?= postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable
COMPOSE_DB_URL ?= postgres://wagering:wagering@postgres:5432/wagering?sslmode=disable
GO_IMAGE ?= golang:1.27

up:
	docker compose up -d --build

down:
	docker compose down

deps:
	docker compose up -d postgres keycloak localstack
	docker compose run --rm migrate -path=/migrations -database="$(COMPOSE_DB_URL)" up

migrate-up:
	docker compose run --rm migrate -path=/migrations -database="$(COMPOSE_DB_URL)" up

migrate-down:
	docker compose run --rm migrate -path=/migrations -database="$(COMPOSE_DB_URL)" down 1

fmt:
	gofmt -l -w .

vet:
	go vet ./...

test:
	go test ./...

# The race detector needs cgo and a C compiler, so the default target runs inside a Go container.
test-race:
	docker run --rm -v "$(CURDIR):/src" -w /src $(GO_IMAGE) go test -race ./...

test-race-local:
	CGO_ENABLED=1 go test -race ./...

# Requires `make deps` and the variables from .env.example exported in the shell.
test-integration:
	go test -tags=integration -count=1 ./internal/...

test-e2e:
	go test -tags=e2e -count=1 -timeout 15m ./test/e2e/...
