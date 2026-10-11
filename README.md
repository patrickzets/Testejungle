# Wagering Service

Distributed processing of provider wagering operations (BET, WIN, LOSS, REFUND, ROLLBACK) against player wallets.
Go 1.27, Uber Fx, PostgreSQL 16, AWS SQS FIFO (LocalStack), Keycloak (OIDC). Design decisions are in
[ARCHITECTURE.md](ARCHITECTURE.md).

## Prerequisites

- Docker with Compose v2
- Go 1.27 (only to run tests outside containers)
- `curl` and `python` (or `jq`) for the examples

## Quick start

```bash
docker compose up --build -d
```

This starts PostgreSQL, applies the migrations (one-shot `migrate` service), Keycloak with the imported `wagering`
realm, LocalStack with the queues, and the application on <http://localhost:8000>.

```bash
curl http://localhost:8000/health/ready     # {"checks":{"postgres":"up","sqs":"up"},"status":"ready"}
```

## Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | required | PostgreSQL connection string |
| `HTTP_ADDR` | `:8080` | HTTP listen address (`:8000` in `.env.example`) |
| `AWS_ENDPOINT_URL`, `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | | SQS access (LocalStack: `test`/`test`) |
| `WAGER_QUEUE_URL` | required | inbound FIFO queue |
| `EVENTS_QUEUE_URL` | required | outbound events FIFO queue |
| `OIDC_ISSUER_URL` | required | expected `iss` of tokens |
| `OIDC_JWKS_URL` | issuer + `/protocol/openid-connect/certs` | JWKS endpoint (differs from the issuer inside Docker) |
| `OIDC_AUDIENCE` | `wagering-api` | expected `aud` |
| `INSTANCE_ID` | hostname | label in logs and outbox leases |
| `SHUTDOWN_TIMEOUT` | `25s` | graceful shutdown budget |
| `WORKER_INTERVAL` | `1s` | idle sleep of background workers |
| `CONSUMER_WORKERS` | `4` | concurrent SQS handlers |
| `REFERENCE_MAX_ATTEMPTS`, `REFERENCE_TTL`, `REFERENCE_BASE_DELAY`, `REFERENCE_MAX_DELAY` | `8`, `10m`, `2s`, `5m` | retry policy for missing references |

## Queues

`deploy/localstack/init-sqs.sh` runs automatically when LocalStack becomes ready and creates:

- `wager-transactions.fifo` (visibility timeout 30s, redrive to the DLQ after 5 receives)
- `wager-transactions-dlq.fifo`
- `wager-events.fifo` (outbound events)

## Migrations

Files live in `migrations/` (golang-migrate format).

```bash
make migrate-up      # apply all
make migrate-down    # revert the last migration
```

## Authentication

Keycloak issues tokens with `client_credentials`. Local-only clients (see `.env.example`):

| Client | Secret | Role | Claim |
|---|---|---|---|
| `provider-a` | `provider-a-secret` | `provider` | `provider_id=provider-a` |
| `provider-b` | `provider-b-secret` | `provider` | `provider_id=provider-b` |
| `wallet-internal` | `wallet-internal-secret` | `wallet-admin` | |

```bash
token() {
  curl -s -X POST http://localhost:8080/realms/wagering/protocol/openid-connect/token \
    -d grant_type=client_credentials -d client_id="$1" -d client_secret="$2" \
    | python -c "import sys,json;print(json.load(sys.stdin)['access_token'])"
}
ADMIN=$(token wallet-internal wallet-internal-secret)
PROVIDER=$(token provider-a provider-a-secret)
```

## Example calls

```bash
PLAYER=$(python -c "import uuid;print(uuid.uuid4())")

# open a wallet (internal service only)
curl -s -X POST localhost:8000/wallets -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' \
  -d "{\"playerId\":\"$PLAYER\",\"initialBalance\":{\"amount\":\"1000.00\",\"currency\":\"BRL\"}}"

WALLET=<id from the response>

# place a bet (provider)
curl -s -X POST localhost:8000/wagering/transactions \
  -H "Authorization: Bearer $PROVIDER" -H 'Idempotency-Key: provider-a:transaction-123' \
  -H 'Content-Type: application/json' \
  -d "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"transaction-123\",\"playerId\":\"$PLAYER\",
       \"walletId\":\"$WALLET\",\"roundId\":\"round-987\",\"gameId\":\"fortune-chimp\",\"kind\":\"BET\",
       \"money\":{\"amount\":\"25.00\",\"currency\":\"BRL\"}}"

curl -s localhost:8000/wallets/$WALLET/ledger?limit=50 -H "Authorization: Bearer $ADMIN"
curl -s -X POST localhost:8000/wallets/$WALLET/reconciliation -H "Authorization: Bearer $ADMIN"
curl -s localhost:8000/providers/provider-a/wagering/transactions/transaction-123 -H "Authorization: Bearer $PROVIDER"
```

### HTTP status codes

| Situation | Status | Body |
|---|---|---|
| Processed (or replay of a processed one) | 200 | `{transactionId,status:"PROCESSED",balance,idempotentReplay}` |
| Waiting for a reference | 202 | `{transactionId,status:"PENDING_REFERENCE",idempotentReplay}` |
| Business rejection | 422 | `{transactionId,status:"REJECTED",failureCode,idempotentReplay}` |
| Invalid input (money format, ids, kind, missing `Idempotency-Key`, unknown fields) | 400 | `{error:{code:"INVALID_REQUEST",message}}` |
| Missing or invalid token | 401 | `{error:{code:"UNAUTHENTICATED"}}` |
| Not allowed (wrong provider, wallet endpoints for providers) | 403 | `{error:{code:"FORBIDDEN"}}` |
| Unknown or foreign resource | 404 | `{error:{code:"NOT_FOUND"}}` |
| Key reused with another payload / external id under another key / wallet exists | 409 | `IDEMPOTENCY_CONFLICT`, `DUPLICATE_EXTERNAL_TRANSACTION`, `WALLET_ALREADY_EXISTS` |
| Transient failure (database or broker unavailable) | 503 | `{error:{code:"SERVICE_UNAVAILABLE"}}` plus `Retry-After`; retry the same request |

Rejection codes: `INSUFFICIENT_FUNDS`, `REVERSAL_INSUFFICIENT_FUNDS`, `REFERENCE_NOT_FOUND`, `REFERENCE_NOT_PROCESSED`,
`REFERENCE_MISMATCH`, `ALREADY_REVERSED`, `INVALID_REFERENCE_KIND`, `CURRENCY_MISMATCH`, `PLAYER_MISMATCH`,
`BALANCE_LIMIT_EXCEEDED`. A transaction left in `FAILED` (`INTERNAL_FAILURE`) is an audited permanent fault.

### SQS message

Send `WagerTransactionRequested` envelopes to `wager-transactions.fifo` with `MessageGroupId=<walletId>` and
`MessageDeduplicationId=<messageId>` (format in ARCHITECTURE.md section 10).

## Tests

```bash
make deps              # postgres, keycloak, localstack + migrations
set -a; . ./.env.example; set +a      # export the variables used by integration tests

go vet ./...
go test ./...                         # unit tests, no infrastructure
make test-race                        # go test -race inside a golang container (no C compiler needed)
make test-integration                 # real PostgreSQL, LocalStack and Fx lifecycle (-tags=integration)
make test-e2e                         # three independent OS processes against the real stack (-tags=e2e)
```

The e2e suite builds the binary, starts three instances with their own pools and memory on different ports, and
covers duplicate submissions, the two 80.00 bets over a 100.00 balance, parallel wallets, reversals arriving before
their reference, abrupt kills, restarts, outbox publisher races and authorization isolation.

On Linux or macOS with a C toolchain, `make test-race-local` runs the race detector without Docker.

## Running several instances

```bash
docker compose up -d --build --scale app=3     # instances listen on host ports 8000-8002
```

Instances coordinate only through PostgreSQL (row locks, unique constraints, leases) and SQS.

## Layout

See ARCHITECTURE.md section 1.
