# Architecture

Distributed wager-processing service. Go 1.27, Uber Fx, PostgreSQL, AWS SQS (LocalStack), Keycloak.

## 1. Layout

```
cmd/wagering/            main (fx.New)
internal/domain/         Money, Wallet, WagerTransaction, LedgerEntry, events, errors (no infra imports)
internal/app/            use cases + ports (repositories, TxRunner, Clock, IDs, Publisher)
internal/adapters/postgres/   pgx repositories, TxRunner, outbox/inbox stores, migrations runner
internal/adapters/sqs/        wager consumer, outbox publisher, queue health
internal/adapters/httpapi/    handlers, middleware, problem responses
internal/auth/           OIDC JWT verification, principal, authorization policies
internal/platform/       config, slog, prometheus metrics
internal/fxapp/          fx.Module wiring and lifecycle hooks
migrations/              golang-migrate files NNNNNN_name.{up,down}.sql
deploy/                  keycloak realm, localstack init
```

Dependency rule: `domain` imports only the standard library. `app` imports `domain`. Adapters import `app` and `domain`.

## 2. Money

`Money{minor int64, currency Currency}`, immutable. Scale is fixed at 2 decimals; `minor` is the amount in cents.
Parsing accepts `^\d+\.\d{2}$` only (no sign, exponent, NaN, Infinity, empty, or extra scale). No normalization is applied,
so `"25.0"` and `"25"` are rejected, and the idempotency hash sees exactly one spelling per value.
Add, sub and negate use checked arithmetic and return `ErrMoneyOverflow`. Max value is `math.MaxInt64` cents.
Currencies are validated against an ISO 4217 allow-list. Operations across currencies fail with `ErrCurrencyMismatch`.
Persistence: `BIGINT` minor units plus `CHAR(3)` currency. No float anywhere.

## 3. Database library and transactions

pgx v5 with explicit SQL. `app.TxRunner.WithinTx(ctx, func(ctx) error)` opens one SQL transaction and places it in the
context. Every repository reads the transaction from the context (falling back to the pool), so one use case commits
wallet, transaction, ledger, inbox and outbox rows atomically. Serialization failures and deadlocks (`40001`, `40P01`)
are retried by the runner a bounded number of times.

## 4. Concurrency

Pessimistic per-wallet locking: `SELECT ... FROM wallets WHERE id=$1 FOR UPDATE` inside the transaction, then an
`UPDATE ... SET balance_minor, version=version+1 WHERE id=$1 AND version=$2`. The version predicate is a second guard
against lost updates. `CHECK (balance_minor >= 0)` makes a negative balance impossible even if application logic fails.
Locks are row-level; different wallets never contend. No global locks.

Lock order on the submit path is: wallet row (`FOR UPDATE`), then insert of the transaction row. The order matters:
`wager_transactions.wallet_id` is a foreign key, so inserting first takes `FOR KEY SHARE` on the wallet row, and two
writers that later upgrade to `FOR UPDATE` deadlock (found by the multi-process suite). Replays are answered by a
lock-free read of committed data before the wallet is locked, so retried requests never queue behind the wallet.
The resolver claims the transaction row (`SKIP LOCKED`) and then locks the wallet; a submit never waits on a claimed
transaction row, so the two paths cannot form a cycle. Remaining `40001`/`40P01` errors are retried by the runner.

## 5. Idempotency

`wager_transactions` has `UNIQUE (provider_id, idempotency_key)` and `UNIQUE (provider_id, external_transaction_id)` for
external rows. Keys are scoped by provider so one provider cannot occupy another provider's key. Flow: read by
`(provider, key)`; if absent, lock the wallet and insert with `ON CONFLICT DO NOTHING`; a concurrent duplicate that
slipped past the read blocks behind the wallet lock, then finds the committed row and answers from it. Same key and same hash returns the stored result with
`idempotentReplay=true` (the original `result_balance_minor`, not the current balance). Same key and different hash
returns `409 IDEMPOTENCY_CONFLICT`. Same `(provider, externalId)` under another key returns `409 DUPLICATE_EXTERNAL_TRANSACTION`.

Hash: SHA-256 hex of canonical JSON (keys sorted, no whitespace) of
`providerId, externalTransactionId, playerId, walletId, roundId, gameId, kind, money{amount,currency}, referenceExternalTransactionId`
(omitted when empty). The idempotency key and transport metadata are excluded. HTTP and SQS build the same struct, so the
hash is identical across channels.

## 6. Transaction state machine

```
PENDING -> PROCESSED | REJECTED | PENDING_REFERENCE | FAILED
PENDING_REFERENCE -> PROCESSED | REJECTED | PENDING_REFERENCE (retry) | FAILED
```
PROCESSED, REJECTED and FAILED are terminal. Operations without a missing dependency complete in the same commit as
their acceptance, so PENDING is never visible. Anything PENDING or PENDING_REFERENCE that is committed is picked up by
the resolver worker (`FOR UPDATE SKIP LOCKED`), so another instance can resume it after a crash.
Transient errors (timeouts, deadlocks, connection loss) roll back and are retried; they never produce FAILED.
FAILED is only written for permanent infrastructure faults (for example corrupted persisted data) for audit.

## 7. Operations and failure codes

| Kind | Movement | Rules |
|---|---|---|
| BET | debit | amount > 0, balance >= amount |
| WIN | credit | amount > 0, optional reference must be a BET of the same round |
| LOSS | none | amount == 0.00, no ledger, no version change |
| REFUND | credit | references a PROCESSED BET, full amount |
| ROLLBACK | opposite of original | references a PROCESSED BET, WIN or REFUND, full amount |

Reference matching on `(providerId, referenceExternalTransactionId)`; provider, player, wallet, currency, round and
amount must match.

One reversal per reference: a partial unique index on `reference_transaction_id` where kind in (REFUND, ROLLBACK) and
status = PROCESSED. That also makes REFUND and ROLLBACK on the same BET mutually exclusive: the second is rejected
`ALREADY_REVERSED`. A ROLLBACK that would debit more than the balance is rejected `REVERSAL_INSUFFICIENT_FUNDS`
(a BET without funds is `INSUFFICIENT_FUNDS`).

Missing reference: stored as PENDING_REFERENCE with exponential backoff (base 2s, factor 2, cap 5m, max 8 attempts or
10 min TTL). On exhaustion: REJECTED `REFERENCE_NOT_FOUND`. Reference exists but is PENDING or PENDING_REFERENCE: keep
waiting under the same policy. Reference is REJECTED or FAILED: REJECTED `REFERENCE_NOT_PROCESSED`.

Failure codes: `INVALID_AMOUNT`, `CURRENCY_MISMATCH`, `PLAYER_MISMATCH`, `INSUFFICIENT_FUNDS`,
`REVERSAL_INSUFFICIENT_FUNDS`, `REFERENCE_NOT_FOUND`, `REFERENCE_NOT_PROCESSED`, `REFERENCE_MISMATCH`,
`ALREADY_REVERSED`, `INVALID_REFERENCE_KIND`, `BALANCE_LIMIT_EXCEEDED`. Malformed input (400) is never persisted.

## 8. Ledger

`wallet_ledger_entries` is append-only: a trigger raises on UPDATE and DELETE (and TRUNCATE is revoked/blocked).
`UNIQUE (wallet_id, transaction_id)`, `CHECK (balance_after = balance_before +/- amount by direction)`. The wallet row
carries `balance_minor` and `version`; reconciliation rebuilds the balance from the ledger and never mutates anything.

## 9. Inbox and outbox

Inbox `(consumer_name, message_id)` primary key, payload hash, received and completed timestamps. Inserted in the same
transaction as the domain changes; a redelivery with an equal hash is acknowledged, a different hash is a poison message
and goes to the DLQ.
Outbox rows are written in the same commit as the state change. Publishers claim rows with
`FOR UPDATE SKIP LOCKED`, set a lease (`locked_until`), publish, then set `published_at`. A crashed publisher's lease
expires and another instance republishes with the same `event_id` (SQS FIFO dedup id). Failures bump `attempts` and set
`next_attempt_at` with backoff.

## 10. Messaging

Queues: `wager-transactions.fifo` (redrive to `wager-transactions-dlq.fifo`, maxReceiveCount 5, visibility timeout 30s,
`FifoThroughputLimit=perMessageGroupId`, `DeduplicationScope=messageGroup`) and `wager-events.fifo` for outbound events.
Messages of one group inside a received batch are processed sequentially and in order; different groups run in parallel
up to `CONSUMER_WORKERS`. When a message fails, the rest of its group in the batch is released (visibility 0) so it is
redelivered behind the failed one. Transient errors back off from `ApproximateReceiveCount` (2s doubling, cap 5m);
invalid or poison messages (bad JSON, wrong type, invalid money, idempotency conflict, inbox hash mismatch) get
visibility 0 and reach the DLQ after five receives. Inbound `MessageGroupId` is `walletId`; `MessageDeduplicationId` is the
envelope `messageId`. Outbound group is `aggregateId`, dedup id is `eventId`. A message is deleted only after the
database commit. Business rejections are committed and deleted; transient errors are left for redelivery with backoff via
visibility changes; invalid or poison messages are not deleted and reach the DLQ. On SIGTERM the poller stops, in-flight
messages finish within the shutdown deadline, otherwise visibility is reset to 0.

Outbound delivery is at-least-once: SQS FIFO deduplication has a finite window and a lease can expire after a
successful publish, so consumers must deduplicate on `eventId`.

## 11. Authentication and authorization

Keycloak (OIDC). Services authenticate with `client_credentials`. The API verifies the JWT signature through the issuer's
JWKS, plus `iss`, `aud` and `exp`. Providers (`provider-a`, `provider-b`) carry a `provider_id` claim and the role
`provider`; the internal client carries the role `wallet-admin`. A provider can only submit and read transactions whose
`providerId` equals its claim (other providers' resources return 404). Wallet endpoints and reconciliation require
`wallet-admin`. Health endpoints are public. Broker access uses its own credentials; the consumer re-validates all
domain rules on every message.

## 12. Fx lifecycle

Constructors are provided per `fx.Module`. Start hooks validate config and ping PostgreSQL and SQS, then start the HTTP
server and workers with their own cancellable contexts. Stop hooks stop accepting input, wait for workers (observable
via a done channel) within the stop timeout, and close the pool last.

## 13. Limitations and interpretations

- Processing is synchronous: an accepted operation is committed in its final state, so `PENDING` is a transient
  in-memory state. Only `PENDING_REFERENCE` survives a commit, and the resolver worker (any instance) resumes it.
  Transient errors (timeouts, deadlocks, connection loss) roll back and are retried. `FAILED` (`INTERNAL_FAILURE`) is
  written by the resolver when a pending transaction hits an invariant violation that retrying cannot fix (invalid
  money or state); it is terminal and kept for audit. A credit that would overflow the balance is rejected
  deterministically as `BALANCE_LIMIT_EXCEEDED`.
- A reversal whose reference exists but is still `PENDING_REFERENCE` keeps waiting and expires with
  `REFERENCE_NOT_PROCESSED`; a missing reference expires with `REFERENCE_NOT_FOUND`.
- A WIN may carry an optional reference to a BET; when given it follows the same waiting rules.
- Partial reversals are not supported. A ROLLBACK of a ROLLBACK is rejected (`INVALID_REFERENCE_KIND`).
- Currencies: BRL, USD, EUR and GBP are allowed; each wallet is single-currency. The allow-list is in
  `internal/domain/money.go`.
- Authentication trusts the OIDC issuer's JWKS (RS256). Roles come from the top-level `roles` claim and the provider
  from `provider_id`. When the JWKS endpoint is unreachable, requests get 503 instead of 401. `/metrics` is served
  unauthenticated on the application port and should be restricted at the network layer.
- Broker authorization is the broker's own credentials and policies; LocalStack uses static test credentials.
- Outbound events are at-least-once; consumers must deduplicate on `eventId`.
- A message group per wallet serializes a wallet's SQS traffic; HTTP traffic is serialized by the row lock.
- No OpenTelemetry tracing, dashboards, double-entry ledger or load tests (optional items).
- The race detector needs cgo; on Windows it runs inside a golang container (`make test-race`).
- LocalStack's FIFO implementation keeps a message invisible for the full visibility timeout when its consumer is
  killed mid-poll, so tests that kill instances wait for it. Real SQS behaves the same way.
