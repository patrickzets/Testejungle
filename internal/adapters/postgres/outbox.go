package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/patrickkaua/wagering/internal/app"
	"github.com/patrickkaua/wagering/internal/domain"
)

type OutboxRepository struct {
	pool *pgxpool.Pool
}

func NewOutboxRepository(pool *pgxpool.Pool) *OutboxRepository {
	return &OutboxRepository{pool: pool}
}

func (r *OutboxRepository) Add(ctx context.Context, events ...domain.Event) error {
	if len(events) == 0 {
		return nil
	}
	db := GetDBTX(ctx, r.pool)
	const query = `
		INSERT INTO outbox (
			event_id, aggregate_id, event_type, payload,
			occurred_at, attempts, next_attempt_at
		) VALUES ($1, $2, $3, $4, $5, 0, $6)
	`
	for _, e := range events {
		payload, err := json.Marshal(e)
		if err != nil {
			return fmt.Errorf("marshal outbox event: %w", err)
		}
		_, err = db.Exec(ctx, query,
			e.EventID.String(),
			e.AggregateID.String(),
			e.EventType,
			payload,
			e.OccurredAt,
			e.OccurredAt,
		)
		if err != nil {
			return fmt.Errorf("insert outbox row: %w", err)
		}
	}
	return nil
}

func (r *OutboxRepository) Claim(ctx context.Context, owner string, now time.Time, lease time.Duration, limit int) ([]app.OutboxRecord, error) {
	now = now.UTC()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin claim tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	const selectQuery = `
		SELECT event_id, payload, attempts
		FROM outbox
		WHERE published_at IS NULL
		  AND next_attempt_at <= $1
		  AND (locked_until IS NULL OR locked_until < $1)
		ORDER BY next_attempt_at ASC
		LIMIT $2
		FOR UPDATE SKIP LOCKED
	`

	rows, err := tx.Query(ctx, selectQuery, now, limit)
	if err != nil {
		return nil, fmt.Errorf("select claimable outbox rows: %w", err)
	}
	defer rows.Close()

	type rawRecord struct {
		id       string
		payload  []byte
		attempts int
	}
	var rawRecords []rawRecord
	var ids []string

	for rows.Next() {
		var rec rawRecord
		if err := rows.Scan(&rec.id, &rec.payload, &rec.attempts); err != nil {
			return nil, fmt.Errorf("scan claimable outbox row: %w", err)
		}
		rawRecords = append(rawRecords, rec)
		ids = append(ids, rec.id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate claimable rows: %w", err)
	}

	if len(ids) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit empty claim tx: %w", err)
		}
		return []app.OutboxRecord{}, nil
	}

	lockedUntil := now.Add(lease)
	const updateQuery = `
		UPDATE outbox
		SET locked_by = $1, locked_until = $2
		WHERE event_id = ANY($3)
	`
	if _, err := tx.Exec(ctx, updateQuery, owner, lockedUntil, ids); err != nil {
		return nil, fmt.Errorf("lock claimable outbox rows: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit claim tx: %w", err)
	}

	records := make([]app.OutboxRecord, 0, len(rawRecords))
	for _, raw := range rawRecords {
		var evt domain.Event
		if err := json.Unmarshal(raw.payload, &evt); err != nil {
			return nil, fmt.Errorf("unmarshal outbox event: %w", err)
		}
		records = append(records, app.OutboxRecord{
			Event:    evt,
			Attempts: raw.attempts,
		})
	}

	return records, nil
}

func (r *OutboxRepository) MarkPublished(ctx context.Context, eventID domain.ID, now time.Time) error {
	db := GetDBTX(ctx, r.pool)
	const query = `
		UPDATE outbox
		SET published_at = $1, locked_until = NULL, locked_by = NULL
		WHERE event_id = $2
	`
	_, err := db.Exec(ctx, query, now.UTC(), eventID.String())
	if err != nil {
		return fmt.Errorf("mark outbox published: %w", err)
	}
	return nil
}

func (r *OutboxRepository) Retry(ctx context.Context, eventID domain.ID, next time.Time) error {
	db := GetDBTX(ctx, r.pool)
	const query = `
		UPDATE outbox
		SET attempts = attempts + 1, next_attempt_at = $1, locked_until = NULL, locked_by = NULL
		WHERE event_id = $2
	`
	_, err := db.Exec(ctx, query, next.UTC(), eventID.String())
	if err != nil {
		return fmt.Errorf("retry outbox event: %w", err)
	}
	return nil
}

func (r *OutboxRepository) Lag(ctx context.Context, now time.Time) (time.Duration, error) {
	db := GetDBTX(ctx, r.pool)
	const query = `
		SELECT occurred_at
		FROM outbox
		WHERE published_at IS NULL
		ORDER BY occurred_at ASC
		LIMIT 1
	`
	var occurredAt time.Time
	err := db.QueryRow(ctx, query).Scan(&occurredAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("query outbox lag: %w", err)
	}

	diff := now.UTC().Sub(occurredAt)
	if diff < 0 {
		diff = 0
	}
	return diff, nil
}
