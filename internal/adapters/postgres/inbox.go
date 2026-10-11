package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/patrickkaua/wagering/internal/app"
)

type InboxRepository struct {
	pool *pgxpool.Pool
}

func NewInboxRepository(pool *pgxpool.Pool) *InboxRepository {
	return &InboxRepository{pool: pool}
}

func (r *InboxRepository) Begin(ctx context.Context, consumer, messageID, payloadHash string, now time.Time) (app.InboxStatus, error) {
	db := GetDBTX(ctx, r.pool)
	const query = `
		INSERT INTO inbox (consumer_name, message_id, payload_hash, received_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (consumer_name, message_id) DO NOTHING
	`
	tag, err := db.Exec(ctx, query, consumer, messageID, payloadHash, now.UTC())
	if err != nil {
		return app.InboxNew, fmt.Errorf("insert inbox: %w", err)
	}

	if tag.RowsAffected() == 1 {
		return app.InboxNew, nil
	}

	const selectQuery = `
		SELECT payload_hash
		FROM inbox
		WHERE consumer_name = $1 AND message_id = $2
	`
	var storedHash string
	if err := db.QueryRow(ctx, selectQuery, consumer, messageID).Scan(&storedHash); err != nil {
		return app.InboxNew, fmt.Errorf("get existing inbox hash: %w", err)
	}

	if storedHash == payloadHash {
		return app.InboxDuplicate, nil
	}
	return app.InboxConflict, nil
}

func (r *InboxRepository) Complete(ctx context.Context, consumer, messageID string, now time.Time) error {
	db := GetDBTX(ctx, r.pool)
	const query = `
		UPDATE inbox
		SET completed_at = $1
		WHERE consumer_name = $2 AND message_id = $3
	`
	_, err := db.Exec(ctx, query, now.UTC(), consumer, messageID)
	if err != nil {
		return fmt.Errorf("complete inbox message: %w", err)
	}
	return nil
}
