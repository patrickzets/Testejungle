package domain

import (
	"encoding/json"
	"time"
)

const (
	EventWagerTransactionProcessed        = "WagerTransactionProcessed"
	EventWagerTransactionRejected         = "WagerTransactionRejected"
	EventWalletBalanceChanged             = "WalletBalanceChanged"
	EventWagerTransactionPendingReference = "WagerTransactionPendingReference"

	eventVersion = 1
)

// Event is an immutable outbox snapshot. Data holds the serialized typed payload.
type Event struct {
	EventID       ID              `json:"eventId"`
	EventType     string          `json:"eventType"`
	AggregateID   ID              `json:"aggregateId"`
	CorrelationID string          `json:"correlationId"`
	CausationID   string          `json:"causationId,omitempty"`
	OccurredAt    time.Time       `json:"occurredAt"`
	Version       int             `json:"version"`
	Data          json.RawMessage `json:"data"`
}

type WagerTransactionProcessedData struct {
	TransactionID         ID     `json:"transactionId"`
	Kind                  Kind   `json:"kind"`
	Origin                Origin `json:"origin"`
	WalletID              ID     `json:"walletId"`
	PlayerID              ID     `json:"playerId"`
	Money                 Money  `json:"money"`
	Balance               Money  `json:"balance"`
	ProviderID            string `json:"providerId,omitempty"`
	ExternalTransactionID string `json:"externalTransactionId,omitempty"`
	RoundID               string `json:"roundId,omitempty"`
	GameID                string `json:"gameId,omitempty"`
}

type WagerTransactionRejectedData struct {
	TransactionID         ID          `json:"transactionId"`
	Kind                  Kind        `json:"kind"`
	WalletID              ID          `json:"walletId"`
	PlayerID              ID          `json:"playerId"`
	Money                 Money       `json:"money"`
	FailureCode           FailureCode `json:"failureCode"`
	ProviderID            string      `json:"providerId"`
	ExternalTransactionID string      `json:"externalTransactionId"`
	RoundID               string      `json:"roundId"`
	GameID                string      `json:"gameId"`
}

type WalletBalanceChangedData struct {
	WalletID      ID        `json:"walletId"`
	TransactionID ID        `json:"transactionId"`
	Direction     Direction `json:"direction"`
	Money         Money     `json:"money"`
	BalanceBefore Money     `json:"balanceBefore"`
	BalanceAfter  Money     `json:"balanceAfter"`
	WalletVersion int64     `json:"walletVersion"`
}

type WagerTransactionPendingReferenceData struct {
	TransactionID                  ID        `json:"transactionId"`
	Kind                           Kind      `json:"kind"`
	WalletID                       ID        `json:"walletId"`
	ProviderID                     string    `json:"providerId"`
	ExternalTransactionID          string    `json:"externalTransactionId"`
	ReferenceExternalTransactionID string    `json:"referenceExternalTransactionId"`
	Attempts                       int       `json:"attempts"`
	NextAttemptAt                  time.Time `json:"nextAttemptAt"`
}

func newEvent(eventType string, aggregate ID, correlationID, causationID string, now time.Time, data any) (Event, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return Event{}, err
	}
	if correlationID == "" {
		correlationID = string(aggregate)
	}
	return Event{
		EventID: NewID(), EventType: eventType, AggregateID: aggregate, CorrelationID: correlationID,
		CausationID: causationID, OccurredAt: now.UTC(), Version: eventVersion, Data: raw,
	}, nil
}

func NewWagerTransactionProcessedEvent(t *WagerTransaction, correlationID string, now time.Time) (Event, error) {
	balance := Money{}
	if t.resultBalance != nil {
		balance = *t.resultBalance
	}
	return newEvent(EventWagerTransactionProcessed, t.walletID, correlationID, string(t.id), now,
		WagerTransactionProcessedData{
			TransactionID: t.id, Kind: t.kind, Origin: t.origin, WalletID: t.walletID, PlayerID: t.playerID,
			Money: t.money, Balance: balance, ProviderID: t.op.ProviderID,
			ExternalTransactionID: t.op.ExternalTransactionID, RoundID: t.op.RoundID, GameID: t.op.GameID,
		})
}

func NewWagerTransactionRejectedEvent(t *WagerTransaction, correlationID string, now time.Time) (Event, error) {
	return newEvent(EventWagerTransactionRejected, t.walletID, correlationID, string(t.id), now,
		WagerTransactionRejectedData{
			TransactionID: t.id, Kind: t.kind, WalletID: t.walletID, PlayerID: t.playerID, Money: t.money,
			FailureCode: t.failureCode, ProviderID: t.op.ProviderID, ExternalTransactionID: t.op.ExternalTransactionID,
			RoundID: t.op.RoundID, GameID: t.op.GameID,
		})
}

func NewWalletBalanceChangedEvent(entry LedgerEntry, walletVersion int64, correlationID string, now time.Time) (Event, error) {
	return newEvent(EventWalletBalanceChanged, entry.WalletID(), correlationID, string(entry.TransactionID()), now,
		WalletBalanceChangedData{
			WalletID: entry.WalletID(), TransactionID: entry.TransactionID(), Direction: entry.Direction(),
			Money: entry.Money(), BalanceBefore: entry.BalanceBefore(), BalanceAfter: entry.BalanceAfter(),
			WalletVersion: walletVersion,
		})
}

func NewWagerTransactionPendingReferenceEvent(t *WagerTransaction, correlationID string, now time.Time) (Event, error) {
	next := now
	if t.nextAttemptAt != nil {
		next = *t.nextAttemptAt
	}
	return newEvent(EventWagerTransactionPendingReference, t.walletID, correlationID, string(t.id), now,
		WagerTransactionPendingReferenceData{
			TransactionID: t.id, Kind: t.kind, WalletID: t.walletID, ProviderID: t.op.ProviderID,
			ExternalTransactionID:          t.op.ExternalTransactionID,
			ReferenceExternalTransactionID: t.op.ReferenceExternalTransactionID,
			Attempts:                       t.attempts, NextAttemptAt: next.UTC(),
		})
}
