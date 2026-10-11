package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
)

type Kind string

const (
	KindOpening  Kind = "OPENING"
	KindBet      Kind = "BET"
	KindWin      Kind = "WIN"
	KindLoss     Kind = "LOSS"
	KindRefund   Kind = "REFUND"
	KindRollback Kind = "ROLLBACK"
)

func ParseExternalKind(s string) (Kind, error) {
	switch k := Kind(s); k {
	case KindBet, KindWin, KindLoss, KindRefund, KindRollback:
		return k, nil
	case KindOpening:
		return "", ErrOpeningNotAllowed
	}
	return "", invalid("kind", "must be one of BET, WIN, LOSS, REFUND, ROLLBACK")
}

func (k Kind) IsReversal() bool { return k == KindRefund || k == KindRollback }

type Status string

const (
	StatusPending          Status = "PENDING"
	StatusPendingReference Status = "PENDING_REFERENCE"
	StatusProcessed        Status = "PROCESSED"
	StatusRejected         Status = "REJECTED"
	StatusFailed           Status = "FAILED"
)

func (s Status) IsTerminal() bool {
	return s == StatusProcessed || s == StatusRejected || s == StatusFailed
}

type Origin string

const (
	OriginInternal Origin = "INTERNAL"
	OriginExternal Origin = "EXTERNAL"
)

const maxFieldLen = 128

// ExternalOperation is the business payload shared by HTTP and SQS. Its hash drives idempotency.
type ExternalOperation struct {
	ProviderID                     string
	ExternalTransactionID          string
	PlayerID                       ID
	WalletID                       ID
	RoundID                        string
	GameID                         string
	Kind                           Kind
	Money                          Money
	ReferenceExternalTransactionID string
}

func (o ExternalOperation) Validate() error {
	for _, f := range []struct{ name, value string }{
		{"providerId", o.ProviderID},
		{"externalTransactionId", o.ExternalTransactionID},
		{"roundId", o.RoundID},
		{"gameId", o.GameID},
	} {
		if f.value == "" || len(f.value) > maxFieldLen {
			return invalid(f.name, "required, up to %d characters", maxFieldLen)
		}
	}
	if len(o.ReferenceExternalTransactionID) > maxFieldLen {
		return invalid("referenceExternalTransactionId", "up to %d characters", maxFieldLen)
	}
	if o.PlayerID.IsZero() || o.WalletID.IsZero() {
		return invalid("ids", "playerId and walletId are required")
	}
	if o.Kind == KindOpening {
		return ErrOpeningNotAllowed
	}
	if _, err := ParseExternalKind(string(o.Kind)); err != nil {
		return err
	}
	if !o.Money.IsValid() || o.Money.IsNegative() {
		return invalid("money", "must be a valid non-negative amount")
	}
	if o.Kind == KindLoss {
		if !o.Money.IsZero() {
			return invalid("money", "LOSS requires amount 0.00")
		}
	} else if !o.Money.IsPositive() {
		return invalid("money", "%s requires a positive amount", o.Kind)
	}
	if o.Kind.IsReversal() && o.ReferenceExternalTransactionID == "" {
		return invalid("referenceExternalTransactionId", "required for %s", o.Kind)
	}
	if !o.Kind.IsReversal() && o.Kind != KindWin && o.ReferenceExternalTransactionID != "" {
		return invalid("referenceExternalTransactionId", "not allowed for %s", o.Kind)
	}
	return nil
}

// Hash is SHA-256 over canonical JSON (sorted keys) of the business fields.
func (o ExternalOperation) Hash() string {
	m := map[string]any{
		"providerId":            o.ProviderID,
		"externalTransactionId": o.ExternalTransactionID,
		"playerId":              string(o.PlayerID),
		"walletId":              string(o.WalletID),
		"roundId":               o.RoundID,
		"gameId":                o.GameID,
		"kind":                  string(o.Kind),
		"money":                 map[string]string{"amount": o.Money.Amount(), "currency": string(o.Money.Currency())},
	}
	if o.ReferenceExternalTransactionID != "" {
		m["referenceExternalTransactionId"] = o.ReferenceExternalTransactionID
	}
	b, _ := json.Marshal(m)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// WagerTransaction records one operation and its outcome. State changes go through explicit transitions.
type WagerTransaction struct {
	id             ID
	origin         Origin
	op             ExternalOperation
	idempotencyKey string
	payloadHash    string
	walletID       ID
	playerID       ID
	kind           Kind
	money          Money
	referenceTxID  ID
	status         Status
	failureCode    FailureCode
	resultBalance  *Money
	attempts       int
	nextAttemptAt  *time.Time
	firstPendingAt *time.Time
	createdAt      time.Time
	updatedAt      time.Time
}

func NewExternalTransaction(id ID, op ExternalOperation, idempotencyKey string, now time.Time) (*WagerTransaction, error) {
	if err := op.Validate(); err != nil {
		return nil, err
	}
	if id.IsZero() {
		return nil, invalid("id", "required")
	}
	if idempotencyKey == "" || len(idempotencyKey) > maxFieldLen {
		return nil, invalid("idempotencyKey", "required, up to 128 characters")
	}
	now = now.UTC()
	return &WagerTransaction{
		id: id, origin: OriginExternal, op: op, idempotencyKey: idempotencyKey, payloadHash: op.Hash(),
		walletID: op.WalletID, playerID: op.PlayerID, kind: op.Kind, money: op.Money,
		status: StatusPending, createdAt: now, updatedAt: now,
	}, nil
}

// NewOpeningTransaction builds the internal, already processed OPENING transaction of a wallet.
func NewOpeningTransaction(id, walletID, playerID ID, money Money, now time.Time) (*WagerTransaction, error) {
	if id.IsZero() || walletID.IsZero() || playerID.IsZero() {
		return nil, invalid("opening", "ids are required")
	}
	if !money.IsPositive() {
		return nil, invalid("money", "opening amount must be positive")
	}
	now = now.UTC()
	balance := money
	return &WagerTransaction{
		id: id, origin: OriginInternal, walletID: walletID, playerID: playerID, kind: KindOpening,
		money: money, status: StatusProcessed, resultBalance: &balance, createdAt: now, updatedAt: now,
	}, nil
}

type PersistedTransaction struct {
	ID             ID
	Origin         Origin
	Operation      ExternalOperation
	IdempotencyKey string
	PayloadHash    string
	WalletID       ID
	PlayerID       ID
	Kind           Kind
	Money          Money
	ReferenceTxID  ID
	Status         Status
	FailureCode    FailureCode
	ResultBalance  *Money
	Attempts       int
	NextAttemptAt  *time.Time
	FirstPendingAt *time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// RehydrateTransaction restores a persisted transaction without re-validating or transitioning it.
func RehydrateTransaction(p PersistedTransaction) *WagerTransaction {
	return &WagerTransaction{
		id: p.ID, origin: p.Origin, op: p.Operation, idempotencyKey: p.IdempotencyKey, payloadHash: p.PayloadHash,
		walletID: p.WalletID, playerID: p.PlayerID, kind: p.Kind, money: p.Money, referenceTxID: p.ReferenceTxID,
		status: p.Status, failureCode: p.FailureCode, resultBalance: p.ResultBalance, attempts: p.Attempts,
		nextAttemptAt: p.NextAttemptAt, firstPendingAt: p.FirstPendingAt, createdAt: p.CreatedAt, updatedAt: p.UpdatedAt,
	}
}

func (t *WagerTransaction) ID() ID                       { return t.id }
func (t *WagerTransaction) Origin() Origin               { return t.origin }
func (t *WagerTransaction) Operation() ExternalOperation { return t.op }
func (t *WagerTransaction) ProviderID() string           { return t.op.ProviderID }
func (t *WagerTransaction) ExternalID() string           { return t.op.ExternalTransactionID }
func (t *WagerTransaction) IdempotencyKey() string       { return t.idempotencyKey }
func (t *WagerTransaction) PayloadHash() string          { return t.payloadHash }
func (t *WagerTransaction) WalletID() ID                 { return t.walletID }
func (t *WagerTransaction) PlayerID() ID                 { return t.playerID }
func (t *WagerTransaction) RoundID() string              { return t.op.RoundID }
func (t *WagerTransaction) GameID() string               { return t.op.GameID }
func (t *WagerTransaction) Kind() Kind                   { return t.kind }
func (t *WagerTransaction) Money() Money                 { return t.money }
func (t *WagerTransaction) ReferenceExternalID() string  { return t.op.ReferenceExternalTransactionID }
func (t *WagerTransaction) ReferenceTxID() ID            { return t.referenceTxID }
func (t *WagerTransaction) Status() Status               { return t.status }
func (t *WagerTransaction) FailureCode() FailureCode     { return t.failureCode }
func (t *WagerTransaction) ResultBalance() *Money        { return t.resultBalance }
func (t *WagerTransaction) Attempts() int                { return t.attempts }
func (t *WagerTransaction) NextAttemptAt() *time.Time    { return t.nextAttemptAt }
func (t *WagerTransaction) FirstPendingAt() *time.Time   { return t.firstPendingAt }
func (t *WagerTransaction) CreatedAt() time.Time         { return t.createdAt }
func (t *WagerTransaction) UpdatedAt() time.Time         { return t.updatedAt }

func (t *WagerTransaction) open() error {
	if t.status.IsTerminal() {
		return ErrTerminalTransaction
	}
	return nil
}

func (t *WagerTransaction) MarkProcessed(resultBalance Money, now time.Time) error {
	if err := t.open(); err != nil {
		return err
	}
	if resultBalance.Currency() != t.money.Currency() || resultBalance.IsNegative() {
		return ErrInvalidMoney
	}
	t.status = StatusProcessed
	t.resultBalance = &resultBalance
	t.failureCode = ""
	t.nextAttemptAt = nil
	t.updatedAt = now.UTC()
	return nil
}

func (t *WagerTransaction) Reject(code FailureCode, now time.Time) error {
	if err := t.open(); err != nil {
		return err
	}
	t.status = StatusRejected
	t.failureCode = code
	t.nextAttemptAt = nil
	t.updatedAt = now.UTC()
	return nil
}

func (t *WagerTransaction) Fail(code FailureCode, now time.Time) error {
	if err := t.open(); err != nil {
		return err
	}
	t.status = StatusFailed
	t.failureCode = code
	t.nextAttemptAt = nil
	t.updatedAt = now.UTC()
	return nil
}

// AwaitReference parks the transaction until its reference shows up or the retry budget is spent.
func (t *WagerTransaction) AwaitReference(nextAttempt time.Time, now time.Time) error {
	if err := t.open(); err != nil {
		return err
	}
	if !t.kind.IsReversal() && t.kind != KindWin {
		return ErrInvalidTransition
	}
	now = now.UTC()
	next := nextAttempt.UTC()
	if t.firstPendingAt == nil {
		t.firstPendingAt = &now
	}
	t.status = StatusPendingReference
	t.attempts++
	t.nextAttemptAt = &next
	t.updatedAt = now
	return nil
}

func (t *WagerTransaction) ResolveReference(refTxID ID) {
	t.referenceTxID = refTxID
}

// Movement returns the direction applied by a movement-producing kind. LOSS has none.
func Movement(k Kind) (Direction, bool) {
	switch k {
	case KindBet:
		return Debit, true
	case KindWin, KindRefund:
		return Credit, true
	}
	return "", false
}

// CheckReference validates that ref is an admissible target for t and returns the movement t must apply.
// A *Rejection is returned for definitive mismatches; the caller decides what to do with pending references.
func (t *WagerTransaction) CheckReference(ref *WagerTransaction) (Direction, *Rejection) {
	if ref.ProviderID() != t.ProviderID() || ref.WalletID() != t.walletID || ref.PlayerID() != t.playerID ||
		ref.RoundID() != t.RoundID() {
		return "", reject(CodeReferenceMismatch, "reference belongs to a different provider, player, wallet or round")
	}
	if ref.money.Currency() != t.money.Currency() {
		return "", reject(CodeCurrencyMismatch, "reference currency differs")
	}
	if t.kind == KindWin {
		if ref.Kind() != KindBet {
			return "", reject(CodeInvalidReferenceKind, "WIN may only reference a BET")
		}
		return Credit, nil
	}
	if !ref.money.Equal(t.money) {
		return "", reject(CodeReferenceMismatch, "reversal amount must equal the referenced amount")
	}
	switch t.kind {
	case KindRefund:
		if ref.Kind() != KindBet {
			return "", reject(CodeInvalidReferenceKind, "REFUND may only reference a BET")
		}
		return Credit, nil
	case KindRollback:
		original, ok := Movement(ref.Kind())
		if !ok || ref.Kind() == KindRollback {
			return "", reject(CodeInvalidReferenceKind, "ROLLBACK may only reference a BET, WIN or REFUND")
		}
		return original.Opposite(), nil
	}
	return "", reject(CodeInvalidReferenceKind, "%s does not take a reference", t.kind)
}

func RejectionFor(code FailureCode, format string, args ...any) *Rejection {
	return reject(code, format, args...)
}
