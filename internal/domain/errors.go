package domain

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidInput        = errors.New("invalid input")
	ErrInvalidMoney        = errors.New("invalid money")
	ErrMoneyOverflow       = errors.New("money overflow")
	ErrCurrencyMismatch    = errors.New("currency mismatch")
	ErrInsufficientFunds   = errors.New("insufficient funds")
	ErrInvalidTransition   = errors.New("invalid state transition")
	ErrOpeningNotAllowed   = errors.New("opening transactions are internal only")
	ErrTerminalTransaction = errors.New("transaction is in a terminal state")
)

type FailureCode string

const (
	CodeInvalidAmount             FailureCode = "INVALID_AMOUNT"
	CodeCurrencyMismatch          FailureCode = "CURRENCY_MISMATCH"
	CodePlayerMismatch            FailureCode = "PLAYER_MISMATCH"
	CodeInsufficientFunds         FailureCode = "INSUFFICIENT_FUNDS"
	CodeReversalInsufficientFunds FailureCode = "REVERSAL_INSUFFICIENT_FUNDS"
	CodeReferenceNotFound         FailureCode = "REFERENCE_NOT_FOUND"
	CodeReferenceNotProcessed     FailureCode = "REFERENCE_NOT_PROCESSED"
	CodeReferenceMismatch         FailureCode = "REFERENCE_MISMATCH"
	CodeAlreadyReversed           FailureCode = "ALREADY_REVERSED"
	CodeInvalidReferenceKind      FailureCode = "INVALID_REFERENCE_KIND"
	CodeBalanceLimitExceeded      FailureCode = "BALANCE_LIMIT_EXCEEDED"
	CodeInternalFailure           FailureCode = "INTERNAL_FAILURE"
)

// Rejection is a definitive business refusal that must be persisted as REJECTED.
type Rejection struct {
	Code    FailureCode
	Message string
}

func (r *Rejection) Error() string {
	return fmt.Sprintf("rejected: %s: %s", r.Code, r.Message)
}

func reject(code FailureCode, format string, args ...any) *Rejection {
	return &Rejection{Code: code, Message: fmt.Sprintf(format, args...)}
}

// ValidationError wraps ErrInvalidInput with the offending field.
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("invalid %s: %s", e.Field, e.Message)
}

func (e *ValidationError) Unwrap() error { return ErrInvalidInput }

func invalid(field, format string, args ...any) error {
	return &ValidationError{Field: field, Message: fmt.Sprintf(format, args...)}
}
