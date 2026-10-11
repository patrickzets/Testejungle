package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/patrickkaua/wagering/internal/app"
	"github.com/patrickkaua/wagering/internal/auth"
	"github.com/patrickkaua/wagering/internal/domain"
)

type problem struct {
	Error problemBody `json:"error"`
}

type problemBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeProblem(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, problem{Error: problemBody{Code: code, Message: message}})
}

// writeError maps application and domain errors to the documented HTTP contract.
func writeError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, auth.ErrForbidden):
		writeProblem(w, http.StatusForbidden, "FORBIDDEN", "the caller is not allowed to perform this operation")
	case errors.Is(err, app.ErrIdempotencyConflict):
		writeProblem(w, http.StatusConflict, "IDEMPOTENCY_CONFLICT", "the idempotency key was already used with a different payload")
	case errors.Is(err, app.ErrDuplicateExternal):
		writeProblem(w, http.StatusConflict, "DUPLICATE_EXTERNAL_TRANSACTION", "the external transaction was already submitted under another idempotency key")
	case errors.Is(err, app.ErrWalletExists):
		writeProblem(w, http.StatusConflict, "WALLET_ALREADY_EXISTS", "a wallet already exists for this player and currency")
	case errors.Is(err, app.ErrWalletNotFound), errors.Is(err, app.ErrNotFound):
		writeProblem(w, http.StatusNotFound, "NOT_FOUND", "resource not found")
	case errors.Is(err, domain.ErrInvalidInput), errors.Is(err, domain.ErrOpeningNotAllowed),
		errors.Is(err, domain.ErrMoneyOverflow), errors.Is(err, domain.ErrInvalidMoney):
		writeProblem(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
	default:
		w.Header().Set("Retry-After", "1")
		writeProblem(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "temporary failure, retry the same request")
	}
}

func decode(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		if errors.Is(err, domain.ErrInvalidInput) || errors.Is(err, domain.ErrMoneyOverflow) {
			return err
		}
		return &requestError{msg: "malformed JSON body"}
	}
	if dec.More() {
		return &requestError{msg: "unexpected data after JSON body"}
	}
	return nil
}

type requestError struct{ msg string }

func (e *requestError) Error() string { return e.msg }
func (e *requestError) Unwrap() error { return domain.ErrInvalidInput }
