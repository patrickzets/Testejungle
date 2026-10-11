package httpapi

import (
	"net/http"
	"strconv"
	"time"

	"github.com/patrickkaua/wagering/internal/app"
	"github.com/patrickkaua/wagering/internal/auth"
	"github.com/patrickkaua/wagering/internal/domain"
)

type createWalletRequest struct {
	PlayerID       string       `json:"playerId"`
	InitialBalance domain.Money `json:"initialBalance"`
}

type walletResponse struct {
	ID       domain.ID    `json:"id"`
	PlayerID domain.ID    `json:"playerId"`
	Balance  domain.Money `json:"balance"`
	Version  int64        `json:"version"`
}

func toWalletResponse(w *domain.Wallet) walletResponse {
	return walletResponse{ID: w.ID(), PlayerID: w.PlayerID(), Balance: w.Balance(), Version: w.Version()}
}

func (s *Server) createWallet(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	if err := p.AuthorizeWalletAdmin(); err != nil {
		writeError(w, err)
		return
	}
	var req createWalletRequest
	if err := decode(w, r, &req); err != nil {
		writeError(w, err)
		return
	}
	playerID, err := domain.ParseID(req.PlayerID)
	if err != nil {
		writeError(w, err)
		return
	}
	wallet, err := s.svc.OpenWallet(r.Context(), playerID, req.InitialBalance, CorrelationID(r.Context()))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, toWalletResponse(wallet))
}

func (s *Server) getWallet(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	if err := p.AuthorizeWalletAdmin(); err != nil {
		writeError(w, err)
		return
	}
	id, err := domain.ParseID(r.PathValue("walletId"))
	if err != nil {
		writeError(w, err)
		return
	}
	wallet, err := s.svc.GetWallet(r.Context(), id)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toWalletResponse(wallet))
}

type ledgerEntryResponse struct {
	ID            domain.ID        `json:"id"`
	TransactionID domain.ID        `json:"transactionId"`
	Direction     domain.Direction `json:"direction"`
	Money         domain.Money     `json:"money"`
	BalanceBefore domain.Money     `json:"balanceBefore"`
	BalanceAfter  domain.Money     `json:"balanceAfter"`
	CreatedAt     time.Time        `json:"createdAt"`
}

type ledgerResponse struct {
	Entries    []ledgerEntryResponse `json:"entries"`
	NextCursor string                `json:"nextCursor,omitempty"`
}

func (s *Server) listLedger(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	if err := p.AuthorizeWalletAdmin(); err != nil {
		writeError(w, err)
		return
	}
	id, err := domain.ParseID(r.PathValue("walletId"))
	if err != nil {
		writeError(w, err)
		return
	}
	limit := 0
	if raw := r.URL.Query().Get("limit"); raw != "" {
		limit, err = strconv.Atoi(raw)
		if err != nil || limit < 1 {
			writeProblem(w, http.StatusBadRequest, "INVALID_REQUEST", "limit must be a positive integer")
			return
		}
	}
	page, err := s.svc.ListLedger(r.Context(), id, r.URL.Query().Get("cursor"), limit)
	if err != nil {
		writeError(w, err)
		return
	}
	out := ledgerResponse{Entries: make([]ledgerEntryResponse, 0, len(page.Entries)), NextCursor: page.NextCursor}
	for _, e := range page.Entries {
		out.Entries = append(out.Entries, ledgerEntryResponse{
			ID: e.ID(), TransactionID: e.TransactionID(), Direction: e.Direction(), Money: e.Money(),
			BalanceBefore: e.BalanceBefore(), BalanceAfter: e.BalanceAfter(), CreatedAt: e.CreatedAt(),
		})
	}
	writeJSON(w, http.StatusOK, out)
}

type reconciliationResponse struct {
	WalletID          domain.ID    `json:"walletId"`
	StoredBalance     domain.Money `json:"storedBalance"`
	CalculatedBalance domain.Money `json:"calculatedBalance"`
	Difference        domain.Money `json:"difference"`
	Consistent        bool         `json:"consistent"`
	CheckedEntries    int          `json:"checkedEntries"`
}

func (s *Server) reconcile(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	if err := p.AuthorizeWalletAdmin(); err != nil {
		writeError(w, err)
		return
	}
	id, err := domain.ParseID(r.PathValue("walletId"))
	if err != nil {
		writeError(w, err)
		return
	}
	report, err := s.svc.Reconcile(r.Context(), id)
	if err != nil {
		writeError(w, err)
		return
	}
	if !report.Consistent {
		s.log.Error("reconciliation divergence", "walletId", id, "correlationId", CorrelationID(r.Context()),
			"stored", report.Stored.String(), "calculated", report.Calculated.String(), "difference", report.Difference.String())
	}
	writeJSON(w, http.StatusOK, reconciliationResponse{
		WalletID: report.WalletID, StoredBalance: report.Stored, CalculatedBalance: report.Calculated,
		Difference: report.Difference, Consistent: report.Consistent, CheckedEntries: report.Entries,
	})
}

type submitRequest struct {
	ProviderID                     string       `json:"providerId"`
	ExternalTransactionID          string       `json:"externalTransactionId"`
	PlayerID                       string       `json:"playerId"`
	WalletID                       string       `json:"walletId"`
	RoundID                        string       `json:"roundId"`
	GameID                         string       `json:"gameId"`
	Kind                           string       `json:"kind"`
	Money                          domain.Money `json:"money"`
	ReferenceExternalTransactionID string       `json:"referenceExternalTransactionId,omitempty"`
}

// ToOperation validates ids and kind and builds the shared business payload.
func (r submitRequest) ToOperation() (domain.ExternalOperation, error) {
	playerID, err := domain.ParseID(r.PlayerID)
	if err != nil {
		return domain.ExternalOperation{}, err
	}
	walletID, err := domain.ParseID(r.WalletID)
	if err != nil {
		return domain.ExternalOperation{}, err
	}
	kind, err := domain.ParseExternalKind(r.Kind)
	if err != nil {
		return domain.ExternalOperation{}, err
	}
	return domain.ExternalOperation{
		ProviderID: r.ProviderID, ExternalTransactionID: r.ExternalTransactionID, PlayerID: playerID,
		WalletID: walletID, RoundID: r.RoundID, GameID: r.GameID, Kind: kind, Money: r.Money,
		ReferenceExternalTransactionID: r.ReferenceExternalTransactionID,
	}, nil
}

type submitResponse struct {
	TransactionID    domain.ID          `json:"transactionId"`
	Status           domain.Status      `json:"status"`
	Balance          *domain.Money      `json:"balance,omitempty"`
	FailureCode      domain.FailureCode `json:"failureCode,omitempty"`
	IdempotentReplay bool               `json:"idempotentReplay"`
}

func (s *Server) submitTransaction(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		writeProblem(w, http.StatusBadRequest, "INVALID_REQUEST", "Idempotency-Key header is required")
		return
	}
	var req submitRequest
	if err := decode(w, r, &req); err != nil {
		writeError(w, err)
		return
	}
	if err := p.AuthorizeProvider(req.ProviderID); err != nil {
		writeError(w, err)
		return
	}
	op, err := req.ToOperation()
	if err != nil {
		writeError(w, err)
		return
	}
	res, err := s.svc.Submit(r.Context(), app.SubmitInput{
		Operation: op, IdempotencyKey: key, CorrelationID: CorrelationID(r.Context()),
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeSubmitResult(w, res)
}

func writeSubmitResult(w http.ResponseWriter, res app.SubmitResult) {
	t := res.Transaction
	body := submitResponse{TransactionID: t.ID(), Status: t.Status(), IdempotentReplay: res.Replay}
	status := http.StatusOK
	switch t.Status() {
	case domain.StatusProcessed:
		body.Balance = t.ResultBalance()
	case domain.StatusRejected:
		body.FailureCode = t.FailureCode()
		status = http.StatusUnprocessableEntity
	case domain.StatusFailed:
		body.FailureCode = t.FailureCode()
		status = http.StatusInternalServerError
	default:
		status = http.StatusAccepted
	}
	writeJSON(w, status, body)
}

type transactionResponse struct {
	TransactionID                  domain.ID          `json:"transactionId"`
	ProviderID                     string             `json:"providerId"`
	ExternalTransactionID          string             `json:"externalTransactionId"`
	WalletID                       domain.ID          `json:"walletId"`
	PlayerID                       domain.ID          `json:"playerId"`
	RoundID                        string             `json:"roundId"`
	GameID                         string             `json:"gameId"`
	Kind                           domain.Kind        `json:"kind"`
	Money                          domain.Money       `json:"money"`
	ReferenceExternalTransactionID string             `json:"referenceExternalTransactionId,omitempty"`
	Status                         domain.Status      `json:"status"`
	FailureCode                    domain.FailureCode `json:"failureCode,omitempty"`
	Balance                        *domain.Money      `json:"balance,omitempty"`
	Attempts                       int                `json:"attempts"`
	NextAttemptAt                  *time.Time         `json:"nextAttemptAt,omitempty"`
	CreatedAt                      time.Time          `json:"createdAt"`
	UpdatedAt                      time.Time          `json:"updatedAt"`
}

func toTransactionResponse(t *domain.WagerTransaction) transactionResponse {
	return transactionResponse{
		TransactionID: t.ID(), ProviderID: t.ProviderID(), ExternalTransactionID: t.ExternalID(), WalletID: t.WalletID(),
		PlayerID: t.PlayerID(), RoundID: t.RoundID(), GameID: t.GameID(), Kind: t.Kind(), Money: t.Money(),
		ReferenceExternalTransactionID: t.ReferenceExternalID(), Status: t.Status(), FailureCode: t.FailureCode(),
		Balance: t.ResultBalance(), Attempts: t.Attempts(), NextAttemptAt: t.NextAttemptAt(),
		CreatedAt: t.CreatedAt(), UpdatedAt: t.UpdatedAt(),
	}
}

func (s *Server) getTransaction(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	id, err := domain.ParseID(r.PathValue("transactionId"))
	if err != nil {
		writeError(w, err)
		return
	}
	t, err := s.svc.GetTransaction(r.Context(), id)
	if err != nil {
		writeError(w, err)
		return
	}
	if !p.CanReadProviderResource(t.ProviderID()) {
		writeError(w, app.ErrNotFound)
		return
	}
	writeJSON(w, http.StatusOK, toTransactionResponse(t))
}

func (s *Server) getProviderTransaction(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	providerID := r.PathValue("providerId")
	if !p.CanReadProviderResource(providerID) {
		writeError(w, app.ErrNotFound)
		return
	}
	t, err := s.svc.GetProviderTransaction(r.Context(), providerID, r.PathValue("externalTransactionId"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toTransactionResponse(t))
}
