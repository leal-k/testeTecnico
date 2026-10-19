package httpapi

import (
	"time"

	"github.com/google/uuid"

	"github.com/leal-k/testeTecnico/internal/app"
	"github.com/leal-k/testeTecnico/internal/domain/ledger"
	"github.com/leal-k/testeTecnico/internal/domain/money"
	"github.com/leal-k/testeTecnico/internal/domain/wager"
	"github.com/leal-k/testeTecnico/internal/domain/wallet"
)

// moneyDTO é o formato de dinheiro do contrato: {"amount":"25.00","currency":"BRL"}.
// amount é string: um número JSON (25.00) é recusado na decodificação, então o
// valor nunca passa por float.
type moneyDTO struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

func toMoneyDTO(m money.Money) moneyDTO {
	return moneyDTO{Amount: m.Amount(), Currency: m.Currency().String()}
}

func optionalMoney(m money.Money) *moneyDTO {
	if !m.IsInitialized() {
		return nil
	}
	d := toMoneyDTO(m)
	return &d
}

// ---- requisições ------------------------------------------------------------

type openWalletRequest struct {
	PlayerID       string    `json:"playerId"`
	InitialBalance *moneyDTO `json:"initialBalance"`
}

type processRequest struct {
	ProviderID                     string    `json:"providerId"`
	ExternalTransactionID          string    `json:"externalTransactionId"`
	PlayerID                       string    `json:"playerId"`
	WalletID                       string    `json:"walletId"`
	RoundID                        string    `json:"roundId"`
	GameID                         string    `json:"gameId"`
	Kind                           string    `json:"kind"`
	Money                          *moneyDTO `json:"money"`
	ReferenceExternalTransactionID string    `json:"referenceExternalTransactionId,omitempty"`
}

// ---- respostas --------------------------------------------------------------

type walletResponse struct {
	ID        string    `json:"id"`
	PlayerID  string    `json:"playerId"`
	Balance   moneyDTO  `json:"balance"`
	Version   int64     `json:"version"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

func toWalletResponse(w *wallet.Wallet) walletResponse {
	return walletResponse{
		ID:        w.ID().String(),
		PlayerID:  w.PlayerID().String(),
		Balance:   toMoneyDTO(w.Balance()),
		Version:   w.Version(),
		CreatedAt: w.CreatedAt().UTC(),
		UpdatedAt: w.UpdatedAt().UTC(),
	}
}

type ledgerEntryResponse struct {
	ID            string    `json:"id"`
	TransactionID string    `json:"transactionId"`
	Direction     string    `json:"direction"`
	Money         moneyDTO  `json:"money"`
	BalanceBefore moneyDTO  `json:"balanceBefore"`
	BalanceAfter  moneyDTO  `json:"balanceAfter"`
	CreatedAt     time.Time `json:"createdAt"`
}

type ledgerResponse struct {
	WalletID   string                `json:"walletId"`
	Entries    []ledgerEntryResponse `json:"entries"`
	NextCursor *string               `json:"nextCursor"`
}

func toLedgerResponse(walletID string, page app.LedgerPage) ledgerResponse {
	out := ledgerResponse{WalletID: walletID, Entries: make([]ledgerEntryResponse, 0, len(page.Entries))}
	for _, e := range page.Entries {
		out.Entries = append(out.Entries, toLedgerEntry(e))
	}
	if page.NextCursor != "" {
		c := page.NextCursor
		out.NextCursor = &c
	}
	return out
}

func toLedgerEntry(e ledger.Entry) ledgerEntryResponse {
	return ledgerEntryResponse{
		ID:            e.ID().String(),
		TransactionID: e.TransactionID().String(),
		Direction:     string(e.Direction()),
		Money:         toMoneyDTO(e.Amount()),
		BalanceBefore: toMoneyDTO(e.BalanceBefore()),
		BalanceAfter:  toMoneyDTO(e.BalanceAfter()),
		CreatedAt:     e.CreatedAt().UTC(),
	}
}

// processResponse é a resposta de POST /wagering/transactions.
type processResponse struct {
	TransactionID    string    `json:"transactionId"`
	Status           string    `json:"status"`
	Balance          *moneyDTO `json:"balance,omitempty"`
	FailureCode      string    `json:"failureCode,omitempty"`
	IdempotentReplay bool      `json:"idempotentReplay"`
}

func toProcessResponse(r app.Result) processResponse {
	return processResponse{
		TransactionID:    r.TransactionID.String(),
		Status:           string(r.Status),
		Balance:          optionalMoney(r.Balance),
		FailureCode:      string(r.FailureCode),
		IdempotentReplay: r.Replay,
	}
}

// transactionResponse é a resposta das consultas de transação.
type transactionResponse struct {
	TransactionID                  string     `json:"transactionId"`
	Kind                           string     `json:"kind"`
	Status                         string     `json:"status"`
	ProviderID                     string     `json:"providerId,omitempty"`
	ExternalTransactionID          string     `json:"externalTransactionId,omitempty"`
	WalletID                       string     `json:"walletId"`
	PlayerID                       string     `json:"playerId"`
	RoundID                        string     `json:"roundId,omitempty"`
	GameID                         string     `json:"gameId,omitempty"`
	Money                          moneyDTO   `json:"money"`
	Balance                        *moneyDTO  `json:"balance,omitempty"`
	FailureCode                    string     `json:"failureCode,omitempty"`
	ReferenceExternalTransactionID string     `json:"referenceExternalTransactionId,omitempty"`
	ReferenceTransactionID         string     `json:"referenceTransactionId,omitempty"`
	Attempts                       int        `json:"attempts,omitempty"`
	NextAttemptAt                  *time.Time `json:"nextAttemptAt,omitempty"`
	CreatedAt                      time.Time  `json:"createdAt"`
	UpdatedAt                      time.Time  `json:"updatedAt"`
}

func toTransactionResponse(tx *wager.Transaction) transactionResponse {
	s := tx.Snapshot()
	out := transactionResponse{
		TransactionID:                  s.ID.String(),
		Kind:                           string(s.Kind),
		Status:                         string(s.Status),
		ProviderID:                     s.ProviderID,
		ExternalTransactionID:          s.ExternalID,
		WalletID:                       s.WalletID.String(),
		PlayerID:                       s.PlayerID.String(),
		RoundID:                        s.RoundID,
		GameID:                         s.GameID,
		Money:                          toMoneyDTO(s.Amount),
		Balance:                        optionalMoney(s.ResultBalance),
		FailureCode:                    string(s.FailureCode),
		ReferenceExternalTransactionID: s.ReferenceExternalID,
		Attempts:                       s.Attempts,
		CreatedAt:                      s.CreatedAt.UTC(),
		UpdatedAt:                      s.UpdatedAt.UTC(),
	}
	if s.ReferenceID != uuid.Nil {
		out.ReferenceTransactionID = s.ReferenceID.String()
	}
	if !s.NextAttemptAt.IsZero() {
		next := s.NextAttemptAt.UTC()
		out.NextAttemptAt = &next
	}
	return out
}

type reconciliationResponse struct {
	WalletID          string   `json:"walletId"`
	StoredBalance     moneyDTO `json:"storedBalance"`
	CalculatedBalance moneyDTO `json:"calculatedBalance"`
	Difference        moneyDTO `json:"difference"`
	Consistent        bool     `json:"consistent"`
	CheckedEntries    int      `json:"checkedEntries"`
}

func toReconciliationResponse(r app.Reconciliation) reconciliationResponse {
	return reconciliationResponse{
		WalletID:          r.WalletID.String(),
		StoredBalance:     toMoneyDTO(r.Stored),
		CalculatedBalance: toMoneyDTO(r.Calculated),
		Difference:        toMoneyDTO(r.Difference),
		Consistent:        r.Consistent,
		CheckedEntries:    r.CheckedEntries,
	}
}

// errorResponse é o corpo de todo erro: {"error":{"code":"...","message":"..."}}.
type errorResponse struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
