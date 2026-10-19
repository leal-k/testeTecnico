// Package event define os eventos de integração gravados na outbox, na mesma
// transação do negócio, e publicados depois.
//
// O Payload de cada evento é um snapshot JSON imutável (o campo "data" do
// envelope publicado). O envelope completo (eventId, eventType, aggregateId,
// correlationId, causationId, occurredAt, version, data) é montado pelo
// publisher a partir dos campos de Event.
package event

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/leal-k/testeTecnico/internal/domain/money"
	"github.com/leal-k/testeTecnico/internal/domain/wager"
	"github.com/leal-k/testeTecnico/internal/domain/wallet"
)

// Type é o tipo do evento, exatamente como publicado.
type Type string

const (
	TypeWagerTransactionProcessed        Type = "WagerTransactionProcessed"
	TypeWagerTransactionRejected         Type = "WagerTransactionRejected"
	TypeWalletBalanceChanged             Type = "WalletBalanceChanged"
	TypeWagerTransactionPendingReference Type = "WagerTransactionPendingReference"
)

const (
	AggregateWagerTransaction = "wager_transaction"
	AggregateWallet           = "wallet"

	currentVersion = 1
)

var ErrInvalidEvent = errors.New("event: invalid event")

// Event é um evento pronto para a outbox.
type Event struct {
	ID            uuid.UUID
	Type          Type
	Version       int
	AggregateType string
	AggregateID   uuid.UUID
	CorrelationID string
	CausationID   string
	OccurredAt    time.Time
	Payload       []byte
}

// Meta reúne os metadados comuns a todo evento.
type Meta struct {
	ID            uuid.UUID
	CorrelationID string
	CausationID   string
	OccurredAt    time.Time
}

type moneyData struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

func moneyOf(m money.Money) moneyData {
	return moneyData{Amount: m.Amount(), Currency: m.Currency().String()}
}

type transactionData struct {
	TransactionID                  string     `json:"transactionId"`
	WalletID                       string     `json:"walletId"`
	PlayerID                       string     `json:"playerId"`
	ProviderID                     string     `json:"providerId,omitempty"`
	ExternalTransactionID          string     `json:"externalTransactionId,omitempty"`
	Type                           string     `json:"type"`
	Status                         string     `json:"status"`
	Amount                         moneyData  `json:"amount"`
	Balance                        *moneyData `json:"balance,omitempty"`
	FailureCode                    string     `json:"failureCode,omitempty"`
	ReferenceExternalTransactionID string     `json:"referenceExternalTransactionId,omitempty"`
	Attempts                       int        `json:"attempts,omitempty"`
	NextAttemptAt                  *time.Time `json:"nextAttemptAt,omitempty"`
}

type balanceData struct {
	WalletID      string    `json:"walletId"`
	TransactionID string    `json:"transactionId"`
	Direction     string    `json:"direction"`
	Money         moneyData `json:"money"`
	BalanceBefore moneyData `json:"balanceBefore"`
	BalanceAfter  moneyData `json:"balanceAfter"`
	WalletVersion int64     `json:"walletVersion"`
}

// TransactionProcessed: a transação terminou com sucesso (inclui LOSS).
func TransactionProcessed(s wager.Snapshot, m Meta) (Event, error) {
	return transactionEvent(TypeWagerTransactionProcessed, wager.StatusProcessed, s, m)
}

// TransactionRejected: a transação foi recusada por regra de negócio.
func TransactionRejected(s wager.Snapshot, m Meta) (Event, error) {
	return transactionEvent(TypeWagerTransactionRejected, wager.StatusRejected, s, m)
}

// TransactionPendingReference: a transação aguarda a transação referenciada.
func TransactionPendingReference(s wager.Snapshot, m Meta) (Event, error) {
	return transactionEvent(TypeWagerTransactionPendingReference, wager.StatusPendingReference, s, m)
}

// WalletBalanceChanged: o saldo de uma carteira mudou (um por lançamento).
func WalletBalanceChanged(walletID, transactionID uuid.UUID, mv wallet.Movement, walletVersion int64, m Meta) (Event, error) {
	data := balanceData{
		WalletID:      walletID.String(),
		TransactionID: transactionID.String(),
		Direction:     string(mv.Direction),
		Money:         moneyOf(mv.Amount),
		BalanceBefore: moneyOf(mv.BalanceBefore),
		BalanceAfter:  moneyOf(mv.BalanceAfter),
		WalletVersion: walletVersion,
	}
	return build(TypeWalletBalanceChanged, AggregateWallet, walletID, data, m)
}

func transactionEvent(t Type, want wager.Status, s wager.Snapshot, m Meta) (Event, error) {
	if s.Status != want {
		return Event{}, fmt.Errorf("%w: %s requires a %s transaction, got %s", ErrInvalidEvent, t, want, s.Status)
	}

	data := transactionData{
		TransactionID:                  s.ID.String(),
		WalletID:                       s.WalletID.String(),
		PlayerID:                       s.PlayerID.String(),
		ProviderID:                     s.ProviderID,
		ExternalTransactionID:          s.ExternalID,
		Type:                           string(s.Kind),
		Status:                         string(s.Status),
		Amount:                         moneyOf(s.Amount),
		FailureCode:                    string(s.FailureCode),
		ReferenceExternalTransactionID: s.ReferenceExternalID,
	}
	if s.ResultBalance.IsInitialized() {
		balance := moneyOf(s.ResultBalance)
		data.Balance = &balance
	}
	if s.Status == wager.StatusPendingReference {
		data.Attempts = s.Attempts
		next := s.NextAttemptAt.UTC()
		data.NextAttemptAt = &next
	}
	return build(t, AggregateWagerTransaction, s.ID, data, m)
}

func build(t Type, aggregateType string, aggregateID uuid.UUID, data any, m Meta) (Event, error) {
	if m.ID == uuid.Nil {
		return Event{}, fmt.Errorf("%w: event id is required", ErrInvalidEvent)
	}
	if m.CorrelationID == "" {
		return Event{}, fmt.Errorf("%w: correlation id is required", ErrInvalidEvent)
	}
	if m.OccurredAt.IsZero() {
		return Event{}, fmt.Errorf("%w: occurredAt is required", ErrInvalidEvent)
	}
	payload, err := json.Marshal(data)
	if err != nil {
		return Event{}, fmt.Errorf("event: marshaling %s: %w", t, err)
	}
	return Event{
		ID:            m.ID,
		Type:          t,
		Version:       currentVersion,
		AggregateType: aggregateType,
		AggregateID:   aggregateID,
		CorrelationID: m.CorrelationID,
		CausationID:   m.CausationID,
		OccurredAt:    m.OccurredAt.UTC(),
		Payload:       payload,
	}, nil
}
