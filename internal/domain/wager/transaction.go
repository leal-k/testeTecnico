// Package wager implementa a transação de aposta (WagerTransaction): seus
// tipos, estados e as regras de cada operação financeira.
//
// Estados:
//
//	PENDING            aceita e persistida, ainda não processada
//	PENDING_REFERENCE  aguardando a transação referenciada (REFUND/ROLLBACK/WIN)
//	PROCESSED          aplicada com sucesso (terminal)
//	REJECTED           recusada por regra de negócio, definitivamente (terminal)
//	FAILED             falha técnica permanente (terminal)
//
// Estados terminais nunca mudam. Uma transação REFUND ou ROLLBACK só pode ficar
// PROCESSED depois de ter a referência interna resolvida: é essa coluna que o
// índice único do banco usa para impedir duas reversões da mesma transação.
package wager

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/leal-k/testeTecnico/internal/domain/money"
)

var (
	ErrInvalidID           = errors.New("wager: invalid id")
	ErrInvalidKind         = errors.New("wager: invalid kind")
	ErrInvalidStatus       = errors.New("wager: invalid status")
	ErrInvalidAmount       = errors.New("wager: invalid amount")
	ErrInvalidField        = errors.New("wager: invalid field")
	ErrInvalidShape        = errors.New("wager: inconsistent transaction fields")
	ErrTerminalState       = errors.New("wager: transaction is in a terminal state")
	ErrReferenceAlreadySet = errors.New("wager: a different reference is already set")
)

// Kind é o tipo da operação.
type Kind string

const (
	KindOpening  Kind = "OPENING"
	KindBet      Kind = "BET"
	KindWin      Kind = "WIN"
	KindLoss     Kind = "LOSS"
	KindRefund   Kind = "REFUND"
	KindRollback Kind = "ROLLBACK"
)

func (k Kind) IsValid() bool {
	switch k {
	case KindOpening, KindBet, KindWin, KindLoss, KindRefund, KindRollback:
		return true
	}
	return false
}

// IsExternal indica os tipos que chegam por HTTP/SQS. OPENING é só interno.
func (k Kind) IsExternal() bool {
	return k.IsValid() && k != KindOpening
}

// Status é o estado da transação.
type Status string

const (
	StatusPending          Status = "PENDING"
	StatusPendingReference Status = "PENDING_REFERENCE"
	StatusProcessed        Status = "PROCESSED"
	StatusRejected         Status = "REJECTED"
	StatusFailed           Status = "FAILED"
)

func (s Status) IsValid() bool {
	switch s {
	case StatusPending, StatusPendingReference, StatusProcessed, StatusRejected, StatusFailed:
		return true
	}
	return false
}

func (s Status) IsTerminal() bool {
	return s == StatusProcessed || s == StatusRejected || s == StatusFailed
}

// FailureCode é o código estável devolvido ao provedor quando a operação é
// REJECTED ou FAILED. Cada código distingue um motivo diferente.
type FailureCode string

const (
	// BET sem saldo suficiente.
	FailureInsufficientFunds FailureCode = "INSUFFICIENT_FUNDS"
	// REFUND/ROLLBACK que deixaria o saldo negativo (código distinto do de BET).
	FailureReversalInsufficientFunds FailureCode = "REVERSAL_INSUFFICIENT_FUNDS"
	// A referência nunca apareceu dentro do prazo.
	FailureReferenceNotFound FailureCode = "REFERENCE_NOT_FOUND"
	// A referência existe, mas terminou sem sucesso (REJECTED/FAILED) ou
	// continuou pendente até o prazo acabar.
	FailureReferenceNotProcessed FailureCode = "REFERENCE_NOT_PROCESSED"
	// A referência pertence a outro provedor, jogador, carteira, moeda ou rodada.
	FailureReferenceMismatch FailureCode = "REFERENCE_MISMATCH"
	// O tipo referenciado não admite essa operação.
	FailureInvalidReferenceKind FailureCode = "INVALID_REFERENCE_KIND"
	// O valor da reversão difere do valor referenciado (não há reversão parcial).
	FailureReferenceAmountMismatch FailureCode = "REFERENCE_AMOUNT_MISMATCH"
	// A referência já tem uma reversão bem-sucedida.
	FailureAlreadyReversed FailureCode = "ALREADY_REVERSED"
	// A moeda da operação difere da moeda da carteira.
	FailureCurrencyMismatch FailureCode = "CURRENCY_MISMATCH"
	// O crédito ultrapassaria o limite representável do saldo.
	FailureBalanceLimitExceeded FailureCode = "BALANCE_LIMIT_EXCEEDED"
	// Falha técnica permanente (estado FAILED).
	FailureProcessingFailed FailureCode = "PROCESSING_FAILED"
)

const maxFieldLen = 200

// ExternalParams reúne os dados de uma operação recebida de um provedor.
type ExternalParams struct {
	ID                  uuid.UUID
	Kind                Kind
	ProviderID          string
	ExternalID          string
	IdempotencyKey      string
	WalletID            uuid.UUID
	PlayerID            uuid.UUID
	RoundID             string
	GameID              string
	Amount              money.Money
	ReferenceExternalID string
}

// ComputePayloadHash calcula o hash determinístico do conteúdo de negócio da
// operação. É o mesmo para HTTP e SQS e serve para distinguir um replay (mesma
// chave, mesmo conteúdo) de um conflito (mesma chave, conteúdo diferente).
//
// Algoritmo: SHA-256, em hexadecimal, de um JSON de chaves ordenadas com os
// campos de negócio. O valor entra na forma canônica de Money.Amount() (duas
// casas), então "25" e "25.00" geram o mesmo hash. Ficam de fora o ID interno,
// a chave de idempotência e qualquer metadado de transporte.
func ComputePayloadHash(p ExternalParams) (string, error) {
	canonical := map[string]string{
		"amount":              p.Amount.Amount(),
		"currency":            p.Amount.Currency().String(),
		"externalId":          p.ExternalID,
		"gameId":              p.GameID,
		"kind":                string(p.Kind),
		"playerId":            p.PlayerID.String(),
		"providerId":          p.ProviderID,
		"referenceExternalId": p.ReferenceExternalID,
		"roundId":             p.RoundID,
		"walletId":            p.WalletID.String(),
	}
	// json.Marshal ordena as chaves de um map, o que torna a saída canônica.
	raw, err := json.Marshal(canonical)
	if err != nil {
		return "", fmt.Errorf("wager: hashing payload: %w", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

// Snapshot é a fotografia completa do estado de uma transação, usada para
// persistir e para reidratar. Campos opcionais usam o valor zero quando
// ausentes (string vazia, uuid.Nil, money.Money{}, time.Time{}).
type Snapshot struct {
	ID                  uuid.UUID
	Kind                Kind
	Status              Status
	ProviderID          string
	ExternalID          string
	IdempotencyKey      string
	PayloadHash         string
	RoundID             string
	GameID              string
	ReferenceExternalID string
	WalletID            uuid.UUID
	PlayerID            uuid.UUID
	Amount              money.Money
	ReferenceID         uuid.UUID
	FailureCode         FailureCode
	ResultBalance       money.Money
	Attempts            int
	NextAttemptAt       time.Time
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

// Transaction é a entidade; o estado só muda pelos métodos de transição.
type Transaction struct {
	data Snapshot
}

// NewExternal cria uma transação recebida de um provedor, no estado PENDING.
func NewExternal(p ExternalParams, now time.Time) (*Transaction, error) {
	if !p.Kind.IsExternal() {
		return nil, fmt.Errorf("%w: %q", ErrInvalidKind, p.Kind)
	}
	hash, err := ComputePayloadHash(p)
	if err != nil {
		return nil, err
	}
	s := Snapshot{
		ID:                  p.ID,
		Kind:                p.Kind,
		Status:              StatusPending,
		ProviderID:          p.ProviderID,
		ExternalID:          p.ExternalID,
		IdempotencyKey:      p.IdempotencyKey,
		PayloadHash:         hash,
		RoundID:             p.RoundID,
		GameID:              p.GameID,
		ReferenceExternalID: p.ReferenceExternalID,
		WalletID:            p.WalletID,
		PlayerID:            p.PlayerID,
		Amount:              p.Amount,
		NextAttemptAt:       now,
		CreatedAt:           now,
		UpdatedAt:           now,
	}
	if err := validate(s); err != nil {
		return nil, err
	}
	return &Transaction{data: s}, nil
}

// NewOpening cria o crédito inicial de uma carteira. É interna (sem provedor,
// chave ou referência) e já nasce PROCESSED, com o saldo resultante igual ao
// valor creditado.
func NewOpening(id, walletID, playerID uuid.UUID, amount money.Money, now time.Time) (*Transaction, error) {
	s := Snapshot{
		ID:            id,
		Kind:          KindOpening,
		Status:        StatusProcessed,
		WalletID:      walletID,
		PlayerID:      playerID,
		Amount:        amount,
		ResultBalance: amount,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	if err := validate(s); err != nil {
		return nil, err
	}
	return &Transaction{data: s}, nil
}

// Rehydrate reconstrói uma transação persistida. Valida o estado, mas não
// aplica nenhuma regra de negócio nem gera efeitos.
func Rehydrate(s Snapshot) (*Transaction, error) {
	if err := validate(s); err != nil {
		return nil, err
	}
	return &Transaction{data: s}, nil
}

func (t *Transaction) Snapshot() Snapshot { return t.data }

func (t *Transaction) ID() uuid.UUID               { return t.data.ID }
func (t *Transaction) Kind() Kind                  { return t.data.Kind }
func (t *Transaction) Status() Status              { return t.data.Status }
func (t *Transaction) Amount() money.Money         { return t.data.Amount }
func (t *Transaction) ReferenceExternalID() string { return t.data.ReferenceExternalID }
func (t *Transaction) ReferenceID() uuid.UUID      { return t.data.ReferenceID }
func (t *Transaction) IsInternal() bool            { return t.data.Kind == KindOpening }

// MarkPendingReference registra que a referência ainda não está disponível e
// agenda a próxima tentativa. Pode ser chamado várias vezes (cada chamada conta
// uma tentativa).
func (t *Transaction) MarkPendingReference(nextAttemptAt, now time.Time) error {
	return t.change(func(s *Snapshot) error {
		if s.ReferenceExternalID == "" {
			return fmt.Errorf("%w: transaction has no reference", ErrInvalidShape)
		}
		if nextAttemptAt.IsZero() {
			return fmt.Errorf("%w: next attempt time is required", ErrInvalidField)
		}
		s.Status = StatusPendingReference
		s.Attempts++
		s.NextAttemptAt = nextAttemptAt
		s.UpdatedAt = now
		return nil
	})
}

// SetReference grava o ID interno da transação referenciada. É idempotente para
// o mesmo ID e falha se já houver uma referência diferente.
func (t *Transaction) SetReference(id uuid.UUID, now time.Time) error {
	return t.change(func(s *Snapshot) error {
		if id == uuid.Nil {
			return ErrInvalidID
		}
		if s.ReferenceID != uuid.Nil && s.ReferenceID != id {
			return ErrReferenceAlreadySet
		}
		s.ReferenceID = id
		s.UpdatedAt = now
		return nil
	})
}

// Process conclui a transação com sucesso, guardando o saldo resultante (é o
// que um replay devolve ao provedor).
func (t *Transaction) Process(resultBalance money.Money, now time.Time) error {
	return t.change(func(s *Snapshot) error {
		s.Status = StatusProcessed
		s.ResultBalance = resultBalance
		s.NextAttemptAt = time.Time{}
		s.UpdatedAt = now
		return nil
	})
}

// Reject conclui a transação como recusada por regra de negócio. observed é o
// saldo da carteira no momento (pode ser money.Money{} se não se aplica).
func (t *Transaction) Reject(code FailureCode, observed money.Money, now time.Time) error {
	return t.change(func(s *Snapshot) error {
		s.Status = StatusRejected
		s.FailureCode = code
		s.ResultBalance = observed
		s.NextAttemptAt = time.Time{}
		s.UpdatedAt = now
		return nil
	})
}

// Fail conclui a transação como falha técnica permanente.
func (t *Transaction) Fail(code FailureCode, now time.Time) error {
	return t.change(func(s *Snapshot) error {
		s.Status = StatusFailed
		s.FailureCode = code
		s.NextAttemptAt = time.Time{}
		s.UpdatedAt = now
		return nil
	})
}

// change aplica uma transição sobre uma cópia, valida o resultado e só então o
// adota. Assim uma transição inválida nunca deixa a transação pela metade, e
// estados terminais são imutáveis.
func (t *Transaction) change(mutate func(s *Snapshot) error) error {
	if t.data.Status.IsTerminal() {
		return fmt.Errorf("%w: %s", ErrTerminalState, t.data.Status)
	}
	next := t.data
	if err := mutate(&next); err != nil {
		return err
	}
	if err := validate(next); err != nil {
		return err
	}
	t.data = next
	return nil
}

func validate(s Snapshot) error {
	if s.ID == uuid.Nil || s.WalletID == uuid.Nil || s.PlayerID == uuid.Nil {
		return ErrInvalidID
	}
	if !s.Kind.IsValid() {
		return fmt.Errorf("%w: %q", ErrInvalidKind, s.Kind)
	}
	if !s.Status.IsValid() {
		return fmt.Errorf("%w: %q", ErrInvalidStatus, s.Status)
	}
	if err := validateAmount(s.Kind, s.Amount); err != nil {
		return err
	}
	if s.Kind == KindOpening {
		if err := validateInternal(s); err != nil {
			return err
		}
	} else if err := validateExternal(s); err != nil {
		return err
	}
	return validateState(s)
}

// LOSS exige valor zero; todos os demais exigem valor positivo.
func validateAmount(kind Kind, amount money.Money) error {
	if !amount.IsInitialized() {
		return fmt.Errorf("%w: amount not initialized", ErrInvalidAmount)
	}
	if kind == KindLoss {
		if !amount.IsZero() {
			return fmt.Errorf("%w: LOSS requires a zero amount", ErrInvalidAmount)
		}
		return nil
	}
	if !amount.IsPositive() {
		return fmt.Errorf("%w: %s requires a positive amount", ErrInvalidAmount, kind)
	}
	return nil
}

func validateInternal(s Snapshot) error {
	if s.ProviderID != "" || s.ExternalID != "" || s.IdempotencyKey != "" ||
		s.PayloadHash != "" || s.RoundID != "" || s.GameID != "" ||
		s.ReferenceExternalID != "" || s.ReferenceID != uuid.Nil {
		return fmt.Errorf("%w: OPENING cannot carry external fields", ErrInvalidShape)
	}
	if s.Status != StatusProcessed {
		return fmt.Errorf("%w: OPENING must be PROCESSED", ErrInvalidShape)
	}
	return nil
}

func validateExternal(s Snapshot) error {
	required := []struct{ name, value string }{
		{"providerId", s.ProviderID},
		{"externalId", s.ExternalID},
		{"idempotencyKey", s.IdempotencyKey},
		{"payloadHash", s.PayloadHash},
		{"roundId", s.RoundID},
		{"gameId", s.GameID},
	}
	for _, f := range required {
		if err := validateText(f.name, f.value); err != nil {
			return err
		}
	}

	hasRef := s.ReferenceExternalID != ""
	switch s.Kind {
	case KindBet:
		if hasRef {
			return fmt.Errorf("%w: BET cannot reference another transaction", ErrInvalidShape)
		}
	case KindRefund, KindRollback:
		if !hasRef {
			return fmt.Errorf("%w: %s requires a reference", ErrInvalidShape, s.Kind)
		}
	}
	if hasRef {
		if err := validateText("referenceExternalId", s.ReferenceExternalID); err != nil {
			return err
		}
	}
	if s.ReferenceID != uuid.Nil && !hasRef {
		return fmt.Errorf("%w: internal reference without an external reference", ErrInvalidShape)
	}
	return nil
}

func validateState(s Snapshot) error {
	failed := s.Status == StatusRejected || s.Status == StatusFailed
	if failed && s.FailureCode == "" {
		return fmt.Errorf("%w: %s requires a failure code", ErrInvalidShape, s.Status)
	}
	if !failed && s.FailureCode != "" {
		return fmt.Errorf("%w: failure code only applies to REJECTED or FAILED", ErrInvalidShape)
	}

	if s.ResultBalance.IsInitialized() {
		if s.ResultBalance.IsNegative() || s.ResultBalance.Currency() != s.Amount.Currency() {
			return fmt.Errorf("%w: result balance must be non-negative and in the transaction currency", ErrInvalidAmount)
		}
	} else if s.Status == StatusProcessed {
		return fmt.Errorf("%w: PROCESSED requires a result balance", ErrInvalidShape)
	}

	if s.Status == StatusProcessed && (s.Kind == KindRefund || s.Kind == KindRollback) && s.ReferenceID == uuid.Nil {
		return fmt.Errorf("%w: a processed %s requires the resolved reference", ErrInvalidShape, s.Kind)
	}
	if s.Attempts < 0 {
		return fmt.Errorf("%w: negative attempts", ErrInvalidShape)
	}
	if s.Status.IsTerminal() && !s.NextAttemptAt.IsZero() {
		return fmt.Errorf("%w: terminal transaction cannot have a next attempt", ErrInvalidShape)
	}
	return nil
}

func validateText(name, value string) error {
	if value == "" || len(value) > maxFieldLen {
		return fmt.Errorf("%w: %s must have between 1 and %d bytes", ErrInvalidField, name, maxFieldLen)
	}
	return nil
}
