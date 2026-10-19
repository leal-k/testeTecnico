// Package wallet implementa o agregado Wallet: o saldo de um jogador em uma
// moeda e as regras de débito e crédito.
//
// Invariantes mantidas por este tipo (e repetidas como constraints no banco):
//   - o saldo nunca é negativo;
//   - a moeda da movimentação é a moeda da carteira;
//   - a versão começa em 1 e sobe exatamente +1 a cada mudança de saldo.
//
// Wallet não é seguro para uso concorrente. A coordenação entre requisições
// simultâneas é responsabilidade do repositório (lock da linha da carteira no
// PostgreSQL); a versão serve de conferência adicional.
package wallet

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/leal-k/testeTecnico/internal/domain/money"
)

var (
	ErrInvalidID         = errors.New("wallet: invalid id")
	ErrNegativeBalance   = errors.New("wallet: balance cannot be negative")
	ErrInvalidVersion    = errors.New("wallet: version must be at least 1")
	ErrNonPositiveAmount = errors.New("wallet: movement amount must be positive")
	ErrInsufficientFunds = errors.New("wallet: insufficient funds")
)

// Direction indica o sentido de uma movimentação de saldo.
type Direction string

const (
	DirectionDebit  Direction = "DEBIT"
	DirectionCredit Direction = "CREDIT"
)

// Movement descreve uma mudança de saldo já aplicada à carteira. É o que o
// caso de uso transforma em lançamento do ledger, na mesma transação.
type Movement struct {
	Direction     Direction
	Amount        money.Money
	BalanceBefore money.Money
	BalanceAfter  money.Money
}

// Wallet é a raiz do agregado.
type Wallet struct {
	id        uuid.UUID
	playerID  uuid.UUID
	balance   money.Money
	version   int64
	createdAt time.Time
	updatedAt time.Time
}

// Open cria uma carteira nova com o saldo inicial informado (zero ou
// positivo). Quando o saldo inicial é positivo, devolve também a movimentação
// de crédito inicial (antes = 0), que deve virar o primeiro lançamento do
// ledger; com saldo zero não há movimentação.
func Open(id, playerID uuid.UUID, initial money.Money, now time.Time) (*Wallet, *Movement, error) {
	if id == uuid.Nil || playerID == uuid.Nil {
		return nil, nil, ErrInvalidID
	}
	if !initial.IsInitialized() {
		return nil, nil, money.ErrUninitialized
	}
	if initial.IsNegative() {
		return nil, nil, ErrNegativeBalance
	}

	w := &Wallet{
		id:        id,
		playerID:  playerID,
		balance:   initial,
		version:   1,
		createdAt: now,
		updatedAt: now,
	}
	if initial.IsZero() {
		return w, nil, nil
	}

	zero, err := money.Zero(initial.Currency())
	if err != nil {
		return nil, nil, err
	}
	return w, &Movement{
		Direction:     DirectionCredit,
		Amount:        initial,
		BalanceBefore: zero,
		BalanceAfter:  initial,
	}, nil
}

// Rehydrate reconstrói uma carteira a partir do que está persistido. Valida
// as invariantes, mas não reaplica movimentações nem gera efeitos.
func Rehydrate(id, playerID uuid.UUID, balance money.Money, version int64, createdAt, updatedAt time.Time) (*Wallet, error) {
	if id == uuid.Nil || playerID == uuid.Nil {
		return nil, ErrInvalidID
	}
	if !balance.IsInitialized() {
		return nil, money.ErrUninitialized
	}
	if balance.IsNegative() {
		return nil, ErrNegativeBalance
	}
	if version < 1 {
		return nil, ErrInvalidVersion
	}
	return &Wallet{
		id:        id,
		playerID:  playerID,
		balance:   balance,
		version:   version,
		createdAt: createdAt,
		updatedAt: updatedAt,
	}, nil
}

func (w *Wallet) ID() uuid.UUID {
	return w.id
}

func (w *Wallet) PlayerID() uuid.UUID {
	return w.playerID
}

func (w *Wallet) Balance() money.Money {
	return w.balance
}

func (w *Wallet) Currency() money.Currency {
	return w.balance.Currency()
}

func (w *Wallet) Version() int64 {
	return w.version
}

func (w *Wallet) CreatedAt() time.Time {
	return w.createdAt
}

func (w *Wallet) UpdatedAt() time.Time {
	return w.updatedAt
}

// Debit retira amount do saldo. Falha, sem alterar a carteira, se o valor não
// for positivo, a moeda for diferente ou o saldo for insuficiente.
func (w *Wallet) Debit(amount money.Money, now time.Time) (Movement, error) {
	if err := validateAmount(amount); err != nil {
		return Movement{}, err
	}
	cmp, err := w.balance.Cmp(amount)
	if err != nil {
		return Movement{}, fmt.Errorf("wallet debit: %w", err)
	}
	if cmp < 0 {
		return Movement{}, ErrInsufficientFunds
	}
	after, err := w.balance.Sub(amount)
	if err != nil {
		return Movement{}, fmt.Errorf("wallet debit: %w", err)
	}
	return w.apply(DirectionDebit, amount, after, now), nil
}

// Credit adiciona amount ao saldo. Falha, sem alterar a carteira, se o valor
// não for positivo, a moeda for diferente ou a soma estourar o limite de Money.
func (w *Wallet) Credit(amount money.Money, now time.Time) (Movement, error) {
	if err := validateAmount(amount); err != nil {
		return Movement{}, err
	}
	after, err := w.balance.Add(amount)
	if err != nil {
		return Movement{}, fmt.Errorf("wallet credit: %w", err)
	}
	return w.apply(DirectionCredit, amount, after, now), nil
}

// apply grava o novo saldo; só é chamado depois de todas as validações.
func (w *Wallet) apply(dir Direction, amount, after money.Money, now time.Time) Movement {
	before := w.balance
	w.balance = after
	w.version++
	w.updatedAt = now
	return Movement{
		Direction:     dir,
		Amount:        amount,
		BalanceBefore: before,
		BalanceAfter:  after,
	}
}

func validateAmount(amount money.Money) error {
	if !amount.IsInitialized() {
		return money.ErrUninitialized
	}
	if !amount.IsPositive() {
		return ErrNonPositiveAmount
	}
	return nil
}
