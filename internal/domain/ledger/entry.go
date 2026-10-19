// Package ledger implementa o lançamento imutável do livro-razão da carteira.
// Cada mudança de saldo gera exatamente um lançamento, gravado na mesma
// transação de banco que o novo saldo.
package ledger

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/leal-k/testeTecnico/internal/domain/money"
	"github.com/leal-k/testeTecnico/internal/domain/wallet"
)

var (
	ErrInvalidID    = errors.New("ledger: invalid id")
	ErrInvalidEntry = errors.New("ledger: invalid entry")
)

// Entry é um lançamento. Não há métodos de alteração: correções são novos
// lançamentos.
type Entry struct {
	id            uuid.UUID
	walletID      uuid.UUID
	transactionID uuid.UUID
	direction     wallet.Direction
	amount        money.Money
	balanceBefore money.Money
	balanceAfter  money.Money
	createdAt     time.Time
}

// New cria o lançamento correspondente a uma movimentação da carteira.
func New(id, walletID, transactionID uuid.UUID, mv wallet.Movement, now time.Time) (Entry, error) {
	return Rehydrate(id, walletID, transactionID, mv.Direction, mv.Amount, mv.BalanceBefore, mv.BalanceAfter, now)
}

// Rehydrate reconstrói um lançamento persistido, validando as mesmas regras.
func Rehydrate(
	id, walletID, transactionID uuid.UUID,
	direction wallet.Direction,
	amount, before, after money.Money,
	createdAt time.Time,
) (Entry, error) {
	if id == uuid.Nil || walletID == uuid.Nil || transactionID == uuid.Nil {
		return Entry{}, ErrInvalidID
	}
	if direction != wallet.DirectionDebit && direction != wallet.DirectionCredit {
		return Entry{}, fmt.Errorf("%w: direction %q", ErrInvalidEntry, direction)
	}
	if !amount.IsPositive() {
		return Entry{}, fmt.Errorf("%w: amount must be positive", ErrInvalidEntry)
	}
	if !before.IsInitialized() || !after.IsInitialized() {
		return Entry{}, fmt.Errorf("%w: balances must be initialized", ErrInvalidEntry)
	}
	if before.IsNegative() || after.IsNegative() {
		return Entry{}, fmt.Errorf("%w: balances cannot be negative", ErrInvalidEntry)
	}

	var expected money.Money
	var err error
	if direction == wallet.DirectionCredit {
		expected, err = before.Add(amount)
	} else {
		expected, err = before.Sub(amount)
	}
	if err != nil {
		return Entry{}, fmt.Errorf("%w: %w", ErrInvalidEntry, err)
	}
	if expected != after {
		return Entry{}, fmt.Errorf("%w: balance after must equal balance before %s amount", ErrInvalidEntry, sign(direction))
	}

	return Entry{
		id:            id,
		walletID:      walletID,
		transactionID: transactionID,
		direction:     direction,
		amount:        amount,
		balanceBefore: before,
		balanceAfter:  after,
		createdAt:     createdAt,
	}, nil
}

func sign(d wallet.Direction) string {
	if d == wallet.DirectionCredit {
		return "+"
	}
	return "-"
}

func (e Entry) ID() uuid.UUID {
	return e.id
}

func (e Entry) WalletID() uuid.UUID {
	return e.walletID
}

func (e Entry) TransactionID() uuid.UUID {
	return e.transactionID
}

func (e Entry) Direction() wallet.Direction {
	return e.direction
}

func (e Entry) Amount() money.Money {
	return e.amount
}

func (e Entry) BalanceBefore() money.Money {
	return e.balanceBefore
}

func (e Entry) BalanceAfter() money.Money {
	return e.balanceAfter
}

func (e Entry) CreatedAt() time.Time {
	return e.createdAt
}
