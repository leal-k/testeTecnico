package ledger

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/leal-k/testeTecnico/internal/domain/money"
	"github.com/leal-k/testeTecnico/internal/domain/wallet"
)

var now0 = time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)

func brl(t *testing.T, amount string) money.Money {
	t.Helper()
	m, err := money.Parse(amount, "BRL")
	if err != nil {
		t.Fatalf("Parse(%q): %v", amount, err)
	}
	return m
}

func TestNew_FromRealWalletMovements(t *testing.T) {
	w, opening, err := wallet.Open(uuid.New(), uuid.New(), brl(t, "100.00"), now0)
	if err != nil {
		t.Fatal(err)
	}
	debit, err := w.Debit(brl(t, "30.00"), now0)
	if err != nil {
		t.Fatal(err)
	}
	credit, err := w.Credit(brl(t, "5.50"), now0)
	if err != nil {
		t.Fatal(err)
	}

	for name, mv := range map[string]wallet.Movement{
		"opening credit": *opening,
		"debit":          debit,
		"credit":         credit,
	} {
		t.Run(name, func(t *testing.T) {
			id, txID := uuid.New(), uuid.New()
			e, err := New(id, w.ID(), txID, mv, now0)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			if e.ID() != id || e.WalletID() != w.ID() || e.TransactionID() != txID {
				t.Errorf("ids not preserved")
			}
			if e.Direction() != mv.Direction || e.Amount() != mv.Amount ||
				e.BalanceBefore() != mv.BalanceBefore || e.BalanceAfter() != mv.BalanceAfter {
				t.Errorf("entry does not mirror the movement: %+v", e)
			}
			if !e.CreatedAt().Equal(now0) {
				t.Errorf("createdAt = %v, want %v", e.CreatedAt(), now0)
			}
		})
	}
}

func TestNew_Invalid(t *testing.T) {
	usd, err := money.Parse("30.00", "USD")
	if err != nil {
		t.Fatal(err)
	}
	negative, err := money.FromMinor(-2000, money.BRL)
	if err != nil {
		t.Fatal(err)
	}
	valid := wallet.Movement{
		Direction:     wallet.DirectionDebit,
		Amount:        brl(t, "30.00"),
		BalanceBefore: brl(t, "100.00"),
		BalanceAfter:  brl(t, "70.00"),
	}

	tests := []struct {
		name   string
		id     uuid.UUID
		mutate func(m *wallet.Movement)
		want   error
	}{
		{"nil entry id", uuid.Nil, func(m *wallet.Movement) {}, ErrInvalidID},
		{"unknown direction", uuid.New(), func(m *wallet.Movement) { m.Direction = "SIDEWAYS" }, ErrInvalidEntry},
		{"zero amount", uuid.New(), func(m *wallet.Movement) { m.Amount = brl(t, "0.00") }, ErrInvalidEntry},
		{"wrong arithmetic", uuid.New(), func(m *wallet.Movement) { m.BalanceAfter = brl(t, "80.00") }, ErrInvalidEntry},
		{"uninitialized before", uuid.New(), func(m *wallet.Movement) { m.BalanceBefore = money.Money{} }, ErrInvalidEntry},
		{"negative after", uuid.New(), func(m *wallet.Movement) {
			m.BalanceBefore = brl(t, "10.00")
			m.BalanceAfter = negative
		}, ErrInvalidEntry},
		{"currency mismatch", uuid.New(), func(m *wallet.Movement) { m.Amount = usd }, ErrInvalidEntry},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mv := valid
			tc.mutate(&mv)
			if _, err := New(tc.id, uuid.New(), uuid.New(), mv, now0); !errors.Is(err, tc.want) {
				t.Errorf("error = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestNew_NilWalletOrTransactionID(t *testing.T) {
	mv := wallet.Movement{
		Direction:     wallet.DirectionCredit,
		Amount:        brl(t, "1.00"),
		BalanceBefore: brl(t, "0.00"),
		BalanceAfter:  brl(t, "1.00"),
	}
	if _, err := New(uuid.New(), uuid.Nil, uuid.New(), mv, now0); !errors.Is(err, ErrInvalidID) {
		t.Errorf("nil wallet id error = %v, want ErrInvalidID", err)
	}
	if _, err := New(uuid.New(), uuid.New(), uuid.Nil, mv, now0); !errors.Is(err, ErrInvalidID) {
		t.Errorf("nil transaction id error = %v, want ErrInvalidID", err)
	}
}
