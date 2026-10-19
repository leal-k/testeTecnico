package wallet

import (
	"errors"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/leal-k/testeTecnico/internal/domain/money"
)

var t0 = time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)

func brl(t *testing.T, amount string) money.Money {
	t.Helper()
	m, err := money.Parse(amount, "BRL")
	if err != nil {
		t.Fatalf("Parse(%q): %v", amount, err)
	}
	return m
}

func newWallet(t *testing.T, balance string) *Wallet {
	t.Helper()
	w, _, err := Open(uuid.New(), uuid.New(), brl(t, balance), t0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return w
}

// assertUnchanged confere que uma operação que falhou não mexeu na carteira.
func assertUnchanged(t *testing.T, w *Wallet, balance string, version int64, updatedAt time.Time) {
	t.Helper()
	if got := w.Balance().Amount(); got != balance {
		t.Errorf("balance = %s, want %s", got, balance)
	}
	if w.Version() != version {
		t.Errorf("version = %d, want %d", w.Version(), version)
	}
	if !w.UpdatedAt().Equal(updatedAt) {
		t.Errorf("updatedAt = %v, want %v", w.UpdatedAt(), updatedAt)
	}
}

func TestOpen_PositiveBalanceReturnsOpeningMovement(t *testing.T) {
	w, mv, err := Open(uuid.New(), uuid.New(), brl(t, "100.00"), t0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if w.Balance().Amount() != "100.00" || w.Version() != 1 {
		t.Errorf("wallet = %s v%d, want 100.00 v1", w.Balance().Amount(), w.Version())
	}
	if !w.CreatedAt().Equal(t0) || !w.UpdatedAt().Equal(t0) {
		t.Errorf("timestamps not set from now")
	}
	if mv == nil {
		t.Fatal("expected an opening movement")
	}
	if mv.Direction != DirectionCredit || mv.Amount.Amount() != "100.00" ||
		mv.BalanceBefore.Amount() != "0.00" || mv.BalanceAfter.Amount() != "100.00" {
		t.Errorf("movement = %+v", *mv)
	}
}

func TestOpen_ZeroBalanceHasNoMovement(t *testing.T) {
	w, mv, err := Open(uuid.New(), uuid.New(), brl(t, "0.00"), t0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if mv != nil {
		t.Errorf("zero balance must not produce a movement, got %+v", *mv)
	}
	if w.Version() != 1 {
		t.Errorf("version = %d, want 1", w.Version())
	}
}

func TestOpen_Invalid(t *testing.T) {
	neg, err := money.FromMinor(-1, money.BRL)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name     string
		id, play uuid.UUID
		initial  money.Money
		want     error
	}{
		{"nil wallet id", uuid.Nil, uuid.New(), brl(t, "1.00"), ErrInvalidID},
		{"nil player id", uuid.New(), uuid.Nil, brl(t, "1.00"), ErrInvalidID},
		{"negative balance", uuid.New(), uuid.New(), neg, ErrNegativeBalance},
		{"uninitialized balance", uuid.New(), uuid.New(), money.Money{}, money.ErrUninitialized},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w, mv, err := Open(tc.id, tc.play, tc.initial, t0)
			if !errors.Is(err, tc.want) {
				t.Errorf("error = %v, want %v", err, tc.want)
			}
			if w != nil || mv != nil {
				t.Errorf("must not return a wallet or movement on error")
			}
		})
	}
}

func TestRehydrate_KeepsStateAndDoesNotApplyAnything(t *testing.T) {
	id, player := uuid.New(), uuid.New()
	created := t0.Add(-time.Hour)
	w, err := Rehydrate(id, player, brl(t, "20.00"), 7, created, t0)
	if err != nil {
		t.Fatalf("Rehydrate: %v", err)
	}
	if w.ID() != id || w.PlayerID() != player {
		t.Errorf("ids not preserved")
	}
	if w.Balance().Amount() != "20.00" || w.Version() != 7 {
		t.Errorf("state = %s v%d, want 20.00 v7", w.Balance().Amount(), w.Version())
	}
	if !w.CreatedAt().Equal(created) || !w.UpdatedAt().Equal(t0) {
		t.Errorf("timestamps not preserved")
	}
}

func TestRehydrate_Invalid(t *testing.T) {
	neg, _ := money.FromMinor(-1, money.BRL)
	ok := brl(t, "1.00")

	tests := []struct {
		name    string
		id      uuid.UUID
		balance money.Money
		version int64
		want    error
	}{
		{"nil id", uuid.Nil, ok, 1, ErrInvalidID},
		{"negative balance", uuid.New(), neg, 1, ErrNegativeBalance},
		{"uninitialized balance", uuid.New(), money.Money{}, 1, money.ErrUninitialized},
		{"version zero", uuid.New(), ok, 0, ErrInvalidVersion},
		{"negative version", uuid.New(), ok, -3, ErrInvalidVersion},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Rehydrate(tc.id, uuid.New(), tc.balance, tc.version, t0, t0)
			if !errors.Is(err, tc.want) {
				t.Errorf("error = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestDebit_Success(t *testing.T) {
	w := newWallet(t, "100.00")
	later := t0.Add(time.Minute)

	mv, err := w.Debit(brl(t, "30.00"), later)
	if err != nil {
		t.Fatalf("Debit: %v", err)
	}
	if w.Balance().Amount() != "70.00" || w.Version() != 2 || !w.UpdatedAt().Equal(later) {
		t.Errorf("wallet = %s v%d at %v", w.Balance().Amount(), w.Version(), w.UpdatedAt())
	}
	if mv.Direction != DirectionDebit || mv.Amount.Amount() != "30.00" ||
		mv.BalanceBefore.Amount() != "100.00" || mv.BalanceAfter.Amount() != "70.00" {
		t.Errorf("movement = %+v", mv)
	}
}

func TestDebit_ExactBalanceGoesToZero(t *testing.T) {
	w := newWallet(t, "80.00")
	if _, err := w.Debit(brl(t, "80.00"), t0); err != nil {
		t.Fatalf("Debit: %v", err)
	}
	if !w.Balance().IsZero() {
		t.Errorf("balance = %s, want 0.00", w.Balance().Amount())
	}
}

func TestDebit_Failures_LeaveWalletUntouched(t *testing.T) {
	usd, err := money.Parse("1.00", "USD")
	if err != nil {
		t.Fatal(err)
	}
	zero := brl(t, "0.00")
	neg, _ := money.FromMinor(-100, money.BRL)

	tests := []struct {
		name   string
		amount money.Money
		want   error
	}{
		{"insufficient funds", brl(t, "100.01"), ErrInsufficientFunds},
		{"zero amount", zero, ErrNonPositiveAmount},
		{"negative amount", neg, ErrNonPositiveAmount},
		{"currency mismatch", usd, money.ErrCurrencyMismatch},
		{"uninitialized amount", money.Money{}, money.ErrUninitialized},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := newWallet(t, "100.00")
			_, err := w.Debit(tc.amount, t0.Add(time.Hour))
			if !errors.Is(err, tc.want) {
				t.Errorf("error = %v, want %v", err, tc.want)
			}
			assertUnchanged(t, w, "100.00", 1, t0)
		})
	}
}

func TestCredit_Success(t *testing.T) {
	w := newWallet(t, "10.00")
	later := t0.Add(time.Minute)

	mv, err := w.Credit(brl(t, "5.50"), later)
	if err != nil {
		t.Fatalf("Credit: %v", err)
	}
	if w.Balance().Amount() != "15.50" || w.Version() != 2 || !w.UpdatedAt().Equal(later) {
		t.Errorf("wallet = %s v%d at %v", w.Balance().Amount(), w.Version(), w.UpdatedAt())
	}
	if mv.Direction != DirectionCredit || mv.BalanceBefore.Amount() != "10.00" || mv.BalanceAfter.Amount() != "15.50" {
		t.Errorf("movement = %+v", mv)
	}
}

func TestCredit_Failures_LeaveWalletUntouched(t *testing.T) {
	usd, _ := money.Parse("1.00", "USD")

	tests := []struct {
		name   string
		amount money.Money
		want   error
	}{
		{"zero amount", brl(t, "0.00"), ErrNonPositiveAmount},
		{"currency mismatch", usd, money.ErrCurrencyMismatch},
		{"uninitialized amount", money.Money{}, money.ErrUninitialized},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := newWallet(t, "10.00")
			_, err := w.Credit(tc.amount, t0.Add(time.Hour))
			if !errors.Is(err, tc.want) {
				t.Errorf("error = %v, want %v", err, tc.want)
			}
			assertUnchanged(t, w, "10.00", 1, t0)
		})
	}
}

func TestCredit_OverflowLeavesWalletUntouched(t *testing.T) {
	max, err := money.FromMinor(math.MaxInt64, money.BRL)
	if err != nil {
		t.Fatal(err)
	}
	w, _, err := Open(uuid.New(), uuid.New(), max, t0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	_, err = w.Credit(brl(t, "0.01"), t0.Add(time.Hour))
	if !errors.Is(err, money.ErrOverflow) {
		t.Errorf("error = %v, want ErrOverflow", err)
	}
	if w.Balance().Minor() != math.MaxInt64 || w.Version() != 1 {
		t.Errorf("wallet changed after overflow: %s v%d", w.Balance().Amount(), w.Version())
	}
}

// Cenário do README, no nível do domínio: 100.00 com duas apostas de 80.00.
func TestTwoBetsOfEightyOnHundred(t *testing.T) {
	w := newWallet(t, "100.00")

	if _, err := w.Debit(brl(t, "80.00"), t0); err != nil {
		t.Fatalf("first debit: %v", err)
	}
	if _, err := w.Debit(brl(t, "80.00"), t0); !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("second debit error = %v, want ErrInsufficientFunds", err)
	}
	if w.Balance().Amount() != "20.00" {
		t.Errorf("final balance = %s, want 20.00", w.Balance().Amount())
	}
	if w.Version() != 2 {
		t.Errorf("version = %d, want 2 (only the successful debit counts)", w.Version())
	}
}

// Varre uma sequência pseudoaleatória (semente fixa, reproduzível) de débitos
// e créditos e confere as invariantes a cada passo.
func TestInvariantsHoldOverRandomSequence(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	w := newWallet(t, "500.00")
	expectedVersion := int64(1)

	for i := 0; i < 5000; i++ {
		amount, err := money.FromMinor(int64(rng.Intn(30000)+1), money.BRL)
		if err != nil {
			t.Fatal(err)
		}
		before := w.Balance()

		var mv Movement
		var opErr error
		if rng.Intn(2) == 0 {
			mv, opErr = w.Debit(amount, t0)
		} else {
			mv, opErr = w.Credit(amount, t0)
		}

		if w.Balance().IsNegative() {
			t.Fatalf("step %d: balance went negative: %s", i, w.Balance())
		}
		if opErr != nil {
			if !errors.Is(opErr, ErrInsufficientFunds) {
				t.Fatalf("step %d: unexpected error: %v", i, opErr)
			}
			if w.Balance() != before || w.Version() != expectedVersion {
				t.Fatalf("step %d: failed operation changed the wallet", i)
			}
			continue
		}

		expectedVersion++
		if w.Version() != expectedVersion {
			t.Fatalf("step %d: version = %d, want %d", i, w.Version(), expectedVersion)
		}
		if mv.BalanceBefore != before || mv.BalanceAfter != w.Balance() {
			t.Fatalf("step %d: movement does not match the balances", i)
		}
		var check money.Money
		if mv.Direction == DirectionDebit {
			check, _ = mv.BalanceBefore.Sub(mv.Amount)
		} else {
			check, _ = mv.BalanceBefore.Add(mv.Amount)
		}
		if check != mv.BalanceAfter {
			t.Fatalf("step %d: after != before ± amount", i)
		}
	}
}
