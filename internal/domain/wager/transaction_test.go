package wager

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/leal-k/testeTecnico/internal/domain/money"
)

var (
	now0    = time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	retryAt = now0.Add(30 * time.Second)
)

func brl(t *testing.T, amount string) money.Money {
	t.Helper()
	m, err := money.Parse(amount, "BRL")
	if err != nil {
		t.Fatalf("Parse(%q): %v", amount, err)
	}
	return m
}

func baseParams(t *testing.T, kind Kind, amount string) ExternalParams {
	t.Helper()
	p := ExternalParams{
		ID:             uuid.New(),
		Kind:           kind,
		ProviderID:     "provider-a",
		ExternalID:     "ext-1",
		IdempotencyKey: "idem-1",
		WalletID:       uuid.New(),
		PlayerID:       uuid.New(),
		RoundID:        "round-1",
		GameID:         "game-1",
		Amount:         brl(t, amount),
	}
	if kind == KindRefund || kind == KindRollback {
		p.ReferenceExternalID = "ext-0"
	}
	return p
}

func newTx(t *testing.T, kind Kind, amount string) *Transaction {
	t.Helper()
	tx, err := NewExternal(baseParams(t, kind, amount), now0)
	if err != nil {
		t.Fatalf("NewExternal(%s): %v", kind, err)
	}
	return tx
}

func hashOf(t *testing.T, p ExternalParams) string {
	t.Helper()
	h, err := ComputePayloadHash(p)
	if err != nil {
		t.Fatalf("ComputePayloadHash: %v", err)
	}
	return h
}

func TestNewExternal_ValidKinds(t *testing.T) {
	tests := []struct {
		kind   Kind
		amount string
	}{
		{KindBet, "10.00"},
		{KindWin, "5.00"},
		{KindLoss, "0.00"},
		{KindRefund, "10.00"},
		{KindRollback, "10.00"},
	}
	for _, tc := range tests {
		t.Run(string(tc.kind), func(t *testing.T) {
			tx := newTx(t, tc.kind, tc.amount)
			s := tx.Snapshot()
			if s.Status != StatusPending || s.Attempts != 0 || !s.NextAttemptAt.Equal(now0) {
				t.Errorf("initial state = %s attempts=%d next=%v", s.Status, s.Attempts, s.NextAttemptAt)
			}
			if len(s.PayloadHash) != 64 {
				t.Errorf("payload hash length = %d, want 64", len(s.PayloadHash))
			}
			if !s.CreatedAt.Equal(now0) || !s.UpdatedAt.Equal(now0) {
				t.Errorf("timestamps not set from now")
			}
			if tx.IsInternal() {
				t.Errorf("external transaction reported as internal")
			}
		})
	}
}

func TestNewExternal_Invalid(t *testing.T) {
	zero := brl(t, "0.00")
	usdNeg, err := money.FromMinor(-100, money.BRL)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		mutate func(p *ExternalParams)
		want   error
	}{
		{"OPENING is internal only", func(p *ExternalParams) { p.Kind = KindOpening }, ErrInvalidKind},
		{"unknown kind", func(p *ExternalParams) { p.Kind = "NOPE" }, ErrInvalidKind},
		{"BET with zero amount", func(p *ExternalParams) { p.Amount = zero }, ErrInvalidAmount},
		{"BET with negative amount", func(p *ExternalParams) { p.Amount = usdNeg }, ErrInvalidAmount},
		{"uninitialized amount", func(p *ExternalParams) { p.Amount = money.Money{} }, ErrInvalidAmount},
		{"LOSS with non-zero amount", func(p *ExternalParams) { p.Kind = KindLoss }, ErrInvalidAmount},
		{"BET with a reference", func(p *ExternalParams) { p.ReferenceExternalID = "ext-0" }, ErrInvalidShape},
		{"REFUND without a reference", func(p *ExternalParams) { p.Kind = KindRefund }, ErrInvalidShape},
		{"ROLLBACK without a reference", func(p *ExternalParams) { p.Kind = KindRollback }, ErrInvalidShape},
		{"empty provider", func(p *ExternalParams) { p.ProviderID = "" }, ErrInvalidField},
		{"empty external id", func(p *ExternalParams) { p.ExternalID = "" }, ErrInvalidField},
		{"empty idempotency key", func(p *ExternalParams) { p.IdempotencyKey = "" }, ErrInvalidField},
		{"empty round", func(p *ExternalParams) { p.RoundID = "" }, ErrInvalidField},
		{"empty game", func(p *ExternalParams) { p.GameID = "" }, ErrInvalidField},
		{"provider too long", func(p *ExternalParams) { p.ProviderID = strings.Repeat("x", 201) }, ErrInvalidField},
		{"nil transaction id", func(p *ExternalParams) { p.ID = uuid.Nil }, ErrInvalidID},
		{"nil wallet id", func(p *ExternalParams) { p.WalletID = uuid.Nil }, ErrInvalidID},
		{"nil player id", func(p *ExternalParams) { p.PlayerID = uuid.Nil }, ErrInvalidID},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := baseParams(t, KindBet, "10.00")
			tc.mutate(&p)
			tx, err := NewExternal(p, now0)
			if !errors.Is(err, tc.want) {
				t.Errorf("error = %v, want %v", err, tc.want)
			}
			if tx != nil {
				t.Errorf("must not return a transaction on error")
			}
		})
	}
}

func TestNewOpening(t *testing.T) {
	id, wallet, player := uuid.New(), uuid.New(), uuid.New()

	tx, err := NewOpening(id, wallet, player, brl(t, "100.00"), now0)
	if err != nil {
		t.Fatalf("NewOpening: %v", err)
	}
	s := tx.Snapshot()
	if s.Kind != KindOpening || s.Status != StatusProcessed || !tx.IsInternal() {
		t.Errorf("opening = %s %s internal=%v", s.Kind, s.Status, tx.IsInternal())
	}
	if s.ResultBalance != brl(t, "100.00") {
		t.Errorf("result balance = %s, want 100.00", s.ResultBalance)
	}
	if s.ProviderID != "" || s.IdempotencyKey != "" || s.PayloadHash != "" || s.ReferenceID != uuid.Nil {
		t.Errorf("opening must not carry external fields: %+v", s)
	}

	if _, err := NewOpening(id, wallet, player, brl(t, "0.00"), now0); !errors.Is(err, ErrInvalidAmount) {
		t.Errorf("zero opening error = %v, want ErrInvalidAmount", err)
	}
	if _, err := NewOpening(uuid.Nil, wallet, player, brl(t, "1.00"), now0); !errors.Is(err, ErrInvalidID) {
		t.Errorf("nil id error = %v, want ErrInvalidID", err)
	}
}

func TestPayloadHash_IgnoresIDAndIdempotencyKey(t *testing.T) {
	a := baseParams(t, KindBet, "25.00")
	b := a
	b.ID = uuid.New()
	b.IdempotencyKey = "another-key"

	if hashOf(t, a) != hashOf(t, b) {
		t.Errorf("hash must not depend on the internal id or the idempotency key")
	}
}

func TestPayloadHash_NormalizesAmount(t *testing.T) {
	a := baseParams(t, KindBet, "25")
	b := a
	b.Amount = brl(t, "25.00")

	if hashOf(t, a) != hashOf(t, b) {
		t.Errorf("25 and 25.00 must produce the same hash")
	}
}

func TestPayloadHash_ChangesWithEveryBusinessField(t *testing.T) {
	base := baseParams(t, KindBet, "25.00")
	baseHash := hashOf(t, base)

	otherAmount := brl(t, "25.01")
	usd, err := money.Parse("25.00", "USD")
	if err != nil {
		t.Fatal(err)
	}

	mutators := map[string]func(p *ExternalParams){
		"kind":       func(p *ExternalParams) { p.Kind = KindWin },
		"provider":   func(p *ExternalParams) { p.ProviderID = "provider-b" },
		"externalId": func(p *ExternalParams) { p.ExternalID = "ext-2" },
		"wallet":     func(p *ExternalParams) { p.WalletID = uuid.New() },
		"player":     func(p *ExternalParams) { p.PlayerID = uuid.New() },
		"round":      func(p *ExternalParams) { p.RoundID = "round-2" },
		"game":       func(p *ExternalParams) { p.GameID = "game-2" },
		"amount":     func(p *ExternalParams) { p.Amount = otherAmount },
		"currency":   func(p *ExternalParams) { p.Amount = usd },
		"reference":  func(p *ExternalParams) { p.ReferenceExternalID = "ext-0" },
	}
	for name, mutate := range mutators {
		p := base
		mutate(&p)
		if hashOf(t, p) == baseHash {
			t.Errorf("changing %s did not change the hash", name)
		}
	}
}

func TestNewExternal_StoresTheComputedHash(t *testing.T) {
	p := baseParams(t, KindWin, "9.99")
	tx, err := NewExternal(p, now0)
	if err != nil {
		t.Fatal(err)
	}
	if tx.Snapshot().PayloadHash != hashOf(t, p) {
		t.Errorf("stored hash differs from ComputePayloadHash")
	}
}

func TestMarkPendingReference(t *testing.T) {
	tx := newTx(t, KindRefund, "10.00")

	if err := tx.MarkPendingReference(retryAt, now0); err != nil {
		t.Fatalf("MarkPendingReference: %v", err)
	}
	s := tx.Snapshot()
	if s.Status != StatusPendingReference || s.Attempts != 1 || !s.NextAttemptAt.Equal(retryAt) {
		t.Errorf("state = %s attempts=%d next=%v", s.Status, s.Attempts, s.NextAttemptAt)
	}

	// novas tentativas continuam somando
	later := retryAt.Add(time.Minute)
	if err := tx.MarkPendingReference(later, retryAt); err != nil {
		t.Fatalf("second MarkPendingReference: %v", err)
	}
	if tx.Snapshot().Attempts != 2 || !tx.Snapshot().NextAttemptAt.Equal(later) {
		t.Errorf("attempts/next not updated: %+v", tx.Snapshot())
	}
}

func TestMarkPendingReference_Invalid(t *testing.T) {
	bet := newTx(t, KindBet, "10.00")
	before := bet.Snapshot()
	if err := bet.MarkPendingReference(retryAt, now0); !errors.Is(err, ErrInvalidShape) {
		t.Errorf("BET error = %v, want ErrInvalidShape", err)
	}
	if bet.Snapshot() != before {
		t.Errorf("failed transition changed the transaction")
	}

	refund := newTx(t, KindRefund, "10.00")
	if err := refund.MarkPendingReference(time.Time{}, now0); !errors.Is(err, ErrInvalidField) {
		t.Errorf("zero next attempt error = %v, want ErrInvalidField", err)
	}
	if refund.Status() != StatusPending {
		t.Errorf("status = %s, want PENDING", refund.Status())
	}
}

func TestSetReference(t *testing.T) {
	tx := newTx(t, KindRefund, "10.00")
	ref := uuid.New()

	if err := tx.SetReference(ref, now0); err != nil {
		t.Fatalf("SetReference: %v", err)
	}
	if tx.ReferenceID() != ref {
		t.Errorf("reference not stored")
	}
	if err := tx.SetReference(ref, now0); err != nil {
		t.Errorf("setting the same reference again must be allowed: %v", err)
	}
	if err := tx.SetReference(uuid.New(), now0); !errors.Is(err, ErrReferenceAlreadySet) {
		t.Errorf("different reference error = %v, want ErrReferenceAlreadySet", err)
	}
	if err := tx.SetReference(uuid.Nil, now0); !errors.Is(err, ErrInvalidID) {
		t.Errorf("nil reference error = %v, want ErrInvalidID", err)
	}

	bet := newTx(t, KindBet, "10.00")
	if err := bet.SetReference(uuid.New(), now0); !errors.Is(err, ErrInvalidShape) {
		t.Errorf("BET reference error = %v, want ErrInvalidShape", err)
	}
}

func TestProcess_Success(t *testing.T) {
	tx := newTx(t, KindBet, "80.00")
	later := now0.Add(time.Second)

	if err := tx.Process(brl(t, "20.00"), later); err != nil {
		t.Fatalf("Process: %v", err)
	}
	s := tx.Snapshot()
	if s.Status != StatusProcessed || s.ResultBalance != brl(t, "20.00") {
		t.Errorf("state = %s result=%s", s.Status, s.ResultBalance)
	}
	if !s.NextAttemptAt.IsZero() || !s.UpdatedAt.Equal(later) {
		t.Errorf("next attempt must be cleared and updatedAt set: %+v", s)
	}
}

func TestProcess_InvalidBalanceKeepsTransactionPending(t *testing.T) {
	usd, err := money.Parse("1.00", "USD")
	if err != nil {
		t.Fatal(err)
	}
	negative, err := money.FromMinor(-1, money.BRL)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		balance money.Money
		want    error
	}{
		{"uninitialized", money.Money{}, ErrInvalidShape},
		{"negative", negative, ErrInvalidAmount},
		{"other currency", usd, ErrInvalidAmount},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tx := newTx(t, KindBet, "10.00")
			if err := tx.Process(tc.balance, now0); !errors.Is(err, tc.want) {
				t.Errorf("error = %v, want %v", err, tc.want)
			}
			if tx.Status() != StatusPending {
				t.Errorf("status = %s, want PENDING", tx.Status())
			}
		})
	}
}

func TestProcess_ReversalNeedsResolvedReference(t *testing.T) {
	for _, kind := range []Kind{KindRefund, KindRollback} {
		t.Run(string(kind), func(t *testing.T) {
			tx := newTx(t, kind, "10.00")
			if err := tx.Process(brl(t, "10.00"), now0); !errors.Is(err, ErrInvalidShape) {
				t.Fatalf("Process without reference error = %v, want ErrInvalidShape", err)
			}
			if err := tx.SetReference(uuid.New(), now0); err != nil {
				t.Fatal(err)
			}
			if err := tx.Process(brl(t, "10.00"), now0); err != nil {
				t.Errorf("Process with reference: %v", err)
			}
		})
	}
}

func TestReject(t *testing.T) {
	tx := newTx(t, KindBet, "80.00")
	if err := tx.Reject("", money.Money{}, now0); !errors.Is(err, ErrInvalidShape) {
		t.Errorf("reject without code error = %v, want ErrInvalidShape", err)
	}
	if tx.Status() != StatusPending {
		t.Fatalf("status = %s, want PENDING after a failed reject", tx.Status())
	}

	if err := tx.Reject(FailureInsufficientFunds, brl(t, "20.00"), now0); err != nil {
		t.Fatalf("Reject: %v", err)
	}
	s := tx.Snapshot()
	if s.Status != StatusRejected || s.FailureCode != FailureInsufficientFunds || s.ResultBalance != brl(t, "20.00") {
		t.Errorf("state = %s code=%s result=%s", s.Status, s.FailureCode, s.ResultBalance)
	}
}

func TestRejectFromPendingReference(t *testing.T) {
	tx := newTx(t, KindRollback, "10.00")
	if err := tx.MarkPendingReference(retryAt, now0); err != nil {
		t.Fatal(err)
	}
	if err := tx.Reject(FailureReferenceNotFound, money.Money{}, retryAt); err != nil {
		t.Fatalf("Reject: %v", err)
	}
	if tx.Status() != StatusRejected || !tx.Snapshot().NextAttemptAt.IsZero() {
		t.Errorf("state = %+v", tx.Snapshot())
	}
}

func TestFail(t *testing.T) {
	tx := newTx(t, KindBet, "10.00")
	if err := tx.Fail("", now0); !errors.Is(err, ErrInvalidShape) {
		t.Errorf("fail without code error = %v, want ErrInvalidShape", err)
	}
	if err := tx.Fail(FailureProcessingFailed, now0); err != nil {
		t.Fatalf("Fail: %v", err)
	}
	if tx.Status() != StatusFailed || tx.Snapshot().FailureCode != FailureProcessingFailed {
		t.Errorf("state = %+v", tx.Snapshot())
	}
}

func TestTerminalStatesBlockEveryTransition(t *testing.T) {
	processed := newTx(t, KindBet, "10.00")
	if err := processed.Process(brl(t, "5.00"), now0); err != nil {
		t.Fatal(err)
	}
	rejected := newTx(t, KindBet, "10.00")
	if err := rejected.Reject(FailureInsufficientFunds, money.Money{}, now0); err != nil {
		t.Fatal(err)
	}
	failed := newTx(t, KindBet, "10.00")
	if err := failed.Fail(FailureProcessingFailed, now0); err != nil {
		t.Fatal(err)
	}

	terminals := map[string]*Transaction{
		"PROCESSED": processed,
		"REJECTED":  rejected,
		"FAILED":    failed,
	}
	for name, tx := range terminals {
		t.Run(name, func(t *testing.T) {
			before := tx.Snapshot()
			attempts := map[string]error{
				"MarkPendingReference": tx.MarkPendingReference(retryAt, now0),
				"SetReference":         tx.SetReference(uuid.New(), now0),
				"Process":              tx.Process(brl(t, "1.00"), now0),
				"Reject":               tx.Reject(FailureInsufficientFunds, money.Money{}, now0),
				"Fail":                 tx.Fail(FailureProcessingFailed, now0),
			}
			for op, err := range attempts {
				if !errors.Is(err, ErrTerminalState) {
					t.Errorf("%s from %s: error = %v, want ErrTerminalState", op, name, err)
				}
			}
			if tx.Snapshot() != before {
				t.Errorf("terminal transaction changed")
			}
		})
	}
}

func TestStatusIsTerminal(t *testing.T) {
	tests := map[Status]bool{
		StatusPending:          false,
		StatusPendingReference: false,
		StatusProcessed:        true,
		StatusRejected:         true,
		StatusFailed:           true,
	}
	for status, want := range tests {
		if got := status.IsTerminal(); got != want {
			t.Errorf("%s.IsTerminal() = %v, want %v", status, got, want)
		}
	}
}

func TestRehydrate_RoundTrip(t *testing.T) {
	pending := newTx(t, KindBet, "10.00")

	pendingRef := newTx(t, KindRefund, "10.00")
	if err := pendingRef.MarkPendingReference(retryAt, now0); err != nil {
		t.Fatal(err)
	}

	processed := newTx(t, KindBet, "10.00")
	if err := processed.Process(brl(t, "5.00"), now0); err != nil {
		t.Fatal(err)
	}

	rejected := newTx(t, KindBet, "10.00")
	if err := rejected.Reject(FailureInsufficientFunds, brl(t, "5.00"), now0); err != nil {
		t.Fatal(err)
	}

	for name, tx := range map[string]*Transaction{
		"pending":           pending,
		"pending reference": pendingRef,
		"processed":         processed,
		"rejected":          rejected,
	} {
		t.Run(name, func(t *testing.T) {
			s := tx.Snapshot()
			back, err := Rehydrate(s)
			if err != nil {
				t.Fatalf("Rehydrate: %v", err)
			}
			if back.Snapshot() != s {
				t.Errorf("round trip changed the snapshot")
			}
		})
	}
}

func TestRehydrate_Invalid(t *testing.T) {
	negative, err := money.FromMinor(-1, money.BRL)
	if err != nil {
		t.Fatal(err)
	}
	one := brl(t, "1.00")

	tests := []struct {
		name   string
		mutate func(s *Snapshot)
		want   error
	}{
		{"unknown status", func(s *Snapshot) { s.Status = "WHATEVER" }, ErrInvalidStatus},
		{"unknown kind", func(s *Snapshot) { s.Kind = "WHATEVER" }, ErrInvalidKind},
		{"rejected without code", func(s *Snapshot) { s.Status = StatusRejected }, ErrInvalidShape},
		{"failure code on pending", func(s *Snapshot) { s.FailureCode = FailureInsufficientFunds }, ErrInvalidShape},
		{"processed without result", func(s *Snapshot) {
			s.Status = StatusProcessed
			s.NextAttemptAt = time.Time{}
		}, ErrInvalidShape},
		{"terminal with next attempt", func(s *Snapshot) {
			s.Status = StatusProcessed
			s.ResultBalance = one
		}, ErrInvalidShape},
		{"negative result balance", func(s *Snapshot) { s.ResultBalance = negative }, ErrInvalidAmount},
		{"negative attempts", func(s *Snapshot) { s.Attempts = -1 }, ErrInvalidShape},
		{"nil id", func(s *Snapshot) { s.ID = uuid.Nil }, ErrInvalidID},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTx(t, KindBet, "10.00").Snapshot()
			tc.mutate(&s)
			if _, err := Rehydrate(s); !errors.Is(err, tc.want) {
				t.Errorf("error = %v, want %v", err, tc.want)
			}
		})
	}
}
