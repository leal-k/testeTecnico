package event

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/leal-k/testeTecnico/internal/domain/money"
	"github.com/leal-k/testeTecnico/internal/domain/wager"
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

func meta() Meta {
	return Meta{ID: uuid.New(), CorrelationID: "corr-1", CausationID: "cause-1", OccurredAt: now0}
}

func newTx(t *testing.T, kind wager.Kind, amount string) *wager.Transaction {
	t.Helper()
	p := wager.ExternalParams{
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
	if kind == wager.KindRefund || kind == wager.KindRollback {
		p.ReferenceExternalID = "ext-0"
	}
	tx, err := wager.NewExternal(p, now0)
	if err != nil {
		t.Fatalf("NewExternal: %v", err)
	}
	return tx
}

func decode(t *testing.T, e Event) map[string]any {
	t.Helper()
	if !json.Valid(e.Payload) {
		t.Fatalf("payload is not valid JSON: %s", e.Payload)
	}
	var m map[string]any
	if err := json.Unmarshal(e.Payload, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestTransactionProcessed(t *testing.T) {
	tx := newTx(t, wager.KindBet, "80.00")
	if err := tx.Process(brl(t, "20.00"), now0); err != nil {
		t.Fatal(err)
	}
	m := meta()

	e, err := TransactionProcessed(tx.Snapshot(), m)
	if err != nil {
		t.Fatalf("TransactionProcessed: %v", err)
	}
	if e.ID != m.ID || e.Type != TypeWagerTransactionProcessed || e.Version != 1 {
		t.Errorf("envelope = %+v", e)
	}
	if e.AggregateType != AggregateWagerTransaction || e.AggregateID != tx.ID() {
		t.Errorf("aggregate = %s/%s", e.AggregateType, e.AggregateID)
	}
	if e.CorrelationID != "corr-1" || e.CausationID != "cause-1" || !e.OccurredAt.Equal(now0) {
		t.Errorf("metadata not preserved: %+v", e)
	}

	data := decode(t, e)
	if data["type"] != "BET" || data["status"] != "PROCESSED" || data["transactionId"] != tx.ID().String() {
		t.Errorf("data = %v", data)
	}
	amount, _ := data["amount"].(map[string]any)
	if amount["amount"] != "80.00" || amount["currency"] != "BRL" {
		t.Errorf("amount = %v", data["amount"])
	}
	balance, _ := data["balance"].(map[string]any)
	if balance["amount"] != "20.00" {
		t.Errorf("balance = %v", data["balance"])
	}
	if _, present := data["failureCode"]; present {
		t.Errorf("failureCode must be absent on a processed event")
	}
}

func TestTransactionRejected(t *testing.T) {
	tx := newTx(t, wager.KindBet, "80.00")
	if err := tx.Reject(wager.FailureInsufficientFunds, brl(t, "20.00"), now0); err != nil {
		t.Fatal(err)
	}

	e, err := TransactionRejected(tx.Snapshot(), meta())
	if err != nil {
		t.Fatalf("TransactionRejected: %v", err)
	}
	data := decode(t, e)
	if e.Type != TypeWagerTransactionRejected || data["status"] != "REJECTED" || data["failureCode"] != "INSUFFICIENT_FUNDS" {
		t.Errorf("event = %+v data = %v", e, data)
	}
}

func TestTransactionPendingReference(t *testing.T) {
	tx := newTx(t, wager.KindRefund, "10.00")
	next := now0.Add(30 * time.Second)
	if err := tx.MarkPendingReference(next, now0); err != nil {
		t.Fatal(err)
	}

	e, err := TransactionPendingReference(tx.Snapshot(), meta())
	if err != nil {
		t.Fatalf("TransactionPendingReference: %v", err)
	}
	data := decode(t, e)
	if e.Type != TypeWagerTransactionPendingReference || data["status"] != "PENDING_REFERENCE" {
		t.Errorf("event = %+v data = %v", e, data)
	}
	if data["referenceExternalTransactionId"] != "ext-0" || data["attempts"] != float64(1) {
		t.Errorf("reference/attempts missing: %v", data)
	}
	if data["nextAttemptAt"] != "2026-10-19T12:00:30Z" {
		t.Errorf("nextAttemptAt = %v", data["nextAttemptAt"])
	}
}

func TestTransactionEvents_RequireTheMatchingStatus(t *testing.T) {
	pending := newTx(t, wager.KindBet, "10.00").Snapshot()

	for name, build := range map[string]func(wager.Snapshot, Meta) (Event, error){
		"processed": TransactionProcessed,
		"rejected":  TransactionRejected,
		"pending":   TransactionPendingReference,
	} {
		if _, err := build(pending, meta()); !errors.Is(err, ErrInvalidEvent) {
			t.Errorf("%s event from a PENDING transaction: error = %v, want ErrInvalidEvent", name, err)
		}
	}
}

func TestWalletBalanceChanged(t *testing.T) {
	w, _, err := wallet.Open(uuid.New(), uuid.New(), brl(t, "100.00"), now0)
	if err != nil {
		t.Fatal(err)
	}
	mv, err := w.Debit(brl(t, "30.00"), now0)
	if err != nil {
		t.Fatal(err)
	}
	txID := uuid.New()

	e, err := WalletBalanceChanged(w.ID(), txID, mv, w.Version(), meta())
	if err != nil {
		t.Fatalf("WalletBalanceChanged: %v", err)
	}
	if e.Type != TypeWalletBalanceChanged || e.AggregateType != AggregateWallet || e.AggregateID != w.ID() {
		t.Errorf("envelope = %+v", e)
	}

	data := decode(t, e)
	if data["walletId"] != w.ID().String() || data["transactionId"] != txID.String() ||
		data["direction"] != "DEBIT" || data["walletVersion"] != float64(2) {
		t.Errorf("data = %v", data)
	}
	for field, want := range map[string]string{"money": "30.00", "balanceBefore": "100.00", "balanceAfter": "70.00"} {
		got, _ := data[field].(map[string]any)
		if got["amount"] != want || got["currency"] != "BRL" {
			t.Errorf("%s = %v, want %s BRL", field, data[field], want)
		}
	}
}

func TestMetaValidation(t *testing.T) {
	tx := newTx(t, wager.KindBet, "10.00")
	if err := tx.Process(brl(t, "5.00"), now0); err != nil {
		t.Fatal(err)
	}

	tests := map[string]Meta{
		"missing event id":       {CorrelationID: "c", OccurredAt: now0},
		"missing correlation id": {ID: uuid.New(), OccurredAt: now0},
		"missing occurredAt":     {ID: uuid.New(), CorrelationID: "c"},
	}
	for name, m := range tests {
		if _, err := TransactionProcessed(tx.Snapshot(), m); !errors.Is(err, ErrInvalidEvent) {
			t.Errorf("%s: error = %v, want ErrInvalidEvent", name, err)
		}
	}
}
