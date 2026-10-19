package wager

import (
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/leal-k/testeTecnico/internal/domain/money"
	"github.com/leal-k/testeTecnico/internal/domain/wallet"
)

func newWallet(t *testing.T, balance string) *wallet.Wallet {
	t.Helper()
	w, _, err := wallet.Open(uuid.New(), uuid.New(), brl(t, balance), now0)
	if err != nil {
		t.Fatalf("wallet.Open: %v", err)
	}
	return w
}

func paramsFor(t *testing.T, w *wallet.Wallet, kind Kind, amount string) ExternalParams {
	t.Helper()
	p := baseParams(t, kind, amount)
	p.WalletID = w.ID()
	p.PlayerID = w.PlayerID()
	return p
}

func txFor(t *testing.T, w *wallet.Wallet, kind Kind, amount string) *Transaction {
	t.Helper()
	tx, err := NewExternal(paramsFor(t, w, kind, amount), now0)
	if err != nil {
		t.Fatalf("NewExternal(%s): %v", kind, err)
	}
	return tx
}

// processedFrom cria uma transação já PROCESSED a partir dos parâmetros, para
// servir de referência.
func processedFrom(t *testing.T, p ExternalParams, balance money.Money) *Transaction {
	t.Helper()
	tx, err := NewExternal(p, now0)
	if err != nil {
		t.Fatalf("NewExternal(%s): %v", p.Kind, err)
	}
	if p.Kind == KindRefund || p.Kind == KindRollback {
		if err := tx.SetReference(uuid.New(), now0); err != nil {
			t.Fatalf("SetReference: %v", err)
		}
	}
	if err := tx.Process(balance, now0); err != nil {
		t.Fatalf("Process: %v", err)
	}
	return tx
}

func processed(t *testing.T, w *wallet.Wallet, kind Kind, amount string) *Transaction {
	t.Helper()
	p := paramsFor(t, w, kind, amount)
	p.ExternalID = "ext-ref-" + string(kind)
	p.IdempotencyKey = "idem-ref-" + string(kind)
	return processedFrom(t, p, w.Balance())
}

// against cria uma operação do tipo kind que referencia a transação ref.
func against(t *testing.T, w *wallet.Wallet, kind Kind, amount string, ref *Transaction) *Transaction {
	t.Helper()
	p := paramsFor(t, w, kind, amount)
	p.ReferenceExternalID = ref.Snapshot().ExternalID
	tx, err := NewExternal(p, now0)
	if err != nil {
		t.Fatalf("NewExternal(%s): %v", kind, err)
	}
	return tx
}

func wantRejection(t *testing.T, err error, code FailureCode) {
	t.Helper()
	var rej *Rejection
	if !errors.As(err, &rej) {
		t.Fatalf("error = %v, want a *Rejection with code %s", err, code)
	}
	if rej.Code != code {
		t.Errorf("rejection code = %s, want %s", rej.Code, code)
	}
}

func assertWallet(t *testing.T, w *wallet.Wallet, balance string, version int64) {
	t.Helper()
	if w.Balance().Amount() != balance || w.Version() != version {
		t.Errorf("wallet = %s v%d, want %s v%d", w.Balance().Amount(), w.Version(), balance, version)
	}
}

func TestApply_Bet_Success(t *testing.T) {
	w := newWallet(t, "100.00")
	tx := txFor(t, w, KindBet, "80.00")

	mv, err := Apply(tx, w, nil, now0)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if mv == nil || mv.Direction != wallet.DirectionDebit || mv.BalanceAfter.Amount() != "20.00" {
		t.Errorf("movement = %+v", mv)
	}
	assertWallet(t, w, "20.00", 2)
	if tx.Status() != StatusPending {
		t.Errorf("Apply must not change the transaction, status = %s", tx.Status())
	}
}

func TestApply_Bet_InsufficientFunds(t *testing.T) {
	w := newWallet(t, "100.00")
	_, err := Apply(txFor(t, w, KindBet, "100.01"), w, nil, now0)

	wantRejection(t, err, FailureInsufficientFunds)
	assertWallet(t, w, "100.00", 1)
}

// Cenário do README: 100.00 com duas apostas de 80.00.
func TestApply_TwoBetsOfEightyOnHundred(t *testing.T) {
	w := newWallet(t, "100.00")

	if _, err := Apply(txFor(t, w, KindBet, "80.00"), w, nil, now0); err != nil {
		t.Fatalf("first bet: %v", err)
	}
	_, err := Apply(txFor(t, w, KindBet, "80.00"), w, nil, now0)
	wantRejection(t, err, FailureInsufficientFunds)
	assertWallet(t, w, "20.00", 2)
}

func TestApply_Win(t *testing.T) {
	t.Run("without reference", func(t *testing.T) {
		w := newWallet(t, "10.00")
		mv, err := Apply(txFor(t, w, KindWin, "30.00"), w, nil, now0)
		if err != nil || mv == nil || mv.Direction != wallet.DirectionCredit {
			t.Fatalf("Apply = %+v, %v", mv, err)
		}
		assertWallet(t, w, "40.00", 2)
	})

	t.Run("with reference to a BET of the same round", func(t *testing.T) {
		w := newWallet(t, "10.00")
		bet := processed(t, w, KindBet, "5.00")
		_, err := Apply(against(t, w, KindWin, "30.00", bet), w, &Reference{Tx: bet}, now0)
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
		assertWallet(t, w, "40.00", 2)
	})

	t.Run("reference is not a BET", func(t *testing.T) {
		w := newWallet(t, "10.00")
		other := processed(t, w, KindWin, "5.00")
		_, err := Apply(against(t, w, KindWin, "30.00", other), w, &Reference{Tx: other}, now0)
		wantRejection(t, err, FailureInvalidReferenceKind)
		assertWallet(t, w, "10.00", 1)
	})

	t.Run("reference from another round", func(t *testing.T) {
		w := newWallet(t, "10.00")
		bet := processed(t, w, KindBet, "5.00")
		p := paramsFor(t, w, KindWin, "30.00")
		p.ReferenceExternalID = bet.Snapshot().ExternalID
		p.RoundID = "round-2"
		win, err := NewExternal(p, now0)
		if err != nil {
			t.Fatal(err)
		}
		_, err = Apply(win, w, &Reference{Tx: bet}, now0)
		wantRejection(t, err, FailureReferenceMismatch)
		assertWallet(t, w, "10.00", 1)
	})
}

func TestApply_Loss_HasNoMovement(t *testing.T) {
	t.Run("without reference", func(t *testing.T) {
		w := newWallet(t, "50.00")
		mv, err := Apply(txFor(t, w, KindLoss, "0.00"), w, nil, now0)
		if err != nil || mv != nil {
			t.Fatalf("Apply = %+v, %v; want nil movement and no error", mv, err)
		}
		assertWallet(t, w, "50.00", 1)
	})

	t.Run("with reference to a BET", func(t *testing.T) {
		w := newWallet(t, "50.00")
		bet := processed(t, w, KindBet, "5.00")
		mv, err := Apply(against(t, w, KindLoss, "0.00", bet), w, &Reference{Tx: bet}, now0)
		if err != nil || mv != nil {
			t.Fatalf("Apply = %+v, %v; want nil movement and no error", mv, err)
		}
		assertWallet(t, w, "50.00", 1)
	})
}

func TestApply_Refund(t *testing.T) {
	t.Run("credits the full BET amount", func(t *testing.T) {
		w := newWallet(t, "20.00")
		bet := processed(t, w, KindBet, "80.00")
		mv, err := Apply(against(t, w, KindRefund, "80.00", bet), w, &Reference{Tx: bet}, now0)
		if err != nil || mv == nil || mv.Direction != wallet.DirectionCredit {
			t.Fatalf("Apply = %+v, %v", mv, err)
		}
		assertWallet(t, w, "100.00", 2)
	})

	t.Run("reference must be a BET", func(t *testing.T) {
		w := newWallet(t, "20.00")
		win := processed(t, w, KindWin, "80.00")
		_, err := Apply(against(t, w, KindRefund, "80.00", win), w, &Reference{Tx: win}, now0)
		wantRejection(t, err, FailureInvalidReferenceKind)
		assertWallet(t, w, "20.00", 1)
	})

	t.Run("partial amount is rejected", func(t *testing.T) {
		w := newWallet(t, "20.00")
		bet := processed(t, w, KindBet, "80.00")
		_, err := Apply(against(t, w, KindRefund, "79.99", bet), w, &Reference{Tx: bet}, now0)
		wantRejection(t, err, FailureReferenceAmountMismatch)
		assertWallet(t, w, "20.00", 1)
	})

	t.Run("already reversed", func(t *testing.T) {
		w := newWallet(t, "20.00")
		bet := processed(t, w, KindBet, "80.00")
		_, err := Apply(against(t, w, KindRefund, "80.00", bet), w, &Reference{Tx: bet, AlreadyReversed: true}, now0)
		wantRejection(t, err, FailureAlreadyReversed)
		assertWallet(t, w, "20.00", 1)
	})
}

func TestApply_Rollback_ReversesTheReferencedEffect(t *testing.T) {
	tests := []struct {
		name      string
		refKind   Kind
		refAmount string
		start     string
		wantDir   wallet.Direction
		wantAfter string
	}{
		{"rollback of a BET gives the money back", KindBet, "80.00", "20.00", wallet.DirectionCredit, "100.00"},
		{"rollback of a WIN takes the money back", KindWin, "30.00", "100.00", wallet.DirectionDebit, "70.00"},
		{"rollback of a REFUND takes the money back", KindRefund, "30.00", "100.00", wallet.DirectionDebit, "70.00"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := newWallet(t, tc.start)
			ref := processed(t, w, tc.refKind, tc.refAmount)

			mv, err := Apply(against(t, w, KindRollback, tc.refAmount, ref), w, &Reference{Tx: ref}, now0)
			if err != nil || mv == nil {
				t.Fatalf("Apply = %+v, %v", mv, err)
			}
			if mv.Direction != tc.wantDir {
				t.Errorf("direction = %s, want %s", mv.Direction, tc.wantDir)
			}
			assertWallet(t, w, tc.wantAfter, 2)
		})
	}
}

func TestApply_Rollback_Rejections(t *testing.T) {
	t.Run("reversal that would overdraw uses its own code", func(t *testing.T) {
		w := newWallet(t, "10.00")
		win := processed(t, w, KindWin, "30.00")
		_, err := Apply(against(t, w, KindRollback, "30.00", win), w, &Reference{Tx: win}, now0)

		wantRejection(t, err, FailureReversalInsufficientFunds)
		var rej *Rejection
		if errors.As(err, &rej) && rej.Code == FailureInsufficientFunds {
			t.Errorf("reversal must not reuse the BET insufficient funds code")
		}
		assertWallet(t, w, "10.00", 1)
	})

	t.Run("LOSS cannot be rolled back", func(t *testing.T) {
		w := newWallet(t, "100.00")
		loss := processed(t, w, KindLoss, "0.00")
		_, err := Apply(against(t, w, KindRollback, "10.00", loss), w, &Reference{Tx: loss}, now0)
		wantRejection(t, err, FailureInvalidReferenceKind)
	})

	t.Run("ROLLBACK cannot be rolled back", func(t *testing.T) {
		w := newWallet(t, "100.00")
		rb := processed(t, w, KindRollback, "10.00")
		_, err := Apply(against(t, w, KindRollback, "10.00", rb), w, &Reference{Tx: rb}, now0)
		wantRejection(t, err, FailureInvalidReferenceKind)
	})

	t.Run("already reversed", func(t *testing.T) {
		w := newWallet(t, "100.00")
		bet := processed(t, w, KindBet, "10.00")
		_, err := Apply(against(t, w, KindRollback, "10.00", bet), w, &Reference{Tx: bet, AlreadyReversed: true}, now0)
		wantRejection(t, err, FailureAlreadyReversed)
		assertWallet(t, w, "100.00", 1)
	})

	t.Run("amount differs from the referenced one", func(t *testing.T) {
		w := newWallet(t, "100.00")
		bet := processed(t, w, KindBet, "10.00")
		_, err := Apply(against(t, w, KindRollback, "9.00", bet), w, &Reference{Tx: bet}, now0)
		wantRejection(t, err, FailureReferenceAmountMismatch)
	})

	t.Run("referencing an OPENING is a mismatch", func(t *testing.T) {
		w := newWallet(t, "100.00")
		opening, err := NewOpening(uuid.New(), w.ID(), w.PlayerID(), brl(t, "100.00"), now0)
		if err != nil {
			t.Fatal(err)
		}
		p := paramsFor(t, w, KindRollback, "100.00")
		p.ReferenceExternalID = "ext-opening"
		rb, err := NewExternal(p, now0)
		if err != nil {
			t.Fatal(err)
		}
		_, err = Apply(rb, w, &Reference{Tx: opening}, now0)
		wantRejection(t, err, FailureReferenceMismatch)
	})
}

func TestApply_ReferenceAvailability(t *testing.T) {
	w := newWallet(t, "100.00")

	pendingBet := txFor(t, w, KindBet, "10.00")

	// uma referência que ela própria aguarda outra referência
	pendingRefRefund := txFor(t, w, KindRefund, "10.00")
	if err := pendingRefRefund.MarkPendingReference(retryAt, now0); err != nil {
		t.Fatal(err)
	}

	rejectedBet := txFor(t, w, KindBet, "10.00")
	if err := rejectedBet.Reject(FailureInsufficientFunds, money.Money{}, now0); err != nil {
		t.Fatal(err)
	}
	failedBet := txFor(t, w, KindBet, "10.00")
	if err := failedBet.Fail(FailureProcessingFailed, now0); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name     string
		ref      *Reference
		wantErr  error
		wantCode FailureCode
	}{
		{"reference not found", nil, ErrReferenceNotFound, ""},
		{"reference with no transaction", &Reference{}, ErrReferenceNotFound, ""},
		{"reference still PENDING", &Reference{Tx: pendingBet}, ErrReferencePending, ""},
		{"reference in PENDING_REFERENCE", &Reference{Tx: pendingRefRefund}, ErrReferencePending, ""},
		{"reference REJECTED", &Reference{Tx: rejectedBet}, nil, FailureReferenceNotProcessed},
		{"reference FAILED", &Reference{Tx: failedBet}, nil, FailureReferenceNotProcessed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := paramsFor(t, w, KindRefund, "10.00")
			// o ExternalID da referência é o que a transação procura
			if tc.ref != nil && tc.ref.Tx != nil {
				p.ReferenceExternalID = tc.ref.Tx.Snapshot().ExternalID
			}
			refund, err := NewExternal(p, now0)
			if err != nil {
				t.Fatal(err)
			}

			_, err = Apply(refund, w, tc.ref, now0)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Errorf("error = %v, want %v", err, tc.wantErr)
				}
			} else {
				wantRejection(t, err, tc.wantCode)
			}
			assertWallet(t, w, "100.00", 1)
		})
	}
}

func TestApply_ReferenceFromAnotherContext(t *testing.T) {
	w := newWallet(t, "100.00")
	other := newWallet(t, "100.00")

	otherProvider := paramsFor(t, w, KindBet, "10.00")
	otherProvider.ProviderID = "provider-b"

	otherRound := paramsFor(t, w, KindBet, "10.00")
	otherRound.RoundID = "round-9"

	tests := []struct {
		name string
		ref  *Transaction
	}{
		{"another provider", processedFrom(t, otherProvider, w.Balance())},
		{"another round", processedFrom(t, otherRound, w.Balance())},
		{"another wallet and player", processed(t, other, KindBet, "10.00")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := paramsFor(t, w, KindRefund, "10.00")
			p.ReferenceExternalID = tc.ref.Snapshot().ExternalID
			refund, err := NewExternal(p, now0)
			if err != nil {
				t.Fatal(err)
			}

			_, err = Apply(refund, w, &Reference{Tx: tc.ref}, now0)
			wantRejection(t, err, FailureReferenceMismatch)
			assertWallet(t, w, "100.00", 1)
		})
	}
}

func TestApply_Guards(t *testing.T) {
	t.Run("terminal transaction", func(t *testing.T) {
		w := newWallet(t, "100.00")
		tx := txFor(t, w, KindBet, "10.00")
		if err := tx.Fail(FailureProcessingFailed, now0); err != nil {
			t.Fatal(err)
		}
		if _, err := Apply(tx, w, nil, now0); !errors.Is(err, ErrTerminalState) {
			t.Errorf("error = %v, want ErrTerminalState", err)
		}
		assertWallet(t, w, "100.00", 1)
	})

	t.Run("OPENING is not applicable", func(t *testing.T) {
		w := newWallet(t, "100.00")
		opening, err := NewOpening(uuid.New(), w.ID(), w.PlayerID(), brl(t, "100.00"), now0)
		if err != nil {
			t.Fatal(err)
		}
		// já nasce PROCESSED, então o primeiro bloqueio é o estado terminal
		if _, err := Apply(opening, w, nil, now0); err == nil {
			t.Errorf("Apply on an OPENING must fail")
		}
	})

	t.Run("wallet of another player", func(t *testing.T) {
		w := newWallet(t, "100.00")
		other := newWallet(t, "100.00")
		tx := txFor(t, w, KindBet, "10.00")
		if _, err := Apply(tx, other, nil, now0); !errors.Is(err, ErrWalletMismatch) {
			t.Errorf("error = %v, want ErrWalletMismatch", err)
		}
		assertWallet(t, other, "100.00", 1)
	})

	t.Run("currency different from the wallet", func(t *testing.T) {
		usd, err := money.Parse("100.00", "USD")
		if err != nil {
			t.Fatal(err)
		}
		w, _, err := wallet.Open(uuid.New(), uuid.New(), usd, now0)
		if err != nil {
			t.Fatal(err)
		}
		tx := txFor(t, w, KindBet, "10.00") // valor em BRL, carteira em USD

		_, err = Apply(tx, w, nil, now0)
		wantRejection(t, err, FailureCurrencyMismatch)
		assertWallet(t, w, "100.00", 1)
	})
}

// Uma aposta seguida do seu ROLLBACK devolve a carteira ao saldo original.
func TestApply_BetThenRollbackRestoresTheBalance(t *testing.T) {
	w := newWallet(t, "100.00")

	bet := txFor(t, w, KindBet, "40.00")
	if _, err := Apply(bet, w, nil, now0); err != nil {
		t.Fatalf("bet: %v", err)
	}
	if err := bet.Process(w.Balance(), now0); err != nil {
		t.Fatal(err)
	}
	assertWallet(t, w, "60.00", 2)

	rollback := against(t, w, KindRollback, "40.00", bet)
	if _, err := Apply(rollback, w, &Reference{Tx: bet}, now0); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	assertWallet(t, w, "100.00", 3)
}
