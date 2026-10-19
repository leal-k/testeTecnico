package wager

import (
	"errors"
	"fmt"
	"time"

	"github.com/leal-k/testeTecnico/internal/domain/money"
	"github.com/leal-k/testeTecnico/internal/domain/wallet"
)

var (
	// A transação referenciada ainda não existe (talvez chegue depois).
	ErrReferenceNotFound = errors.New("wager: referenced transaction not found")
	// A transação referenciada existe, mas ainda não terminou.
	ErrReferencePending = errors.New("wager: referenced transaction is not finished yet")
	// A transação não pode passar por Apply (por exemplo, OPENING).
	ErrNotApplicable = errors.New("wager: transaction cannot be applied")
	// A carteira informada não é a da transação (erro de programação).
	ErrWalletMismatch = errors.New("wager: wallet does not belong to the transaction")
)

// Rejection é uma recusa de negócio definitiva: a operação é válida como
// requisição, mas não pode ser aplicada. Quem chama deve registrar a transação
// como REJECTED com o código informado.
type Rejection struct {
	Code FailureCode
	Err  error
}

func (r *Rejection) Error() string {
	if r.Err == nil {
		return fmt.Sprintf("wager: rejected (%s)", r.Code)
	}
	return fmt.Sprintf("wager: rejected (%s): %v", r.Code, r.Err)
}

func (r *Rejection) Unwrap() error { return r.Err }

func reject(code FailureCode, err error) error {
	return &Rejection{Code: code, Err: err}
}

// Reference é a transação referenciada por um WIN, LOSS, REFUND ou ROLLBACK,
// localizada pelo chamador a partir de (provedor, referenceExternalId).
// AlreadyReversed informa se ela já tem uma reversão bem-sucedida.
type Reference struct {
	Tx              *Transaction
	AlreadyReversed bool
}

// Apply aplica as regras financeiras de tx sobre a carteira w e devolve a
// movimentação feita (nil quando não há movimentação, como em LOSS).
//
// Resultados possíveis:
//   - sucesso: a carteira foi alterada; o chamador grava o ledger e conclui
//     a transação (SetReference, se houver referência, e Process);
//   - *Rejection: recusa definitiva, sem alterar a carteira; o chamador conclui
//     a transação com Reject usando o código da recusa;
//   - ErrReferenceNotFound ou ErrReferencePending: a referência ainda não está
//     disponível; o chamador agenda nova tentativa (MarkPendingReference) e, ao
//     esgotar o prazo, rejeita com REFERENCE_NOT_FOUND ou REFERENCE_NOT_PROCESSED;
//   - qualquer outro erro: falha técnica ou erro de programação.
//
// Apply não altera tx nem persiste nada.
func Apply(tx *Transaction, w *wallet.Wallet, ref *Reference, now time.Time) (*wallet.Movement, error) {
	s := tx.data
	if s.Status.IsTerminal() {
		return nil, fmt.Errorf("%w: %s", ErrTerminalState, s.Status)
	}
	if tx.IsInternal() {
		return nil, fmt.Errorf("%w: OPENING is created together with the wallet", ErrNotApplicable)
	}
	if s.WalletID != w.ID() || s.PlayerID != w.PlayerID() {
		return nil, ErrWalletMismatch
	}
	if s.Amount.Currency() != w.Currency() {
		return nil, reject(FailureCurrencyMismatch, money.ErrCurrencyMismatch)
	}

	var referenced *Transaction
	if s.ReferenceExternalID != "" {
		r, err := resolveReference(s, ref)
		if err != nil {
			return nil, err
		}
		referenced = r
	}

	switch s.Kind {
	case KindBet:
		return debit(w, s.Amount, FailureInsufficientFunds, now)

	case KindWin:
		if referenced != nil {
			if err := requireKind(referenced, KindBet); err != nil {
				return nil, err
			}
		}
		return credit(w, s.Amount, now)

	case KindLoss:
		if referenced != nil {
			if err := requireKind(referenced, KindBet); err != nil {
				return nil, err
			}
		}
		return nil, nil

	case KindRefund:
		if err := requireReversible(s, ref, referenced, KindBet); err != nil {
			return nil, err
		}
		return credit(w, s.Amount, now)

	case KindRollback:
		if err := requireReversible(s, ref, referenced, KindBet, KindWin, KindRefund); err != nil {
			return nil, err
		}
		// Desfaz o efeito da transação referenciada: uma aposta foi um débito
		// (volta como crédito); um ganho e um estorno foram créditos (voltam
		// como débito, que pode não ter saldo).
		if referenced.data.Kind == KindBet {
			return credit(w, s.Amount, now)
		}
		return debit(w, s.Amount, FailureReversalInsufficientFunds, now)
	}
	return nil, fmt.Errorf("%w: unsupported kind %s", ErrNotApplicable, s.Kind)
}

// resolveReference confere se a referência está disponível, terminou com
// sucesso e pertence ao mesmo contexto (provedor, jogador, carteira, moeda,
// rodada) da transação.
func resolveReference(s Snapshot, ref *Reference) (*Transaction, error) {
	if ref == nil || ref.Tx == nil {
		return nil, ErrReferenceNotFound
	}
	r := ref.Tx.data

	switch r.Status {
	case StatusPending, StatusPendingReference:
		return nil, ErrReferencePending
	case StatusRejected, StatusFailed:
		return nil, reject(FailureReferenceNotProcessed,
			fmt.Errorf("referenced transaction ended as %s", r.Status))
	}

	if r.ID == s.ID ||
		r.ExternalID != s.ReferenceExternalID ||
		r.ProviderID != s.ProviderID ||
		r.PlayerID != s.PlayerID ||
		r.WalletID != s.WalletID ||
		r.RoundID != s.RoundID ||
		r.Amount.Currency() != s.Amount.Currency() {
		return nil, reject(FailureReferenceMismatch,
			errors.New("referenced transaction belongs to another provider, player, wallet, currency or round"))
	}
	return ref.Tx, nil
}

func requireKind(r *Transaction, kinds ...Kind) error {
	for _, k := range kinds {
		if r.data.Kind == k {
			return nil
		}
	}
	return reject(FailureInvalidReferenceKind,
		fmt.Errorf("referenced transaction is a %s", r.data.Kind))
}

// requireReversible vale para REFUND e ROLLBACK: o tipo referenciado deve ser
// um dos aceitos, a referência não pode ter sido revertida antes e o valor
// deve ser igual ao referenciado (não há reversão parcial).
func requireReversible(s Snapshot, ref *Reference, r *Transaction, kinds ...Kind) error {
	if err := requireKind(r, kinds...); err != nil {
		return err
	}
	if ref.AlreadyReversed {
		return reject(FailureAlreadyReversed,
			errors.New("referenced transaction already has a successful reversal"))
	}
	if s.Amount != r.data.Amount {
		return reject(FailureReferenceAmountMismatch,
			fmt.Errorf("reversal amount %s differs from referenced amount %s", s.Amount, r.data.Amount))
	}
	return nil
}

func debit(w *wallet.Wallet, amount money.Money, insufficient FailureCode, now time.Time) (*wallet.Movement, error) {
	mv, err := w.Debit(amount, now)
	if err != nil {
		if errors.Is(err, wallet.ErrInsufficientFunds) {
			return nil, reject(insufficient, err)
		}
		return nil, err
	}
	return &mv, nil
}

func credit(w *wallet.Wallet, amount money.Money, now time.Time) (*wallet.Movement, error) {
	mv, err := w.Credit(amount, now)
	if err != nil {
		if errors.Is(err, money.ErrOverflow) {
			return nil, reject(FailureBalanceLimitExceeded, err)
		}
		return nil, err
	}
	return &mv, nil
}
