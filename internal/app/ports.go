// Package app contém os casos de uso. Ele depende apenas do domínio e das
// interfaces ("portas") declaradas aqui; os adaptadores (PostgreSQL, SQS,
// HTTP) implementam essas interfaces.
package app

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/leal-k/testeTecnico/internal/domain/event"
	"github.com/leal-k/testeTecnico/internal/domain/ledger"
	"github.com/leal-k/testeTecnico/internal/domain/wager"
	"github.com/leal-k/testeTecnico/internal/domain/wallet"
)

var (
	// ErrNotFound é devolvido pelo Store quando o registro não existe.
	ErrNotFound = errors.New("app: not found")
	// ErrVersionConflict indica que a versão da carteira mudou entre a leitura
	// e a gravação. Com o lock da carteira não deveria ocorrer.
	ErrVersionConflict = errors.New("app: wallet version conflict")
	// ErrConcurrentInsert indica que outra requisição gravou a mesma transação
	// (mesma chave de idempotência ou mesmo externalId) no meio do caminho. O
	// caso de uso refaz a operação, que então enxerga a transação existente.
	ErrConcurrentInsert = errors.New("app: a concurrent request stored the same transaction")
)

// Store reúne as operações de persistência dentro de UMA transação de banco.
// Tudo o que um caso de uso grava por ele é confirmado ou desfeito junto.
type Store interface {
	// LockWallet lê a carteira travando a linha (FOR UPDATE). Serializa as
	// operações da mesma carteira sem bloquear as demais.
	LockWallet(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error)
	// SaveWallet grava o novo saldo e a nova versão, conferindo a versão anterior.
	SaveWallet(ctx context.Context, w *wallet.Wallet, expectedVersion int64) error
	// InsertWallet grava uma carteira nova. Devolve false, sem erro, se já
	// existir carteira para o mesmo (jogador, moeda).
	InsertWallet(ctx context.Context, w *wallet.Wallet) (bool, error)

	FindTransactionByKey(ctx context.Context, providerID, idempotencyKey string) (*wager.Transaction, error)
	FindTransactionByExternalID(ctx context.Context, providerID, externalID string) (*wager.Transaction, error)
	GetTransaction(ctx context.Context, id uuid.UUID) (*wager.Transaction, error)
	// ResolveReference localiza a transação referenciada. Devolve nil, nil
	// quando ela não existe.
	ResolveReference(ctx context.Context, providerID, referenceExternalID string) (*wager.Reference, error)

	// InsertTransaction grava uma transação nova. Devolve false, sem erro, se já
	// existir uma com a mesma chave de idempotência ou o mesmo externalId.
	InsertTransaction(ctx context.Context, tx *wager.Transaction) (bool, error)
	UpdateTransaction(ctx context.Context, tx *wager.Transaction) error

	InsertLedgerEntry(ctx context.Context, e ledger.Entry) error
	AddOutboxEvent(ctx context.Context, e event.Event) error
}

// TxManager executa fn dentro de uma transação de banco: confirma se fn
// retornar nil e desfaz tudo se retornar erro.
type TxManager interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context, s Store) error) error
}

// PendingLister lista as transações PENDING_REFERENCE cuja próxima tentativa
// já venceu. É usada pelo worker de referências pendentes.
type PendingLister interface {
	ListDuePending(ctx context.Context, now time.Time, limit int) ([]uuid.UUID, error)
}
