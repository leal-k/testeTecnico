package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/leal-k/testeTecnico/internal/domain/event"
	"github.com/leal-k/testeTecnico/internal/domain/ledger"
	"github.com/leal-k/testeTecnico/internal/domain/money"
	"github.com/leal-k/testeTecnico/internal/domain/wager"
	"github.com/leal-k/testeTecnico/internal/domain/wallet"
)

// ---- banco em memória (apenas para testar a orquestração) -----------------
//
// Imita o comportamento transacional: se a função falhar, TUDO o que ela
// gravou é desfeito. A concorrência e o lock real são testados depois, contra
// um PostgreSQL de verdade.

var (
	_ Store     = (*fakeStore)(nil)
	_ TxManager = (*fakeDB)(nil)
)

type walletRow struct {
	playerID uuid.UUID
	balance  money.Money
	version  int64
	created  time.Time
	updated  time.Time
}

// fakeControl guarda controles de teste que NÃO são desfeitos no rollback.
type fakeControl struct {
	failLedger   error
	conflictOnce bool
	competitors  []wager.Snapshot
}

type fakeDB struct {
	wallets map[uuid.UUID]walletRow
	txs     map[uuid.UUID]wager.Snapshot
	entries []ledger.Entry
	outbox  []event.Event
	inbox   map[string]string // consumer|messageId -> hash
	ctl     *fakeControl
}

func newFakeDB() *fakeDB {
	return &fakeDB{
		wallets: map[uuid.UUID]walletRow{},
		txs:     map[uuid.UUID]wager.Snapshot{},
		inbox:   map[string]string{},
		ctl:     &fakeControl{},
	}
}

func (db *fakeDB) clone() *fakeDB {
	c := &fakeDB{
		wallets: make(map[uuid.UUID]walletRow, len(db.wallets)),
		txs:     make(map[uuid.UUID]wager.Snapshot, len(db.txs)),
		entries: append([]ledger.Entry(nil), db.entries...),
		outbox:  append([]event.Event(nil), db.outbox...),
		inbox:   make(map[string]string, len(db.inbox)),
		ctl:     db.ctl,
	}
	for k, v := range db.wallets {
		c.wallets[k] = v
	}
	for k, v := range db.txs {
		c.txs[k] = v
	}
	for k, v := range db.inbox {
		c.inbox[k] = v
	}
	return c
}

func (db *fakeDB) WithinTx(ctx context.Context, fn func(ctx context.Context, s Store) error) error {
	backup := db.clone()
	if err := fn(ctx, &fakeStore{db: db}); err != nil {
		*db = *backup
		// transações gravadas por "outra requisição" sobrevivem ao rollback
		for _, c := range db.ctl.competitors {
			db.txs[c.ID] = c
		}
		db.ctl.competitors = nil
		return err
	}
	return nil
}

type fakeStore struct {
	db *fakeDB
}

func (s *fakeStore) LockWallet(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	row, ok := s.db.wallets[id]
	if !ok {
		return nil, ErrNotFound
	}
	return wallet.Rehydrate(id, row.playerID, row.balance, row.version, row.created, row.updated)
}

func (s *fakeStore) SaveWallet(ctx context.Context, w *wallet.Wallet, expectedVersion int64) error {
	row, ok := s.db.wallets[w.ID()]
	if !ok {
		return ErrNotFound
	}
	if row.version != expectedVersion {
		return ErrVersionConflict
	}
	row.balance = w.Balance()
	row.version = w.Version()
	row.updated = w.UpdatedAt()
	s.db.wallets[w.ID()] = row
	return nil
}

func (s *fakeStore) InsertWallet(ctx context.Context, w *wallet.Wallet) (bool, error) {
	for _, row := range s.db.wallets {
		if row.playerID == w.PlayerID() && row.balance.Currency() == w.Currency() {
			return false, nil
		}
	}
	s.db.wallets[w.ID()] = walletRow{
		playerID: w.PlayerID(),
		balance:  w.Balance(),
		version:  w.Version(),
		created:  w.CreatedAt(),
		updated:  w.UpdatedAt(),
	}
	return true, nil
}

func (s *fakeStore) find(match func(wager.Snapshot) bool) (*wager.Transaction, error) {
	for _, snap := range s.db.txs {
		if match(snap) {
			return wager.Rehydrate(snap)
		}
	}
	return nil, ErrNotFound
}

func (s *fakeStore) FindTransactionByKey(ctx context.Context, providerID, idempotencyKey string) (*wager.Transaction, error) {
	return s.find(func(x wager.Snapshot) bool {
		return x.ProviderID == providerID && x.IdempotencyKey == idempotencyKey
	})
}

func (s *fakeStore) FindTransactionByExternalID(ctx context.Context, providerID, externalID string) (*wager.Transaction, error) {
	return s.find(func(x wager.Snapshot) bool {
		return x.ProviderID == providerID && x.ExternalID == externalID
	})
}

func (s *fakeStore) GetTransaction(ctx context.Context, id uuid.UUID) (*wager.Transaction, error) {
	snap, ok := s.db.txs[id]
	if !ok {
		return nil, ErrNotFound
	}
	return wager.Rehydrate(snap)
}

func (s *fakeStore) ResolveReference(ctx context.Context, providerID, referenceExternalID string) (*wager.Reference, error) {
	tx, err := s.find(func(x wager.Snapshot) bool {
		return x.ProviderID == providerID && x.ExternalID == referenceExternalID
	})
	if errors.Is(err, ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	reversed := false
	for _, x := range s.db.txs {
		isReversal := x.Kind == wager.KindRefund || x.Kind == wager.KindRollback
		if isReversal && x.Status == wager.StatusProcessed && x.ReferenceID == tx.ID() {
			reversed = true
		}
	}
	return &wager.Reference{Tx: tx, AlreadyReversed: reversed}, nil
}

func (s *fakeStore) InsertTransaction(ctx context.Context, tx *wager.Transaction) (bool, error) {
	snap := tx.Snapshot()

	if s.db.ctl.conflictOnce {
		// simula outra requisição que gravou a mesma operação no meio do caminho
		s.db.ctl.conflictOnce = false
		competitor := snap
		competitor.ID = uuid.New()
		competitor.Status = wager.StatusProcessed
		competitor.ResultBalance = snap.Amount
		competitor.NextAttemptAt = time.Time{}
		s.db.ctl.competitors = append(s.db.ctl.competitors, competitor)
		s.db.txs[competitor.ID] = competitor
		return false, nil
	}

	for _, x := range s.db.txs {
		sameKey := x.ProviderID == snap.ProviderID && x.IdempotencyKey == snap.IdempotencyKey
		sameExternal := x.ProviderID == snap.ProviderID && x.ExternalID == snap.ExternalID
		if sameKey || sameExternal {
			return false, nil
		}
	}
	s.db.txs[snap.ID] = snap
	return true, nil
}

func (s *fakeStore) UpdateTransaction(ctx context.Context, tx *wager.Transaction) error {
	snap := tx.Snapshot()
	if _, ok := s.db.txs[snap.ID]; !ok {
		return ErrNotFound
	}
	s.db.txs[snap.ID] = snap
	return nil
}

func (s *fakeStore) InsertLedgerEntry(ctx context.Context, e ledger.Entry) error {
	if s.db.ctl.failLedger != nil {
		return s.db.ctl.failLedger
	}
	s.db.entries = append(s.db.entries, e)
	return nil
}

func (s *fakeStore) AddOutboxEvent(ctx context.Context, e event.Event) error {
	s.db.outbox = append(s.db.outbox, e)
	return nil
}

func (s *fakeStore) RecordInbox(ctx context.Context, rec InboxRecord, at time.Time) (bool, string, error) {
	key := rec.Consumer + "|" + rec.MessageID
	if hash, ok := s.db.inbox[key]; ok {
		return false, hash, nil
	}
	s.db.inbox[key] = rec.PayloadHash
	return true, "", nil
}

// ---- fixture e auxiliares ---------------------------------------------------

type fakeClock struct {
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.now = c.now.Add(d)
}

type fixture struct {
	t        *testing.T
	db       *fakeDB
	clock    *fakeClock
	proc     *Processor
	walletID uuid.UUID
	playerID uuid.UUID
}

func mustBRL(t *testing.T, amount string) money.Money {
	t.Helper()
	m, err := money.Parse(amount, "BRL")
	if err != nil {
		t.Fatalf("Parse(%q): %v", amount, err)
	}
	return m
}

func newFixture(t *testing.T, balance string) *fixture {
	t.Helper()
	clock := &fakeClock{now: time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)}
	db := newFakeDB()

	w, _, err := wallet.Open(uuid.New(), uuid.New(), mustBRL(t, balance), clock.Now())
	if err != nil {
		t.Fatalf("wallet.Open: %v", err)
	}
	db.wallets[w.ID()] = walletRow{
		playerID: w.PlayerID(),
		balance:  w.Balance(),
		version:  w.Version(),
		created:  w.CreatedAt(),
		updated:  w.UpdatedAt(),
	}

	cfg := Config{ReferenceTTL: 5 * time.Minute, BackoffBase: time.Second, BackoffMax: time.Minute}
	return &fixture{
		t:        t,
		db:       db,
		clock:    clock,
		proc:     NewProcessor(db, cfg, clock.Now, uuid.New),
		walletID: w.ID(),
		playerID: w.PlayerID(),
	}
}

func (f *fixture) cmd(kind wager.Kind, amount, ext, key string) ProcessCommand {
	f.t.Helper()
	return ProcessCommand{
		ProviderID:     "provider-a",
		IdempotencyKey: key,
		ExternalID:     ext,
		Kind:           kind,
		WalletID:       f.walletID,
		PlayerID:       f.playerID,
		RoundID:        "round-1",
		GameID:         "game-1",
		Amount:         mustBRL(f.t, amount),
		CorrelationID:  "corr-" + key,
	}
}

func (f *fixture) refCmd(kind wager.Kind, amount, ext, key, refExt string) ProcessCommand {
	f.t.Helper()
	c := f.cmd(kind, amount, ext, key)
	c.ReferenceExternalID = refExt
	return c
}

func (f *fixture) process(cmd ProcessCommand) Result {
	f.t.Helper()
	res, err := f.proc.Process(context.Background(), cmd)
	if err != nil {
		f.t.Fatalf("Process(%s %s): %v", cmd.Kind, cmd.ExternalID, err)
	}
	return res
}

func (f *fixture) retry(id uuid.UUID) {
	f.t.Helper()
	if err := f.proc.RetryPending(context.Background(), id); err != nil {
		f.t.Fatalf("RetryPending: %v", err)
	}
}

func (f *fixture) balance() string {
	return f.db.wallets[f.walletID].balance.Amount()
}

func (f *fixture) version() int64 {
	return f.db.wallets[f.walletID].version
}

func typesOf(evs []event.Event) []event.Type {
	out := make([]event.Type, len(evs))
	for i, e := range evs {
		out[i] = e.Type
	}
	return out
}

func assertTypes(t *testing.T, got []event.Event, want ...event.Type) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("outbox types = %v, want %v", typesOf(got), want)
	}
	for i := range want {
		if got[i].Type != want[i] {
			t.Fatalf("outbox types = %v, want %v", typesOf(got), want)
		}
	}
}

// ---- testes -----------------------------------------------------------------

func TestProcess_Bet_AppliesEverythingTogether(t *testing.T) {
	f := newFixture(t, "100.00")

	res := f.process(f.cmd(wager.KindBet, "80.00", "ext-1", "key-1"))

	if res.Status != wager.StatusProcessed || res.Replay || res.Balance.Amount() != "20.00" {
		t.Errorf("result = %+v", res)
	}
	if f.balance() != "20.00" || f.version() != 2 {
		t.Errorf("wallet = %s v%d, want 20.00 v2", f.balance(), f.version())
	}
	if len(f.db.entries) != 1 {
		t.Fatalf("ledger entries = %d, want 1", len(f.db.entries))
	}
	e := f.db.entries[0]
	if e.Direction() != wallet.DirectionDebit || e.BalanceBefore().Amount() != "100.00" ||
		e.BalanceAfter().Amount() != "20.00" || e.TransactionID() != res.TransactionID {
		t.Errorf("ledger entry = %+v", e)
	}
	stored := f.db.txs[res.TransactionID]
	if stored.Status != wager.StatusProcessed || stored.ResultBalance.Amount() != "20.00" {
		t.Errorf("stored transaction = %+v", stored)
	}
	assertTypes(t, f.db.outbox, event.TypeWalletBalanceChanged, event.TypeWagerTransactionProcessed)
}

func TestProcess_Replay_ReturnsStoredResultWithoutReapplying(t *testing.T) {
	f := newFixture(t, "100.00")
	cmd := f.cmd(wager.KindBet, "80.00", "ext-1", "key-1")

	first := f.process(cmd)
	second := f.process(cmd)

	if !second.Replay || second.TransactionID != first.TransactionID ||
		second.Status != wager.StatusProcessed || second.Balance.Amount() != "20.00" {
		t.Errorf("replay = %+v, first = %+v", second, first)
	}
	if f.balance() != "20.00" || f.version() != 2 || len(f.db.entries) != 1 {
		t.Errorf("replay changed state: %s v%d entries=%d", f.balance(), f.version(), len(f.db.entries))
	}
	if len(f.db.outbox) != 2 {
		t.Errorf("replay must not publish new events, outbox has %d", len(f.db.outbox))
	}
}

func TestProcess_Conflicts(t *testing.T) {
	f := newFixture(t, "100.00")
	ctx := context.Background()
	f.process(f.cmd(wager.KindBet, "10.00", "ext-1", "key-1"))

	tests := []struct {
		name string
		cmd  ProcessCommand
		want error
	}{
		{"same key, different amount", f.cmd(wager.KindBet, "11.00", "ext-1", "key-1"), ErrIdempotencyConflict},
		{"same key, different external id", f.cmd(wager.KindBet, "10.00", "ext-2", "key-1"), ErrIdempotencyConflict},
		{"same external id, different key", f.cmd(wager.KindBet, "10.00", "ext-1", "key-2"), ErrExternalIDConflict},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := f.proc.Process(ctx, tc.cmd); !errors.Is(err, tc.want) {
				t.Errorf("error = %v, want %v", err, tc.want)
			}
		})
	}
	if f.balance() != "90.00" || len(f.db.txs) != 1 || len(f.db.entries) != 1 {
		t.Errorf("conflicts changed state: %s txs=%d entries=%d", f.balance(), len(f.db.txs), len(f.db.entries))
	}
}

func TestProcess_InsufficientFunds_IsStoredAsRejectedAndReplayed(t *testing.T) {
	f := newFixture(t, "100.00")
	cmd := f.cmd(wager.KindBet, "100.01", "ext-1", "key-1")

	res := f.process(cmd)
	if res.Status != wager.StatusRejected || res.FailureCode != wager.FailureInsufficientFunds ||
		res.Balance.Amount() != "100.00" {
		t.Errorf("result = %+v", res)
	}
	if f.balance() != "100.00" || f.version() != 1 || len(f.db.entries) != 0 {
		t.Errorf("rejection changed state: %s v%d entries=%d", f.balance(), f.version(), len(f.db.entries))
	}
	assertTypes(t, f.db.outbox, event.TypeWagerTransactionRejected)

	// O saldo aumenta depois; o replay continua devolvendo a recusa original.
	f.process(f.cmd(wager.KindWin, "50.00", "ext-2", "key-2"))
	again := f.process(cmd)
	if !again.Replay || again.Status != wager.StatusRejected || again.FailureCode != wager.FailureInsufficientFunds {
		t.Errorf("replay of a rejection = %+v", again)
	}
	if f.balance() != "150.00" {
		t.Errorf("replay must not debit: balance = %s", f.balance())
	}
}

// Cenário do README, ponta a ponta no caso de uso.
func TestProcess_TwoBetsOfEightyOnHundred(t *testing.T) {
	f := newFixture(t, "100.00")

	first := f.process(f.cmd(wager.KindBet, "80.00", "ext-1", "key-1"))
	second := f.process(f.cmd(wager.KindBet, "80.00", "ext-2", "key-2"))

	if first.Status != wager.StatusProcessed {
		t.Errorf("first = %+v", first)
	}
	if second.Status != wager.StatusRejected || second.FailureCode != wager.FailureInsufficientFunds {
		t.Errorf("second = %+v", second)
	}
	if f.balance() != "20.00" || len(f.db.entries) != 1 {
		t.Errorf("final state: %s entries=%d, want 20.00 and 1 debit", f.balance(), len(f.db.entries))
	}
}

func TestProcess_Loss_HasNoLedgerAndNoBalanceEvent(t *testing.T) {
	f := newFixture(t, "50.00")

	res := f.process(f.cmd(wager.KindLoss, "0.00", "ext-1", "key-1"))

	if res.Status != wager.StatusProcessed || res.Balance.Amount() != "50.00" {
		t.Errorf("result = %+v", res)
	}
	if f.version() != 1 || len(f.db.entries) != 0 {
		t.Errorf("LOSS must not move the wallet: v%d entries=%d", f.version(), len(f.db.entries))
	}
	assertTypes(t, f.db.outbox, event.TypeWagerTransactionProcessed)
}

func TestProcess_InvalidInputs_PersistNothing(t *testing.T) {
	f := newFixture(t, "100.00")
	ctx := context.Background()
	usd, err := money.Parse("10.00", "USD")
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		mutate func(c *ProcessCommand)
		want   error
	}{
		{"wallet not found", func(c *ProcessCommand) { c.WalletID = uuid.New() }, ErrWalletNotFound},
		{"wallet of another player", func(c *ProcessCommand) { c.PlayerID = uuid.New() }, ErrWalletPlayerMismatch},
		{"currency different from the wallet", func(c *ProcessCommand) { c.Amount = usd }, ErrCurrencyMismatch},
		{"zero amount on a BET", func(c *ProcessCommand) { c.Amount = mustBRL(t, "0.00") }, ErrInvalidCommand},
		{"missing idempotency key", func(c *ProcessCommand) { c.IdempotencyKey = "" }, ErrInvalidCommand},
		{"missing external id", func(c *ProcessCommand) { c.ExternalID = "" }, ErrInvalidCommand},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cmd := f.cmd(wager.KindBet, "10.00", "ext-1", "key-1")
			tc.mutate(&cmd)
			if _, err := f.proc.Process(ctx, cmd); !errors.Is(err, tc.want) {
				t.Errorf("error = %v, want %v", err, tc.want)
			}
			if len(f.db.txs) != 0 || len(f.db.outbox) != 0 || f.balance() != "100.00" {
				t.Errorf("invalid input persisted something")
			}
		})
	}
}

func TestProcess_TechnicalFailure_RollsBackEverything(t *testing.T) {
	f := newFixture(t, "100.00")
	cmd := f.cmd(wager.KindBet, "80.00", "ext-1", "key-1")
	boom := errors.New("disk full")

	// O ledger falha DEPOIS de a transação e o saldo terem sido gravados.
	f.db.ctl.failLedger = boom
	if _, err := f.proc.Process(context.Background(), cmd); !errors.Is(err, boom) {
		t.Fatalf("error = %v, want %v", err, boom)
	}
	if f.balance() != "100.00" || f.version() != 1 {
		t.Errorf("wallet = %s v%d, want 100.00 v1 after rollback", f.balance(), f.version())
	}
	if len(f.db.txs) != 0 || len(f.db.entries) != 0 || len(f.db.outbox) != 0 {
		t.Errorf("rollback left data behind: txs=%d entries=%d outbox=%d",
			len(f.db.txs), len(f.db.entries), len(f.db.outbox))
	}

	// Com a falha resolvida, o mesmo comando funciona normalmente.
	f.db.ctl.failLedger = nil
	res := f.process(cmd)
	if res.Status != wager.StatusProcessed || res.Replay || f.balance() != "20.00" {
		t.Errorf("retry after failure = %+v, balance %s", res, f.balance())
	}
}

func TestProcess_ConcurrentInsert_IsRetriedAsReplay(t *testing.T) {
	f := newFixture(t, "100.00")
	f.db.ctl.conflictOnce = true

	res := f.process(f.cmd(wager.KindBet, "80.00", "ext-1", "key-1"))

	if !res.Replay || res.Status != wager.StatusProcessed {
		t.Errorf("result = %+v, want a replay of the concurrent request", res)
	}
	// Quem gravou foi a "outra requisição"; esta não debitou nada.
	if f.balance() != "100.00" || len(f.db.entries) != 0 || len(f.db.txs) != 1 {
		t.Errorf("state: %s entries=%d txs=%d", f.balance(), len(f.db.entries), len(f.db.txs))
	}
}

func TestProcess_PendingReference_IsResolvedWhenTheReferenceArrives(t *testing.T) {
	f := newFixture(t, "100.00")
	start := f.clock.Now()

	// O REFUND chega antes da aposta que ele referencia.
	pending := f.process(f.refCmd(wager.KindRefund, "80.00", "refund-1", "key-refund", "bet-1"))
	if pending.Status != wager.StatusPendingReference || pending.Balance.Amount() != "100.00" {
		t.Fatalf("pending = %+v", pending)
	}
	stored := f.db.txs[pending.TransactionID]
	if stored.Attempts != 1 || !stored.NextAttemptAt.Equal(start.Add(time.Second)) {
		t.Errorf("attempts=%d next=%v, want 1 and start+1s", stored.Attempts, stored.NextAttemptAt)
	}
	if f.balance() != "100.00" || len(f.db.entries) != 0 {
		t.Errorf("a pending transaction must not move the wallet")
	}

	// Nova tentativa sem a referência: continua pendente, com backoff maior.
	f.clock.Advance(2 * time.Second)
	f.retry(pending.TransactionID)
	stored = f.db.txs[pending.TransactionID]
	if stored.Status != wager.StatusPendingReference || stored.Attempts != 2 ||
		!stored.NextAttemptAt.Equal(f.clock.Now().Add(2*time.Second)) {
		t.Errorf("after retry: %s attempts=%d next=%v", stored.Status, stored.Attempts, stored.NextAttemptAt)
	}

	// A aposta chega; a próxima tentativa conclui o REFUND.
	bet := f.process(f.cmd(wager.KindBet, "80.00", "bet-1", "key-bet"))
	if bet.Status != wager.StatusProcessed || f.balance() != "20.00" {
		t.Fatalf("bet = %+v, balance %s", bet, f.balance())
	}
	f.clock.Advance(3 * time.Second)
	f.retry(pending.TransactionID)

	stored = f.db.txs[pending.TransactionID]
	if stored.Status != wager.StatusProcessed || stored.ReferenceID != bet.TransactionID {
		t.Errorf("refund = %s ref=%s, want PROCESSED referencing the bet", stored.Status, stored.ReferenceID)
	}
	if f.balance() != "100.00" || f.version() != 3 || len(f.db.entries) != 2 {
		t.Errorf("wallet = %s v%d entries=%d, want 100.00 v3 and 2 entries", f.balance(), f.version(), len(f.db.entries))
	}
	assertTypes(t, f.db.outbox,
		event.TypeWagerTransactionPendingReference, // 1ª tentativa
		event.TypeWagerTransactionPendingReference, // retry sem referência
		event.TypeWalletBalanceChanged,             // aposta
		event.TypeWagerTransactionProcessed,
		event.TypeWalletBalanceChanged, // refund concluído
		event.TypeWagerTransactionProcessed,
	)
}

func TestProcess_PendingReference_ExpiresWhenTheReferenceNeverArrives(t *testing.T) {
	f := newFixture(t, "100.00")
	pending := f.process(f.refCmd(wager.KindRefund, "80.00", "refund-1", "key-refund", "bet-1"))

	f.clock.Advance(6 * time.Minute) // além do ReferenceTTL de 5 minutos
	f.retry(pending.TransactionID)

	stored := f.db.txs[pending.TransactionID]
	if stored.Status != wager.StatusRejected || stored.FailureCode != wager.FailureReferenceNotFound {
		t.Errorf("stored = %s %s, want REJECTED REFERENCE_NOT_FOUND", stored.Status, stored.FailureCode)
	}
	if !stored.NextAttemptAt.IsZero() {
		t.Errorf("a terminal transaction must not keep a next attempt")
	}
	if f.balance() != "100.00" || len(f.db.entries) != 0 {
		t.Errorf("expired reference moved the wallet")
	}
}

func TestProcess_PendingReference_ExpiresWhenTheReferenceStaysPending(t *testing.T) {
	f := newFixture(t, "100.00")
	// rb-1 espera uma referência que nunca chega; o REFUND referencia rb-1.
	f.process(f.refCmd(wager.KindRollback, "10.00", "rb-1", "key-rb", "missing"))
	refund := f.process(f.refCmd(wager.KindRefund, "10.00", "refund-1", "key-refund", "rb-1"))
	if refund.Status != wager.StatusPendingReference {
		t.Fatalf("refund = %+v, want PENDING_REFERENCE", refund)
	}

	f.clock.Advance(6 * time.Minute)
	f.retry(refund.TransactionID)

	stored := f.db.txs[refund.TransactionID]
	if stored.Status != wager.StatusRejected || stored.FailureCode != wager.FailureReferenceNotProcessed {
		t.Errorf("stored = %s %s, want REJECTED REFERENCE_NOT_PROCESSED", stored.Status, stored.FailureCode)
	}
}

func TestProcess_SecondReversalOfTheSameTransactionIsRejected(t *testing.T) {
	f := newFixture(t, "100.00")
	f.process(f.cmd(wager.KindBet, "80.00", "bet-1", "key-bet"))

	refund := f.process(f.refCmd(wager.KindRefund, "80.00", "refund-1", "key-refund", "bet-1"))
	if refund.Status != wager.StatusProcessed || f.balance() != "100.00" {
		t.Fatalf("refund = %+v, balance %s", refund, f.balance())
	}

	rollback := f.process(f.refCmd(wager.KindRollback, "80.00", "rb-1", "key-rb", "bet-1"))
	if rollback.Status != wager.StatusRejected || rollback.FailureCode != wager.FailureAlreadyReversed {
		t.Errorf("rollback = %+v, want REJECTED ALREADY_REVERSED", rollback)
	}
	if f.balance() != "100.00" {
		t.Errorf("the bet was given back twice: balance = %s", f.balance())
	}
}

func TestRetryPending_IgnoresUnknownAndFinishedTransactions(t *testing.T) {
	f := newFixture(t, "100.00")

	f.retry(uuid.New()) // não existe: nada a fazer, sem erro

	res := f.process(f.cmd(wager.KindBet, "10.00", "ext-1", "key-1"))
	outboxBefore := len(f.db.outbox)
	f.retry(res.TransactionID) // já PROCESSED

	if f.db.txs[res.TransactionID].Status != wager.StatusProcessed || len(f.db.outbox) != outboxBefore {
		t.Errorf("retry touched a finished transaction")
	}
}

func withInbox(c ProcessCommand, messageID, hash string) ProcessCommand {
	c.Inbox = &InboxRecord{Consumer: "wager-transactions", MessageID: messageID, PayloadHash: hash}
	return c
}

func TestProcess_Inbox_RecordedTogetherWithTheOperation(t *testing.T) {
	f := newFixture(t, "100.00")
	res := f.process(withInbox(f.cmd(wager.KindBet, "10.00", "ext-1", "key-1"), "msg-1", "h1"))

	if res.Status != wager.StatusProcessed || res.DuplicateMessage {
		t.Errorf("result = %+v", res)
	}
	if f.db.inbox["wager-transactions|msg-1"] != "h1" {
		t.Errorf("inbox = %v", f.db.inbox)
	}
}

func TestProcess_Inbox_RedeliveryIsADuplicateWithoutReapplying(t *testing.T) {
	f := newFixture(t, "100.00")
	cmd := withInbox(f.cmd(wager.KindBet, "10.00", "ext-1", "key-1"), "msg-1", "h1")

	first := f.process(cmd)
	second := f.process(cmd)

	if !second.DuplicateMessage || !second.Replay || second.TransactionID != first.TransactionID {
		t.Errorf("redelivery = %+v", second)
	}
	if f.balance() != "90.00" || len(f.db.entries) != 1 {
		t.Errorf("redelivery debited again: %s entries=%d", f.balance(), len(f.db.entries))
	}
}

func TestProcess_Inbox_SameMessageIDWithAnotherPayloadIsAConflict(t *testing.T) {
	f := newFixture(t, "100.00")
	f.process(withInbox(f.cmd(wager.KindBet, "10.00", "ext-1", "key-1"), "msg-1", "h1"))

	_, err := f.proc.Process(context.Background(), withInbox(f.cmd(wager.KindBet, "20.00", "ext-2", "key-2"), "msg-1", "h2"))
	if !errors.Is(err, ErrInboxConflict) {
		t.Errorf("error = %v, want ErrInboxConflict", err)
	}
	if f.balance() != "90.00" || len(f.db.txs) != 1 {
		t.Errorf("the conflicting message changed state: %s txs=%d", f.balance(), len(f.db.txs))
	}
}

func TestProcess_Inbox_IsRolledBackWhenTheOperationFails(t *testing.T) {
	f := newFixture(t, "100.00")
	f.db.ctl.failLedger = errors.New("disk full")
	cmd := withInbox(f.cmd(wager.KindBet, "10.00", "ext-1", "key-1"), "msg-1", "h1")

	if _, err := f.proc.Process(context.Background(), cmd); err == nil {
		t.Fatal("expected the failure")
	}
	if len(f.db.inbox) != 0 {
		t.Errorf("the inbox kept a message whose operation was rolled back: %v", f.db.inbox)
	}

	// Na reentrega, com o banco de volta, a mensagem é tratada normalmente.
	f.db.ctl.failLedger = nil
	if res := f.process(cmd); res.Status != wager.StatusProcessed || res.DuplicateMessage {
		t.Errorf("redelivery after the failure = %+v", res)
	}
}

func TestProcess_Inbox_SameOperationFromHTTPAndSQS(t *testing.T) {
	f := newFixture(t, "100.00")
	viaHTTP := f.process(f.cmd(wager.KindBet, "10.00", "ext-1", "key-1"))
	viaSQS := f.process(withInbox(f.cmd(wager.KindBet, "10.00", "ext-1", "key-1"), "msg-1", "h1"))

	if !viaSQS.Replay || viaSQS.DuplicateMessage || viaSQS.TransactionID != viaHTTP.TransactionID {
		t.Errorf("SQS after HTTP = %+v", viaSQS)
	}
	if f.balance() != "90.00" {
		t.Errorf("balance = %s, want a single debit", f.balance())
	}
}

func TestBackoff(t *testing.T) {
	p := &Processor{cfg: Config{BackoffBase: time.Second, BackoffMax: time.Minute}}

	tests := []struct {
		attempts int
		want     time.Duration
	}{
		{0, time.Second},
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
		{5, 32 * time.Second},
		{6, time.Minute}, // 64s limitado a 1 minuto
		{7, time.Minute},
		{50, time.Minute},
	}
	for _, tc := range tests {
		if got := p.backoff(tc.attempts); got != tc.want {
			t.Errorf("backoff(%d) = %v, want %v", tc.attempts, got, tc.want)
		}
	}
}
