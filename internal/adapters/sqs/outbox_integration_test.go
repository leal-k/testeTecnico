package sqs_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"

	"github.com/leal-k/testeTecnico/internal/adapters/postgres"
	consumer "github.com/leal-k/testeTecnico/internal/adapters/sqs"
	"github.com/leal-k/testeTecnico/internal/app"
	"github.com/leal-k/testeTecnico/internal/domain/event"
	"github.com/leal-k/testeTecnico/internal/domain/wager"
)

// ---- apoio ------------------------------------------------------------------

func (e *env) eventsQueue() (name, url string) {
	e.t.Helper()
	name = "it-" + uuid.NewString()[:8] + "-events.fifo"
	return name, e.createQueue(name)
}

// clearOutbox marca como publicado o que outros testes deixaram pendente.
func (e *env) clearOutbox() {
	e.t.Helper()
	if _, err := e.pool.Exec(context.Background(),
		`UPDATE outbox_events SET published_at = now(), locked_by = NULL, locked_until = NULL WHERE published_at IS NULL`); err != nil {
		e.t.Fatal(err)
	}
}

// makeEvents processa apostas; cada uma grava 2 eventos na outbox.
func (e *env) makeEvents(bets int) int {
	e.t.Helper()
	w := e.openWallet("1000.00")
	for i := 0; i < bets; i++ {
		ext := fmt.Sprintf("bet-%d", i)
		_, err := e.proc.Process(context.Background(), app.ProcessCommand{
			ProviderID: w.provider, IdempotencyKey: w.provider + ":" + ext, ExternalID: ext,
			Kind: wager.KindBet, WalletID: w.id, PlayerID: w.player, RoundID: "r", GameID: "g",
			Amount: brl(e.t, "1.00"), CorrelationID: ext,
		})
		if err != nil {
			e.t.Fatal(err)
		}
	}
	// + 2 eventos da abertura da carteira
	return bets*2 + 2
}

func (e *env) pendingOutbox() int {
	return e.count(`SELECT count(*) FROM outbox_events WHERE published_at IS NULL`)
}

// receiveAll lê a fila de eventos até ela ficar vazia.
func (e *env) receiveAll(url string) []consumer.EventEnvelope {
	e.t.Helper()
	var out []consumer.EventEnvelope
	for {
		res, err := e.client.ReceiveMessage(context.Background(), &awssqs.ReceiveMessageInput{
			QueueUrl:            aws.String(url),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     1,
		})
		if err != nil {
			e.t.Fatal(err)
		}
		if len(res.Messages) == 0 {
			return out
		}
		for _, m := range res.Messages {
			var env consumer.EventEnvelope
			if err := json.Unmarshal([]byte(aws.ToString(m.Body)), &env); err != nil {
				e.t.Fatalf("event body is not an envelope: %v", err)
			}
			out = append(out, env)
			_, _ = e.client.DeleteMessage(context.Background(), &awssqs.DeleteMessageInput{
				QueueUrl: aws.String(url), ReceiptHandle: m.ReceiptHandle,
			})
		}
	}
}

func relayConfig() app.OutboxConfig {
	cfg := app.DefaultOutboxConfig()
	cfg.Interval = 50 * time.Millisecond
	cfg.BatchSize = 5 // lotes pequenos: mais disputa entre os publishers
	cfg.Lease = 2 * time.Second
	cfg.PublishTimeout = 500 * time.Millisecond
	return cfg
}

// countingPublisher registra cada evento publicado (para provar republicações).
type countingPublisher struct {
	inner app.EventPublisher
	mu    sync.Mutex
	ids   []uuid.UUID
}

func (c *countingPublisher) Publish(ctx context.Context, ev event.Event) error {
	if err := c.inner.Publish(ctx, ev); err != nil {
		return err
	}
	c.mu.Lock()
	c.ids = append(c.ids, ev.ID)
	c.mu.Unlock()
	return nil
}

func (c *countingPublisher) published() []uuid.UUID {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]uuid.UUID(nil), c.ids...)
}

func runRelay(ctx context.Context, wg *sync.WaitGroup, relay *app.OutboxRelay) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		relay.Run(ctx)
	}()
}

// ---- testes -----------------------------------------------------------------

// Eventos confirmados ANTES de qualquer publisher existir (como depois de uma
// queda entre o commit e a publicação) são publicados quando os publishers
// sobem. Dois publishers disputam a outbox: todos os eventos saem, cada um com
// o seu eventId, e o envelope segue o contrato.
func TestIntegration_Outbox_TwoPublishersShareTheWork(t *testing.T) {
	e := newEnv(t)
	e.clearOutbox()
	total := e.makeEvents(8)
	queueName, queueURL := e.eventsQueue()
	db := postgres.New(e.pool)

	pubA := &countingPublisher{inner: consumer.NewPublisher(e.client, queueName)}
	pubB := &countingPublisher{inner: consumer.NewPublisher(e.client, queueName)}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	runRelay(ctx, &wg, app.NewOutboxRelay(db, pubA, "publisher-a", relayConfig(), nil))
	runRelay(ctx, &wg, app.NewOutboxRelay(db, pubB, "publisher-b", relayConfig(), nil))

	e.waitFor("the outbox to drain", func() bool { return e.pendingOutbox() == 0 })
	cancel()
	wg.Wait()

	a, b := pubA.published(), pubB.published()
	if len(a)+len(b) != total {
		t.Errorf("published %d+%d events, want %d in total with no duplicates", len(a), len(b), total)
	}
	t.Logf("publisher-a sent %d events, publisher-b sent %d", len(a), len(b))

	received := e.receiveAll(queueURL)
	byID := map[string]consumer.EventEnvelope{}
	for _, env := range received {
		byID[env.EventID] = env
	}
	if len(byID) != total {
		t.Fatalf("the events queue has %d distinct events, want %d", len(byID), total)
	}

	// Contrato do envelope.
	for _, env := range byID {
		if _, err := uuid.Parse(env.EventID); err != nil {
			t.Errorf("eventId %q", env.EventID)
		}
		if _, err := time.Parse(time.RFC3339Nano, env.OccurredAt); err != nil {
			t.Errorf("occurredAt %q is not RFC 3339", env.OccurredAt)
		}
		if env.Version != 1 || env.AggregateID == "" || env.CorrelationID == "" || len(env.Data) == 0 {
			t.Errorf("envelope = %+v", env)
		}
		if env.EventType == string(event.TypeWalletBalanceChanged) {
			var data map[string]any
			_ = json.Unmarshal(env.Data, &data)
			for _, field := range []string{"walletId", "transactionId", "direction", "money", "balanceBefore", "balanceAfter", "walletVersion"} {
				if _, ok := data[field]; !ok {
					t.Errorf("WalletBalanceChanged without %s: %s", field, env.Data)
				}
			}
		}
	}
}

// crashingStore simula o publisher morrendo entre publicar e confirmar: a
// publicação acontece, mas a marcação nunca chega ao banco.
type crashingStore struct {
	app.OutboxStore
}

func (crashingStore) MarkOutboxPublished(context.Context, uuid.UUID) error {
	return errors.New("process died before recording the publication")
}

// O publisher A publica e morre antes de confirmar. Quando o lease vence, o
// publisher B republica os MESMOS eventos, com os mesmos eventIds.
func TestIntegration_Outbox_RecoveryAfterPublishingButBeforeConfirming(t *testing.T) {
	e := newEnv(t)
	e.clearOutbox()
	total := e.makeEvents(3)
	queueName, queueURL := e.eventsQueue()
	db := postgres.New(e.pool)

	// Publisher A: publica tudo, mas não consegue confirmar nada.
	pubA := &countingPublisher{inner: consumer.NewPublisher(e.client, queueName)}
	relayA := app.NewOutboxRelay(crashingStore{db}, pubA, "publisher-a", relayConfig(), nil)
	for len(pubA.published()) < total {
		if relayA.RunOnce(context.Background()) == 0 {
			break
		}
	}
	if len(pubA.published()) != total {
		t.Fatalf("publisher-a sent %d, want %d", len(pubA.published()), total)
	}
	if e.pendingOutbox() != total {
		t.Fatalf("the outbox should still have %d pending events", total)
	}

	// Publisher B assume depois que o lease (1s) de A vence.
	pubB := &countingPublisher{inner: consumer.NewPublisher(e.client, queueName)}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	runRelay(ctx, &wg, app.NewOutboxRelay(db, pubB, "publisher-b", relayConfig(), nil))
	e.waitFor("publisher-b to recover the abandoned events", func() bool { return e.pendingOutbox() == 0 })
	cancel()
	wg.Wait()

	sentByA := map[uuid.UUID]bool{}
	for _, id := range pubA.published() {
		sentByA[id] = true
	}
	republished := pubB.published()
	if len(republished) != total {
		t.Fatalf("publisher-b republished %d events, want %d", len(republished), total)
	}
	for _, id := range republished {
		if !sentByA[id] {
			t.Errorf("republication changed the eventId: %s", id)
		}
	}

	// O SQS FIFO descarta a cópia (MessageDeduplicationId = eventId): o
	// consumidor recebe cada evento uma vez.
	received := e.receiveAll(queueURL)
	seen := map[string]int{}
	for _, env := range received {
		seen[env.EventID]++
	}
	if len(seen) != total {
		t.Errorf("distinct events received = %d, want %d", len(seen), total)
	}
	for id, n := range seen {
		if n > 1 {
			t.Logf("event %s delivered %d times (the broker did not deduplicate; consumers deduplicate by eventId)", id, n)
		}
	}
}

// Broker fora do ar: nada é perdido; quando volta, tudo é publicado.
func TestIntegration_Outbox_BrokerOutageLosesNothing(t *testing.T) {
	e := newEnv(t)
	e.clearOutbox()
	total := e.makeEvents(2)
	db := postgres.New(e.pool)

	broken := consumer.NewPublisher(e.client, "queue-that-does-not-exist.fifo")
	cfg := relayConfig()
	cfg.BackoffBase = 100 * time.Millisecond
	cfg.BackoffMax = 200 * time.Millisecond
	relay := app.NewOutboxRelay(db, broken, "publisher-a", cfg, nil)
	relay.RunOnce(context.Background())

	if e.pendingOutbox() != total {
		t.Fatalf("pending = %d, want %d (nothing may be lost)", e.pendingOutbox(), total)
	}
	if n := e.count(`SELECT count(*) FROM outbox_events WHERE published_at IS NULL AND attempts >= 1 AND last_error IS NOT NULL`); n == 0 {
		t.Errorf("the failed attempts were not recorded")
	}

	// O destino "volta".
	queueName, queueURL := e.eventsQueue()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	runRelay(ctx, &wg, app.NewOutboxRelay(db, consumer.NewPublisher(e.client, queueName), "publisher-b", cfg, nil))
	e.waitFor("the outbox to drain after the outage", func() bool { return e.pendingOutbox() == 0 })
	cancel()
	wg.Wait()

	if got := len(e.receiveAll(queueURL)); got != total {
		t.Errorf("received %d events, want %d", got, total)
	}
}
