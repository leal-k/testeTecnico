package sqs_test

// Testes de integração do consumidor contra um SQS REAL (MiniStack) e um
// PostgreSQL REAL, ambos em containers. Ignorados com -short ou sem Docker.

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/leal-k/testeTecnico/internal/adapters/postgres"
	consumer "github.com/leal-k/testeTecnico/internal/adapters/sqs"
	"github.com/leal-k/testeTecnico/internal/app"
	"github.com/leal-k/testeTecnico/internal/domain/money"
	"github.com/leal-k/testeTecnico/internal/domain/wager"
	"github.com/leal-k/testeTecnico/internal/testsupport/pgtest"
)

var (
	dsn        string
	sqsURL     string
	skipReason string
)

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		skipReason = "integration tests are skipped with -short"
		os.Exit(m.Run())
	}
	ctx := context.Background()

	d, stopPG, err := pgtest.Start(ctx)
	if err != nil {
		skipReason = "PostgreSQL container unavailable: " + err.Error()
		os.Exit(m.Run())
	}
	dsn = d

	ms, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "ministackorg/ministack:latest",
			ExposedPorts: []string{"4566/tcp"},
			WaitingFor:   wait.ForListeningPort("4566/tcp").WithStartupTimeout(90 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		stopPG()
		skipReason = "MiniStack container unavailable: " + err.Error()
		os.Exit(m.Run())
	}
	endpoint, err := ms.PortEndpoint(ctx, "4566/tcp", "http")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	sqsURL = endpoint

	code := m.Run()
	_ = ms.Terminate(context.Background())
	stopPG()
	os.Exit(code)
}

// ---- ambiente ---------------------------------------------------------------

type env struct {
	t        *testing.T
	client   *awssqs.Client
	pool     *pgxpool.Pool
	proc     *app.Processor
	wallets  *app.WalletService
	queue    string
	dlq      string
	queueURL string
	dlqURL   string
}

func newEnv(t *testing.T) *env {
	t.Helper()
	if skipReason != "" {
		t.Skip(skipReason)
	}
	ctx := context.Background()

	client := awssqs.New(awssqs.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(sqsURL),
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
		}),
	})

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	db := postgres.New(pool)

	suffix := uuid.NewString()[:8]
	e := &env{
		t:       t,
		client:  client,
		pool:    pool,
		proc:    app.NewProcessor(db, app.DefaultConfig(), nil, nil),
		wallets: app.NewWalletService(db, postgres.NewQueries(pool), nil, nil),
		queue:   "it-" + suffix + ".fifo",
		dlq:     "it-" + suffix + "-dlq.fifo",
	}
	e.dlqURL = e.createQueue(e.dlq)
	e.queueURL = e.createQueue(e.queue)
	return e
}

// createQueue tenta por alguns segundos: o MiniStack pode ainda estar subindo.
func (e *env) createQueue(name string) string {
	e.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		out, err := e.client.CreateQueue(context.Background(), &awssqs.CreateQueueInput{
			QueueName:  aws.String(name),
			Attributes: map[string]string{"FifoQueue": "true"},
		})
		if err == nil {
			return aws.ToString(out.QueueUrl)
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("creating queue %s: %v", name, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (e *env) config() consumer.Config {
	cfg := consumer.DefaultConfig()
	cfg.QueueName = e.queue
	cfg.DLQName = e.dlq
	cfg.WaitTime = time.Second
	cfg.VisibilityTimeout = 3 * time.Second
	cfg.HandleTimeout = 2 * time.Second
	cfg.RetryBase = time.Second
	cfg.RetryMax = 2 * time.Second
	return cfg
}

type outcomes struct {
	mu   sync.Mutex
	list []consumer.Outcome
}

func (o *outcomes) add(x consumer.Outcome) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.list = append(o.list, x)
}

func (o *outcomes) count(x consumer.Outcome) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	n := 0
	for _, v := range o.list {
		if v == x {
			n++
		}
	}
	return n
}

// startConsumers sobe n consumidores independentes (como n instâncias).
func (e *env) startConsumers(n int, client consumer.Client) *outcomes {
	e.t.Helper()
	if client == nil {
		client = e.client
	}
	out := &outcomes{}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		c := consumer.NewConsumer(client, e.proc, e.config(), nil)
		c.OnOutcome(out.add)
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.Run(ctx)
		}()
	}
	e.t.Cleanup(func() {
		cancel()
		wg.Wait()
	})
	return out
}

func brl(t *testing.T, s string) money.Money {
	t.Helper()
	m, err := money.Parse(s, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	return m
}

type wallet struct {
	id, player uuid.UUID
	provider   string
}

func (e *env) openWallet(balance string) wallet {
	e.t.Helper()
	w, err := e.wallets.Open(context.Background(), app.Caller{Internal: true},
		app.OpenWalletCommand{PlayerID: uuid.New(), InitialBalance: brl(e.t, balance)})
	if err != nil {
		e.t.Fatal(err)
	}
	return wallet{id: w.ID(), player: w.PlayerID(), provider: "provider-" + uuid.NewString()[:8]}
}

// body monta a mensagem. O messageId recebe o prefixo do provedor do teste:
// a inbox é global e durável, então um messageId reaproveitado entre testes
// com outro conteúdo seria (corretamente) tratado como conflito.
func body(w wallet, messageID, kind, amount, ext, ref string) string {
	messageID = w.provider + ":" + messageID
	refField := ""
	if ref != "" {
		refField = fmt.Sprintf(`,"referenceExternalTransactionId":%q`, ref)
	}
	return fmt.Sprintf(`{"messageId":%q,"type":"WagerTransactionRequested","occurredAt":"2026-09-08T12:00:00.000Z",
		"data":{"providerId":%q,"externalTransactionId":%q,"idempotencyKey":%q,"playerId":%q,"walletId":%q,
		"roundId":"round-1","gameId":"game-1","kind":%q,"money":{"amount":%q,"currency":"BRL"}%s}}`,
		messageID, w.provider, ext, w.provider+":"+ext, w.player, w.id, kind, amount, refField)
}

// send publica com MessageGroupId = carteira (ordem por carteira) e um
// MessageDeduplicationId próprio, para simular reenvios do produtor que o
// SQS não deduplica (a deduplicação fica por conta da aplicação).
func (e *env) send(w wallet, msgBody string) {
	e.t.Helper()
	_, err := e.client.SendMessage(context.Background(), &awssqs.SendMessageInput{
		QueueUrl:               aws.String(e.queueURL),
		MessageBody:            aws.String(msgBody),
		MessageGroupId:         aws.String(w.id.String()),
		MessageDeduplicationId: aws.String(uuid.NewString()),
	})
	if err != nil {
		e.t.Fatalf("sending message: %v", err)
	}
}

func (e *env) waitFor(what string, cond func() bool) {
	e.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			e.t.Fatalf("timed out waiting for: %s", what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (e *env) count(query string, args ...any) int {
	e.t.Helper()
	var n int
	if err := e.pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		e.t.Fatalf("count failed: %v\n%s", err, query)
	}
	return n
}

func (e *env) balance(w wallet) int64 {
	e.t.Helper()
	var minor int64
	if err := e.pool.QueryRow(context.Background(), `SELECT balance_minor FROM wallets WHERE id = $1`, w.id).Scan(&minor); err != nil {
		e.t.Fatal(err)
	}
	return minor
}

func (e *env) txStatus(w wallet, ext string) string {
	var status string
	err := e.pool.QueryRow(context.Background(),
		`SELECT status FROM wager_transactions WHERE provider_id = $1 AND external_transaction_id = $2`, w.provider, ext).Scan(&status)
	if err != nil {
		return ""
	}
	return status
}

// drain lê o que restou numa fila (com os consumidores já parados).
func (e *env) drain(queueURL string) []types.Message {
	e.t.Helper()
	out, err := e.client.ReceiveMessage(context.Background(), &awssqs.ReceiveMessageInput{
		QueueUrl:              aws.String(queueURL),
		MaxNumberOfMessages:   10,
		WaitTimeSeconds:       1,
		MessageAttributeNames: []string{"All"},
	})
	if err != nil {
		e.t.Fatalf("receiving from %s: %v", queueURL, err)
	}
	return out.Messages
}

// assertLedgerConsistent: saldo armazenado = soma dos lançamentos.
func (e *env) assertLedgerConsistent(w wallet) {
	e.t.Helper()
	var stored, net int64
	err := e.pool.QueryRow(context.Background(), `
		SELECT w.balance_minor,
		       COALESCE(SUM(CASE l.direction WHEN 'CREDIT' THEN l.amount_minor ELSE -l.amount_minor END), 0)::bigint
		FROM wallets w LEFT JOIN wallet_ledger_entries l ON l.wallet_id = w.id
		WHERE w.id = $1 GROUP BY w.balance_minor`, w.id).Scan(&stored, &net)
	if err != nil {
		e.t.Fatal(err)
	}
	if stored != net {
		e.t.Errorf("stored balance %d differs from the ledger sum %d", stored, net)
	}
}

// ---- testes -----------------------------------------------------------------

func TestIntegration_SQS_ProcessesThenDeletes(t *testing.T) {
	e := newEnv(t)
	w := e.openWallet("100.00")
	out := e.startConsumers(1, nil)

	e.send(w, body(w, "msg-1", "BET", "30.00", "bet-1", ""))
	e.waitFor("the bet to be processed", func() bool { return e.txStatus(w, "bet-1") == "PROCESSED" })
	e.waitFor("the outcome to be reported", func() bool { return out.count(consumer.OutcomeProcessed) == 1 })

	if b := e.balance(w); b != 7000 {
		t.Errorf("balance = %d, want 7000", b)
	}
	if n := e.count(`SELECT count(*) FROM inbox_messages WHERE message_id = $1 AND completed_at IS NOT NULL`, w.provider+":msg-1"); n != 1 {
		t.Errorf("inbox rows = %d, want 1", n)
	}
	e.assertLedgerConsistent(w)
}

// Reentregas da mesma mensagem, disputadas por 3 consumidores: um único débito.
func TestIntegration_SQS_RedeliveredMessageDebitsOnce(t *testing.T) {
	e := newEnv(t)
	w := e.openWallet("100.00")
	out := e.startConsumers(3, nil)

	msg := body(w, "msg-dup", "BET", "10.00", "bet-1", "")
	for i := 0; i < 5; i++ {
		e.send(w, msg)
	}
	e.waitFor("all 5 deliveries to be handled", func() bool {
		return out.count(consumer.OutcomeProcessed)+out.count(consumer.OutcomeDuplicate) == 5
	})

	if p, d := out.count(consumer.OutcomeProcessed), out.count(consumer.OutcomeDuplicate); p != 1 || d != 4 {
		t.Errorf("processed=%d duplicates=%d, want 1 and 4", p, d)
	}
	if b := e.balance(w); b != 9000 {
		t.Errorf("balance = %d, want 9000 (a single debit)", b)
	}
	if n := e.count(`SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, w.id); n != 1 {
		t.Errorf("debit entries = %d, want 1", n)
	}
	e.assertLedgerConsistent(w)
}

func TestIntegration_SQS_InvalidMessageGoesToTheDLQ(t *testing.T) {
	e := newEnv(t)
	w := e.openWallet("100.00")
	out := e.startConsumers(1, nil)

	e.send(w, `{"messageId":"bad-1","type":"WagerTransactionRequested","data":{"kind":"OPENING"}}`)
	e.waitFor("the message to be dead-lettered", func() bool { return out.count(consumer.OutcomeDLQ) == 1 })

	dead := e.drain(e.dlqURL)
	if len(dead) != 1 {
		t.Fatalf("DLQ has %d messages, want 1", len(dead))
	}
	if attr, ok := dead[0].MessageAttributes["failureReason"]; ok && aws.ToString(attr.StringValue) != "invalid_message" {
		t.Errorf("failureReason = %q", aws.ToString(attr.StringValue))
	}
	if b := e.balance(w); b != 10000 {
		t.Errorf("an invalid message changed the balance: %d", b)
	}
}

func TestIntegration_SQS_BusinessRejectionIsTerminalNotDLQ(t *testing.T) {
	e := newEnv(t)
	w := e.openWallet("100.00")
	out := e.startConsumers(1, nil)

	e.send(w, body(w, "msg-1", "BET", "100.01", "bet-1", ""))
	e.waitFor("the rejection", func() bool { return e.txStatus(w, "bet-1") == "REJECTED" })
	e.waitFor("the outcome", func() bool { return out.count(consumer.OutcomeProcessed) == 1 })

	if out.count(consumer.OutcomeDLQ) != 0 {
		t.Errorf("a business rejection must not go to the DLQ")
	}
	if b := e.balance(w); b != 10000 {
		t.Errorf("balance = %d", b)
	}
}

// crashBeforeDelete simula o consumidor caindo DEPOIS do commit e ANTES de
// apagar a mensagem: a primeira remoção "não acontece".
type crashBeforeDelete struct {
	*awssqs.Client
	mu      sync.Mutex
	crashed bool
}

func (c *crashBeforeDelete) DeleteMessage(ctx context.Context, in *awssqs.DeleteMessageInput, opts ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error) {
	c.mu.Lock()
	first := !c.crashed
	c.crashed = true
	c.mu.Unlock()
	if first {
		return nil, errors.New("process died before deleting the message")
	}
	return c.Client.DeleteMessage(ctx, in, opts...)
}

func TestIntegration_SQS_CrashAfterCommitBeforeDelete_RedeliveryIsDeduplicated(t *testing.T) {
	e := newEnv(t)
	w := e.openWallet("100.00")
	out := e.startConsumers(1, &crashBeforeDelete{Client: e.client})

	e.send(w, body(w, "msg-crash", "BET", "25.00", "bet-1", ""))
	// A mensagem volta quando a visibilidade (3s) expira e é tratada de novo.
	e.waitFor("the redelivery to be recognised as a duplicate", func() bool {
		return out.count(consumer.OutcomeDuplicate) == 1
	})

	if b := e.balance(w); b != 7500 {
		t.Errorf("balance = %d, want 7500 (debited once)", b)
	}
	if n := e.count(`SELECT count(*) FROM wager_transactions WHERE provider_id = $1`, w.provider); n != 1 {
		t.Errorf("transactions = %d, want 1", n)
	}
	e.assertLedgerConsistent(w)
}

// A mesma operação chega primeiro por HTTP (caso de uso direto) e depois por SQS.
func TestIntegration_SQS_SameOperationFromHTTPAndSQS(t *testing.T) {
	e := newEnv(t)
	w := e.openWallet("100.00")

	viaHTTP, err := e.proc.Process(context.Background(), app.ProcessCommand{
		ProviderID: w.provider, IdempotencyKey: w.provider + ":bet-1", ExternalID: "bet-1",
		Kind: wager.KindBet, WalletID: w.id, PlayerID: w.player, RoundID: "round-1", GameID: "game-1",
		Amount: brl(t, "40.00"), CorrelationID: "http",
	})
	if err != nil || viaHTTP.Status != wager.StatusProcessed {
		t.Fatalf("HTTP path = %+v, %v", viaHTTP, err)
	}

	out := e.startConsumers(1, nil)
	e.send(w, body(w, "msg-1", "BET", "40.00", "bet-1", ""))
	e.waitFor("the SQS delivery to be handled", func() bool { return out.count(consumer.OutcomeProcessed) == 1 })

	if b := e.balance(w); b != 6000 {
		t.Errorf("balance = %d, want 6000 (one debit across HTTP and SQS)", b)
	}
	if n := e.count(`SELECT count(*) FROM wager_transactions WHERE provider_id = $1`, w.provider); n != 1 {
		t.Errorf("transactions = %d, want 1", n)
	}
}

// Um REFUND chega pela fila antes da aposta: a mensagem é concluída assim que a
// pendência fica gravada, e a pendência é resolvida quando a aposta chega.
func TestIntegration_SQS_ReversalBeforeReference(t *testing.T) {
	e := newEnv(t)
	w := e.openWallet("100.00")
	out := e.startConsumers(1, nil)

	e.send(w, body(w, "msg-refund", "REFUND", "30.00", "refund-1", "bet-1"))
	e.waitFor("the refund to wait for its reference", func() bool { return e.txStatus(w, "refund-1") == "PENDING_REFERENCE" })
	e.waitFor("the refund message to be completed", func() bool { return out.count(consumer.OutcomeProcessed) == 1 })

	e.send(w, body(w, "msg-bet", "BET", "30.00", "bet-1", ""))
	e.waitFor("the bet", func() bool { return e.txStatus(w, "bet-1") == "PROCESSED" })

	// O worker de pendências resolve o REFUND.
	worker := app.NewPendingWorker(postgres.New(e.pool), e.proc, 50*time.Millisecond, 10, nil)
	ctx, cancel := context.WithCancel(context.Background())
	go worker.Run(ctx)
	defer cancel()

	e.waitFor("the refund to be resolved", func() bool { return e.txStatus(w, "refund-1") == "PROCESSED" })
	if b := e.balance(w); b != 10000 {
		t.Errorf("balance = %d, want 10000", b)
	}
	e.assertLedgerConsistent(w)
}
