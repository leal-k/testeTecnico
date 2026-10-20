package sqs

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"

	"github.com/leal-k/testeTecnico/internal/app"
	"github.com/leal-k/testeTecnico/internal/domain/money"
	"github.com/leal-k/testeTecnico/internal/domain/wager"
)

// exampleBody é o exemplo de mensagem do README.
const exampleBody = `{
  "messageId": "msg-123",
  "type": "WagerTransactionRequested",
  "occurredAt": "2026-09-08T12:00:00.000Z",
  "data": {
    "providerId": "provider-a",
    "externalTransactionId": "transaction-123",
    "idempotencyKey": "provider-a:transaction-123",
    "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
    "walletId": "0192f291-27dd-7d3f-8071-5f8685deef37",
    "roundId": "round-987",
    "gameId": "fortune-chimp",
    "kind": "BET",
    "money": { "amount": "25.00", "currency": "BRL" }
  }
}`

// ---- mensagem ---------------------------------------------------------------

func TestParseMessage_ReadmeExample(t *testing.T) {
	msg, err := parseMessage(exampleBody)
	if err != nil {
		t.Fatalf("parseMessage: %v", err)
	}
	c := msg.Command
	want25, _ := money.Parse("25.00", "BRL")
	if msg.MessageID != "msg-123" || c.IdempotencyKey != "provider-a:transaction-123" ||
		c.ProviderID != "provider-a" || c.ExternalID != "transaction-123" || c.Kind != wager.KindBet ||
		c.Amount != want25 || c.RoundID != "round-987" || c.GameID != "fortune-chimp" ||
		c.WalletID.String() != "0192f291-27dd-7d3f-8071-5f8685deef37" {
		t.Errorf("command = %+v", c)
	}
	if c.CorrelationID != "msg-123" {
		t.Errorf("correlation id = %q, want the messageId", c.CorrelationID)
	}
	if len(msg.Hash) != 64 {
		t.Errorf("hash = %q", msg.Hash)
	}
}

func TestParseMessage_HashIgnoresFormattingButNotContent(t *testing.T) {
	a, err := parseMessage(exampleBody)
	if err != nil {
		t.Fatal(err)
	}
	compact := strings.Join(strings.Fields(exampleBody), "")
	b, err := parseMessage(compact)
	if err != nil {
		t.Fatal(err)
	}
	if a.Hash != b.Hash {
		t.Errorf("the same content with different spacing must have the same hash")
	}
	// "25" e "25.00" são o mesmo valor
	c, err := parseMessage(strings.Replace(exampleBody, `"25.00"`, `"25"`, 1))
	if err != nil {
		t.Fatal(err)
	}
	if a.Hash != c.Hash {
		t.Errorf("equivalent amounts must have the same hash")
	}
	d, err := parseMessage(strings.Replace(exampleBody, `"25.00"`, `"26.00"`, 1))
	if err != nil {
		t.Fatal(err)
	}
	if a.Hash == d.Hash {
		t.Errorf("a different amount must change the hash")
	}
}

func TestParseMessage_Invalid(t *testing.T) {
	replace := func(old, new string) string { return strings.Replace(exampleBody, old, new, 1) }
	tests := map[string]string{
		"not JSON":            "{",
		"missing messageId":   replace(`"messageId": "msg-123",`, ""),
		"unknown type":        replace(`"WagerTransactionRequested"`, `"SomethingElse"`),
		"bad occurredAt":      replace(`"2026-09-08T12:00:00.000Z"`, `"yesterday"`),
		"missing data":        `{"messageId":"m","type":"WagerTransactionRequested"}`,
		"OPENING is internal": replace(`"BET"`, `"OPENING"`),
		"missing key":         replace(`"idempotencyKey": "provider-a:transaction-123",`, ""),
		"amount as a number":  replace(`"amount": "25.00"`, `"amount": 25.00`),
		"scientific notation": replace(`"25.00"`, `"1e3"`),
		"negative amount":     replace(`"25.00"`, `"-1.00"`),
		"walletId not a UUID": replace(`"0192f291-27dd-7d3f-8071-5f8685deef37"`, `"abc"`),
		"missing money":       replace(`"money": { "amount": "25.00", "currency": "BRL" }`, `"x": 1`),
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := parseMessage(body); !errors.Is(err, errInvalidMessage) {
				t.Errorf("error = %v, want errInvalidMessage", err)
			}
		})
	}
}

// ---- dublês -----------------------------------------------------------------

type fakeClient struct {
	mu         sync.Mutex
	batches    [][]types.Message
	deleted    []string
	visibility map[string]int32
	sent       []*awssqs.SendMessageInput
	sendErr    error
	getURLErr  error
}

func newFakeClient(batches ...[]types.Message) *fakeClient {
	return &fakeClient{batches: batches, visibility: map[string]int32{}}
}

func (f *fakeClient) GetQueueUrl(_ context.Context, in *awssqs.GetQueueUrlInput, _ ...func(*awssqs.Options)) (*awssqs.GetQueueUrlOutput, error) {
	if f.getURLErr != nil {
		return nil, f.getURLErr
	}
	return &awssqs.GetQueueUrlOutput{QueueUrl: aws.String("http://sqs/" + aws.ToString(in.QueueName))}, nil
}

func (f *fakeClient) ReceiveMessage(ctx context.Context, _ *awssqs.ReceiveMessageInput, _ ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error) {
	f.mu.Lock()
	if len(f.batches) > 0 {
		b := f.batches[0]
		f.batches = f.batches[1:]
		f.mu.Unlock()
		return &awssqs.ReceiveMessageOutput{Messages: b}, nil
	}
	f.mu.Unlock()
	// long polling sem mensagens
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(20 * time.Millisecond):
		return &awssqs.ReceiveMessageOutput{}, nil
	}
}

func (f *fakeClient) DeleteMessage(_ context.Context, in *awssqs.DeleteMessageInput, _ ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, aws.ToString(in.ReceiptHandle))
	return &awssqs.DeleteMessageOutput{}, nil
}

func (f *fakeClient) ChangeMessageVisibility(_ context.Context, in *awssqs.ChangeMessageVisibilityInput, _ ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.visibility[aws.ToString(in.ReceiptHandle)] = in.VisibilityTimeout
	return &awssqs.ChangeMessageVisibilityOutput{}, nil
}

func (f *fakeClient) SendMessage(_ context.Context, in *awssqs.SendMessageInput, _ ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.sendErr != nil {
		return nil, f.sendErr
	}
	f.sent = append(f.sent, in)
	return &awssqs.SendMessageOutput{}, nil
}

type fakeProcessor struct {
	mu     sync.Mutex
	calls  []app.ProcessCommand
	result app.Result
	err    error
	hook   func()
}

func (f *fakeProcessor) Process(_ context.Context, cmd app.ProcessCommand) (app.Result, error) {
	f.mu.Lock()
	f.calls = append(f.calls, cmd)
	hook := f.hook
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
	return f.result, f.err
}

func message(handle string, receives int, body string) types.Message {
	return types.Message{
		MessageId:     aws.String("sqs-" + handle),
		ReceiptHandle: aws.String(handle),
		Body:          aws.String(body),
		Attributes: map[string]string{
			"ApproximateReceiveCount": strconv.Itoa(receives),
			"MessageGroupId":          "wallet-1",
		},
	}
}

func testConfig() Config {
	cfg := DefaultConfig()
	cfg.HandleTimeout = 2 * time.Second
	cfg.RetryBase = 2 * time.Second
	cfg.RetryMax = time.Minute
	cfg.MaxReceives = 5
	return cfg
}

func newTestConsumer(client *fakeClient, proc *fakeProcessor) (*Consumer, *[]Outcome) {
	c := NewConsumer(client, proc, testConfig(), nil)
	var mu sync.Mutex
	outcomes := &[]Outcome{}
	c.OnOutcome(func(o Outcome) {
		mu.Lock()
		defer mu.Unlock()
		*outcomes = append(*outcomes, o)
	})
	return c, outcomes
}

// ---- tratamento -------------------------------------------------------------

func TestHandle_ProcessedMessageIsDeletedAfterCommit(t *testing.T) {
	client := newFakeClient()
	proc := &fakeProcessor{result: app.Result{TransactionID: uuid.New(), Status: wager.StatusProcessed}}
	c, outcomes := newTestConsumer(client, proc)

	c.handle(message("h1", 1, exampleBody))

	if len(proc.calls) != 1 {
		t.Fatalf("processor calls = %d", len(proc.calls))
	}
	inbox := proc.calls[0].Inbox
	if inbox == nil || inbox.MessageID != "msg-123" || inbox.Consumer != "wager-transactions-consumer" || inbox.PayloadHash == "" {
		t.Errorf("inbox = %+v", inbox)
	}
	if proc.calls[0].IdempotencyKey != "provider-a:transaction-123" {
		t.Errorf("the key must be data.idempotencyKey, got %q", proc.calls[0].IdempotencyKey)
	}
	if len(client.deleted) != 1 || client.deleted[0] != "h1" || len(client.sent) != 0 {
		t.Errorf("deleted=%v sent=%d", client.deleted, len(client.sent))
	}
	if len(*outcomes) != 1 || (*outcomes)[0] != OutcomeProcessed {
		t.Errorf("outcomes = %v", *outcomes)
	}
}

func TestHandle_BusinessRejectionAndPendingAreTerminalForTheMessage(t *testing.T) {
	for _, status := range []wager.Status{wager.StatusRejected, wager.StatusPendingReference} {
		t.Run(string(status), func(t *testing.T) {
			client := newFakeClient()
			proc := &fakeProcessor{result: app.Result{TransactionID: uuid.New(), Status: status}}
			c, _ := newTestConsumer(client, proc)
			c.handle(message("h1", 1, exampleBody))
			if len(client.deleted) != 1 || len(client.sent) != 0 {
				t.Errorf("deleted=%v sent=%d, want deleted and not in the DLQ", client.deleted, len(client.sent))
			}
		})
	}
}

func TestHandle_DuplicateIsDeleted(t *testing.T) {
	client := newFakeClient()
	proc := &fakeProcessor{result: app.Result{TransactionID: uuid.New(), Status: wager.StatusProcessed, Replay: true, DuplicateMessage: true}}
	c, outcomes := newTestConsumer(client, proc)
	c.handle(message("h1", 2, exampleBody))
	if len(client.deleted) != 1 || (*outcomes)[0] != OutcomeDuplicate {
		t.Errorf("deleted=%v outcomes=%v", client.deleted, *outcomes)
	}
}

func TestHandle_PermanentErrorsGoToTheDLQ(t *testing.T) {
	tests := map[string]error{
		"idempotency_key_conflict":         app.ErrIdempotencyConflict,
		"external_transaction_id_conflict": app.ErrExternalIDConflict,
		"message_id_conflict":              app.ErrInboxConflict,
		"wallet_not_found":                 app.ErrWalletNotFound,
		"wallet_player_mismatch":           app.ErrWalletPlayerMismatch,
	}
	for reason, procErr := range tests {
		t.Run(reason, func(t *testing.T) {
			client := newFakeClient()
			c, outcomes := newTestConsumer(client, &fakeProcessor{err: procErr})
			c.handle(message("h1", 1, exampleBody))

			if len(client.sent) != 1 {
				t.Fatalf("DLQ sends = %d, want 1", len(client.sent))
			}
			sent := client.sent[0]
			if aws.ToString(sent.QueueUrl) != "http://sqs/wager-transactions-dlq.fifo" ||
				aws.ToString(sent.MessageGroupId) != "wallet-1" ||
				aws.ToString(sent.MessageAttributes["failureReason"].StringValue) != reason {
				t.Errorf("DLQ message = %+v", sent)
			}
			if len(client.deleted) != 1 || (*outcomes)[0] != OutcomeDLQ {
				t.Errorf("deleted=%v outcomes=%v", client.deleted, *outcomes)
			}
		})
	}
}

func TestHandle_InvalidMessageGoesToTheDLQWithoutCallingTheUseCase(t *testing.T) {
	client := newFakeClient()
	proc := &fakeProcessor{}
	c, _ := newTestConsumer(client, proc)
	c.handle(message("h1", 1, "not json"))

	if len(proc.calls) != 0 {
		t.Errorf("an invalid message reached the use case")
	}
	if len(client.sent) != 1 || aws.ToString(client.sent[0].MessageAttributes["failureReason"].StringValue) != "invalid_message" {
		t.Errorf("sent = %+v", client.sent)
	}
	if len(client.deleted) != 1 {
		t.Errorf("deleted = %v", client.deleted)
	}
}

func TestHandle_TransientErrorIsRetriedWithBackoff(t *testing.T) {
	for receives, wantSeconds := range map[int]int32{1: 2, 2: 4, 3: 8, 4: 16} {
		client := newFakeClient()
		c, outcomes := newTestConsumer(client, &fakeProcessor{err: errors.New("connection refused")})
		c.handle(message("h1", receives, exampleBody))

		if len(client.deleted) != 0 || len(client.sent) != 0 {
			t.Errorf("receive %d: a transient failure must not delete nor dead-letter the message", receives)
		}
		if got := client.visibility["h1"]; got != wantSeconds {
			t.Errorf("receive %d: visibility = %ds, want %ds", receives, got, wantSeconds)
		}
		if (*outcomes)[0] != OutcomeRetry {
			t.Errorf("outcome = %v", *outcomes)
		}
	}
}

func TestHandle_TransientErrorAfterMaxReceivesGoesToTheDLQ(t *testing.T) {
	client := newFakeClient()
	c, _ := newTestConsumer(client, &fakeProcessor{err: errors.New("connection refused")})
	c.handle(message("h1", 5, exampleBody))

	if len(client.sent) != 1 || aws.ToString(client.sent[0].MessageAttributes["failureReason"].StringValue) != "retries_exhausted" {
		t.Errorf("sent = %+v", client.sent)
	}
	if len(client.deleted) != 1 {
		t.Errorf("deleted = %v", client.deleted)
	}
}

func TestHandle_DLQUnavailableKeepsTheMessage(t *testing.T) {
	client := newFakeClient()
	client.sendErr = errors.New("DLQ down")
	c, _ := newTestConsumer(client, &fakeProcessor{err: app.ErrIdempotencyConflict})
	c.handle(message("h1", 1, exampleBody))

	if len(client.deleted) != 0 {
		t.Errorf("a message that could not reach the DLQ must stay in the queue")
	}
	if _, changed := client.visibility["h1"]; !changed {
		t.Errorf("the message must be scheduled for another attempt")
	}
}

// ---- execução e encerramento ------------------------------------------------

func TestRun_ShutdownFinishesTheCurrentMessageAndReleasesTheRest(t *testing.T) {
	batch := []types.Message{
		message("h1", 1, exampleBody),
		message("h2", 1, exampleBody),
		message("h3", 1, exampleBody),
	}
	client := newFakeClient(batch)
	ctx, cancel := context.WithCancel(context.Background())
	// O encerramento chega enquanto a primeira mensagem está sendo tratada.
	proc := &fakeProcessor{result: app.Result{TransactionID: uuid.New(), Status: wager.StatusProcessed}, hook: cancel}
	c, _ := newTestConsumer(client, proc)

	done := make(chan struct{})
	go func() {
		c.Run(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}

	if len(proc.calls) != 1 {
		t.Errorf("processed %d messages, want only the one in progress", len(proc.calls))
	}
	if len(client.deleted) != 1 || client.deleted[0] != "h1" {
		t.Errorf("the message in progress must complete and be deleted: %v", client.deleted)
	}
	for _, h := range []string{"h2", "h3"} {
		if v, ok := client.visibility[h]; !ok || v != 0 {
			t.Errorf("%s must be released (visibility 0), got %v (set=%v)", h, v, ok)
		}
	}
}

func TestRun_SurvivesSQSUnavailability(t *testing.T) {
	client := newFakeClient()
	client.getURLErr = errors.New("connection refused")
	cfg := testConfig()
	cfg.RetryBase = 10 * time.Millisecond
	cfg.RetryMax = 20 * time.Millisecond
	c := NewConsumer(client, &fakeProcessor{}, cfg, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	c.Run(ctx) // não pode entrar em pânico nem sair antes do cancelamento
	if ctx.Err() == nil {
		t.Errorf("Run returned before cancellation")
	}
	if err := c.Ready(context.Background()); err == nil {
		t.Errorf("Ready must report the unavailable queue")
	}
}

func TestBackoff(t *testing.T) {
	tests := []struct {
		attempt int
		want    time.Duration
	}{{1, 2 * time.Second}, {2, 4 * time.Second}, {3, 8 * time.Second}, {6, time.Minute}, {50, time.Minute}}
	for _, tc := range tests {
		if got := backoff(2*time.Second, time.Minute, tc.attempt); got != tc.want {
			t.Errorf("backoff(%d) = %v, want %v", tc.attempt, got, tc.want)
		}
	}
}

// fakeBatchClient acrescenta SendMessageBatch ao fakeClient. failIDs recusa
// as entradas com esses índices; batchErr faz a chamada inteira falhar.
type fakeBatchClient struct {
	*fakeClient
	batchCalls [][]types.SendMessageBatchRequestEntry
	failIDs    map[string]bool
	batchErr   error
}

func (f *fakeBatchClient) SendMessageBatch(_ context.Context, in *awssqs.SendMessageBatchInput, _ ...func(*awssqs.Options)) (*awssqs.SendMessageBatchOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.batchErr != nil {
		return nil, f.batchErr
	}
	f.batchCalls = append(f.batchCalls, in.Entries)
	out := &awssqs.SendMessageBatchOutput{}
	for _, e := range in.Entries {
		if f.failIDs[aws.ToString(e.Id)] {
			out.Failed = append(out.Failed, types.BatchResultErrorEntry{Id: e.Id, Code: aws.String("InternalError"), Message: aws.String("boom")})
			continue
		}
		out.Successful = append(out.Successful, types.SendMessageBatchResultEntry{Id: e.Id})
	}
	return out, nil
}
