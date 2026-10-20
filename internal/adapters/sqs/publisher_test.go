package sqs

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"

	"github.com/leal-k/testeTecnico/internal/domain/event"
)

func TestPublisher_EnvelopeAndRouting(t *testing.T) {
	client := newFakeClient()
	p := NewPublisher(client, "wager-events.fifo")

	e := event.Event{
		ID:            uuid.New(),
		Type:          event.TypeWalletBalanceChanged,
		Version:       1,
		AggregateType: event.AggregateWallet,
		AggregateID:   uuid.New(),
		CorrelationID: "corr-1",
		CausationID:   "msg-1",
		OccurredAt:    time.Date(2026, 10, 19, 12, 0, 0, 123000000, time.UTC),
		Payload:       []byte(`{"walletId":"w","walletVersion":2,"money":{"amount":"30.00","currency":"BRL"}}`),
	}
	if err := p.Publish(context.Background(), e); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(client.sent) != 1 {
		t.Fatalf("sent = %d", len(client.sent))
	}
	in := client.sent[0]
	if aws.ToString(in.QueueUrl) != "http://sqs/wager-events.fifo" ||
		aws.ToString(in.MessageGroupId) != e.AggregateID.String() ||
		aws.ToString(in.MessageDeduplicationId) != e.ID.String() ||
		aws.ToString(in.MessageAttributes["eventType"].StringValue) != "WalletBalanceChanged" {
		t.Errorf("routing = %+v", in)
	}

	var env map[string]any
	if err := json.Unmarshal([]byte(aws.ToString(in.MessageBody)), &env); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"eventId":       e.ID.String(),
		"eventType":     "WalletBalanceChanged",
		"aggregateType": "wallet",
		"aggregateId":   e.AggregateID.String(),
		"correlationId": "corr-1",
		"causationId":   "msg-1",
		"occurredAt":    "2026-10-19T12:00:00.123Z",
		"version":       float64(1),
	}
	for k, v := range want {
		if env[k] != v {
			t.Errorf("%s = %v, want %v", k, env[k], v)
		}
	}
	data, _ := env["data"].(map[string]any)
	if data["walletVersion"] != float64(2) {
		t.Errorf("data must be the stored snapshot, got %v", env["data"])
	}
}

func TestPublisher_CausationIsOptional(t *testing.T) {
	env := Envelope(event.Event{ID: uuid.New(), AggregateID: uuid.New(), Payload: []byte(`{}`)})
	raw, _ := json.Marshal(env)
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	if _, present := m["causationId"]; present {
		t.Errorf("causationId must be omitted when empty")
	}
}

func batchEvents(n int) []event.Event {
	out := make([]event.Event, n)
	for i := range out {
		out[i] = event.Event{ID: uuid.New(), Type: event.TypeWagerTransactionProcessed, AggregateType: event.AggregateWagerTransaction,
			AggregateID: uuid.New(), Payload: []byte(`{}`)}
	}
	return out
}

func TestPublisher_Batch_UsesBrokerSizedCallsWithTheSameRouting(t *testing.T) {
	client := &fakeBatchClient{fakeClient: newFakeClient()}
	p := NewPublisher(client, "wager-events.fifo")
	events := batchEvents(12)

	errs := p.PublishBatch(context.Background(), events)

	for i, err := range errs {
		if err != nil {
			t.Errorf("event %d: %v", i, err)
		}
	}
	if len(client.batchCalls) != 2 || len(client.batchCalls[0]) != 10 || len(client.batchCalls[1]) != 2 {
		t.Fatalf("batch calls = %d", len(client.batchCalls))
	}
	if len(client.sent) != 0 {
		t.Errorf("single sends = %d, want 0", len(client.sent))
	}
	var all []types.SendMessageBatchRequestEntry
	for _, c := range client.batchCalls {
		all = append(all, c...)
	}
	for i, en := range all {
		e := events[i] // a ordem do lote é a ordem recebida
		if aws.ToString(en.MessageGroupId) != e.AggregateID.String() ||
			aws.ToString(en.MessageDeduplicationId) != e.ID.String() ||
			aws.ToString(en.MessageAttributes["eventType"].StringValue) != string(e.Type) {
			t.Errorf("entry %d routing = %+v", i, en)
		}
		var env EventEnvelope
		if err := json.Unmarshal([]byte(aws.ToString(en.MessageBody)), &env); err != nil || env.EventID != e.ID.String() {
			t.Errorf("entry %d body = %s", i, aws.ToString(en.MessageBody))
		}
	}
}

func TestPublisher_Batch_ReportsEachEntryOnItsOwn(t *testing.T) {
	client := &fakeBatchClient{fakeClient: newFakeClient(), failIDs: map[string]bool{"1": true, "11": true}}
	p := NewPublisher(client, "wager-events.fifo")

	errs := p.PublishBatch(context.Background(), batchEvents(12))

	for i, err := range errs {
		if failed := i == 1 || i == 11; failed != (err != nil) {
			t.Errorf("event %d: err = %v", i, err)
		}
	}
}

func TestPublisher_Batch_WholeCallFailureFailsEveryEntryOfThatCall(t *testing.T) {
	client := &fakeBatchClient{fakeClient: newFakeClient(), batchErr: errors.New("connection refused")}
	errs := NewPublisher(client, "wager-events.fifo").PublishBatch(context.Background(), batchEvents(3))
	for i, err := range errs {
		if err == nil {
			t.Errorf("event %d reported as delivered", i)
		}
	}
}

func TestPublisher_Batch_FallsBackToSingleSends(t *testing.T) {
	client := newFakeClient() // sem SendMessageBatch
	errs := NewPublisher(client, "wager-events.fifo").PublishBatch(context.Background(), batchEvents(3))
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(client.sent) != 3 {
		t.Errorf("single sends = %d, want 3", len(client.sent))
	}
}
