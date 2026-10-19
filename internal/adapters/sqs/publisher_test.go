package sqs

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
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
