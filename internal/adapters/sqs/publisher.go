package sqs

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/leal-k/testeTecnico/internal/app"
	"github.com/leal-k/testeTecnico/internal/domain/event"
)

var _ app.EventPublisher = (*Publisher)(nil)

// EventEnvelope é o formato publicado em wager-events.fifo.
type EventEnvelope struct {
	EventID       string          `json:"eventId"`
	EventType     string          `json:"eventType"`
	AggregateType string          `json:"aggregateType"`
	AggregateID   string          `json:"aggregateId"`
	CorrelationID string          `json:"correlationId"`
	CausationID   string          `json:"causationId,omitempty"`
	OccurredAt    string          `json:"occurredAt"`
	Version       int             `json:"version"`
	Data          json.RawMessage `json:"data"`
}

// Envelope monta o envelope a partir do evento gravado na outbox. O conteúdo
// de "data" é o snapshot gravado, sem alteração.
func Envelope(e event.Event) EventEnvelope {
	return EventEnvelope{
		EventID:       e.ID.String(),
		EventType:     string(e.Type),
		AggregateType: e.AggregateType,
		AggregateID:   e.AggregateID.String(),
		CorrelationID: e.CorrelationID,
		CausationID:   e.CausationID,
		OccurredAt:    e.OccurredAt.UTC().Format(time.RFC3339Nano),
		Version:       e.Version,
		Data:          json.RawMessage(e.Payload),
	}
}

// Publisher envia eventos para a fila FIFO de eventos.
//
// Roteamento:
//   - MessageGroupId = aggregateId: eventos do mesmo agregado (carteira ou
//     transação) saem em ordem entre si; agregados diferentes em paralelo;
//   - MessageDeduplicationId = eventId: uma republicação em até 5 minutos é
//     descartada pelo próprio SQS; depois disso, o consumidor deduplica pelo
//     eventId (que nunca muda);
//   - atributos eventType e aggregateType permitem filtrar sem abrir o corpo.
type Publisher struct {
	client    Client
	queueName string

	mu       sync.Mutex
	queueURL string
}

func NewPublisher(client Client, queueName string) *Publisher {
	return &Publisher{client: client, queueName: queueName}
}

func (p *Publisher) Publish(ctx context.Context, e event.Event) error {
	url, err := p.url(ctx)
	if err != nil {
		return err
	}
	body, err := json.Marshal(Envelope(e))
	if err != nil {
		return fmt.Errorf("encoding event %s: %w", e.ID, err)
	}
	_, err = p.client.SendMessage(ctx, &awssqs.SendMessageInput{
		QueueUrl:               aws.String(url),
		MessageBody:            aws.String(string(body)),
		MessageGroupId:         aws.String(e.AggregateID.String()),
		MessageDeduplicationId: aws.String(e.ID.String()),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"eventType":     {DataType: aws.String("String"), StringValue: aws.String(string(e.Type))},
			"aggregateType": {DataType: aws.String("String"), StringValue: aws.String(e.AggregateType)},
		},
	})
	if err != nil {
		return fmt.Errorf("sending event %s: %w", e.ID, err)
	}
	return nil
}

// Ready confere que a fila de eventos existe e responde (sem cache).
func (p *Publisher) Ready(ctx context.Context) error {
	_, err := p.client.GetQueueUrl(ctx, &awssqs.GetQueueUrlInput{QueueName: aws.String(p.queueName)})
	if err != nil {
		return fmt.Errorf("queue %s: %w", p.queueName, err)
	}
	return nil
}

func (p *Publisher) url(ctx context.Context) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.queueURL != "" {
		return p.queueURL, nil
	}
	out, err := p.client.GetQueueUrl(ctx, &awssqs.GetQueueUrlInput{QueueName: aws.String(p.queueName)})
	if err != nil {
		return "", fmt.Errorf("resolving queue %s: %w", p.queueName, err)
	}
	p.queueURL = aws.ToString(out.QueueUrl)
	return p.queueURL, nil
}
