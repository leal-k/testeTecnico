package sqs

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/leal-k/testeTecnico/internal/app"
	"github.com/leal-k/testeTecnico/internal/domain/event"
)

var _ app.BatchPublisher = (*Publisher)(nil)

// batchSender é implementado pelo cliente real do SQS. Fica fora da interface
// Client para não obrigar os dublês do consumidor a implementá-lo; sem ele, o
// Publisher envia um por um.
type batchSender interface {
	SendMessageBatch(ctx context.Context, in *awssqs.SendMessageBatchInput, opts ...func(*awssqs.Options)) (*awssqs.SendMessageBatchOutput, error)
}

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

// outgoingMessage monta corpo, roteamento e atributos de um evento.
type outgoingMessage struct {
	body, group, dedup string
	attrs              map[string]types.MessageAttributeValue
}

func newOutgoingMessage(e event.Event) (outgoingMessage, error) {
	body, err := json.Marshal(Envelope(e))
	if err != nil {
		return outgoingMessage{}, fmt.Errorf("encoding event %s: %w", e.ID, err)
	}
	return outgoingMessage{
		body:  string(body),
		group: e.AggregateID.String(),
		dedup: e.ID.String(),
		attrs: map[string]types.MessageAttributeValue{
			"eventType":     {DataType: aws.String("String"), StringValue: aws.String(string(e.Type))},
			"aggregateType": {DataType: aws.String("String"), StringValue: aws.String(e.AggregateType)},
		},
	}, nil
}

func (p *Publisher) Publish(ctx context.Context, e event.Event) error {
	url, err := p.url(ctx)
	if err != nil {
		return err
	}
	m, err := newOutgoingMessage(e)
	if err != nil {
		return err
	}
	_, err = p.client.SendMessage(ctx, &awssqs.SendMessageInput{
		QueueUrl:               aws.String(url),
		MessageBody:            aws.String(m.body),
		MessageGroupId:         aws.String(m.group),
		MessageDeduplicationId: aws.String(m.dedup),
		MessageAttributes:      m.attrs,
	})
	if err != nil {
		return fmt.Errorf("sending event %s: %w", e.ID, err)
	}
	return nil
}

// PublishBatch envia até app.MaxPublishBatch eventos por chamada
// (SendMessageBatch). Devolve um erro por evento, na ordem recebida: o SQS
// responde entrada por entrada, e uma entrada recusada não afeta as outras.
// Numa fila FIFO, as mensagens do mesmo grupo dentro de um lote entram na
// ordem em que aparecem no lote.
func (p *Publisher) PublishBatch(ctx context.Context, events []event.Event) []error {
	errs := make([]error, len(events))
	bs, ok := p.client.(batchSender)
	if !ok {
		for i, e := range events {
			errs[i] = p.Publish(ctx, e)
		}
		return errs
	}
	url, err := p.url(ctx)
	if err != nil {
		for i := range errs {
			errs[i] = err
		}
		return errs
	}

	for start := 0; start < len(events); start += app.MaxPublishBatch {
		end := min(start+app.MaxPublishBatch, len(events))
		var entries []types.SendMessageBatchRequestEntry
		for i := start; i < end; i++ {
			m, err := newOutgoingMessage(events[i])
			if err != nil {
				errs[i] = err
				continue
			}
			entries = append(entries, types.SendMessageBatchRequestEntry{
				Id:                     aws.String(strconv.Itoa(i)), // índice no lote: liga a resposta ao evento
				MessageBody:            aws.String(m.body),
				MessageGroupId:         aws.String(m.group),
				MessageDeduplicationId: aws.String(m.dedup),
				MessageAttributes:      m.attrs,
			})
		}
		if len(entries) == 0 {
			continue
		}
		out, err := bs.SendMessageBatch(ctx, &awssqs.SendMessageBatchInput{QueueUrl: aws.String(url), Entries: entries})
		if err != nil {
			for _, en := range entries {
				i, _ := strconv.Atoi(aws.ToString(en.Id))
				errs[i] = fmt.Errorf("sending event %s: %w", events[i].ID, err)
			}
			continue
		}
		delivered := map[string]bool{}
		for _, ok := range out.Successful {
			delivered[aws.ToString(ok.Id)] = true
		}
		reasons := map[string]string{}
		for _, f := range out.Failed {
			reasons[aws.ToString(f.Id)] = aws.ToString(f.Code) + ": " + aws.ToString(f.Message)
		}
		for _, en := range entries {
			id := aws.ToString(en.Id)
			if delivered[id] {
				continue
			}
			i, _ := strconv.Atoi(id)
			reason, found := reasons[id]
			if !found {
				reason = "not acknowledged by the broker"
			}
			errs[i] = fmt.Errorf("sending event %s: %s", events[i].ID, reason)
		}
	}
	return errs
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
