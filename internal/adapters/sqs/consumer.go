package sqs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/leal-k/testeTecnico/internal/app"
	"github.com/leal-k/testeTecnico/internal/domain/wager"
)

// Client é o subconjunto da API do SQS usado aqui (o *sqs.Client da AWS o
// implementa; os testes usam dublês).
type Client interface {
	GetQueueUrl(ctx context.Context, in *awssqs.GetQueueUrlInput, opts ...func(*awssqs.Options)) (*awssqs.GetQueueUrlOutput, error)
	ReceiveMessage(ctx context.Context, in *awssqs.ReceiveMessageInput, opts ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, in *awssqs.DeleteMessageInput, opts ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(ctx context.Context, in *awssqs.ChangeMessageVisibilityInput, opts ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error)
	SendMessage(ctx context.Context, in *awssqs.SendMessageInput, opts ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error)
}

type Processor interface {
	Process(ctx context.Context, cmd app.ProcessCommand) (app.Result, error)
}

// Config controla o consumo. Os padrões estão em DefaultConfig.
type Config struct {
	// ConsumerName identifica este consumidor na inbox.
	ConsumerName string
	QueueName    string
	DLQName      string
	// WaitTime é o long polling (máximo do SQS: 20s).
	WaitTime    time.Duration
	MaxMessages int32
	// VisibilityTimeout é o tempo que a mensagem fica invisível para os outros
	// consumidores enquanto esta instância a trata. Deve ser maior que HandleTimeout.
	VisibilityTimeout time.Duration
	// HandleTimeout limita o tratamento de UMA mensagem.
	HandleTimeout time.Duration
	// MaxReceives: depois de tantas falhas transitórias, a mensagem vai para a
	// DLQ (o redrive da fila faz o mesmo, como rede de segurança).
	MaxReceives int
	// RetryBase e RetryMax: backoff exponencial entre tentativas transitórias.
	RetryBase time.Duration
	RetryMax  time.Duration
}

func DefaultConfig() Config {
	return Config{
		ConsumerName:      "wager-transactions-consumer",
		QueueName:         "wager-transactions.fifo",
		DLQName:           "wager-transactions-dlq.fifo",
		WaitTime:          10 * time.Second,
		MaxMessages:       10,
		VisibilityTimeout: 60 * time.Second,
		HandleTimeout:     15 * time.Second,
		MaxReceives:       5,
		RetryBase:         2 * time.Second,
		RetryMax:          time.Minute,
	}
}

// Outcome é o desfecho do tratamento de uma mensagem (para logs e métricas).
type Outcome string

const (
	OutcomeProcessed Outcome = "processed" // operação concluída (inclui rejeição de negócio e pendência)
	OutcomeDuplicate Outcome = "duplicate" // mesma mensagem já tratada antes
	OutcomeRetry     Outcome = "retry"     // falha transitória: volta para a fila com backoff
	OutcomeDLQ       Outcome = "dlq"       // erro permanente ou tentativas esgotadas
)

// Consumer lê a fila e trata as mensagens uma a uma, na ordem de entrega
// (respeita a ordem FIFO dentro de cada MessageGroupId).
type Consumer struct {
	client    Client
	processor Processor
	cfg       Config
	log       *slog.Logger
	onOutcome func(Outcome)

	mu       sync.Mutex
	queueURL string
	dlqURL   string
}

func NewConsumer(client Client, processor Processor, cfg Config, log *slog.Logger) *Consumer {
	if log == nil {
		log = slog.Default()
	}
	return &Consumer{
		client:    client,
		processor: processor,
		cfg:       cfg,
		log:       log.With("worker", "sqs-consumer", "queue", cfg.QueueName),
		onOutcome: func(Outcome) {},
	}
}

// OnOutcome registra uma função chamada a cada mensagem tratada.
func (c *Consumer) OnOutcome(fn func(Outcome)) {
	c.onOutcome = fn
}

// Ready consulta o SQS de verdade a cada chamada (sem cache), para o
// readiness refletir a disponibilidade atual da fila e da DLQ.
func (c *Consumer) Ready(ctx context.Context) error {
	for _, name := range []string{c.cfg.QueueName, c.cfg.DLQName} {
		if _, err := c.client.GetQueueUrl(ctx, &awssqs.GetQueueUrlInput{QueueName: aws.String(name)}); err != nil {
			return fmt.Errorf("queue %s: %w", name, err)
		}
	}
	return nil
}

// Run consome até ctx ser cancelado. Ao parar, termina a mensagem em
// andamento e devolve imediatamente à fila (visibilidade zero) as que já
// tinham sido recebidas e ainda não foram tratadas.
func (c *Consumer) Run(ctx context.Context) {
	c.log.Info("worker started")
	defer c.log.Info("worker stopped")

	failures := 0
	for ctx.Err() == nil {
		queueURL, _, err := c.urls(ctx)
		if err == nil {
			var out *awssqs.ReceiveMessageOutput
			out, err = c.client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
				QueueUrl:            aws.String(queueURL),
				MaxNumberOfMessages: c.cfg.MaxMessages,
				WaitTimeSeconds:     int32(c.cfg.WaitTime / time.Second),
				VisibilityTimeout:   int32(c.cfg.VisibilityTimeout / time.Second),
				MessageSystemAttributeNames: []types.MessageSystemAttributeName{
					types.MessageSystemAttributeNameApproximateReceiveCount,
					types.MessageSystemAttributeNameMessageGroupId,
				},
			})
			if err == nil {
				failures = 0
				for i, m := range out.Messages {
					if ctx.Err() != nil {
						c.release(queueURL, out.Messages[i:])
						return
					}
					c.handle(m)
				}
				continue
			}
		}
		if ctx.Err() != nil {
			return
		}
		// SQS indisponível: espera com backoff e tenta de novo, sem derrubar o processo.
		failures++
		wait := backoff(c.cfg.RetryBase, c.cfg.RetryMax, failures)
		c.log.Warn("receiving from SQS failed", "error", err.Error(), "retryIn", wait.String())
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

// handle trata uma mensagem. Usa um contexto próprio (não o de parada), para
// que a mensagem em andamento termine mesmo durante o encerramento.
func (c *Consumer) handle(m types.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.HandleTimeout)
	defer cancel()

	queueURL, _, err := c.urls(ctx)
	if err != nil {
		return // sem a URL não há o que fazer; a visibilidade expira e a mensagem volta
	}
	receives := receiveCount(m)
	log := c.log.With("sqsMessageId", aws.ToString(m.MessageId), "receiveCount", receives)

	msg, err := parseMessage(aws.ToString(m.Body))
	if err != nil {
		c.toDLQ(ctx, log, queueURL, m, "invalid_message", err)
		return
	}
	log = log.With(
		"messageId", msg.MessageID,
		"correlationId", msg.Command.CorrelationID,
		"providerId", msg.Command.ProviderID,
		"walletId", msg.Command.WalletID.String(),
	)

	cmd := msg.Command
	cmd.Inbox = &app.InboxRecord{Consumer: c.cfg.ConsumerName, MessageID: msg.MessageID, PayloadHash: msg.Hash}

	res, err := c.processor.Process(ctx, cmd)
	switch {
	case err == nil:
		// Tudo confirmado no banco (operação + inbox): só agora a mensagem sai da fila.
		c.delete(ctx, log, queueURL, m)
		outcome := OutcomeProcessed
		if res.DuplicateMessage {
			outcome = OutcomeDuplicate
		}
		c.onOutcome(outcome)
		log.Info("message handled",
			"outcome", string(outcome),
			"transactionId", res.TransactionID.String(),
			"status", string(res.Status),
			"failureCode", string(res.FailureCode),
			"idempotentReplay", res.Replay)

	case isPermanent(err):
		c.toDLQ(ctx, log, queueURL, m, permanentReason(err), err)

	case receives >= c.cfg.MaxReceives:
		c.toDLQ(ctx, log, queueURL, m, "retries_exhausted", err)

	default:
		// Transitória: não apaga. Ajusta a visibilidade para a próxima tentativa
		// acontecer com backoff exponencial.
		wait := backoff(c.cfg.RetryBase, c.cfg.RetryMax, receives)
		c.setVisibility(ctx, log, queueURL, m, wait)
		c.onOutcome(OutcomeRetry)
		log.Warn("transient failure, message will be retried", "error", err.Error(), "retryIn", wait.String())
	}
}

// isPermanent reconhece erros que se repetiriam em toda reentrega.
func isPermanent(err error) bool {
	for _, target := range []error{
		errInvalidMessage,
		app.ErrInvalidCommand,
		app.ErrWalletNotFound,
		app.ErrWalletPlayerMismatch,
		app.ErrCurrencyMismatch,
		app.ErrIdempotencyConflict,
		app.ErrExternalIDConflict,
		app.ErrInboxConflict,
		wager.ErrInvalidKind,
	} {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

func permanentReason(err error) string {
	switch {
	case errors.Is(err, app.ErrIdempotencyConflict):
		return "idempotency_key_conflict"
	case errors.Is(err, app.ErrExternalIDConflict):
		return "external_transaction_id_conflict"
	case errors.Is(err, app.ErrInboxConflict):
		return "message_id_conflict"
	case errors.Is(err, app.ErrWalletNotFound):
		return "wallet_not_found"
	case errors.Is(err, app.ErrWalletPlayerMismatch):
		return "wallet_player_mismatch"
	case errors.Is(err, app.ErrCurrencyMismatch):
		return "currency_mismatch"
	default:
		return "invalid_message"
	}
}

// toDLQ copia a mensagem para a DLQ (com o motivo) e só então a remove da fila
// principal. Se a cópia falhar, a mensagem fica na fila e tenta de novo.
func (c *Consumer) toDLQ(ctx context.Context, log *slog.Logger, queueURL string, m types.Message, reason string, cause error) {
	_, dlqURL, err := c.urls(ctx)
	if err == nil {
		group := m.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]
		if group == "" {
			group = "dead-letter"
		}
		_, err = c.client.SendMessage(ctx, &awssqs.SendMessageInput{
			QueueUrl:               aws.String(dlqURL),
			MessageBody:            m.Body,
			MessageGroupId:         aws.String(group),
			MessageDeduplicationId: m.MessageId,
			MessageAttributes: map[string]types.MessageAttributeValue{
				"failureReason": {DataType: aws.String("String"), StringValue: aws.String(reason)},
			},
		})
	}
	if err != nil {
		wait := backoff(c.cfg.RetryBase, c.cfg.RetryMax, receiveCount(m))
		c.setVisibility(ctx, log, queueURL, m, wait)
		log.Error("could not move message to the DLQ, it will be retried", "reason", reason, "error", err.Error())
		return
	}
	c.delete(ctx, log, queueURL, m)
	c.onOutcome(OutcomeDLQ)
	log.Error("message moved to the DLQ", "reason", reason, "error", cause.Error())
}

func (c *Consumer) delete(ctx context.Context, log *slog.Logger, queueURL string, m types.Message) {
	_, err := c.client.DeleteMessage(ctx, &awssqs.DeleteMessageInput{
		QueueUrl:      aws.String(queueURL),
		ReceiptHandle: m.ReceiptHandle,
	})
	if err != nil {
		// O tratamento já foi confirmado: na reentrega, a inbox reconhece a
		// mensagem como duplicada e nada é reaplicado.
		log.Warn("deleting the message failed, it will be redelivered and deduplicated", "error", err.Error())
	}
}

func (c *Consumer) setVisibility(ctx context.Context, log *slog.Logger, queueURL string, m types.Message, d time.Duration) {
	seconds := int32(d / time.Second)
	if seconds > 43200 { // limite do SQS: 12 horas
		seconds = 43200
	}
	_, err := c.client.ChangeMessageVisibility(ctx, &awssqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(queueURL),
		ReceiptHandle:     m.ReceiptHandle,
		VisibilityTimeout: seconds,
	})
	if err != nil {
		log.Warn("changing message visibility failed", "error", err.Error())
	}
}

// release devolve mensagens recebidas e não tratadas (encerramento).
func (c *Consumer) release(queueURL string, msgs []types.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, m := range msgs {
		c.setVisibility(ctx, c.log, queueURL, m, 0)
	}
	c.log.Info("released unprocessed messages back to the queue", "count", len(msgs))
}

// urls resolve (e guarda) as URLs da fila principal e da DLQ.
func (c *Consumer) urls(ctx context.Context) (string, string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.queueURL != "" && c.dlqURL != "" {
		return c.queueURL, c.dlqURL, nil
	}
	q, err := c.client.GetQueueUrl(ctx, &awssqs.GetQueueUrlInput{QueueName: aws.String(c.cfg.QueueName)})
	if err != nil {
		return "", "", fmt.Errorf("resolving queue %s: %w", c.cfg.QueueName, err)
	}
	d, err := c.client.GetQueueUrl(ctx, &awssqs.GetQueueUrlInput{QueueName: aws.String(c.cfg.DLQName)})
	if err != nil {
		return "", "", fmt.Errorf("resolving queue %s: %w", c.cfg.DLQName, err)
	}
	c.queueURL, c.dlqURL = aws.ToString(q.QueueUrl), aws.ToString(d.QueueUrl)
	return c.queueURL, c.dlqURL, nil
}

func receiveCount(m types.Message) int {
	n, err := strconv.Atoi(m.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
	if err != nil || n < 1 {
		return 1
	}
	return n
}

// backoff: base, 2×base, 4×base... limitado a max.
func backoff(base, max time.Duration, attempt int) time.Duration {
	d := base
	for i := 1; i < attempt && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	return d
}
