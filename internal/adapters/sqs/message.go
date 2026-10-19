// Package sqs implementa o consumidor da fila wager-transactions.fifo. Ele usa
// o MESMO caso de uso da API HTTP (app.Processor), com as mesmas garantias de
// idempotência, mais a deduplicação durável da inbox pelo messageId.
package sqs

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/leal-k/testeTecnico/internal/app"
	"github.com/leal-k/testeTecnico/internal/domain/money"
	"github.com/leal-k/testeTecnico/internal/domain/wager"
)

// MessageTypeWagerTransactionRequested é o único tipo de mensagem aceito.
const MessageTypeWagerTransactionRequested = "WagerTransactionRequested"

// errInvalidMessage marca mensagens que nunca vão funcionar, por mais que sejam
// reentregues: vão direto para a DLQ.
var errInvalidMessage = errors.New("sqs: invalid message")

// envelope é o corpo da mensagem:
//
//	{"messageId":"msg-123","type":"WagerTransactionRequested",
//	 "occurredAt":"2026-09-08T12:00:00.000Z","data":{...}}
type envelope struct {
	MessageID  string       `json:"messageId"`
	Type       string       `json:"type"`
	OccurredAt string       `json:"occurredAt"`
	Data       *requestData `json:"data"`
}

type moneyData struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

type requestData struct {
	ProviderID                     string     `json:"providerId"`
	ExternalTransactionID          string     `json:"externalTransactionId"`
	IdempotencyKey                 string     `json:"idempotencyKey"`
	PlayerID                       string     `json:"playerId"`
	WalletID                       string     `json:"walletId"`
	RoundID                        string     `json:"roundId"`
	GameID                         string     `json:"gameId"`
	Kind                           string     `json:"kind"`
	Money                          *moneyData `json:"money"`
	ReferenceExternalTransactionID string     `json:"referenceExternalTransactionId,omitempty"`
}

// parsedMessage é a mensagem já validada e convertida no comando do caso de uso.
type parsedMessage struct {
	MessageID string
	Hash      string
	Command   app.ProcessCommand
}

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errInvalidMessage, fmt.Sprintf(format, args...))
}

// parseMessage valida o envelope e o conteúdo. Campos extras são ignorados
// (compatibilidade com produtores mais novos); os obrigatórios são exigidos.
func parseMessage(body string) (parsedMessage, error) {
	var env envelope
	if err := json.Unmarshal([]byte(body), &env); err != nil {
		return parsedMessage{}, invalid("body is not valid JSON: %v", err)
	}
	if env.MessageID == "" || len(env.MessageID) > 200 {
		return parsedMessage{}, invalid("messageId is required (up to 200 characters)")
	}
	if env.Type != MessageTypeWagerTransactionRequested {
		return parsedMessage{}, invalid("unsupported message type %q", env.Type)
	}
	if env.OccurredAt != "" {
		if _, err := time.Parse(time.RFC3339Nano, env.OccurredAt); err != nil {
			return parsedMessage{}, invalid("occurredAt must be an RFC 3339 timestamp")
		}
	}
	d := env.Data
	if d == nil {
		return parsedMessage{}, invalid("data is required")
	}

	required := []struct{ name, value string }{
		{"data.providerId", d.ProviderID},
		{"data.externalTransactionId", d.ExternalTransactionID},
		{"data.idempotencyKey", d.IdempotencyKey},
		{"data.playerId", d.PlayerID},
		{"data.walletId", d.WalletID},
		{"data.roundId", d.RoundID},
		{"data.gameId", d.GameID},
		{"data.kind", d.Kind},
	}
	for _, f := range required {
		if f.value == "" {
			return parsedMessage{}, invalid("%s is required", f.name)
		}
	}
	kind := wager.Kind(d.Kind)
	if !kind.IsExternal() {
		// OPENING é interno e nunca aceito pela fila.
		return parsedMessage{}, invalid("data.kind must be one of BET, WIN, LOSS, REFUND, ROLLBACK")
	}
	playerID, err := uuid.Parse(d.PlayerID)
	if err != nil {
		return parsedMessage{}, invalid("data.playerId must be a UUID")
	}
	walletID, err := uuid.Parse(d.WalletID)
	if err != nil {
		return parsedMessage{}, invalid("data.walletId must be a UUID")
	}
	if d.Money == nil {
		return parsedMessage{}, invalid("data.money is required")
	}
	amount, err := money.Parse(d.Money.Amount, d.Money.Currency)
	if err != nil {
		return parsedMessage{}, invalid("data.money is invalid: %v", err)
	}

	// O hash de negócio é exatamente o mesmo da entrada HTTP.
	businessHash, err := wager.ComputePayloadHash(wager.ExternalParams{
		Kind:                kind,
		ProviderID:          d.ProviderID,
		ExternalID:          d.ExternalTransactionID,
		WalletID:            walletID,
		PlayerID:            playerID,
		RoundID:             d.RoundID,
		GameID:              d.GameID,
		Amount:              amount,
		ReferenceExternalID: d.ReferenceExternalTransactionID,
	})
	if err != nil {
		return parsedMessage{}, err
	}

	return parsedMessage{
		MessageID: env.MessageID,
		Hash:      messageHash(env.Type, d.IdempotencyKey, businessHash),
		Command: app.ProcessCommand{
			ProviderID:          d.ProviderID,
			IdempotencyKey:      d.IdempotencyKey,
			ExternalID:          d.ExternalTransactionID,
			Kind:                kind,
			WalletID:            walletID,
			PlayerID:            playerID,
			RoundID:             d.RoundID,
			GameID:              d.GameID,
			Amount:              amount,
			ReferenceExternalID: d.ReferenceExternalTransactionID,
			CorrelationID:       env.MessageID,
			CausationID:         env.MessageID,
		},
	}, nil
}

// messageHash identifica o conteúdo da mensagem para a inbox: o tipo, a chave
// de idempotência e o hash de negócio. Não depende de espaços nem da ordem dos
// campos do JSON, então uma reentrega do mesmo conteúdo sempre bate.
func messageHash(msgType, idempotencyKey, businessHash string) string {
	raw, _ := json.Marshal(map[string]string{
		"businessHash":   businessHash,
		"idempotencyKey": idempotencyKey,
		"type":           msgType,
	})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
