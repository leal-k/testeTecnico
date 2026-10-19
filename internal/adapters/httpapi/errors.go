package httpapi

import (
	"context"
	"errors"
	"net"
	"net/http"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/leal-k/testeTecnico/internal/adapters/auth"
	"github.com/leal-k/testeTecnico/internal/app"
)

// Códigos de erro estáveis do contrato HTTP (campo error.code).
const (
	CodeInvalidRequest         = "INVALID_REQUEST"
	CodeInvalidCursor          = "INVALID_CURSOR"
	CodeMissingIdempotencyKey  = "MISSING_IDEMPOTENCY_KEY"
	CodeUnauthenticated        = "UNAUTHENTICATED"
	CodeForbidden              = "FORBIDDEN"
	CodeWalletNotFound         = "WALLET_NOT_FOUND"
	CodeTransactionNotFound    = "TRANSACTION_NOT_FOUND"
	CodeWalletAlreadyExists    = "WALLET_ALREADY_EXISTS"
	CodeIdempotencyKeyConflict = "IDEMPOTENCY_KEY_CONFLICT"
	CodeExternalIDConflict     = "EXTERNAL_TRANSACTION_ID_CONFLICT"
	CodeWalletPlayerMismatch   = "WALLET_PLAYER_MISMATCH"
	CodeCurrencyMismatch       = "CURRENCY_MISMATCH"
	CodeUnavailable            = "TEMPORARILY_UNAVAILABLE"
	CodeInternal               = "INTERNAL_ERROR"
	CodeNotFound               = "NOT_FOUND"
)

// apiError é um erro já traduzido para o contrato HTTP.
type apiError struct {
	status  int
	code    string
	message string
}

func (e *apiError) Error() string { return e.message }

func badRequest(code, msg string) *apiError {
	return &apiError{status: http.StatusBadRequest, code: code, message: msg}
}

// classify traduz um erro dos casos de uso para status e código HTTP.
//
//	400 entrada inválida (corrigível pelo cliente)
//	401 credencial ausente, inválida ou expirada
//	403 identidade sem permissão para a operação
//	404 recurso inexistente (ou não visível para o chamador)
//	409 conflito de idempotência ou de unicidade
//	422 requisição bem formada que não pode ser aplicada a esta carteira
//	503 indisponibilidade temporária: pode repetir com a mesma chave
//	500 erro inesperado
func classify(err error) *apiError {
	var ae *apiError
	switch {
	case errors.As(err, &ae):
		return ae
	case errors.Is(err, auth.ErrInvalidToken):
		return &apiError{http.StatusUnauthorized, CodeUnauthenticated, "missing, invalid or expired access token"}
	case errors.Is(err, app.ErrForbidden):
		return &apiError{http.StatusForbidden, CodeForbidden, "the authenticated identity is not allowed to perform this operation"}
	case errors.Is(err, app.ErrInvalidCursor):
		return badRequest(CodeInvalidCursor, "invalid cursor")
	case errors.Is(err, app.ErrInvalidCommand):
		return badRequest(CodeInvalidRequest, err.Error())
	case errors.Is(err, app.ErrWalletNotFound):
		return &apiError{http.StatusNotFound, CodeWalletNotFound, "wallet not found"}
	case errors.Is(err, app.ErrTransactionNotFound):
		return &apiError{http.StatusNotFound, CodeTransactionNotFound, "transaction not found"}
	case errors.Is(err, app.ErrWalletAlreadyExists):
		return &apiError{http.StatusConflict, CodeWalletAlreadyExists, "a wallet already exists for this player and currency"}
	case errors.Is(err, app.ErrIdempotencyConflict):
		return &apiError{http.StatusConflict, CodeIdempotencyKeyConflict, "the idempotency key was already used with a different payload"}
	case errors.Is(err, app.ErrExternalIDConflict):
		return &apiError{http.StatusConflict, CodeExternalIDConflict, "this externalTransactionId was already used with another idempotency key"}
	case errors.Is(err, app.ErrWalletPlayerMismatch):
		return &apiError{http.StatusUnprocessableEntity, CodeWalletPlayerMismatch, "the wallet belongs to another player"}
	case errors.Is(err, app.ErrCurrencyMismatch):
		return &apiError{http.StatusUnprocessableEntity, CodeCurrencyMismatch, "the currency differs from the wallet currency"}
	case isTransient(err):
		return &apiError{http.StatusServiceUnavailable, CodeUnavailable, "temporarily unavailable, retry with the same idempotency key"}
	default:
		return &apiError{http.StatusInternalServerError, CodeInternal, "unexpected error"}
	}
}

// isTransient reconhece falhas que costumam passar sozinhas: banco fora do ar,
// rede, tempo esgotado e as disputas que o caso de uso não resolveu nos retries.
func isTransient(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, app.ErrConcurrentInsert) ||
		errors.Is(err, app.ErrVersionConflict) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	var connErr *pgconn.ConnectError
	if errors.As(err, &connErr) {
		return true
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "40001", // serialization_failure
			"40P01",                   // deadlock_detected
			"55P03",                   // lock_not_available
			"57014",                   // query_canceled (statement_timeout)
			"57P01", "57P02", "57P03", // admin/crash shutdown, cannot connect now
			"53300": // too_many_connections
			return true
		}
		return false
	}
	return pgconn.SafeToRetry(err)
}
