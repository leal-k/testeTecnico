// Package httpapi expõe os casos de uso pela API HTTP do desafio.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/google/uuid"

	"github.com/leal-k/testeTecnico/internal/app"
	"github.com/leal-k/testeTecnico/internal/domain/money"
	"github.com/leal-k/testeTecnico/internal/domain/wager"
	"github.com/leal-k/testeTecnico/internal/domain/wallet"
)

const (
	headerIdempotencyKey = "Idempotency-Key"
	headerCorrelationID  = "X-Correlation-Id"
	maxBodyBytes         = 1 << 20 // 1 MiB
	maxCorrelationIDLen  = 128
)

// ---- dependências (interfaces pequenas: facilitam testar só o HTTP) ---------

type Authenticator interface {
	Authenticate(ctx context.Context, rawToken string) (app.Caller, error)
}

type TransactionProcessor interface {
	Process(ctx context.Context, cmd app.ProcessCommand) (app.Result, error)
}

type Wallets interface {
	Open(ctx context.Context, caller app.Caller, cmd app.OpenWalletCommand) (*wallet.Wallet, error)
	Get(ctx context.Context, caller app.Caller, id uuid.UUID) (*wallet.Wallet, error)
	Ledger(ctx context.Context, caller app.Caller, walletID uuid.UUID, cursor string, limit int) (app.LedgerPage, error)
	Reconcile(ctx context.Context, caller app.Caller, walletID uuid.UUID) (app.Reconciliation, error)
}

type Transactions interface {
	ByID(ctx context.Context, caller app.Caller, id uuid.UUID) (*wager.Transaction, error)
	ByExternalID(ctx context.Context, caller app.Caller, providerID, externalID string) (*wager.Transaction, error)
}

// HealthCheck verifica uma dependência para o readiness (ex.: PostgreSQL, SQS).
type HealthCheck struct {
	Name  string
	Check func(ctx context.Context) error
}

type Deps struct {
	Auth           Authenticator
	Processor      TransactionProcessor
	Wallets        Wallets
	Transactions   Transactions
	Checks         []HealthCheck
	Logger         *slog.Logger
	RequestTimeout time.Duration // padrão: 15s
}

// NewHandler monta o roteador com todas as rotas do contrato.
func NewHandler(d Deps) http.Handler {
	if d.Logger == nil {
		d.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	if d.RequestTimeout <= 0 {
		d.RequestTimeout = 15 * time.Second
	}
	s := &server{d: d}

	mux := http.NewServeMux()
	// públicas
	mux.HandleFunc("GET /health/live", s.live)
	mux.HandleFunc("GET /health/ready", s.ready)
	// autenticadas
	mux.Handle("POST /wallets", s.authed(s.openWallet))
	mux.Handle("GET /wallets/{walletId}", s.authed(s.getWallet))
	mux.Handle("GET /wallets/{walletId}/ledger", s.authed(s.ledger))
	mux.Handle("POST /wallets/{walletId}/reconciliation", s.authed(s.reconcile))
	mux.Handle("POST /wagering/transactions", s.authed(s.process))
	mux.Handle("GET /wagering/transactions/{transactionId}", s.authed(s.transactionByID))
	mux.Handle("GET /providers/{providerId}/wagering/transactions/{externalTransactionId}", s.authed(s.transactionByExternalID))

	return s.observe(mux)
}

type server struct {
	d Deps
}

// ---- contexto da requisição (para logs) -------------------------------------

type ctxKey int

const requestInfoKey ctxKey = iota

// requestInfo acumula identificadores para o log final da requisição.
type requestInfo struct {
	mu            sync.Mutex
	correlationID string
	providerID    string
	internal      bool
	transactionID string
	walletID      string
}

func info(ctx context.Context) *requestInfo {
	if ri, ok := ctx.Value(requestInfoKey).(*requestInfo); ok {
		return ri
	}
	return &requestInfo{}
}

func (ri *requestInfo) set(fn func(ri *requestInfo)) {
	ri.mu.Lock()
	defer ri.mu.Unlock()
	fn(ri)
}

// ---- middlewares ------------------------------------------------------------

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// observe aplica correlação, prazo, recuperação de pânico e log de acesso.
func (s *server) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		ri := &requestInfo{correlationID: correlationID(r.Header.Get(headerCorrelationID))}
		w.Header().Set(headerCorrelationID, ri.correlationID)

		ctx, cancel := context.WithTimeout(r.Context(), s.d.RequestTimeout)
		defer cancel()
		ctx = context.WithValue(ctx, requestInfoKey, ri)
		r = r.WithContext(ctx)

		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		defer func() {
			if p := recover(); p != nil {
				s.d.Logger.Error("panic while handling request",
					"panic", fmt.Sprint(p), "stack", string(debug.Stack()),
					"correlationId", ri.correlationID)
				writeError(rec, &apiError{http.StatusInternalServerError, CodeInternal, "unexpected error"})
			}
			s.logAccess(r, rec.status, time.Since(start), ri)
		}()
		next.ServeHTTP(rec, r)
	})
}

func (s *server) logAccess(r *http.Request, status int, elapsed time.Duration, ri *requestInfo) {
	ri.mu.Lock()
	attrs := []any{
		"method", r.Method,
		"route", r.Pattern,
		"status", status,
		"durationMs", elapsed.Milliseconds(),
		"correlationId", ri.correlationID,
	}
	if ri.providerID != "" {
		attrs = append(attrs, "providerId", ri.providerID)
	}
	if ri.internal {
		attrs = append(attrs, "caller", "internal")
	}
	if ri.transactionID != "" {
		attrs = append(attrs, "transactionId", ri.transactionID)
	}
	if ri.walletID != "" {
		attrs = append(attrs, "walletId", ri.walletID)
	}
	ri.mu.Unlock()

	level := slog.LevelInfo
	if status >= 500 {
		level = slog.LevelError
	}
	s.d.Logger.Log(r.Context(), level, "http request", attrs...)
}

// correlationID aceita o identificador do cliente se for curto e imprimível;
// senão gera um novo.
func correlationID(v string) string {
	if v != "" && len(v) <= maxCorrelationIDLen && strings.IndexFunc(v, func(r rune) bool {
		return !unicode.IsPrint(r) || unicode.IsSpace(r)
	}) < 0 {
		return v
	}
	return uuid.NewString()
}

type authedHandler func(w http.ResponseWriter, r *http.Request, caller app.Caller)

// authed exige "Authorization: Bearer <token>" válido e repassa a identidade.
func (s *server) authed(h authedHandler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")
		token = strings.TrimSpace(token)
		if !ok || !strings.EqualFold(scheme, "Bearer") || token == "" {
			w.Header().Set("WWW-Authenticate", `Bearer`)
			writeError(w, &apiError{http.StatusUnauthorized, CodeUnauthenticated, "missing bearer access token"})
			return
		}
		caller, err := s.d.Auth.Authenticate(r.Context(), token)
		if err != nil {
			ae := classify(err)
			if ae.status == http.StatusUnauthorized {
				w.Header().Set("WWW-Authenticate", `Bearer error="invalid_token"`)
			} else if ae.status >= 500 {
				s.d.Logger.Error("authentication failed", "error", err.Error())
			}
			writeError(w, ae)
			return
		}
		info(r.Context()).set(func(ri *requestInfo) {
			ri.providerID = caller.ProviderID
			ri.internal = caller.Internal
		})
		h(w, r, caller)
	})
}

// ---- saúde ------------------------------------------------------------------

func (s *server) live(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *server) ready(w http.ResponseWriter, r *http.Request) {
	results := make(map[string]string, len(s.d.Checks))
	var mu sync.Mutex
	var wg sync.WaitGroup
	healthy := true

	for _, c := range s.d.Checks {
		wg.Add(1)
		go func(c HealthCheck) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			defer cancel()
			err := c.Check(ctx)

			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				healthy = false
				results[c.Name] = "unavailable"
				s.d.Logger.Warn("readiness check failed", "dependency", c.Name, "error", err.Error())
				return
			}
			results[c.Name] = "ok"
		}(c)
	}
	wg.Wait()

	status, code := "ready", http.StatusOK
	if !healthy {
		status, code = "not_ready", http.StatusServiceUnavailable
	}
	writeJSON(w, code, map[string]any{"status": status, "checks": results})
}

// ---- carteiras (serviço interno) ---------------------------------------------

func (s *server) openWallet(w http.ResponseWriter, r *http.Request, caller app.Caller) {
	var req openWalletRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, err)
		return
	}
	playerID, err := parseUUID("playerId", req.PlayerID)
	if err != nil {
		writeError(w, err)
		return
	}
	if req.InitialBalance == nil {
		writeError(w, badRequest(CodeInvalidRequest, "initialBalance is required"))
		return
	}
	initial, err := parseMoney("initialBalance", *req.InitialBalance)
	if err != nil {
		writeError(w, err)
		return
	}

	wlt, err := s.d.Wallets.Open(r.Context(), caller, app.OpenWalletCommand{
		PlayerID:       playerID,
		InitialBalance: initial,
		CorrelationID:  info(r.Context()).correlationID,
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	info(r.Context()).set(func(ri *requestInfo) { ri.walletID = wlt.ID().String() })
	w.Header().Set("Location", "/wallets/"+wlt.ID().String())
	writeJSON(w, http.StatusCreated, toWalletResponse(wlt))
}

func (s *server) getWallet(w http.ResponseWriter, r *http.Request, caller app.Caller) {
	id, ok := s.walletIDFromPath(w, r)
	if !ok {
		return
	}
	wlt, err := s.d.Wallets.Get(r.Context(), caller, id)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toWalletResponse(wlt))
}

func (s *server) ledger(w http.ResponseWriter, r *http.Request, caller app.Caller) {
	id, ok := s.walletIDFromPath(w, r)
	if !ok {
		return
	}
	limit := 0
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > app.MaxLedgerLimit {
			writeError(w, badRequest(CodeInvalidRequest, fmt.Sprintf("limit must be an integer between 1 and %d", app.MaxLedgerLimit)))
			return
		}
		limit = n
	}
	page, err := s.d.Wallets.Ledger(r.Context(), caller, id, r.URL.Query().Get("cursor"), limit)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toLedgerResponse(id.String(), page))
}

func (s *server) reconcile(w http.ResponseWriter, r *http.Request, caller app.Caller) {
	id, ok := s.walletIDFromPath(w, r)
	if !ok {
		return
	}
	rec, err := s.d.Wallets.Reconcile(r.Context(), caller, id)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toReconciliationResponse(rec))
}

func (s *server) walletIDFromPath(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := parseUUID("walletId", r.PathValue("walletId"))
	if err != nil {
		writeError(w, err)
		return uuid.Nil, false
	}
	info(r.Context()).set(func(ri *requestInfo) { ri.walletID = id.String() })
	return id, true
}

// ---- operações (provedores) -------------------------------------------------

// process trata POST /wagering/transactions.
//
//	200 PROCESSED (nova ou replay)
//	202 PENDING_REFERENCE (aguardando a referência; consulte depois)
//	422 REJECTED/FAILED (resultado definitivo, com failureCode)
func (s *server) process(w http.ResponseWriter, r *http.Request, caller app.Caller) {
	// Só provedores enviam operações; o serviço interno não age em nome deles.
	if caller.Internal || caller.ProviderID == "" {
		writeError(w, classify(app.ErrForbidden))
		return
	}
	// A chave é usada exatamente como recebida (nunca recalculada).
	key := r.Header.Get(headerIdempotencyKey)
	if key == "" {
		writeError(w, badRequest(CodeMissingIdempotencyKey, "the Idempotency-Key header is required"))
		return
	}

	var req processRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, err)
		return
	}
	cmd, err := toProcessCommand(req, key)
	if err != nil {
		writeError(w, err)
		return
	}
	// O provedor vem do token: o corpo só pode repetir o mesmo valor.
	if cmd.ProviderID != caller.ProviderID {
		writeError(w, &apiError{http.StatusForbidden, CodeForbidden, "providerId does not match the authenticated provider"})
		return
	}
	cmd.CorrelationID = info(r.Context()).correlationID
	info(r.Context()).set(func(ri *requestInfo) { ri.walletID = cmd.WalletID.String() })

	res, err := s.d.Processor.Process(r.Context(), cmd)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	info(r.Context()).set(func(ri *requestInfo) { ri.transactionID = res.TransactionID.String() })

	status := http.StatusOK
	switch res.Status {
	case wager.StatusPendingReference, wager.StatusPending:
		status = http.StatusAccepted
		w.Header().Set("Location", "/wagering/transactions/"+res.TransactionID.String())
	case wager.StatusRejected, wager.StatusFailed:
		status = http.StatusUnprocessableEntity
	}
	writeJSON(w, status, toProcessResponse(res))
}

func toProcessCommand(req processRequest, key string) (app.ProcessCommand, error) {
	required := []struct{ name, value string }{
		{"providerId", req.ProviderID},
		{"externalTransactionId", req.ExternalTransactionID},
		{"playerId", req.PlayerID},
		{"walletId", req.WalletID},
		{"roundId", req.RoundID},
		{"gameId", req.GameID},
		{"kind", req.Kind},
	}
	for _, f := range required {
		if f.value == "" {
			return app.ProcessCommand{}, badRequest(CodeInvalidRequest, f.name+" is required")
		}
	}
	kind := wager.Kind(req.Kind)
	if !kind.IsExternal() {
		return app.ProcessCommand{}, badRequest(CodeInvalidRequest, "kind must be one of BET, WIN, LOSS, REFUND, ROLLBACK")
	}
	playerID, err := parseUUID("playerId", req.PlayerID)
	if err != nil {
		return app.ProcessCommand{}, err
	}
	walletID, err := parseUUID("walletId", req.WalletID)
	if err != nil {
		return app.ProcessCommand{}, err
	}
	if req.Money == nil {
		return app.ProcessCommand{}, badRequest(CodeInvalidRequest, "money is required")
	}
	amount, err := parseMoney("money", *req.Money)
	if err != nil {
		return app.ProcessCommand{}, err
	}
	return app.ProcessCommand{
		ProviderID:          req.ProviderID,
		IdempotencyKey:      key,
		ExternalID:          req.ExternalTransactionID,
		Kind:                kind,
		WalletID:            walletID,
		PlayerID:            playerID,
		RoundID:             req.RoundID,
		GameID:              req.GameID,
		Amount:              amount,
		ReferenceExternalID: req.ReferenceExternalTransactionID,
	}, nil
}

func (s *server) transactionByID(w http.ResponseWriter, r *http.Request, caller app.Caller) {
	id, err := parseUUID("transactionId", r.PathValue("transactionId"))
	if err != nil {
		writeError(w, err)
		return
	}
	info(r.Context()).set(func(ri *requestInfo) { ri.transactionID = id.String() })
	tx, err := s.d.Transactions.ByID(r.Context(), caller, id)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toTransactionResponse(tx))
}

func (s *server) transactionByExternalID(w http.ResponseWriter, r *http.Request, caller app.Caller) {
	tx, err := s.d.Transactions.ByExternalID(r.Context(), caller,
		r.PathValue("providerId"), r.PathValue("externalTransactionId"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	info(r.Context()).set(func(ri *requestInfo) { ri.transactionID = tx.ID().String() })
	writeJSON(w, http.StatusOK, toTransactionResponse(tx))
}

// ---- utilitários ------------------------------------------------------------

// fail traduz o erro, registra os inesperados e responde.
func (s *server) fail(w http.ResponseWriter, r *http.Request, err error) {
	ae := classify(err)
	switch {
	case ae.status == http.StatusServiceUnavailable:
		w.Header().Set("Retry-After", "1")
		s.d.Logger.Warn("transient failure", "error", err.Error(), "correlationId", info(r.Context()).correlationID)
	case ae.status >= 500:
		s.d.Logger.Error("request failed", "error", err.Error(), "correlationId", info(r.Context()).correlationID)
	}
	writeError(w, ae)
}

// decodeJSON lê exatamente um objeto JSON, recusando campos desconhecidos,
// corpo grande demais e qualquer conteúdo depois do objeto.
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return &apiError{http.StatusRequestEntityTooLarge, CodeInvalidRequest, "request body too large"}
		}
		return badRequest(CodeInvalidRequest, "invalid JSON body: "+err.Error())
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return badRequest(CodeInvalidRequest, "the body must contain a single JSON object")
	}
	return nil
}

func parseUUID(field, raw string) (uuid.UUID, error) {
	id, err := uuid.Parse(raw)
	if err != nil || id == uuid.Nil {
		return uuid.Nil, badRequest(CodeInvalidRequest, field+" must be a valid UUID")
	}
	return id, nil
}

func parseMoney(field string, d moneyDTO) (money.Money, error) {
	m, err := money.Parse(d.Amount, d.Currency)
	if err != nil {
		return money.Money{}, badRequest(CodeInvalidRequest,
			field+` must be {"amount":"<non-negative decimal with up to 2 places>","currency":"<ISO 4217 code>"}`)
	}
	return m, nil
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, err error) {
	ae := classify(err)
	writeJSON(w, ae.status, errorResponse{Error: errorBody{Code: ae.code, Message: ae.message}})
}
