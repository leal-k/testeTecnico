package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/leal-k/testeTecnico/internal/adapters/auth"
	"github.com/leal-k/testeTecnico/internal/app"
	"github.com/leal-k/testeTecnico/internal/domain/ledger"
	"github.com/leal-k/testeTecnico/internal/domain/money"
	"github.com/leal-k/testeTecnico/internal/domain/wager"
	"github.com/leal-k/testeTecnico/internal/domain/wallet"
)

// ---- dublês -----------------------------------------------------------------

// fakeAuth aceita tokens fixos: "internal", "provider-a", "provider-b",
// "no-role" (válido, sem papel); o resto é inválido.
type fakeAuth struct{}

func (fakeAuth) Authenticate(_ context.Context, token string) (app.Caller, error) {
	switch token {
	case "internal":
		return app.Caller{Internal: true}, nil
	case "provider-a", "provider-b":
		return app.Caller{ProviderID: token}, nil
	case "no-role":
		return app.Caller{}, app.ErrForbidden
	default:
		return app.Caller{}, auth.ErrInvalidToken
	}
}

type fakeProcessor struct {
	calls  []app.ProcessCommand
	result app.Result
	err    error
}

func (f *fakeProcessor) Process(_ context.Context, cmd app.ProcessCommand) (app.Result, error) {
	f.calls = append(f.calls, cmd)
	return f.result, f.err
}

type fakeWallets struct {
	wallet    *wallet.Wallet
	page      app.LedgerPage
	rec       app.Reconciliation
	err       error
	lastLimit int
	lastOpen  app.OpenWalletCommand
}

// Os dublês aplicam a mesma regra do caso de uso real: só o serviço interno.
func (f *fakeWallets) Open(_ context.Context, c app.Caller, cmd app.OpenWalletCommand) (*wallet.Wallet, error) {
	if !c.Internal {
		return nil, app.ErrForbidden
	}
	f.lastOpen = cmd
	return f.wallet, f.err
}

func (f *fakeWallets) Get(_ context.Context, c app.Caller, _ uuid.UUID) (*wallet.Wallet, error) {
	if !c.Internal {
		return nil, app.ErrForbidden
	}
	return f.wallet, f.err
}

func (f *fakeWallets) Ledger(_ context.Context, c app.Caller, _ uuid.UUID, _ string, limit int) (app.LedgerPage, error) {
	if !c.Internal {
		return app.LedgerPage{}, app.ErrForbidden
	}
	f.lastLimit = limit
	return f.page, f.err
}

func (f *fakeWallets) Reconcile(_ context.Context, c app.Caller, _ uuid.UUID) (app.Reconciliation, error) {
	if !c.Internal {
		return app.Reconciliation{}, app.ErrForbidden
	}
	return f.rec, f.err
}

type fakeTransactions struct {
	tx  *wager.Transaction
	err error
}

func (f *fakeTransactions) ByID(context.Context, app.Caller, uuid.UUID) (*wager.Transaction, error) {
	return f.tx, f.err
}

func (f *fakeTransactions) ByExternalID(_ context.Context, c app.Caller, providerID, _ string) (*wager.Transaction, error) {
	if !c.Internal && c.ProviderID != providerID {
		return nil, app.ErrForbidden
	}
	return f.tx, f.err
}

// ---- fixture ----------------------------------------------------------------

type fixture struct {
	t       *testing.T
	proc    *fakeProcessor
	wallets *fakeWallets
	txs     *fakeTransactions
	checks  []HealthCheck
	logs    *bytes.Buffer
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	return &fixture{t: t, proc: &fakeProcessor{}, wallets: &fakeWallets{}, txs: &fakeTransactions{}, logs: &bytes.Buffer{}}
}

func (f *fixture) handler() http.Handler {
	return NewHandler(Deps{
		Auth:         fakeAuth{},
		Processor:    f.proc,
		Wallets:      f.wallets,
		Transactions: f.txs,
		Checks:       f.checks,
		Logger:       slog.New(slog.NewJSONHandler(f.logs, nil)),
	})
}

type call struct {
	method, path, token, body string
	headers                   map[string]string
}

func (f *fixture) do(c call) *httptest.ResponseRecorder {
	f.t.Helper()
	req := httptest.NewRequest(c.method, c.path, strings.NewReader(c.body))
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	for k, v := range c.headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	f.handler().ServeHTTP(rec, req)
	return rec
}

func decode(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("response is not JSON: %v\n%s", err, rec.Body.String())
	}
	return out
}

func errorCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	body := decode(t, rec)
	e, _ := body["error"].(map[string]any)
	code, _ := e["code"].(string)
	return code
}

func brl(t *testing.T, amount string) money.Money {
	t.Helper()
	m, err := money.Parse(amount, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	return m
}

var (
	walletID = uuid.MustParse("0192f291-27dd-7d3f-8071-5f8685deef37")
	playerID = uuid.MustParse("0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1")
	txID     = uuid.MustParse("0192f298-345e-7e38-af88-e43f851a819d")
)

// betBody é o exemplo do README.
func betBody(provider string) string {
	return `{
		"providerId": "` + provider + `",
		"externalTransactionId": "transaction-123",
		"playerId": "` + playerID.String() + `",
		"walletId": "` + walletID.String() + `",
		"roundId": "round-987",
		"gameId": "fortune-chimp",
		"kind": "BET",
		"money": { "amount": "25.00", "currency": "BRL" }
	}`
}

func postBet(f *fixture, token, provider, key string) *httptest.ResponseRecorder {
	headers := map[string]string{}
	if key != "" {
		headers[headerIdempotencyKey] = key
	}
	return f.do(call{method: "POST", path: "/wagering/transactions", token: token, body: betBody(provider), headers: headers})
}

// ---- autenticação -----------------------------------------------------------

func TestAuth_MissingInvalidAndForbiddenTokens(t *testing.T) {
	f := newFixture(t)

	tests := []struct {
		name       string
		authHeader string
		wantStatus int
		wantCode   string
	}{
		{"no header", "", http.StatusUnauthorized, CodeUnauthenticated},
		{"not bearer", "Basic dXNlcjpwYXNz", http.StatusUnauthorized, CodeUnauthenticated},
		{"empty bearer", "Bearer ", http.StatusUnauthorized, CodeUnauthenticated},
		{"invalid or expired token", "Bearer garbage", http.StatusUnauthorized, CodeUnauthenticated},
		{"valid token without role", "Bearer no-role", http.StatusForbidden, CodeForbidden},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/wagering/transactions", strings.NewReader(betBody("provider-a")))
			req.Header.Set(headerIdempotencyKey, "k")
			if tc.authHeader != "" {
				req.Header.Set("Authorization", tc.authHeader)
			}
			rec := httptest.NewRecorder()
			f.handler().ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus || errorCode(t, rec) != tc.wantCode {
				t.Errorf("status=%d code=%s, want %d %s", rec.Code, errorCode(t, rec), tc.wantStatus, tc.wantCode)
			}
			if tc.wantStatus == http.StatusUnauthorized && rec.Header().Get("WWW-Authenticate") == "" {
				t.Errorf("401 must carry WWW-Authenticate")
			}
		})
	}
	if len(f.proc.calls) != 0 {
		t.Errorf("unauthenticated requests reached the use case %d times", len(f.proc.calls))
	}
}

func TestAuth_EveryBusinessRouteRequiresAToken(t *testing.T) {
	f := newFixture(t)
	routes := []struct{ method, path string }{
		{"POST", "/wallets"},
		{"GET", "/wallets/" + walletID.String()},
		{"GET", "/wallets/" + walletID.String() + "/ledger"},
		{"POST", "/wallets/" + walletID.String() + "/reconciliation"},
		{"POST", "/wagering/transactions"},
		{"GET", "/wagering/transactions/" + txID.String()},
		{"GET", "/providers/provider-a/wagering/transactions/ext-1"},
	}
	for _, r := range routes {
		if rec := f.do(call{method: r.method, path: r.path, body: "{}"}); rec.Code != http.StatusUnauthorized {
			t.Errorf("%s %s without a token: status %d, want 401", r.method, r.path, rec.Code)
		}
	}
}

func TestHealth_IsPublic(t *testing.T) {
	f := newFixture(t)
	f.checks = []HealthCheck{{Name: "postgres", Check: func(context.Context) error { return nil }}}

	if rec := f.do(call{method: "GET", path: "/health/live"}); rec.Code != http.StatusOK {
		t.Errorf("live: %d", rec.Code)
	}
	rec := f.do(call{method: "GET", path: "/health/ready"})
	if rec.Code != http.StatusOK || decode(t, rec)["status"] != "ready" {
		t.Errorf("ready: %d %s", rec.Code, rec.Body.String())
	}

	f.checks = append(f.checks, HealthCheck{Name: "sqs", Check: func(context.Context) error { return errors.New("down") }})
	rec = f.do(call{method: "GET", path: "/health/ready"})
	body := decode(t, rec)
	checks, _ := body["checks"].(map[string]any)
	if rec.Code != http.StatusServiceUnavailable || checks["sqs"] != "unavailable" || checks["postgres"] != "ok" {
		t.Errorf("ready with a failing dependency: %d %s", rec.Code, rec.Body.String())
	}
}

// ---- envio de operação ------------------------------------------------------

func TestProcess_Processed(t *testing.T) {
	f := newFixture(t)
	f.proc.result = app.Result{TransactionID: txID, Status: wager.StatusProcessed, Balance: brl(t, "975.00")}

	rec := postBet(f, "provider-a", "provider-a", "provider-a:transaction-123")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	body := decode(t, rec)
	balance, _ := body["balance"].(map[string]any)
	if body["transactionId"] != txID.String() || body["status"] != "PROCESSED" ||
		body["idempotentReplay"] != false || balance["amount"] != "975.00" || balance["currency"] != "BRL" {
		t.Errorf("body = %v", body)
	}
	if _, present := body["failureCode"]; present {
		t.Errorf("failureCode must be absent on success")
	}

	if len(f.proc.calls) != 1 {
		t.Fatalf("processor calls = %d", len(f.proc.calls))
	}
	cmd := f.proc.calls[0]
	if cmd.IdempotencyKey != "provider-a:transaction-123" {
		t.Errorf("the key must be used exactly as received, got %q", cmd.IdempotencyKey)
	}
	if cmd.ProviderID != "provider-a" || cmd.Kind != wager.KindBet || cmd.Amount != brl(t, "25.00") ||
		cmd.WalletID != walletID || cmd.PlayerID != playerID || cmd.ExternalID != "transaction-123" {
		t.Errorf("command = %+v", cmd)
	}
	if cmd.CorrelationID == "" || rec.Header().Get(headerCorrelationID) != cmd.CorrelationID {
		t.Errorf("correlation id not propagated")
	}
}

func TestProcess_ResultStatuses(t *testing.T) {
	tests := []struct {
		name       string
		result     app.Result
		wantStatus int
	}{
		{"replay", app.Result{TransactionID: txID, Status: wager.StatusProcessed, Balance: brl(t, "975.00"), Replay: true}, http.StatusOK},
		{"business rejection", app.Result{TransactionID: txID, Status: wager.StatusRejected, Balance: brl(t, "20.00"), FailureCode: wager.FailureInsufficientFunds}, http.StatusUnprocessableEntity},
		{"pending reference", app.Result{TransactionID: txID, Status: wager.StatusPendingReference, Balance: brl(t, "100.00")}, http.StatusAccepted},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			f.proc.result = tc.result
			rec := postBet(f, "provider-a", "provider-a", "k")
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d: %s", rec.Code, tc.wantStatus, rec.Body.String())
			}
			body := decode(t, rec)
			if body["status"] != string(tc.result.Status) || body["idempotentReplay"] != tc.result.Replay {
				t.Errorf("body = %v", body)
			}
			if tc.result.FailureCode != "" && body["failureCode"] != string(tc.result.FailureCode) {
				t.Errorf("failureCode = %v, want %s", body["failureCode"], tc.result.FailureCode)
			}
		})
	}
}

func TestProcess_UseCaseErrors(t *testing.T) {
	tests := []struct {
		err        error
		wantStatus int
		wantCode   string
	}{
		{app.ErrIdempotencyConflict, http.StatusConflict, CodeIdempotencyKeyConflict},
		{app.ErrExternalIDConflict, http.StatusConflict, CodeExternalIDConflict},
		{app.ErrWalletNotFound, http.StatusNotFound, CodeWalletNotFound},
		{app.ErrWalletPlayerMismatch, http.StatusUnprocessableEntity, CodeWalletPlayerMismatch},
		{app.ErrCurrencyMismatch, http.StatusUnprocessableEntity, CodeCurrencyMismatch},
		{app.ErrInvalidCommand, http.StatusBadRequest, CodeInvalidRequest},
		{context.DeadlineExceeded, http.StatusServiceUnavailable, CodeUnavailable},
		{app.ErrConcurrentInsert, http.StatusServiceUnavailable, CodeUnavailable},
		{errors.New("something unexpected"), http.StatusInternalServerError, CodeInternal},
	}
	for _, tc := range tests {
		t.Run(tc.wantCode, func(t *testing.T) {
			f := newFixture(t)
			f.proc.err = tc.err
			rec := postBet(f, "provider-a", "provider-a", "k")
			if rec.Code != tc.wantStatus || errorCode(t, rec) != tc.wantCode {
				t.Errorf("status=%d code=%s, want %d %s", rec.Code, errorCode(t, rec), tc.wantStatus, tc.wantCode)
			}
			if tc.wantStatus == http.StatusServiceUnavailable && rec.Header().Get("Retry-After") == "" {
				t.Errorf("503 must carry Retry-After")
			}
			if strings.Contains(rec.Body.String(), "something unexpected") {
				t.Errorf("internal error details leaked to the client")
			}
		})
	}
}

func TestProcess_Authorization(t *testing.T) {
	t.Run("providerId in the body must match the token", func(t *testing.T) {
		f := newFixture(t)
		rec := postBet(f, "provider-b", "provider-a", "k") // B se passando por A
		if rec.Code != http.StatusForbidden || errorCode(t, rec) != CodeForbidden {
			t.Errorf("status=%d code=%s, want 403 FORBIDDEN", rec.Code, errorCode(t, rec))
		}
		if len(f.proc.calls) != 0 {
			t.Errorf("a forged providerId reached the use case")
		}
	})

	t.Run("the internal service does not send operations", func(t *testing.T) {
		f := newFixture(t)
		rec := postBet(f, "internal", "provider-a", "k")
		if rec.Code != http.StatusForbidden || len(f.proc.calls) != 0 {
			t.Errorf("status=%d calls=%d, want 403 and no call", rec.Code, len(f.proc.calls))
		}
	})
}

func TestProcess_InvalidInput(t *testing.T) {
	valid := func() map[string]any {
		var m map[string]any
		_ = json.Unmarshal([]byte(betBody("provider-a")), &m)
		return m
	}
	encode := func(m map[string]any) string {
		b, _ := json.Marshal(m)
		return string(b)
	}
	with := func(key string, value any) string {
		m := valid()
		m[key] = value
		return encode(m)
	}
	without := func(key string) string {
		m := valid()
		delete(m, key)
		return encode(m)
	}

	tests := map[string]string{
		"money as a JSON number":        strings.Replace(betBody("provider-a"), `"amount": "25.00"`, `"amount": 25.00`, 1),
		"scientific notation":           with("money", map[string]string{"amount": "1e3", "currency": "BRL"}),
		"too many decimal places":       with("money", map[string]string{"amount": "1.234", "currency": "BRL"}),
		"negative amount":               with("money", map[string]string{"amount": "-1.00", "currency": "BRL"}),
		"NaN":                           with("money", map[string]string{"amount": "NaN", "currency": "BRL"}),
		"empty amount":                  with("money", map[string]string{"amount": "", "currency": "BRL"}),
		"bad currency":                  with("money", map[string]string{"amount": "1.00", "currency": "real"}),
		"missing money":                 without("money"),
		"OPENING is internal":           with("kind", "OPENING"),
		"unknown kind":                  with("kind", "JACKPOT"),
		"missing roundId":               without("roundId"),
		"missing externalTransactionId": without("externalTransactionId"),
		"walletId not a UUID":           with("walletId", "abc"),
		"unknown field":                 with("amount", "25.00"),
		"not JSON":                      "{",
		"two objects":                   betBody("provider-a") + "{}",
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			rec := f.do(call{method: "POST", path: "/wagering/transactions", token: "provider-a", body: body,
				headers: map[string]string{headerIdempotencyKey: "k"}})
			if rec.Code != http.StatusBadRequest || errorCode(t, rec) != CodeInvalidRequest {
				t.Errorf("status=%d code=%s, want 400 INVALID_REQUEST: %s", rec.Code, errorCode(t, rec), rec.Body.String())
			}
			if len(f.proc.calls) != 0 {
				t.Errorf("invalid input reached the use case")
			}
		})
	}

	t.Run("missing Idempotency-Key", func(t *testing.T) {
		f := newFixture(t)
		rec := postBet(f, "provider-a", "provider-a", "")
		if rec.Code != http.StatusBadRequest || errorCode(t, rec) != CodeMissingIdempotencyKey {
			t.Errorf("status=%d code=%s, want 400 MISSING_IDEMPOTENCY_KEY", rec.Code, errorCode(t, rec))
		}
	})

	t.Run("reversal carries the reference", func(t *testing.T) {
		f := newFixture(t)
		f.proc.result = app.Result{TransactionID: txID, Status: wager.StatusProcessed, Balance: brl(t, "1.00")}
		m := valid()
		m["kind"] = "REFUND"
		m["referenceExternalTransactionId"] = "transaction-100"
		rec := f.do(call{method: "POST", path: "/wagering/transactions", token: "provider-a", body: encode(m),
			headers: map[string]string{headerIdempotencyKey: "k"}})
		if rec.Code != http.StatusOK || f.proc.calls[0].ReferenceExternalID != "transaction-100" {
			t.Errorf("status=%d command=%+v", rec.Code, f.proc.calls)
		}
	})
}

// ---- carteiras --------------------------------------------------------------

func newWallet(t *testing.T, balance string) *wallet.Wallet {
	t.Helper()
	w, _, err := wallet.Open(walletID, playerID, brl(t, balance), time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func TestOpenWallet(t *testing.T) {
	body := `{"playerId":"` + playerID.String() + `","initialBalance":{"amount":"1000.00","currency":"BRL"}}`

	t.Run("internal service opens the wallet", func(t *testing.T) {
		f := newFixture(t)
		f.wallets.wallet = newWallet(t, "1000.00")
		rec := f.do(call{method: "POST", path: "/wallets", token: "internal", body: body})
		if rec.Code != http.StatusCreated {
			t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
		}
		got := decode(t, rec)
		balance, _ := got["balance"].(map[string]any)
		if got["id"] != walletID.String() || got["playerId"] != playerID.String() ||
			balance["amount"] != "1000.00" || got["version"] != float64(1) {
			t.Errorf("body = %v", got)
		}
		if f.wallets.lastOpen.InitialBalance != brl(t, "1000.00") || f.wallets.lastOpen.PlayerID != playerID {
			t.Errorf("command = %+v", f.wallets.lastOpen)
		}
	})

	t.Run("providers cannot open wallets", func(t *testing.T) {
		f := newFixture(t)
		rec := f.do(call{method: "POST", path: "/wallets", token: "provider-a", body: body})
		if rec.Code != http.StatusForbidden {
			t.Errorf("status = %d, want 403", rec.Code)
		}
	})

	t.Run("duplicate wallet", func(t *testing.T) {
		f := newFixture(t)
		f.wallets.err = app.ErrWalletAlreadyExists
		rec := f.do(call{method: "POST", path: "/wallets", token: "internal", body: body})
		if rec.Code != http.StatusConflict || errorCode(t, rec) != CodeWalletAlreadyExists {
			t.Errorf("status=%d code=%s, want 409", rec.Code, errorCode(t, rec))
		}
	})

	t.Run("negative initial balance", func(t *testing.T) {
		f := newFixture(t)
		bad := strings.Replace(body, `"1000.00"`, `"-1.00"`, 1)
		if rec := f.do(call{method: "POST", path: "/wallets", token: "internal", body: bad}); rec.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want 400", rec.Code)
		}
	})
}

func TestWalletReads(t *testing.T) {
	f := newFixture(t)
	w := newWallet(t, "100.00")
	f.wallets.wallet = w

	entry, err := ledger.New(uuid.New(), walletID, txID, wallet.Movement{
		Direction: wallet.DirectionCredit, Amount: brl(t, "100.00"),
		BalanceBefore: brl(t, "0.00"), BalanceAfter: brl(t, "100.00"),
	}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	f.wallets.page = app.LedgerPage{Entries: []ledger.Entry{entry}, NextCursor: "next-page"}

	rec := f.do(call{method: "GET", path: "/wallets/" + walletID.String(), token: "internal"})
	if rec.Code != http.StatusOK || decode(t, rec)["id"] != walletID.String() {
		t.Errorf("get wallet: %d %s", rec.Code, rec.Body.String())
	}

	rec = f.do(call{method: "GET", path: "/wallets/" + walletID.String() + "/ledger?limit=10", token: "internal"})
	body := decode(t, rec)
	entries, _ := body["entries"].([]any)
	if rec.Code != http.StatusOK || len(entries) != 1 || body["nextCursor"] != "next-page" || f.wallets.lastLimit != 10 {
		t.Errorf("ledger: %d %s (limit %d)", rec.Code, rec.Body.String(), f.wallets.lastLimit)
	}

	for _, q := range []string{"limit=0", "limit=abc", "limit=999"} {
		if rec := f.do(call{method: "GET", path: "/wallets/" + walletID.String() + "/ledger?" + q, token: "internal"}); rec.Code != http.StatusBadRequest {
			t.Errorf("ledger?%s: status %d, want 400", q, rec.Code)
		}
	}

	f.wallets.err = app.ErrInvalidCursor
	if rec := f.do(call{method: "GET", path: "/wallets/" + walletID.String() + "/ledger?cursor=x", token: "internal"}); rec.Code != http.StatusBadRequest || errorCode(t, rec) != CodeInvalidCursor {
		t.Errorf("bad cursor: %d %s", rec.Code, rec.Body.String())
	}
	f.wallets.err = nil

	if rec := f.do(call{method: "GET", path: "/wallets/not-a-uuid", token: "internal"}); rec.Code != http.StatusBadRequest {
		t.Errorf("bad wallet id: status %d, want 400", rec.Code)
	}
	if rec := f.do(call{method: "GET", path: "/wallets/" + walletID.String(), token: "provider-a"}); rec.Code != http.StatusForbidden {
		t.Errorf("provider reading a wallet: status %d, want 403", rec.Code)
	}

	f.wallets.err = app.ErrWalletNotFound
	if rec := f.do(call{method: "GET", path: "/wallets/" + walletID.String(), token: "internal"}); rec.Code != http.StatusNotFound {
		t.Errorf("unknown wallet: status %d, want 404", rec.Code)
	}
}

func TestReconciliation(t *testing.T) {
	f := newFixture(t)
	f.wallets.rec = app.Reconciliation{
		WalletID: walletID, Stored: brl(t, "975.00"), Calculated: brl(t, "975.00"),
		Difference: brl(t, "0.00"), Consistent: true, CheckedEntries: 2,
	}
	rec := f.do(call{method: "POST", path: "/wallets/" + walletID.String() + "/reconciliation", token: "internal"})
	body := decode(t, rec)
	stored, _ := body["storedBalance"].(map[string]any)
	diff, _ := body["difference"].(map[string]any)
	if rec.Code != http.StatusOK || body["walletId"] != walletID.String() || stored["amount"] != "975.00" ||
		diff["amount"] != "0.00" || body["consistent"] != true || body["checkedEntries"] != float64(2) {
		t.Errorf("reconciliation: %d %s", rec.Code, rec.Body.String())
	}
}

// ---- consultas de transação -------------------------------------------------

func TestTransactionQueries(t *testing.T) {
	f := newFixture(t)
	tx, err := wager.NewExternal(wager.ExternalParams{
		ID: txID, Kind: wager.KindRefund, ProviderID: "provider-a", ExternalID: "refund-1",
		IdempotencyKey: "k", WalletID: walletID, PlayerID: playerID, RoundID: "r", GameID: "g",
		Amount: brl(t, "25.00"), ReferenceExternalID: "bet-1",
	}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.MarkPendingReference(time.Now().Add(time.Second), time.Now()); err != nil {
		t.Fatal(err)
	}
	f.txs.tx = tx

	rec := f.do(call{method: "GET", path: "/wagering/transactions/" + txID.String(), token: "provider-a"})
	body := decode(t, rec)
	if rec.Code != http.StatusOK || body["status"] != "PENDING_REFERENCE" || body["attempts"] != float64(1) ||
		body["nextAttemptAt"] == nil || body["referenceExternalTransactionId"] != "bet-1" {
		t.Errorf("by id: %d %s", rec.Code, rec.Body.String())
	}

	rec = f.do(call{method: "GET", path: "/providers/provider-a/wagering/transactions/refund-1", token: "provider-a"})
	if rec.Code != http.StatusOK {
		t.Errorf("by external id: %d %s", rec.Code, rec.Body.String())
	}
	rec = f.do(call{method: "GET", path: "/providers/provider-a/wagering/transactions/refund-1", token: "provider-b"})
	if rec.Code != http.StatusForbidden {
		t.Errorf("another provider's route: status %d, want 403", rec.Code)
	}

	f.txs.err = app.ErrTransactionNotFound
	if rec := f.do(call{method: "GET", path: "/wagering/transactions/" + txID.String(), token: "provider-b"}); rec.Code != http.StatusNotFound {
		t.Errorf("not visible: status %d, want 404", rec.Code)
	}
}

// ---- observabilidade --------------------------------------------------------

func TestAccessLog_IsJSONWithIdentifiersAndNoSecrets(t *testing.T) {
	f := newFixture(t)
	f.proc.result = app.Result{TransactionID: txID, Status: wager.StatusProcessed, Balance: brl(t, "975.00")}

	req := httptest.NewRequest("POST", "/wagering/transactions", strings.NewReader(betBody("provider-a")))
	req.Header.Set("Authorization", "Bearer provider-a")
	req.Header.Set(headerIdempotencyKey, "k")
	req.Header.Set(headerCorrelationID, "corr-42")
	rec := httptest.NewRecorder()
	f.handler().ServeHTTP(rec, req)

	if rec.Header().Get(headerCorrelationID) != "corr-42" {
		t.Errorf("the client correlation id must be echoed back")
	}
	var entry map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(f.logs.Bytes()), &entry); err != nil {
		t.Fatalf("log is not a single JSON line: %v\n%s", err, f.logs.String())
	}
	for key, want := range map[string]any{
		"correlationId": "corr-42",
		"providerId":    "provider-a",
		"transactionId": txID.String(),
		"walletId":      walletID.String(),
		"route":         "POST /wagering/transactions",
		"status":        float64(200),
	} {
		if entry[key] != want {
			t.Errorf("log %s = %v, want %v", key, entry[key], want)
		}
	}
	for _, secret := range []string{"Bearer", "25.00", "fortune-chimp"} {
		if strings.Contains(f.logs.String(), secret) {
			t.Errorf("the access log must not contain %q", secret)
		}
	}
}

func TestHealthChecks_DoNotFloodTheInfoLog(t *testing.T) {
	f := newFixture(t)
	f.checks = []HealthCheck{{Name: "postgres", Check: func(context.Context) error { return nil }}}
	for i := 0; i < 5; i++ {
		f.do(call{method: "GET", path: "/health/live"})
		f.do(call{method: "GET", path: "/health/ready"})
	}
	if f.logs.Len() != 0 {
		t.Errorf("successful health checks were logged at info:\n%s", f.logs.String())
	}

	// uma falha de readiness continua aparecendo
	f.checks = []HealthCheck{{Name: "postgres", Check: func(context.Context) error { return errors.New("down") }}}
	f.do(call{method: "GET", path: "/health/ready"})
	if !strings.Contains(f.logs.String(), "readiness check failed") {
		t.Errorf("a failing readiness check must be logged:\n%s", f.logs.String())
	}
}

func TestMetricsRoute_IsPublicWhenConfigured(t *testing.T) {
	f := newFixture(t)
	h := NewHandler(Deps{
		Auth: fakeAuth{}, Processor: f.proc, Wallets: f.wallets, Transactions: f.txs,
		Metrics: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("metric_x 1\n")) }),
		Logger:  slog.New(slog.NewJSONHandler(f.logs, nil)),
	})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "metric_x 1") {
		t.Errorf("metrics: %d %q", rec.Code, rec.Body.String())
	}
	if f.logs.Len() != 0 {
		t.Errorf("metric scrapes must not be logged at info")
	}
}

func TestPanicIsRecovered(t *testing.T) {
	f := newFixture(t)
	h := NewHandler(Deps{
		Auth:         fakeAuth{},
		Processor:    panicky{},
		Wallets:      f.wallets,
		Transactions: f.txs,
		Logger:       slog.New(slog.NewJSONHandler(f.logs, nil)),
	})
	req := httptest.NewRequest("POST", "/wagering/transactions", strings.NewReader(betBody("provider-a")))
	req.Header.Set("Authorization", "Bearer provider-a")
	req.Header.Set(headerIdempotencyKey, "k")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
}

type panicky struct{}

func (panicky) Process(context.Context, app.ProcessCommand) (app.Result, error) {
	panic("boom")
}
