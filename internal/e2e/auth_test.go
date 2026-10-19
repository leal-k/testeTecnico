package e2e_test

// Testes ponta a ponta de autenticação e autorização, com tudo real:
// Keycloak (em container, com o realm do projeto), PostgreSQL (em container) e
// a aplicação montada pelo Fx, atendendo HTTP de verdade.
//
// Ignorados com -short ou sem Docker. O primeiro uso baixa a imagem do
// Keycloak e leva alguns segundos para ele subir.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"github.com/leal-k/testeTecnico/internal/platform/config"
	"github.com/leal-k/testeTecnico/internal/platform/fxapp"
	"github.com/leal-k/testeTecnico/internal/testsupport/pgtest"
)

// keycloakImage deve ser a mesma versão usada no docker-compose.yml.
const keycloakImage = "quay.io/keycloak/keycloak:26.8.1"

// tokenLifespan curto (só neste realm de teste) para testar token expirado de
// verdade, emitido pelo próprio Keycloak.
const tokenLifespan = 4 * time.Second

var (
	dsn        string
	realmURL   string // http://host:porta/realms/testetecnico
	skipReason string
)

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		skipReason = "end-to-end tests are skipped with -short"
		os.Exit(m.Run())
	}
	os.Exit(run(m))
}

func run(m *testing.M) int {
	ctx := context.Background()

	d, stopPG, err := pgtest.Start(ctx)
	if err != nil {
		skipReason = "PostgreSQL container unavailable: " + err.Error()
		return m.Run()
	}
	defer stopPG()
	dsn = d

	realmFile, err := testRealm()
	if err != nil {
		fmt.Fprintln(os.Stderr, "preparing the test realm:", err)
		return 1
	}
	defer os.Remove(realmFile)

	kc, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        keycloakImage,
			Cmd:          []string{"start-dev", "--import-realm"},
			ExposedPorts: []string{"8080/tcp"},
			Env:          map[string]string{"KC_HTTP_PORT": "8080"},
			Files: []testcontainers.ContainerFile{{
				HostFilePath:      realmFile,
				ContainerFilePath: "/opt/keycloak/data/import/testetecnico-realm.json",
				FileMode:          0o644,
			}},
			WaitingFor: wait.ForHTTP("/realms/testetecnico/.well-known/openid-configuration").
				WithPort("8080/tcp").
				WithStartupTimeout(3 * time.Minute),
		},
		Started: true,
	})
	if kc != nil {
		defer func() { _ = kc.Terminate(context.Background()) }()
	}
	if err != nil {
		skipReason = "Keycloak container unavailable: " + err.Error()
		return m.Run()
	}
	endpoint, err := kc.PortEndpoint(ctx, "8080/tcp", "http")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	realmURL = endpoint + "/realms/testetecnico"
	return m.Run()
}

// testRealm copia o realm do projeto, encurtando só a validade do token.
func testRealm() (string, error) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "deploy", "keycloak", "testetecnico-realm.json"))
	if err != nil {
		return "", err
	}
	var realm map[string]any
	if err := json.Unmarshal(raw, &realm); err != nil {
		return "", err
	}
	realm["accessTokenLifespan"] = int(tokenLifespan.Seconds())
	out, err := json.Marshal(realm)
	if err != nil {
		return "", err
	}
	f, err := os.CreateTemp("", "testetecnico-realm-*.json")
	if err != nil {
		return "", err
	}
	defer f.Close()
	_, err = f.Write(out)
	return f.Name(), err
}

// ---- aplicação --------------------------------------------------------------

type harness struct {
	t    *testing.T
	base string
	pool *pgxpool.Pool
}

func startApp(t *testing.T) *harness {
	t.Helper()
	if skipReason != "" {
		t.Skip(skipReason)
	}
	cfg := config.Config{
		InstanceID:            "e2e",
		LogLevel:              "error",
		HTTPAddr:              "127.0.0.1:0",
		RequestTimeout:        10 * time.Second,
		ShutdownTimeout:       10 * time.Second,
		DatabaseURL:           dsn,
		DBMaxConns:            5,
		AuthIssuerURL:         realmURL,
		AuthJWKSURL:           realmURL + "/protocol/openid-connect/certs",
		AuthAudience:          "testetecnico-api",
		PendingWorkerInterval: time.Second,
		PendingBatchSize:      10,
		ReferenceTTL:          time.Minute,
		ReferenceBackoffBase:  time.Second,
		ReferenceBackoffMax:   time.Second,
		SQSEnabled:            false, // a mensageria tem os próprios testes
	}
	var hs *fxapp.HTTPServer
	app := fxtest.New(t, fxapp.Options(cfg), fx.Populate(&hs))
	app.RequireStart()
	t.Cleanup(app.RequireStop)

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return &harness{t: t, base: "http://" + hs.Addr(), pool: pool}
}

// token pede um token de acesso real ao Keycloak (client_credentials).
func token(t *testing.T, clientID string) string {
	t.Helper()
	resp, err := http.PostForm(realmURL+"/protocol/openid-connect/token", url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {clientID},
		"client_secret": {clientID + "-secret"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil || body.AccessToken == "" {
		t.Fatalf("token for %s (HTTP %d): %v", clientID, resp.StatusCode, err)
	}
	return body.AccessToken
}

type response struct {
	status int
	body   map[string]any
	header http.Header
}

func (r response) errorCode() string {
	e, _ := r.body["error"].(map[string]any)
	code, _ := e["code"].(string)
	return code
}

func (h *harness) call(method, path, bearer, body string, headers map[string]string) response {
	h.t.Helper()
	req, err := http.NewRequest(method, h.base+path, strings.NewReader(body))
	if err != nil {
		h.t.Fatal(err)
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	out := response{status: resp.StatusCode, header: resp.Header}
	_ = json.Unmarshal(raw, &out.body)
	return out
}

type wallet struct{ id, player string }

func (h *harness) openWallet(balance string) wallet {
	h.t.Helper()
	player := uuid.NewString()
	r := h.call("POST", "/wallets", token(h.t, "internal-service"),
		fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":%q,"currency":"BRL"}}`, player, balance), nil)
	if r.status != http.StatusCreated {
		h.t.Fatalf("opening wallet: %d %v", r.status, r.body)
	}
	return wallet{id: r.body["id"].(string), player: player}
}

func betBody(provider string, w wallet, ext, amount string) string {
	return fmt.Sprintf(`{"providerId":%q,"externalTransactionId":%q,"playerId":%q,"walletId":%q,
		"roundId":"round-1","gameId":"game-1","kind":"BET","money":{"amount":%q,"currency":"BRL"}}`,
		provider, ext, w.player, w.id, amount)
}

func (h *harness) bet(bearer, provider string, w wallet, ext, amount string) response {
	h.t.Helper()
	return h.call("POST", "/wagering/transactions", bearer, betBody(provider, w, ext, amount),
		map[string]string{"Idempotency-Key": provider + ":" + ext})
}

// state devolve o saldo (em centavos) e quantas transações a carteira tem.
func (h *harness) state(w wallet) (int64, int) {
	h.t.Helper()
	var balance int64
	var txs int
	err := h.pool.QueryRow(context.Background(), `
		SELECT w.balance_minor, (SELECT count(*) FROM wager_transactions t WHERE t.wallet_id = w.id)
		FROM wallets w WHERE w.id = $1`, w.id).Scan(&balance, &txs)
	if err != nil {
		h.t.Fatal(err)
	}
	return balance, txs
}

// ---- testes -----------------------------------------------------------------

func TestE2E_RealTokensAreAcceptedWithTheRightIdentity(t *testing.T) {
	h := startApp(t)
	w := h.openWallet("100.00")

	r := h.bet(token(t, "provider-a"), "provider-a", w, "bet-1", "10.00")
	if r.status != http.StatusOK || r.body["status"] != "PROCESSED" {
		t.Fatalf("bet with a real provider token: %d %v", r.status, r.body)
	}
	if bal, _ := h.state(w); bal != 9000 {
		t.Errorf("balance = %d, want 9000", bal)
	}
}

func TestE2E_MissingInvalidTamperedAndExpiredCredentialsAreRejected(t *testing.T) {
	h := startApp(t)
	w := h.openWallet("100.00")
	before, txsBefore := h.state(w)

	valid := token(t, "provider-a")
	parts := strings.Split(valid, ".")
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	forged := strings.ReplaceAll(string(payload), `"provider-a"`, `"provider-b"`)
	tampered := parts[0] + "." + base64.RawURLEncoding.EncodeToString([]byte(forged)) + "." + parts[2]

	expired := token(t, "provider-a")
	time.Sleep(tokenLifespan + 2*time.Second) // o token emitido pelo Keycloak vence

	for name, bearer := range map[string]string{
		"missing":  "",
		"garbage":  "not-a-token",
		"tampered": tampered,
		"expired":  expired,
	} {
		t.Run(name, func(t *testing.T) {
			r := h.bet(bearer, "provider-a", w, "bet-"+name, "10.00")
			if r.status != http.StatusUnauthorized || r.errorCode() != "UNAUTHENTICATED" {
				t.Errorf("status=%d code=%s, want 401 UNAUTHENTICATED", r.status, r.errorCode())
			}
			if r.header.Get("WWW-Authenticate") == "" {
				t.Errorf("401 without WWW-Authenticate")
			}
		})
	}

	// Segredo errado: o próprio Keycloak não emite token.
	resp, err := http.PostForm(realmURL+"/protocol/openid-connect/token", url.Values{
		"grant_type": {"client_credentials"}, "client_id": {"provider-a"}, "client_secret": {"wrong"},
	})
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("wrong secret: Keycloak answered %d, want 401", resp.StatusCode)
	}

	// Nenhuma tentativa recusada teve efeito financeiro.
	if after, txsAfter := h.state(w); after != before || txsAfter != txsBefore {
		t.Errorf("rejected credentials changed the wallet: %d→%d, transactions %d→%d", before, after, txsBefore, txsAfter)
	}
}

func TestE2E_ProvidersAreIsolatedInQueriesAndReplays(t *testing.T) {
	h := startApp(t)
	w := h.openWallet("100.00")

	a := h.bet(token(t, "provider-a"), "provider-a", w, "shared-ext", "10.00")
	if a.status != http.StatusOK {
		t.Fatalf("provider-a bet: %d %v", a.status, a.body)
	}
	txA := a.body["transactionId"].(string)
	tokenB := token(t, "provider-b")

	// B não lê a transação de A: por ID responde como inexistente...
	if r := h.call("GET", "/wagering/transactions/"+txA, tokenB, "", nil); r.status != http.StatusNotFound {
		t.Errorf("B reading A's transaction by id: %d, want 404", r.status)
	} else if strings.Contains(fmt.Sprint(r.body), "shared-ext") {
		t.Errorf("the 404 body leaked A's data: %v", r.body)
	}
	// ...e pela rota de A é proibido.
	if r := h.call("GET", "/providers/provider-a/wagering/transactions/shared-ext", tokenB, "", nil); r.status != http.StatusForbidden {
		t.Errorf("B using A's route: %d, want 403", r.status)
	}
	// A continua lendo a própria.
	if r := h.call("GET", "/wagering/transactions/"+txA, token(t, "provider-a"), "", nil); r.status != http.StatusOK {
		t.Errorf("A reading its own transaction: %d", r.status)
	}

	balance, txs := h.state(w)

	// B tenta "replayar" a operação de A com a MESMA chave e o MESMO corpo
	// (providerId de A): recusado antes do caso de uso.
	forged := h.call("POST", "/wagering/transactions", tokenB, betBody("provider-a", w, "shared-ext", "10.00"),
		map[string]string{"Idempotency-Key": "provider-a:shared-ext"})
	if forged.status != http.StatusForbidden {
		t.Errorf("B replaying as A: %d, want 403", forged.status)
	}
	if _, leaked := forged.body["transactionId"]; leaked {
		t.Errorf("B received A's stored result: %v", forged.body)
	}
	if b, n := h.state(w); b != balance || n != txs {
		t.Errorf("the forged replay had a financial effect")
	}

	// Com a própria identidade, a mesma chave é OUTRA operação (escopo por
	// provedor): B nunca recebe o resultado de A.
	own := h.call("POST", "/wagering/transactions", tokenB, betBody("provider-b", w, "shared-ext", "10.00"),
		map[string]string{"Idempotency-Key": "provider-a:shared-ext"})
	if own.status != http.StatusOK || own.body["transactionId"] == txA || own.body["idempotentReplay"] != false {
		t.Errorf("B with its own identity: %d %v", own.status, own.body)
	}
}

func TestE2E_InternalOperationsAreRestricted(t *testing.T) {
	h := startApp(t)
	w := h.openWallet("100.00")
	before, txsBefore := h.state(w)
	provider := token(t, "provider-a")
	internal := token(t, "internal-service")

	forbidden := []struct{ method, path, body string }{
		{"POST", "/wallets", fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":"5.00","currency":"BRL"}}`, uuid.NewString())},
		{"GET", "/wallets/" + w.id, ""},
		{"GET", "/wallets/" + w.id + "/ledger", ""},
		{"POST", "/wallets/" + w.id + "/reconciliation", ""},
	}
	for _, f := range forbidden {
		r := h.call(f.method, f.path, provider, f.body, nil)
		if r.status != http.StatusForbidden || r.errorCode() != "FORBIDDEN" {
			t.Errorf("provider on %s %s: %d %s, want 403", f.method, f.path, r.status, r.errorCode())
		}
		if _, leaked := r.body["balance"]; leaked {
			t.Errorf("%s %s leaked the balance to a provider", f.method, f.path)
		}
	}

	// O serviço interno não envia operações de aposta.
	if r := h.bet(internal, "provider-a", w, "bet-internal", "10.00"); r.status != http.StatusForbidden {
		t.Errorf("internal service sending a bet: %d, want 403", r.status)
	}

	if after, txsAfter := h.state(w); after != before || txsAfter != txsBefore {
		t.Errorf("forbidden calls changed the wallet")
	}
	// E o serviço interno lê normalmente.
	if r := h.call("GET", "/wallets/"+w.id, internal, "", nil); r.status != http.StatusOK {
		t.Errorf("internal reading the wallet: %d", r.status)
	}
}
