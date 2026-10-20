// Comando loadtest gera carga HTTP contra as instâncias da aplicação e mede
// desempenho e correção ao mesmo tempo.
//
// Mede: vazão, latência (p50/p95/p99/máx.), respostas por código e por
// resultado, conflitos de concorrência e atraso da outbox (lidos no /metrics
// das instâncias). Confere: que todo reenvio voltou como replay (sem débito
// duplicado) e, ao final, que o saldo de todas as carteiras bate com o ledger.
//
// Uso (com o ambiente no ar: docker compose up --build -d):
//
//	go run ./cmd/loadtest -duration 60s -concurrency 50 -wallets 100
//	go run ./cmd/loadtest -wallets 1 -report docs/load-test-hot-wallet.md   # disputa máxima
package main

import (
	"bufio"
	"context"
	crand "crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type options struct {
	targets     []string
	keycloak    string
	duration    time.Duration
	concurrency int
	wallets     int
	initial     string
	dupRatio    float64
	winRatio    float64
	report      string
}

func parseFlags() options {
	var o options
	var targets string
	flag.StringVar(&targets, "targets", "http://localhost:8081,http://localhost:8082,http://localhost:8083", "instâncias da aplicação, separadas por vírgula")
	flag.StringVar(&o.keycloak, "keycloak", "http://localhost:8080/realms/testetecnico", "realm do Keycloak")
	flag.DurationVar(&o.duration, "duration", 60*time.Second, "duração da carga (até 4m: os tokens são renovados)")
	flag.IntVar(&o.concurrency, "concurrency", 50, "requisições simultâneas")
	flag.IntVar(&o.wallets, "wallets", 100, "carteiras disputadas (menos carteiras = mais disputa pelo lock)")
	flag.StringVar(&o.initial, "initial-balance", "1000000.00", "saldo inicial de cada carteira")
	flag.Float64Var(&o.dupRatio, "dup-ratio", 0.10, "fração de requisições que reenviam uma operação já feita")
	flag.Float64Var(&o.winRatio, "win-ratio", 0.20, "fração de operações novas que são WIN (o resto é BET)")
	flag.StringVar(&o.report, "report", "", "se definido, grava o relatório em Markdown neste arquivo")
	flag.Parse()
	for _, t := range strings.Split(targets, ",") {
		if t = strings.TrimSpace(t); t != "" {
			o.targets = append(o.targets, strings.TrimRight(t, "/"))
		}
	}
	return o
}

func main() {
	o := parseFlags()
	if err := run(o); err != nil {
		fmt.Fprintln(os.Stderr, "loadtest:", err)
		os.Exit(1)
	}
}

// newUUID gera um UUID v4 aleatório (evita dependência externa).
func newUUID() string {
	var b [16]byte
	_, _ = crand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// ---- tokens -----------------------------------------------------------------

type tokenSource struct {
	realm, client string
	mu            sync.Mutex
	token         string
	fetched       time.Time
	http          *http.Client
}

func (t *tokenSource) get() (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.token != "" && time.Since(t.fetched) < 4*time.Minute {
		return t.token, nil
	}
	resp, err := t.http.PostForm(t.realm+"/protocol/openid-connect/token", url.Values{
		"grant_type": {"client_credentials"}, "client_id": {t.client}, "client_secret": {t.client + "-secret"},
	})
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var body struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil || body.AccessToken == "" {
		return "", fmt.Errorf("token for %s: HTTP %d", t.client, resp.StatusCode)
	}
	t.token, t.fetched = body.AccessToken, time.Now()
	return t.token, nil
}

// ---- operações --------------------------------------------------------------

type wallet struct{ id, player string }

type operation struct {
	key, body string
}

// sentOps guarda operações já enviadas, para os reenvios.
type sentOps struct {
	mu  sync.Mutex
	ops []operation
}

func (s *sentOps) add(op operation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.ops) < 20000 {
		s.ops = append(s.ops, op)
	} else {
		s.ops[rand.IntN(len(s.ops))] = op
	}
}

func (s *sentOps) pick() (operation, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.ops) == 0 {
		return operation{}, false
	}
	return s.ops[rand.IntN(len(s.ops))], true
}

// ---- resultados -------------------------------------------------------------

type sample struct {
	latency   time.Duration
	code      int
	status    string
	replay    bool
	duplicate bool // a requisição era um reenvio
	transport bool // erro de rede, sem resposta
}

type stats struct {
	mu      sync.Mutex
	samples []sample
}

func (s *stats) add(x sample) {
	s.mu.Lock()
	s.samples = append(s.samples, x)
	s.mu.Unlock()
}

// ---- execução ---------------------------------------------------------------

func run(o options) error {
	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        o.concurrency * 2,
			MaxIdleConnsPerHost: o.concurrency,
		},
	}
	internal := &tokenSource{realm: o.keycloak, client: "internal-service", http: client}
	provider := &tokenSource{realm: o.keycloak, client: "provider-a", http: client}
	if _, err := internal.get(); err != nil {
		return fmt.Errorf("Keycloak not reachable (is the environment up?): %w", err)
	}

	fmt.Printf("Abrindo %d carteiras...\n", o.wallets)
	wallets, err := openWallets(client, internal, o)
	if err != nil {
		return err
	}

	before := scrapeAll(client, o.targets)
	runID := newUUID()[:8]
	st := &stats{}
	sent := &sentOps{}
	var seq atomic.Int64

	// Amostragem do atraso da outbox durante a carga.
	var maxLag float64
	var maxPending float64
	samplerCtx, stopSampler := context.WithCancel(context.Background())
	samplerDone := make(chan struct{})
	go func() {
		defer close(samplerDone)
		t := time.NewTicker(500 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-samplerCtx.Done():
				return
			case <-t.C:
				m := scrape(client, o.targets[0])
				maxLag = max(maxLag, m["testetecnico_outbox_lag_seconds"])
				maxPending = max(maxPending, m["testetecnico_outbox_pending_events"])
			}
		}
	}()

	fmt.Printf("Carga: %s, %d requisições simultâneas, %d instâncias, %d carteiras, %.0f%% reenvios\n",
		o.duration, o.concurrency, len(o.targets), o.wallets, o.dupRatio*100)
	ctx, cancel := context.WithTimeout(context.Background(), o.duration)
	defer cancel()
	start := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < o.concurrency; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for ctx.Err() == nil {
				target := o.targets[rand.IntN(len(o.targets))]
				var op operation
				duplicate := false
				if rand.Float64() < o.dupRatio {
					op, duplicate = sent.pick()
				}
				if !duplicate {
					op = newOperation(runID, seq.Add(1), wallets[rand.IntN(len(wallets))], o.winRatio)
				}
				tok, err := provider.get()
				if err != nil {
					st.add(sample{transport: true, duplicate: duplicate})
					continue
				}
				x := send(client, target, tok, op)
				x.duplicate = duplicate
				st.add(x)
				if !duplicate && !x.transport && x.code < 500 {
					sent.add(op)
				}
			}
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)

	// Tempo para a outbox esvaziar depois da carga.
	drainStart := time.Now()
	drained := time.Duration(-1)
	for time.Since(drainStart) < 2*time.Minute {
		if scrape(client, o.targets[0])["testetecnico_outbox_pending_events"] == 0 {
			drained = time.Since(drainStart)
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	stopSampler()
	<-samplerDone

	after := scrapeAll(client, o.targets)

	fmt.Println("Reconciliando as carteiras...")
	consistent, checked := reconcile(client, internal, o.targets[0], wallets)

	r := buildReport(o, st, elapsed, before, after, maxLag, maxPending, drained, consistent, checked)
	fmt.Println()
	fmt.Println(r)
	if o.report != "" {
		if err := os.WriteFile(o.report, []byte(r), 0o644); err != nil {
			return err
		}
		fmt.Println("Relatório gravado em", o.report)
	}
	if consistent != checked {
		return fmt.Errorf("%d of %d wallets are inconsistent", checked-consistent, checked)
	}
	return nil
}

func openWallets(client *http.Client, internal *tokenSource, o options) ([]wallet, error) {
	out := make([]wallet, 0, o.wallets)
	for i := 0; i < o.wallets; i++ {
		tok, err := internal.get()
		if err != nil {
			return nil, err
		}
		player := newUUID()
		body := fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":%q,"currency":"BRL"}}`, player, o.initial)
		req, _ := http.NewRequest("POST", o.targets[i%len(o.targets)]+"/wallets", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+tok)
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		var w struct {
			ID string `json:"id"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&w)
		resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			return nil, fmt.Errorf("opening wallet: HTTP %d", resp.StatusCode)
		}
		out = append(out, wallet{id: w.ID, player: player})
	}
	return out, nil
}

func newOperation(runID string, n int64, w wallet, winRatio float64) operation {
	kind := "BET"
	if rand.Float64() < winRatio {
		kind = "WIN"
	}
	ext := fmt.Sprintf("load-%s-%d", runID, n)
	amount := fmt.Sprintf("%d.%02d", 1+rand.IntN(5), rand.IntN(100))
	body := fmt.Sprintf(`{"providerId":"provider-a","externalTransactionId":%q,"playerId":%q,"walletId":%q,`+
		`"roundId":"round-%d","gameId":"loadtest","kind":%q,"money":{"amount":%q,"currency":"BRL"}}`,
		ext, w.player, w.id, n%1000, kind, amount)
	return operation{key: "provider-a:" + ext, body: body}
}

func send(client *http.Client, target, token string, op operation) sample {
	req, _ := http.NewRequest("POST", target+"/wagering/transactions", strings.NewReader(op.body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", op.key)
	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return sample{latency: time.Since(start), transport: true}
	}
	defer resp.Body.Close()
	var body struct {
		Status string `json:"status"`
		Replay bool   `json:"idempotentReplay"`
	}
	raw, _ := io.ReadAll(resp.Body)
	lat := time.Since(start)
	_ = json.Unmarshal(raw, &body)
	return sample{latency: lat, code: resp.StatusCode, status: body.Status, replay: body.Replay}
}

func reconcile(client *http.Client, internal *tokenSource, target string, wallets []wallet) (int, int) {
	consistent := 0
	for _, w := range wallets {
		tok, err := internal.get()
		if err != nil {
			continue
		}
		req, _ := http.NewRequest("POST", target+"/wallets/"+w.id+"/reconciliation", nil)
		req.Header.Set("Authorization", "Bearer "+tok)
		resp, err := client.Do(req)
		if err != nil {
			continue
		}
		var r struct {
			Consistent bool `json:"consistent"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&r)
		resp.Body.Close()
		if r.Consistent {
			consistent++
		}
	}
	return consistent, len(wallets)
}

// ---- métricas das instâncias ------------------------------------------------

// scrape lê as métricas sem rótulos de uma instância (nome -> valor).
func scrape(client *http.Client, target string) map[string]float64 {
	out := map[string]float64{}
	resp, err := client.Get(target + "/metrics")
	if err != nil {
		return out
	}
	defer resp.Body.Close()
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "#") || !strings.HasPrefix(line, "testetecnico_") {
			continue
		}
		name, value, ok := strings.Cut(line, " ")
		if !ok {
			continue
		}
		if v, err := strconv.ParseFloat(strings.TrimSpace(value), 64); err == nil {
			out[name] = v
		}
	}
	return out
}

// scrapeAll soma as métricas de todas as instâncias.
func scrapeAll(client *http.Client, targets []string) map[string]float64 {
	total := map[string]float64{}
	for _, t := range targets {
		for k, v := range scrape(client, t) {
			total[k] += v
		}
	}
	return total
}

// ---- relatório --------------------------------------------------------------

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(float64(len(sorted)-1) * p)
	return sorted[i]
}

func ms(d time.Duration) string {
	return fmt.Sprintf("%.1f ms", float64(d.Microseconds())/1000)
}

func cpuModel() string {
	f, err := os.Open("/proc/cpuinfo")
	if err != nil {
		return runtime.GOARCH
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if k, v, ok := strings.Cut(sc.Text(), ":"); ok && strings.TrimSpace(k) == "model name" {
			return strings.TrimSpace(v)
		}
	}
	return runtime.GOARCH
}

func buildReport(o options, st *stats, elapsed time.Duration, before, after map[string]float64,
	maxLag, maxPending float64, drained time.Duration, consistent, checked int) string {

	st.mu.Lock()
	samples := st.samples
	st.mu.Unlock()

	var lats []time.Duration
	codes := map[int]int{}
	results := map[string]int{}
	transport, dupRequests, dupReplays, dupNotReplay := 0, 0, 0, 0
	for _, s := range samples {
		if s.transport {
			transport++
			continue
		}
		lats = append(lats, s.latency)
		codes[s.code]++
		label := s.status
		if label == "" {
			label = fmt.Sprintf("HTTP %d", s.code)
		}
		if s.replay {
			label += " (replay)"
		}
		results[label]++
		if s.duplicate {
			dupRequests++
			if s.replay {
				dupReplays++
			} else if s.code < 300 || s.code == http.StatusUnprocessableEntity {
				dupNotReplay++
			}
		}
	}
	slices.Sort(lats)
	total := len(samples)
	errors5xx := 0
	for c, n := range codes {
		if c >= 500 {
			errors5xx += n
		}
	}
	conflicts := after["testetecnico_concurrency_conflicts_total"] - before["testetecnico_concurrency_conflicts_total"]

	var b strings.Builder
	w := func(format string, args ...any) { fmt.Fprintf(&b, format+"\n", args...) }

	w("# Teste de carga: %s", time.Now().Format("2006-01-02 15:04"))
	w("")
	w("## Parâmetros")
	w("")
	w("| Parâmetro | Valor |")
	w("|---|---|")
	w("| Duração | %s |", o.duration)
	w("| Requisições simultâneas | %d |", o.concurrency)
	w("| Instâncias | %d (%s) |", len(o.targets), strings.Join(o.targets, ", "))
	w("| Carteiras disputadas | %d |", o.wallets)
	w("| Mistura | %.0f%% WIN, %.0f%% BET nas operações novas; %.0f%% das requisições são reenvios |", o.winRatio*100, (1-o.winRatio)*100, o.dupRatio*100)
	w("| Comando | `go run ./cmd/loadtest -duration %s -concurrency %d -wallets %d -dup-ratio %.2f` |", o.duration, o.concurrency, o.wallets, o.dupRatio)
	w("")
	w("## Ambiente")
	w("")
	w("| Item | Valor |")
	w("|---|---|")
	w("| CPU | %s (%d núcleos lógicos) |", cpuModel(), runtime.NumCPU())
	w("| Sistema | %s/%s |", runtime.GOOS, runtime.GOARCH)
	w("| Gerador | este programa, na mesma máquina que o ambiente (disputa CPU com ele) |")
	w("")
	w("## Resultados")
	w("")
	w("| Métrica | Valor |")
	w("|---|---|")
	w("| Requisições | %d em %s |", total, elapsed.Round(time.Millisecond))
	w("| Vazão | %.0f req/s |", float64(total)/elapsed.Seconds())
	w("| Latência p50 | %s |", ms(percentile(lats, 0.50)))
	w("| Latência p95 | %s |", ms(percentile(lats, 0.95)))
	w("| Latência p99 | %s |", ms(percentile(lats, 0.99)))
	if len(lats) > 0 {
		w("| Latência máxima | %s |", ms(lats[len(lats)-1]))
	}
	w("| Erros 5xx | %d |", errors5xx)
	w("| Erros de rede | %d |", transport)
	w("| Conflitos de concorrência (métrica) | %.0f |", conflicts)
	w("| Atraso máximo da outbox | %.2f s |", maxLag)
	w("| Pico de eventos pendentes na outbox | %.0f |", maxPending)
	if drained >= 0 {
		w("| Tempo para a outbox esvaziar após a carga | %s |", drained.Round(10*time.Millisecond))
	} else {
		w("| Tempo para a outbox esvaziar após a carga | não esvaziou em 2 min |")
	}
	w("")
	w("### Respostas")
	w("")
	w("| Código HTTP | Quantidade |")
	w("|---|---|")
	keys := make([]int, 0, len(codes))
	for c := range codes {
		keys = append(keys, c)
	}
	slices.Sort(keys)
	for _, c := range keys {
		w("| %d | %d |", c, codes[c])
	}
	w("")
	w("| Resultado | Quantidade |")
	w("|---|---|")
	labels := make([]string, 0, len(results))
	for l := range results {
		labels = append(labels, l)
	}
	slices.Sort(labels)
	for _, l := range labels {
		w("| %s | %d |", l, results[l])
	}
	w("")
	w("## Correção sob carga")
	w("")
	w("| Verificação | Resultado |")
	w("|---|---|")
	w("| Reenvios respondidos como replay | %d de %d |", dupReplays, dupRequests)
	w("| Reenvios aplicados de novo (deve ser 0) | %d |", dupNotReplay)
	w("| Carteiras consistentes na reconciliação | %d de %d |", consistent, checked)
	return b.String()
}
