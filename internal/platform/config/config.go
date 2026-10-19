// Package config lê e valida a configuração da aplicação a partir de
// variáveis de ambiente.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	InstanceID string
	LogLevel   string

	HTTPAddr        string
	RequestTimeout  time.Duration
	ShutdownTimeout time.Duration

	DatabaseURL string
	DBMaxConns  int32

	AuthIssuerURL string
	AuthJWKSURL   string
	AuthAudience  string

	PendingWorkerInterval time.Duration
	PendingBatchSize      int
	ReferenceTTL          time.Duration
	ReferenceBackoffBase  time.Duration
	ReferenceBackoffMax   time.Duration

	SQSEnabled           bool
	AWSRegion            string
	SQSEndpointURL       string
	SQSQueueName         string
	SQSDLQName           string
	SQSConsumerName      string
	SQSWaitTime          time.Duration
	SQSVisibilityTimeout time.Duration
	SQSHandleTimeout     time.Duration
	SQSMaxMessages       int
	SQSMaxReceives       int
	SQSRetryBase         time.Duration
	SQSRetryMax          time.Duration
}

// Load lê o ambiente. Toda variável tem um padrão de desenvolvimento, menos as
// que identificam infraestrutura (banco e IdP), que são obrigatórias.
func Load() (Config, error) {
	return load(os.Getenv)
}

func load(getenv func(string) string) (Config, error) {
	r := reader{getenv: getenv}
	hostname, _ := os.Hostname()

	cfg := Config{
		InstanceID: r.str("INSTANCE_ID", hostname),
		LogLevel:   strings.ToLower(r.str("LOG_LEVEL", "info")),

		HTTPAddr:        r.str("HTTP_ADDR", ":8080"),
		RequestTimeout:  r.duration("HTTP_REQUEST_TIMEOUT", 15*time.Second),
		ShutdownTimeout: r.duration("SHUTDOWN_TIMEOUT", 20*time.Second),

		DatabaseURL: r.required("DATABASE_URL"),
		DBMaxConns:  int32(r.integer("DB_MAX_CONNS", 20)),

		AuthIssuerURL: r.required("AUTH_ISSUER_URL"),
		AuthJWKSURL:   r.required("AUTH_JWKS_URL"),
		AuthAudience:  r.str("AUTH_AUDIENCE", "testetecnico-api"),

		PendingWorkerInterval: r.duration("PENDING_WORKER_INTERVAL", time.Second),
		PendingBatchSize:      r.integer("PENDING_BATCH_SIZE", 100),
		ReferenceTTL:          r.duration("REFERENCE_TTL", 5*time.Minute),
		ReferenceBackoffBase:  r.duration("REFERENCE_BACKOFF_BASE", time.Second),
		ReferenceBackoffMax:   r.duration("REFERENCE_BACKOFF_MAX", time.Minute),

		SQSEnabled:           r.boolean("SQS_ENABLED", true),
		AWSRegion:            r.str("AWS_REGION", "us-east-1"),
		SQSEndpointURL:       r.str("SQS_ENDPOINT_URL", ""),
		SQSQueueName:         r.str("SQS_QUEUE_NAME", "wager-transactions.fifo"),
		SQSDLQName:           r.str("SQS_DLQ_NAME", "wager-transactions-dlq.fifo"),
		SQSConsumerName:      r.str("SQS_CONSUMER_NAME", "wager-transactions-consumer"),
		SQSWaitTime:          r.duration("SQS_WAIT_TIME", 10*time.Second),
		SQSVisibilityTimeout: r.duration("SQS_VISIBILITY_TIMEOUT", 60*time.Second),
		SQSHandleTimeout:     r.duration("SQS_HANDLE_TIMEOUT", 15*time.Second),
		SQSMaxMessages:       r.integer("SQS_MAX_MESSAGES", 10),
		SQSMaxReceives:       r.integer("SQS_MAX_RECEIVES", 5),
		SQSRetryBase:         r.duration("SQS_RETRY_BASE", 2*time.Second),
		SQSRetryMax:          r.duration("SQS_RETRY_MAX", time.Minute),
	}
	if len(r.errs) > 0 {
		return Config{}, errors.Join(r.errs...)
	}
	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) validate() error {
	var errs []error
	check := func(ok bool, format string, args ...any) {
		if !ok {
			errs = append(errs, fmt.Errorf(format, args...))
		}
	}
	for name, raw := range map[string]string{
		"DATABASE_URL":    c.DatabaseURL,
		"AUTH_ISSUER_URL": c.AuthIssuerURL,
		"AUTH_JWKS_URL":   c.AuthJWKSURL,
	} {
		if raw == "" {
			continue // já reportado como ausente
		}
		u, err := url.Parse(raw)
		check(err == nil && u.Scheme != "" && u.Host != "", "%s is not a valid URL", name)
	}
	check(c.LogLevel == "debug" || c.LogLevel == "info" || c.LogLevel == "warn" || c.LogLevel == "error",
		"LOG_LEVEL must be debug, info, warn or error")
	check(c.DBMaxConns >= 2, "DB_MAX_CONNS must be at least 2")
	check(c.RequestTimeout > 0, "HTTP_REQUEST_TIMEOUT must be positive")
	check(c.ShutdownTimeout > 0, "SHUTDOWN_TIMEOUT must be positive")
	check(c.PendingWorkerInterval > 0, "PENDING_WORKER_INTERVAL must be positive")
	check(c.PendingBatchSize > 0, "PENDING_BATCH_SIZE must be positive")
	check(c.ReferenceTTL > 0, "REFERENCE_TTL must be positive")
	check(c.ReferenceBackoffBase > 0 && c.ReferenceBackoffMax >= c.ReferenceBackoffBase,
		"REFERENCE_BACKOFF_BASE must be positive and not larger than REFERENCE_BACKOFF_MAX")

	if c.SQSEnabled {
		if c.SQSEndpointURL != "" {
			u, err := url.Parse(c.SQSEndpointURL)
			check(err == nil && u.Scheme != "" && u.Host != "", "SQS_ENDPOINT_URL is not a valid URL")
		}
		check(c.SQSWaitTime >= 0 && c.SQSWaitTime <= 20*time.Second, "SQS_WAIT_TIME must be between 0s and 20s")
		check(c.SQSMaxMessages >= 1 && c.SQSMaxMessages <= 10, "SQS_MAX_MESSAGES must be between 1 and 10")
		check(c.SQSMaxReceives >= 1, "SQS_MAX_RECEIVES must be at least 1")
		check(c.SQSHandleTimeout > 0, "SQS_HANDLE_TIMEOUT must be positive")
		// A mensagem não pode reaparecer para outra instância enquanto ainda
		// está sendo tratada aqui.
		check(c.SQSVisibilityTimeout > c.SQSHandleTimeout,
			"SQS_VISIBILITY_TIMEOUT must be larger than SQS_HANDLE_TIMEOUT")
		// O encerramento precisa caber a mensagem em andamento.
		check(c.ShutdownTimeout > c.SQSHandleTimeout,
			"SHUTDOWN_TIMEOUT must be larger than SQS_HANDLE_TIMEOUT")
		check(c.SQSRetryBase > 0 && c.SQSRetryMax >= c.SQSRetryBase,
			"SQS_RETRY_BASE must be positive and not larger than SQS_RETRY_MAX")
	}
	return errors.Join(errs...)
}

type reader struct {
	getenv func(string) string
	errs   []error
}

func (r *reader) str(key, def string) string {
	if v := strings.TrimSpace(r.getenv(key)); v != "" {
		return v
	}
	return def
}

func (r *reader) required(key string) string {
	v := strings.TrimSpace(r.getenv(key))
	if v == "" {
		r.errs = append(r.errs, fmt.Errorf("%s is required", key))
	}
	return v
}

func (r *reader) duration(key string, def time.Duration) time.Duration {
	v := strings.TrimSpace(r.getenv(key))
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s must be a duration like 500ms or 2s", key))
		return def
	}
	return d
}

func (r *reader) boolean(key string, def bool) bool {
	v := strings.TrimSpace(r.getenv(key))
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s must be true or false", key))
		return def
	}
	return b
}

func (r *reader) integer(key string, def int) int {
	v := strings.TrimSpace(r.getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s must be an integer", key))
		return def
	}
	return n
}
