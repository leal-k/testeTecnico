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
