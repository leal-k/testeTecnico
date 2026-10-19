package config

import (
	"strings"
	"testing"
	"time"
)

func env(values map[string]string) func(string) string {
	return func(k string) string { return values[k] }
}

func minimal() map[string]string {
	return map[string]string{
		"DATABASE_URL":    "postgres://app:app@localhost:5432/testetecnico?sslmode=disable",
		"AUTH_ISSUER_URL": "http://localhost:8080/realms/testetecnico",
		"AUTH_JWKS_URL":   "http://keycloak:8080/realms/testetecnico/protocol/openid-connect/certs",
	}
}

func TestLoad_Defaults(t *testing.T) {
	cfg, err := load(env(minimal()))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.HTTPAddr != ":8080" || cfg.AuthAudience != "testetecnico-api" ||
		cfg.ReferenceTTL != 5*time.Minute || cfg.PendingWorkerInterval != time.Second || cfg.DBMaxConns != 20 {
		t.Errorf("defaults = %+v", cfg)
	}
}

func TestLoad_Overrides(t *testing.T) {
	v := minimal()
	v["HTTP_ADDR"] = ":9999"
	v["REFERENCE_TTL"] = "30s"
	v["DB_MAX_CONNS"] = "5"
	v["LOG_LEVEL"] = "DEBUG"
	cfg, err := load(env(v))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.HTTPAddr != ":9999" || cfg.ReferenceTTL != 30*time.Second || cfg.DBMaxConns != 5 || cfg.LogLevel != "debug" {
		t.Errorf("overrides = %+v", cfg)
	}
}

func TestLoad_ReportsEveryProblemAtOnce(t *testing.T) {
	_, err := load(env(map[string]string{
		"REFERENCE_TTL": "five minutes",
		"DB_MAX_CONNS":  "many",
	}))
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, want := range []string{"DATABASE_URL", "AUTH_ISSUER_URL", "AUTH_JWKS_URL", "REFERENCE_TTL", "DB_MAX_CONNS"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %s: %v", want, err)
		}
	}
}

func TestLoad_InvalidValues(t *testing.T) {
	tests := map[string]map[string]string{
		"bad URL":             {"DATABASE_URL": "not a url"},
		"bad log level":       {"LOG_LEVEL": "loud"},
		"zero interval":       {"PENDING_WORKER_INTERVAL": "0s"},
		"backoff base > max":  {"REFERENCE_BACKOFF_BASE": "2m", "REFERENCE_BACKOFF_MAX": "1m"},
		"too few connections": {"DB_MAX_CONNS": "1"},
	}
	for name, overrides := range tests {
		t.Run(name, func(t *testing.T) {
			v := minimal()
			for k, val := range overrides {
				v[k] = val
			}
			if _, err := load(env(v)); err == nil {
				t.Errorf("expected an error for %v", overrides)
			}
		})
	}
}
