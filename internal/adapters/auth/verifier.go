// Package auth valida os tokens de acesso emitidos pelo IdP (Keycloak) e os
// converte na identidade usada pelos casos de uso (app.Caller).
package auth

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/coreos/go-oidc/v3/oidc"

	"github.com/leal-k/testeTecnico/internal/app"
)

const (
	RoleProvider = "provider"
	RoleInternal = "internal"
)

// ErrInvalidToken: token ausente, malformado, com assinatura inválida, expirado
// ou emitido para outro emissor ou audiência. Vira HTTP 401.
var ErrInvalidToken = errors.New("auth: invalid token")

// Config descreve o IdP confiável.
type Config struct {
	// IssuerURL é o valor exato esperado no claim "iss"
	// (ex.: http://localhost:8080/realms/testetecnico).
	IssuerURL string
	// JWKSURL é onde ficam as chaves públicas de assinatura. Pode usar outro
	// host que o emissor (ex.: o nome do serviço dentro do Docker Compose).
	JWKSURL string
	// Audience é o valor exigido no claim "aud" (ex.: testetecnico-api).
	Audience string
}

func (c Config) validate() error {
	var missing []string
	if c.IssuerURL == "" {
		missing = append(missing, "issuer URL")
	}
	if c.JWKSURL == "" {
		missing = append(missing, "JWKS URL")
	}
	if c.Audience == "" {
		missing = append(missing, "audience")
	}
	if len(missing) > 0 {
		return fmt.Errorf("auth: missing configuration: %s", strings.Join(missing, ", "))
	}
	return nil
}

// Verifier valida tokens localmente com as chaves públicas do IdP, que ficam
// em cache e são renovadas quando aparece um "kid" desconhecido (rotação).
type Verifier struct {
	verifier *oidc.IDTokenVerifier
}

// NewVerifier cria o validador. ctx deve durar enquanto o Verifier for usado:
// ele é usado para buscar as chaves do IdP.
func NewVerifier(ctx context.Context, cfg Config) (*Verifier, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	keySet := oidc.NewRemoteKeySet(ctx, cfg.JWKSURL)
	v := oidc.NewVerifier(cfg.IssuerURL, keySet, &oidc.Config{
		ClientID:             cfg.Audience,
		SupportedSigningAlgs: []string{oidc.RS256},
	})
	return &Verifier{verifier: v}, nil
}

type claims struct {
	Type        string   `json:"typ"`
	ProviderID  string   `json:"providerId"`
	Roles       []string `json:"roles"`
	RealmAccess struct {
		Roles []string `json:"roles"`
	} `json:"realm_access"`
}

// Authenticate valida o token e devolve a identidade do chamador.
//
//   - token inválido (assinatura, emissor, audiência, expiração, algoritmo):
//     ErrInvalidToken;
//   - token válido sem um papel reconhecido, ou com papéis ambíguos:
//     app.ErrForbidden.
func (v *Verifier) Authenticate(ctx context.Context, rawToken string) (app.Caller, error) {
	tok, err := v.verifier.Verify(ctx, rawToken)
	if err != nil {
		return app.Caller{}, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}
	var c claims
	if err := tok.Claims(&c); err != nil {
		return app.Caller{}, fmt.Errorf("%w: reading claims: %v", ErrInvalidToken, err)
	}
	// Só tokens de acesso: um ID token (typ "ID") não autoriza chamadas.
	if c.Type != "" && !strings.EqualFold(c.Type, "Bearer") {
		return app.Caller{}, fmt.Errorf("%w: unexpected token type %q", ErrInvalidToken, c.Type)
	}
	return callerFrom(c)
}

func callerFrom(c claims) (app.Caller, error) {
	roles := append(slices.Clone(c.Roles), c.RealmAccess.Roles...)
	isInternal := slices.Contains(roles, RoleInternal)
	isProvider := slices.Contains(roles, RoleProvider)

	switch {
	case isInternal && isProvider:
		return app.Caller{}, fmt.Errorf("%w: token carries both provider and internal roles", app.ErrForbidden)
	case isInternal:
		return app.Caller{Internal: true}, nil
	case isProvider:
		if c.ProviderID == "" {
			return app.Caller{}, fmt.Errorf("%w: provider token without providerId", app.ErrForbidden)
		}
		return app.Caller{ProviderID: c.ProviderID}, nil
	default:
		return app.Caller{}, fmt.Errorf("%w: token has no recognized role", app.ErrForbidden)
	}
}
