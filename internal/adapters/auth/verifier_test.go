package auth

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/leal-k/testeTecnico/internal/app"
)

const (
	testIssuer   = "http://issuer.test/realms/testetecnico"
	testAudience = "testetecnico-api"
)

// fakeIdP publica um JWKS com a chave "good" e assina tokens com qualquer chave.
type fakeIdP struct {
	good *rsa.PrivateKey
	evil *rsa.PrivateKey // chave que NÃO está no JWKS
	srv  *httptest.Server
}

func newFakeIdP(t *testing.T) *fakeIdP {
	t.Helper()
	good, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	evil, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	idp := &fakeIdP{good: good, evil: evil}

	jwks := map[string]any{"keys": []map[string]string{{
		"kty": "RSA",
		"kid": "good",
		"use": "sig",
		"alg": "RS256",
		"n":   b64(good.PublicKey.N.Bytes()),
		"e":   b64(big.NewInt(int64(good.PublicKey.E)).Bytes()),
	}}}
	idp.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(jwks)
	}))
	t.Cleanup(idp.srv.Close)
	return idp
}

func (idp *fakeIdP) verifier(t *testing.T) *Verifier {
	t.Helper()
	v, err := NewVerifier(context.Background(), Config{
		IssuerURL: testIssuer,
		JWKSURL:   idp.srv.URL,
		Audience:  testAudience,
	})
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func b64(b []byte) string {
	return base64.RawURLEncoding.EncodeToString(b)
}

func baseClaims() map[string]any {
	now := time.Now()
	return map[string]any{
		"iss": testIssuer,
		"aud": testAudience,
		"sub": "service-account",
		"typ": "Bearer",
		"iat": now.Unix(),
		"exp": now.Add(5 * time.Minute).Unix(),
	}
}

func providerClaims(providerID string) map[string]any {
	c := baseClaims()
	c["providerId"] = providerID
	c["roles"] = []string{RoleProvider}
	return c
}

// sign monta um JWT RS256 "na mão", sem bibliotecas extras.
func sign(t *testing.T, key *rsa.PrivateKey, kid string, claims map[string]any) string {
	t.Helper()
	header, _ := json.Marshal(map[string]string{"alg": "RS256", "typ": "JWT", "kid": kid})
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	signingInput := b64(header) + "." + b64(payload)
	sum := sha256.Sum256([]byte(signingInput))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, sum[:])
	if err != nil {
		t.Fatal(err)
	}
	return signingInput + "." + b64(sig)
}

func unsigned(t *testing.T, alg string, claims map[string]any) string {
	t.Helper()
	header, _ := json.Marshal(map[string]string{"alg": alg, "typ": "JWT"})
	payload, _ := json.Marshal(claims)
	return b64(header) + "." + b64(payload) + "."
}

func TestAuthenticate_ValidTokens(t *testing.T) {
	idp := newFakeIdP(t)
	v := idp.verifier(t)
	ctx := context.Background()

	caller, err := v.Authenticate(ctx, sign(t, idp.good, "good", providerClaims("provider-a")))
	if err != nil || caller.Internal || caller.ProviderID != "provider-a" {
		t.Errorf("provider token = %+v, %v", caller, err)
	}

	internal := baseClaims()
	internal["realm_access"] = map[string]any{"roles": []string{RoleInternal}}
	caller, err = v.Authenticate(ctx, sign(t, idp.good, "good", internal))
	if err != nil || !caller.Internal || caller.ProviderID != "" {
		t.Errorf("internal token = %+v, %v", caller, err)
	}

	// "aud" também pode vir como lista
	multiAud := providerClaims("provider-b")
	multiAud["aud"] = []string{"account", testAudience}
	if _, err := v.Authenticate(ctx, sign(t, idp.good, "good", multiAud)); err != nil {
		t.Errorf("audience list: %v", err)
	}
}

func TestAuthenticate_InvalidTokens(t *testing.T) {
	idp := newFakeIdP(t)
	v := idp.verifier(t)

	expired := providerClaims("provider-a")
	expired["exp"] = time.Now().Add(-time.Minute).Unix()

	wrongIssuer := providerClaims("provider-a")
	wrongIssuer["iss"] = "http://evil.test/realms/testetecnico"

	wrongAudience := providerClaims("provider-a")
	wrongAudience["aud"] = "another-api"

	idToken := providerClaims("provider-a")
	idToken["typ"] = "ID"

	valid := sign(t, idp.good, "good", providerClaims("provider-a"))
	parts := strings.Split(valid, ".")
	tamperedClaims := providerClaims("provider-b") // troca o provedor sem reassinar
	tamperedPayload, _ := json.Marshal(tamperedClaims)
	tampered := parts[0] + "." + b64(tamperedPayload) + "." + parts[2]

	tests := map[string]string{
		"empty":                      "",
		"garbage":                    "not-a-jwt",
		"expired":                    sign(t, idp.good, "good", expired),
		"wrong issuer":               sign(t, idp.good, "good", wrongIssuer),
		"wrong audience":             sign(t, idp.good, "good", wrongAudience),
		"signed by unknown key":      sign(t, idp.evil, "good", providerClaims("provider-a")),
		"tampered payload":           tampered,
		"alg none":                   unsigned(t, "none", providerClaims("provider-a")),
		"HS256 not accepted":         unsigned(t, "HS256", providerClaims("provider-a")) + "c2ln",
		"ID token instead of access": sign(t, idp.good, "good", idToken),
	}
	for name, token := range tests {
		t.Run(name, func(t *testing.T) {
			caller, err := v.Authenticate(context.Background(), token)
			if !errors.Is(err, ErrInvalidToken) {
				t.Errorf("error = %v, want ErrInvalidToken", err)
			}
			if caller != (app.Caller{}) {
				t.Errorf("an invalid token must not produce a caller: %+v", caller)
			}
		})
	}
}

func TestAuthenticate_ValidTokenWithoutUsablePermissions(t *testing.T) {
	idp := newFakeIdP(t)
	v := idp.verifier(t)

	noRole := baseClaims()

	providerWithoutID := baseClaims()
	providerWithoutID["roles"] = []string{RoleProvider}

	bothRoles := providerClaims("provider-a")
	bothRoles["roles"] = []string{RoleProvider, RoleInternal}

	for name, claims := range map[string]map[string]any{
		"no role":                          noRole,
		"provider role without providerId": providerWithoutID,
		"both roles":                       bothRoles,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := v.Authenticate(context.Background(), sign(t, idp.good, "good", claims))
			if !errors.Is(err, app.ErrForbidden) {
				t.Errorf("error = %v, want app.ErrForbidden", err)
			}
		})
	}
}

func TestNewVerifier_RequiresConfiguration(t *testing.T) {
	if _, err := NewVerifier(context.Background(), Config{}); err == nil {
		t.Error("an empty configuration must be rejected")
	}
}

// TestKeycloak_RealTokens usa o Keycloak do Docker Compose, com o realm
// provisionado do projeto. Roda só quando KEYCLOAK_URL estiver definido:
//
//	KEYCLOAK_URL=http://localhost:8080 go test ./internal/adapters/auth/ -run Keycloak -v
func TestKeycloak_RealTokens(t *testing.T) {
	base := os.Getenv("KEYCLOAK_URL")
	if base == "" {
		t.Skip("set KEYCLOAK_URL (e.g. http://localhost:8080) to run against the real Keycloak")
	}
	realm := strings.TrimRight(base, "/") + "/realms/testetecnico"
	v, err := NewVerifier(context.Background(), Config{
		IssuerURL: realm,
		JWKSURL:   realm + "/protocol/openid-connect/certs",
		Audience:  testAudience,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	for clientID, want := range map[string]app.Caller{
		"provider-a":       {ProviderID: "provider-a"},
		"provider-b":       {ProviderID: "provider-b"},
		"internal-service": {Internal: true},
	} {
		token := fetchToken(t, realm, clientID, clientID+"-secret")
		got, err := v.Authenticate(ctx, token)
		if err != nil || got != want {
			t.Errorf("%s: caller = %+v, %v; want %+v", clientID, got, err, want)
		}
	}

	// Token real adulterado: troca provider-a por provider-b sem reassinar.
	token := fetchToken(t, realm, "provider-a", "provider-a-secret")
	parts := strings.Split(token, ".")
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	forged := strings.Replace(string(payload), `"provider-a"`, `"provider-b"`, -1)
	tampered := parts[0] + "." + b64([]byte(forged)) + "." + parts[2]
	if _, err := v.Authenticate(ctx, tampered); !errors.Is(err, ErrInvalidToken) {
		t.Errorf("tampered real token: error = %v, want ErrInvalidToken", err)
	}

	// Segredo errado: o próprio Keycloak recusa emitir o token.
	resp, err := http.PostForm(realm+"/protocol/openid-connect/token", url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {"provider-a"},
		"client_secret": {"wrong-secret"},
	})
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("wrong secret: Keycloak answered %d, want 401", resp.StatusCode)
	}
}

func fetchToken(t *testing.T, realm, clientID, secret string) string {
	t.Helper()
	resp, err := http.PostForm(realm+"/protocol/openid-connect/token", url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {clientID},
		"client_secret": {secret},
	})
	if err != nil {
		t.Fatalf("requesting token for %s: %v", clientID, err)
	}
	defer resp.Body.Close()
	var body struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil || body.AccessToken == "" {
		t.Fatalf("token response for %s (HTTP %d): %v", clientID, resp.StatusCode, err)
	}
	return body.AccessToken
}
