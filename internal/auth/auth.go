package auth

import (
	"context"
	"errors"
	"slices"
	"strings"

	"github.com/coreos/go-oidc/v3/oidc"
)

const (
	RoleProvider    = "provider"
	RoleWalletAdmin = "wallet-admin"
)

var (
	ErrUnauthenticated = errors.New("unauthenticated")
	ErrForbidden       = errors.New("forbidden")
	ErrAuthUnavailable = errors.New("identity provider unavailable")
)

// Principal is the authenticated caller derived from a verified token.
type Principal struct {
	Subject    string
	ProviderID string
	Roles      []string
}

func (p Principal) HasRole(role string) bool { return slices.Contains(p.Roles, role) }

func (p Principal) IsProvider() bool { return p.HasRole(RoleProvider) && p.ProviderID != "" }

// AuthorizeProvider allows a provider principal to act only for its own providerId.
func (p Principal) AuthorizeProvider(providerID string) error {
	if !p.IsProvider() || p.ProviderID != providerID {
		return ErrForbidden
	}
	return nil
}

func (p Principal) AuthorizeWalletAdmin() error {
	if !p.HasRole(RoleWalletAdmin) {
		return ErrForbidden
	}
	return nil
}

// CanReadProviderResource lets a provider see only its own data; the internal service sees everything.
func (p Principal) CanReadProviderResource(providerID string) bool {
	return p.HasRole(RoleWalletAdmin) || (p.IsProvider() && p.ProviderID == providerID)
}

type ctxKey struct{}

func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, ctxKey{}, p)
}

func FromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(ctxKey{}).(Principal)
	return p, ok
}

type claims struct {
	Subject    string   `json:"sub"`
	ProviderID string   `json:"provider_id"`
	Roles      []string `json:"roles"`
}

// Verifier validates bearer tokens against an OIDC issuer: signature through the JWKS, plus iss, aud and exp.
type Verifier struct {
	verifier *oidc.IDTokenVerifier
}

func NewVerifier(ctx context.Context, issuer, jwksURL, audience string) *Verifier {
	if jwksURL == "" {
		jwksURL = strings.TrimRight(issuer, "/") + "/protocol/openid-connect/certs"
	}
	keys := oidc.NewRemoteKeySet(ctx, jwksURL)
	return &Verifier{verifier: oidc.NewVerifier(issuer, keys, &oidc.Config{
		ClientID:             audience,
		SupportedSigningAlgs: []string{oidc.RS256},
	})}
}

func (v *Verifier) Verify(ctx context.Context, rawToken string) (Principal, error) {
	token, err := v.verifier.Verify(ctx, rawToken)
	if err != nil {
		if isKeyFetchFailure(err) {
			return Principal{}, ErrAuthUnavailable
		}
		return Principal{}, ErrUnauthenticated
	}
	var c claims
	if err := token.Claims(&c); err != nil {
		return Principal{}, ErrUnauthenticated
	}
	return Principal{Subject: token.Subject, ProviderID: c.ProviderID, Roles: c.Roles}, nil
}

// BearerToken extracts the token from an Authorization header value.
func BearerToken(header string) (string, bool) {
	scheme, token, ok := strings.Cut(header, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || strings.TrimSpace(token) == "" {
		return "", false
	}
	return strings.TrimSpace(token), true
}

// isKeyFetchFailure separates an unreachable JWKS endpoint from a bad token.
func isKeyFetchFailure(err error) bool {
	return strings.Contains(err.Error(), "fetching keys") || errors.Is(err, context.DeadlineExceeded)
}
