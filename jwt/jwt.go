// Package jwt provides a small framework for issuing and parsing JWT
// tokens with a generic claims payload, plus signer/verifier interfaces
// that allow swapping the signing algorithm without changing call sites.
//
// The HS256 implementation lives in core/jwt/hs256. Future algorithm
// subpackages (RS256, EdDSA, JWKS) implement the Signer/Verifier
// interfaces and are dropped in without breaking AuthService callers.
package jwt

import (
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

// Signer issues a signed JWT for the given claims. Implemented per
// algorithm in core/jwt/<algo> subpackages.
type Signer interface {
	// Sign returns the signed JWT string.
	Sign(claims jwt.Claims) (string, error)
}

// Verifier parses and validates a JWT, populating dst with the claims.
// dst must be a pointer to a jwt.Claims-compatible struct (e.g.,
// *AuthClaims[T]). Returns an error on signature mismatch, expired
// tokens, or malformed input.
type Verifier interface {
	Verify(tokenString string, dst jwt.Claims) error
}

// AuthClaims embeds the standard jwt.RegisteredClaims and adds a
// caller-defined typed payload accessible as AuthInfo.
type AuthClaims[T any] struct {
	jwt.RegisteredClaims

	AuthInfo T `json:"authInfo"`
}

// AuthService issues and parses tokens carrying a typed AuthInfo payload.
// Construct via NewAuthService with a Signer and Verifier from one of the
// algorithm subpackages.
type AuthService[T any] struct {
	signer       Signer
	verifier     Verifier
	serviceName  string
	claimSubject string
	tokenTTL     time.Duration
}

// NewAuthService builds an AuthService.
//
// signer and verifier come from the chosen algorithm subpackage, e.g.:
//
//	signer := hs256.NewSigner([]byte("secret"))
//	verifier := hs256.NewVerifier([]byte("secret"))
//	svc := jwt.NewAuthService[MyAuthInfo](signer, verifier, "my-service", "user", time.Hour)
//
// serviceName is used as the Issuer and Audience claims; claimSubject
// becomes the Subject claim; tokenTTL bounds the ExpiresAt claim.
func NewAuthService[T any](
	signer Signer,
	verifier Verifier,
	serviceName, claimSubject string,
	tokenTTL time.Duration,
) *AuthService[T] {
	return &AuthService[T]{
		signer:       signer,
		verifier:     verifier,
		serviceName:  serviceName,
		claimSubject: claimSubject,
		tokenTTL:     tokenTTL,
	}
}

// CreateToken issues a fresh signed token containing authInfo. Sets all
// standard claims (iss, sub, aud, iat, nbf, exp, jti).
func (s *AuthService[T]) CreateToken(authInfo T) (string, error) {
	now := time.Now()
	claims := AuthClaims[T]{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    s.serviceName,
			Subject:   s.claimSubject,
			Audience:  jwt.ClaimStrings{s.serviceName},
			ExpiresAt: jwt.NewNumericDate(now.Add(s.tokenTTL)),
			NotBefore: jwt.NewNumericDate(now),
			IssuedAt:  jwt.NewNumericDate(now),
			ID:        uuid.New().String(),
		},
		AuthInfo: authInfo,
	}

	token, err := s.signer.Sign(claims)
	if err != nil {
		return "", fmt.Errorf("create token: %w", err)
	}

	return token, nil
}

// ParseToken validates the given JWT and returns its AuthInfo payload.
//
//nolint:ireturn // Returning T is the whole point of the generic.
func (s *AuthService[T]) ParseToken(tokenString string) (T, error) {
	var claims AuthClaims[T]
	if err := s.verifier.Verify(tokenString, &claims); err != nil {
		var zero T

		return zero, fmt.Errorf("parse token: %w", err)
	}

	return claims.AuthInfo, nil
}
