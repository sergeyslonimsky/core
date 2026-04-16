// Package hs256 implements jwt.Signer and jwt.Verifier using the HS256
// HMAC-SHA256 signing algorithm.
//
// Use Signer/Verifier with the generic core/jwt.AuthService:
//
//	signer := hs256.NewSigner([]byte("secret"))
//	verifier := hs256.NewVerifier([]byte("secret"))
//	svc := jwt.NewAuthService[MyAuthInfo](signer, verifier, "myservice", "user", time.Hour)
//
// The package also exposes the older generic CreateToken/ParseToken
// helpers for callers that don't need AuthService.
package hs256

import (
	"errors"
	"fmt"

	"github.com/golang-jwt/jwt/v5"

	corejwt "github.com/sergeyslonimsky/core/jwt"
)

// Sentinel errors returned by ParseToken / Verifier.
var (
	ErrInvalidSigningMethod = errors.New("unexpected signing method")
	ErrInvalidClaimsType    = errors.New("invalid claims type")
	ErrInvalidToken         = errors.New("invalid token")
)

// Signer signs JWT claims using HS256 with a shared secret. Implements
// core/jwt.Signer.
type Signer struct {
	secret []byte
}

// NewSigner returns an HS256 jwt.Signer that uses the given shared secret.
func NewSigner(secret []byte) *Signer {
	return &Signer{secret: secret}
}

// Sign — see core/jwt.Signer.Sign.
func (s *Signer) Sign(claims jwt.Claims) (string, error) {
	return signHS256(s.secret, claims)
}

// Verifier validates HS256-signed JWTs against a shared secret. Implements
// core/jwt.Verifier.
type Verifier struct {
	secret []byte
}

// NewVerifier returns an HS256 jwt.Verifier that validates against the
// given shared secret.
func NewVerifier(secret []byte) *Verifier {
	return &Verifier{secret: secret}
}

// Verify — see core/jwt.Verifier.Verify.
func (v *Verifier) Verify(tokenString string, dst jwt.Claims) error {
	return parseHS256(v.secret, tokenString, dst)
}

// CreateToken is a generic helper that signs the given claims with HS256.
// Useful when AuthService's payload model doesn't fit your use case.
//
//nolint:ireturn // returning string by design
func CreateToken[T jwt.Claims](secret []byte, claims T) (string, error) {
	return signHS256(secret, claims)
}

// ParseToken is a generic helper that validates a token and returns the
// populated claims.
//
//nolint:ireturn // returning T is the point of the generic
func ParseToken[T jwt.Claims](tokenString string, secret []byte) (T, error) {
	var claims T

	claimsPtr, ok := any(&claims).(jwt.Claims)
	if !ok {
		return claims, ErrInvalidClaimsType
	}

	if err := parseHS256(secret, tokenString, claimsPtr); err != nil {
		return claims, err
	}

	return claims, nil
}

// signHS256 is the shared signing implementation used by Signer.Sign and
// the legacy CreateToken helper.
func signHS256(secret []byte, claims jwt.Claims) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)

	tokenString, err := token.SignedString(secret)
	if err != nil {
		return "", fmt.Errorf("signed string: %w", err)
	}

	return tokenString, nil
}

// parseHS256 is the shared verification implementation used by
// Verifier.Verify and the legacy ParseToken helper.
func parseHS256(secret []byte, tokenString string, claims jwt.Claims) error {
	token, err := jwt.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (any, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, ErrInvalidSigningMethod
		}

		return secret, nil
	})
	if err != nil {
		return fmt.Errorf("parse token: %w", err)
	}

	if !token.Valid {
		return ErrInvalidToken
	}

	return nil
}

// Compile-time interface assertions.
var (
	_ corejwt.Signer   = (*Signer)(nil)
	_ corejwt.Verifier = (*Verifier)(nil)
)
