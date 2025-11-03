package jwt

import (
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/sergeyslonimsky/core/jwt/hs256"
)

type AuthClaims[T any] struct {
	jwt.RegisteredClaims

	AuthInfo T `json:"authInfo"`
}

type AuthService[T any] struct {
	jwtSecret    string
	serviceName  string
	claimSubject string
	tokenTTL     time.Duration
}

func NewAuthService[T any](jwtSecret, serviceName, claimSubject string, tokenTTL time.Duration) *AuthService[T] {
	return &AuthService[T]{
		jwtSecret:    jwtSecret,
		serviceName:  serviceName,
		claimSubject: claimSubject,
		tokenTTL:     tokenTTL,
	}
}

func (s *AuthService[T]) CreateToken(authInfo T) (string, error) {
	claims := AuthClaims[T]{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    s.serviceName,
			Subject:   s.claimSubject,
			Audience:  jwt.ClaimStrings{s.serviceName},
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(s.tokenTTL)),
			NotBefore: jwt.NewNumericDate(time.Now()),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			ID:        uuid.New().String(),
		},
		AuthInfo: authInfo,
	}

	token, err := hs256.CreateToken([]byte(s.jwtSecret), claims)
	if err != nil {
		return "", fmt.Errorf("create token: %w", err)
	}

	return token, nil
}

func (s *AuthService[T]) ParseToken(tokenString string) (T, error) { //nolint:ireturn
	claims, err := hs256.ParseToken[AuthClaims[T]](tokenString, []byte(s.jwtSecret))
	if err != nil {
		return *new(T), fmt.Errorf("parse token: %w", err)
	}

	return claims.AuthInfo, nil
}
