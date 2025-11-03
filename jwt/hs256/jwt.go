package hs256

import (
	"errors"
	"fmt"

	"github.com/golang-jwt/jwt/v5"
)

var (
	ErrInvalidSigningMethod = errors.New("unexpected signing method")
	ErrInvalidClaimsType    = errors.New("invalid claims type")
	ErrInvalidToken         = errors.New("invalid token")
)

func CreateToken[T jwt.Claims](secret []byte, claims T) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)

	tokenString, err := token.SignedString(secret)
	if err != nil {
		return "", fmt.Errorf("signed string: %w", err)
	}

	return tokenString, nil
}

func ParseToken[T jwt.Claims](tokenString string, secret []byte) (T, error) { //nolint:ireturn
	var claims T

	// Ensure that new(T) is a valid pointer to jwt.Claims
	claimsPtr, ok := any(&claims).(jwt.Claims)
	if !ok {
		return claims, ErrInvalidClaimsType
	}

	token, err := jwt.ParseWithClaims(tokenString, claimsPtr, func(token *jwt.Token) (interface{}, error) {
		if _, ok = token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, ErrInvalidSigningMethod
		}

		return secret, nil
	})
	if err != nil {
		return claims, fmt.Errorf("parse token: %w", err)
	}

	if !token.Valid {
		return claims, ErrInvalidToken
	}

	return claims, nil
}
