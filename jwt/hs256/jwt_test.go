package hs256_test

import (
	"testing"
	"time"

	jwt2 "github.com/golang-jwt/jwt/v5"
	"github.com/sergeyslonimsky/core/jwt/hs256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestClaims struct {
	jwt2.RegisteredClaims

	TestKey string `json:"testKey"`
}

func TestService_CreateHS256Token(t *testing.T) {
	t.Parallel()

	secret := []byte("testSecret")

	token, err := hs256.CreateToken[TestClaims](secret, TestClaims{
		RegisteredClaims: jwt2.RegisteredClaims{
			Issuer:    "testService",
			ExpiresAt: jwt2.NewNumericDate(time.Now().Add(10 * time.Second)),
		},
		TestKey: "testValue",
	})

	require.NoError(t, err)
	require.NotEmpty(t, token)

	claims, err := hs256.ParseToken[TestClaims](token, secret)
	require.NoError(t, err)

	assert.Equal(t, "testService", claims.Issuer)
	assert.Equal(t, "testValue", claims.TestKey)

	assert.WithinDuration(t, time.Now().Add(10*time.Second), claims.ExpiresAt.Time, 1*time.Second)
}
