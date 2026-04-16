# jwt

Generic JWT issuer/verifier with pluggable signing algorithms. The root `jwt` package defines `Signer`/`Verifier` interfaces and `AuthService[T]` for typed payloads; algorithm-specific subpackages (`jwt/hs256` today; RS256/EdDSA/JWKS planned) implement the interfaces.

## When to use

Any service issuing or validating JWTs for authentication, service-to-service auth, or download links.

## Quickstart

```go
package main

import (
    "log"
    "time"

    "github.com/sergeyslonimsky/core/jwt"
    "github.com/sergeyslonimsky/core/jwt/hs256"
)

type AuthInfo struct {
    UserID int    `json:"user_id"`
    Role   string `json:"role"`
}

func main() {
    signer := hs256.NewSigner([]byte("super-secret"))
    verifier := hs256.NewVerifier([]byte("super-secret"))

    svc := jwt.NewAuthService[AuthInfo](
        signer, verifier,
        "myservice", "user", time.Hour,
    )

    token, err := svc.CreateToken(AuthInfo{UserID: 42, Role: "admin"})
    if err != nil { log.Fatal(err) }

    info, err := svc.ParseToken(token)
    if err != nil { log.Fatal(err) }
    log.Printf("user %d (%s)", info.UserID, info.Role)
}
```

## API

```go
// core/jwt
type Signer interface {
    Sign(claims jwt.Claims) (string, error)
}
type Verifier interface {
    Verify(tokenString string, dst jwt.Claims) error
}

type AuthClaims[T any] struct {
    jwt.RegisteredClaims
    AuthInfo T `json:"authInfo"`
}

type AuthService[T any] struct { /* opaque */ }

func NewAuthService[T any](signer Signer, verifier Verifier, serviceName, claimSubject string, ttl time.Duration) *AuthService[T]
func (s *AuthService[T]) CreateToken(authInfo T) (string, error)
func (s *AuthService[T]) ParseToken(tokenString string) (T, error)
```

```go
// core/jwt/hs256
func NewSigner(secret []byte) *Signer
func NewVerifier(secret []byte) *Verifier

// Legacy generic helpers (kept for non-AuthService use)
func CreateToken[T jwt.Claims](secret []byte, claims T) (string, error)
func ParseToken[T jwt.Claims](tokenString string, secret []byte) (T, error)
```

## Algorithm support

| Algorithm | Status |
|---|---|
| HS256 (HMAC-SHA256) | ✅ `jwt/hs256` |
| RS256 (RSA-SHA256) | planned (`jwt/rs256`) |
| EdDSA | planned (`jwt/eddsa`) |
| JWKS-backed multi-key verifier | planned (`jwt/jwks`) |

New algorithms drop in by implementing `jwt.Signer` and/or `jwt.Verifier`. AuthService and downstream code are unchanged.

## Lifecycle

JWT is stateless — no `Resource`, `Runner`, or `Healthchecker`. Signers and verifiers are plain values with no background work; nothing to register with `app.App`.

## Testing

For unit tests, construct a Signer + Verifier with a known secret and round-trip tokens. See `hs256/jwt_test.go` for the pattern.

## See also

- [`golang-jwt/jwt/v5`](https://pkg.go.dev/github.com/golang-jwt/jwt/v5) — underlying library.
