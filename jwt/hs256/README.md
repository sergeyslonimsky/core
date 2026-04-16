# jwt/hs256

HMAC-SHA256 implementation of `core/jwt.Signer` and `core/jwt.Verifier`. Use with `core/jwt.AuthService` for typed-payload tokens, or via the standalone `CreateToken` / `ParseToken` helpers.

See [`core/jwt`](../README.md) for the framework overview and `Signer`/`Verifier` interfaces.

## Quickstart

```go
import (
    "github.com/sergeyslonimsky/core/jwt"
    "github.com/sergeyslonimsky/core/jwt/hs256"
)

signer   := hs256.NewSigner([]byte("super-secret"))
verifier := hs256.NewVerifier([]byte("super-secret"))

svc := jwt.NewAuthService[MyAuthInfo](signer, verifier, "myservice", "user", time.Hour)
```

## Standalone helpers

For callers that don't use `AuthService`:

```go
token, err := hs256.CreateToken(secret, claims)
claims, err := hs256.ParseToken[MyClaims](token, secret)
```

## Errors

| Error | Cause |
|---|---|
| `ErrInvalidSigningMethod` | Token signed with an algorithm other than HS256. |
| `ErrInvalidClaimsType` | Generic ParseToken called with a non-pointer claims type. |
| `ErrInvalidToken` | Token failed validation (expired, signature mismatch, etc.). |

## Algorithm details

- `HS256` (HMAC-SHA256): symmetric. The same secret is used to sign and verify.
- Suitable for service-to-service auth where both ends share a secret. Not appropriate where the verifier should not be able to mint tokens — use RSA / EdDSA in that case (planned, see [`core/jwt`](../README.md)).
