package uhpharness

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"
)

// Principal is the authenticated caller. Sessions, responses and interactions
// belong to the principal that created them; another principal gets 404.
type Principal struct {
	Subject string
}

// Authenticator resolves the caller from the request. Return ErrMissingCredential
// when no credential was presented (401 missing_credential); any other error is
// 401 invalid_credential. Credentials are read from headers only.
type Authenticator interface {
	Authenticate(r *http.Request) (Principal, error)
}

// AuthenticatorFunc adapts a function to Authenticator.
type AuthenticatorFunc func(r *http.Request) (Principal, error)

// Authenticate calls f.
func (f AuthenticatorFunc) Authenticate(r *http.Request) (Principal, error) { return f(r) }

// ErrMissingCredential reports a request without a credential.
var ErrMissingCredential = errors.New("uhpharness: missing credential")

var errInvalidCredential = errors.New("uhpharness: invalid credential")

// BearerToken accepts one bearer credential, the harness access token the
// platform issued when this harness was registered, and maps it to subject. Only
// the token's SHA-256 is kept; comparison is constant time.
func BearerToken(token, subject string) Authenticator {
	want := sha256.Sum256([]byte(token))
	return AuthenticatorFunc(func(r *http.Request) (Principal, error) {
		raw, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || raw == "" {
			return Principal{}, ErrMissingCredential
		}
		got := sha256.Sum256([]byte(raw))
		if subtle.ConstantTimeCompare(got[:], want[:]) != 1 {
			return Principal{}, errInvalidCredential
		}
		return Principal{Subject: subject}, nil
	})
}
