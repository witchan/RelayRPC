package auth

import (
	"context"
	"net/http"
	"strings"
)

type ctxKey struct{}

func ContextToken(ctx context.Context) string {
	v, _ := ctx.Value(ctxKey{}).(string)
	return v
}

type TokenStore struct {
	tokens map[string]bool
}

func NewTokenStore(tokens []string) *TokenStore {
	m := make(map[string]bool, len(tokens))
	for _, t := range tokens {
		m[t] = true
	}
	return &TokenStore{tokens: m}
}

func (ts *TokenStore) Valid(token string) bool {
	return ts.tokens[token]
}

func (ts *TokenStore) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := r.Header.Get("Authorization")
		if header == "" {
			http.Error(w, `{"error":{"code":"MISSING_TOKEN","message":"authorization header required"}}`, http.StatusUnauthorized)
			return
		}

		token := strings.TrimPrefix(header, "Bearer ")
		if token == header || token == "" {
			http.Error(w, `{"error":{"code":"INVALID_TOKEN","message":"bearer token required"}}`, http.StatusUnauthorized)
			return
		}

		if !ts.Valid(token) {
			http.Error(w, `{"error":{"code":"INVALID_TOKEN","message":"invalid token"}}`, http.StatusUnauthorized)
			return
		}

		ctx := context.WithValue(r.Context(), ctxKey{}, token)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
