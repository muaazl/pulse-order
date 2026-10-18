package middleware

import (
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"strings"
)

// ============================================================================
// ARCHITECTURAL TEACHING: HTTP 401 UNAUTHORIZED VS. 403 FORBIDDEN
// ============================================================================
// A frequent backend interview question is the semantic distinction between
// HTTP 401 and HTTP 403:
//   - 401 Unauthorized (Authentication Failure): "Who are you?"
//     The request lacks valid credentials (`X-API-Key` is missing or invalid).
//   - 403 Forbidden (Authorization Failure): "I know who you are, but you lack
//     permission for this resource."
//     The caller authenticated with a valid API key, but their role header
//     (`X-Admin-Role`) is not `"admin"`.

// AdminAuthGuard enforces API-Key authentication (401) and Role authorization (403).
type AdminAuthGuard struct {
	expectedAPIKey string
}

// NewAdminAuthGuard constructs an AdminAuthGuard with the configured secret key.
func NewAdminAuthGuard(expectedAPIKey string) *AdminAuthGuard {
	return &AdminAuthGuard{expectedAPIKey: expectedAPIKey}
}

// Wrap protects administrative endpoints such as `POST /api/v1/admin/stock-reset`.
func (ag *AdminAuthGuard) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		providedKey := strings.TrimSpace(r.Header.Get("X-API-Key"))
		if providedKey == "" || subtle.ConstantTimeCompare([]byte(providedKey), []byte(ag.expectedAPIKey)) != 1 {
			w.Header().Set("WWW-Authenticate", `ApiKey realm="PulseOrder Admin"`)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error":   "unauthorized",
				"message": "missing or invalid X-API-Key header (401 Authentication required)",
			})
			return
		}

		role := strings.ToLower(strings.TrimSpace(r.Header.Get("X-Admin-Role")))
		if role != "admin" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error":   "forbidden",
				"message": "valid API key, but X-Admin-Role: admin is required for this endpoint (403 Authorization failed)",
			})
			return
		}

		next.ServeHTTP(w, r)
	})
}

