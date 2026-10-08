package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestMiddlewareFallsBackToSessionCookieAfterInvalidBearer(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc, err := New("admin", "password", "test-token-secret", 0)
	if err != nil {
		t.Fatalf("create auth service: %v", err)
	}
	token, _, err := svc.Login("admin", "password")
	if err != nil {
		t.Fatalf("create session token: %v", err)
	}

	router := gin.New()
	router.Use(svc.Middleware())
	router.GET("/protected", func(c *gin.Context) {
		if subject, _ := c.Get("authSubject"); subject != "admin" {
			c.Status(http.StatusUnauthorized)
			return
		}
		c.Status(http.StatusNoContent)
	})

	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set("Authorization", "Bearer stale-legacy-token")
	req.AddCookie(&http.Cookie{Name: "uh_token", Value: token})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("cookie fallback status = %d, want %d", rec.Code, http.StatusNoContent)
	}
}
