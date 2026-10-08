package connector

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// ErrAPIOriginMismatch marks a response that proves the configured endpoint
// is not speaking the connector's API protocol (for example, a web frontend
// returning an HTML document). Callers can use this stable classification to
// avoid retrying a permanent configuration/type mismatch.
var ErrAPIOriginMismatch = errors.New("upstream API origin or response protocol mismatch")

// ErrUpstreamChallenge is returned after a connector has converted an
// anti-bot response into an actionable provider-level diagnostic. It keeps
// retry classification intact even when the original HTTP response is no
// longer exposed to the caller.
var ErrUpstreamChallenge = errors.New("upstream anti-bot challenge")

func IsAPIOriginMismatch(err error) bool {
	return errors.Is(err, ErrAPIOriginMismatch)
}

const maxHTTPErrorSummaryRunes = 300

type HTTPError struct {
	StatusCode int
	Summary    string
	body       []byte
}

func (e *HTTPError) Error() string {
	if e.Summary == "" {
		return fmt.Sprintf("status %d", e.StatusCode)
	}
	return fmt.Sprintf("status %d: %s", e.StatusCode, e.Summary)
}

func HTTPStatusError(statusCode int, body []byte) error {
	return &HTTPError{StatusCode: statusCode, Summary: summarizeHTTPErrorBody(body), body: bytes.Clone(body)}
}

func IsAuthHTTPError(err error) bool {
	var httpErr *HTTPError
	return errors.As(err, &httpErr) && (httpErr.StatusCode == 401 || httpErr.StatusCode == 403)
}

// IsUpstreamChallengeHTTPError distinguishes a provider anti-bot challenge
// from an invalid credential. Cloudflare commonly returns a 403 HTML page
// titled "Just a moment..." before the request reaches the provider API.
// Treating that response as a revoked token would unnecessarily rotate or
// quarantine valid credentials.
func IsUpstreamChallengeHTTPError(err error) bool {
	if errors.Is(err, ErrUpstreamChallenge) {
		return true
	}
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) {
		return false
	}
	return IsUpstreamChallengeResponse(httpErr.StatusCode, httpErr.body)
}

// IsUpstreamChallengeResponse is the raw-response variant used by gateway
// paths that classify an upstream response before wrapping it as HTTPError.
func IsUpstreamChallengeResponse(status int, body []byte) bool {
	if status != http.StatusForbidden {
		return false
	}
	text := strings.ToLower(strings.TrimSpace(string(body)))
	if text == "" {
		return false
	}
	for _, marker := range []string{
		"cf-mitigated",
		"challenge-platform",
		"cf_chl_opt",
		"cloudflare ray id",
	} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	// Cloudflare's challenge document is commonly titled "Just a moment...".
	// Require an HTML document as well so a provider's ordinary JSON message
	// containing that phrase is not treated as an anti-bot challenge.
	if strings.Contains(text, "just a moment") &&
		(strings.Contains(text, "<html") || strings.Contains(text, "<title")) {
		return true
	}
	return false
}

// IsAccountBalanceHTTPError distinguishes an explicit account-balance
// rejection from an authentication-shaped HTTP response. Callers use it to
// avoid deleting a still-valid managed key.
func IsAccountBalanceHTTPError(err error) bool {
	var httpErr *HTTPError
	return errors.As(err, &httpErr) && IsAccountBalanceFailure(httpErr.StatusCode, httpErr.body)
}

func summarizeHTTPErrorBody(body []byte) string {
	text := strings.TrimSpace(string(body))
	if text == "" {
		return ""
	}

	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err == nil {
		for _, key := range []string{"message", "error", "detail", "msg"} {
			if value, ok := payload[key]; ok {
				msg := strings.TrimSpace(fmt.Sprint(value))
				if msg != "" {
					return truncateHTTPErrorSummary(collapseHTTPErrorWhitespace(msg))
				}
			}
		}
	}

	if title := htmlTagText(text, "title"); title != "" {
		return truncateHTTPErrorSummary(collapseHTTPErrorWhitespace(title))
	}
	if h1 := htmlTagText(text, "h1"); h1 != "" {
		return truncateHTTPErrorSummary(collapseHTTPErrorWhitespace(h1))
	}

	return truncateHTTPErrorSummary(collapseHTTPErrorWhitespace(text))
}

func htmlTagText(text string, tag string) string {
	lower := strings.ToLower(text)
	open := "<" + tag
	start := strings.Index(lower, open)
	if start < 0 {
		return ""
	}
	gt := strings.Index(lower[start:], ">")
	if gt < 0 {
		return ""
	}
	contentStart := start + gt + 1
	closeTag := "</" + tag + ">"
	end := strings.Index(lower[contentStart:], closeTag)
	if end < 0 {
		return ""
	}
	return strings.TrimSpace(text[contentStart : contentStart+end])
}

func collapseHTTPErrorWhitespace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func truncateHTTPErrorSummary(s string) string {
	runes := []rune(s)
	if len(runes) <= maxHTTPErrorSummaryRunes {
		return s
	}
	return string(runes[:maxHTTPErrorSummaryRunes]) + "..."
}
