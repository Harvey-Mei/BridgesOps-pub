package netproxy

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"unicode/utf8"
)

const MaxNetworkProxyNameBytes = 128

var (
	diagnosticURLSensitivePart = regexp.MustCompile(`(?i)\b([a-z][a-z0-9+.-]*://[^\s?#]+)[?#][^\s]+`)
	diagnosticURLUserInfo      = regexp.MustCompile(`(?i)\b([a-z][a-z0-9+.-]*://)[^/\s@]+@`)
	diagnosticSecretValue      = regexp.MustCompile(`(?i)((?:["']?(?:proxy-authorization|authorization|username|password|passwd|token|secret|cookie|api[_-]?key)["']?)\s*[:=]\s*)(?:bearer\s+|basic\s+)?(?:["'][^"']*["']|[^\s,;}&\]]+)`)
	displayNameSensitiveValue  = regexp.MustCompile(`(?i)(?:://|[@?#]|(?:^|[^[:alnum:]_])(?:username|user|password|passwd|token|secret|cookie|authorization|api[_-]?key)\s*[:=])`)
)

// ValidateNetworkProxyName keeps manual labels within the schema and API
// boundary. Imported display labels use SafeDisplayName instead.
func ValidateNetworkProxyName(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", errors.New("proxy name is required")
	}
	if IsSensitiveDisplayName(value) {
		return "", errors.New("proxy name must not contain URI or credential data")
	}
	if len(value) > MaxNetworkProxyNameBytes {
		return "", fmt.Errorf("proxy name exceeds %d bytes", MaxNetworkProxyNameBytes)
	}
	return value, nil
}

// IsSensitiveDisplayName reports whether a display label can carry a proxy URI
// or credential-like data. It is shared by validation and legacy persistence
// cleanup so either path applies the same boundary.
func IsSensitiveDisplayName(value string) bool {
	value = strings.TrimSpace(value)
	return value != "" && displayNameContainsSensitiveValue(value)
}

// SafeDisplayName accepts a human-facing import label only when it cannot be
// mistaken for a URI or credential-bearing value. The endpoint fallback is
// derived from already-normalized host and port fields, never from credentials.
func SafeDisplayName(value string, endpoint Endpoint) string {
	fallback := "代理"
	if normalized, err := endpoint.Normalized(); err == nil {
		fallback = endpointDisplayName(normalized)
	}
	return safeDisplayNameValue(value, fallback)
}

func safeDisplayNameValue(value, fallback string) string {
	value = strings.Join(strings.Fields(value), " ")
	if value == "" || displayNameContainsSensitiveValue(value) {
		value = fallback
	}
	value = TruncateDisplayName(value, MaxNetworkProxyNameBytes)
	if value == "" {
		return "代理"
	}
	return value
}

func displayNameContainsSensitiveValue(value string) bool {
	if displayNameSensitiveValue.MatchString(value) {
		return true
	}
	decoded, err := url.QueryUnescape(value)
	return err == nil && decoded != value && displayNameSensitiveValue.MatchString(decoded)
}

// TruncateDisplayName preserves UTF-8 validity while reserving room for an
// import collision suffix such as " (2)".
func TruncateDisplayName(value string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	if len(value) <= maxBytes {
		return value
	}
	cut := maxBytes
	for cut > 0 && !utf8.ValidString(value[:cut]) {
		cut--
	}
	return strings.TrimSpace(value[:cut])
}

// SanitizeDiagnostic keeps a compact, actionable network error while removing
// proxy credentials, authorization values, URL query tokens, and cookies.
// It is deliberately shared by API responses and persisted test diagnostics so
// a legacy row cannot re-expose a secret in an administrator dialog later.
func SanitizeDiagnostic(value string, maxBytes int) string {
	value = strings.ReplaceAll(value, "\r\n", "\n")
	value = strings.ReplaceAll(value, "\r", "\n")
	value = diagnosticURLSensitivePart.ReplaceAllString(value, "$1?[redacted]")
	value = diagnosticURLUserInfo.ReplaceAllString(value, "$1[redacted]@")
	value = diagnosticSecretValue.ReplaceAllString(value, "$1[redacted]")
	value = strings.TrimSpace(value)
	if maxBytes <= 0 || len(value) <= maxBytes {
		return value
	}
	cut := maxBytes
	for cut > 0 && !utf8.ValidString(value[:cut]) {
		cut--
	}
	return strings.TrimSpace(value[:cut]) + "…"
}
