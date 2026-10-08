package connector

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
)

// IsAccountBalanceFailure recognizes an explicit account-balance rejection on
// an otherwise authentication-shaped response. A bare 401/403, a truncated
// JSON body, or a generic per-key quota error is not enough evidence.
func IsAccountBalanceFailure(status int, raw []byte) bool {
	if status != http.StatusUnauthorized && status != http.StatusForbidden {
		return false
	}
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return false
	}

	var payload any
	if json.Unmarshal(raw, &payload) != nil {
		return false
	}
	return accountBalanceFailureValue(payload)
}

func accountBalanceFailureValue(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			normalizedKey := normalizeBalanceFailureIdentifier(key)
			if text, ok := child.(string); ok {
				switch normalizedKey {
				case "code", "type", "error_code", "error_type", "reason":
					if accountBalanceFailureIdentifier(text) {
						return true
					}
				case "message", "detail", "error_description":
					if accountBalanceFailureMessage(text) {
						return true
					}
				case "error":
					if accountBalanceFailureIdentifier(text) || accountBalanceFailureMessage(text) {
						return true
					}
				}
			}
			if accountBalanceFailureValue(child) {
				return true
			}
		}
	case []any:
		for _, child := range typed {
			if accountBalanceFailureValue(child) {
				return true
			}
		}
	}
	return false
}

func accountBalanceFailureIdentifier(value string) bool {
	switch normalizeBalanceFailureIdentifier(value) {
	case "insufficient_balance", "insufficient_account_balance", "account_balance_insufficient", "balance_insufficient":
		return true
	default:
		return false
	}
}

func normalizeBalanceFailureIdentifier(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	return strings.NewReplacer("-", "_", ".", "_", " ", "_").Replace(value)
}

func accountBalanceFailureMessage(value string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	for _, marker := range []string{
		"insufficient account balance",
		"insufficient balance",
		"account balance is insufficient",
		"account has insufficient balance",
	} {
		if strings.Contains(value, marker) {
			return true
		}
	}
	return false
}
