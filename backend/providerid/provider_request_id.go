// Package providerid defines the bounded, byte-exact contract for upstream
// provider request IDs used by dispatch auditing and cost reconciliation.
package providerid

import (
	"errors"
	"fmt"
	"unicode/utf8"
)

const MaxBytes = 512

var ErrInvalid = errors.New("provider request ID is invalid")

// Normalize preserves the provider-issued identifier byte-for-byte. Overlong
// and malformed identifiers are rejected; they must never be truncated or
// normalized into a different exact-match key.
func Normalize(value string) (string, error) {
	if !utf8.ValidString(value) {
		return "", fmt.Errorf("%w: must be valid UTF-8", ErrInvalid)
	}
	if len(value) > MaxBytes {
		return "", fmt.Errorf("%w: exceeds %d bytes", ErrInvalid, MaxBytes)
	}
	return value, nil
}
