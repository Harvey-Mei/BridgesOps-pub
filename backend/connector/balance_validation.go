package connector

import (
	"errors"
	"fmt"
	"math"
)

// BalanceValidationError identifies a balance field that cannot be persisted
// safely. Finite signed balances are valid upstream account observations.
type BalanceValidationError struct {
	Field string
	Value float64
	// Negative is retained for compatibility with older callers. Current
	// validation accepts every finite signed balance and never sets it.
	Negative bool
}

func (e *BalanceValidationError) Error() string {
	if e == nil {
		return "upstream balance is invalid"
	}
	return fmt.Sprintf("upstream %s is invalid", e.Field)
}

func IsNegativeBalanceValidationError(err error) bool {
	var validationErr *BalanceValidationError
	return errors.As(err, &validationErr) && validationErr.Negative
}

// ValidateBalanceResult rejects values that cannot safely become a channel
// balance observation. A negative upstream balance is valid account state, not
// a reason to retain a stale positive snapshot.
func ValidateBalanceResult(result *BalanceResult) error {
	if result == nil {
		return errors.New("upstream balance result is empty")
	}
	if !finiteBalanceNumber(result.RawBalance) {
		return &BalanceValidationError{Field: "raw balance", Value: result.RawBalance}
	}
	if !finiteBalanceNumber(result.Balance) {
		return &BalanceValidationError{Field: "balance", Value: result.Balance}
	}
	if result.AppliedFactor < 0 || !finiteBalanceNumber(result.AppliedFactor) {
		return &BalanceValidationError{Field: "balance factor", Value: result.AppliedFactor}
	}
	return nil
}

func finiteBalanceNumber(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}
