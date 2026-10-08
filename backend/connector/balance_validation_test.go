package connector

import (
	"math"
	"testing"
)

func TestValidateBalanceResult(t *testing.T) {
	tests := []struct {
		name   string
		result *BalanceResult
		wantOK bool
	}{
		{name: "valid", result: &BalanceResult{RawBalance: 1, Balance: 1, AppliedFactor: 1}, wantOK: true},
		{name: "negative raw and actual", result: &BalanceResult{RawBalance: -1, Balance: -1, AppliedFactor: 1}, wantOK: true},
		{name: "negative actual", result: &BalanceResult{RawBalance: 1, Balance: -1, AppliedFactor: 1}, wantOK: true},
		{name: "invalid factor", result: &BalanceResult{RawBalance: 1, Balance: 1, AppliedFactor: math.Inf(1)}},
		{name: "not a number", result: &BalanceResult{RawBalance: math.NaN(), Balance: 1, AppliedFactor: 1}},
		{name: "actual not a number", result: &BalanceResult{RawBalance: 1, Balance: math.NaN(), AppliedFactor: 1}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateBalanceResult(tc.result); (err == nil) != tc.wantOK {
				t.Fatalf("ValidateBalanceResult(%+v) = %v, want success=%t", tc.result, err, tc.wantOK)
			}
		})
	}
}
