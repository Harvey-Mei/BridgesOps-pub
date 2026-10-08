package connector

import (
	"math"
	"testing"
)

func TestRechargeCostFactor(t *testing.T) {
	tests := []struct {
		name       string
		multiplier *float64
		mode       string
		want       float64
	}{
		{name: "default", want: 1},
		{name: "multiply", multiplier: float64Ptr(2.5), mode: RechargeMultiplierModeMultiply, want: 2.5},
		{name: "divide", multiplier: float64Ptr(2.5), mode: RechargeMultiplierModeDivide, want: 0.4},
		{name: "unknown mode keeps legacy divide", multiplier: float64Ptr(4), mode: "unknown", want: 0.25},
		{name: "zero", multiplier: float64Ptr(0), mode: RechargeMultiplierModeDivide, want: 1},
		{name: "negative", multiplier: float64Ptr(-2), mode: RechargeMultiplierModeMultiply, want: 1},
		{name: "nan", multiplier: float64Ptr(math.NaN()), mode: RechargeMultiplierModeMultiply, want: 1},
		{name: "infinity", multiplier: float64Ptr(math.Inf(1)), mode: RechargeMultiplierModeMultiply, want: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := RechargeCostFactor(tt.multiplier, tt.mode); got != tt.want {
				t.Fatalf("RechargeCostFactor() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestApplyRechargeMultiplierUsesFactorOnce(t *testing.T) {
	multiplier := 2.0
	factor := RechargeCostFactor(&multiplier, RechargeMultiplierModeDivide)
	raw := 14.4

	got := ApplyRechargeMultiplier(raw, &multiplier, RechargeMultiplierModeDivide)
	if got != round4(raw*factor) {
		t.Fatalf("ApplyRechargeMultiplier() = %v, want raw*factor %v", got, round4(raw*factor))
	}
	if got == round4(raw*factor*factor) {
		t.Fatalf("ApplyRechargeMultiplier() applied factor twice: %v", got)
	}
	if got := ApplyRechargeMultiplier(1.234567, nil, RechargeMultiplierModeDivide); got != 1.234567 {
		t.Fatalf("missing multiplier changed compatibility value: %v", got)
	}
}

func float64Ptr(value float64) *float64 {
	return &value
}
