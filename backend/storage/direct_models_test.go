package storage

import (
	"strings"
	"testing"
)

func TestParseDirectModelsNormalizesAndDeduplicates(t *testing.T) {
	models, err := ParseDirectModels(`[" gpt-4o ","gpt-4o","", "claude-3-5-sonnet"]`)
	if err != nil {
		t.Fatalf("ParseDirectModels: %v", err)
	}
	want := []string{"gpt-4o", "claude-3-5-sonnet"}
	if len(models) != len(want) {
		t.Fatalf("models = %#v, want %#v", models, want)
	}
	for i := range want {
		if models[i] != want[i] {
			t.Fatalf("models = %#v, want %#v", models, want)
		}
	}
}

func TestParseDirectModelsRejectsInvalidInput(t *testing.T) {
	for _, raw := range []string{"", `null`, `{"model":"gpt-4o"}`, `[]`, `["   "]`, `[1]`} {
		if _, err := ParseDirectModels(raw); err == nil {
			t.Errorf("ParseDirectModels(%q) accepted invalid input", raw)
		}
	}
}

func TestParseDirectModelsRejectsOversizedModel(t *testing.T) {
	if _, err := ParseDirectModels(`["` + strings.Repeat("x", maxDirectModelIDLen+1) + `"]`); err == nil {
		t.Fatal("ParseDirectModels accepted an oversized model id")
	}
}

func TestParseDirectModelsRejectsCommaThatCannotRoundTripThroughModelFacts(t *testing.T) {
	if _, err := ParseDirectModels(`["vendor,model"]`); err == nil {
		t.Fatal("comma-containing direct model ID was accepted")
	}
}
