package gateway

import (
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

func TestSelectModelMappingOptionKeepsDeclaredPriorities(t *testing.T) {
	scoreHigh, scoreMiddle, scoreLow := 96.0, 82.0, 70.0
	costHigh, costMiddle, costLow := 0.9, 0.4, 0.1
	timeHigh, timeMiddle, timeLow := 90.0, 45.0, 15.0
	options := []ModelMappingOption{
		{Model: "high-iq", Score: &scoreHigh, CostUSD: &costHigh, WallSeconds: &timeHigh},
		{Model: "middle", Score: &scoreMiddle, CostUSD: &costMiddle, WallSeconds: &timeMiddle},
		{Model: "low-cost", Score: &scoreLow, CostUSD: &costLow, WallSeconds: &timeLow},
	}

	checks := []struct {
		strategy ModelMappingStrategy
		want     string
	}{
		{ModelMappingStrategyIQ, "high-iq"},
		{ModelMappingStrategyCost, "low-cost"},
		{ModelMappingStrategyTime, "low-cost"},
		{ModelMappingStrategyBalanced, "low-cost"},
	}
	for _, check := range checks {
		got, err := SelectModelMappingOption(check.strategy, options, "", "")
		if err != nil {
			t.Fatalf("select %s: %v", check.strategy, err)
		}
		if got.Model != check.want {
			t.Fatalf("select %s = %q, want %q", check.strategy, got.Model, check.want)
		}
	}
}

func TestSelectModelMappingOptionLetsAnExplicitReasoningOverrideRadar(t *testing.T) {
	score := 96.0
	selected, err := SelectModelMappingOption(
		ModelMappingStrategyIQ,
		[]ModelMappingOption{{Model: "gpt-5.4", ReasoningEffort: "medium", Score: &score}},
		"",
		"high",
	)
	if err != nil {
		t.Fatalf("select mapped option: %v", err)
	}
	if selected.ReasoningEffort != "high" {
		t.Fatalf("reasoning effort = %q, want explicit high", selected.ReasoningEffort)
	}
}

func TestNormalizeModelMappingReasoningEffortAcceptsCodexRadarMax(t *testing.T) {
	got, err := NormalizeModelMappingReasoningEffort(storage.GatewayAppCodex, "MAX")
	if err != nil {
		t.Fatalf("normalize max reasoning effort: %v", err)
	}
	if got != "max" {
		t.Fatalf("normalized reasoning effort = %q, want max", got)
	}
}

func TestDefaultModelMappingReasoningEffortDoesNotInventClaudeBudget(t *testing.T) {
	if got := DefaultModelMappingReasoningEffort(storage.GatewayAppClaude); got != "" {
		t.Fatalf("Claude default reasoning effort = %q, want empty", got)
	}
}

func TestFilterCandidatesForMappedModelPreservesCandidateOrder(t *testing.T) {
	cooldownUntil := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)
	candidates := []Candidate{
		{Channel: storage.Channel{ID: 1, Name: "first"}, Rate: storage.RateSnapshot{ModelName: "priority"}, App: storage.GatewayAppCodex, ModelIDs: []string{"gpt-5", "gpt-5-mini"}, UpstreamGeneration: storage.UpstreamGroupGeneration{ID: 11, ChannelID: 1, CanonicalGroupName: "priority", PresenceGeneration: 3}},
		{Channel: storage.Channel{ID: 2, Name: "second"}, Rate: storage.RateSnapshot{ModelName: "fallback"}, App: storage.GatewayAppCodex, ModelIDs: []string{"gpt-4.1"}, UpstreamGeneration: storage.UpstreamGroupGeneration{ID: 12, ChannelID: 2, CanonicalGroupName: "fallback", PresenceGeneration: 4}, FailureCount: 4, CooldownUntil: &cooldownUntil},
		{Channel: storage.Channel{ID: 3, Name: "third"}, Rate: storage.RateSnapshot{ModelName: "last"}, App: storage.GatewayAppCodex, ModelIDs: nil, UpstreamGeneration: storage.UpstreamGroupGeneration{ID: 13, ChannelID: 3, CanonicalGroupName: "last", PresenceGeneration: 5}},
	}

	filtered, exclusions, err := (&Service{}).FilterCandidatesForMappedModel(candidates, "gpt-5")
	if err != nil {
		t.Fatalf("filter candidates: %v", err)
	}
	if len(filtered) != 1 || filtered[0].Channel.Name != "first" {
		t.Fatalf("filtered order = %+v", filtered)
	}
	if len(exclusions) != 2 || exclusions[0].ChannelName != "second" || exclusions[0].Reason != "model_not_listed" || exclusions[1].ChannelName != "third" || exclusions[1].Reason != "model_capability_unknown" {
		t.Fatalf("exclusions = %+v", exclusions)
	}
	if exclusions[0].UpstreamGroupID != 12 || exclusions[0].PresenceGeneration != 4 || exclusions[0].FailureCount != 4 || exclusions[0].CooldownUntil == nil || !exclusions[0].CooldownUntil.Equal(cooldownUntil) {
		t.Fatalf("mapped-model exclusion snapshot = %+v", exclusions[0])
	}
	if exclusions[1].UpstreamGroupID != 13 || exclusions[1].PresenceGeneration != 5 || exclusions[1].FailureCount != 0 || exclusions[1].CooldownUntil != nil {
		t.Fatalf("capability-unknown snapshot = %+v", exclusions[1])
	}
}

func TestModelMappingFamilyUsesExactSynchronizedFamilies(t *testing.T) {
	if !ModelMappingFamilySupported([]string{"codex", "grok"}, "codex") {
		t.Fatal("codex synchronized family was rejected")
	}
	if ModelMappingFamilySupported([]string{"grok"}, "codex") {
		t.Fatal("OpenAI-compatible Grok family was accepted as Codex radar")
	}
	if ModelMappingFamilySupported([]string{"claude"}, "codex") {
		t.Fatal("Claude family was accepted as Codex radar")
	}
	if got := ModelMappingFamilies([]string{"grok", "claude", "codex", "claude"}); len(got) != 2 || got[0] != "codex" || got[1] != "claude" {
		t.Fatalf("mapping families = %#v", got)
	}
}

func TestClaudeThinkingBudget(t *testing.T) {
	if budget, ok := ClaudeThinkingBudget("2048"); !ok || budget != 2048 {
		t.Fatalf("budget = %d, %v", budget, ok)
	}
	for _, value := range []string{"", "0", "high", "-1"} {
		if _, ok := ClaudeThinkingBudget(value); ok {
			t.Fatalf("invalid budget %q was accepted", value)
		}
	}
}
