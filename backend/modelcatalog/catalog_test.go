package modelcatalog

import (
	"reflect"
	"testing"
)

func TestFamiliesFromModelReportsAmbiguity(t *testing.T) {
	if got := FamiliesFromModel("claude-gpt-bridge"); !reflect.DeepEqual(got, []string{"codex", "claude"}) {
		t.Fatalf("families = %v", got)
	}
	if got := FamiliesFromModel("unknown-model"); len(got) != 0 {
		t.Fatalf("unknown families = %v", got)
	}
}

func TestCompatibleAppsUsesStructuredFamilies(t *testing.T) {
	got := CompatibleApps([]string{"grok", "claude", "other"})
	if !reflect.DeepEqual(got, []string{"codex", "claude"}) {
		t.Fatalf("apps = %v", got)
	}
	if Version() == "" || Version() != Version() {
		t.Fatal("catalog version must be stable")
	}
}
