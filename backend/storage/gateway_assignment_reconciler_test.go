package storage

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestAutomaticAssignmentRuleUpdateRequiresRuleVersion(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	target := &GatewayCandidateGroup{Name: "rule cas target"}
	if err := db.Create(target).Error; err != nil {
		t.Fatal(err)
	}
	config, err := store.AutomaticAssignmentConfig()
	if err != nil {
		t.Fatal(err)
	}
	rule := &GatewayCandidateGroupAutoRule{
		CandidateGroupID: target.ID,
		Enabled:          true,
		Priority:         10,
		ModelFamilies:    []string{"codex"},
		FamilyMatchMode:  "single_any",
	}
	state, err := store.SaveAutomaticAssignmentRule(rule, config.State.RulesetVersion)
	if err != nil {
		t.Fatal(err)
	}
	originalRuleVersion := rule.RuleVersion
	originalRulesetVersion := state.RulesetVersion

	update := *rule
	update.Priority = 20
	update.RuleVersion = 0
	if _, err := store.SaveAutomaticAssignmentRule(&update, originalRulesetVersion); !errors.Is(err, ErrGatewayExpectedVersionNeeded) {
		t.Fatalf("missing rule version error = %v", err)
	}

	var stored GatewayCandidateGroupAutoRule
	if err := db.First(&stored, rule.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.Priority != 10 || stored.RuleVersion != originalRuleVersion {
		t.Fatalf("rule changed after rejected update: %+v", stored)
	}
	config, err = store.AutomaticAssignmentConfig()
	if err != nil {
		t.Fatal(err)
	}
	if config.State.RulesetVersion != originalRulesetVersion {
		t.Fatalf("ruleset version changed after rejected update: got %d want %d", config.State.RulesetVersion, originalRulesetVersion)
	}
}

func TestAutomaticAssignmentReconcilerRunsImmediatelyAndStops(t *testing.T) {
	reconciler := NewGatewayAutomaticAssignmentReconciler(nil, 10*time.Millisecond, 17)
	var calls atomic.Int32
	var wrongLimit atomic.Bool
	reconciler.reconcileFn = func(ctx context.Context, limit int) (GatewayAutomaticAssignmentReconcileResult, error) {
		if limit != 17 {
			wrongLimit.Store(true)
		}
		calls.Add(1)
		return GatewayAutomaticAssignmentReconcileResult{}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		reconciler.Run(ctx, nil)
	}()
	waitForReconcileCalls(t, &calls, 2)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reconciler did not stop after cancellation")
	}
	stoppedAt := calls.Load()
	time.Sleep(30 * time.Millisecond)
	if calls.Load() != stoppedAt {
		t.Fatalf("reconciler ran after shutdown: before=%d after=%d", stoppedAt, calls.Load())
	}
	if wrongLimit.Load() {
		t.Fatal("configured reconciliation limit was not propagated")
	}
}

func TestAutomaticAssignmentReconcilerSkipsConcurrentRun(t *testing.T) {
	reconciler := NewGatewayAutomaticAssignmentReconciler(nil, time.Hour, 1)
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	reconciler.reconcileFn = func(context.Context, int) (GatewayAutomaticAssignmentReconcileResult, error) {
		calls.Add(1)
		close(started)
		<-release
		return GatewayAutomaticAssignmentReconcileResult{PresenceProcessed: 1}, nil
	}

	firstDone := make(chan error, 1)
	go func() {
		_, err := reconciler.ReconcileOnce(context.Background())
		firstDone <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first reconciliation did not start")
	}
	result, err := reconciler.ReconcileOnce(context.Background())
	if err != nil || !result.Skipped {
		t.Fatalf("concurrent reconciliation result=%+v err=%v", result, err)
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("reconcile function called %d times", calls.Load())
	}
}

func TestAutomaticAssignmentReconcilerPropagatesCancellation(t *testing.T) {
	reconciler := NewGatewayAutomaticAssignmentReconciler(nil, time.Hour, 1)
	started := make(chan struct{})
	reconciler.reconcileFn = func(ctx context.Context, _ int) (GatewayAutomaticAssignmentReconcileResult, error) {
		close(started)
		<-ctx.Done()
		return GatewayAutomaticAssignmentReconcileResult{}, ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := reconciler.ReconcileOnce(ctx)
		done <- err
	}()
	<-started
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation error = %v", err)
	}
}

func TestAutomaticAssignmentReconciliationDoesNotExecutePendingChange(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "reconcile without execute", Type: ChannelTypeNewAPI, SiteURL: "https://reconcile-no-execute.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	discoveredAt := time.Now().UTC().Round(time.Second)
	rates := NewRates(db)
	for _, name := range []string{"seed", "ready"} {
		if _, err := rates.Upsert(&RateSnapshot{ChannelID: channel.ID, ModelName: name, Ratio: 1, FirstSeenAt: discoveredAt, LastSeenAt: discoveredAt}); err != nil {
			t.Fatal(err)
		}
	}
	createUnsyncedTrustedAssignmentKeyFixture(t, db, channel.ID, "seed")
	target := &GatewayCandidateGroup{Name: "reconcile target"}
	if err := store.SaveCandidateGroup(target, []GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "seed"}}); err != nil {
		t.Fatal(err)
	}
	config, err := store.AutomaticAssignmentConfig()
	if err != nil {
		t.Fatal(err)
	}
	state, err := store.SaveAutomaticAssignmentRule(&GatewayCandidateGroupAutoRule{
		CandidateGroupID: target.ID,
		Enabled:          true,
		Priority:         100,
		ModelFamilies:    []string{"codex"},
		FamilyMatchMode:  "single_any",
	}, config.State.RulesetVersion)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpdateAutomaticAssignmentState(state.RulesetVersion, true, true); err != nil {
		t.Fatal(err)
	}
	syncedAt := discoveredAt.Add(time.Second)
	readyGeneration, err := store.CaptureUpstreamGroupGeneration(channel.ID, "ready")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "ready", ModelFamilies: "codex", ModelIDs: "gpt-5.4",
		ModelsSyncedAt: &syncedAt, Enabled: true, LatencyProbeEnabled: true,
		ModelFactsUpstreamGroupID: readyGeneration.ID, ModelFactsPresenceGeneration: readyGeneration.PresenceGeneration,
		ModelFactsChannelIdentityRevision: channel.UpstreamAccountIdentityRevision,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "ready", "added", 1, "managed", discoveredAt); err != nil {
		t.Fatal(err)
	}

	reconciler := NewGatewayAutomaticAssignmentReconciler(store, time.Hour, 100)
	for run := 1; run <= 2; run++ {
		result, err := reconciler.ReconcileOnce(context.Background())
		if err != nil {
			t.Fatalf("reconciliation %d: %v", run, err)
		}
		if result.FactsProcessed != 1 || result.PreviewsPersisted == 0 {
			t.Fatalf("reconciliation %d result = %+v", run, result)
		}
	}
	var identity GatewayUpstreamGroup
	if err := db.Where("channel_id = ? AND canonical_group_name = ?", channel.ID, "ready").First(&identity).Error; err != nil {
		t.Fatal(err)
	}
	var change GatewayUpstreamGroupChange
	if err := db.Where("upstream_group_id = ? AND kind = ?", identity.ID, "added").Order("id DESC").First(&change).Error; err != nil {
		t.Fatal(err)
	}
	if change.Status != "pending" || change.AssignmentState != "ready" {
		t.Fatalf("reconciliation executed or failed to preview pending change: %+v", change)
	}
	var claims int64
	if err := db.Model(&GatewayCandidateGroupMembershipClaim{}).
		Where("upstream_group_id = ? AND source_kind = ? AND active = ?", identity.ID, "auto_rule", true).
		Count(&claims).Error; err != nil {
		t.Fatal(err)
	}
	if claims != 0 {
		t.Fatalf("reconciliation created %d automatic membership claims", claims)
	}
}

func waitForReconcileCalls(t *testing.T, calls *atomic.Int32, want int32) {
	t.Helper()
	deadline := time.After(time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for calls.Load() < want {
		select {
		case <-deadline:
			t.Fatalf("reconciler calls = %d, want at least %d", calls.Load(), want)
		case <-ticker.C:
		}
	}
}
