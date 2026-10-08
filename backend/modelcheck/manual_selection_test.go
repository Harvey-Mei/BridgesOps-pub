package modelcheck

import (
	"context"
	"errors"
	"github.com/Harvey-Mei/BridgeOps/backend/config"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"sync"
	"testing"
	"time"
)

type manualPelicanRunner struct {
	pelicanRunner
	manual []TargetDescriptor
}

func (r *manualPelicanRunner) ReadManualModelTargets(context.Context, TargetDescriptor) ([]TargetDescriptor, error) {
	return modelCheckTestTargets(r.manual), nil
}

func (r *manualPelicanRunner) DiscoverManualTargets(context.Context, time.Time) ([]TargetDescriptor, error) {
	return modelCheckTestTargets(r.manual), nil
}

func TestManualModelCheckKeepsExactSelectionAndDoesNotExpandSchedule(t *testing.T) {
	store := openModelCheckTestStore(t)
	auto := TargetDescriptor{ChannelID: 3, ChannelName: "Trusted", GroupName: "group-a", ModelName: "gpt-mini", App: storage.GatewayAppCodex, UpstreamAccountIdentityRevision: 1}
	manual := auto
	manual.ModelName = "gpt-manual"
	runner := &manualPelicanRunner{pelicanRunner: pelicanRunner{fakeRunner: fakeRunner{targets: []TargetDescriptor{auto}}}, manual: []TargetDescriptor{auto, manual}}
	service := New(Options{Store: store, Runner: runner, Random: zeroReader{}})
	before, err := service.SyncTargets(context.Background())
	if err != nil || len(before) != 1 {
		t.Fatalf("auto targets=%v err=%v", before, err)
	}
	cfg := config.ModelCheckConfig{ProbesPerTarget: 2, RequestBudget24h: 10, TimeoutSeconds: 30}
	choice := manual
	choice.ChannelName = "untrusted browser name"
	run, err := service.RunManualTarget(context.Background(), choice, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if run.ChannelID != 3 || run.ChannelName != "Trusted" || run.GroupName != "group-a" || run.ModelName != "gpt-manual" || run.App != storage.GatewayAppCodex || run.RequestCount != 2 {
		t.Fatalf("wrong frozen selection: %+v", run)
	}
	target, err := store.FindTarget(run.TargetID)
	if err != nil {
		t.Fatal(err)
	}
	if target.Active {
		t.Fatal("manual-only model added to automatic schedule")
	}
	after, err := service.SyncTargets(context.Background())
	if err != nil || len(after) != 1 || after[0].ID != before[0].ID {
		t.Fatalf("auto targets changed: %+v %v", after, err)
	}
	for _, mutate := range []func(*TargetDescriptor){func(v *TargetDescriptor) { v.ModelName = "gpt-missing" }, func(v *TargetDescriptor) { v.GroupName = "group-b" }, func(v *TargetDescriptor) { v.UpstreamAccountIdentityRevision++ }, func(v *TargetDescriptor) { v.App = storage.GatewayAppClaude }} {
		bad := choice
		mutate(&bad)
		if _, err := service.RunManualTarget(context.Background(), bad, cfg); err != ErrManualTargetUnavailable {
			t.Fatalf("invalid choice accepted: %v", err)
		}
	}
	if runner.probeCalls != 2 {
		t.Fatalf("invalid choices sent requests: %d", runner.probeCalls)
	}
	if err := store.SetTargetEnabled(run.TargetID, false); err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunManualTarget(context.Background(), choice, cfg); err == nil {
		t.Fatal("manual execution ignored operator disable")
	}
	if runner.probeCalls != 2 {
		t.Fatal("disabled target sent requests")
	}
}

type blockedManualRunner struct {
	manualPelicanRunner
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (r *blockedManualRunner) RunProbe(ctx context.Context, target TargetDescriptor, request ProbeRequest) (ProbeResponse, error) {
	r.once.Do(func() { close(r.started); <-r.release })
	return r.pelicanRunner.RunProbe(ctx, target, request)
}

func TestManualModelCheckPreservesOperatorAndDiscoveryUpdatesDuringRun(t *testing.T) {
	store := openModelCheckTestStore(t)
	option := TargetDescriptor{ChannelID: 3, ChannelName: "Trusted", GroupName: "manual", ModelName: "gpt-test", App: storage.GatewayAppCodex, UpstreamAccountIdentityRevision: 1}
	runner := &blockedManualRunner{manualPelicanRunner: manualPelicanRunner{manual: []TargetDescriptor{option}}, started: make(chan struct{}), release: make(chan struct{})}
	service := New(Options{Store: store, Runner: runner, Random: zeroReader{}})
	cfg := config.ModelCheckConfig{RequestBudget24h: 10, TimeoutSeconds: 30}
	done := make(chan error, 1)
	go func() { _, err := service.RunManualTarget(context.Background(), option, cfg); done <- err }()
	select {
	case <-runner.started:
	case <-time.After(5 * time.Second):
		t.Fatal("probe did not start")
	}
	discoveredAt := time.Now().UTC().Add(time.Minute)
	targets, err := store.SyncTargets([]storage.ModelCheckTargetDiscovery{{ChannelID: 3, UpstreamAccountIdentityRevision: 1, ChannelName: "Renamed", GroupName: "manual", ModelName: "gpt-test", App: storage.GatewayAppCodex}}, discoveredAt)
	if err != nil || len(targets) != 1 {
		close(runner.release)
		t.Fatalf("sync=%v %v", targets, err)
	}
	if err := store.SetTargetEnabled(targets[0].ID, false); err != nil {
		close(runner.release)
		t.Fatal(err)
	}
	close(runner.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	current, err := store.FindTarget(targets[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Enabled || !current.Active || current.ChannelName != "Renamed" || current.LastDiscoveredAt.Sub(discoveredAt).Abs() > time.Millisecond {
		t.Fatalf("operator/discovery fields overwritten: %+v", current)
	}
}

func TestModelCheckRunRereadsBaselineAfterWaitingForTargetLock(t *testing.T) {
	store := openModelCheckTestStore(t)
	option := TargetDescriptor{ChannelID: 3, ChannelName: "Trusted", GroupName: "manual", ModelName: "gpt-test", App: storage.GatewayAppCodex, UpstreamAccountIdentityRevision: 1}
	runner := &manualPelicanRunner{manual: []TargetDescriptor{option}}
	service := New(Options{Store: store, Runner: runner, Random: zeroReader{}})
	target, err := store.EnsureManualTarget(storage.ModelCheckTargetDiscovery{ChannelID: 3, UpstreamAccountIdentityRevision: 1, ChannelName: "Trusted", GroupName: "manual", ModelName: "gpt-test", App: storage.GatewayAppCodex}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	cfg := NormalizeConfig(config.ModelCheckConfig{RequestBudget24h: 10, TimeoutSeconds: 30})
	// Two callers can have captured this same target snapshot before the lock.
	for i := 0; i < 2; i++ {
		if _, err := service.executeTarget(context.Background(), *target, cfg); err != nil {
			t.Fatal(err)
		}
	}
	current, err := store.FindTarget(target.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.BaselineSamples != 2 {
		t.Fatalf("stale baseline reused: %d", current.BaselineSamples)
	}
}

func TestModelCheckQueuedRunCancellationReleasesBudgetAndCapacity(t *testing.T) {
	store := openModelCheckTestStore(t)
	runner := &manualPelicanRunner{}
	service := New(Options{Store: store, Runner: runner, Random: zeroReader{}})
	target := storage.ModelCheckTarget{ID: 1}
	lock := service.targetLock(target.ID)
	lock <- struct{}{}
	defer func() { <-lock }()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := service.executeTarget(ctx, target, NormalizeConfig(config.ModelCheckConfig{}))
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("canceled queue: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled run kept waiting for long-running target")
	}
	if service.activeRuns != 0 || runner.probeCalls != 0 {
		t.Fatal("canceled queue retained capacity or sent a paid request")
	}
	// A fresh reservation can consume the entire limit after cancellation.
	release, err := service.reserveRequestBudget(2, 2)
	if err != nil {
		t.Fatal(err)
	}
	release()
}

type deadlinePelicanRunner struct {
	manualPelicanRunner
	deadlines map[string]time.Duration
}

func (r *deadlinePelicanRunner) RunProbe(ctx context.Context, target TargetDescriptor, request ProbeRequest) (ProbeResponse, error) {
	deadline, _ := ctx.Deadline()
	r.deadlines[request.Kind] = time.Until(deadline)
	return r.pelicanRunner.RunProbe(ctx, target, request)
}

func TestManualModelCheckUsesSeparatePelicanDeadlineAndProgress(t *testing.T) {
	option := TargetDescriptor{ChannelID: 3, ChannelName: "Trusted", GroupName: "manual", ModelName: "gpt-test", App: storage.GatewayAppCodex, UpstreamAccountIdentityRevision: 1}
	runner := &deadlinePelicanRunner{manualPelicanRunner: manualPelicanRunner{manual: []TargetDescriptor{option}}, deadlines: map[string]time.Duration{}}
	service := New(Options{Store: openModelCheckTestStore(t), Runner: runner, Random: zeroReader{}})
	var events []ProgressEvent
	ctx := WithProgressObserver(context.Background(), func(event ProgressEvent) { events = append(events, event) })
	if _, err := service.RunManualTarget(ctx, option, config.ModelCheckConfig{}); err != nil {
		t.Fatal(err)
	}
	for kind, want := range map[string]time.Duration{PelicanProbeKind: 600 * time.Second, "candy_shapes": 60 * time.Second} {
		if remaining := runner.deadlines[kind]; remaining < want-time.Second || remaining > want {
			t.Fatalf("%s deadline = %s, want %s", kind, remaining, want)
		}
	}
	if runner.probeCalls != 2 || len(events) != 5 || events[0].Type != "probe_started" || events[4].Type != "saving" {
		t.Fatalf("requests=%d progress=%+v", runner.probeCalls, events)
	}
}
