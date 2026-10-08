package modelcheck

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/config"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
)

type zeroReader struct{}

const modelCheckTestIdentityRevision uint64 = 1

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

// modelCheckTestTargets supplies the identity fence for the fixed local
// channel fixtures below. Individual behavior tests can stay focused on their
// probe outcome while still exercising the current-identity persistence path.
func modelCheckTestTargets(targets []TargetDescriptor) []TargetDescriptor {
	cloned := append([]TargetDescriptor(nil), targets...)
	for i := range cloned {
		if cloned[i].UpstreamAccountIdentityRevision == 0 {
			cloned[i].UpstreamAccountIdentityRevision = modelCheckTestIdentityRevision
		}
	}
	return cloned
}

type fakeRunner struct {
	targets    []TargetDescriptor
	response   ProbeResponse
	err        error
	probeCalls int
}

func (f *fakeRunner) DiscoverTargets(context.Context, time.Time) ([]TargetDescriptor, error) {
	return modelCheckTestTargets(f.targets), nil
}

func (f *fakeRunner) RunProbe(_ context.Context, target TargetDescriptor, _ ProbeRequest) (ProbeResponse, error) {
	f.probeCalls++
	response := f.response
	if response.ActualModel == "" {
		response.ActualModel = target.ModelName
	}
	return response, f.err
}

type partialDeferredRunner struct {
	targets []TargetDescriptor
	calls   int
}

func (r *partialDeferredRunner) DiscoverTargets(context.Context, time.Time) ([]TargetDescriptor, error) {
	return modelCheckTestTargets(r.targets), nil
}

func (r *partialDeferredRunner) RunProbe(_ context.Context, target TargetDescriptor, _ ProbeRequest) (ProbeResponse, error) {
	r.calls++
	if r.calls > 1 {
		return ProbeResponse{}, ErrProbeDeferred
	}
	return ProbeResponse{Text: `{}`, ActualModel: target.ModelName, FirstByteMS: 100, LatencyMS: 200}, nil
}

func TestBuildChallengesUsesStrictMachineScoring(t *testing.T) {
	challenges, err := buildChallenges(zeroReader{}, availableProbeCount())
	if err != nil {
		t.Fatalf("build challenges: %v", err)
	}
	if len(challenges) != availableProbeCount() {
		t.Fatalf("challenge count = %d", len(challenges))
	}
	for _, challenge := range challenges {
		answer := challenge.expected
		if challenge.kind == PelicanProbeKind {
			answer = testPelicanSVG(challenge.expected)
		}
		if passed, detail := challenge.evaluate(answer); !passed {
			t.Fatalf("%s rejected expected answer: %s", challenge.kind, detail)
		}
		if passed, _ := challenge.evaluate("```json\n" + challenge.expected + "\n```"); passed {
			t.Fatalf("%s accepted markdown-wrapped JSON", challenge.kind)
		}
		if len(challenge.hash) != 64 {
			t.Fatalf("%s challenge hash length = %d", challenge.kind, len(challenge.hash))
		}
	}
}

func TestRunTargetAlertsOnlyAfterRepeatedThinkingLeak(t *testing.T) {
	store := openModelCheckTestStore(t)
	runner := &fakeRunner{
		targets:  []TargetDescriptor{{ChannelID: 7, ChannelName: "Example", GroupName: "gpt", ModelName: "gpt-5", App: storage.GatewayAppCodex}},
		response: ProbeResponse{Text: "<thinking>private plan</thinking>{}", PromptTokens: 10, CompletionTokens: 8, TotalTokens: 18, LatencyMS: 120},
	}
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	service := New(Options{Store: store, Runner: runner, Random: zeroReader{}, Now: func() time.Time {
		now = now.Add(time.Second)
		return now
	}})
	targets, err := service.SyncTargets(context.Background())
	if err != nil || len(targets) != 1 {
		t.Fatalf("sync targets = %+v, %v", targets, err)
	}
	cfg := config.ModelCheckConfig{ProbesPerTarget: 2, TimeoutSeconds: 30, AlertFailures: 2, HistoryDays: 90, RequestBudget24h: 20}
	first, err := service.RunTarget(context.Background(), targets[0].ID, cfg)
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	if first.Status != storage.ModelCheckRunAnomaly || !strings.Contains(first.AnomalyReasons, "thinking") {
		t.Fatalf("first run = %+v", first)
	}
	stored, err := store.FindTarget(targets[0].ID)
	if err != nil {
		t.Fatalf("find first target: %v", err)
	}
	if stored.AlertActive {
		t.Fatal("single anomaly activated alert")
	}
	if stored.LastStatus != storage.ModelCheckTargetSuspect {
		t.Fatalf("first visible anomaly status = %q, want suspect", stored.LastStatus)
	}
	second, err := service.RunTarget(context.Background(), targets[0].ID, cfg)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	stored, err = store.FindTarget(targets[0].ID)
	if err != nil {
		t.Fatalf("find second target: %v", err)
	}
	if !stored.AlertActive || stored.LastStatus != storage.ModelCheckTargetSuspect || stored.RecentAnomalies != 2 {
		t.Fatalf("target after repeated anomalies = %+v", stored)
	}
	if second.RequestCount != 2 || runner.probeCalls != 4 {
		t.Fatalf("requests run=%d calls=%d", second.RequestCount, runner.probeCalls)
	}
	if strings.Contains(second.EvidenceJSON, "api-key") {
		t.Fatal("evidence unexpectedly contains key material")
	}
}

func TestRunTargetEnforcesRollingRequestBudget(t *testing.T) {
	store := openModelCheckTestStore(t)
	runner := &fakeRunner{
		targets:  []TargetDescriptor{{ChannelID: 3, ChannelName: "Budget", GroupName: "gpt", ModelName: "gpt-5", App: storage.GatewayAppCodex}},
		response: ProbeResponse{Text: `{}`, LatencyMS: 10},
	}
	service := New(Options{Store: store, Runner: runner, Random: zeroReader{}})
	targets, err := service.SyncTargets(context.Background())
	if err != nil || len(targets) != 1 {
		t.Fatalf("sync targets = %+v, %v", targets, err)
	}
	cfg := config.ModelCheckConfig{Concurrency: 2, ProbesPerTarget: 2, RequestBudget24h: 2, TimeoutSeconds: 30, AlertFailures: 2, HistoryDays: 90}
	if _, err := service.RunTarget(context.Background(), targets[0].ID, cfg); err != nil {
		t.Fatalf("first run: %v", err)
	}
	if _, err := service.RunTarget(context.Background(), targets[0].ID, cfg); !errors.Is(err, ErrRequestBudgetExceeded) {
		t.Fatalf("second run error = %v, want request budget exhaustion", err)
	}
	if runner.probeCalls != 2 {
		t.Fatalf("probe calls = %d, want 2", runner.probeCalls)
	}
}

func TestRunTargetDeferredCapacityDoesNotCountRequestsOrChangeHealthyTarget(t *testing.T) {
	store := openModelCheckTestStore(t)
	runner := &fakeRunner{
		targets: []TargetDescriptor{{ChannelID: 31, ChannelName: "Deferred", GroupName: "gpt", ModelName: "gpt-5", App: storage.GatewayAppCodex}},
		err:     ErrProbeDeferred,
	}
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	service := New(Options{Store: store, Runner: runner, Random: zeroReader{}, Now: func() time.Time { return now }})
	targets, err := service.SyncTargets(context.Background())
	if err != nil || len(targets) != 1 {
		t.Fatalf("sync targets = %+v, %v", targets, err)
	}
	target, err := store.FindTarget(targets[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	checkedAt := now.Add(-time.Hour)
	lastScore := 100.0
	target.LastStatus = storage.ModelCheckTargetHealthy
	target.LastCheckedAt = &checkedAt
	target.LastScore = &lastScore
	target.LastActualModel = "gpt-5"
	seededAt := now.Add(-48 * time.Hour)
	if err := store.SaveRunAndTarget(&storage.ModelCheckRun{
		TargetID: target.ID, ChannelID: target.ChannelID, UpstreamAccountIdentityRevision: target.UpstreamAccountIdentityRevision, ChannelName: target.ChannelName,
		GroupName: target.GroupName, ModelName: target.ModelName, App: target.App,
		ProbeVersion: probeVersion, Status: storage.ModelCheckRunPassed, Score: 100,
		EvidenceJSON: "[]", StartedAt: seededAt, FinishedAt: seededAt,
	}, target); err != nil {
		t.Fatal(err)
	}

	cfg := config.ModelCheckConfig{Concurrency: 1, ProbesPerTarget: 2, RequestBudget24h: 10, TimeoutSeconds: 30, AlertFailures: 2, HistoryDays: 90}
	for attempt := 0; attempt < 2; attempt++ {
		if run, err := service.RunTarget(context.Background(), target.ID, cfg); run != nil || !errors.Is(err, ErrProbeDeferred) {
			t.Fatalf("deferred run %d = %+v, %v", attempt, run, err)
		}
	}
	if runner.probeCalls != 2 {
		t.Fatalf("deferred probe calls = %d, want one local rejection per run", runner.probeCalls)
	}
	stored, err := store.FindTarget(target.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.LastStatus != storage.ModelCheckTargetHealthy || stored.ConsecutiveErrors != 0 || stored.LastError != "" ||
		stored.LastCheckedAt == nil || !stored.LastCheckedAt.Equal(checkedAt) || stored.LastScore == nil || *stored.LastScore != lastScore {
		t.Fatalf("deferred probes changed target health: %+v", stored)
	}
	_, total, err := store.ListRunsPage(target.ID, 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 {
		t.Fatalf("deferred probes persisted runs: total=%d", total)
	}
	if used, err := service.RequestsUsed24h(); err != nil || used != 0 {
		t.Fatalf("deferred request count = %d, err=%v", used, err)
	}
}

func TestRunTargetPartialDeferredCapacityDoesNotPersistPartialHealthResult(t *testing.T) {
	store := openModelCheckTestStore(t)
	runner := &partialDeferredRunner{
		targets: []TargetDescriptor{{ChannelID: 33, ChannelName: "Partially deferred", GroupName: "gpt", ModelName: "gpt-5", App: storage.GatewayAppCodex}},
	}
	service := New(Options{Store: store, Runner: runner, Random: zeroReader{}})
	targets, err := service.SyncTargets(context.Background())
	if err != nil || len(targets) != 1 {
		t.Fatalf("sync targets = %+v, %v", targets, err)
	}
	before, err := store.FindTarget(targets[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	run, err := service.RunTarget(context.Background(), before.ID, config.ModelCheckConfig{
		Concurrency: 1, ProbesPerTarget: 2, RequestBudget24h: 10, TimeoutSeconds: 30, AlertFailures: 2, HistoryDays: 90,
	})
	if run == nil || run.Status != storage.ModelCheckRunDeferred || run.RequestCount != 1 || !errors.Is(err, ErrProbeDeferred) {
		t.Fatalf("partial deferred run = %+v, %v", run, err)
	}
	if runner.calls != 2 {
		t.Fatalf("partial deferred probe calls = %d, want one request then one local rejection", runner.calls)
	}
	after, err := store.FindTarget(before.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.LastCheckedAt != before.LastCheckedAt || after.LastStatus != before.LastStatus || after.ConsecutiveErrors != before.ConsecutiveErrors ||
		after.AlertActive != before.AlertActive || after.LastError != before.LastError {
		t.Fatalf("partial deferred probe changed target health: before=%+v after=%+v", before, after)
	}
	_, total, err := store.ListRunsPage(before.ID, 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 {
		t.Fatalf("partial deferred audit run total = %d, want 1", total)
	}
	recent, err := store.ListRecentRuns(before.ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(recent) != 0 {
		t.Fatalf("deferred audit entered health history: %+v", recent)
	}
	if used, err := service.RequestsUsed24h(); err != nil || used != 1 {
		t.Fatalf("partial deferred request count = %d, err=%v", used, err)
	}
}

func TestRunScheduledReportsDeferredWithoutBatchError(t *testing.T) {
	store := openModelCheckTestStore(t)
	runner := &fakeRunner{
		targets: []TargetDescriptor{{ChannelID: 32, ChannelName: "Deferred batch", GroupName: "gpt", ModelName: "gpt-5", App: storage.GatewayAppCodex}},
		err:     ErrProbeDeferred,
	}
	service := New(Options{Store: store, Runner: runner, Random: zeroReader{}})
	batch, err := service.RunScheduled(context.Background(), config.ModelCheckConfig{
		Enabled: true, Concurrency: 1, MaxTargetsPerRun: 1, ProbesPerTarget: 2,
		RequestBudget24h: 10, TimeoutSeconds: 30, AlertFailures: 2, HistoryDays: 90,
	})
	if err != nil {
		t.Fatalf("scheduled deferred batch: %v", err)
	}
	if batch.Targets != 1 || batch.Deferred != 1 || batch.Errors != 0 || batch.Requests != 0 {
		t.Fatalf("scheduled deferred batch = %+v", batch)
	}
}

func TestRunScheduledCountsRequestsFromPartialDeferredAudit(t *testing.T) {
	store := openModelCheckTestStore(t)
	runner := &partialDeferredRunner{
		targets: []TargetDescriptor{{ChannelID: 34, ChannelName: "Partially deferred batch", GroupName: "gpt", ModelName: "gpt-5", App: storage.GatewayAppCodex}},
	}
	service := New(Options{Store: store, Runner: runner, Random: zeroReader{}})
	batch, err := service.RunScheduled(context.Background(), config.ModelCheckConfig{
		Enabled: true, Concurrency: 1, MaxTargetsPerRun: 1, ProbesPerTarget: 2,
		RequestBudget24h: 10, TimeoutSeconds: 30, AlertFailures: 2, HistoryDays: 90,
	})
	if err != nil {
		t.Fatalf("scheduled partial deferred batch: %v", err)
	}
	if batch.Targets != 1 || batch.Deferred != 1 || batch.Errors != 0 || batch.Requests != 1 {
		t.Fatalf("scheduled partial deferred batch = %+v", batch)
	}
}

type concurrentRunner struct {
	targets []TargetDescriptor
	active  atomic.Int32
	maximum atomic.Int32
}

func (r *concurrentRunner) DiscoverTargets(context.Context, time.Time) ([]TargetDescriptor, error) {
	return modelCheckTestTargets(r.targets), nil
}

func (r *concurrentRunner) RunProbe(ctx context.Context, target TargetDescriptor, _ ProbeRequest) (ProbeResponse, error) {
	active := r.active.Add(1)
	defer r.active.Add(-1)
	for {
		maximum := r.maximum.Load()
		if active <= maximum || r.maximum.CompareAndSwap(maximum, active) {
			break
		}
	}
	select {
	case <-ctx.Done():
		return ProbeResponse{}, ctx.Err()
	case <-time.After(25 * time.Millisecond):
		return ProbeResponse{Text: `{}`, ActualModel: target.ModelName, LatencyMS: 25}, nil
	}
}

func TestRunTargetSharesConfiguredConcurrencyAcrossManualRuns(t *testing.T) {
	store := openModelCheckTestStore(t)
	runner := &concurrentRunner{targets: []TargetDescriptor{
		{ChannelID: 4, ChannelName: "One", GroupName: "gpt-a", ModelName: "gpt-5", App: storage.GatewayAppCodex},
		{ChannelID: 5, ChannelName: "Two", GroupName: "gpt-b", ModelName: "gpt-5", App: storage.GatewayAppCodex},
	}}
	service := New(Options{Store: store, Runner: runner, Random: zeroReader{}})
	targets, err := service.SyncTargets(context.Background())
	if err != nil || len(targets) != 2 {
		t.Fatalf("sync targets = %+v, %v", targets, err)
	}
	cfg := config.ModelCheckConfig{Concurrency: 1, ProbesPerTarget: 1, RequestBudget24h: 10, TimeoutSeconds: 30, AlertFailures: 2, HistoryDays: 90}
	errCh := make(chan error, len(targets))
	var wg sync.WaitGroup
	for i := range targets {
		wg.Add(1)
		go func(id uint) {
			defer wg.Done()
			_, runErr := service.RunTarget(context.Background(), id, cfg)
			errCh <- runErr
		}(targets[i].ID)
	}
	wg.Wait()
	close(errCh)
	for runErr := range errCh {
		if runErr != nil {
			t.Fatalf("concurrent run: %v", runErr)
		}
	}
	if got := runner.maximum.Load(); got != 1 {
		t.Fatalf("maximum concurrent probes = %d, want 1", got)
	}
}

func TestSyncTargetsSerializesConcurrentRefreshes(t *testing.T) {
	store := openModelCheckTestStore(t)
	runner := &fakeRunner{targets: []TargetDescriptor{{ChannelID: 6, ChannelName: "Sync", GroupName: "gpt", ModelName: "gpt-5", App: storage.GatewayAppCodex}}}
	service := New(Options{Store: store, Runner: runner})
	errCh := make(chan error, 8)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, syncErr := service.SyncTargets(context.Background())
			errCh <- syncErr
		}()
	}
	wg.Wait()
	close(errCh)
	for syncErr := range errCh {
		if syncErr != nil {
			t.Fatalf("concurrent sync: %v", syncErr)
		}
	}
	targets, err := store.ListTargets(false)
	if err != nil || len(targets) != 1 {
		t.Fatalf("stored targets = %+v, %v", targets, err)
	}
}

type sequenceRunner struct {
	targets []TargetDescriptor
	calls   int
}

func (r *sequenceRunner) DiscoverTargets(context.Context, time.Time) ([]TargetDescriptor, error) {
	return modelCheckTestTargets(r.targets), nil
}

func (r *sequenceRunner) RunProbe(_ context.Context, target TargetDescriptor, _ ProbeRequest) (ProbeResponse, error) {
	r.calls++
	if r.calls == 2 {
		return ProbeResponse{}, errors.New("probe failed")
	}
	return ProbeResponse{Text: `{}`, ActualModel: target.ModelName, FirstByteMS: 100, LatencyMS: 200}, nil
}

func TestRunTargetAveragesLatencyAcrossSuccessfulResponsesOnly(t *testing.T) {
	store := openModelCheckTestStore(t)
	runner := &sequenceRunner{targets: []TargetDescriptor{{ChannelID: 8, ChannelName: "Latency", GroupName: "gpt", ModelName: "gpt-5", App: storage.GatewayAppCodex}}}
	service := New(Options{Store: store, Runner: runner, Random: zeroReader{}})
	targets, err := service.SyncTargets(context.Background())
	if err != nil || len(targets) != 1 {
		t.Fatalf("sync targets = %+v, %v", targets, err)
	}
	run, err := service.RunTarget(context.Background(), targets[0].ID, config.ModelCheckConfig{
		Concurrency: 1, ProbesPerTarget: 2, RequestBudget24h: 10, TimeoutSeconds: 30, AlertFailures: 2, HistoryDays: 90,
	})
	if err != nil {
		t.Fatalf("run target: %v", err)
	}
	if run.RequestCount != 2 || run.FirstByteMS != 100 || run.LatencyMS != 200 {
		t.Fatalf("run latency = requests %d first-byte %d total %d", run.RequestCount, run.FirstByteMS, run.LatencyMS)
	}
}

type partialSuccessRunner struct {
	targets []TargetDescriptor
	calls   int
}

func (r *partialSuccessRunner) DiscoverTargets(context.Context, time.Time) ([]TargetDescriptor, error) {
	return modelCheckTestTargets(r.targets), nil
}

func (r *partialSuccessRunner) RunProbe(_ context.Context, target TargetDescriptor, request ProbeRequest) (ProbeResponse, error) {
	r.calls++
	if r.calls > 1 {
		return ProbeResponse{}, errors.New("second probe failed")
	}
	responses := map[string]string{
		CandyProbeKind:          `{"nonce":"000000000000","answer":14}`,
		"arithmetic_json":       `{"nonce":"000000000000","answer":81}`,
		"constraint_order":      `{"nonce":"000000000000","order":["Birch","Cedar","Dune","Elm","Aster"]}`,
		"instruction_transform": `{"nonce":"000000000000","value":"AAAAAAAA"}`,
	}
	return ProbeResponse{Text: responses[request.Kind], ActualModel: target.ModelName, FirstByteMS: 100, LatencyMS: 200}, nil
}

func TestRunTargetRetainsScoreFromPartialSuccessfulResponses(t *testing.T) {
	store := openModelCheckTestStore(t)
	runner := &partialSuccessRunner{targets: []TargetDescriptor{{ChannelID: 10, ChannelName: "Partial", GroupName: "gpt", ModelName: "gpt-5", App: storage.GatewayAppCodex}}}
	service := New(Options{Store: store, Runner: runner, Random: zeroReader{}})
	targets, err := service.SyncTargets(context.Background())
	if err != nil || len(targets) != 1 {
		t.Fatalf("sync targets = %+v, %v", targets, err)
	}
	run, err := service.RunTarget(context.Background(), targets[0].ID, config.ModelCheckConfig{
		Concurrency: 1, ProbesPerTarget: 2, RequestBudget24h: 10, TimeoutSeconds: 30, AlertFailures: 2, HistoryDays: 90,
	})
	if err != nil {
		t.Fatalf("run target: %v", err)
	}
	if run.Status != storage.ModelCheckRunError || run.Score != 50 {
		t.Fatalf("partial run = %+v", run)
	}
	target, err := store.FindTarget(targets[0].ID)
	if err != nil {
		t.Fatalf("find target: %v", err)
	}
	if target.LastScore == nil || *target.LastScore != run.Score {
		t.Fatalf("target did not retain partial score: %+v", target)
	}
}

func TestRunTargetDoesNotClassifyAllFailedMatureTargetAsAnomaly(t *testing.T) {
	store := openModelCheckTestStore(t)
	runner := &fakeRunner{
		targets: []TargetDescriptor{{ChannelID: 9, ChannelName: "Unavailable", GroupName: "gpt", ModelName: "gpt-5", App: storage.GatewayAppCodex}},
		err:     errors.New("proxy failure Proxy-Authorization: Basic dXNlcjpwYXNz token=t1 password=p1 https://proxy.invalid/check?token=q1"),
	}
	now := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	service := New(Options{Store: store, Runner: runner, Random: zeroReader{}, Now: func() time.Time { return now }})
	targets, err := service.SyncTargets(context.Background())
	if err != nil || len(targets) != 1 {
		t.Fatalf("sync targets = %+v, %v", targets, err)
	}
	target, err := store.FindTarget(targets[0].ID)
	if err != nil {
		t.Fatalf("find target: %v", err)
	}
	baseline := 100.0
	target.BaselineScore = &baseline
	target.BaselineSamples = baselineMinSamples
	lastScore := 100.0
	target.LastScore = &lastScore
	target.LastActualModel = "gpt-5"
	historicalAt := now.Add(-48 * time.Hour)
	if err := store.SaveRunAndTarget(&storage.ModelCheckRun{
		TargetID: target.ID, ChannelID: target.ChannelID, UpstreamAccountIdentityRevision: target.UpstreamAccountIdentityRevision, ChannelName: target.ChannelName,
		GroupName: target.GroupName, ModelName: target.ModelName, App: target.App,
		ProbeVersion: probeVersion, Status: storage.ModelCheckRunPassed, Score: 100, EvidenceJSON: "[]",
		StartedAt: historicalAt, FinishedAt: historicalAt,
	}, target); err != nil {
		t.Fatalf("seed mature target: %v", err)
	}
	cfg := config.ModelCheckConfig{Concurrency: 1, ProbesPerTarget: 2, RequestBudget24h: 10, TimeoutSeconds: 30, AlertFailures: 2, HistoryDays: 90}
	run, err := service.RunTarget(context.Background(), target.ID, cfg)
	if err != nil {
		t.Fatalf("first failed run: %v", err)
	}
	if run.Status != storage.ModelCheckRunError || strings.Contains(run.AnomalyReasons, "基线") {
		t.Fatalf("first failed run = %+v", run)
	}
	if run.Error != ProbeErrorUnknown || strings.Contains(run.EvidenceJSON, "dXNlcjpwYXNz") || strings.Contains(run.EvidenceJSON, "q1") {
		t.Fatalf("first failed diagnostics leaked or were not normalized: error=%q evidence=%s", run.Error, run.EvidenceJSON)
	}
	target, err = store.FindTarget(target.ID)
	if err != nil {
		t.Fatalf("find target after first failure: %v", err)
	}
	if target.LastStatus != storage.ModelCheckTargetPending || target.AlertActive {
		t.Fatalf("target after first failed run = %+v", target)
	}
	if target.LastScore == nil || *target.LastScore != lastScore {
		t.Fatalf("all-failed run overwrote last score: %+v", target.LastScore)
	}
	if target.LastActualModel != "gpt-5" {
		t.Fatalf("all-failed run overwrote last actual model: %q", target.LastActualModel)
	}
	if _, err := service.RunTarget(context.Background(), target.ID, cfg); err != nil {
		t.Fatalf("second failed run: %v", err)
	}
	target, err = store.FindTarget(target.ID)
	if err != nil {
		t.Fatalf("find target after second failure: %v", err)
	}
	if target.LastStatus != storage.ModelCheckTargetUnavailable || target.AlertActive {
		t.Fatalf("target after second failed run = %+v", target)
	}
}

func TestSanitizeProbeErrorDiagnosticKeepsOnlyKnownCategoryAndStatus(t *testing.T) {
	if got := ProbeErrorDiagnostic(NewProbeError(ProbeErrorHTTPStatus, 502), 0); got != "http_status (HTTP 502)" {
		t.Fatalf("typed diagnostic = %q", got)
	}
	if got := SanitizeProbeErrorDiagnostic("http_status (HTTP 502)"); got != "http_status (HTTP 502)" {
		t.Fatalf("known diagnostic = %q", got)
	}
	if got := SanitizeProbeErrorDiagnostic("Proxy-Authorization: Basic dXNlcjpwYXNz token=t1 https://proxy.invalid/check?token=q1"); got != ProbeErrorUnknown {
		t.Fatalf("unsafe legacy diagnostic = %q", got)
	}
	if got := SanitizeProbeErrorDiagnostics("network_error\nhttp_status (HTTP 502)"); got != "network_error\nhttp_status (HTTP 502)" {
		t.Fatalf("multiple diagnostics = %q", got)
	}
}

func TestUpdateTargetStateRequiresTwoHealthyRunsToRecover(t *testing.T) {
	service := New(Options{})
	now := time.Now()
	baseline := 100.0
	target := storage.ModelCheckTarget{AlertActive: true, AlertReason: "failed", BaselineScore: &baseline, BaselineSamples: 4}
	run := &storage.ModelCheckRun{Status: storage.ModelCheckRunPassed, Score: 100, FinishedAt: now}
	service.updateTargetState(&target, run, []storage.ModelCheckRun{{Status: storage.ModelCheckRunAnomaly}}, nil, true, config.ModelCheckConfig{AlertFailures: 2})
	if !target.AlertActive {
		t.Fatal("alert recovered after only one passing run")
	}
	service.updateTargetState(&target, run, []storage.ModelCheckRun{{Status: storage.ModelCheckRunPassed}}, nil, true, config.ModelCheckConfig{AlertFailures: 2})
	if target.AlertActive || target.LastStatus != storage.ModelCheckTargetHealthy || target.RecoveredAt == nil {
		t.Fatalf("target did not recover after two passes: %+v", target)
	}
}

func TestUpdateTargetStateShowsUnavailableAfterConsecutiveErrorsWhileAlerting(t *testing.T) {
	service := New(Options{})
	now := time.Now()
	target := storage.ModelCheckTarget{AlertActive: true, AlertReason: "existing"}
	cfg := config.ModelCheckConfig{AlertFailures: 2}

	service.updateTargetState(&target, &storage.ModelCheckRun{Status: storage.ModelCheckRunError, FinishedAt: now}, nil, nil, false, cfg)
	if !target.AlertActive || target.LastStatus != storage.ModelCheckTargetSuspect || target.ConsecutiveErrors != 1 || target.AlertReason != "existing" {
		t.Fatalf("target after first error while alerting = %+v", target)
	}

	service.updateTargetState(&target, &storage.ModelCheckRun{Status: storage.ModelCheckRunError, FinishedAt: now.Add(time.Second)}, nil, nil, false, cfg)
	if !target.AlertActive || target.LastStatus != storage.ModelCheckTargetUnavailable || target.ConsecutiveErrors != 2 || target.AlertReason != "existing" {
		t.Fatalf("target after second error while alerting = %+v", target)
	}
}

func TestVisibleThinkingMarkup(t *testing.T) {
	for _, value := range []string{"<thinking>x</thinking>", "before </THINKING> after"} {
		if !visibleThinkingMarkup(value) {
			t.Fatalf("thinking markup not detected in %q", value)
		}
	}
	if visibleThinkingMarkup("ordinary reasoning summary") {
		t.Fatal("ordinary reasoning text was flagged")
	}
}

func openModelCheckTestStore(t *testing.T) *storage.ModelChecks {
	t.Helper()
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("db handle: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	channels := storage.NewChannels(db)
	for _, id := range []uint{3, 4, 5, 6, 7, 8, 9, 10, 31, 32, 33, 34} {
		channel := &storage.Channel{
			ID: id, Name: fmt.Sprintf("model-check-fixture-%d", id),
			Type: storage.ChannelTypeNewAPI, GatewayApp: storage.GatewayAppCodex,
			SiteURL: fmt.Sprintf("https://model-check-%d.invalid", id),
		}
		if err := channels.Create(channel); err != nil {
			t.Fatalf("create model-check fixture channel %d: %v", id, err)
		}
	}
	return storage.NewModelChecks(db)
}

func TestDecodeStrictJSONRejectsTrailingContent(t *testing.T) {
	var got map[string]any
	if err := decodeStrictJSON(`{"ok":true} trailing`, &got); err == nil {
		t.Fatal("trailing content was accepted")
	}
	if err := decodeStrictJSON(string(bytes.TrimSpace([]byte(` {"ok":true} `))), &got); err != nil {
		t.Fatalf("valid JSON rejected: %v", err)
	}
}
