package modelcheck

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/config"
	appcrypto "github.com/Harvey-Mei/BridgeOps/backend/crypto"
	"github.com/Harvey-Mei/BridgeOps/backend/notify"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
)

type delayedIdentityProbeOutcome string

const (
	delayedIdentityProbeSuccess  delayedIdentityProbeOutcome = "success"
	delayedIdentityProbeAnomaly  delayedIdentityProbeOutcome = "anomaly"
	delayedIdentityProbeCanceled delayedIdentityProbeOutcome = "canceled"
)

type delayedIdentityRunner struct {
	target  TargetDescriptor
	started chan<- struct{}
	release <-chan struct{}
	outcome delayedIdentityProbeOutcome
}

func (r *delayedIdentityRunner) DiscoverTargets(context.Context, time.Time) ([]TargetDescriptor, error) {
	return []TargetDescriptor{r.target}, nil
}

func (r *delayedIdentityRunner) RunProbe(ctx context.Context, target TargetDescriptor, request ProbeRequest) (ProbeResponse, error) {
	select {
	case r.started <- struct{}{}:
	default:
	}
	if r.outcome == delayedIdentityProbeCanceled {
		<-ctx.Done()
		return ProbeResponse{}, ctx.Err()
	}
	select {
	case <-ctx.Done():
		return ProbeResponse{}, ctx.Err()
	case <-r.release:
	}
	if r.outcome == delayedIdentityProbeAnomaly {
		return ProbeResponse{Text: "<thinking>delayed account-a result</thinking>{}", ActualModel: target.ModelName}, nil
	}
	return ProbeResponse{
		Text: modelCheckPassingResponse(request.Kind), ActualModel: target.ModelName,
		FirstByteMS: 10, LatencyMS: 20,
	}, nil
}

func modelCheckPassingResponse(kind string) string {
	responses := map[string]string{
		"arithmetic_json":       `{"nonce":"000000000000","answer":81}`,
		"constraint_order":      `{"nonce":"000000000000","order":["Birch","Cedar","Dune","Elm","Aster"]}`,
		"instruction_transform": `{"nonce":"000000000000","value":"AAAAAAAA"}`,
	}
	return responses[kind]
}

func TestDelayedModelCheckResultCannotCrossAccountIdentityRevision(t *testing.T) {
	for _, outcome := range []delayedIdentityProbeOutcome{
		delayedIdentityProbeSuccess,
		delayedIdentityProbeAnomaly,
		delayedIdentityProbeCanceled,
	} {
		t.Run(string(outcome), func(t *testing.T) {
			db := testdb.Open(t)
			channels := storage.NewChannels(db)
			store := storage.NewModelChecks(db)
			notifications := storage.NewNotifications(db)
			cipher, err := appcrypto.NewCipher("model-check-identity-revision-test-secret")
			if err != nil {
				t.Fatalf("new cipher: %v", err)
			}

			var webhookHits atomic.Int32
			webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				webhookHits.Add(1)
				w.WriteHeader(http.StatusNoContent)
			}))
			defer webhook.Close()
			webhookConfig, err := json.Marshal(map[string]string{"url": webhook.URL})
			if err != nil {
				t.Fatalf("encode webhook config: %v", err)
			}
			configCipher, err := cipher.Encrypt(string(webhookConfig))
			if err != nil {
				t.Fatalf("encrypt webhook config: %v", err)
			}
			if err := notifications.CreateChannel(&storage.NotificationChannel{
				Name: "model-check identity webhook", Type: storage.NotifyWebhook, ConfigCipher: configCipher, Enabled: true,
			}); err != nil {
				t.Fatalf("create notification channel: %v", err)
			}

			accountA := &storage.Channel{
				Name: "model-check-account-a", Type: storage.ChannelTypeNewAPI,
				GatewayApp: storage.GatewayAppCodex, SiteURL: "https://account-a.invalid",
				Username: "account-a", PasswordCipher: "credential-a",
			}
			if err := channels.Create(accountA); err != nil {
				t.Fatalf("create account A channel: %v", err)
			}

			started := make(chan struct{}, 1)
			release := make(chan struct{})
			runner := &delayedIdentityRunner{
				target: TargetDescriptor{
					ChannelID: accountA.ID, UpstreamAccountIdentityRevision: accountA.UpstreamAccountIdentityRevision,
					ChannelName: accountA.Name, GroupName: "gpt", ModelName: "gpt-5", App: storage.GatewayAppCodex,
				},
				started: started, release: release, outcome: outcome,
			}
			dispatcher := notify.NewDispatcher(notifications, cipher, slog.New(slog.NewTextHandler(io.Discard, nil)), notify.Policy{SendMaxAttempts: 1})
			service := New(Options{
				Store: store, Channels: channels, Runner: runner, Dispatcher: dispatcher,
				Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Random: zeroReader{},
			})
			targets, err := service.SyncTargets(context.Background())
			if err != nil || len(targets) != 1 {
				t.Fatalf("sync account A target: targets=%+v err=%v", targets, err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			type result struct {
				run *storage.ModelCheckRun
				err error
			}
			completed := make(chan result, 1)
			go func() {
				run, runErr := service.RunTarget(ctx, targets[0].ID, config.ModelCheckConfig{
					Concurrency: 1, ProbesPerTarget: 1, RequestBudget24h: 10,
					TimeoutSeconds: 10, AlertFailures: 1, HistoryDays: 90,
				})
				completed <- result{run: run, err: runErr}
			}()

			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("account A probe did not begin")
			}
			accountB := *accountA
			accountB.Name = "model-check-account-b"
			accountB.Username = "account-b"
			accountB.PasswordCipher = "credential-b"
			if err := channels.UpdateWithUpstreamAccountIdentityChange(&accountB); err != nil {
				t.Fatalf("switch to account B: %v", err)
			}
			accountBCurrent, err := channels.FindByID(accountB.ID)
			if err != nil {
				t.Fatalf("read account B channel: %v", err)
			}
			if accountBCurrent.UpstreamAccountIdentityRevision == accountA.UpstreamAccountIdentityRevision {
				t.Fatal("account identity revision did not advance")
			}

			if outcome == delayedIdentityProbeCanceled {
				cancel()
			} else {
				close(release)
			}
			resultAfterSwitch := <-completed
			if resultAfterSwitch.run != nil || !errors.Is(resultAfterSwitch.err, storage.ErrStaleChannelAccountIdentityRevision) {
				t.Fatalf("late %s result=%+v err=%v, want stale identity rejection", outcome, resultAfterSwitch.run, resultAfterSwitch.err)
			}

			bTargets, err := store.SyncTargets([]storage.ModelCheckTargetDiscovery{{
				ChannelID: accountBCurrent.ID, UpstreamAccountIdentityRevision: accountBCurrent.UpstreamAccountIdentityRevision,
				ChannelName: accountBCurrent.Name, GroupName: "gpt", ModelName: "gpt-5", App: storage.GatewayAppCodex,
			}}, time.Now().UTC())
			if err != nil || len(bTargets) != 1 {
				t.Fatalf("sync account B target: targets=%+v err=%v", bTargets, err)
			}
			currentTarget, err := store.FindTarget(bTargets[0].ID)
			if err != nil {
				t.Fatalf("find account B target: %v", err)
			}
			if currentTarget.UpstreamAccountIdentityRevision != accountBCurrent.UpstreamAccountIdentityRevision ||
				currentTarget.LastStatus != storage.ModelCheckTargetPending || currentTarget.AlertActive ||
				currentTarget.LastCheckedAt != nil || currentTarget.LastScore != nil || currentTarget.BaselineScore != nil {
				t.Fatalf("account B target inherited account A probe state: %+v", currentTarget)
			}
			var persistedRuns int64
			if err := db.Model(&storage.ModelCheckRun{}).Count(&persistedRuns).Error; err != nil {
				t.Fatalf("count raw runs: %v", err)
			}
			if persistedRuns != 0 {
				t.Fatalf("late %s probe persisted %d runs", outcome, persistedRuns)
			}
			if got := webhookHits.Load(); got != 0 {
				t.Fatalf("late %s probe sent %d notifications", outcome, got)
			}
		})
	}
}
