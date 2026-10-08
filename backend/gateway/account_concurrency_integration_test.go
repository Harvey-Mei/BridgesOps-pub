package gateway

import (
	"testing"
	"time"

	appcrypto "github.com/Harvey-Mei/BridgeOps/backend/crypto"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
)

func TestCandidateConstructionBatchesSharedAccountConcurrencyAndManualFixedBypasses(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("candidate-account-concurrency-test")
	if err != nil {
		t.Fatal(err)
	}
	ciphertext, err := cipher.Encrypt("shared-upstream-key")
	if err != nil {
		t.Fatal(err)
	}

	values := make([]storage.Channel, 2)
	base := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	limit := 1
	for index := range values {
		channel := storage.Channel{
			Name: "shared-account-" + string(rune('a'+index)), Type: storage.ChannelTypeSub2API,
			GatewayApp: storage.GatewayAppCodex, SiteURL: "https://api.example.com", Username: "shared-user",
			PasswordCipher: "encrypted", CredentialMode: storage.CredentialModePassword, MonitorEnabled: true,
		}
		if err := channels.Create(&channel); err != nil {
			t.Fatal(err)
		}
		if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "primary", Ratio: 1, LastSeenAt: base}); err != nil {
			t.Fatal(err)
		}
		if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
			ChannelID: channel.ID, GroupName: "primary", GroupRatio: 1,
			APIKeyCipher: ciphertext, APIKeyPrefix: "shared",
			Enabled: true, LatencyProbeEnabled: true,
		}); err != nil {
			t.Fatal(err)
		}
		if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "added", int64(index+1), "BOG primary", base.Add(time.Duration(index)*time.Second)); err != nil {
			t.Fatal(err)
		}
		generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary")
		if err != nil {
			t.Fatal(err)
		}
		if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "primary", "codex", "gpt-test", generation.DiscoveredAt.Add(time.Second), "", generation); err != nil {
			t.Fatal(err)
		}
		if _, err := channels.ObserveUpstreamAccountConcurrency(storage.UpstreamAccountConcurrencyObservation{
			ChannelID: channel.ID, ChannelIdentityRevision: channel.UpstreamAccountIdentityRevision,
			Provider: storage.ChannelTypeSub2API, APIOrigin: channel.SiteURL, ExternalAccountID: "shared-account",
			Limit: &limit, Scope: storage.UpstreamConcurrencyScopeAccount,
			Source: "sub2api.auth.me.concurrency", ObservedAt: base.Add(time.Duration(index) * time.Minute),
		}); err != nil {
			t.Fatal(err)
		}
		values[index] = channel
	}

	service := NewService(channels, rates, store, cipher, nil, nil)
	candidates, err := service.CandidatesForApp(storage.GatewayAppCodex, "gpt-test")
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 2 || candidates[0].UpstreamAccountConcurrency == nil || candidates[1].UpstreamAccountConcurrency == nil ||
		candidates[0].UpstreamAccountConcurrency.AccountRecordID == 0 ||
		candidates[0].UpstreamAccountConcurrency.AccountRecordID != candidates[1].UpstreamAccountConcurrency.AccountRecordID {
		t.Fatalf("candidate account snapshots = %+v", candidates)
	}

	manual, err := service.manualRouteCandidateWithStore(store, nil, storage.GatewayAppCodex, values[1].ID, "primary")
	if err != nil {
		t.Fatal(err)
	}
	if manual.UpstreamAccountConcurrency == nil || manual.UpstreamAccountConcurrency.AccountRecordID != candidates[0].UpstreamAccountConcurrency.AccountRecordID {
		t.Fatalf("manual candidate account snapshot = %+v", manual.UpstreamAccountConcurrency)
	}
	active, activeDecision := service.TryAcquireCandidateAccountConcurrency(candidates[0], AccountConcurrencySmartData)
	if active == nil || !activeDecision.Admitted {
		t.Fatalf("active smart decision = %#v", activeDecision)
	}
	fixed, fixedDecision := service.TryAcquireCandidateAccountConcurrency(manual, AccountConcurrencyFixedData)
	if fixed == nil || !fixedDecision.Admitted || !fixedDecision.Tracked {
		t.Fatalf("manual fixed decision = %#v", fixedDecision)
	}
	active.Release()
	fixed.Release()
}
