package monitor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/channel"
	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	"github.com/Harvey-Mei/BridgeOps/backend/costing"
	"github.com/Harvey-Mei/BridgeOps/backend/crypto"
	"github.com/Harvey-Mei/BridgeOps/backend/notify"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"gorm.io/gorm"
)

type advertisedFactorSamplingCalls struct {
	authMe          atomic.Int32
	checkout        atomic.Int32
	groupsAvailable atomic.Int32
	groupRates      atomic.Int32
}

func TestManualCostBasisOverrideSkipsAdvertisedRateFence(t *testing.T) {
	factor := 0.1
	sampledAt := time.Now().UTC()
	state := &storage.ChannelCostState{
		FundingFactor: 0.25, Source: storage.CostBasisOverrideSource, Status: costing.StateStatusReady,
		ObservedProviderFactor: &factor, ObservedProviderFactorSource: "advertised",
		ObservedProviderFactorSampledAt: &sampledAt,
	}
	if costBasisRequiresAdvertisedRateFence(state) {
		t.Fatal("manual override was fenced to stale provider evidence")
	}
	state.Source = "advertised"
	if !costBasisRequiresAdvertisedRateFence(state) {
		t.Fatal("provider-owned basis lost its advertised factor fence")
	}
}

func TestManualCostBasisOverrideKeepsIncompleteProviderHistoryRetryable(t *testing.T) {
	state := &storage.ChannelCostState{
		ChannelID: 7, Version: 4, FundingFactor: 0.25,
		Source: storage.CostBasisOverrideSource, Status: costing.StateStatusReady,
	}
	if err := (&Service{}).persistSafeProviderFactorEvidence(nil, nil, state, nil, nil, time.Time{}); err != nil {
		t.Fatalf("manual basis was blocked by incomplete provider history: %v", err)
	}
}

func TestRefreshRatesBoundsAdvertisedFactorSampling(t *testing.T) {
	calls := &advertisedFactorSamplingCalls{}
	srv := newAdvertisedFactorSamplingServer(t, calls, 10)
	defer srv.Close()
	svc, rates, channelRecord, _, _ := newAdvertisedFactorSamplingService(t, srv.URL)

	if err := svc.RefreshRates(context.Background(), channelRecord); err != nil {
		t.Fatalf("RefreshRates: %v", err)
	}
	if got := calls.authMe.Load(); got != 1 {
		t.Fatalf("auth/me calls = %d, want one session authentication", got)
	}
	if got := calls.checkout.Load(); got != 2 {
		t.Fatalf("checkout calls = %d, want before/after factor fence", got)
	}
	if got := calls.groupsAvailable.Load(); got != 1 {
		t.Fatalf("groups/available calls = %d, want 1", got)
	}
	if got := calls.groupRates.Load(); got != 1 {
		t.Fatalf("groups/rates calls = %d, want 1", got)
	}
	stored, err := rates.ListByChannel(channelRecord.ID)
	if err != nil || len(stored) != 1 || stored[0].ModelName != "pro" || stored[0].Ratio != 0.06 {
		t.Fatalf("stored rate snapshot = %+v, %v", stored, err)
	}
}

func TestRefreshRatesStopsAtChangedAdvertisedFactorWithoutCompensation(t *testing.T) {
	calls := &advertisedFactorSamplingCalls{}
	srv := newAdvertisedFactorSamplingServer(t, calls, 5)
	defer srv.Close()
	svc, _, channelRecord, costSvc, _ := newAdvertisedFactorSamplingService(t, srv.URL)

	err := svc.RefreshRates(context.Background(), channelRecord)
	if !errors.Is(err, costing.ErrCostUnitChangeRequiresRebase) {
		t.Fatalf("RefreshRates error = %v", err)
	}
	if got := calls.authMe.Load(); got != 1 {
		t.Fatalf("auth/me calls = %d, accounting gate must not re-authenticate", got)
	}
	if got := calls.checkout.Load(); got != 1 {
		t.Fatalf("checkout calls = %d, changed pre-factor must stop immediately", got)
	}
	if got := calls.groupsAvailable.Load(); got != 0 {
		t.Fatalf("groups/available calls = %d, want 0", got)
	}
	if got := calls.groupRates.Load(); got != 0 {
		t.Fatalf("groups/rates calls = %d, want 0", got)
	}
	state, stateErr := costSvc.State(channelRecord.ID)
	if stateErr != nil || state.Status != costing.StateStatusNeedsReview {
		t.Fatalf("cost state = %+v, %v", state, stateErr)
	}
}

func TestRefreshRatesDoesNotRetryUnavailableAdvertisedFactorEvidence(t *testing.T) {
	calls := &advertisedFactorSamplingCalls{}
	srv := newAdvertisedFactorSamplingServer(t, calls, 0)
	defer srv.Close()
	svc, rates, channelRecord, costSvc, _ := newAdvertisedFactorSamplingService(t, srv.URL)
	if err := svc.channels.RecordSyncRetryFailure(channelRecord.ID, "stale upstream failure"); err != nil {
		t.Fatalf("seed stale retry state: %v", err)
	}

	err := svc.RefreshRates(context.Background(), channelRecord)
	if !errors.Is(err, costing.ErrCostUnitEvidenceUnavailable) {
		t.Fatalf("RefreshRates error = %v", err)
	}
	if got := calls.authMe.Load(); got != 1 {
		t.Fatalf("auth/me calls = %d, invalid evidence must not re-authenticate", got)
	}
	if got := calls.checkout.Load(); got != 1 {
		t.Fatalf("checkout calls = %d, invalid evidence must not be retried", got)
	}
	if got := calls.groupsAvailable.Load(); got != 0 {
		t.Fatalf("groups/available calls = %d, want 0", got)
	}
	if got := calls.groupRates.Load(); got != 0 {
		t.Fatalf("groups/rates calls = %d, want 0", got)
	}
	stored, listErr := rates.ListByChannel(channelRecord.ID)
	if listErr != nil || len(stored) != 0 {
		t.Fatalf("rate snapshot was stored without factor evidence: %+v, %v", stored, listErr)
	}
	state, stateErr := costSvc.State(channelRecord.ID)
	if stateErr != nil || state.Status != costing.StateStatusReady {
		t.Fatalf("unavailable evidence changed cost state = %+v, %v", state, stateErr)
	}
	storedChannel, channelErr := svc.channels.FindByID(channelRecord.ID)
	if channelErr != nil || storedChannel.LastError != "" || storedChannel.SyncFailureCount != 0 || storedChannel.NextSyncRetryAt != nil {
		t.Fatalf("accounting gate retained stale sync retry state: %+v, %v", storedChannel, channelErr)
	}
}

func TestRefreshRatesDoesNotRequireAdvertisedEvidenceForConfirmedFundingBasis(t *testing.T) {
	calls := &advertisedFactorSamplingCalls{}
	srv := newAdvertisedFactorSamplingServer(t, calls, 0)
	defer srv.Close()
	svc, rates, channelRecord, costSvc, db := newAdvertisedFactorSamplingService(t, srv.URL)

	result := db.Model(&storage.ChannelCostState{}).
		Where("channel_id = ?", channelRecord.ID).
		Updates(map[string]any{
			"source":                              "confirmed_redeem_code",
			"status":                              costing.StateStatusNeedsReview,
			"last_error":                          costing.LegacyAdvertisedFactorEvidenceReviewReason,
			"observed_provider_factor":            nil,
			"observed_provider_factor_source":     "",
			"observed_provider_factor_sampled_at": nil,
		})
	if result.Error != nil || result.RowsAffected != 1 {
		t.Fatalf("configure confirmed funding basis: err=%v rows=%d", result.Error, result.RowsAffected)
	}
	before, err := costSvc.State(channelRecord.ID)
	if err != nil {
		t.Fatal(err)
	}

	if err := svc.RefreshRates(context.Background(), channelRecord); err != nil {
		t.Fatalf("RefreshRates: %v", err)
	}
	if got := calls.checkout.Load(); got != 0 {
		t.Fatalf("checkout calls = %d, confirmed funding basis must not require advertised evidence", got)
	}
	if got := calls.groupsAvailable.Load(); got != 1 {
		t.Fatalf("groups/available calls = %d, want 1", got)
	}
	if got := calls.groupRates.Load(); got != 1 {
		t.Fatalf("groups/rates calls = %d, want 1", got)
	}
	stored, err := rates.ListByChannel(channelRecord.ID)
	if err != nil || len(stored) != 1 || stored[0].ModelName != "pro" {
		t.Fatalf("stored rate snapshot = %+v, %v", stored, err)
	}
	after, err := costSvc.State(channelRecord.ID)
	if err != nil || after.Status != costing.StateStatusReady || after.LastError != "" || after.Version != before.Version+1 ||
		after.RawBalance != before.RawBalance || after.ActualInventory != before.ActualInventory ||
		after.RawTodayCost != before.RawTodayCost || after.ActualTodayCost != before.ActualTodayCost ||
		after.RawTotalCost != before.RawTotalCost || after.ActualTotalCost != before.ActualTotalCost ||
		after.ObservedProviderFactor != nil || after.ObservedProviderFactorSource != "" || after.ObservedProviderFactorSampledAt != nil {
		t.Fatalf("confirmed basis legacy review recovery changed financial evidence: before=%+v after=%+v err=%v", before, after, err)
	}
}

func TestRefreshRatesFencesConfirmedFundingWhenAdvertisedEvidenceExists(t *testing.T) {
	calls := &advertisedFactorSamplingCalls{}
	srv := newAdvertisedFactorSamplingServer(t, calls, 10, 5)
	defer srv.Close()
	svc, rates, channelRecord, _, db := newAdvertisedFactorSamplingService(t, srv.URL)

	result := db.Model(&storage.ChannelCostState{}).
		Where("channel_id = ?", channelRecord.ID).
		Update("source", "confirmed_redeem_code")
	if result.Error != nil || result.RowsAffected != 1 {
		t.Fatalf("configure confirmed funding basis: err=%v rows=%d", result.Error, result.RowsAffected)
	}

	err := svc.RefreshRates(context.Background(), channelRecord)
	if err == nil || !errors.Is(err, costing.ErrCostUnitChangeRequiresRebase) {
		t.Fatalf("confirmed funding advertised fence error = %v", err)
	}
	if got := calls.checkout.Load(); got != 2 {
		t.Fatalf("checkout calls = %d, want pre/post advertised samples", got)
	}
	stored, listErr := rates.ListByChannel(channelRecord.ID)
	if listErr != nil {
		t.Fatal(listErr)
	}
	if len(stored) != 0 {
		t.Fatalf("rate snapshot committed across advertised factor change: %+v", stored)
	}
}

func TestRefreshRatesRejectsSnapshotWhenAdvertisedFactorChangesAfterRates(t *testing.T) {
	calls := &advertisedFactorSamplingCalls{}
	srv := newAdvertisedFactorSamplingServer(t, calls, 10, 5)
	defer srv.Close()
	svc, rates, channelRecord, costSvc, _ := newAdvertisedFactorSamplingService(t, srv.URL)

	err := svc.RefreshRates(context.Background(), channelRecord)
	if !errors.Is(err, costing.ErrCostUnitChangeRequiresRebase) {
		t.Fatalf("RefreshRates error = %v", err)
	}
	if got := calls.authMe.Load(); got != 1 {
		t.Fatalf("auth/me calls = %d, want one session authentication", got)
	}
	if got := calls.checkout.Load(); got != 2 {
		t.Fatalf("checkout calls = %d, want the bounded before/after pair", got)
	}
	if got := calls.groupsAvailable.Load(); got != 1 {
		t.Fatalf("groups/available calls = %d, want 1", got)
	}
	if got := calls.groupRates.Load(); got != 1 {
		t.Fatalf("groups/rates calls = %d, want 1", got)
	}
	stored, listErr := rates.ListByChannel(channelRecord.ID)
	if listErr != nil || len(stored) != 0 {
		t.Fatalf("mixed-unit rate snapshot was stored: %+v, %v", stored, listErr)
	}
	state, stateErr := costSvc.State(channelRecord.ID)
	if stateErr != nil || state.Status != costing.StateStatusNeedsReview {
		t.Fatalf("cost state = %+v, %v", state, stateErr)
	}
}

func TestRefreshRatesRecoversMatchingLegacyAdvertisedFactorEvidence(t *testing.T) {
	calls := &advertisedFactorSamplingCalls{}
	srv := newAdvertisedFactorSamplingServer(t, calls, 10)
	defer srv.Close()
	svc, rates, channelRecord, costSvc, db := newAdvertisedFactorSamplingService(t, srv.URL)

	if result := db.Model(&storage.ChannelCostState{}).
		Where("channel_id = ?", channelRecord.ID).
		Updates(map[string]any{
			"observed_provider_factor":            nil,
			"observed_provider_factor_source":     "",
			"observed_provider_factor_sampled_at": nil,
			"status":                              costing.StateStatusNeedsReview,
			"last_error":                          costing.LegacyAdvertisedFactorEvidenceReviewReason,
		}); result.Error != nil || result.RowsAffected != 1 {
		t.Fatalf("erase post-migration factor evidence: err=%v rows=%d", result.Error, result.RowsAffected)
	}
	legacyState, err := costSvc.State(channelRecord.ID)
	if err != nil || legacyState.ObservedProviderFactor != nil {
		t.Fatalf("legacy factor evidence was not erased: %+v, %v", legacyState, err)
	}

	if err = svc.RefreshRates(context.Background(), channelRecord); err != nil {
		t.Fatalf("RefreshRates: %v", err)
	}
	if got := calls.checkout.Load(); got != 4 {
		t.Fatalf("checkout calls = %d, want financial recovery pair plus rate fence pair", got)
	}
	if got := calls.groupsAvailable.Load(); got != 1 {
		t.Fatalf("groups/available calls = %d, want 1", got)
	}
	if got := calls.groupRates.Load(); got != 1 {
		t.Fatalf("groups/rates calls = %d, want 1", got)
	}
	stored, listErr := rates.ListByChannel(channelRecord.ID)
	if listErr != nil || len(stored) != 1 {
		t.Fatalf("recovered rate snapshot = %+v, %v", stored, listErr)
	}
	state, stateErr := costSvc.State(channelRecord.ID)
	if stateErr != nil || state.Source != "advertised" || state.Status != costing.StateStatusReady || state.LastError != "" ||
		state.ObservedProviderFactor == nil || *state.ObservedProviderFactor != state.FundingFactor ||
		state.ObservedProviderFactorSource != "advertised" || state.ObservedProviderFactorSampledAt == nil {
		t.Fatalf("legacy state was not recovered from matching evidence: %+v, %v", state, stateErr)
	}
}

func TestRefreshRatesDoesNotRecoverLegacyAdvertisedEvidenceWithManualMultiplier(t *testing.T) {
	calls := &advertisedFactorSamplingCalls{}
	srv := newAdvertisedFactorSamplingServer(t, calls, 10)
	defer srv.Close()
	svc, rates, channelRecord, costSvc, db := newAdvertisedFactorSamplingService(t, srv.URL)

	if result := db.Model(&storage.ChannelCostState{}).
		Where("channel_id = ?", channelRecord.ID).
		Updates(map[string]any{
			"observed_provider_factor":            nil,
			"observed_provider_factor_source":     "",
			"observed_provider_factor_sampled_at": nil,
			"status":                              costing.StateStatusNeedsReview,
			"last_error":                          costing.LegacyAdvertisedFactorEvidenceReviewReason,
		}); result.Error != nil || result.RowsAffected != 1 {
		t.Fatalf("erase post-migration factor evidence: err=%v rows=%d", result.Error, result.RowsAffected)
	}
	multiplier := 10.0
	channelRecord.RechargeMultiplier = &multiplier
	if err := svc.channels.UpdateAdminFields(channelRecord, storage.ChannelAdminFieldRechargeMultiplier); err != nil {
		t.Fatalf("configure manual recharge multiplier: %v", err)
	}

	err := svc.RefreshBalance(context.Background(), channelRecord)
	if !errors.Is(err, costing.ErrCostUnitEvidenceUnavailable) {
		t.Fatalf("RefreshBalance error = %v, want cost-unit evidence gate", err)
	}
	state, stateErr := costSvc.State(channelRecord.ID)
	if stateErr != nil || state.Status != costing.StateStatusNeedsReview ||
		state.LastError != costing.LegacyAdvertisedFactorEvidenceReviewReason ||
		state.ObservedProviderFactor != nil || state.ObservedProviderFactorSource != "" ||
		state.ObservedProviderFactorSampledAt != nil {
		t.Fatalf("manual balance refresh polluted legacy advertised evidence: %+v, %v", state, stateErr)
	}

	err = svc.RefreshRates(context.Background(), channelRecord)
	if !errors.Is(err, storage.ErrRateSnapshotCostUnitReview) {
		t.Fatalf("RefreshRates error = %v, want cost-unit review gate", err)
	}
	if got := calls.checkout.Load(); got != 0 {
		t.Fatalf("checkout calls = %d, manual multiplier must not sample advertised recovery evidence", got)
	}
	if got := calls.groupsAvailable.Load(); got != 1 {
		t.Fatalf("groups/available calls = %d, want 1", got)
	}
	storedRates, listErr := rates.ListByChannel(channelRecord.ID)
	if listErr != nil || len(storedRates) != 0 {
		t.Fatalf("manual-factor rate snapshot bypassed review gate: %+v, %v", storedRates, listErr)
	}
	state, stateErr = costSvc.State(channelRecord.ID)
	if stateErr != nil || state.Status != costing.StateStatusNeedsReview ||
		state.LastError != costing.LegacyAdvertisedFactorEvidenceReviewReason ||
		state.ObservedProviderFactor != nil || state.ObservedProviderFactorSource != "" ||
		state.ObservedProviderFactorSampledAt != nil {
		t.Fatalf("manual multiplier recovered legacy advertised evidence: %+v, %v", state, stateErr)
	}
}

func TestRefreshBalanceRejectsChangedLegacyAdvertisedFactorEvidence(t *testing.T) {
	calls := &advertisedFactorSamplingCalls{}
	srv := newAdvertisedFactorSamplingServer(t, calls, 5)
	defer srv.Close()
	svc, _, channelRecord, costSvc, db := newAdvertisedFactorSamplingService(t, srv.URL)

	if result := db.Model(&storage.ChannelCostState{}).
		Where("channel_id = ?", channelRecord.ID).
		Updates(map[string]any{
			"observed_provider_factor":            nil,
			"observed_provider_factor_source":     "",
			"observed_provider_factor_sampled_at": nil,
			"status":                              costing.StateStatusNeedsReview,
			"last_error":                          costing.LegacyAdvertisedFactorEvidenceReviewReason,
		}); result.Error != nil || result.RowsAffected != 1 {
		t.Fatalf("erase post-migration factor evidence: err=%v rows=%d", result.Error, result.RowsAffected)
	}
	before, err := costSvc.State(channelRecord.ID)
	if err != nil {
		t.Fatal(err)
	}

	err = svc.RefreshBalance(context.Background(), channelRecord)
	if !errors.Is(err, costing.ErrCostUnitEvidenceUnavailable) {
		t.Fatalf("RefreshBalance error = %v, want unavailable changed legacy evidence", err)
	}
	after, stateErr := costSvc.State(channelRecord.ID)
	if stateErr != nil || after.Version != before.Version || after.FundingFactor != before.FundingFactor ||
		after.Status != costing.StateStatusNeedsReview || after.ObservedProviderFactor != nil {
		t.Fatalf("changed legacy factor rewrote the financial basis: before=%+v after=%+v err=%v", before, after, stateErr)
	}
}

func TestRefreshRatesReauthenticatesRefreshedSub2APISession(t *testing.T) {
	calls := &advertisedFactorSamplingCalls{}
	srv := newAdvertisedFactorSamplingServer(t, calls, 10)
	defer srv.Close()
	svc, rates, channelRecord, _, _ := newAdvertisedFactorSamplingService(t, srv.URL)

	// Bind the original token before replacing it with a near-expiry stored
	// session. The refresh endpoint returns a session without identity facts;
	// the rates guard must authenticate that refreshed token once.
	if _, _, _, err := svc.prepare(context.Background(), channelRecord); err != nil {
		t.Fatalf("bind initial token identity: %v", err)
	}
	channelRecord.CredentialMode = storage.CredentialModePassword
	channelRecord.PasswordCipher = mustEncrypt(t, svc.channelSvc.Cipher, "password")
	if err := svc.channels.UpdateAdminFields(channelRecord,
		storage.ChannelAdminFieldPasswordCipher,
		storage.ChannelAdminFieldCredentialMode,
	); err != nil {
		t.Fatalf("switch to stored password session: %v", err)
	}
	if _, err := svc.channels.ObserveUpstreamAccountIdentity(storage.UpstreamAccountIdentityObservation{
		ChannelID: channelRecord.ID, ExpectedIdentityRevision: channelRecord.UpstreamAccountIdentityRevision,
		ExpectedAdminRevision: channelRecord.AdminRevision,
		Provider:              storage.ChannelTypeSub2API, APIOrigin: channelRecord.SiteURL,
		AccountID: "4242", Scope: storage.UpstreamConcurrencyScopeAccount,
		Source: "sub2api.auth.me.concurrency", ObservedAt: time.Now().UTC().Add(-time.Minute),
	}); err != nil {
		t.Fatalf("bind refreshed-session account fence: %v", err)
	}
	expiresAt := time.Now().UTC().Add(time.Minute)
	if err := svc.channelSvc.AuthSessions.Upsert(&storage.AuthSession{
		ChannelID:          channelRecord.ID,
		AccessTokenCipher:  mustEncrypt(t, svc.channelSvc.Cipher, "old-token"),
		RefreshTokenCipher: mustEncrypt(t, svc.channelSvc.Cipher, "refresh-token"),
		CookieCipher:       mustEncrypt(t, svc.channelSvc.Cipher, ""),
		CSRFTokenCipher:    mustEncrypt(t, svc.channelSvc.Cipher, ""),
		ExpiresAt:          &expiresAt,
	}); err != nil {
		t.Fatalf("seed near-expiry stored session: %v", err)
	}

	if err := svc.RefreshRates(context.Background(), channelRecord); err != nil {
		t.Fatalf("RefreshRates: %v", err)
	}
	if got := calls.authMe.Load(); got != 2 {
		t.Fatalf("auth/me calls = %d, want initial authentication plus refreshed-session proof", got)
	}
	if got := calls.checkout.Load(); got != 2 {
		t.Fatalf("checkout calls = %d, want before/after factor fence", got)
	}
	stored, err := rates.ListByChannel(channelRecord.ID)
	if err != nil || len(stored) != 1 {
		t.Fatalf("stored rates = %+v, %v", stored, err)
	}
}

func TestRefreshRatesRejectsRefreshedSessionBoundToDifferentAccount(t *testing.T) {
	calls := &advertisedFactorSamplingCalls{}
	srv := newAdvertisedFactorSamplingServerWithRefreshedAccount(t, calls, "9999", 10)
	defer srv.Close()
	svc, rates, channelRecord, _, _ := newAdvertisedFactorSamplingService(t, srv.URL)
	if _, _, _, err := svc.prepare(context.Background(), channelRecord); err != nil {
		t.Fatalf("bind initial token identity: %v", err)
	}
	channelRecord.CredentialMode = storage.CredentialModePassword
	channelRecord.PasswordCipher = mustEncrypt(t, svc.channelSvc.Cipher, "password")
	if err := svc.channels.UpdateAdminFields(channelRecord,
		storage.ChannelAdminFieldPasswordCipher,
		storage.ChannelAdminFieldCredentialMode,
	); err != nil {
		t.Fatalf("switch to stored password session: %v", err)
	}
	if _, err := svc.channels.ObserveUpstreamAccountIdentity(storage.UpstreamAccountIdentityObservation{
		ChannelID: channelRecord.ID, ExpectedIdentityRevision: channelRecord.UpstreamAccountIdentityRevision,
		ExpectedAdminRevision: channelRecord.AdminRevision,
		Provider:              storage.ChannelTypeSub2API, APIOrigin: channelRecord.SiteURL,
		AccountID: "4242", Scope: storage.UpstreamConcurrencyScopeAccount,
		Source: "sub2api.auth.me.concurrency", ObservedAt: time.Now().UTC().Add(-time.Minute),
	}); err != nil {
		t.Fatalf("bind refreshed-session account fence: %v", err)
	}
	expiresAt := time.Now().UTC().Add(time.Minute)
	if err := svc.channelSvc.AuthSessions.Upsert(&storage.AuthSession{
		ChannelID:          channelRecord.ID,
		AccessTokenCipher:  mustEncrypt(t, svc.channelSvc.Cipher, "old-token"),
		RefreshTokenCipher: mustEncrypt(t, svc.channelSvc.Cipher, "refresh-token"),
		CookieCipher:       mustEncrypt(t, svc.channelSvc.Cipher, ""),
		CSRFTokenCipher:    mustEncrypt(t, svc.channelSvc.Cipher, ""),
		ExpiresAt:          &expiresAt,
	}); err != nil {
		t.Fatalf("seed near-expiry stored session: %v", err)
	}

	err := svc.RefreshRates(context.Background(), channelRecord)
	if !errors.Is(err, costing.ErrCostUnitEvidenceUnavailable) {
		t.Fatalf("RefreshRates error = %v, want account-switch evidence gate", err)
	}
	if got := calls.authMe.Load(); got != 2 {
		t.Fatalf("auth/me calls = %d, want initial binding plus refreshed-session proof", got)
	}
	if got := calls.checkout.Load(); got != 0 {
		t.Fatalf("checkout calls = %d, stale refreshed session must stop before factor sampling", got)
	}
	if got := calls.groupsAvailable.Load(); got != 0 {
		t.Fatalf("groups/available calls = %d, want 0", got)
	}
	if got := calls.groupRates.Load(); got != 0 {
		t.Fatalf("groups/rates calls = %d, want 0", got)
	}
	stored, err := rates.ListByChannel(channelRecord.ID)
	if err != nil || len(stored) != 0 {
		t.Fatalf("stale refreshed session allowed rate persistence: %+v, %v", stored, err)
	}
}

func TestRateSnapshotBatchRejectsIdentityRevisionAtSameEpoch(t *testing.T) {
	calls := &advertisedFactorSamplingCalls{}
	srv := newAdvertisedFactorSamplingServer(t, calls, 10)
	defer srv.Close()
	_, rates, channelRecord, _, db := newAdvertisedFactorSamplingService(t, srv.URL)

	updated := db.Model(&storage.Channel{}).
		Where("id = ? AND upstream_account_identity_revision = ?", channelRecord.ID, channelRecord.UpstreamAccountIdentityRevision).
		Update("upstream_account_identity_revision", channelRecord.UpstreamAccountIdentityRevision+1)
	if updated.Error != nil || updated.RowsAffected != 1 {
		t.Fatalf("advance test identity revision: err=%v rows=%d", updated.Error, updated.RowsAffected)
	}
	_, err := rates.ReplaceChannelSnapshotForCostUnitEpoch(storage.RateSnapshotBatchInput{
		ChannelID: channelRecord.ID, ExpectedIdentityRevision: channelRecord.UpstreamAccountIdentityRevision,
		ExpectedCostUnitEpoch: channelRecord.CostUnitEpoch, ObservedAt: time.Now().UTC(),
		Items: []storage.RateSnapshot{{ChannelID: channelRecord.ID, ModelName: "pro", Ratio: 0.06}},
	})
	if !errors.Is(err, storage.ErrStaleChannelAccountIdentityRevision) {
		t.Fatalf("rate batch error = %v, want identity revision conflict", err)
	}
	stored, err := rates.ListByChannel(channelRecord.ID)
	if err != nil || len(stored) != 0 {
		t.Fatalf("identity-mismatched rate batch was stored: %+v, %v", stored, err)
	}
}

func TestRefreshBalanceRejectsDefaultFactorWhenCheckoutUnavailable(t *testing.T) {
	calls := &advertisedFactorSamplingCalls{}
	srv := newAdvertisedFactorSamplingServer(t, calls, 0)
	defer srv.Close()
	svc, _, channelRecord, costSvc, db := newAdvertisedFactorSamplingService(t, srv.URL)
	reset := db.Model(&storage.ChannelCostState{}).Where("channel_id = ?", channelRecord.ID).Updates(map[string]any{
		"raw_balance": 0, "actual_inventory": 0, "funding_factor": 1,
		"raw_today_cost": 0, "actual_today_cost": 0, "raw_total_cost": 0, "actual_total_cost": 0,
		"balance_sampled_at": nil, "cost_sampled_at": nil,
		"observed_provider_factor": nil, "observed_provider_factor_source": "", "observed_provider_factor_sampled_at": nil,
		"source": "default", "status": costing.StateStatusReady, "last_error": "",
	})
	if reset.Error != nil || reset.RowsAffected != 1 {
		t.Fatalf("seed fresh default state: err=%v rows=%d", reset.Error, reset.RowsAffected)
	}
	before, err := costSvc.State(channelRecord.ID)
	if err != nil {
		t.Fatal(err)
	}
	var balanceSnapshotsBefore, costSnapshotsBefore int64
	if err := db.Model(&storage.BalanceSnapshot{}).Where("channel_id = ?", channelRecord.ID).Count(&balanceSnapshotsBefore).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&storage.CostSnapshot{}).Where("channel_id = ?", channelRecord.ID).Count(&costSnapshotsBefore).Error; err != nil {
		t.Fatal(err)
	}

	err = svc.RefreshBalance(context.Background(), channelRecord)
	if !errors.Is(err, costing.ErrCostUnitEvidenceUnavailable) {
		t.Fatalf("RefreshBalance error = %v, want unavailable advertised factor", err)
	}
	if got := calls.checkout.Load(); got != 2 {
		t.Fatalf("checkout calls = %d, want one compatibility read for balance and costs", got)
	}
	after, stateErr := costSvc.State(channelRecord.ID)
	if stateErr != nil || after.Version != before.Version || after.RawBalance != before.RawBalance ||
		after.RawTodayCost != before.RawTodayCost || after.RawTotalCost != before.RawTotalCost ||
		after.FundingFactor != before.FundingFactor || after.Source != before.Source {
		t.Fatalf("default factor changed financial state: before=%+v after=%+v err=%v", before, after, stateErr)
	}
	var balanceSnapshotsAfter, costSnapshotsAfter int64
	if err := db.Model(&storage.BalanceSnapshot{}).Where("channel_id = ?", channelRecord.ID).Count(&balanceSnapshotsAfter).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&storage.CostSnapshot{}).Where("channel_id = ?", channelRecord.ID).Count(&costSnapshotsAfter).Error; err != nil {
		t.Fatal(err)
	}
	if balanceSnapshotsAfter != balanceSnapshotsBefore || costSnapshotsAfter != costSnapshotsBefore {
		t.Fatalf("default factor appended snapshots: balance %d->%d cost %d->%d",
			balanceSnapshotsBefore, balanceSnapshotsAfter, costSnapshotsBefore, costSnapshotsAfter)
	}
}

func TestRefreshRatesBootstrapsFreshAdvertisedFactorBeforeRates(t *testing.T) {
	calls := &advertisedFactorSamplingCalls{}
	srv := newAdvertisedFactorSamplingServer(t, calls, 10)
	defer srv.Close()
	svc, rates, channelRecord, costSvc, db := newAdvertisedFactorSamplingService(t, srv.URL)
	reset := db.Model(&storage.ChannelCostState{}).Where("channel_id = ?", channelRecord.ID).Updates(map[string]any{
		"raw_balance": 0, "actual_inventory": 0, "funding_factor": 1,
		"raw_today_cost": 0, "actual_today_cost": 0, "raw_total_cost": 0, "actual_total_cost": 0,
		"balance_sampled_at": nil, "cost_sampled_at": nil,
		"observed_provider_factor": nil, "observed_provider_factor_source": "", "observed_provider_factor_sampled_at": nil,
		"source": "default", "status": costing.StateStatusReady, "last_error": "",
	})
	if reset.Error != nil || reset.RowsAffected != 1 {
		t.Fatalf("seed fresh default state: err=%v rows=%d", reset.Error, reset.RowsAffected)
	}

	if err := svc.RefreshRates(context.Background(), channelRecord); err != nil {
		t.Fatalf("RefreshRates: %v", err)
	}
	if got := calls.authMe.Load(); got != 3 {
		t.Fatalf("auth/me calls = %d, want financial bootstrap pair plus rate authentication", got)
	}
	if got := calls.checkout.Load(); got != 4 {
		t.Fatalf("checkout calls = %d, want financial bootstrap pair plus rate fence pair", got)
	}
	if got := calls.groupsAvailable.Load(); got != 1 {
		t.Fatalf("groups/available calls = %d, want 1", got)
	}
	if got := calls.groupRates.Load(); got != 1 {
		t.Fatalf("groups/rates calls = %d, want 1", got)
	}
	state, stateErr := costSvc.State(channelRecord.ID)
	if stateErr != nil || state.ObservedProviderFactor == nil || *state.ObservedProviderFactor != 0.1 ||
		state.ObservedProviderFactorSource != "advertised" || state.FundingFactor != 0.1 || state.Source != "advertised" {
		t.Fatalf("bootstrapped cost state = %+v, %v", state, stateErr)
	}
	stored, err := rates.ListByChannel(channelRecord.ID)
	if err != nil || len(stored) != 1 || stored[0].CostUnitEpoch != state.CostUnitEpoch {
		t.Fatalf("bootstrapped rates = %+v, %v", stored, err)
	}
}

func TestRefreshBalanceTreatsConcurrentCostStateAdvanceAsAccountingGate(t *testing.T) {
	var database atomic.Pointer[gorm.DB]
	var channelID atomic.Uint64
	var advanced atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/auth/me":
			_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"id":4242,"balance":100}}`))
		case "/api/v1/payment/checkout-info":
			_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"balance_recharge_multiplier":10}}`))
		case "/api/v1/usage/dashboard/stats":
			db := database.Load()
			id := uint(channelID.Load())
			if db == nil || id == 0 {
				t.Error("test database is unavailable")
				http.Error(w, "test database is unavailable", http.StatusInternalServerError)
				return
			}
			if advanced.CompareAndSwap(false, true) {
				if err := db.Model(&storage.ChannelCostState{}).
					Where("channel_id = ?", id).
					Update("version", gorm.Expr("version + 1")).Error; err != nil {
					t.Errorf("advance cost state during sample: %v", err)
				}
			}
			_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"today_actual_cost":2,"total_actual_cost":30}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	svc, _, channelRecord, costSvc, testDB := newAdvertisedFactorSamplingService(t, srv.URL)
	database.Store(testDB)
	channelID.Store(uint64(channelRecord.ID))
	if err := svc.channels.RecordSyncRetryFailure(channelRecord.ID, "stale upstream failure"); err != nil {
		t.Fatalf("seed stale retry state: %v", err)
	}
	before, err := costSvc.State(channelRecord.ID)
	if err != nil {
		t.Fatal(err)
	}

	err = svc.RefreshBalance(context.Background(), channelRecord)
	if !errors.Is(err, storage.ErrCostStateConflict) {
		t.Fatalf("RefreshBalance error = %v, want cost state CAS conflict", err)
	}
	after, stateErr := costSvc.State(channelRecord.ID)
	if stateErr != nil || after.Version != before.Version+1 || after.RawBalance != before.RawBalance ||
		after.RawTodayCost != before.RawTodayCost || after.RawTotalCost != before.RawTotalCost {
		t.Fatalf("stale observation changed financial state: before=%+v after=%+v err=%v", before, after, stateErr)
	}
	storedChannel, channelErr := svc.channels.FindByID(channelRecord.ID)
	if channelErr != nil || storedChannel.LastError != "" || storedChannel.SyncFailureCount != 0 || storedChannel.NextSyncRetryAt != nil {
		t.Fatalf("cost state conflict polluted sync retry state: %+v, %v", storedChannel, channelErr)
	}
}

func newAdvertisedFactorSamplingServer(t *testing.T, calls *advertisedFactorSamplingCalls, multipliers ...float64) *httptest.Server {
	return newAdvertisedFactorSamplingServerWithRefreshedAccount(t, calls, "", multipliers...)
}

func newAdvertisedFactorSamplingServerWithRefreshedAccount(t *testing.T, calls *advertisedFactorSamplingCalls, refreshedAccount string, multipliers ...float64) *httptest.Server {
	t.Helper()
	if len(multipliers) == 0 {
		t.Fatal("at least one advertised multiplier is required")
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/auth/me":
			calls.authMe.Add(1)
			accountID := "4242"
			if refreshedAccount != "" && r.Header.Get("Authorization") == "Bearer refreshed-token" {
				accountID = refreshedAccount
			}
			_, _ = fmt.Fprintf(w, `{"code":0,"message":"","data":{"id":%s,"balance":100}}`, accountID)
		case "/api/v1/auth/refresh":
			_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"access_token":"refreshed-token","refresh_token":"refreshed-refresh","expires_in":3600}}`))
		case "/api/v1/usage/dashboard/stats":
			_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"today_actual_cost":2,"total_actual_cost":30}}`))
		case "/api/v1/payment/checkout-info":
			call := calls.checkout.Add(1)
			index := min(int(call)-1, len(multipliers)-1)
			multiplier := multipliers[index]
			_, _ = fmt.Fprintf(w, `{"code":0,"message":"","data":{"balance_recharge_multiplier":%g}}`, multiplier)
		case "/api/v1/groups/available":
			calls.groupsAvailable.Add(1)
			_, _ = w.Write([]byte(`{"code":0,"message":"","data":[{"id":7,"name":"pro","rate_multiplier":0.06}]}`))
		case "/api/v1/groups/rates":
			calls.groupRates.Add(1)
			_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"7":0.06}}`))
		default:
			http.NotFound(w, r)
		}
	}))
}

func newAdvertisedFactorSamplingService(t *testing.T, upstreamURL string) (*Service, *storage.Rates, *storage.Channel, *costing.Service, *gorm.DB) {
	t.Helper()
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	authSessions := storage.NewAuthSessions(db)
	captchas := storage.NewCaptchas(db)
	announcements := storage.NewUpstreamAnnouncements(db)
	rates := storage.NewRates(db)
	monitorLogs := storage.NewMonitorLogs(db)
	notifications := storage.NewNotifications(db)
	cipher, err := crypto.NewCipher("secret")
	if err != nil {
		t.Fatal(err)
	}
	channelRecord := &storage.Channel{
		Name: "advertised-factor-sampling", Type: storage.ChannelTypeSub2API,
		SiteURL: upstreamURL, APIURL: upstreamURL, Username: "u",
		PasswordCipher:         mustEncrypt(t, cipher, `{"access_token":"token"}`),
		CredentialMode:         storage.CredentialModeToken,
		RechargeMultiplierMode: connector.RechargeMultiplierModeDivide,
		MonitorEnabled:         true,
		IgnoreAnnouncements:    true,
	}
	if err := channels.Create(channelRecord); err != nil {
		t.Fatal(err)
	}
	channelSvc := channel.NewService(channels, authSessions, captchas, rates, monitorLogs, cipher)
	dispatcher := notify.NewDispatcher(notifications, cipher, slog.New(slog.NewTextHandler(io.Discard, nil)), notify.Policy{SendMaxAttempts: 1})
	svc := NewService(channels, announcements, rates, monitorLogs, channelSvc, dispatcher, slog.New(slog.NewTextHandler(io.Discard, nil)))
	costSvc := costing.NewService(storage.NewCosting(db))
	svc.SetCosting(costSvc)

	sampledAt := time.Now().UTC().Add(-time.Minute)
	state, candidate, err := costSvc.RecordObservation(costing.ObservationInput{
		Balance: costing.BalanceInput{
			ChannelID: channelRecord.ID, ExpectedVersion: 1,
			RawBalance: 100, ObservedFactor: 0.1, Source: "advertised", SampledAt: sampledAt,
		},
		Costs: costing.CostInput{
			ChannelID: channelRecord.ID, ExpectedVersion: 1,
			RawTodayCost: 1, RawTotalCost: 10, SampledAt: sampledAt,
		},
	})
	if err != nil || candidate != nil || state.ObservedProviderFactor == nil || *state.ObservedProviderFactor != 0.1 {
		t.Fatalf("seed advertised cost state = %+v, %+v, %v", state, candidate, err)
	}
	return svc, rates, channelRecord, costSvc, db
}
