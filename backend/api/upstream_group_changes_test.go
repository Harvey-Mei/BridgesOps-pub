package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	"github.com/Harvey-Mei/BridgeOps/backend/costing"
	"github.com/Harvey-Mei/BridgeOps/backend/gateway"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/gin-gonic/gin"
)

type upstreamGroupChangeNoopKeyManager struct{}

type upstreamGroupChangePreflightMonitor struct {
	calls    []uint
	failures map[uint]error
	refresh  func(*storage.Channel) error
}

type upstreamGroupChangeFastPreflightMonitor struct {
	fastCalls    int
	regularCalls int
}

func (m *upstreamGroupChangeFastPreflightMonitor) RefreshBalance(context.Context, *storage.Channel) error {
	return nil
}

func (m *upstreamGroupChangeFastPreflightMonitor) RefreshRates(context.Context, *storage.Channel) error {
	m.regularCalls++
	return nil
}

func (m *upstreamGroupChangeFastPreflightMonitor) RefreshUpstreamGroupFacts(context.Context, *storage.Channel) error {
	m.regularCalls++
	return nil
}

func (m *upstreamGroupChangeFastPreflightMonitor) RefreshUpstreamGroupFactsForAssignment(context.Context, *storage.Channel) error {
	m.fastCalls++
	return nil
}

func (m *upstreamGroupChangeFastPreflightMonitor) CheckSubscriptionUsageAlerts(context.Context, *storage.Channel) error {
	return nil
}

func (m *upstreamGroupChangePreflightMonitor) RefreshBalance(context.Context, *storage.Channel) error {
	return nil
}

func (m *upstreamGroupChangePreflightMonitor) RefreshRates(_ context.Context, channel *storage.Channel) error {
	m.calls = append(m.calls, channel.ID)
	if m.refresh != nil {
		return m.refresh(channel)
	}
	return m.failures[channel.ID]
}

func (m *upstreamGroupChangePreflightMonitor) RefreshUpstreamGroupFacts(ctx context.Context, channel *storage.Channel) error {
	return m.RefreshRates(ctx, channel)
}

func (m *upstreamGroupChangePreflightMonitor) CheckSubscriptionUsageAlerts(context.Context, *storage.Channel) error {
	return nil
}

func TestGatewayAssignmentPreflightUsesLightweightMonitorRefresh(t *testing.T) {
	monitor := &upstreamGroupChangeFastPreflightMonitor{}
	channel := &storage.Channel{ID: 7}
	if err := refreshUpstreamGroupFactsForAssignment(context.Background(), monitor, channel); err != nil {
		t.Fatalf("lightweight refresh = %v", err)
	}
	if monitor.fastCalls != 1 || monitor.regularCalls != 0 {
		t.Fatalf("refresh calls = fast:%d regular:%d", monitor.fastCalls, monitor.regularCalls)
	}
}

func TestGatewayAssignmentPreflightContextIsBounded(t *testing.T) {
	ctx, cancel := gatewayUpstreamGroupAssignmentPreflightContext(context.Background())
	defer cancel()
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("assignment preflight context has no deadline")
	}
	remaining := time.Until(deadline)
	if remaining <= 0 || remaining > gatewayUpstreamGroupAssignmentPreflightTimeout {
		t.Fatalf("assignment preflight deadline remaining = %v, want <= %v", remaining, gatewayUpstreamGroupAssignmentPreflightTimeout)
	}
}

func TestGatewayUpstreamGroupAssignmentPreflightFailsClosedWithoutMonitor(t *testing.T) {
	ready, failures := refreshGatewayUpstreamGroupAssignmentChannels(context.Background(), &Deps{}, []uint{1, 1, 2})
	if len(ready) != 0 || len(failures) != 2 {
		t.Fatalf("preflight result = ready:%v failures:%v", ready, failures)
	}
	for _, id := range []uint{1, 2} {
		if !strings.Contains(failures[id], "实时刷新服务不可用") {
			t.Fatalf("failure %d = %q", id, failures[id])
		}
	}
}

func TestGatewayUpstreamGroupAssignmentPreflightCapturesPostRefreshIdentityRevision(t *testing.T) {
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	channel := &storage.Channel{
		Name: "preflight identity revision", Type: storage.ChannelTypeNewAPI,
		SiteURL: "https://preflight-identity-revision.invalid", MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "promo", Ratio: 1, LastSeenAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "promo", UpstreamKeyID: 7, UpstreamKeyName: "managed-promo",
		OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated, APIKeyCipher: "cipher", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "promo", "added", 7, "managed-promo", time.Now()); err != nil {
		t.Fatal(err)
	}
	changes, err := store.ListUpstreamGroupChanges(true)
	if err != nil || len(changes) != 1 {
		t.Fatalf("pending changes = %+v, err=%v", changes, err)
	}
	expectedRevision := channel.UpstreamAccountIdentityRevision + 1
	monitor := &upstreamGroupChangePreflightMonitor{
		failures: map[uint]error{},
		refresh: func(current *storage.Channel) error {
			return db.Model(&storage.Channel{}).Where("id = ?", current.ID).
				Update("upstream_account_identity_revision", expectedRevision).Error
		},
	}
	service := gateway.NewService(channels, rates, store, nil, nil, nil)
	ready, failures, revisions := refreshGatewayUpstreamGroupAssignmentChannelsWithRevisions(
		context.Background(),
		&Deps{Channels: channels, Gateway: service, Monitor: monitor},
		[]uint{changes[0].ID},
	)
	if len(ready) != 1 || ready[0] != changes[0].ID || len(failures) != 0 {
		t.Fatalf("preflight result = ready:%v failures:%v", ready, failures)
	}
	if revisions[channel.ID] != expectedRevision {
		t.Fatalf("captured identity revision = %d, want %d", revisions[channel.ID], expectedRevision)
	}
}

func (upstreamGroupChangeNoopKeyManager) ListAPIKeys(context.Context, uint, connector.APIKeyQuery) (*connector.APIKeyPage, error) {
	return &connector.APIKeyPage{}, nil
}

func (upstreamGroupChangeNoopKeyManager) ListAPIKeyGroups(context.Context, uint) ([]connector.APIKeyGroup, error) {
	return nil, nil
}

func (upstreamGroupChangeNoopKeyManager) CreateAPIKey(context.Context, uint, connector.APIKeyCreateRequest) (*connector.APIKey, error) {
	return nil, nil
}

func (upstreamGroupChangeNoopKeyManager) RevealAPIKey(context.Context, uint, int64) (string, error) {
	return "", nil
}

func (upstreamGroupChangeNoopKeyManager) PrepareManagedAPIKeyDeletion(context.Context, uint, connector.APIKey, uint64) (func() error, error) {
	return func() error { return nil }, nil
}

func TestGatewayUpstreamGroupChangesUseCanonicalRates(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	channel := &storage.Channel{
		Name: "canonical rates", Type: storage.ChannelTypeNewAPI,
		SiteURL: "https://canonical-rates.invalid", MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "promo", Ratio: 2, CompletionRatio: 4}); err != nil {
		t.Fatalf("upsert rate: %v", err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "same-as-input", Ratio: 3}); err != nil {
		t.Fatalf("upsert fallback rate: %v", err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "promo", "added", 0, "", time.Now()); err != nil {
		t.Fatalf("record rated change: %v", err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "missing", "added", 0, "", time.Now()); err != nil {
		t.Fatalf("record missing-rate change: %v", err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "same-as-input", "added", 0, "", time.Now()); err != nil {
		t.Fatalf("record fallback-rate change: %v", err)
	}

	costStore := storage.NewCosting(db)
	state, err := costStore.FindState(channel.ID)
	if err != nil {
		t.Fatalf("find cost state: %v", err)
	}
	version := state.Version
	state.FundingFactor = 0.25
	if err := costStore.UpdateState(state, version); err != nil {
		t.Fatalf("update cost state: %v", err)
	}
	if err := db.Model(&storage.Channel{}).Where("id = ?", channel.ID).Update("funding_factor", 0.75).Error; err != nil {
		t.Fatalf("make channel projection stale: %v", err)
	}

	service := gateway.NewService(channels, rates, store, nil, nil, nil)
	service.SetCosting(costing.NewService(costStore))
	router := gin.New()
	registerGateway(router.Group("/api"), &Deps{Gateway: service})
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/gateway/upstream-group-changes", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
	}

	var payload struct {
		Data []struct {
			GroupName       string   `json:"group_name"`
			Ratio           *float64 `json:"ratio"`
			CompletionRatio *float64 `json:"completion_ratio"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	items := make(map[string]struct {
		Ratio           *float64
		CompletionRatio *float64
	}, len(payload.Data))
	for _, item := range payload.Data {
		items[item.GroupName] = struct {
			Ratio           *float64
			CompletionRatio *float64
		}{Ratio: item.Ratio, CompletionRatio: item.CompletionRatio}
	}
	if item := items["promo"]; item.Ratio == nil || *item.Ratio != 0.5 || item.CompletionRatio == nil || *item.CompletionRatio != 1 {
		t.Fatalf("canonical promo rates = %+v", item)
	}
	if item := items["missing"]; item.Ratio != nil || item.CompletionRatio != nil {
		t.Fatalf("missing rates = %+v", item)
	}
	if item := items["same-as-input"]; item.Ratio == nil || *item.Ratio != 0.75 || item.CompletionRatio == nil || *item.CompletionRatio != 0.75 {
		t.Fatalf("fallback output rate = %+v", item)
	}
}

func TestGatewayUpstreamGroupChangeManualReviewResumeRouteRejectsMissingIntent(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	service := gateway.NewService(
		storage.NewChannels(db), storage.NewRates(db), storage.NewGateway(db), nil,
		upstreamGroupChangeNoopKeyManager{}, nil,
	)
	router := gin.New()
	registerGateway(router.Group("/api"), &Deps{Gateway: service})

	response := httptest.NewRecorder()
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/gateway/upstream-group-changes/999999/manual-review/resume",
		strings.NewReader(`{"resolution":"remote_revalidated"}`),
	)
	request.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
	}
}

func TestGatewayUpstreamGroupChangeMutationsUseAuthenticatedActor(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	channel := &storage.Channel{Name: "upstream actor", Type: storage.ChannelTypeNewAPI, SiteURL: "https://upstream-actor.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	for _, groupName := range []string{"seed", "assigned", "ignored"} {
		if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: groupName, Ratio: 1, LastSeenAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "seed", OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceAdopted,
		APIKeyCipher: "seed", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	target := &storage.GatewayCandidateGroup{Name: "upstream actor target"}
	if err := store.SaveCandidateGroup(target, []storage.GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "seed"}}); err != nil {
		t.Fatal(err)
	}
	for index, groupName := range []string{"assigned", "ignored"} {
		keyID := int64(index + 10)
		keyName := "managed-" + groupName
		if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
			ChannelID: channel.ID, GroupName: groupName, UpstreamKeyID: keyID, UpstreamKeyName: keyName,
			OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated, APIKeyCipher: "cipher", Enabled: true,
		}); err != nil {
			t.Fatal(err)
		}
		if err := store.RecordUpstreamGroupChange(channel.ID, groupName, "added", keyID, keyName, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	changes, err := store.ListUpstreamGroupChanges(true)
	if err != nil {
		t.Fatal(err)
	}
	changeByName := make(map[string]storage.GatewayUpstreamGroupChange, len(changes))
	for _, change := range changes {
		changeByName[change.GroupName] = change
	}

	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("authSubject", "operator:upstream-groups")
		c.Next()
	})
	preflight := &upstreamGroupChangePreflightMonitor{failures: map[uint]error{}}
	registerGateway(router.Group("/api"), &Deps{
		Channels: channels,
		Gateway:  gateway.NewService(channels, rates, store, nil, nil, nil),
		Monitor:  preflight,
	})
	assigned := changeByName["assigned"]
	assignRequest := httptest.NewRequest(http.MethodPost,
		fmt.Sprintf("/api/gateway/upstream-group-changes/%d/resolve", assigned.ID),
		strings.NewReader(fmt.Sprintf(`{"candidate_group_ids":[%d]}`, target.ID)))
	assignRequest.Header.Set("Content-Type", "application/json")
	assignResponse := httptest.NewRecorder()
	router.ServeHTTP(assignResponse, assignRequest)
	if assignResponse.Code != http.StatusOK {
		t.Fatalf("assign change = %d %s", assignResponse.Code, assignResponse.Body.String())
	}
	if len(preflight.calls) != 1 || preflight.calls[0] != channel.ID {
		t.Fatalf("assignment preflight calls = %v", preflight.calls)
	}
	ignored := changeByName["ignored"]
	ignoreRequest := httptest.NewRequest(http.MethodPost, "/api/gateway/upstream-group-changes/resolve-batch",
		strings.NewReader(fmt.Sprintf(`{"ids":[%d],"ignore":true}`, ignored.ID)))
	ignoreRequest.Header.Set("Content-Type", "application/json")
	ignoreResponse := httptest.NewRecorder()
	router.ServeHTTP(ignoreResponse, ignoreRequest)
	if ignoreResponse.Code != http.StatusOK || !strings.Contains(ignoreResponse.Body.String(), `"resolved":1`) {
		t.Fatalf("ignore batch = %d %s", ignoreResponse.Code, ignoreResponse.Body.String())
	}
	for _, change := range []storage.GatewayUpstreamGroupChange{assigned, ignored} {
		var override storage.GatewayUpstreamGroupAssignmentOverride
		if err := db.Where("upstream_group_id = ?", *change.UpstreamGroupID).First(&override).Error; err != nil {
			t.Fatal(err)
		}
		if override.CreatedBy != "operator:upstream-groups" || change.PresenceGeneration == nil || override.PresenceGeneration != *change.PresenceGeneration {
			t.Fatalf("change override actor/generation = %+v change=%+v", override, change)
		}
		var audit storage.GatewayUpstreamGroupAssignmentAudit
		if err := db.Where("change_id = ?", change.ID).Order("id DESC").First(&audit).Error; err != nil {
			t.Fatal(err)
		}
		if audit.Actor != "operator:upstream-groups" {
			t.Fatalf("change %d audit actor = %q", change.ID, audit.Actor)
		}
	}
}

func TestGatewayUpstreamGroupChangeBatchAssignmentRefreshesEachChannelAndContinuesAfterFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	createdChannels := make([]*storage.Channel, 0, 2)
	for _, name := range []string{"fresh assignment one", "fresh assignment two"} {
		channel := &storage.Channel{Name: name, Type: storage.ChannelTypeNewAPI, SiteURL: "https://" + strings.ReplaceAll(name, " ", "-") + ".invalid", MonitorEnabled: true}
		if err := channels.Create(channel); err != nil {
			t.Fatal(err)
		}
		createdChannels = append(createdChannels, channel)
	}
	first, second := createdChannels[0], createdChannels[1]
	for _, seed := range []struct {
		channel *storage.Channel
		group   string
	}{
		{channel: first, group: "seed"},
		{channel: first, group: "first-added"},
		{channel: second, group: "second-added"},
	} {
		if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: seed.channel.ID, ModelName: seed.group, Ratio: 1, LastSeenAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
		if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
			ChannelID: seed.channel.ID, GroupName: seed.group, UpstreamKeyID: int64(seed.channel.ID*100 + uint(len(seed.group))),
			UpstreamKeyName: "managed-" + seed.group, OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated,
			APIKeyCipher: "cipher", Enabled: true,
		}); err != nil {
			t.Fatal(err)
		}
	}
	target := &storage.GatewayCandidateGroup{Name: "fresh assignment target"}
	if err := store.SaveCandidateGroup(target, []storage.GatewayCandidateGroupMember{{ChannelID: first.ID, GroupName: "seed"}}); err != nil {
		t.Fatal(err)
	}
	for _, seed := range []struct {
		channel *storage.Channel
		group   string
	}{{first, "first-added"}, {second, "second-added"}} {
		if err := store.RecordUpstreamGroupChange(seed.channel.ID, seed.group, "added", 0, "managed-"+seed.group, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	changes, err := store.ListUpstreamGroupChanges(true)
	if err != nil {
		t.Fatal(err)
	}
	changeByGroup := make(map[string]storage.GatewayUpstreamGroupChange, len(changes))
	for _, change := range changes {
		changeByGroup[change.GroupName] = change
	}
	firstChange, secondChange := changeByGroup["first-added"], changeByGroup["second-added"]
	refreshErr := errors.New("upstream group endpoint unavailable")
	monitor := &upstreamGroupChangePreflightMonitor{failures: map[uint]error{second.ID: refreshErr}}
	service := gateway.NewService(channels, rates, store, nil, nil, nil)
	router := gin.New()
	registerGateway(router.Group("/api"), &Deps{Channels: channels, Gateway: service, Monitor: monitor})

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/gateway/upstream-group-changes/resolve-batch", strings.NewReader(fmt.Sprintf(
		`{"ids":[%d,%d,%d],"candidate_group_ids":[%d]}`,
		firstChange.ID, firstChange.ID, secondChange.ID, target.ID,
	)))
	request.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
	}
	var payload struct {
		Data struct {
			Resolved int             `json:"resolved"`
			Failures map[uint]string `json:"failures"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Data.Resolved != 1 || payload.Data.Failures[firstChange.ID] != "" || !strings.Contains(payload.Data.Failures[secondChange.ID], refreshErr.Error()) {
		t.Fatalf("batch result = %+v body=%s", payload.Data, response.Body.String())
	}
	callCounts := map[uint]int{}
	for _, channelID := range monitor.calls {
		callCounts[channelID]++
	}
	if callCounts[first.ID] != 1 || callCounts[second.ID] != 1 || len(monitor.calls) != 2 {
		t.Fatalf("refresh calls = %v", monitor.calls)
	}
	firstStored, err := store.FindUpstreamGroupChange(firstChange.ID)
	if err != nil {
		t.Fatal(err)
	}
	secondStored, err := store.FindUpstreamGroupChange(secondChange.ID)
	if err != nil {
		t.Fatal(err)
	}
	if firstStored.Status != "assigned" || secondStored.Status != "pending" {
		t.Fatalf("change statuses = %q / %q", firstStored.Status, secondStored.Status)
	}
}

func TestGatewayUpstreamGroupChangeSingleAssignmentRejectsFailedFreshnessPreflight(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	channel := &storage.Channel{Name: "single fresh assignment", Type: storage.ChannelTypeNewAPI, SiteURL: "https://single-fresh-assignment.invalid", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	for _, group := range []string{"seed", "fresh-added"} {
		if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: group, Ratio: 1, LastSeenAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
		if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
			ChannelID: channel.ID, GroupName: group, UpstreamKeyID: int64(len(group)), UpstreamKeyName: "managed-" + group,
			OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated, APIKeyCipher: "cipher", Enabled: true,
		}); err != nil {
			t.Fatal(err)
		}
	}
	target := &storage.GatewayCandidateGroup{Name: "single fresh target"}
	if err := store.SaveCandidateGroup(target, []storage.GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "seed"}}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "fresh-added", "added", 0, "managed-fresh-added", time.Now()); err != nil {
		t.Fatal(err)
	}
	changes, err := store.ListUpstreamGroupChanges(true)
	if err != nil || len(changes) != 1 {
		t.Fatalf("changes = %+v err=%v", changes, err)
	}
	refreshErr := errors.New("cannot verify current upstream groups")
	monitor := &upstreamGroupChangePreflightMonitor{failures: map[uint]error{channel.ID: refreshErr}}
	service := gateway.NewService(channels, rates, store, nil, nil, nil)
	router := gin.New()
	registerGateway(router.Group("/api"), &Deps{Channels: channels, Gateway: service, Monitor: monitor})

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/gateway/upstream-group-changes/%d/resolve", changes[0].ID), strings.NewReader(fmt.Sprintf(`{"candidate_group_ids":[%d]}`, target.ID)))
	request.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(response, request)
	if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), refreshErr.Error()) {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
	}
	stored, err := store.FindUpstreamGroupChange(changes[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != "pending" || len(monitor.calls) != 1 || monitor.calls[0] != channel.ID {
		t.Fatalf("stored=%+v refresh calls=%v", stored, monitor.calls)
	}

	ignoreResponse := httptest.NewRecorder()
	ignoreRequest := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/gateway/upstream-group-changes/%d/resolve", changes[0].ID), strings.NewReader(`{"ignore":true}`))
	ignoreRequest.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(ignoreResponse, ignoreRequest)
	if ignoreResponse.Code != http.StatusOK {
		t.Fatalf("ignore status = %d body=%s", ignoreResponse.Code, ignoreResponse.Body.String())
	}
	stored, err = store.FindUpstreamGroupChange(changes[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != "ignored" || len(monitor.calls) != 1 {
		t.Fatalf("ignored stored=%+v refresh calls=%v", stored, monitor.calls)
	}
}
