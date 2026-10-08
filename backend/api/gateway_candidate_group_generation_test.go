package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/gateway"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/gin-gonic/gin"
)

func TestCandidateGroupMutationUsesAuthActorAndCurrentGenerationCAS(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	channel := &storage.Channel{
		Name: "candidate API generation CAS", Type: storage.ChannelTypeNewAPI,
		SiteURL: "https://candidate-api-generation.example", MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "primary", Ratio: 1, LastSeenAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "primary", UpstreamKeyID: 1, UpstreamKeyName: "managed",
		OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated, APIKeyCipher: "cipher", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Truncate(time.Microsecond)
	if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "added", 1, "managed", base); err != nil {
		t.Fatal(err)
	}
	observed, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary")
	if err != nil {
		t.Fatal(err)
	}

	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("authSubject", "operator:candidate-groups")
		c.Next()
	})
	registerGateway(router.Group("/api"), &Deps{Gateway: gateway.NewService(channels, rates, store, nil, nil, nil)})
	body := fmt.Sprintf(`{"name":"current target","members":[{"channel_id":%d,"group_name":"primary","upstream_group_id":%d,"presence_generation":%d}]}`,
		channel.ID, observed.ID, observed.PresenceGeneration)
	request := httptest.NewRequest(http.MethodPost, "/api/gateway/candidate-groups", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("create candidate group = %d %s", recorder.Code, recorder.Body.String())
	}
	var created struct {
		Data storage.GatewayCandidateGroup `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if created.Data.ID == 0 || created.Data.MembershipVersion == 0 {
		t.Fatalf("created candidate snapshot = %+v", created.Data)
	}
	var override storage.GatewayUpstreamGroupAssignmentOverride
	if err := db.Where("upstream_group_id = ?", observed.ID).First(&override).Error; err != nil {
		t.Fatal(err)
	}
	if override.PresenceGeneration != observed.PresenceGeneration || override.CreatedBy != "operator:candidate-groups" {
		t.Fatalf("candidate override actor/generation = %+v", override)
	}
	var audits []storage.GatewayUpstreamGroupAssignmentAudit
	if err := db.Where("upstream_group_id = ?", observed.ID).Order("id ASC").Find(&audits).Error; err != nil {
		t.Fatal(err)
	}
	if len(audits) == 0 {
		t.Fatal("candidate mutation wrote no assignment audit")
	}
	for _, audit := range audits {
		if audit.Actor != "operator:candidate-groups" {
			t.Fatalf("candidate assignment audit actor = %q", audit.Actor)
		}
	}
	listRequest := httptest.NewRequest(http.MethodGet, "/api/gateway/candidate-groups", nil)
	listRecorder := httptest.NewRecorder()
	router.ServeHTTP(listRecorder, listRequest)
	if listRecorder.Code != http.StatusOK ||
		!strings.Contains(listRecorder.Body.String(), fmt.Sprintf(`"upstream_group_id":%d`, observed.ID)) ||
		!strings.Contains(listRecorder.Body.String(), fmt.Sprintf(`"presence_generation":%d`, observed.PresenceGeneration)) {
		t.Fatalf("candidate group list omitted generation snapshot: %d %s", listRecorder.Code, listRecorder.Body.String())
	}

	missingVersionRequest := httptest.NewRequest(http.MethodPatch, fmt.Sprintf("/api/gateway/candidate-groups/%d", created.Data.ID), strings.NewReader(body))
	missingVersionRequest.Header.Set("Content-Type", "application/json")
	missingVersionRecorder := httptest.NewRecorder()
	router.ServeHTTP(missingVersionRecorder, missingVersionRequest)
	if missingVersionRecorder.Code != http.StatusConflict {
		t.Fatalf("missing candidate membership version = %d %s", missingVersionRecorder.Code, missingVersionRecorder.Body.String())
	}
	if err := db.Model(&storage.GatewayCandidateGroup{}).Where("id = ?", created.Data.ID).
		Update("membership_version", created.Data.MembershipVersion+1).Error; err != nil {
		t.Fatal(err)
	}
	staleVersionBody := fmt.Sprintf(`{"name":"stale editor overwrite","expected_membership_version":%d,"members":[{"channel_id":%d,"group_name":"primary","upstream_group_id":%d,"presence_generation":%d}]}`,
		created.Data.MembershipVersion, channel.ID, observed.ID, observed.PresenceGeneration)
	staleVersionRequest := httptest.NewRequest(http.MethodPatch, fmt.Sprintf("/api/gateway/candidate-groups/%d", created.Data.ID), strings.NewReader(staleVersionBody))
	staleVersionRequest.Header.Set("Content-Type", "application/json")
	staleVersionRecorder := httptest.NewRecorder()
	router.ServeHTTP(staleVersionRecorder, staleVersionRequest)
	if staleVersionRecorder.Code != http.StatusConflict {
		t.Fatalf("stale candidate membership version = %d %s", staleVersionRecorder.Code, staleVersionRecorder.Body.String())
	}
	var unchanged storage.GatewayCandidateGroup
	if err := db.First(&unchanged, created.Data.ID).Error; err != nil {
		t.Fatal(err)
	}
	if unchanged.Name != "current target" {
		t.Fatalf("stale candidate editor changed name to %q", unchanged.Name)
	}

	if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "removed", 1, "managed", base.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "added", 2, "managed-2", base.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	staleBody := fmt.Sprintf(`{"name":"stale target","members":[{"channel_id":%d,"group_name":"primary","upstream_group_id":%d,"presence_generation":%d}]}`,
		channel.ID, observed.ID, observed.PresenceGeneration)
	staleRequest := httptest.NewRequest(http.MethodPost, "/api/gateway/candidate-groups", strings.NewReader(staleBody))
	staleRequest.Header.Set("Content-Type", "application/json")
	staleRecorder := httptest.NewRecorder()
	router.ServeHTTP(staleRecorder, staleRequest)
	if staleRecorder.Code != http.StatusConflict {
		t.Fatalf("stale candidate generation = %d %s", staleRecorder.Code, staleRecorder.Body.String())
	}
	var staleGroupCount int64
	if err := db.Model(&storage.GatewayCandidateGroup{}).Where("name = ?", "stale target").Count(&staleGroupCount).Error; err != nil {
		t.Fatal(err)
	}
	if staleGroupCount != 0 {
		t.Fatalf("stale candidate mutation committed %d groups", staleGroupCount)
	}
}
