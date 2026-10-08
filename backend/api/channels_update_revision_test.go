package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Harvey-Mei/BridgeOps/backend/channel"
	appcrypto "github.com/Harvey-Mei/BridgeOps/backend/crypto"
	"github.com/Harvey-Mei/BridgeOps/backend/gateway"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type recordingContextChannelLifecycle struct {
	channelService
	lifecycle    contextChannelLifecycle
	deleteLocked bool
	clearLocked  bool
}

func (s *recordingContextChannelLifecycle) DeleteContext(ctx context.Context, id uint) error {
	s.deleteLocked = storage.ChannelOperationLockHeld(ctx, id)
	return s.lifecycle.DeleteContext(ctx, id)
}

func (s *recordingContextChannelLifecycle) ClearLoginInfoContext(ctx context.Context, id uint) (*storage.Channel, error) {
	s.clearLocked = storage.ChannelOperationLockHeld(ctx, id)
	return s.lifecycle.ClearLoginInfoContext(ctx, id)
}

func TestUpdateChannelRequiresCurrentAdminRevision(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	stored := &storage.Channel{
		Name: "revision", Type: storage.ChannelTypeSub2API,
		SiteURL: "https://example.com", Username: "user", PasswordCipher: "cipher",
		CredentialMode: storage.CredentialModePassword, MonitorEnabled: true,
	}
	if err := channels.Create(stored); err != nil {
		t.Fatal(err)
	}
	cipher, err := appcrypto.NewCipher("channel-admin-revision-test")
	if err != nil {
		t.Fatal(err)
	}
	service := channel.NewService(
		channels,
		storage.NewAuthSessions(db),
		storage.NewCaptchas(db),
		storage.NewRates(db),
		storage.NewMonitorLogs(db),
		cipher,
	)
	router := gin.New()
	registerChannels(router.Group("/api"), &Deps{Channels: channels, ChannelSvc: service})

	for _, test := range []struct {
		name string
		body string
	}{
		{name: "missing", body: `{"name":"missing-revision"}`},
		{name: "zero", body: `{"expected_admin_revision":0,"name":"zero-revision"}`},
		{name: "stale", body: `{"expected_admin_revision":99,"name":"stale-revision"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPut, "/api/channels/1", strings.NewReader(test.body))
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)
			if response.Code != http.StatusConflict {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
		})
	}

	current, err := channels.FindByID(stored.ID)
	if err != nil || current.Name != "revision" {
		t.Fatalf("rejected update changed channel: %#v err=%v", current, err)
	}
	request := httptest.NewRequest(http.MethodPut, "/api/channels/1", strings.NewReader(fmt.Sprintf(`{"expected_admin_revision":%d,"name":"updated"}`, current.AdminRevision)))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("current revision status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestUpdateChannelRejectsStaleEmptyPatch(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	stored := &storage.Channel{
		Name: "empty-revision", Type: storage.ChannelTypeSub2API,
		SiteURL: "https://example.com", Username: "user", PasswordCipher: "cipher",
		CredentialMode: storage.CredentialModePassword, MonitorEnabled: true,
	}
	if err := channels.Create(stored); err != nil {
		t.Fatal(err)
	}
	cipher, err := appcrypto.NewCipher("channel-admin-empty-revision-test")
	if err != nil {
		t.Fatal(err)
	}
	service := channel.NewService(
		channels,
		storage.NewAuthSessions(db),
		storage.NewCaptchas(db),
		storage.NewRates(db),
		storage.NewMonitorLogs(db),
		cipher,
	)
	router := gin.New()
	registerChannels(router.Group("/api"), &Deps{Channels: channels, ChannelSvc: service})

	request := httptest.NewRequest(http.MethodPut, "/api/channels/1", strings.NewReader(`{"expected_admin_revision":99}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusConflict {
		t.Fatalf("stale empty patch status=%d body=%s", response.Code, response.Body.String())
	}

	request = httptest.NewRequest(http.MethodPut, "/api/channels/1", strings.NewReader(fmt.Sprintf(`{"expected_admin_revision":%d}`, stored.AdminRevision)))
	request.Header.Set("Content-Type", "application/json")
	response = httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), fmt.Sprintf(`"admin_revision":%d`, stored.AdminRevision)) {
		t.Fatalf("current empty patch status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestDeleteDirectChannelRequiresGatewayCompoundLock(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	stored := &storage.Channel{
		Name: "direct-delete", Type: storage.ChannelTypeNewAPI,
		SiteURL: "https://direct-delete.invalid", Username: "",
		PasswordCipher: "cipher", CredentialMode: storage.CredentialModeDirect,
		DirectModels: storage.EncodeDirectModels([]string{"vendor-alias"}), MonitorEnabled: true,
	}
	if err := channels.Create(stored); err != nil {
		t.Fatal(err)
	}
	cipher, err := appcrypto.NewCipher("direct-delete-compound-lock-test")
	if err != nil {
		t.Fatal(err)
	}
	service := channel.NewService(
		channels,
		storage.NewAuthSessions(db),
		storage.NewCaptchas(db),
		storage.NewRates(db),
		storage.NewMonitorLogs(db),
		cipher,
	)
	router := gin.New()
	registerChannels(router.Group("/api"), &Deps{Channels: channels, ChannelSvc: service})

	request := httptest.NewRequest(http.MethodDelete, fmt.Sprintf("/api/channels/%d", stored.ID), nil)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if _, err := channels.FindByID(stored.ID); err != nil {
		t.Fatalf("rejected direct delete removed channel: %v", err)
	}
}

func TestManagedCredentialLifecycleRunsUnderGatewayCompoundLock(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	stored := &storage.Channel{
		Name: "managed-compound-lock", Type: storage.ChannelTypeNewAPI,
		SiteURL: "https://managed-compound-lock.invalid", Username: "user",
		PasswordCipher: "cipher", CredentialMode: storage.CredentialModePassword, MonitorEnabled: true,
	}
	if err := channels.Create(stored); err != nil {
		t.Fatal(err)
	}
	cipher, err := appcrypto.NewCipher("managed-credential-compound-lock-test")
	if err != nil {
		t.Fatal(err)
	}
	service := channel.NewService(
		channels,
		storage.NewAuthSessions(db),
		storage.NewCaptchas(db),
		rates,
		storage.NewMonitorLogs(db),
		cipher,
	)
	recorder := &recordingContextChannelLifecycle{channelService: service, lifecycle: service}
	router := gin.New()
	registerChannels(router.Group("/api"), &Deps{
		Channels:   channels,
		ChannelSvc: recorder,
		Gateway:    gateway.NewService(channels, rates, storage.NewGateway(db), cipher, nil, nil),
	})

	clearRequest := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/channels/%d/clear-login-info", stored.ID), nil)
	clearResponse := httptest.NewRecorder()
	router.ServeHTTP(clearResponse, clearRequest)
	if clearResponse.Code != http.StatusOK {
		t.Fatalf("clear status=%d body=%s", clearResponse.Code, clearResponse.Body.String())
	}
	if !recorder.clearLocked {
		t.Fatal("managed clear callback ran without the gateway channel operation lock")
	}

	deleteRequest := httptest.NewRequest(http.MethodDelete, fmt.Sprintf("/api/channels/%d", stored.ID), nil)
	deleteResponse := httptest.NewRecorder()
	router.ServeHTTP(deleteResponse, deleteRequest)
	if deleteResponse.Code != http.StatusAccepted {
		t.Fatalf("delete status=%d body=%s", deleteResponse.Code, deleteResponse.Body.String())
	}
	if !recorder.deleteLocked {
		t.Fatal("managed delete callback ran without the gateway channel operation lock")
	}
}

func TestDirectCredentialLifecycleCleansGatewayStateUnderCompoundLock(t *testing.T) {
	for _, test := range []struct {
		name       string
		method     string
		pathSuffix string
		wantStatus int
	}{
		{name: "clear", method: http.MethodPost, pathSuffix: "/clear-login-info", wantStatus: http.StatusOK},
		{name: "delete", method: http.MethodDelete, wantStatus: http.StatusAccepted},
	} {
		t.Run(test.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			db := openTestDB(t)
			channels := storage.NewChannels(db)
			rates := storage.NewRates(db)
			store := storage.NewGateway(db)
			stored := &storage.Channel{
				Name: "direct-" + test.name, Type: storage.ChannelTypeNewAPI,
				SiteURL:        "https://direct-" + test.name + ".invalid",
				PasswordCipher: "direct-cipher", CredentialMode: storage.CredentialModeDirect,
				DirectModels: storage.EncodeDirectModels([]string{"vendor-alias"}),
				GatewayApp:   storage.GatewayAppCodex, MonitorEnabled: true,
			}
			if err := channels.Create(stored); err != nil {
				t.Fatal(err)
			}
			if _, err := rates.Upsert(&storage.RateSnapshot{
				ChannelID: stored.ID, ModelName: storage.DirectChannelGroupName, Ratio: 1,
			}); err != nil {
				t.Fatal(err)
			}
			if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
				ChannelID: stored.ID, GroupName: storage.DirectChannelGroupName,
				UpstreamKeyID: 0, UpstreamKeyName: storage.DirectChannelUpstreamKeyName,
				APIKeyCipher: "direct-key-cipher", APIKeyPrefix: "sk-direct", Enabled: true,
			}); err != nil {
				t.Fatal(err)
			}
			cipher, err := appcrypto.NewCipher("direct-credential-compound-lock-" + test.name)
			if err != nil {
				t.Fatal(err)
			}
			service := channel.NewService(
				channels,
				storage.NewAuthSessions(db),
				storage.NewCaptchas(db),
				rates,
				storage.NewMonitorLogs(db),
				cipher,
			)
			router := gin.New()
			registerChannels(router.Group("/api"), &Deps{
				Channels:   channels,
				ChannelSvc: service,
				Gateway:    gateway.NewService(channels, rates, store, cipher, nil, nil),
			})

			request := httptest.NewRequest(test.method, fmt.Sprintf("/api/channels/%d%s", stored.ID, test.pathSuffix), nil)
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)
			if response.Code != test.wantStatus {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			if _, err := store.FindUpstreamKeyAny(stored.ID, storage.DirectChannelGroupName); !errors.Is(err, gorm.ErrRecordNotFound) {
				t.Fatalf("direct gateway key remains after %s: %v", test.name, err)
			}
			if _, err := rates.FindByChannelAndModel(stored.ID, storage.DirectChannelGroupName); !errors.Is(err, gorm.ErrRecordNotFound) {
				t.Fatalf("direct rate remains after %s: %v", test.name, err)
			}
			if test.name == "delete" {
				var claims int64
				if err := db.Model(&storage.ChannelDeletionKeyClaim{}).Where("channel_id = ?", stored.ID).Count(&claims).Error; err != nil {
					t.Fatal(err)
				}
				if claims != 0 {
					t.Fatalf("direct delete prepared remote-key claims = %d, want 0", claims)
				}
			}
		})
	}
}
