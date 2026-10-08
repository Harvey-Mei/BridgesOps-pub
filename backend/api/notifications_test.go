package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/Harvey-Mei/BridgeOps/backend/crypto"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/gin-gonic/gin"
)

func TestNotificationChannelEditKeepsStoredSecrets(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	notifies := storage.NewNotifications(db)
	cipher, err := crypto.NewCipher("notification-test-secret")
	if err != nil {
		t.Fatalf("new cipher: %v", err)
	}
	original := `{"host":"smtp.example.com","port":465,"username":"alerts","password":"smtp-secret","from":"alerts@example.com","to":["operator@example.com"],"use_tls":true}`
	encoded, err := cipher.Encrypt(original)
	if err != nil {
		t.Fatalf("encrypt config: %v", err)
	}
	channel := &storage.NotificationChannel{
		Name:          "mail",
		Type:          storage.NotifyEmail,
		ConfigCipher:  encoded,
		Subscriptions: "[]",
		Enabled:       true,
	}
	if err := notifies.CreateChannel(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}

	router := gin.New()
	registerNotifications(router.Group("/api"), &Deps{Notifies: notifies, Cipher: cipher})

	listReq := httptest.NewRequest(http.MethodGet, "/api/notifications/channels", nil)
	listRec := httptest.NewRecorder()
	router.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("list status = %d, body=%s", listRec.Code, listRec.Body.String())
	}
	if strings.Contains(listRec.Body.String(), "smtp-secret") {
		t.Fatalf("list leaked password: %s", listRec.Body.String())
	}
	var listed struct {
		Data []struct {
			Config map[string]any `json:"config"`
		} `json:"data"`
	}
	if err := json.Unmarshal(listRec.Body.Bytes(), &listed); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(listed.Data) != 1 || listed.Data[0].Config["host"] != "smtp.example.com" || listed.Data[0].Config["password"] != nil {
		t.Fatalf("safe config = %#v", listed.Data)
	}

	updateBody := `{"name":"mail","type":"email","enabled":true,"proxy_enabled":false,"subscriptions":"[]","config":"{\"from_name\":\"BridgeOps 运维\"}"}`
	updateReq := httptest.NewRequest(http.MethodPut, "/api/notifications/channels/"+strconv.FormatUint(uint64(channel.ID), 10), strings.NewReader(updateBody))
	updateReq.Header.Set("Content-Type", "application/json")
	updateRec := httptest.NewRecorder()
	router.ServeHTTP(updateRec, updateReq)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("update status = %d, body=%s", updateRec.Code, updateRec.Body.String())
	}
	stored, err := notifies.FindChannel(channel.ID)
	if err != nil {
		t.Fatalf("find updated channel: %v", err)
	}
	plain, err := cipher.Decrypt(stored.ConfigCipher)
	if err != nil {
		t.Fatalf("decrypt updated config: %v", err)
	}
	var config map[string]any
	if err := json.Unmarshal([]byte(plain), &config); err != nil {
		t.Fatalf("decode updated config: %v", err)
	}
	if config["password"] != "smtp-secret" || config["host"] != "smtp.example.com" || config["from_name"] != "BridgeOps 运维" {
		t.Fatalf("merged config = %#v", config)
	}
}

func TestNotificationChannelRejectsTypeChangeWithoutConfiguration(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	notifies := storage.NewNotifications(db)
	cipher, err := crypto.NewCipher("notification-type-change-test-secret")
	if err != nil {
		t.Fatalf("new cipher: %v", err)
	}
	ciphertext, err := cipher.Encrypt(`{"bot_token":"secret","chat_id":"123"}`)
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.NotificationChannel{Name: "alerts", Type: storage.NotifyTelegram, ConfigCipher: ciphertext, Subscriptions: "[]", Enabled: true}
	if err := notifies.CreateChannel(channel); err != nil {
		t.Fatal(err)
	}
	router := gin.New()
	registerNotifications(router.Group("/api"), &Deps{Notifies: notifies, Cipher: cipher})
	body := `{"name":"alerts","type":"email","enabled":true,"proxy_enabled":false,"subscriptions":"[]"}`
	request := httptest.NewRequest(http.MethodPut, "/api/notifications/channels/"+strconv.FormatUint(uint64(channel.ID), 10), strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("type change status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	stored, err := notifies.FindChannel(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Type != storage.NotifyTelegram {
		t.Fatalf("type changed despite rejected config: %q", stored.Type)
	}
}
