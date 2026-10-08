package channel

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

type balanceRetryConnector struct {
	connector.Connector
	checkErr     error
	checkCalls   int
	refreshCalls int
	loginCalls   int
}

func (c *balanceRetryConnector) CheckAuth(context.Context, *connector.Channel, *connector.AuthSession) error {
	c.checkCalls++
	return c.checkErr
}

func (c *balanceRetryConnector) RefreshSession(context.Context, *connector.Channel, *connector.AuthSession) (*connector.AuthSession, error) {
	c.refreshCalls++
	return &connector.AuthSession{AccessToken: "refreshed", RefreshToken: "rotated"}, nil
}

func (c *balanceRetryConnector) Login(context.Context, *connector.Channel) (*connector.AuthSession, error) {
	c.loginCalls++
	return &connector.AuthSession{AccessToken: "logged-in"}, nil
}

func TestEnsureTokenSessionDoesNotRefreshOrPolluteLastErrorOnBalanceFailure(t *testing.T) {
	svc, cipher := testService(t)
	credential, err := cipher.Encrypt(`{"access_token":"access","refresh_token":"refresh"}`)
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{
		Name: "balance-token", Type: storage.ChannelTypeSub2API, SiteURL: "https://example.com",
		Username: "u", PasswordCipher: credential, CredentialMode: storage.CredentialModeToken,
		LastError: "keep this rate error",
	}
	if err := svc.Channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	checkErr := connector.HTTPStatusError(403, []byte(`{"error":{"code":"INSUFFICIENT_BALANCE"}}`))
	conn := &balanceRetryConnector{checkErr: checkErr}
	_, err = svc.EnsureSession(context.Background(), channel, &connector.Channel{}, conn)
	if err == nil || !connector.IsAccountBalanceHTTPError(err) {
		t.Fatalf("EnsureSession error = %v, want account balance failure", err)
	}
	if conn.checkCalls != 1 || conn.refreshCalls != 0 || conn.loginCalls != 0 {
		t.Fatalf("session calls = check:%d refresh:%d login:%d", conn.checkCalls, conn.refreshCalls, conn.loginCalls)
	}
	stored, err := svc.Channels.FindByID(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.LastError != "keep this rate error" {
		t.Fatalf("last error = %q, want unchanged", stored.LastError)
	}
}

func TestEnsurePasswordSessionDoesNotRefreshOrLoginOnBalanceFailure(t *testing.T) {
	svc, _ := testService(t)
	channel := &storage.Channel{
		Name: "balance-password", Type: storage.ChannelTypeNewAPI, SiteURL: "https://example.com",
		Username: "u", PasswordCipher: "cipher", CredentialMode: storage.CredentialModePassword,
		LastError: "keep this rate error",
	}
	if err := svc.Channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := svc.persistSession(channel, &connector.AuthSession{
		AccessToken: "access", RefreshToken: "refresh", ExpiresAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	checkErr := connector.HTTPStatusError(401, []byte(`{"error":{"message":"Insufficient account balance"}}`))
	conn := &balanceRetryConnector{checkErr: checkErr}
	_, err := svc.EnsureSession(context.Background(), channel, &connector.Channel{}, conn)
	if err == nil || !connector.IsAccountBalanceHTTPError(err) || strings.Contains(err.Error(), "复用已有会话失败") {
		t.Fatalf("EnsureSession error = %v, want original balance failure", err)
	}
	if conn.checkCalls != 1 || conn.refreshCalls != 0 || conn.loginCalls != 0 {
		t.Fatalf("session calls = check:%d refresh:%d login:%d", conn.checkCalls, conn.refreshCalls, conn.loginCalls)
	}
	stored, err := svc.Channels.FindByID(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.LastError != "keep this rate error" {
		t.Fatalf("last error = %q, want unchanged", stored.LastError)
	}
}
