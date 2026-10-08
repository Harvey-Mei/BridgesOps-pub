package channel

import (
	"context"
	"net/http"
	"testing"

	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

type challengeRefreshConnector struct {
	connector.Connector
	err   error
	calls int
}

func (c *challengeRefreshConnector) RefreshSession(context.Context, *connector.Channel, *connector.AuthSession) (*connector.AuthSession, error) {
	c.calls++
	return nil, c.err
}

func TestRefreshStoredSessionPreservesCloudflareChallenge(t *testing.T) {
	challenge := connector.HTTPStatusError(http.StatusForbidden, []byte("<html><title>Just a moment...</title></html>"))
	conn := &challengeRefreshConnector{err: challenge}
	svc := &Service{}
	_, ok, err := svc.refreshStoredSession(
		context.Background(),
		&storage.Channel{},
		&connector.Channel{},
		conn,
		&connector.AuthSession{RefreshToken: "refresh"},
	)
	if ok {
		t.Fatal("challenge refresh unexpectedly returned a refreshed session")
	}
	if !connector.IsUpstreamChallengeHTTPError(err) {
		t.Fatalf("refresh error = %v, want Cloudflare challenge", err)
	}
	if conn.calls != 1 {
		t.Fatalf("refresh calls = %d, want one", conn.calls)
	}
}
