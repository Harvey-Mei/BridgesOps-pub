package netproxy

import (
	"context"
	"net/netip"
	"testing"
)

func TestSubscriptionFetchRejectsPrivateTargets(t *testing.T) {
	if _, err := FetchSubscription(context.Background(), "http://127.0.0.1:8080/subscription"); err == nil {
		t.Fatal("expected private subscription target to be rejected")
	}
}

func TestIsPublicSubscriptionAddress(t *testing.T) {
	for _, raw := range []string{"127.0.0.1", "10.0.0.1", "100.64.0.1", "169.254.1.1", "192.0.2.1", "::1"} {
		address, err := netip.ParseAddr(raw)
		if err != nil {
			t.Fatal(err)
		}
		if isPublicSubscriptionAddress(address) {
			t.Fatalf("%s must not be public", raw)
		}
	}
	address := netip.MustParseAddr("1.1.1.1")
	if !isPublicSubscriptionAddress(address) {
		t.Fatal("public address was rejected")
	}
}
