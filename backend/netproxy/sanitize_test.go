package netproxy

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestSanitizeDiagnosticRedactsProxyCredentialsAndTokens(t *testing.T) {
	raw := "Get socks5://bridge:secret@proxy.example:1080/path?token=query-secret failed; Proxy-Authorization: Basic encoded-secret; password=plain-secret; cookie=session-secret"
	got := SanitizeDiagnostic(raw, 240)
	for _, secret := range []string{"bridge:secret", "query-secret", "encoded-secret", "plain-secret", "session-secret"} {
		if strings.Contains(got, secret) {
			t.Fatalf("diagnostic leaked %q: %s", secret, got)
		}
	}
	if !strings.Contains(got, "socks5://[redacted]@proxy.example:1080/path?[redacted]") {
		t.Fatalf("diagnostic lost useful endpoint context: %s", got)
	}
}

func TestSanitizeDiagnosticTruncatesUTF8Safely(t *testing.T) {
	got := SanitizeDiagnostic("网络代理错误："+strings.Repeat("测", 100), 21)
	if !utf8.ValidString(got) || !strings.HasSuffix(got, "…") {
		t.Fatalf("unsafe truncation: %q", got)
	}
}

func TestSafeDisplayNameFallsBackForURIAndCredentialBearingValues(t *testing.T) {
	endpoint := Endpoint{Protocol: ProtocolSOCKS5, Host: "safe.example", Port: 1080}
	for _, raw := range []string{
		"socks5://user:password@name.example:1080?token=query-token",
		"socks5%3A%2F%2Fuser%3Apassword%40name.example%3A1080%3Ftoken%3Dquery-token",
		"region?token=query-token",
		"password=plain-secret",
	} {
		got := SafeDisplayName(raw, endpoint)
		if got != "safe.example:1080" {
			t.Fatalf("SafeDisplayName(%q) = %q, want endpoint fallback", raw, got)
		}
		for _, secret := range []string{"user", "password", "query-token", "plain-secret"} {
			if strings.Contains(got, secret) {
				t.Fatalf("display name leaked %q: %q", secret, got)
			}
		}
	}
}

func TestValidateNetworkProxyNameRejectsSensitiveDisplayValues(t *testing.T) {
	for _, raw := range []string{
		"socks5://user:password@name.example:1080?token=query-token",
		"socks5%3A%2F%2Fuser%3Apassword%40name.example%3A1080%3Ftoken%3Dquery-token",
		"region?token=query-token",
		"password=plain-secret",
	} {
		if _, err := ValidateNetworkProxyName(raw); err == nil {
			t.Fatal("accepted a credential-bearing proxy display name")
		}
	}
	if got, err := ValidateNetworkProxyName("office proxy"); err != nil || got != "office proxy" {
		t.Fatalf("valid proxy name = %q, err = %v", got, err)
	}
}
