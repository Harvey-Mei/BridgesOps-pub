package netproxy

import (
	"fmt"
	"strings"
	"testing"
)

func TestParseImportURIListKeepsDirectEndpointsAndSkipsNodeProtocols(t *testing.T) {
	result, err := ParseImport(ImportFormatURI, "http://user:pass@proxy.example:8080\nsocks5h://127.0.0.1:1080\nvless://secret@example.invalid")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Endpoints) != 2 || result.Endpoints[0].Endpoint.Protocol != ProtocolHTTP || result.Endpoints[1].Endpoint.Protocol != ProtocolSOCKS5 {
		t.Fatalf("endpoints = %#v", result.Endpoints)
	}
	if len(result.Skipped) != 1 || result.Skipped[0].Entry != "第 3 项" || !strings.Contains(result.Skipped[0].Reason, "HTTP/SOCKS") {
		t.Fatalf("skipped=%#v", result.Skipped)
	}
}

func TestParseImportClashOnlyAcceptsDirectProxyNodes(t *testing.T) {
	result, err := ParseImport(ImportFormatClash, `
proxies:
  - name: local-http
    type: http
    server: 127.0.0.1
    port: 7890
  - name: socks
    type: socks5
    server: proxy.example
    port: "1080"
    username: bridge
    password: secret
  - name: vless-node
    type: vless
    server: ignored.example
    port: 443
`)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Endpoints) != 2 || result.Endpoints[1].Endpoint.Username != "bridge" {
		t.Fatalf("endpoints = %#v", result.Endpoints)
	}
	if len(result.Skipped) != 1 || result.Skipped[0].Entry != "第 3 项" || !strings.Contains(result.Skipped[0].Reason, "HTTP/SOCKS") {
		t.Fatalf("skipped=%#v", result.Skipped)
	}
}

func TestParseImportClashNeverUsesCredentialBearingNames(t *testing.T) {
	result, err := ParseImport(ImportFormatClash, `
proxies:
  - name: "socks5://user:clash-password@name.example:1080?token=clash-token"
    type: socks5
    server: safe.example
    port: 1080
  - name: "password=skip-secret"
    type: ss
    server: ignored.example
    port: 443
`)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Endpoints) != 1 || result.Endpoints[0].Name != "safe.example:1080" {
		t.Fatalf("endpoints = %#v", result.Endpoints)
	}
	if len(result.Skipped) != 1 || result.Skipped[0].Entry != "第 2 项" {
		t.Fatalf("skipped=%#v", result.Skipped)
	}
	for _, secret := range []string{"clash-password", "clash-token", "skip-secret", "user@", "password="} {
		if strings.Contains(result.Endpoints[0].Name, secret) {
			t.Fatalf("Clash label leaked %q: %#v", secret, result)
		}
		if strings.Contains(result.Skipped[0].Entry, secret) {
			t.Fatalf("Clash skip label leaked %q: %#v", secret, result)
		}
	}
}

func TestParseImportClashAlwaysUsesEndpointDisplayName(t *testing.T) {
	result, err := ParseImport(ImportFormatClash, `
proxies:
  - name: "regional token private-value"
    type: http
    server: safe.example
    port: 8080
`)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Endpoints) != 1 || result.Endpoints[0].Name != "safe.example:8080" {
		t.Fatalf("endpoints = %#v", result.Endpoints)
	}
	if strings.Contains(result.Endpoints[0].Name, "token") || strings.Contains(result.Endpoints[0].Name, "private-value") {
		t.Fatalf("Clash display name retained subscription label: %#v", result.Endpoints[0])
	}
}

func TestParseImportDecodesBase64Subscription(t *testing.T) {
	result, err := ParseImport(ImportFormat3XUI, "c29ja3M1Oi8vMTI3LjAuMC4xOjEwODA=")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Endpoints) != 1 || result.Endpoints[0].Endpoint.Port != 1080 {
		t.Fatalf("endpoints = %#v", result.Endpoints)
	}
}

func TestParseURLAcceptsCommonTrailingSlash(t *testing.T) {
	endpoint, err := ParseURL("https://user:password@proxy.example:8443/")
	if err != nil {
		t.Fatal(err)
	}
	if endpoint.Protocol != ProtocolHTTPS || endpoint.Host != "proxy.example" || endpoint.Port != 8443 {
		t.Fatalf("endpoint = %#v", endpoint)
	}
}

func TestParseImportBoundsEndpointBatch(t *testing.T) {
	var content strings.Builder
	for index := 0; index < MaxImportedEndpoints+3; index++ {
		fmt.Fprintf(&content, "http://proxy-%d.example:8080\n", index)
	}
	result, err := ParseImport(ImportFormatURI, content.String())
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Endpoints) != MaxImportedEndpoints {
		t.Fatalf("endpoint count = %d, want %d", len(result.Endpoints), MaxImportedEndpoints)
	}
	if len(result.Skipped) != 1 || !strings.Contains(result.Skipped[0].Reason, "最多导入") {
		t.Fatalf("skipped = %#v", result.Skipped)
	}
}
