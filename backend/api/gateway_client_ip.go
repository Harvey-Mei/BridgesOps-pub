package api

import (
	"net"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

const (
	gatewayClientIPSourceDirect       = "direct"
	gatewayClientIPSourceTrustedProxy = "trusted_proxy"
	gatewayClientIPSourceUnknown      = "unknown"
)

// gatewayClientIPSource records whether Gin accepted a different address from
// the configured trusted-proxy chain. The address itself still comes from
// c.ClientIP(), so this helper never trusts a forwarded header independently.
func gatewayClientIPSource(c *gin.Context) string {
	if c == nil || c.Request == nil {
		return gatewayClientIPSourceUnknown
	}
	clientIP := normalizeGatewayClientIPAddress(c.ClientIP())
	peerIP := gatewayRequestPeerIP(c.Request)
	if clientIP == "" || peerIP == "" {
		return gatewayClientIPSourceUnknown
	}
	if clientIP != peerIP {
		return gatewayClientIPSourceTrustedProxy
	}
	return gatewayClientIPSourceDirect
}

func gatewayRequestPeerIP(request *http.Request) string {
	if request == nil {
		return ""
	}
	raw := strings.TrimSpace(request.RemoteAddr)
	if raw == "" {
		return ""
	}
	host, _, err := net.SplitHostPort(raw)
	if err != nil {
		host = strings.TrimSuffix(strings.TrimPrefix(raw, "["), "]")
	}
	return normalizeGatewayClientIPAddress(host)
}

func normalizeGatewayClientIPAddress(value string) string {
	parsed := net.ParseIP(strings.TrimSpace(value))
	if parsed == nil {
		return ""
	}
	return parsed.String()
}
