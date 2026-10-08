package api

import (
	"strings"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

// gatewayConnectionEvent values are derived from durable request facts rather
// than raw upstream text. They are observability labels only: none of them
// changes billing, routing health, retry, or aggregate semantics.
const (
	gatewayConnectionEventClientDisconnect    = "client_disconnect"
	gatewayConnectionEventUpstreamInterrupted = "upstream_interrupted"
	gatewayConnectionEventDispatchRecovery    = "dispatch_recovery"
)

func gatewayRequestConnectionEvent(item storage.GatewayRequestLogItem) string {
	reason := strings.ToLower(strings.TrimSpace(item.RouteReason))
	failure := strings.ToLower(strings.TrimSpace(item.TerminalFailureCode))
	errorText := strings.ToLower(strings.TrimSpace(item.Error))

	switch {
	case reason == "dispatch_recovery" || failure == "dispatch_recovery":
		return gatewayConnectionEventDispatchRecovery
	case reason == "client_canceled" || item.StatusCode == 499 || errorText == "context canceled" || errorText == "客户端取消":
		return gatewayConnectionEventClientDisconnect
	case item.UpstreamAccepted && item.StatusCode >= 500 &&
		(failure == "transient_after_acceptance" || failure == "upstream_5xx" ||
			strings.Contains(errorText, "stream read failed after upstream acceptance") ||
			strings.Contains(errorText, "stream disconnected before completion")):
		return gatewayConnectionEventUpstreamInterrupted
	default:
		return ""
	}
}
