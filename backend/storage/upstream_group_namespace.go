package storage

import (
	"errors"
	"strings"
)

// ReservedInternalUpstreamGroupPrefix separates BridgeOps bookkeeping
// identities from provider-advertised upstream group names. Provider facts
// must never be persisted inside this namespace.
const ReservedInternalUpstreamGroupPrefix = "__bridgeops_internal_"

var ErrReservedInternalUpstreamGroupName = errors.New("upstream group name uses the BridgeOps internal namespace")

func isReservedInternalUpstreamGroupName(groupName string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(groupName)), ReservedInternalUpstreamGroupPrefix)
}
