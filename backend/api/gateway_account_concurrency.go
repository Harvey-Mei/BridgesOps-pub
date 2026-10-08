package api

import "github.com/Harvey-Mei/BridgeOps/backend/gateway"

// observeGatewayAccountConcurrencyPressure keeps provider capacity evidence
// separate from ordinary 429s such as billing or token quotas. The governor
// itself applies the evidence only to unknown, exact route-generation state.
func observeGatewayAccountConcurrencyPressure(lease *gateway.AccountConcurrencyLease, status int, message string) {
	if lease == nil || !gateway.IsExplicitAccountConcurrencyPressure(status, message) {
		return
	}
	lease.ObservePressure()
}

func observeGatewayAccountConcurrencySuccess(lease *gateway.AccountConcurrencyLease, succeeded bool) {
	if lease == nil || !succeeded {
		return
	}
	lease.ObserveUsefulSuccess()
}
