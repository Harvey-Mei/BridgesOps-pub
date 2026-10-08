package gateway

import "time"

// Sub2API freezes usage timestamps before asynchronously committing usage
// rows. Waiting past its normal worker window avoids a guaranteed first
// exact-ID miss while the local estimated cost remains immediately visible.
const requestCostInitialSettlementDelay = 6 * time.Second

func InitialRequestCostReconcileAt(finishedAt time.Time) time.Time {
	if finishedAt.IsZero() {
		finishedAt = time.Now()
	}
	return finishedAt.Add(requestCostInitialSettlementDelay)
}
