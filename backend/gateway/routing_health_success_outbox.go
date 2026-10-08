package gateway

const routingHealthSuccessOutboxBatch = 100

func (s *Service) flushRoutingHealthSuccessOutbox() {
	if s == nil || s.Store == nil {
		return
	}
	delivered, err := s.Store.DeliverRoutingHealthSuccessOutbox(routingHealthSuccessOutboxBatch)
	if err != nil {
		if s.Log != nil {
			s.Log.Warn("deliver durable routing health successes failed; will retry", "delivered", delivered, "err", err)
		}
		return
	}
	if delivered > 0 && s.Log != nil {
		s.Log.Debug("delivered durable routing health successes", "count", delivered)
	}
}
