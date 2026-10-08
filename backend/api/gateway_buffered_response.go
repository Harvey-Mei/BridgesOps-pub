package api

import (
	"context"
	"errors"
	"io"
	"time"
)

type gatewayBufferedUpstreamResponse struct {
	Spool             *gatewayUpstreamResponseSpool
	Facts             gatewayBufferedResponseFacts
	Prefix            []byte
	Size              int64
	FirstBodyObserved bool
}

// bufferGatewayUpstreamResponse persists the exact upstream bytes before any
// structured interpretation. Projection uses an independent cursor, so the
// returned spool remains positioned at byte zero and can be replayed directly
// to the downstream client.
func bufferGatewayUpstreamResponse(ctx context.Context, controller *gatewayUpstreamResponseSpoolController, reader io.Reader, requestPath string, onFirstByte func(), admissionTimeout ...time.Duration) (gatewayBufferedUpstreamResponse, error) {
	result := gatewayBufferedUpstreamResponse{}
	spool, err := controller.spool(ctx, reader, func() {
		result.FirstBodyObserved = true
		if onFirstByte != nil {
			onFirstByte()
		}
	}, admissionTimeout...)
	if err != nil {
		return result, err
	}
	result.Spool = spool
	result.Size = spool.Size()
	result.Prefix = spool.Prefix()
	facts, err := projectGatewayUpstreamResponse(ctx, spool, gatewayUpstreamResponseProjectionOptions{RequestPath: requestPath})
	if err != nil {
		result.Spool = nil
		return result, errors.Join(err, spool.Close())
	}
	result.Facts = facts
	return result, nil
}
