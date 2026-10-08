package modelcheck

import (
	"context"
	"time"
)

type ProgressEvent struct {
	Type         string    `json:"type"`
	Kind         string    `json:"kind,omitempty"`
	At           time.Time `json:"at"`
	Path         string    `json:"path,omitempty"`
	RequestBody  string    `json:"request_body,omitempty"`
	ResponseText string    `json:"response_text,omitempty"`
	StatusCode   int       `json:"status_code,omitempty"`
	Message      string    `json:"message,omitempty"`
	Passed       *bool     `json:"passed,omitempty"`
	LatencyMS    int64     `json:"latency_ms,omitempty"`
	TotalTokens  int64     `json:"total_tokens,omitempty"`
}

type progressContextKey struct{}

func WithProgressObserver(ctx context.Context, observer func(ProgressEvent)) context.Context {
	return context.WithValue(ctx, progressContextKey{}, observer)
}
func EmitProgress(ctx context.Context, event ProgressEvent) {
	observer, ok := ctx.Value(progressContextKey{}).(func(ProgressEvent))
	if !ok {
		return
	}
	event.At = time.Now().UTC()
	event.RequestBody = boundedText(event.RequestBody, 4096)
	event.ResponseText = boundedText(event.ResponseText, MaxPelicanSVGBytes)
	event.Message = boundedText(event.Message, 256)
	observer(event)
}
