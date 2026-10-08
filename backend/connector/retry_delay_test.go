package connector

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestRetryDelayPreservesCauseAndShortestHint(t *testing.T) {
	base := context.DeadlineExceeded
	wrapped := WithRetryDelay(base, time.Minute)
	if !errors.Is(wrapped, base) {
		t.Fatalf("retry hint lost cause: %v", wrapped)
	}
	if delay, ok := RetryDelay(wrapped); !ok || delay != time.Minute {
		t.Fatalf("retry delay = %s/%v", delay, ok)
	}
	wrapped = WithRetryDelay(wrapped, 5*time.Minute)
	if delay, _ := RetryDelay(wrapped); delay != time.Minute {
		t.Fatalf("longer nested hint replaced shortest delay: %s", delay)
	}
}

func TestTransientRetryDelayClassifiesProviderFailures(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "429", err: HTTPStatusError(http.StatusTooManyRequests, nil), want: true},
		{name: "503", err: HTTPStatusError(http.StatusServiceUnavailable, nil), want: true},
		{name: "not found", err: HTTPStatusError(http.StatusNotFound, nil), want: false},
		{name: "unauthorized", err: HTTPStatusError(http.StatusUnauthorized, nil), want: false},
		{name: "deadline", err: context.DeadlineExceeded, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := TransientRetryDelay(test.err)
			if ok != test.want {
				t.Fatalf("TransientRetryDelay(%v) = %s/%v, want transient=%t", test.err, got, ok, test.want)
			}
			if ok && got != DefaultTransientRetryDelay {
				t.Fatalf("TransientRetryDelay(%v) = %s, want %s", test.err, got, DefaultTransientRetryDelay)
			}
		})
	}
}
