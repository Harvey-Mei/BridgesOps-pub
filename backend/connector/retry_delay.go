package connector

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"
)

// DefaultTransientRetryDelay is the scheduler delay for provider failures
// which are expected to recover without changing channel configuration.
const DefaultTransientRetryDelay = time.Minute

type retryDelayCarrier interface {
	RetryDelay() time.Duration
}

type retryDelayError struct {
	err   error
	delay time.Duration
}

func (e *retryDelayError) Error() string             { return e.err.Error() }
func (e *retryDelayError) Unwrap() error             { return e.err }
func (e *retryDelayError) RetryDelay() time.Duration { return e.delay }

// WithRetryDelay attaches a recovery scheduling hint without changing the
// operator-facing error or hiding its original error chain.
func WithRetryDelay(err error, delay time.Duration) error {
	if err == nil || delay <= 0 {
		return err
	}
	if current, ok := RetryDelay(err); ok && current <= delay {
		return err
	}
	return &retryDelayError{err: err, delay: delay}
}

func RetryDelay(err error) (time.Duration, bool) {
	var carrier retryDelayCarrier
	if !errors.As(err, &carrier) {
		return 0, false
	}
	delay := carrier.RetryDelay()
	return delay, delay > 0
}

// TransientRetryDelay returns an explicit retry hint first, then classifies
// common transport and HTTP failures. It intentionally excludes auth errors,
// balance rejections, malformed responses, and other configuration failures.
func TransientRetryDelay(err error) (time.Duration, bool) {
	if err == nil {
		return 0, false
	}
	if delay, ok := RetryDelay(err); ok {
		return delay, true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return DefaultTransientRetryDelay, true
	}
	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		status := httpErr.StatusCode
		if status == http.StatusRequestTimeout || status == http.StatusTooEarly || status == http.StatusTooManyRequests || status >= 500 && status <= 599 {
			return DefaultTransientRetryDelay, true
		}
	}
	var networkErr net.Error
	if errors.As(err, &networkErr) && networkErr.Timeout() {
		return DefaultTransientRetryDelay, true
	}
	return 0, false
}

// IsTransientUpstreamError reports whether an error should be retried by the
// scheduler without an immediate session re-authentication attempt.
func IsTransientUpstreamError(err error) bool {
	_, ok := TransientRetryDelay(err)
	return ok
}
