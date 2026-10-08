package api

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"time"
)

const (
	defaultGatewayResponseSpoolMaxResponseBytes int64 = 256 << 20
	defaultGatewayResponseSpoolPrefixBytes            = int64(1 << 20)
	defaultGatewayResponseSpoolModelsBytes            = int64(1 << 20)
	defaultGatewayResponseSpoolErrorBytes             = int64(64 << 10)
	defaultGatewayResponseSpoolMaxInFlightBytes       = int64(512 << 20)
	defaultGatewayResponseSpoolMaxInFlightFiles int64 = 8
	defaultGatewayResponseSpoolMaxQueuedFiles   int64 = 16
	defaultGatewayResponseSpoolOrphanMinimumAge       = 6 * time.Hour
)

// GatewayUpstreamResponseSpoolLimits configures the single disk admission
// domain shared by every gateway handler in this process. TempDir is required
// and must be an absolute, dedicated path; there is deliberately no implicit
// os.TempDir fallback.
type GatewayUpstreamResponseSpoolLimits struct {
	TempDir             string
	MaxResponseBytes    int64
	PrefixBytes         int64
	ModelsResponseBytes int64
	ErrorResponseBytes  int64
	MaxInFlightBytes    int64
	MaxInFlightFiles    int64
	MaxQueuedFiles      int64
	OrphanMinimumAge    time.Duration
}

type gatewayUpstreamResponseSpoolController struct {
	tempDir             string
	maxResponseBytes    int64
	prefixBytes         int64
	modelsResponseBytes int64
	errorResponseBytes  int64
	admission           *gatewayUpstreamResponseSpoolAdmission
}

func DefaultGatewayUpstreamResponseSpoolLimits(tempDir string) GatewayUpstreamResponseSpoolLimits {
	return GatewayUpstreamResponseSpoolLimits{
		TempDir:             tempDir,
		MaxResponseBytes:    defaultGatewayResponseSpoolMaxResponseBytes,
		PrefixBytes:         defaultGatewayResponseSpoolPrefixBytes,
		ModelsResponseBytes: defaultGatewayResponseSpoolModelsBytes,
		ErrorResponseBytes:  defaultGatewayResponseSpoolErrorBytes,
		MaxInFlightBytes:    defaultGatewayResponseSpoolMaxInFlightBytes,
		MaxInFlightFiles:    defaultGatewayResponseSpoolMaxInFlightFiles,
		MaxQueuedFiles:      defaultGatewayResponseSpoolMaxQueuedFiles,
		OrphanMinimumAge:    defaultGatewayResponseSpoolOrphanMinimumAge,
	}
}

func newGatewayUpstreamResponseSpoolController(limits GatewayUpstreamResponseSpoolLimits, now time.Time) (*gatewayUpstreamResponseSpoolController, int, error) {
	if limits.MaxResponseBytes <= 0 ||
		limits.PrefixBytes < 0 || limits.PrefixBytes > limits.MaxResponseBytes || limits.PrefixBytes > gatewayUpstreamResponseSpoolMaxPrefixBytes ||
		limits.ModelsResponseBytes <= 0 || limits.ModelsResponseBytes > limits.MaxResponseBytes ||
		limits.ErrorResponseBytes <= 0 || limits.ErrorResponseBytes > limits.MaxResponseBytes ||
		limits.MaxInFlightBytes <= 0 || limits.MaxResponseBytes > limits.MaxInFlightBytes ||
		limits.MaxInFlightFiles <= 0 || limits.MaxQueuedFiles < 0 || limits.OrphanMinimumAge <= 0 || now.IsZero() {
		return nil, 0, fmt.Errorf("%w: invalid gateway response spool limits", errGatewayResponseBufferInvalid)
	}
	directory, err := prepareGatewayUpstreamResponseSpoolDirectory(limits.TempDir)
	if err != nil {
		return nil, 0, err
	}
	removed, err := cleanupGatewayUpstreamResponseSpoolOrphans(directory, now, limits.OrphanMinimumAge)
	if err != nil {
		return nil, 0, err
	}
	admission, err := newGatewayUpstreamResponseSpoolAdmission(gatewayUpstreamResponseSpoolAdmissionLimits{
		MaxInFlightFiles: limits.MaxInFlightFiles,
		MaxInFlightBytes: limits.MaxInFlightBytes,
		MaxQueuedFiles:   limits.MaxQueuedFiles,
	})
	if err != nil {
		return nil, 0, err
	}
	return &gatewayUpstreamResponseSpoolController{
		tempDir:             directory,
		maxResponseBytes:    limits.MaxResponseBytes,
		prefixBytes:         limits.PrefixBytes,
		modelsResponseBytes: limits.ModelsResponseBytes,
		errorResponseBytes:  limits.ErrorResponseBytes,
		admission:           admission,
	}, removed, nil
}

// ConfigureGatewayUpstreamResponseSpool prepares the private directory,
// removes only old product-owned orphan files, and installs exactly one
// controller on the dependency graph. It must run before handlers are served.
func (d *Deps) ConfigureGatewayUpstreamResponseSpool(limits GatewayUpstreamResponseSpoolLimits, now time.Time) (int, error) {
	if d == nil {
		return 0, fmt.Errorf("%w: dependencies are required", errGatewayResponseBufferInvalid)
	}
	removed := 0
	installed := false
	d.gatewayResponseSpoolOnce.Do(func() {
		controller, cleaned, err := newGatewayUpstreamResponseSpoolController(limits, now)
		if err != nil {
			d.gatewayResponseSpoolErr = err
			return
		}
		d.gatewayResponseSpool = controller
		removed = cleaned
		installed = true
	})
	if d.gatewayResponseSpoolErr != nil {
		return 0, d.gatewayResponseSpoolErr
	}
	if !installed {
		return 0, fmt.Errorf("%w: response spool is already configured", errGatewayResponseBufferInvalid)
	}
	return removed, nil
}

func gatewayUpstreamResponseSpoolControllerForDeps(d *Deps) (*gatewayUpstreamResponseSpoolController, error) {
	if d == nil {
		return nil, fmt.Errorf("%w: response spool is not configured", errGatewayResponseBufferInvalid)
	}
	if d.gatewayResponseSpool == nil && flag.Lookup("test.v") != nil {
		// Direct handler tests historically construct Deps without a startup
		// bootstrap. Keep that path bounded and disk-backed, but make the fallback
		// impossible in a production binary. Production startup always installs an
		// explicit /var/lib controller before route registration.
		directory, err := os.MkdirTemp("", "bridgeops-test-upstream-response-spool-")
		if err != nil {
			return nil, err
		}
		if _, err := d.ConfigureGatewayUpstreamResponseSpool(DefaultGatewayUpstreamResponseSpoolLimits(directory), time.Now().UTC()); err != nil {
			_ = os.Remove(directory)
			if d.gatewayResponseSpool == nil {
				return nil, err
			}
		}
	}
	if d.gatewayResponseSpool == nil {
		return nil, fmt.Errorf("%w: response spool is not configured", errGatewayResponseBufferInvalid)
	}
	return d.gatewayResponseSpool, nil
}

func (c *gatewayUpstreamResponseSpoolController) spool(ctx context.Context, reader io.Reader, onFirstByte func(), admissionTimeout ...time.Duration) (*gatewayUpstreamResponseSpool, error) {
	queueTimeout := time.Duration(0)
	if len(admissionTimeout) > 0 {
		queueTimeout = admissionTimeout[0]
	}
	if c == nil {
		return nil, fmt.Errorf("%w: response spool controller is unavailable", errGatewayResponseBufferInvalid)
	}
	return spoolGatewayUpstreamResponse(ctx, reader, gatewayUpstreamResponseSpoolOptions{
		AdmissionTimeout: queueTimeout,
		TempDir:          c.tempDir,
		MaxBytes:         c.maxResponseBytes,
		PrefixBytes:      c.prefixBytes,
		OnFirstByte:      onFirstByte,
		Admission:        c.admission,
	})
}
