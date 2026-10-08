package api

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/Harvey-Mei/BridgeOps/backend/modelcheck"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

func TestModelCheckRunHTTPErrorDoesNotReportLocalDeferralAsUpstreamFailure(t *testing.T) {
	status, err := modelCheckRunHTTPError(fmt.Errorf("capacity: %w", modelcheck.ErrProbeDeferred))
	if status != http.StatusTooManyRequests {
		t.Fatalf("deferred status = %d, want %d", status, http.StatusTooManyRequests)
	}
	if err == nil || errors.Is(err, modelcheck.ErrProbeDeferred) || err.Error() != "本机当前为在线请求保留上游容量，本次模型检测已延后，请稍后重试" {
		t.Fatalf("deferred public error = %v", err)
	}
}

func TestModelCheckRunHTTPErrorPreservesOtherMappings(t *testing.T) {
	status, err := modelCheckRunHTTPError(modelcheck.ErrRequestBudgetExceeded)
	if status != http.StatusTooManyRequests || !errors.Is(err, modelcheck.ErrRequestBudgetExceeded) {
		t.Fatalf("budget mapping = (%d, %v)", status, err)
	}

	upstreamErr := errors.New("upstream unavailable")
	status, err = modelCheckRunHTTPError(upstreamErr)
	if status != http.StatusBadGateway || !errors.Is(err, upstreamErr) {
		t.Fatalf("upstream mapping = (%d, %v)", status, err)
	}
}

func TestModelCheckRunViewRejectsUnsafeSVGArtifact(t *testing.T) {
	run := storage.ModelCheckRun{EvidenceJSON: `[{"kind":"pelican_svg","artifact_svg":"<svg xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"0 0 100 100\"><script>alert(1)</script></svg>","response":"bounded"}]`}
	view := modelCheckRunView(run)
	if len(view.Evidence) != 1 {
		t.Fatal("missing evidence")
	}
	if _, exists := view.Evidence[0]["artifact_svg"]; exists {
		t.Fatal("unsafe legacy artifact exposed")
	}
}
