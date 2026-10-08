package api

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/config"
	"github.com/Harvey-Mei/BridgeOps/backend/modelcheck"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
	"github.com/gin-gonic/gin"
)

type streamModelCheckRunner struct {
	choice           modelcheck.TargetDescriptor
	started, release chan struct{}
	calls            int
}

func (r *streamModelCheckRunner) DiscoverTargets(context.Context, time.Time) ([]modelcheck.TargetDescriptor, error) {
	return nil, nil
}
func (r *streamModelCheckRunner) ReadManualModelTargets(context.Context, modelcheck.TargetDescriptor) ([]modelcheck.TargetDescriptor, error) {
	return []modelcheck.TargetDescriptor{r.choice}, nil
}
func (r *streamModelCheckRunner) RunProbe(ctx context.Context, _ modelcheck.TargetDescriptor, request modelcheck.ProbeRequest) (modelcheck.ProbeResponse, error) {
	r.calls++
	modelcheck.EmitProgress(ctx, modelcheck.ProgressEvent{Type: "request_sent", Kind: request.Kind, Path: "/v1/chat/completions", RequestBody: `{"model":"gpt-test"}`})
	if r.calls == 1 {
		close(r.started)
		select {
		case <-r.release:
		case <-ctx.Done():
			return modelcheck.ProbeResponse{}, ctx.Err()
		}
	}
	return modelcheck.ProbeResponse{Text: "{}", StatusCode: 200, TotalTokens: 2}, nil
}

func TestManualModelCheckStreamFlushesProgressBeforeCompletion(t *testing.T) {
	db := testdb.Open(t)
	channels := storage.NewChannels(db)
	channel := &storage.Channel{Name: "selected", Type: storage.ChannelTypeNewAPI, SiteURL: "https://example.invalid"}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	runner := &streamModelCheckRunner{choice: modelcheck.TargetDescriptor{ChannelID: channel.ID, UpstreamAccountIdentityRevision: channel.UpstreamAccountIdentityRevision, ChannelName: channel.Name, GroupName: "group", ModelName: "gpt-test", App: storage.GatewayAppCodex}, started: make(chan struct{}), release: make(chan struct{})}
	service := modelcheck.New(modelcheck.Options{Store: storage.NewModelChecks(db), Runner: runner})
	engine := gin.New()
	engine.POST("/run", func(c *gin.Context) {
		streamManualModelCheck(c, &Deps{ModelCheck: service}, runner.choice, config.ModelCheckConfig{})
	})
	server := httptest.NewServer(engine)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/run", nil)
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatal("not an SSE response")
	}
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	kind := ""
	sawRequest, sawComplete := false, false
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "event: ") {
			kind = strings.TrimPrefix(line, "event: ")
		}
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		raw := []byte(strings.TrimPrefix(line, "data: "))
		if kind == "progress" {
			var event modelcheck.ProgressEvent
			if err := json.Unmarshal(raw, &event); err != nil {
				t.Fatal(err)
			}
			if event.Type == "request_sent" && !sawRequest {
				sawRequest = true
				close(runner.release)
			}
		}
		if kind == "complete" {
			var run modelCheckRunOutput
			if err := json.Unmarshal(raw, &run); err != nil {
				t.Fatal(err)
			}
			if !sawRequest || run.ID == 0 || run.ChannelID != channel.ID || run.RequestCount != 2 {
				t.Fatalf("invalid completion: %+v", run)
			}
			sawComplete = true
		}
		if kind == "error" {
			t.Fatalf("unexpected stream error: %s", raw)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if !sawRequest || !sawComplete {
		t.Fatal("stream failed to send incremental progress and completion")
	}
}
