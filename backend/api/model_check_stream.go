package api

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/config"
	"github.com/Harvey-Mei/BridgeOps/backend/modelcheck"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/gin-gonic/gin"
)

type modelCheckStreamItem struct {
	event *modelcheck.ProgressEvent
	run   *storage.ModelCheckRun
	err   error
	done  bool
}

func streamManualModelCheck(c *gin.Context, d *Deps, selection modelcheck.TargetDescriptor, cfg config.ModelCheckConfig) {
	ctx, cancel := context.WithCancel(c.Request.Context())
	defer cancel()
	events := make(chan modelCheckStreamItem, 16)
	send := func(item modelCheckStreamItem) {
		select {
		case events <- item:
		case <-ctx.Done():
		}
	}
	ctx = modelcheck.WithProgressObserver(ctx, func(event modelcheck.ProgressEvent) { send(modelCheckStreamItem{event: &event}) })
	go func() {
		defer close(events)
		run, err := d.ModelCheck.RunManualTarget(ctx, selection, cfg)
		send(modelCheckStreamItem{run: run, err: err, done: true})
	}()
	c.Header("Content-Type", "text/event-stream; charset=utf-8")
	c.Header("Cache-Control", "no-cache, no-store")
	c.Header("X-Accel-Buffering", "no")
	write := func(kind string, value any) bool {
		raw, err := json.Marshal(value)
		if err != nil {
			return false
		}
		if _, err = fmt.Fprintf(c.Writer, "event: %s\ndata: %s\n\n", kind, raw); err != nil {
			return false
		}
		c.Writer.Flush()
		return true
	}
	if !write("progress", modelcheck.ProgressEvent{Type: "preparing", At: time.Now().UTC(), Message: "正在核对渠道、分组与模型"}) {
		return
	}
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := fmt.Fprint(c.Writer, ": keep-alive\n\n"); err != nil {
				return
			}
			c.Writer.Flush()
		case item, ok := <-events:
			if !ok {
				return
			}
			if item.event != nil {
				if !write("progress", item.event) {
					return
				}
				continue
			}
			if item.done {
				if item.err != nil {
					status, err := modelCheckRunHTTPError(item.err)
					write("error", gin.H{"message": err.Error(), "status": status})
				} else if item.run != nil {
					write("complete", modelCheckRunView(*item.run))
				}
				return
			}
		}
	}
}
