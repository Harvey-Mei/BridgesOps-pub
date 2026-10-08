package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/config"
	"github.com/Harvey-Mei/BridgeOps/backend/netproxy"
	"github.com/gin-gonic/gin"
)

type proxyTestResult struct {
	OK        bool   `json:"ok"`
	LatencyMS int64  `json:"latency_ms"`
	IP        string `json:"ip"`
	Provider  string `json:"provider"`
	Error     string `json:"error,omitempty"`
}

type proxyIPProvider struct {
	name    string
	url     string
	parseIP func([]byte) string
	lastErr error
}

var proxyIPProviders = []proxyIPProvider{
	{
		name: "ipify",
		url:  "https://api.ipify.org?format=json",
		parseIP: func(body []byte) string {
			var out struct {
				IP string `json:"ip"`
			}
			_ = json.Unmarshal(body, &out)
			return out.IP
		},
	},
	{
		name: "ipinfo",
		url:  "https://ipinfo.io/json",
		parseIP: func(body []byte) string {
			var out struct {
				IP string `json:"ip"`
			}
			_ = json.Unmarshal(body, &out)
			return out.IP
		},
	},
}

func testProxy(c *gin.Context, d *Deps) {
	var in settingsProxyConfigInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	current := config.ProxyConfig{}
	if d != nil && d.Runtime != nil {
		cfg, err := config.LoadFile(d.Runtime.ConfigPath())
		if err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		current = cfg.Proxy
	}
	proxyURL, err := in.merge(current).URL()
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}

	providers := make([]proxyIPProvider, len(proxyIPProviders))
	copy(providers, proxyIPProviders)

	client, err := netproxy.NewClient(&http.Client{Timeout: 15 * time.Second}, proxyURL)
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}

	for i := range providers {
		provider := providers[i]
		started := time.Now()
		ctx, cancel := context.WithTimeout(c.Request.Context(), 15*time.Second)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, provider.url, nil)
		var response *http.Response
		if err == nil {
			req.Header.Set("Accept", "application/json")
			response, err = client.Do(req)
		}
		latency := time.Since(started).Milliseconds()
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		var body []byte
		if err == nil && status >= http.StatusOK && status < http.StatusMultipleChoices {
			body, err = ioReadAndClose(response)
		} else if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		cancel()
		if err != nil {
			providers[i].lastErr = err
			continue
		}
		if status < http.StatusOK || status >= http.StatusMultipleChoices {
			providers[i].lastErr = fmt.Errorf("HTTP %d", status)
			continue
		}
		ip := provider.parseIP(body)
		if ip == "" {
			providers[i].lastErr = fmt.Errorf("%s 未返回 IP", provider.name)
			continue
		}
		c.JSON(http.StatusOK, gin.H{"data": proxyTestResult{
			OK:        true,
			LatencyMS: latency,
			IP:        ip,
			Provider:  provider.name,
		}})
		return
	}

	msg := "代理测试失败"
	for i := len(providers) - 1; i >= 0; i-- {
		if providers[i].lastErr != nil {
			msg = fmt.Sprintf("%s: %s", providers[i].name, safeNetworkProxyError(providers[i].lastErr))
			break
		}
	}
	c.JSON(http.StatusOK, gin.H{"data": proxyTestResult{OK: false, Error: msg}})
}
