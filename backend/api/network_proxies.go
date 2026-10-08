package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/netproxy"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/gin-gonic/gin"
)

const (
	networkProxyTestResponseLimit  = 64 << 10
	networkProxyRequestLimit       = 32 << 10
	networkProxyImportRequestLimit = netproxy.MaxImportContentBytes*2 + 64<<10
)

func registerNetworkProxies(g *gin.RouterGroup, d *Deps) {
	gp := g.Group("/network-proxies")
	gp.GET("", func(c *gin.Context) { listNetworkProxies(c, d) })
	gp.POST("", func(c *gin.Context) { createNetworkProxy(c, d) })
	gp.POST("/import", func(c *gin.Context) { importNetworkProxies(c, d) })
	gp.PUT("/:id", func(c *gin.Context) { updateNetworkProxy(c, d) })
	gp.POST("/:id/test", func(c *gin.Context) { testNetworkProxy(c, d) })
	gp.DELETE("/:id", func(c *gin.Context) { deleteNetworkProxy(c, d) })
}

type networkProxyOutput struct {
	ID                uint       `json:"id"`
	Name              string     `json:"name"`
	Protocol          string     `json:"protocol"`
	Host              string     `json:"host"`
	Port              int        `json:"port"`
	Source            string     `json:"source"`
	CleanIP           bool       `json:"clean_ip"`
	Enabled           bool       `json:"enabled"`
	AuthConfigured    bool       `json:"auth_configured"`
	BoundChannelCount int64      `json:"bound_channel_count"`
	LastTestedAt      *time.Time `json:"last_tested_at,omitempty"`
	LastLatencyMS     *int64     `json:"last_latency_ms,omitempty"`
	LastEgressIP      string     `json:"last_egress_ip,omitempty"`
	LastError         string     `json:"last_error,omitempty"`
	CreatedAt         time.Time  `json:"created_at"`
	UpdatedAt         time.Time  `json:"updated_at"`
}

type networkProxyInput struct {
	Name             string `json:"name" binding:"required"`
	Protocol         string `json:"protocol" binding:"required"`
	Host             string `json:"host" binding:"required"`
	Port             int    `json:"port" binding:"required"`
	Username         string `json:"username"`
	Password         string `json:"password"`
	ClearCredentials bool   `json:"clear_credentials"`
	Source           string `json:"source"`
	CleanIP          bool   `json:"clean_ip"`
	Enabled          *bool  `json:"enabled"`
}

type networkProxyImportInput struct {
	Format          netproxy.ImportFormat `json:"format"`
	Content         string                `json:"content"`
	SubscriptionURL string                `json:"subscription_url"`
	CleanIP         bool                  `json:"clean_ip"`
	Enabled         *bool                 `json:"enabled"`
}

type networkProxyTestResult struct {
	OK        bool   `json:"ok"`
	LatencyMS int64  `json:"latency_ms"`
	IP        string `json:"ip"`
	Provider  string `json:"provider"`
	Error     string `json:"error,omitempty"`
}

func listNetworkProxies(c *gin.Context, d *Deps) {
	if d.NetworkProxies == nil {
		fail(c, http.StatusServiceUnavailable, errors.New("network proxy storage is unavailable"))
		return
	}
	list, err := d.NetworkProxies.List()
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	output, err := networkProxyOutputsFor(d, list)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": output})
}

func createNetworkProxy(c *gin.Context, d *Deps) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, networkProxyRequestLimit)
	var in networkProxyInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	proxy, err := makeNetworkProxy(d, in, "manual")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if err := d.NetworkProxies.Create(proxy); err != nil {
		fail(c, http.StatusConflict, err)
		return
	}
	output, err := networkProxyOutputFor(d, proxy)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": output})
}

func updateNetworkProxy(c *gin.Context, d *Deps) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, networkProxyRequestLimit)
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	current, err := d.NetworkProxies.FindByID(id)
	if err != nil {
		fail(c, http.StatusNotFound, err)
		return
	}
	var in networkProxyInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	updated, err := updateNetworkProxyFromInput(d, current, in)
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if err := d.NetworkProxies.Update(updated); err != nil {
		fail(c, http.StatusConflict, err)
		return
	}
	output, err := networkProxyOutputFor(d, updated)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": output})
}

func deleteNetworkProxy(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if err := d.NetworkProxies.Delete(id); err != nil {
		if errors.Is(err, storage.ErrNetworkProxyInUse) {
			fail(c, http.StatusConflict, errors.New("该代理仍被渠道使用，请先解绑后再删除"))
			return
		}
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func importNetworkProxies(c *gin.Context, d *Deps) {
	// Bound JSON decoding before allocating a pasted subscription. The content
	// itself is checked again by netproxy.ParseImport after unescaping.
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, networkProxyImportRequestLimit)
	var in networkProxyImportInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	content := strings.TrimSpace(in.Content)
	if len(content) > netproxy.MaxImportContentBytes {
		fail(c, http.StatusBadRequest, errors.New("代理导入内容超过 2 MiB 限制"))
		return
	}
	var err error
	if strings.TrimSpace(in.SubscriptionURL) != "" {
		if content != "" {
			fail(c, http.StatusBadRequest, errors.New("只能填写导入内容或订阅 URL 其中之一"))
			return
		}
		content, err = netproxy.FetchSubscription(c.Request.Context(), in.SubscriptionURL)
		if err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
	}
	parsed, err := netproxy.ParseImport(in.Format, content)
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	enabled := true
	if in.Enabled != nil {
		enabled = *in.Enabled
	}
	existing, err := d.NetworkProxies.List()
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	usedNames := make(map[string]struct{}, len(existing)+len(parsed.Endpoints))
	for _, item := range existing {
		usedNames[strings.ToLower(item.Name)] = struct{}{}
	}
	skipped := append([]netproxy.ImportIssue(nil), parsed.Skipped...)
	createdProfiles := make([]*storage.NetworkProxy, 0, len(parsed.Endpoints))
	for _, imported := range parsed.Endpoints {
		if len(createdProfiles) >= netproxy.MaxImportedEndpoints {
			skipped = appendNetworkProxyImportIssue(skipped, netproxy.ImportIssue{Entry: "其余条目", Reason: "一次最多导入 500 个网络代理"})
			break
		}
		name := uniqueNetworkProxyName(imported.Name, usedNames)
		credentials, err := (netproxy.Credentials{Username: imported.Endpoint.Username, Password: imported.Endpoint.Password}).Encode()
		if err != nil {
			skipped = appendNetworkProxyImportIssue(skipped, netproxy.ImportIssue{Entry: name, Reason: "无法编码代理凭据"})
			continue
		}
		cipherText, err := encryptNetworkProxyCredentials(d, credentials)
		if err != nil {
			skipped = appendNetworkProxyImportIssue(skipped, netproxy.ImportIssue{Entry: name, Reason: "无法加密代理凭据"})
			continue
		}
		profile := &storage.NetworkProxy{
			Name:             name,
			Protocol:         string(imported.Endpoint.Protocol),
			Host:             imported.Endpoint.Host,
			Port:             imported.Endpoint.Port,
			CredentialCipher: cipherText,
			Source:           imported.Source,
			CleanIP:          in.CleanIP,
			Enabled:          enabled,
		}
		if err := d.NetworkProxies.Create(profile); err != nil {
			skipped = appendNetworkProxyImportIssue(skipped, netproxy.ImportIssue{Entry: name, Reason: "保存代理失败"})
			continue
		}
		createdProfiles = append(createdProfiles, profile)
	}
	createdRows := make([]storage.NetworkProxy, 0, len(createdProfiles))
	for _, profile := range createdProfiles {
		if profile != nil {
			createdRows = append(createdRows, *profile)
		}
	}
	created, err := networkProxyOutputsFor(d, createdRows)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"created": created, "skipped": skipped}})
}

func testNetworkProxy(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	profile, err := d.NetworkProxies.FindByID(id)
	if err != nil {
		fail(c, http.StatusNotFound, err)
		return
	}
	endpoint, err := resolveNetworkProxyEndpoint(d, profile)
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	result := testNetworkProxyEndpoint(c.Request.Context(), endpoint)
	testedAt := time.Now().UTC()
	var latency *int64
	if result.OK {
		latency = &result.LatencyMS
	}
	if err := d.NetworkProxies.UpdateTestResult(profile.ID, testedAt, latency, result.IP, result.Error); err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": result})
}

func networkProxyOutputFor(d *Deps, profile *storage.NetworkProxy) (networkProxyOutput, error) {
	if profile == nil {
		return networkProxyOutput{}, errors.New("network proxy is required")
	}
	items, err := networkProxyOutputsFor(d, []storage.NetworkProxy{*profile})
	if err != nil {
		return networkProxyOutput{}, err
	}
	return items[0], nil
}

func networkProxyOutputsFor(d *Deps, profiles []storage.NetworkProxy) ([]networkProxyOutput, error) {
	if d == nil || d.NetworkProxies == nil {
		return nil, errors.New("network proxy storage is unavailable")
	}
	ids := make([]uint, 0, len(profiles))
	for i := range profiles {
		ids = append(ids, profiles[i].ID)
	}
	boundCounts, err := d.NetworkProxies.BoundChannelCounts(ids)
	if err != nil {
		return nil, err
	}
	output := make([]networkProxyOutput, 0, len(profiles))
	for i := range profiles {
		output = append(output, networkProxyOutputWithBoundCount(&profiles[i], boundCounts[profiles[i].ID]))
	}
	return output, nil
}

func networkProxyOutputWithBoundCount(profile *storage.NetworkProxy, boundCount int64) networkProxyOutput {
	if profile == nil {
		return networkProxyOutput{}
	}
	endpoint := netproxy.Endpoint{
		Protocol: netproxy.Protocol(profile.Protocol),
		Host:     profile.Host,
		Port:     profile.Port,
	}
	normalized, endpointErr := endpoint.Normalized()
	name := netproxy.SafeDisplayName(profile.Name, endpoint)
	protocol, host, port := profile.Protocol, profile.Host, profile.Port
	if endpointErr != nil {
		name, protocol, host, port = "无效代理", "", "", 0
	} else {
		endpoint = normalized
		protocol, host, port = string(endpoint.Protocol), endpoint.Host, endpoint.Port
		if strings.EqualFold(profile.Source, "clash") {
			name = netproxy.SafeDisplayName("", endpoint)
		}
	}
	return networkProxyOutput{
		ID:                profile.ID,
		Name:              name,
		Protocol:          protocol,
		Host:              host,
		Port:              port,
		Source:            profile.Source,
		CleanIP:           profile.CleanIP,
		Enabled:           profile.Enabled,
		AuthConfigured:    profile.CredentialCipher != "",
		BoundChannelCount: boundCount,
		LastTestedAt:      profile.LastTestedAt,
		LastLatencyMS:     profile.LastLatencyMS,
		LastEgressIP:      profile.LastEgressIP,
		LastError:         netproxy.SanitizeDiagnostic(profile.LastError, 240),
		CreatedAt:         profile.CreatedAt,
		UpdatedAt:         profile.UpdatedAt,
	}
}

func makeNetworkProxy(d *Deps, in networkProxyInput, fallbackSource string) (*storage.NetworkProxy, error) {
	name, err := netproxy.ValidateNetworkProxyName(in.Name)
	if err != nil {
		return nil, err
	}
	endpoint, err := (netproxy.Endpoint{
		Protocol: netproxy.Protocol(in.Protocol),
		Host:     in.Host,
		Port:     in.Port,
		Username: in.Username,
		Password: in.Password,
	}).Normalized()
	if err != nil {
		return nil, err
	}
	credentials, err := (netproxy.Credentials{Username: endpoint.Username, Password: endpoint.Password}).Encode()
	if err != nil {
		return nil, err
	}
	cipherText, err := encryptNetworkProxyCredentials(d, credentials)
	if err != nil {
		return nil, err
	}
	enabled := true
	if in.Enabled != nil {
		enabled = *in.Enabled
	}
	return &storage.NetworkProxy{
		Name:             name,
		Protocol:         string(endpoint.Protocol),
		Host:             endpoint.Host,
		Port:             endpoint.Port,
		CredentialCipher: cipherText,
		Source:           normalizeNetworkProxySource(in.Source, fallbackSource),
		CleanIP:          in.CleanIP,
		Enabled:          enabled,
	}, nil
}

func updateNetworkProxyFromInput(d *Deps, current *storage.NetworkProxy, in networkProxyInput) (*storage.NetworkProxy, error) {
	if current == nil {
		return nil, errors.New("network proxy is required")
	}
	name, err := netproxy.ValidateNetworkProxyName(in.Name)
	if err != nil {
		return nil, err
	}
	credentials, err := decryptNetworkProxyCredentials(d, current.CredentialCipher)
	if err != nil {
		return nil, err
	}
	if in.ClearCredentials {
		credentials = netproxy.Credentials{}
	} else {
		if strings.TrimSpace(in.Username) != "" {
			credentials.Username = in.Username
		}
		if in.Password != "" {
			credentials.Password = in.Password
		}
	}
	endpoint, err := (netproxy.Endpoint{
		Protocol: netproxy.Protocol(in.Protocol),
		Host:     in.Host,
		Port:     in.Port,
		Username: credentials.Username,
		Password: credentials.Password,
	}).Normalized()
	if err != nil {
		return nil, err
	}
	rawCredentials, err := (netproxy.Credentials{Username: endpoint.Username, Password: endpoint.Password}).Encode()
	if err != nil {
		return nil, err
	}
	cipherText, err := encryptNetworkProxyCredentials(d, rawCredentials)
	if err != nil {
		return nil, err
	}
	current.Name = name
	current.Protocol = string(endpoint.Protocol)
	current.Host = endpoint.Host
	current.Port = endpoint.Port
	current.CredentialCipher = cipherText
	current.Source = normalizeNetworkProxySource(in.Source, current.Source)
	current.CleanIP = in.CleanIP
	if in.Enabled != nil {
		current.Enabled = *in.Enabled
	}
	return current, nil
}

func normalizeNetworkProxySource(value, fallback string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	switch value {
	case "manual", "uri", "clash", "3x-ui", "mihomo":
		return value
	}
	if strings.TrimSpace(fallback) == "" {
		return "manual"
	}
	return fallback
}

func encryptNetworkProxyCredentials(d *Deps, raw string) (string, error) {
	if strings.TrimSpace(raw) == "" {
		return "", nil
	}
	if d == nil || d.Cipher == nil {
		return "", errors.New("credential cipher is unavailable")
	}
	return d.Cipher.Encrypt(raw)
}

func decryptNetworkProxyCredentials(d *Deps, cipherText string) (netproxy.Credentials, error) {
	if strings.TrimSpace(cipherText) == "" {
		return netproxy.Credentials{}, nil
	}
	if d == nil || d.Cipher == nil {
		return netproxy.Credentials{}, errors.New("credential cipher is unavailable")
	}
	raw, err := d.Cipher.Decrypt(cipherText)
	if err != nil {
		return netproxy.Credentials{}, errors.New("decrypt proxy credentials")
	}
	return netproxy.DecodeCredentials(raw)
}

func resolveNetworkProxyEndpoint(d *Deps, profile *storage.NetworkProxy) (string, error) {
	credentials, err := decryptNetworkProxyCredentials(d, profile.CredentialCipher)
	if err != nil {
		return "", err
	}
	return (netproxy.Endpoint{
		Protocol: netproxy.Protocol(profile.Protocol),
		Host:     profile.Host,
		Port:     profile.Port,
		Username: credentials.Username,
		Password: credentials.Password,
	}).URL()
}

func appendNetworkProxyImportIssue(items []netproxy.ImportIssue, issue netproxy.ImportIssue) []netproxy.ImportIssue {
	if len(items) >= 100 {
		return items
	}
	return append(items, issue)
}

func uniqueNetworkProxyName(preferred string, used map[string]struct{}) string {
	base := netproxy.TruncateDisplayName(strings.TrimSpace(preferred), netproxy.MaxNetworkProxyNameBytes)
	if base == "" {
		base = "代理"
	}
	for suffix := 1; ; suffix++ {
		candidate := base
		if suffix > 1 {
			suffixText := fmt.Sprintf(" (%d)", suffix)
			candidate = netproxy.TruncateDisplayName(base, netproxy.MaxNetworkProxyNameBytes-len(suffixText)) + suffixText
		}
		key := strings.ToLower(candidate)
		if _, exists := used[key]; !exists {
			used[key] = struct{}{}
			return candidate
		}
	}
}

func testNetworkProxyEndpoint(ctx context.Context, endpoint string) networkProxyTestResult {
	client, err := netproxy.NewClient(&http.Client{Timeout: 15 * time.Second}, endpoint)
	if err != nil {
		return networkProxyTestResult{Error: "创建代理客户端失败"}
	}
	providers := append([]proxyIPProvider(nil), proxyIPProviders...)
	for _, provider := range providers {
		started := time.Now()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, provider.url, nil)
		if err != nil {
			continue
		}
		req.Header.Set("Accept", "application/json")
		resp, err := client.Do(req)
		latency := time.Since(started).Milliseconds()
		if err != nil {
			provider.lastErr = err
			continue
		}
		body, readErr := ioReadAndClose(resp)
		if readErr != nil {
			provider.lastErr = readErr
			continue
		}
		if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
			provider.lastErr = fmt.Errorf("HTTP %d", resp.StatusCode)
			continue
		}
		ip := provider.parseIP(body)
		if ip == "" {
			provider.lastErr = errors.New("未返回出口 IP")
			continue
		}
		return networkProxyTestResult{OK: true, LatencyMS: latency, IP: ip, Provider: provider.name}
	}
	for index := len(providers) - 1; index >= 0; index-- {
		if providers[index].lastErr != nil {
			return networkProxyTestResult{Error: fmt.Sprintf("%s: %s", providers[index].name, safeNetworkProxyError(providers[index].lastErr))}
		}
	}
	return networkProxyTestResult{Error: "代理测试失败"}
}

func safeNetworkProxyError(err error) string {
	if err == nil {
		return ""
	}
	return netproxy.SanitizeDiagnostic(err.Error(), 240)
}

func ioReadAndClose(response *http.Response) ([]byte, error) {
	if response == nil || response.Body == nil {
		return nil, errors.New("empty proxy test response")
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, networkProxyTestResponseLimit+1))
	if err != nil {
		return nil, err
	}
	if len(body) > networkProxyTestResponseLimit {
		return nil, errors.New("proxy test response exceeded 64 KiB")
	}
	return body, nil
}
