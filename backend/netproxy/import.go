package netproxy

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

type ImportFormat string

const (
	ImportFormatAuto  ImportFormat = "auto"
	ImportFormatURI   ImportFormat = "uri"
	ImportFormatClash ImportFormat = "clash"
	ImportFormat3XUI  ImportFormat = "3x-ui"

	// MaxImportContentBytes bounds both pasted configurations and fetched
	// subscriptions. It protects the admin endpoint from treating a malformed
	// subscription as an unbounded memory or database import.
	MaxImportContentBytes = 2 << 20
	// MaxImportedEndpoints keeps one batch useful for normal subscriptions
	// without letting a single import flood the proxy table.
	MaxImportedEndpoints = 500
	maxImportIssues      = 100
)

// ImportedEndpoint carries a decrypted endpoint only inside the import path.
// The endpoint is intentionally excluded from JSON responses.
type ImportedEndpoint struct {
	Name     string   `json:"name"`
	Endpoint Endpoint `json:"-"`
	Source   string   `json:"source"`
}

type ImportIssue struct {
	Entry  string `json:"entry"`
	Reason string `json:"reason"`
}

type ImportResult struct {
	Endpoints []ImportedEndpoint `json:"endpoints"`
	Skipped   []ImportIssue      `json:"skipped"`
}

func ParseImport(format ImportFormat, content string) (ImportResult, error) {
	content = strings.TrimSpace(content)
	if content == "" {
		return ImportResult{}, errors.New("proxy import content is required")
	}
	if len(content) > MaxImportContentBytes {
		return ImportResult{}, errors.New("proxy import content exceeds the 2 MiB limit")
	}
	format = ImportFormat(strings.ToLower(strings.TrimSpace(string(format))))
	if format == "" {
		format = ImportFormatAuto
	}

	if format == ImportFormatAuto {
		if looksLikeClash(content) {
			format = ImportFormatClash
		} else if decoded, ok := decodeSubscription(content); ok {
			content = decoded
			if looksLikeClash(content) {
				format = ImportFormatClash
			} else {
				format = ImportFormatURI
			}
		} else {
			format = ImportFormatURI
		}
	}

	var result ImportResult
	var err error
	switch format {
	case ImportFormatURI:
		result = parseURIList(content, "uri")
	case ImportFormat3XUI:
		if decoded, ok := decodeSubscription(content); ok {
			content = decoded
		}
		result = parseURIList(content, "3x-ui")
	case ImportFormatClash:
		result, err = parseClash(content)
	default:
		return ImportResult{}, fmt.Errorf("unsupported proxy import format: %s", format)
	}
	if err != nil {
		return ImportResult{}, err
	}
	result.Endpoints = deduplicateEndpoints(result.Endpoints)
	if len(result.Endpoints) == 0 && len(result.Skipped) == 0 {
		return ImportResult{}, errors.New("no proxy endpoints found")
	}
	return result, nil
}

func looksLikeClash(content string) bool {
	var document struct {
		Proxies []any `yaml:"proxies"`
	}
	if err := yaml.Unmarshal([]byte(content), &document); err != nil {
		return false
	}
	return len(document.Proxies) > 0
}

func decodeSubscription(content string) (string, bool) {
	compact := strings.Map(func(r rune) rune {
		if r == '\r' || r == '\n' || r == ' ' || r == '\t' {
			return -1
		}
		return r
	}, content)
	if len(compact) < 12 || strings.Contains(compact, "://") {
		return "", false
	}
	for _, encoding := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		decoded, err := encoding.DecodeString(compact)
		if err != nil {
			continue
		}
		text := strings.TrimSpace(string(decoded))
		if strings.Contains(text, "://") || looksLikeClash(text) {
			return text, true
		}
	}
	return "", false
}

func parseURIList(content, source string) ImportResult {
	result := ImportResult{}
	lines := strings.Split(content, "\n")
	for index, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		endpoint, err := ParseURL(line)
		if err == nil {
			if !result.addEndpoint(ImportedEndpoint{
				Name:     SafeDisplayName("", endpoint),
				Endpoint: endpoint,
				Source:   source,
			}) {
				break
			}
			continue
		}
		if endpoint, ok := parseHostPortLine(line); ok {
			if !result.addEndpoint(ImportedEndpoint{
				Name:     SafeDisplayName("", endpoint),
				Endpoint: endpoint,
				Source:   source,
			}) {
				break
			}
			continue
		}
		result.addIssue(ImportIssue{Entry: importEntryLabel(index), Reason: importSkipReason(line)})
	}
	return result
}

func parseHostPortLine(line string) (Endpoint, bool) {
	if strings.Contains(line, "://") || strings.Contains(line, "[") {
		return Endpoint{}, false
	}
	parts := strings.SplitN(line, ":", 4)
	if len(parts) != 2 && len(parts) != 4 {
		return Endpoint{}, false
	}
	port, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil {
		return Endpoint{}, false
	}
	endpoint := Endpoint{Protocol: ProtocolHTTP, Host: strings.TrimSpace(parts[0]), Port: port}
	if len(parts) == 4 {
		endpoint.Username = parts[2]
		endpoint.Password = parts[3]
	}
	normalized, err := endpoint.Normalized()
	return normalized, err == nil
}

func importEntryLabel(index int) string {
	return fmt.Sprintf("第 %d 项", index+1)
}

func importSkipReason(line string) string {
	lower := strings.ToLower(strings.TrimSpace(line))
	for _, prefix := range []string{"vless://", "vmess://", "trojan://", "ss://", "ssr://", "hysteria://", "hysteria2://", "hy2://", "tuic://", "wireguard://"} {
		if strings.HasPrefix(lower, prefix) {
			return "该节点协议需要先由 Mihomo、Clash、Xray 或 sing-box 提供 HTTP/SOCKS 出口"
		}
	}
	return "仅支持 HTTP、HTTPS、SOCKS5 代理地址或 host:port[:username:password]"
}

type clashDocument struct {
	Proxies []map[string]any `yaml:"proxies"`
}

func parseClash(content string) (ImportResult, error) {
	var document clashDocument
	if err := yaml.Unmarshal([]byte(content), &document); err != nil {
		return ImportResult{}, errors.New("parse Clash configuration")
	}
	result := ImportResult{}
	for index, item := range document.Proxies {
		kind := strings.ToLower(mapString(item, "type"))
		if kind != "http" && kind != "socks5" && kind != "socks5h" {
			result.addIssue(ImportIssue{Entry: clashEntryLabel(index), Reason: "该 Clash 节点协议需要先提供 HTTP/SOCKS 出口"})
			continue
		}
		if mapBool(item, "skip-cert-verify") {
			result.addIssue(ImportIssue{Entry: clashEntryLabel(index), Reason: "不导入要求跳过 TLS 证书校验的代理节点"})
			continue
		}
		port, ok := mapPort(item["port"])
		if !ok {
			result.addIssue(ImportIssue{Entry: clashEntryLabel(index), Reason: "缺少有效端口"})
			continue
		}
		protocol := Protocol(kind)
		if kind == "http" && mapBool(item, "tls") {
			protocol = ProtocolHTTPS
		}
		endpoint, err := (Endpoint{
			Protocol: protocol,
			Host:     mapString(item, "server"),
			Port:     port,
			Username: mapString(item, "username"),
			Password: mapString(item, "password"),
		}).Normalized()
		if err != nil {
			result.addIssue(ImportIssue{Entry: clashEntryLabel(index), Reason: "代理地址无效"})
			continue
		}
		// A Clash node name is untrusted subscription content. Never persist or
		// return it: a normalized endpoint label cannot carry URI credentials,
		// query tokens, or a password embedded in the source name.
		if !result.addEndpoint(ImportedEndpoint{Name: SafeDisplayName("", endpoint), Endpoint: endpoint, Source: "clash"}) {
			break
		}
	}
	return result, nil
}

func mapString(item map[string]any, key string) string {
	value, _ := item[key].(string)
	return strings.TrimSpace(value)
}

func mapBool(item map[string]any, key string) bool {
	value, _ := item[key].(bool)
	return value
}

func mapPort(value any) (int, bool) {
	switch current := value.(type) {
	case int:
		return current, current > 0 && current <= 65535
	case int64:
		return int(current), current > 0 && current <= 65535
	case float64:
		return int(current), current > 0 && current <= 65535 && current == float64(int(current))
	case string:
		port, err := strconv.Atoi(strings.TrimSpace(current))
		return port, err == nil && port > 0 && port <= 65535
	default:
		return 0, false
	}
}

func clashEntryLabel(index int) string {
	return importEntryLabel(index)
}

func endpointDisplayName(endpoint Endpoint) string {
	return netHostPort(endpoint.Host, endpoint.Port)
}

func netHostPort(host string, port int) string {
	if strings.Contains(host, ":") {
		return "[" + host + "]:" + strconv.Itoa(port)
	}
	return host + ":" + strconv.Itoa(port)
}

func deduplicateEndpoints(in []ImportedEndpoint) []ImportedEndpoint {
	seen := make(map[string]struct{}, len(in))
	out := make([]ImportedEndpoint, 0, len(in))
	for _, item := range in {
		key, err := item.Endpoint.URL()
		if err != nil {
			continue
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, item)
	}
	return out
}

func (r *ImportResult) addEndpoint(endpoint ImportedEndpoint) bool {
	if len(r.Endpoints) >= MaxImportedEndpoints {
		r.addIssue(ImportIssue{Entry: "其余条目", Reason: "一次最多导入 500 个网络代理"})
		return false
	}
	r.Endpoints = append(r.Endpoints, endpoint)
	return true
}

func (r *ImportResult) addIssue(issue ImportIssue) {
	if len(r.Skipped) >= maxImportIssues {
		return
	}
	r.Skipped = append(r.Skipped, issue)
}
