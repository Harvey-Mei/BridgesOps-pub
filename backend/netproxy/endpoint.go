// Package netproxy provides the small, explicit outbound proxy boundary used
// by BridgeOps. It intentionally supports only protocols that Go's HTTP stack
// can use directly: HTTP CONNECT, HTTPS CONNECT, and SOCKS5.
package netproxy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	xproxy "golang.org/x/net/proxy"
)

type Protocol string

const (
	ProtocolHTTP   Protocol = "http"
	ProtocolHTTPS  Protocol = "https"
	ProtocolSOCKS5 Protocol = "socks5"

	MaxNetworkProxyHostBytes     = 255
	MaxNetworkProxyUsernameBytes = 256
	MaxNetworkProxyPasswordBytes = 4096
)

// Endpoint stores a decrypted proxy endpoint. It must never be serialized to
// an API response because it can contain credentials.
type Endpoint struct {
	Protocol Protocol
	Host     string
	Port     int
	Username string
	Password string
}

// CacheKey is safe for in-memory client caches: it never retains or exposes
// proxy credentials while still changing when the endpoint changes.
func CacheKey(rawProxyURL string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(rawProxyURL)))
	return hex.EncodeToString(sum[:])
}

func (e Endpoint) Normalized() (Endpoint, error) {
	e.Protocol = Protocol(strings.ToLower(strings.TrimSpace(string(e.Protocol))))
	if e.Protocol == "socks5h" {
		e.Protocol = ProtocolSOCKS5
	}
	switch e.Protocol {
	case ProtocolHTTP, ProtocolHTTPS, ProtocolSOCKS5:
	default:
		return Endpoint{}, fmt.Errorf("unsupported proxy protocol: %s", e.Protocol)
	}

	e.Host = strings.Trim(strings.TrimSpace(e.Host), "[]")
	if e.Host == "" {
		return Endpoint{}, errors.New("proxy host is required")
	}
	if len(e.Host) > MaxNetworkProxyHostBytes {
		return Endpoint{}, fmt.Errorf("proxy host exceeds %d bytes", MaxNetworkProxyHostBytes)
	}
	if strings.ContainsAny(e.Host, "/?@#") {
		return Endpoint{}, errors.New("proxy host is invalid")
	}
	if e.Port < 1 || e.Port > 65535 {
		return Endpoint{}, errors.New("proxy port must be between 1 and 65535")
	}
	if len(e.Username) > MaxNetworkProxyUsernameBytes {
		return Endpoint{}, fmt.Errorf("proxy username exceeds %d bytes", MaxNetworkProxyUsernameBytes)
	}
	if len(e.Password) > MaxNetworkProxyPasswordBytes {
		return Endpoint{}, fmt.Errorf("proxy password exceeds %d bytes", MaxNetworkProxyPasswordBytes)
	}
	return e, nil
}

func (e Endpoint) URL() (string, error) {
	normalized, err := e.Normalized()
	if err != nil {
		return "", err
	}
	u := url.URL{
		Scheme: string(normalized.Protocol),
		Host:   net.JoinHostPort(normalized.Host, strconv.Itoa(normalized.Port)),
	}
	if normalized.Username != "" || normalized.Password != "" {
		u.User = url.UserPassword(normalized.Username, normalized.Password)
	}
	return u.String(), nil
}

// ParseURL only accepts endpoint URLs. A trailing slash is accepted because
// it is commonly emitted by proxy dashboards, but paths and query strings
// otherwise create ambiguous proxy semantics and are rejected.
func ParseURL(raw string) (Endpoint, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return Endpoint{}, errors.New("parse proxy endpoint")
	}
	if u.Scheme == "" || u.Hostname() == "" || u.Port() == "" {
		return Endpoint{}, errors.New("proxy endpoint must include protocol, host, and port")
	}
	if (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return Endpoint{}, errors.New("proxy endpoint must not include a path, query, or fragment")
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil {
		return Endpoint{}, errors.New("proxy port is invalid")
	}
	endpoint := Endpoint{
		Protocol: Protocol(u.Scheme),
		Host:     u.Hostname(),
		Port:     port,
	}
	if u.User != nil {
		endpoint.Username = u.User.Username()
		endpoint.Password, _ = u.User.Password()
	}
	return endpoint.Normalized()
}

// DirectTransport returns a clone that never consults HTTP_PROXY, HTTPS_PROXY,
// or NO_PROXY. Channel traffic must opt into a selected proxy explicitly.
func DirectTransport(base *http.Transport) *http.Transport {
	if base == nil {
		base = http.DefaultTransport.(*http.Transport)
	}
	transport := base.Clone()
	transport.Proxy = nil
	transport.ProxyConnectHeader = nil
	transport.GetProxyConnectHeader = nil
	return transport
}

// NewTransport builds an isolated transport for one proxy endpoint. An empty
// proxy URL means explicit direct traffic rather than environment-derived
// proxying.
func NewTransport(base *http.Transport, rawProxyURL string) (*http.Transport, error) {
	transport := DirectTransport(base)
	if strings.TrimSpace(rawProxyURL) == "" {
		return transport, nil
	}

	endpoint, err := ParseURL(rawProxyURL)
	if err != nil {
		return nil, err
	}
	normalizedURL, err := endpoint.URL()
	if err != nil {
		return nil, err
	}
	parsed, err := url.Parse(normalizedURL)
	if err != nil {
		return nil, errors.New("parse normalized proxy endpoint")
	}

	switch endpoint.Protocol {
	case ProtocolHTTP, ProtocolHTTPS:
		transport.Proxy = http.ProxyURL(parsed)
		return transport, nil
	case ProtocolSOCKS5:
		dialer, err := xproxy.FromURL(parsed, &net.Dialer{
			Timeout:   15 * time.Second,
			KeepAlive: 30 * time.Second,
		})
		if err != nil {
			return nil, errors.New("create SOCKS5 proxy dialer")
		}
		if contextDialer, ok := dialer.(xproxy.ContextDialer); ok {
			transport.DialContext = contextDialer.DialContext
		} else {
			transport.DialContext = func(_ context.Context, network, address string) (net.Conn, error) {
				return dialer.Dial(network, address)
			}
		}
		return transport, nil
	default:
		return nil, errors.New("unsupported proxy protocol")
	}
}

// NewClient clones base while replacing its transport with an isolated direct
// or proxy transport. It preserves timeout, cookie jar, redirect policy, and
// other safe client-level settings.
func NewClient(base *http.Client, rawProxyURL string) (*http.Client, error) {
	if base == nil {
		base = &http.Client{}
	}
	next := *base
	var baseTransport *http.Transport
	if base.Transport != nil {
		var ok bool
		baseTransport, ok = base.Transport.(*http.Transport)
		if !ok {
			return nil, errors.New("base HTTP transport is not configurable")
		}
	}
	transport, err := NewTransport(baseTransport, rawProxyURL)
	if err != nil {
		return nil, err
	}
	next.Transport = transport
	return &next, nil
}

type noRedirectRoundTripper struct {
	base       http.RoundTripper
	onRedirect func(*http.Response, http.Header)
}

func (t noRedirectRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	response, err := base.RoundTrip(request)
	if response == nil || response.StatusCode < 300 || response.StatusCode >= 400 || response.Header == nil {
		return response, err
	}
	originalHeader := response.Header
	filteredHeader := originalHeader.Clone()
	hasLocation := false
	for name := range filteredHeader {
		if strings.EqualFold(name, "Location") {
			delete(filteredHeader, name)
			hasLocation = true
		}
	}
	if !hasLocation {
		return response, err
	}
	response.Header = filteredHeader
	if t.onRedirect != nil {
		t.onRedirect(response, originalHeader)
	}
	return response, err
}

// DoWithoutRedirects preserves http.Client timeout, cookie, context, and body
// semantics while forcing every 3xx to remain the current request's first-hop
// response. Hiding Location at the transport boundary is necessary because
// Client.Do parses it before invoking CheckRedirect; malformed values would
// otherwise become client errors and discard the original response evidence.
func DoWithoutRedirects(client *http.Client, request *http.Request) (*http.Response, error) {
	if client == nil {
		client = http.DefaultClient
	}
	clientCopy := *client
	var redirectResponse *http.Response
	var redirectHeader http.Header
	clientCopy.Transport = noRedirectRoundTripper{
		base: clientCopy.Transport,
		onRedirect: func(response *http.Response, header http.Header) {
			redirectResponse = response
			redirectHeader = header
		},
	}
	response, err := clientCopy.Do(request)
	if redirectResponse != nil {
		redirectResponse.Header = redirectHeader
	}
	return response, err
}
