package netproxy

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"time"
)

// FetchSubscription retrieves a public proxy subscription without consulting
// environment proxies. The custom dialer resolves and validates every target
// address so an administrator import endpoint cannot be used to reach local
// metadata services or private networks.
func FetchSubscription(ctx context.Context, rawURL string) (string, error) {
	requestURL, err := parseSubscriptionURL(rawURL)
	if err != nil {
		return "", err
	}
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		Proxy:                 nil,
		DialContext:           publicSubscriptionDialContext(dialer),
		ForceAttemptHTTP2:     true,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 15 * time.Second,
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   20 * time.Second,
		CheckRedirect: func(request *http.Request, via []*http.Request) error {
			if len(via) >= 3 {
				return errors.New("subscription redirect limit reached")
			}
			_, err := parseSubscriptionURL(request.URL.String())
			return err
		},
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL.String(), nil)
	if err != nil {
		return "", errors.New("create subscription request")
	}
	request.Header.Set("Accept", "text/plain, application/yaml, application/json, */*")
	response, err := client.Do(request)
	if err != nil {
		return "", errors.New("fetch proxy subscription")
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return "", errors.New("proxy subscription returned an unsuccessful status")
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, MaxImportContentBytes+1))
	if err != nil {
		return "", errors.New("read proxy subscription")
	}
	if len(body) > MaxImportContentBytes {
		return "", errors.New("proxy subscription exceeds the 2 MiB import limit")
	}
	return strings.TrimSpace(string(body)), nil
}

func parseSubscriptionURL(raw string) (*url.URL, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return nil, errors.New("parse proxy subscription URL")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, errors.New("proxy subscription URL must use HTTP or HTTPS")
	}
	if strings.TrimSpace(parsed.Hostname()) == "" {
		return nil, errors.New("proxy subscription URL host is required")
	}
	return parsed, nil
}

func publicSubscriptionDialContext(dialer *net.Dialer) func(context.Context, string, string) (net.Conn, error) {
	if dialer == nil {
		dialer = &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	}
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, errors.New("resolve proxy subscription host")
		}
		if parsed, err := netip.ParseAddr(host); err == nil {
			if !isPublicSubscriptionAddress(parsed) {
				return nil, errors.New("proxy subscription host resolves to a private address")
			}
			return dialer.DialContext(ctx, network, net.JoinHostPort(parsed.String(), port))
		}
		addresses, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		if err != nil {
			return nil, errors.New("resolve proxy subscription host")
		}
		for _, address := range addresses {
			if !isPublicSubscriptionAddress(address) {
				continue
			}
			connection, err := dialer.DialContext(ctx, network, net.JoinHostPort(address.String(), port))
			if err == nil {
				return connection, nil
			}
		}
		return nil, errors.New("proxy subscription host has no reachable public address")
	}
}

func isPublicSubscriptionAddress(address netip.Addr) bool {
	address = address.Unmap()
	if !address.IsValid() || address.IsUnspecified() || address.IsLoopback() || address.IsMulticast() || address.IsLinkLocalUnicast() || address.IsLinkLocalMulticast() || address.IsPrivate() {
		return false
	}
	if address.Is4() {
		bytes := address.As4()
		if bytes[0] == 0 || bytes[0] == 10 || bytes[0] == 127 {
			return false
		}
		if bytes[0] == 100 && bytes[1] >= 64 && bytes[1] <= 127 {
			return false
		}
		if bytes[0] == 169 && bytes[1] == 254 {
			return false
		}
		if bytes[0] == 192 && bytes[1] == 0 && bytes[2] == 0 {
			return false
		}
		if bytes[0] == 192 && bytes[1] == 0 && bytes[2] == 2 {
			return false
		}
		if bytes[0] == 198 && (bytes[1] == 18 || bytes[1] == 19 || bytes[1] == 51) {
			return false
		}
		if bytes[0] == 203 && bytes[1] == 0 && bytes[2] == 113 {
			return false
		}
	}
	return true
}
