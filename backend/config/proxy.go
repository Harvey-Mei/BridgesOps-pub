package config

import (
	"strings"

	"github.com/Harvey-Mei/BridgeOps/backend/netproxy"
)

func (p ProxyConfig) URL() (string, error) {
	protocol := strings.ToLower(strings.TrimSpace(p.Protocol))
	if protocol == "" {
		protocol = "http"
	}
	return (netproxy.Endpoint{
		Protocol: netproxy.Protocol(protocol),
		Host:     p.Host,
		Port:     p.Port,
		Username: p.Username,
		Password: p.Password,
	}).URL()
}

func (p ProxyConfig) ActiveURL() (string, error) {
	if !p.Enabled {
		return "", nil
	}
	return p.URL()
}
