// Package geoip provides local-only IP geolocation for the administrator
// request-log view. It never sends a recorded client IP to an external service.
package geoip

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"

	"github.com/oschwald/geoip2-golang"
)

var (
	ErrUnavailable  = errors.New("local IP geolocation is not configured")
	ErrInvalidIP    = errors.New("IP must be a public unicast address")
	ErrNotFound     = errors.New("IP is not present in local geolocation data")
	carrierGradeNAT = netip.MustParsePrefix("100.64.0.0/10")
)

// Config only accepts local MaxMind-compatible database files. Keeping both
// paths local prevents administrator-page lookups from disclosing client IPs
// to a third-party network service.
type Config struct {
	Enabled          bool
	CityDatabasePath string
	ASNDatabasePath  string
}

// Result intentionally contains network-location metadata, not a claim about
// a person's physical location. AccuracyRadiusKM comes from the City database
// and lets the UI suppress an unreliable city-level label.
type Result struct {
	IP               string   `json:"ip"`
	Provider         string   `json:"provider"`
	Country          string   `json:"country,omitempty"`
	CountryCode      string   `json:"country_code,omitempty"`
	Region           string   `json:"region,omitempty"`
	City             string   `json:"city,omitempty"`
	Postal           string   `json:"postal,omitempty"`
	Latitude         *float64 `json:"latitude,omitempty"`
	Longitude        *float64 `json:"longitude,omitempty"`
	AccuracyRadiusKM uint16   `json:"accuracy_radius_km,omitempty"`
	ASN              string   `json:"asn,omitempty"`
	Organization     string   `json:"organization,omitempty"`
	Timezone         string   `json:"timezone,omitempty"`
}

type cityReader interface {
	City(net.IP) (*geoip2.City, error)
	Close() error
}

type asnReader interface {
	ASN(net.IP) (*geoip2.ASN, error)
	Close() error
}

type Service struct {
	city cityReader
	asn  asnReader
}

// Open creates a local reader. It returns a disabled service plus a generic
// error when the configured file cannot be opened so callers can keep serving
// requests and return a safe 503 from the administrator-only lookup endpoint.
func Open(cfg Config) (*Service, error) {
	service := &Service{}
	if !cfg.Enabled {
		return service, nil
	}
	cityPath := strings.TrimSpace(cfg.CityDatabasePath)
	if cityPath == "" {
		return service, ErrUnavailable
	}
	city, err := geoip2.Open(cityPath)
	if err != nil {
		return service, fmt.Errorf("%w: city database could not be opened", ErrUnavailable)
	}
	service.city = city

	asnPath := strings.TrimSpace(cfg.ASNDatabasePath)
	if asnPath == "" {
		return service, nil
	}
	asn, err := geoip2.Open(asnPath)
	if err != nil {
		_ = service.Close()
		return &Service{}, fmt.Errorf("%w: ASN database could not be opened", ErrUnavailable)
	}
	service.asn = asn
	return service, nil
}

func (s *Service) Close() error {
	if s == nil {
		return nil
	}
	var closeErrors []error
	if s.city != nil {
		closeErrors = append(closeErrors, s.city.Close())
		s.city = nil
	}
	if s.asn != nil {
		closeErrors = append(closeErrors, s.asn.Close())
		s.asn = nil
	}
	return errors.Join(closeErrors...)
}

func (s *Service) Lookup(rawIP string) (Result, error) {
	if s == nil || s.city == nil {
		return Result{}, ErrUnavailable
	}
	address, err := parsePublicIP(rawIP)
	if err != nil {
		return Result{}, err
	}
	city, err := s.city.City(net.IP(address.AsSlice()))
	if err != nil {
		return Result{}, fmt.Errorf("%w: city lookup failed", ErrUnavailable)
	}
	result := Result{
		IP:               address.String(),
		Provider:         "local_maxmind",
		Country:          localizedName(city.Country.Names),
		CountryCode:      strings.TrimSpace(city.Country.IsoCode),
		City:             localizedName(city.City.Names),
		Postal:           strings.TrimSpace(city.Postal.Code),
		Timezone:         strings.TrimSpace(city.Location.TimeZone),
		AccuracyRadiusKM: city.Location.AccuracyRadius,
	}
	if len(city.Subdivisions) > 0 {
		result.Region = localizedName(city.Subdivisions[0].Names)
	}
	if city.Location.Latitude != 0 || city.Location.Longitude != 0 {
		latitude, longitude := city.Location.Latitude, city.Location.Longitude
		result.Latitude = &latitude
		result.Longitude = &longitude
	}
	if result.CountryCode == "" && result.Country == "" && result.Region == "" && result.City == "" {
		return Result{}, ErrNotFound
	}
	if s.asn != nil {
		asn, lookupErr := s.asn.ASN(net.IP(address.AsSlice()))
		if lookupErr == nil && asn != nil {
			if asn.AutonomousSystemNumber > 0 {
				result.ASN = fmt.Sprintf("AS%d", asn.AutonomousSystemNumber)
			}
			result.Organization = strings.TrimSpace(asn.AutonomousSystemOrganization)
		}
	}
	return result, nil
}

func parsePublicIP(raw string) (netip.Addr, error) {
	address, err := netip.ParseAddr(strings.TrimSpace(raw))
	if err != nil {
		return netip.Addr{}, ErrInvalidIP
	}
	address = address.Unmap()
	if !address.IsGlobalUnicast() || address.IsPrivate() || address.IsLoopback() || address.IsLinkLocalUnicast() || address.IsLinkLocalMulticast() || address.IsUnspecified() || carrierGradeNAT.Contains(address) {
		return netip.Addr{}, ErrInvalidIP
	}
	return address, nil
}

func localizedName(names map[string]string) string {
	for _, language := range []string{"zh-CN", "zh", "en"} {
		if value := strings.TrimSpace(names[language]); value != "" {
			return value
		}
	}
	return ""
}
