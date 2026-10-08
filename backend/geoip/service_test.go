package geoip

import (
	"errors"
	"net"
	"path/filepath"
	"strings"
	"testing"

	"github.com/oschwald/geoip2-golang"
)

type cityReaderFunc func(net.IP) (*geoip2.City, error)

func (f cityReaderFunc) City(ip net.IP) (*geoip2.City, error) { return f(ip) }
func (cityReaderFunc) Close() error                           { return nil }

type asnReaderFunc func(net.IP) (*geoip2.ASN, error)

func (f asnReaderFunc) ASN(ip net.IP) (*geoip2.ASN, error) { return f(ip) }
func (asnReaderFunc) Close() error                         { return nil }

func TestLookupUsesLocalCityAndASNRecords(t *testing.T) {
	service := &Service{
		city: cityReaderFunc(func(ip net.IP) (*geoip2.City, error) {
			if got := ip.String(); got != "8.8.8.8" {
				t.Fatalf("lookup IP = %q", got)
			}
			result := &geoip2.City{}
			result.Country.Names = map[string]string{"zh-CN": "美国", "en": "United States"}
			result.Country.IsoCode = "US"
			result.Subdivisions = append(result.Subdivisions, struct {
				Names     map[string]string `maxminddb:"names"`
				IsoCode   string            `maxminddb:"iso_code"`
				GeoNameID uint              `maxminddb:"geoname_id"`
			}{Names: map[string]string{"en": "California"}})
			result.City.Names = map[string]string{"en": "Mountain View"}
			result.Postal.Code = "94043"
			result.Location.Latitude = 37.4056
			result.Location.Longitude = -122.0775
			result.Location.AccuracyRadius = 20
			result.Location.TimeZone = "America/Los_Angeles"
			return result, nil
		}),
		asn: asnReaderFunc(func(net.IP) (*geoip2.ASN, error) {
			return &geoip2.ASN{AutonomousSystemNumber: 15169, AutonomousSystemOrganization: "Google LLC"}, nil
		}),
	}

	result, err := service.Lookup("8.8.8.8")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if result.Provider != "local_maxmind" || result.Country != "美国" || result.CountryCode != "US" || result.Region != "California" || result.City != "Mountain View" {
		t.Fatalf("location = %#v", result)
	}
	if result.AccuracyRadiusKM != 20 || result.Latitude == nil || result.Longitude == nil || result.ASN != "AS15169" || result.Organization != "Google LLC" {
		t.Fatalf("location detail = %#v", result)
	}
}

func TestLookupRejectsNonPublicAddressWithoutReadingDatabase(t *testing.T) {
	for _, rawIP := range []string{"10.0.0.1", "100.64.0.1"} {
		t.Run(rawIP, func(t *testing.T) {
			called := false
			service := &Service{city: cityReaderFunc(func(net.IP) (*geoip2.City, error) {
				called = true
				return &geoip2.City{}, nil
			})}
			_, err := service.Lookup(rawIP)
			if !errors.Is(err, ErrInvalidIP) {
				t.Fatalf("lookup error = %v", err)
			}
			if called {
				t.Fatal("non-public address reached local database")
			}
		})
	}
}

func TestOpenDisabledHasNoDatabaseFallback(t *testing.T) {
	service, err := Open(Config{})
	if err != nil {
		t.Fatalf("open disabled service: %v", err)
	}
	_, err = service.Lookup("8.8.8.8")
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("disabled lookup error = %v", err)
	}
}

func TestOpenFailsClosedWhenEnabledDatabaseIsMissing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing-city.mmdb")
	service, err := Open(Config{Enabled: true, CityDatabasePath: path})
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("open error = %v", err)
	}
	if strings.Contains(err.Error(), path) {
		t.Fatalf("open error leaks database path: %v", err)
	}
	_, err = service.Lookup("8.8.8.8")
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("lookup error = %v", err)
	}
}
