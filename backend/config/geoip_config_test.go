package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadFileReadsLocalGeoIPConfiguration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("geoIP:\n  enabled: true\n  cityDatabasePath: /srv/geo/City.mmdb\n  asnDatabasePath: /srv/geo/ASN.mmdb\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err := LoadFile(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if !cfg.GeoIP.Enabled || cfg.GeoIP.CityDatabasePath != "/srv/geo/City.mmdb" || cfg.GeoIP.ASNDatabasePath != "/srv/geo/ASN.mmdb" {
		t.Fatalf("geoIP config = %#v", cfg.GeoIP)
	}
}
