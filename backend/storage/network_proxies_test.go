package storage

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
)

func TestNetworkProxyConnectionRevisionOnlyChangesForConfigurationUpdates(t *testing.T) {
	db := openTestDB(t)
	proxies := NewNetworkProxies(db)
	profile := &NetworkProxy{
		Name:     "isolated-proxy",
		Protocol: "socks5",
		Host:     "127.0.0.1",
		Port:     1080,
		Enabled:  true,
	}
	if err := proxies.Create(profile); err != nil {
		t.Fatal(err)
	}
	if profile.ConnectionRevision != 1 {
		t.Fatalf("create revision = %d, want 1", profile.ConnectionRevision)
	}

	latency := int64(42)
	if err := proxies.UpdateTestResult(profile.ID, time.Now().UTC(), &latency, "203.0.113.10", ""); err != nil {
		t.Fatal(err)
	}
	afterTest, err := proxies.FindByID(profile.ID)
	if err != nil {
		t.Fatal(err)
	}
	if afterTest.ConnectionRevision != 1 {
		t.Fatalf("test result changed revision to %d", afterTest.ConnectionRevision)
	}

	afterTest.Name = "isolated-proxy-renamed"
	if err := proxies.Update(afterTest); err != nil {
		t.Fatal(err)
	}
	updated, err := proxies.FindByID(profile.ID)
	if err != nil {
		t.Fatal(err)
	}
	if updated.ConnectionRevision != 2 {
		t.Fatalf("configuration update revision = %d, want 2", updated.ConnectionRevision)
	}
}

func TestNetworkProxyUpdateUsesCurrentDatabaseRevisionForStaleEdits(t *testing.T) {
	db := openTestDB(t)
	proxies := NewNetworkProxies(db)
	profile := &NetworkProxy{Name: "concurrent-proxy", Protocol: "http", Host: "one.example", Port: 8080, Enabled: true}
	if err := proxies.Create(profile); err != nil {
		t.Fatal(err)
	}
	first, err := proxies.FindByID(profile.ID)
	if err != nil {
		t.Fatal(err)
	}
	second, err := proxies.FindByID(profile.ID)
	if err != nil {
		t.Fatal(err)
	}
	first.Host = "two.example"
	if err := proxies.Update(first); err != nil {
		t.Fatal(err)
	}
	second.Host = "three.example"
	if err := proxies.Update(second); err != nil {
		t.Fatal(err)
	}
	if second.ConnectionRevision != 3 {
		t.Fatalf("stale edit revision = %d, want 3", second.ConnectionRevision)
	}
	stored, err := proxies.FindByID(profile.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.ConnectionRevision != 3 || stored.Host != "three.example" {
		t.Fatalf("stored proxy = %#v", stored)
	}
}

func TestNetworkProxyDeleteCannotOvertakeConcurrentBinding(t *testing.T) {
	db := openTestDB(t)
	deleteDB, err := testdb.ReopenWithError(t, db)
	if err != nil {
		t.Fatal(err)
	}
	proxies := NewNetworkProxies(db)
	deleteProxies := NewNetworkProxies(deleteDB)
	profile := &NetworkProxy{Name: "concurrent-binding", Protocol: "http", Host: "proxy.example", Port: 8080, Enabled: true}
	if err := proxies.Create(profile); err != nil {
		t.Fatal(err)
	}

	tx := db.Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	committed := false
	deleteDone := make(chan error, 1)
	deleteRunning := false
	deleteFinished := false
	defer func() {
		if !committed {
			_ = tx.Rollback().Error
		}
		if deleteRunning && !deleteFinished {
			select {
			case <-deleteDone:
			case <-time.After(2 * time.Second):
			}
		}
	}()

	bound := &Channel{
		Name:           "concurrent-bound-channel",
		Type:           ChannelTypeNewAPI,
		SiteURL:        "https://example.com",
		Username:       "u",
		PasswordCipher: "cipher",
		ProxyEnabled:   true,
		NetworkProxyID: &profile.ID,
	}
	if err := tx.Create(bound).Error; err != nil {
		t.Fatal(err)
	}
	deleteStarted := make(chan struct{})
	go func() {
		close(deleteStarted)
		deleteDone <- deleteProxies.Delete(profile.ID)
	}()
	deleteRunning = true
	<-deleteStarted
	select {
	case err := <-deleteDone:
		deleteFinished = true
		t.Fatalf("delete completed before the binding transaction: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := tx.Commit().Error; err != nil {
		t.Fatal(err)
	}
	committed = true

	select {
	case err := <-deleteDone:
		deleteFinished = true
		if !errors.Is(err, ErrNetworkProxyInUse) {
			t.Fatalf("delete error = %v, want in-use", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("delete did not finish after binding committed")
	}
	if _, err := proxies.FindByID(profile.ID); err != nil {
		t.Fatalf("bound proxy was deleted: %v", err)
	}
}

func TestNetworkProxyBindingAfterDeleteFailsClosed(t *testing.T) {
	db := openTestDB(t)
	proxies := NewNetworkProxies(db)
	channels := NewChannels(db)
	profile := &NetworkProxy{Name: "delete-before-bind", Protocol: "socks5", Host: "proxy.example", Port: 1080, Enabled: true}
	if err := proxies.Create(profile); err != nil {
		t.Fatal(err)
	}
	if err := proxies.Delete(profile.ID); err != nil {
		t.Fatal(err)
	}

	err := channels.Create(&Channel{
		Name:           "must-not-be-orphaned",
		Type:           ChannelTypeNewAPI,
		SiteURL:        "https://example.com",
		Username:       "u",
		PasswordCipher: "cipher",
		ProxyEnabled:   true,
		NetworkProxyID: &profile.ID,
	})
	if !errors.Is(err, ErrNetworkProxyNotFound) {
		t.Fatalf("binding error = %v, want missing proxy", err)
	}
	var count int64
	if err := db.Model(&Channel{}).Where("network_proxy_id = ?", profile.ID).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("orphaned bindings = %d", count)
	}
}

func TestNetworkProxyBoundChannelCountsAggregatesRequestedProfiles(t *testing.T) {
	db := openTestDB(t)
	proxies := NewNetworkProxies(db)
	channels := NewChannels(db)
	first := &NetworkProxy{Name: "first-count", Protocol: "http", Host: "one.example", Port: 8080, Enabled: true}
	second := &NetworkProxy{Name: "second-count", Protocol: "socks5", Host: "two.example", Port: 1080, Enabled: true}
	third := &NetworkProxy{Name: "third-count", Protocol: "https", Host: "three.example", Port: 8443, Enabled: true}
	for _, profile := range []*NetworkProxy{first, second, third} {
		if err := proxies.Create(profile); err != nil {
			t.Fatal(err)
		}
	}
	for index, proxyID := range []uint{first.ID, first.ID, second.ID} {
		if err := channels.Create(&Channel{
			Name:           fmt.Sprintf("count-channel-%d", index),
			Type:           ChannelTypeNewAPI,
			SiteURL:        "https://example.com",
			Username:       "u",
			PasswordCipher: "cipher",
			NetworkProxyID: &proxyID,
		}); err != nil {
			t.Fatal(err)
		}
	}

	counts, err := proxies.BoundChannelCounts([]uint{first.ID, second.ID, third.ID, first.ID, 0})
	if err != nil {
		t.Fatal(err)
	}
	if counts[first.ID] != 2 || counts[second.ID] != 1 || counts[third.ID] != 0 {
		t.Fatalf("binding counts = %#v", counts)
	}
	if count, err := proxies.BoundChannelCount(first.ID); err != nil || count != 2 {
		t.Fatalf("single binding count = %d, err=%v", count, err)
	}
}

func TestNetworkProxyStorageRejectsInvalidDirectEndpoint(t *testing.T) {
	db := openTestDB(t)
	proxies := NewNetworkProxies(db)
	profile := &NetworkProxy{Name: "invalid-host", Protocol: "http", Host: "http://user:secret@proxy.example", Port: 8080, Enabled: true}
	if err := proxies.Create(profile); err == nil {
		t.Fatal("storage accepted a credential-bearing endpoint host")
	}
	var count int64
	if err := db.Model(&NetworkProxy{}).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("invalid endpoint persisted count=%d", count)
	}
}

func TestNetworkProxiesPersistentlySanitizeLegacySensitiveNames(t *testing.T) {
	db := openTestDB(t)
	existing := &NetworkProxy{Name: "safe.example:1080", Protocol: "socks5", Host: "safe.example", Port: 1080, Enabled: true}
	legacyOne := &NetworkProxy{Name: "socks5://user:legacy-secret@name.example:1080?token=legacy-token", Protocol: "socks5", Host: "safe.example", Port: 1080, Enabled: true}
	legacyTwo := &NetworkProxy{Name: "password=other-legacy-secret", Protocol: "socks5", Host: "safe.example", Port: 1080, Enabled: true}
	legacyClash := &NetworkProxy{Name: "regional token private-value", Protocol: "socks5", Host: "safe.example", Port: 1080, Source: "clash", Enabled: true}
	for _, profile := range []*NetworkProxy{existing, legacyOne, legacyTwo, legacyClash} {
		if err := db.Create(profile).Error; err != nil {
			t.Fatal(err)
		}
	}

	_ = NewNetworkProxies(db)
	for _, expected := range []struct {
		id   uint
		name string
	}{
		{id: existing.ID, name: "safe.example:1080"},
		{id: legacyOne.ID, name: "safe.example:1080 (2)"},
		{id: legacyTwo.ID, name: "safe.example:1080 (3)"},
		{id: legacyClash.ID, name: "safe.example:1080 (4)"},
	} {
		var stored NetworkProxy
		if err := db.First(&stored, expected.id).Error; err != nil {
			t.Fatal(err)
		}
		if stored.Name != expected.name {
			t.Fatal("legacy proxy name was not safely normalized")
		}
		for _, secret := range []string{"legacy-secret", "legacy-token", "other-legacy-secret", "private-value", "user@", "?", "@"} {
			if strings.Contains(stored.Name, secret) {
				t.Fatal("stored legacy proxy name contains sensitive data")
			}
		}
	}
	var marker SystemMetadata
	if err := db.First(&marker, "key = ?", networkProxyNameSanitizationMetadataKey).Error; err != nil {
		t.Fatal(err)
	}
	if marker.Value != networkProxyNameSanitizationComplete {
		t.Fatal("legacy proxy name sanitization was not marked complete")
	}
}
