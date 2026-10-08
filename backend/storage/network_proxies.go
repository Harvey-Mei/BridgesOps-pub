package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/netproxy"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrNetworkProxyInUse    = errors.New("network proxy is still bound to channels")
	ErrNetworkProxyNotFound = errors.New("network proxy was not found")
)

// NetworkProxies stores administrator-managed outbound proxy endpoints.
type NetworkProxies struct {
	db *gorm.DB

	legacyNamesOnce sync.Once
	legacyNamesErr  error
}

const (
	networkProxyNameSanitizationMetadataKey = "network_proxy_name_sanitization_v1"
	networkProxyNameSanitizationComplete    = "complete"
	networkProxyNameSanitizationBatchSize   = 100
)

func NewNetworkProxies(db *gorm.DB) *NetworkProxies {
	proxies := &NetworkProxies{db: db}
	// Run the persisted legacy-name cleanup before callers receive the store.
	// Public methods surface an initialization failure instead of exposing a
	// partially sanitized proxy list.
	_ = proxies.ensureReady()
	return proxies
}

func (r *NetworkProxies) ensureReady() error {
	if r == nil || r.db == nil {
		return errors.New("network proxy storage is unavailable")
	}
	if r.db.Dialector == nil {
		return errors.New("network proxy storage is unavailable")
	}
	r.legacyNamesOnce.Do(func() {
		r.legacyNamesErr = r.sanitizeLegacyNetworkProxyNames()
	})
	return r.legacyNamesErr
}

func (r *NetworkProxies) sanitizeLegacyNetworkProxyNames() error {
	err := r.db.Transaction(func(tx *gorm.DB) error {
		marker := &SystemMetadata{Key: networkProxyNameSanitizationMetadataKey, Value: "running"}
		created := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "key"}},
			DoNothing: true,
		}).Create(marker)
		if created.Error != nil {
			return created.Error
		}
		if created.RowsAffected == 0 {
			var existing SystemMetadata
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&existing, "key = ?", networkProxyNameSanitizationMetadataKey).Error; err != nil {
				return err
			}
			if existing.Value == networkProxyNameSanitizationComplete {
				return nil
			}
			return errors.New("network proxy name sanitization is incomplete")
		}

		var lastID uint
		for {
			var batch []NetworkProxy
			if err := tx.Where("id > ?", lastID).Order("id ASC").Limit(networkProxyNameSanitizationBatchSize).Find(&batch).Error; err != nil {
				return err
			}
			for index := range batch {
				profile := &batch[index]
				if !strings.EqualFold(profile.Source, "clash") && !netproxy.IsSensitiveDisplayName(profile.Name) {
					continue
				}
				name, err := availableSanitizedNetworkProxyName(tx, profile)
				if err != nil {
					return err
				}
				if err := tx.Model(&NetworkProxy{}).Where("id = ?", profile.ID).Update("name", name).Error; err != nil {
					return err
				}
			}
			if len(batch) < networkProxyNameSanitizationBatchSize {
				break
			}
			lastID = batch[len(batch)-1].ID
		}
		return tx.Model(&SystemMetadata{}).
			Where("key = ?", networkProxyNameSanitizationMetadataKey).
			Update("value", networkProxyNameSanitizationComplete).Error
	})
	if err != nil {
		return errors.New("normalize stored network proxy names")
	}
	return nil
}

func availableSanitizedNetworkProxyName(tx *gorm.DB, profile *NetworkProxy) (string, error) {
	if profile == nil {
		return "", errors.New("network proxy is required")
	}
	rawName := profile.Name
	if strings.EqualFold(profile.Source, "clash") {
		rawName = ""
	}
	name := netproxy.SafeDisplayName(rawName, netproxy.Endpoint{
		Protocol: netproxy.Protocol(profile.Protocol),
		Host:     profile.Host,
		Port:     profile.Port,
	})
	name = netproxy.TruncateDisplayName(strings.TrimSpace(name), netproxy.MaxNetworkProxyNameBytes)
	if name == "" {
		name = "代理"
	}
	for suffix := 1; ; suffix++ {
		candidate := name
		if suffix > 1 {
			suffixText := fmt.Sprintf(" (%d)", suffix)
			candidate = netproxy.TruncateDisplayName(name, netproxy.MaxNetworkProxyNameBytes-len(suffixText)) + suffixText
		}
		query := tx.Model(&NetworkProxy{}).Where("LOWER(name) = LOWER(?)", candidate)
		if profile.ID != 0 {
			query = query.Where("id <> ?", profile.ID)
		}
		var count int64
		if err := query.Count(&count).Error; err != nil {
			return "", errors.New("find available network proxy name")
		}
		if count == 0 {
			return candidate, nil
		}
	}
}

// lockNetworkProxyForBinding takes a write lock without changing any endpoint
// data. PostgreSQL holds the parent row lock until the surrounding channel
// transaction commits.
func lockNetworkProxyForBinding(tx *gorm.DB, id *uint) error {
	if id == nil || *id == 0 {
		return nil
	}
	result := tx.Model(&NetworkProxy{}).Where("id = ?", *id).UpdateColumn("id", gorm.Expr("id"))
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrNetworkProxyNotFound
	}
	return nil
}

func (r *NetworkProxies) List() ([]NetworkProxy, error) {
	if err := r.ensureReady(); err != nil {
		return nil, err
	}
	var list []NetworkProxy
	if err := r.db.Order("enabled DESC").Order("name ASC").Order("id ASC").Find(&list).Error; err != nil {
		return nil, err
	}
	return list, nil
}

func (r *NetworkProxies) FindByID(id uint) (*NetworkProxy, error) {
	return r.FindByIDContext(context.Background(), id)
}

func (r *NetworkProxies) FindByIDContext(ctx context.Context, id uint) (*NetworkProxy, error) {
	if err := r.ensureReady(); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var proxy NetworkProxy
	if err := r.db.WithContext(ctx).First(&proxy, id).Error; err != nil {
		return nil, err
	}
	return &proxy, nil
}

func (r *NetworkProxies) Create(proxy *NetworkProxy) error {
	if err := r.ensureReady(); err != nil {
		return err
	}
	if proxy == nil {
		return errors.New("network proxy is required")
	}
	if err := normalizeNetworkProxyEndpoint(proxy); err != nil {
		return err
	}
	if proxy.ConnectionRevision == 0 {
		proxy.ConnectionRevision = 1
	}
	if strings.EqualFold(proxy.Source, "clash") || netproxy.IsSensitiveDisplayName(proxy.Name) {
		return r.db.Transaction(func(tx *gorm.DB) error {
			name, err := availableSanitizedNetworkProxyName(tx, proxy)
			if err != nil {
				return err
			}
			proxy.Name = name
			return tx.Create(proxy).Error
		})
	}
	name, err := netproxy.ValidateNetworkProxyName(proxy.Name)
	if err != nil {
		return err
	}
	proxy.Name = name
	return r.db.Create(proxy).Error
}

const maxNetworkProxyConnectionRevision int64 = 1<<63 - 1

// Update persists an administrator configuration change. The revision is
// incremented inside the database transaction instead of from the caller's
// stale copy: concurrent edits must never reuse a client-cache key for a
// different endpoint. Test telemetry must use UpdateTestResult instead, so it
// does not invalidate active clients.
func (r *NetworkProxies) Update(proxy *NetworkProxy) error {
	if err := r.ensureReady(); err != nil {
		return err
	}
	if proxy == nil {
		return errors.New("network proxy is required")
	}
	if err := normalizeNetworkProxyEndpoint(proxy); err != nil {
		return err
	}
	if !strings.EqualFold(proxy.Source, "clash") && !netproxy.IsSensitiveDisplayName(proxy.Name) {
		name, err := netproxy.ValidateNetworkProxyName(proxy.Name)
		if err != nil {
			return err
		}
		proxy.Name = name
	}
	return r.db.Transaction(func(tx *gorm.DB) error {
		if strings.EqualFold(proxy.Source, "clash") || netproxy.IsSensitiveDisplayName(proxy.Name) {
			name, err := availableSanitizedNetworkProxyName(tx, proxy)
			if err != nil {
				return err
			}
			proxy.Name = name
		}
		result := tx.Model(&NetworkProxy{}).Where("id = ?", proxy.ID).Updates(map[string]any{
			"name":              proxy.Name,
			"protocol":          proxy.Protocol,
			"host":              proxy.Host,
			"port":              proxy.Port,
			"credential_cipher": proxy.CredentialCipher,
			"source":            proxy.Source,
			"clean_ip":          proxy.CleanIP,
			"enabled":           proxy.Enabled,
			"connection_revision": gorm.Expr(
				"CASE WHEN connection_revision < 1 THEN 2 WHEN connection_revision >= ? THEN 1 ELSE connection_revision + 1 END",
				maxNetworkProxyConnectionRevision,
			),
			"updated_at": time.Now().UTC(),
		})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return gorm.ErrRecordNotFound
		}
		return tx.First(proxy, proxy.ID).Error
	})
}

func (r *NetworkProxies) BoundChannelCount(id uint) (int64, error) {
	counts, err := r.BoundChannelCounts([]uint{id})
	if err != nil {
		return 0, err
	}
	return counts[id], nil
}

// BoundChannelCounts aggregates all requested proxy bindings in one query.
// The administrator list can contain up to hundreds of imported endpoints, so
// counting each row independently would turn a single page load into N+1 I/O.
func (r *NetworkProxies) BoundChannelCounts(ids []uint) (map[uint]int64, error) {
	if err := r.ensureReady(); err != nil {
		return nil, err
	}
	unique := make([]uint, 0, len(ids))
	seen := make(map[uint]struct{}, len(ids))
	for _, id := range ids {
		if id == 0 {
			continue
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		unique = append(unique, id)
	}
	counts := make(map[uint]int64, len(unique))
	if len(unique) == 0 {
		return counts, nil
	}
	var rows []struct {
		NetworkProxyID uint
		Count          int64
	}
	if err := r.db.Model(&Channel{}).
		Select("network_proxy_id, COUNT(*) AS count").
		Where("network_proxy_id IN ?", unique).
		Group("network_proxy_id").
		Scan(&rows).Error; err != nil {
		return nil, err
	}
	for _, row := range rows {
		counts[row.NetworkProxyID] = row.Count
	}
	return counts, nil
}

// Delete fails closed when any channel still references the endpoint. Clearing
// bindings automatically could silently route those channels somewhere else.
func (r *NetworkProxies) Delete(id uint) error {
	if err := r.ensureReady(); err != nil {
		return err
	}
	return r.db.Transaction(func(tx *gorm.DB) error {
		if err := lockNetworkProxyForBinding(tx, &id); err != nil {
			if errors.Is(err, ErrNetworkProxyNotFound) {
				return nil
			}
			return err
		}
		var count int64
		if err := tx.Model(&Channel{}).Where("network_proxy_id = ?", id).Count(&count).Error; err != nil {
			return err
		}
		if count > 0 {
			return ErrNetworkProxyInUse
		}
		if err := tx.Delete(&NetworkProxy{}, id).Error; err != nil {
			if isNetworkProxyForeignKeyError(err) {
				return ErrNetworkProxyInUse
			}
			return err
		}
		return nil
	})
}

func isNetworkProxyForeignKeyError(err error) bool {
	return err != nil && (errors.Is(err, gorm.ErrForeignKeyViolated) || strings.Contains(strings.ToLower(err.Error()), "foreign key"))
}

func normalizeNetworkProxyEndpoint(proxy *NetworkProxy) error {
	if proxy == nil {
		return errors.New("network proxy is required")
	}
	endpoint, err := (netproxy.Endpoint{
		Protocol: netproxy.Protocol(proxy.Protocol),
		Host:     proxy.Host,
		Port:     proxy.Port,
	}).Normalized()
	if err != nil {
		return err
	}
	proxy.Protocol = string(endpoint.Protocol)
	proxy.Host = endpoint.Host
	proxy.Port = endpoint.Port
	return nil
}

func (r *NetworkProxies) UpdateTestResult(id uint, testedAt time.Time, latencyMS *int64, egressIP, lastErr string) error {
	if err := r.ensureReady(); err != nil {
		return err
	}
	updates := map[string]any{
		"last_tested_at":  testedAt,
		"last_latency_ms": latencyMS,
		"last_error":      strings.TrimSpace(lastErr),
	}
	if strings.TrimSpace(egressIP) != "" {
		updates["last_egress_ip"] = strings.TrimSpace(egressIP)
	}
	return r.db.Model(&NetworkProxy{}).Where("id = ?", id).Updates(updates).Error
}
