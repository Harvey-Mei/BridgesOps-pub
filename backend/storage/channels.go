package storage

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Channels 渠道仓库。
type Channels struct {
	db               *gorm.DB
	channelLockSlots chan struct{}
}

// WithContext returns a repository view whose SQL observes ctx cancellation.
// The underlying channel-operation limiter remains shared with the parent.
func (r *Channels) WithContext(ctx context.Context) *Channels {
	if r == nil || r.db == nil {
		return r
	}
	if ctx == nil {
		ctx = context.Background()
	}
	view := *r
	view.db = r.db.WithContext(ctx)
	return &view
}

type channelOperationLockContextKey struct{}

type channelOperationLockContext struct {
	channelID uint
	parent    *channelOperationLockContext
}

func NewChannels(db *gorm.DB) *Channels {
	return &Channels{db: db, channelLockSlots: sharedGatewayChannelLockLimiter(db)}
}

// LockChannelOperation shares the gateway's cross-instance serialization
// domain so administrator identity changes cannot switch accounts while a
// managed remote-key operation is still verifying and mutating that channel.
func (r *Channels) LockChannelOperation(ctx context.Context, channelID uint) (func(), error) {
	if r == nil {
		return nil, errors.New("channel operation lock is unavailable")
	}
	return lockGatewayChannelOperation(ctx, r.db, r.channelLockSlots, channelID)
}

// WithChannelOperationLockHeld marks a context only after the caller has
// acquired LockChannelOperation for channelID. Downstream helpers use the
// marker to reuse the same non-reentrant PostgreSQL advisory lock safely.
func WithChannelOperationLockHeld(ctx context.Context, channelID uint) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if channelID == 0 || ChannelOperationLockHeld(ctx, channelID) {
		return ctx
	}
	parent, _ := ctx.Value(channelOperationLockContextKey{}).(*channelOperationLockContext)
	return context.WithValue(ctx, channelOperationLockContextKey{}, &channelOperationLockContext{
		channelID: channelID,
		parent:    parent,
	})
}

// ChannelOperationLockHeld reports whether channelID is present in the
// current lock ownership chain.
func ChannelOperationLockHeld(ctx context.Context, channelID uint) bool {
	if ctx == nil || channelID == 0 {
		return false
	}
	state, _ := ctx.Value(channelOperationLockContextKey{}).(*channelOperationLockContext)
	for state != nil {
		if state.channelID == channelID {
			return true
		}
		state = state.parent
	}
	return false
}

func (r *Channels) RequireUpstreamAccountIdentityRevision(channelID uint, expectedRevision uint64) error {
	if r == nil || r.db == nil || channelID == 0 || expectedRevision == 0 {
		return ErrStaleChannelAccountIdentityRevision
	}
	var channel Channel
	err := r.db.Transaction(func(tx *gorm.DB) error {
		var err error
		channel, err = requireOrdinaryChannelWriteTx(tx, channelID)
		return err
	})
	if err != nil {
		return err
	}
	if channel.UpstreamAccountIdentityRevision != expectedRevision {
		return ErrStaleChannelAccountIdentityRevision
	}
	return nil
}

func requireUpstreamAccountIdentityRevisionTx(tx *gorm.DB, channelID uint, expectedRevision uint64) error {
	if tx == nil || channelID == 0 || expectedRevision == 0 {
		return ErrStaleChannelAccountIdentityRevision
	}
	channel, err := requireOrdinaryChannelWriteTx(tx, channelID)
	if err != nil {
		return err
	}
	if channel.UpstreamAccountIdentityRevision != expectedRevision {
		return ErrStaleChannelAccountIdentityRevision
	}
	return nil
}

func (r *Channels) Create(c *Channel) error {
	return r.CreateWithDirectCredentials(c, nil)
}

// CreateWithDirectCredentials commits a new channel and its direct routing
// configuration in one transaction. Callers pass nil for non-direct channels.
func (r *Channels) CreateWithDirectCredentials(c *Channel, credentials []ChannelDirectCredential) error {
	if c.UpstreamAccountIdentityRevision == 0 {
		c.UpstreamAccountIdentityRevision = 1
	}
	if c.AdminRevision == 0 {
		c.AdminRevision = 1
	}
	return r.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(c).Error; err != nil {
			return err
		}
		factor := legacyFundingFactor(c.RechargeMultiplier, c.RechargeMultiplierMode)
		if err := tx.Create(&ChannelCostState{
			ChannelID: c.ID, FundingFactor: factor, Version: 1,
			Source: "default", Status: "ready",
		}).Error; err != nil {
			return err
		}
		if len(credentials) > 0 {
			return replaceChannelDirectCredentialsTx(tx, c.ID, credentials, time.Now().UTC())
		}
		return nil
	})
}

// ChannelAdminField is an explicitly writable administrator-facing channel
// column. Callers must name the fields supplied by the current request so a
// stale editor cannot overwrite unrelated configuration or account identity
// values that it merely read earlier.
type ChannelAdminField string

const (
	ChannelAdminFieldName                   ChannelAdminField = "name"
	ChannelAdminFieldGatewayApp             ChannelAdminField = "gateway_app"
	ChannelAdminFieldSiteURL                ChannelAdminField = "site_url"
	ChannelAdminFieldAPIURL                 ChannelAdminField = "api_url"
	ChannelAdminFieldUsername               ChannelAdminField = "username"
	ChannelAdminFieldSortOrder              ChannelAdminField = "sort_order"
	ChannelAdminFieldPasswordCipher         ChannelAdminField = "password_cipher"
	ChannelAdminFieldCredentialMode         ChannelAdminField = "credential_mode"
	ChannelAdminFieldDirectModels           ChannelAdminField = "direct_models"
	ChannelAdminFieldLoginExtraParams       ChannelAdminField = "login_extra_params"
	ChannelAdminFieldTurnstileEnabled       ChannelAdminField = "turnstile_enabled"
	ChannelAdminFieldIgnoreAnnouncements    ChannelAdminField = "ignore_announcements"
	ChannelAdminFieldSubscriptionEnabled    ChannelAdminField = "subscription_enabled"
	ChannelAdminFieldProxyEnabled           ChannelAdminField = "proxy_enabled"
	ChannelAdminFieldNetworkProxyID         ChannelAdminField = "network_proxy_id"
	ChannelAdminFieldCaptchaConfigID        ChannelAdminField = "captcha_config_id"
	ChannelAdminFieldBalanceThreshold       ChannelAdminField = "balance_threshold"
	ChannelAdminFieldRechargeMultiplier     ChannelAdminField = "recharge_multiplier"
	ChannelAdminFieldRechargeMultiplierMode ChannelAdminField = "recharge_multiplier_mode"
	ChannelAdminFieldMonitorEnabled         ChannelAdminField = "monitor_enabled"
)

// UpdateAdminFields serializes every administrator edit on the channel row,
// rejects a snapshot from an older account-identity revision, and writes only
// the request's explicit field whitelist. The channel row is always locked
// before a newly selected proxy row, matching account observation and avoiding
// the former Save-hook proxy->channel lock inversion.
func (r *Channels) UpdateAdminFields(c *Channel, fields ...ChannelAdminField) error {
	if len(fields) == 0 {
		return r.requireChannelAdminSnapshot(c)
	}
	return r.updateChannelFieldsWithDirectCredentials(c, true, false, false, nil, false, fields...)
}

// UpdateAdminFieldsWithDirectCredentials applies a full direct-credential
// replacement under the same channel CAS transaction as the surrounding form
// fields. An empty slice intentionally removes every configured direct group.
func (r *Channels) UpdateAdminFieldsWithDirectCredentials(c *Channel, credentials []ChannelDirectCredential, fields ...ChannelAdminField) error {
	return r.updateChannelFieldsWithDirectCredentials(c, true, false, true, credentials, false, fields...)
}

// requireChannelAdminSnapshot makes an empty administrator patch a real CAS
// check without advancing either revision. The caller's snapshot is refreshed
// with the authoritative revisions so a no-op response cannot move a client
// back to the revision it submitted.
func (r *Channels) requireChannelAdminSnapshot(c *Channel) error {
	if r == nil || r.db == nil || c == nil || c.ID == 0 {
		return errors.New("channel administrator update is unavailable")
	}
	if c.UpstreamAccountIdentityRevision == 0 || c.UpstreamAccountIdentityRevision > maxUpstreamAccountIdentityRevision {
		return errors.New("channel account identity revision is invalid")
	}
	if c.AdminRevision == 0 || c.AdminRevision > maxChannelAdminRevision {
		return errors.New("channel administrator revision is invalid")
	}
	var current Channel
	if err := r.db.Transaction(func(tx *gorm.DB) error {
		if _, err := requireOrdinaryChannelWriteTx(tx, c.ID); err != nil {
			return err
		}
		if err := tx.
			Select("id", "upstream_account_identity_revision", "admin_revision", "updated_at").
			First(&current, c.ID).Error; err != nil {
			return err
		}
		if current.UpstreamAccountIdentityRevision != c.UpstreamAccountIdentityRevision {
			return ErrStaleChannelAccountIdentityRevision
		}
		if current.AdminRevision != c.AdminRevision {
			return ErrStaleChannelAdminRevision
		}
		return nil
	}); err != nil {
		return err
	}
	c.UpstreamAccountIdentityRevision = current.UpstreamAccountIdentityRevision
	c.AdminRevision = current.AdminRevision
	c.UpdatedAt = current.UpdatedAt
	return nil
}

func (r *Channels) updateChannelFields(c *Channel, advanceAdminRevision bool, forceIdentityRevision bool, fields ...ChannelAdminField) error {
	return r.updateChannelFieldsWithDirectCredentials(c, advanceAdminRevision, forceIdentityRevision, false, nil, false, fields...)
}

func (r *Channels) updateChannelFieldsWithDirectCredentials(c *Channel, advanceAdminRevision bool, forceIdentityRevision bool, replaceDirectCredentials bool, directCredentials []ChannelDirectCredential, clearDirectCredentialSecrets bool, fields ...ChannelAdminField) error {
	if r == nil || r.db == nil || c == nil || c.ID == 0 {
		return errors.New("channel administrator update is unavailable")
	}
	if replaceDirectCredentials && clearDirectCredentialSecrets {
		return errors.New("direct credentials cannot be replaced and cleared together")
	}
	expectedRevision := c.UpstreamAccountIdentityRevision
	if expectedRevision == 0 || expectedRevision > maxUpstreamAccountIdentityRevision {
		return errors.New("channel account identity revision is invalid")
	}
	expectedAdminRevision := c.AdminRevision
	if advanceAdminRevision && (expectedAdminRevision == 0 || expectedAdminRevision > maxChannelAdminRevision) {
		return errors.New("channel administrator revision is invalid")
	}

	updates := make(map[string]any, len(fields)+2)
	identityFields := make([]ChannelAdminField, 0, len(fields))
	seen := make(map[ChannelAdminField]struct{}, len(fields))
	proxyEnabledSelected := false
	proxyIDSelected := false
	for _, field := range fields {
		if _, duplicate := seen[field]; duplicate {
			continue
		}
		seen[field] = struct{}{}
		column, value, identity, err := channelAdminFieldValue(c, field)
		if err != nil {
			return err
		}
		updates[column] = value
		if identity {
			identityFields = append(identityFields, field)
		}
		switch field {
		case ChannelAdminFieldProxyEnabled:
			proxyEnabledSelected = true
		case ChannelAdminFieldNetworkProxyID:
			proxyIDSelected = true
		}
	}
	if len(updates) == 0 && !advanceAdminRevision && !forceIdentityRevision {
		return nil
	}
	if proxyEnabledSelected != proxyIDSelected {
		return errors.New("channel proxy fields must be updated together")
	}
	if proxyIDSelected {
		hasProxyID := c.NetworkProxyID != nil && *c.NetworkProxyID != 0
		if c.ProxyEnabled != hasProxyID {
			return errors.New("channel proxy binding is inconsistent")
		}
	}

	// An identity-only patch is already fenced by the account identity
	// revision.  A concurrent display/monitor edit advances admin_revision but
	// cannot change any field in this patch, so rejecting it would turn two
	// disjoint administrator updates into a false conflict.  Mixed and
	// non-identity patches still use the full administrator CAS, which is what
	// prevents a stale full-form payload from replaying unrelated fields.
	identityOnlyPatch := len(identityFields) > 0 && len(identityFields) == len(updates)
	requireAdminRevision := advanceAdminRevision && (!identityOnlyPatch || replaceDirectCredentials || clearDirectCredentialSecrets)

	var revision uint64
	var adminRevision uint64
	var updatedAt time.Time
	err := r.db.Transaction(func(tx *gorm.DB) error {
		var current Channel
		if _, err := requireOrdinaryChannelWriteTx(tx, c.ID); err != nil {
			return err
		}
		if err := tx.
			Select(
				"id",
				"site_url",
				"api_url",
				"username",
				"password_cipher",
				"credential_mode",
				"login_extra_params",
				"upstream_account_identity_revision",
				"admin_revision",
			).
			First(&current, c.ID).Error; err != nil {
			return err
		}
		if current.UpstreamAccountIdentityRevision != expectedRevision {
			return ErrStaleChannelAccountIdentityRevision
		}
		if requireAdminRevision && current.AdminRevision != expectedAdminRevision {
			return ErrStaleChannelAdminRevision
		}
		if clearDirectCredentialSecrets && current.CredentialMode != CredentialModeDirect {
			return errors.New("direct credential clearing requires direct mode")
		}

		// Take a target proxy lock only after the channel row. Skip the model
		// hook below because it historically took these locks in reverse order.
		if proxyIDSelected {
			if err := lockNetworkProxyForBinding(tx, c.NetworkProxyID); err != nil {
				return err
			}
		}

		revision = current.UpstreamAccountIdentityRevision
		identityChanged := forceIdentityRevision
		for _, field := range identityFields {
			if channelAdminIdentityFieldChanged(&current, c, field) {
				identityChanged = true
				break
			}
		}
		if identityChanged {
			next, err := nextUpstreamAccountIdentityRevision(revision)
			if err != nil {
				return err
			}
			identityChangedAt := time.Now().UTC()
			if err := resetChannelCostStateForIdentityChangeTx(tx, c.ID, identityChangedAt); err != nil {
				return err
			}
			if err := resetChannelModelFactsForIdentityChangeTx(tx, c.ID, identityChangedAt); err != nil {
				return err
			}
			if err := resetChannelAccountScopedMonitorStateTx(tx, c.ID); err != nil {
				return err
			}
			if err := tx.Where("channel_id = ?", c.ID).
				Delete(&GatewayChannelUpstreamAccountBinding{}).Error; err != nil {
				return err
			}
			if err := tx.Where("channel_id = ?", c.ID).Delete(&AuthSession{}).Error; err != nil {
				return err
			}
			revision = next
			updates["upstream_account_identity_revision"] = revision
			resetChannelBalanceObservation(updates)
			if advanceAdminRevision {
				if err := invalidateManagedKeysForChannelIdentityChangeTx(tx, c.ID, time.Now().UTC()); err != nil {
					return err
				}
			}
		}
		adminRevision = current.AdminRevision
		if adminRevision == 0 {
			adminRevision = 1
		}
		if advanceAdminRevision {
			next, err := nextChannelAdminRevision(adminRevision)
			if err != nil {
				return err
			}
			adminRevision = next
			updates["admin_revision"] = adminRevision
		}

		updatedAt = time.Now().UTC()
		updates["updated_at"] = updatedAt
		query := tx.Session(&gorm.Session{SkipHooks: true}).
			Model(&Channel{}).
			Where("id = ? AND upstream_account_identity_revision = ?", c.ID, expectedRevision)
		if requireAdminRevision {
			query = query.Where("admin_revision = ?", expectedAdminRevision)
		}
		result := query.Updates(updates)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrStaleChannelAccountIdentityRevision
		}
		if replaceDirectCredentials {
			if err := replaceChannelDirectCredentialsTx(tx, c.ID, directCredentials, updatedAt); err != nil {
				return err
			}
		} else if clearDirectCredentialSecrets {
			var exhausted int64
			if err := tx.Model(&ChannelDirectCredential{}).
				Where("channel_id = ? AND revision >= ?", c.ID, maxChannelDirectCredentialRevision).
				Count(&exhausted).Error; err != nil {
				return err
			}
			if exhausted > 0 {
				return errors.New("direct credential revision exhausted")
			}
			if err := tx.Model(&ChannelDirectCredential{}).Where("channel_id = ?", c.ID).Updates(map[string]any{
				"api_key_cipher": "",
				"api_key_prefix": "",
				"revision":       gorm.Expr("revision + 1"),
				"updated_at":     updatedAt,
			}).Error; err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	c.UpstreamAccountIdentityRevision = revision
	c.AdminRevision = adminRevision
	c.UpdatedAt = updatedAt
	return nil
}

func replaceChannelDirectCredentialsTx(tx *gorm.DB, channelID uint, desired []ChannelDirectCredential, updatedAt time.Time) error {
	if tx == nil || channelID == 0 {
		return errors.New("direct credential replacement is unavailable")
	}
	existing := make([]ChannelDirectCredential, 0)
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("channel_id = ?", channelID).Order("id ASC").Find(&existing).Error; err != nil {
		return err
	}
	prepared, err := prepareChannelDirectCredentialReplacementCanonicalized(channelID, existing, desired, updatedAt, func(raw string) (string, error) {
		return canonicalUpstreamGroupName(tx, raw)
	})
	if err != nil {
		return err
	}

	retainedIDs := make(map[uint]struct{}, len(prepared))
	for _, item := range prepared {
		if item.ID != 0 {
			retainedIDs[item.ID] = struct{}{}
		}
	}
	removedIDs := make([]uint, 0, len(existing))
	for _, item := range existing {
		if _, retained := retainedIDs[item.ID]; !retained {
			removedIDs = append(removedIDs, item.ID)
		}
	}
	if len(removedIDs) > 0 {
		if err := tx.Where("channel_id = ? AND id IN ?", channelID, removedIDs).Delete(&ChannelDirectCredential{}).Error; err != nil {
			return err
		}
	}

	// Move retained rows out of the normalized-name namespace before applying
	// final values. This permits an administrator to swap two group names while
	// preserving both stable credential IDs and their runtime foreign keys.
	for id := range retainedIDs {
		placeholder := fmt.Sprintf("%sdirect_reconcile_%d_%d", ReservedInternalUpstreamGroupPrefix, channelID, id)
		result := tx.Model(&ChannelDirectCredential{}).
			Where("channel_id = ? AND id = ?", channelID, id).
			UpdateColumn("group_name", placeholder)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return fmt.Errorf("direct credential %d disappeared during replacement", id)
		}
	}

	newRows := make([]ChannelDirectCredential, 0, len(prepared))
	for _, item := range prepared {
		if item.ID == 0 {
			newRows = append(newRows, item)
			continue
		}
		result := tx.Model(&ChannelDirectCredential{}).
			Where("channel_id = ? AND id = ?", channelID, item.ID).
			Updates(map[string]any{
				"group_name": item.GroupName, "base_url": item.BaseURL,
				"api_key_cipher": item.APIKeyCipher, "api_key_prefix": item.APIKeyPrefix,
				"models": item.Models, "ratio": item.Ratio, "completion_ratio": item.CompletionRatio,
				"enabled": item.Enabled, "revision": item.Revision, "updated_at": item.UpdatedAt,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return fmt.Errorf("direct credential %d disappeared during replacement", item.ID)
		}
	}
	if len(newRows) > 0 {
		return tx.Create(&newRows).Error
	}
	return nil
}

const maxChannelDirectCredentialRevision = uint64(1<<63 - 1)

func prepareChannelDirectCredentialReplacement(channelID uint, existing, desired []ChannelDirectCredential, updatedAt time.Time) ([]ChannelDirectCredential, error) {
	return prepareChannelDirectCredentialReplacementCanonicalized(channelID, existing, desired, updatedAt, func(raw string) (string, error) {
		validated, err := validateUpstreamGroupName(raw)
		if err != nil {
			return "", err
		}
		return strings.ToLower(validated), nil
	})
}

func prepareChannelDirectCredentialReplacementCanonicalized(channelID uint, existing, desired []ChannelDirectCredential, updatedAt time.Time, canonicalize func(string) (string, error)) ([]ChannelDirectCredential, error) {
	if channelID == 0 {
		return nil, errors.New("direct credential replacement is unavailable")
	}
	if canonicalize == nil {
		return nil, errors.New("direct credential group canonicalizer is unavailable")
	}
	existingByGroup := make(map[string]ChannelDirectCredential, len(existing))
	existingByID := make(map[uint]ChannelDirectCredential, len(existing))
	for _, item := range existing {
		canonical, err := canonicalize(item.GroupName)
		if err != nil {
			return nil, fmt.Errorf("canonicalize existing direct credential %d: %w", item.ID, err)
		}
		if previous, duplicate := existingByGroup[canonical]; duplicate && previous.ID != item.ID {
			return nil, fmt.Errorf("direct credentials %d and %d share normalized group %q", previous.ID, item.ID, canonical)
		}
		existingByGroup[canonical] = item
		existingByID[item.ID] = item
	}
	seen := make(map[string]struct{}, len(desired))
	claimedExistingIDs := make(map[uint]struct{}, len(desired))
	prepared := make([]ChannelDirectCredential, 0, len(desired))
	for _, item := range desired {
		item.ChannelID = channelID
		item.GroupName = strings.TrimSpace(item.GroupName)
		canonical, err := canonicalize(item.GroupName)
		if err != nil {
			return nil, fmt.Errorf("direct credential group %q: %w", item.GroupName, err)
		}
		if _, duplicate := seen[canonical]; duplicate {
			return nil, fmt.Errorf("direct credential group %q is duplicated", item.GroupName)
		}
		seen[canonical] = struct{}{}
		if isReservedInternalUpstreamGroupName(item.GroupName) {
			return nil, ErrReservedInternalUpstreamGroupName
		}
		if strings.TrimSpace(item.APIKeyCipher) == "" {
			return nil, fmt.Errorf("direct credential group %q has no API key", item.GroupName)
		}
		if strings.TrimSpace(item.BaseURL) == "" {
			return nil, fmt.Errorf("direct credential group %q has no base URL", item.GroupName)
		}
		if _, err := ParseDirectModels(item.Models); err != nil {
			return nil, fmt.Errorf("direct credential group %q: %w", item.GroupName, err)
		}
		if item.Ratio <= 0 || math.IsNaN(item.Ratio) || math.IsInf(item.Ratio, 0) ||
			item.CompletionRatio <= 0 || math.IsNaN(item.CompletionRatio) || math.IsInf(item.CompletionRatio, 0) {
			return nil, fmt.Errorf("direct credential group %q has an invalid ratio", item.GroupName)
		}
		item.UpdatedAt = updatedAt
		old, ok := existingByGroup[canonical]
		if item.ID != 0 {
			var idFound bool
			old, idFound = existingByID[item.ID]
			if !idFound {
				return nil, fmt.Errorf("direct credential %d does not belong to channel %d", item.ID, channelID)
			}
			ok = true
		}
		if ok {
			if _, claimed := claimedExistingIDs[old.ID]; claimed {
				return nil, fmt.Errorf("direct credential %d is referenced more than once", old.ID)
			}
			claimedExistingIDs[old.ID] = struct{}{}
			item.ID = old.ID
			item.CreatedAt = old.CreatedAt
			item.Revision = old.Revision
			changed := item.APIKeyCipher != old.APIKeyCipher || item.APIKeyPrefix != old.APIKeyPrefix || item.BaseURL != old.BaseURL || item.Models != old.Models || item.Ratio != old.Ratio ||
				item.CompletionRatio != old.CompletionRatio || item.Enabled != old.Enabled || item.GroupName != old.GroupName
			if changed {
				if item.Revision >= maxChannelDirectCredentialRevision {
					return nil, errors.New("direct credential revision exhausted")
				}
				item.Revision++
			} else {
				item.UpdatedAt = old.UpdatedAt
			}
		}
		if !ok {
			item.ID = 0
			item.CreatedAt = time.Time{}
			item.Revision = 1
		}
		prepared = append(prepared, item)
	}
	return prepared, nil
}

func (r *Channels) ListDirectCredentials(channelID uint) ([]ChannelDirectCredential, error) {
	if r == nil || r.db == nil || channelID == 0 {
		return nil, errors.New("direct credential storage is unavailable")
	}
	var list []ChannelDirectCredential
	if err := r.db.Where("channel_id = ?", channelID).Order("id ASC").Find(&list).Error; err != nil {
		return nil, err
	}
	return list, nil
}

// EnsureLegacyDirectCredential bootstraps a pre-000070 direct channel when a
// test fixture or an interrupted migration still has only the old channel
// ciphertext fields. The copied ciphertext is never decrypted here.
func (r *Channels) EnsureLegacyDirectCredential(channel *Channel) ([]ChannelDirectCredential, error) {
	if r == nil || r.db == nil || channel == nil || channel.ID == 0 || channel.CredentialMode != CredentialModeDirect {
		return nil, errors.New("legacy direct credential bootstrap is unavailable")
	}
	list, err := r.ListDirectCredentials(channel.ID)
	if err != nil || len(list) > 0 {
		return list, err
	}
	if strings.TrimSpace(channel.PasswordCipher) == "" {
		return nil, errors.New("direct API key is empty")
	}
	models := strings.TrimSpace(channel.DirectModels)
	if models == "" {
		models = "[]"
	}
	if _, err := ParseDirectModels(models); err != nil {
		return nil, err
	}
	baseURL := strings.TrimRight(strings.TrimSpace(channel.SiteURL), "/")
	if baseURL == "" {
		return nil, errors.New("direct base URL is empty")
	}
	row := ChannelDirectCredential{
		ChannelID: channel.ID, GroupName: DirectChannelGroupName, BaseURL: baseURL,
		APIKeyCipher: channel.PasswordCipher, Models: models, Ratio: 1, CompletionRatio: 1,
		Enabled: true, Revision: 1,
	}
	if err := r.db.Create(&row).Error; err != nil {
		return nil, err
	}
	return []ChannelDirectCredential{row}, nil
}

func channelAdminFieldValue(c *Channel, field ChannelAdminField) (column string, value any, identity bool, err error) {
	switch field {
	case ChannelAdminFieldName:
		return string(field), c.Name, false, nil
	case ChannelAdminFieldGatewayApp:
		return string(field), c.GatewayApp, false, nil
	case ChannelAdminFieldSiteURL:
		return string(field), c.SiteURL, true, nil
	case ChannelAdminFieldAPIURL:
		return string(field), c.APIURL, true, nil
	case ChannelAdminFieldUsername:
		return string(field), c.Username, true, nil
	case ChannelAdminFieldSortOrder:
		return string(field), c.SortOrder, false, nil
	case ChannelAdminFieldPasswordCipher:
		return string(field), c.PasswordCipher, true, nil
	case ChannelAdminFieldCredentialMode:
		return string(field), c.CredentialMode, true, nil
	case ChannelAdminFieldDirectModels:
		return string(field), c.DirectModels, false, nil
	case ChannelAdminFieldLoginExtraParams:
		return string(field), c.LoginExtraParams, true, nil
	case ChannelAdminFieldTurnstileEnabled:
		return string(field), c.TurnstileEnabled, false, nil
	case ChannelAdminFieldIgnoreAnnouncements:
		return string(field), c.IgnoreAnnouncements, false, nil
	case ChannelAdminFieldSubscriptionEnabled:
		return string(field), c.SubscriptionEnabled, false, nil
	case ChannelAdminFieldProxyEnabled:
		return string(field), c.ProxyEnabled, false, nil
	case ChannelAdminFieldNetworkProxyID:
		return string(field), c.NetworkProxyID, false, nil
	case ChannelAdminFieldCaptchaConfigID:
		return string(field), c.CaptchaConfigID, false, nil
	case ChannelAdminFieldBalanceThreshold:
		return string(field), c.BalanceThreshold, false, nil
	case ChannelAdminFieldRechargeMultiplier:
		return string(field), c.RechargeMultiplier, false, nil
	case ChannelAdminFieldRechargeMultiplierMode:
		return string(field), c.RechargeMultiplierMode, false, nil
	case ChannelAdminFieldMonitorEnabled:
		return string(field), c.MonitorEnabled, false, nil
	default:
		return "", nil, false, fmt.Errorf("channel administrator field %q is not writable", field)
	}
}

func channelAdminIdentityFieldChanged(current, desired *Channel, field ChannelAdminField) bool {
	if current == nil || desired == nil {
		return current != desired
	}
	switch field {
	case ChannelAdminFieldSiteURL:
		return current.SiteURL != desired.SiteURL
	case ChannelAdminFieldAPIURL:
		return current.APIURL != desired.APIURL
	case ChannelAdminFieldUsername:
		// Token-authenticated upstreams do not select an account by display
		// username. A note edit must not invalidate the live account, session,
		// or managed-key ownership; password mode still treats the username as
		// part of account identity.
		if current.CredentialMode == CredentialModeToken && desired.CredentialMode == CredentialModeToken {
			return false
		}
		return current.Username != desired.Username
	case ChannelAdminFieldPasswordCipher:
		return current.PasswordCipher != desired.PasswordCipher
	case ChannelAdminFieldCredentialMode:
		return current.CredentialMode != desired.CredentialMode
	case ChannelAdminFieldLoginExtraParams:
		return current.LoginExtraParams != desired.LoginExtraParams
	default:
		return false
	}
}

// UpdateWithUpstreamAccountIdentityChange saves an administrator or token
// credential change while atomically invalidating account facts from the old
// configuration. Locking the channel first matches the observation lock order,
// so an old response either binds before this transaction and is removed here,
// or arrives afterward and fails the revision check.
func (r *Channels) UpdateWithUpstreamAccountIdentityChange(c *Channel) error {
	return r.updateChannelFields(c, false, false,
		ChannelAdminFieldSiteURL,
		ChannelAdminFieldAPIURL,
		ChannelAdminFieldUsername,
		ChannelAdminFieldPasswordCipher,
		ChannelAdminFieldCredentialMode,
		ChannelAdminFieldLoginExtraParams,
	)
}

// resetChannelAccountScopedMonitorStateTx prevents facts and notification
// cooldowns from one configured upstream account from suppressing or appearing
// under the next account that reuses the same BridgeOps channel row.
func resetChannelAccountScopedMonitorStateTx(tx *gorm.DB, channelID uint) error {
	if err := tx.Where("channel_id = ?", channelID).Delete(&UpstreamAnnouncement{}).Error; err != nil {
		return err
	}
	return tx.Where("channel_id = ?", channelID).Delete(&NotificationCooldown{}).Error
}

// UpdateTokenCredentialForIdentity rotates a token-mode credential issued by
// the already authenticated upstream account. Refresh-token rotation must not
// be treated as an administrator account switch: the identity revision,
// model facts, cooldowns, managed keys, and cost evidence all remain valid.
// The revision fence still prevents a refresh started before an administrator
// identity change from overwriting the new credential.
func (r *Channels) UpdateTokenCredentialForIdentity(c *Channel, expectedCredential string) error {
	if r == nil || r.db == nil || c == nil || c.ID == 0 || c.UpstreamAccountIdentityRevision == 0 {
		return ErrStaleChannelAccountIdentityRevision
	}
	if c.CredentialMode != CredentialModeToken || strings.TrimSpace(c.PasswordCipher) == "" || strings.TrimSpace(expectedCredential) == "" {
		return errors.New("token credential rotation requires a non-empty token credential")
	}
	expectedRevision := c.UpstreamAccountIdentityRevision
	var updatedAt time.Time
	err := r.db.Transaction(func(tx *gorm.DB) error {
		var current Channel
		if _, err := requireOrdinaryChannelWriteTx(tx, c.ID); err != nil {
			return err
		}
		if err := tx.
			Select("id", "password_cipher", "credential_mode", "upstream_account_identity_revision").
			First(&current, c.ID).Error; err != nil {
			return err
		}
		if current.UpstreamAccountIdentityRevision != expectedRevision {
			return ErrStaleChannelAccountIdentityRevision
		}
		if current.PasswordCipher != expectedCredential {
			return ErrStaleChannelTokenCredential
		}
		if current.CredentialMode != CredentialModeToken {
			return errors.New("token credential rotation requires token mode")
		}
		updatedAt = time.Now().UTC()
		result := tx.Session(&gorm.Session{SkipHooks: true}).
			Model(&Channel{}).
			Where("id = ? AND password_cipher = ? AND credential_mode = ? AND upstream_account_identity_revision = ?", c.ID, expectedCredential, CredentialModeToken, expectedRevision).
			Updates(map[string]any{
				"password_cipher": c.PasswordCipher,
				"updated_at":      updatedAt,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrStaleChannelTokenCredential
		}
		return nil
	})
	if err == nil {
		c.UpdatedAt = updatedAt
	}
	return err
}

// InvalidateAdminAuthState is the password-mode "clear login" fence. Even
// though no account-selecting field changes, advancing the identity revision
// prevents a login/refresh started before the command from recreating the
// deleted session. The administrator revision also advances so an older open
// edit form cannot silently overwrite this command.
func (r *Channels) InvalidateAdminAuthState(c *Channel) error {
	return r.updateChannelFields(c, true, true)
}

// InvalidateAdminDirectAuthState clears every administrator-owned direct API
// secret in the same transaction that advances the channel identity and admin
// fences. Configuration rows and their stable IDs remain available for editing.
func (r *Channels) InvalidateAdminDirectAuthState(c *Channel) error {
	if c == nil || c.CredentialMode != CredentialModeDirect {
		return errors.New("direct credential clearing requires direct mode")
	}
	c.PasswordCipher = ""
	return r.updateChannelFieldsWithDirectCredentials(
		c,
		true,
		true,
		false,
		nil,
		true,
		ChannelAdminFieldPasswordCipher,
	)
}

func invalidateManagedKeysForChannelIdentityChangeTx(tx *gorm.DB, channelID uint, at time.Time) error {
	if err := tx.Model(&GatewayUpstreamKey{}).Where("channel_id = ?", channelID).Updates(map[string]any{
		"ownership_provenance":                     GatewayUpstreamKeyProvenanceUnknown,
		"provisioning":                             false,
		"provisioning_started_at":                  nil,
		"enabled":                                  false,
		"last_used_at":                             nil,
		"last_routing_failure_error":               "",
		"last_routing_failure_kind":                "",
		"last_routing_failure_status":              0,
		"last_routing_failure_at":                  nil,
		"routing_failure_count":                    0,
		"routing_cooldown_until":                   nil,
		"routing_isolated_at":                      nil,
		"routing_health_upstream_group_id":         0,
		"routing_health_presence_generation":       0,
		"routing_health_channel_identity_revision": 0,
		"last_error":                               "channel account identity changed; managed key ownership requires review",
		"updated_at":                               at,
	}).Error; err != nil {
		return err
	}
	if err := tx.Where("channel_id = ?", channelID).Delete(&GatewayModelCapability{}).Error; err != nil {
		return err
	}
	return tx.Where("channel_id = ?", channelID).Delete(&GatewayRoutingFailure{}).Error
}

// resetChannelCostStateForIdentityChangeTx fences in-flight cost workers and
// prevents pending funding evidence from the old upstream account from being
// applied to the new one. Historical rows remain auditable as superseded.
func resetChannelCostStateForIdentityChangeTx(tx *gorm.DB, channelID uint, at time.Time) error {
	result := tx.Model(&ChannelCostState{}).
		Where("channel_id = ?", channelID).
		Updates(map[string]any{
			"raw_balance":        0,
			"actual_inventory":   0,
			"funding_factor":     1,
			"raw_today_cost":     0,
			"actual_today_cost":  0,
			"raw_total_cost":     0,
			"actual_total_cost":  0,
			"cost_sampled_at":    nil,
			"balance_sampled_at": nil,
			"version":            gorm.Expr("version + 1"),
			"source":             "default",
			"status":             "ready",
			"last_error":         "",
			"updated_at":         at.UTC(),
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("channel cost state %d is missing", channelID)
	}
	return tx.Model(&ChannelFundingCandidate{}).
		Where("channel_id = ? AND status = ?", channelID, FundingCandidatePending).
		Updates(map[string]any{
			"status":     FundingCandidateSuperseded,
			"note":       gorm.Expr("CASE WHEN COALESCE(note, '') = '' THEN ? ELSE note || '; ' || ? END", "upstream account identity changed", "upstream account identity changed"),
			"updated_at": at.UTC(),
		}).Error
}

const maxUpstreamAccountIdentityRevision = uint64(1<<63 - 1)
const maxChannelAdminRevision = uint64(1<<63 - 1)
const maxChannelDeletionEpoch = uint64(1<<63 - 1)

var ErrStaleChannelAccountIdentityRevision = errors.New("channel account identity revision changed")
var ErrStaleChannelTokenCredential = errors.New("channel token credential changed")
var ErrStaleChannelAdminRevision = errors.New("channel administrator revision changed")
var ErrChannelAdminRevisionRequired = errors.New("channel administrator revision is required")
var ErrChannelDeletionPending = errors.New("channel deletion is pending remote cleanup")
var ErrStaleChannelDeletionFence = errors.New("channel deletion epoch or identity changed")
var ErrChannelDeletionIntentMissing = errors.New("channel deletion intent is missing")

func nextUpstreamAccountIdentityRevision(current uint64) (uint64, error) {
	if current == 0 {
		current = 1
	}
	if current >= maxUpstreamAccountIdentityRevision {
		return 0, fmt.Errorf("upstream account identity revision exhausted")
	}
	return current + 1, nil
}

func nextChannelAdminRevision(current uint64) (uint64, error) {
	if current == 0 {
		current = 1
	}
	if current >= maxChannelAdminRevision {
		return 0, fmt.Errorf("channel administrator revision exhausted")
	}
	return current + 1, nil
}

// UpdateAPIURLRevisionIfUnchanged persists a discovered API origin and
// advances the account-identity revision in the same channel-locked
// transaction. The caller receives the revision required to persist facts
// obtained from that discovery request.
func (r *Channels) UpdateAPIURLRevisionIfUnchanged(id uint, expectedRevision uint64, siteURL, previousAPI, discovered string) (bool, uint64, error) {
	if r == nil || r.db == nil || id == 0 || expectedRevision == 0 || expectedRevision > maxUpstreamAccountIdentityRevision {
		return false, 0, errors.New("channel API origin update is unavailable")
	}
	var updated bool
	var revision uint64
	err := r.db.Transaction(func(tx *gorm.DB) error {
		var current Channel
		if _, err := requireOrdinaryChannelWriteTx(tx, id); err != nil {
			return err
		}
		if err := tx.
			Select("id", "site_url", "api_url", "upstream_account_identity_revision").
			First(&current, id).Error; err != nil {
			return err
		}
		revision = current.UpstreamAccountIdentityRevision
		if revision != expectedRevision || current.SiteURL != siteURL || current.APIURL != previousAPI {
			return nil
		}
		next, err := nextUpstreamAccountIdentityRevision(revision)
		if err != nil {
			return err
		}
		if err := tx.Where("channel_id = ?", id).
			Delete(&GatewayChannelUpstreamAccountBinding{}).Error; err != nil {
			return err
		}
		if err := tx.Where("channel_id = ?", id).Delete(&AuthSession{}).Error; err != nil {
			return err
		}
		identityChangedAt := time.Now().UTC()
		if err := resetChannelModelFactsForIdentityChangeTx(tx, id, identityChangedAt); err != nil {
			return err
		}
		if err := invalidateManagedKeysForChannelIdentityChangeTx(tx, id, identityChangedAt); err != nil {
			return err
		}
		if err := resetChannelCostStateForIdentityChangeTx(tx, id, identityChangedAt); err != nil {
			return err
		}
		if err := resetChannelAccountScopedMonitorStateTx(tx, id); err != nil {
			return err
		}
		result := tx.Model(&Channel{}).Where("id = ? AND upstream_account_identity_revision = ?", id, revision).
			Updates(func() map[string]any {
				updates := map[string]any{
					"api_url":                            discovered,
					"upstream_account_identity_revision": next,
					"updated_at":                         time.Now().UTC(),
				}
				resetChannelBalanceObservation(updates)
				return updates
			}())
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return errors.New("channel API origin revision changed unexpectedly")
		}
		updated = true
		revision = next
		return nil
	})
	return updated, revision, err
}

// LegacyNetworkProxyBindingCount reports pre-proxy-pool channel rows. These
// rows used the retired global proxy switch and must be migrated or cleared
// before channel traffic is allowed to resolve.
func (r *Channels) LegacyNetworkProxyBindingCount() (int64, error) {
	var count int64
	err := r.db.Model(&Channel{}).
		Where("proxy_enabled = ? AND network_proxy_id IS NULL", true).
		Count(&count).Error
	return count, err
}

// BindLegacyNetworkProxy atomically converts every remaining legacy channel
// switch to one explicit proxy profile. It is only used during startup
// migration, before schedulers begin channel work.
func (r *Channels) BindLegacyNetworkProxy(proxyID uint) (int64, error) {
	if proxyID == 0 {
		return 0, errors.New("network proxy id is required")
	}
	result := r.db.Model(&Channel{}).
		Where("proxy_enabled = ? AND network_proxy_id IS NULL", true).
		Updates(map[string]any{"network_proxy_id": proxyID, "proxy_enabled": true})
	return result.RowsAffected, result.Error
}

// ClearLegacyNetworkProxyFlags preserves the effective behavior of old rows
// when the retired global proxy is disabled: they become explicit direct
// channels instead of retaining an ambiguous switch.
func (r *Channels) ClearLegacyNetworkProxyFlags() (int64, error) {
	result := r.db.Model(&Channel{}).
		Where("proxy_enabled = ? AND network_proxy_id IS NULL", true).
		Update("proxy_enabled", false)
	return result.RowsAffected, result.Error
}

// Delete only prepares a durable, identity-bound remote cleanup operation.
// It intentionally returns ErrChannelDeletionPending after a successful
// prepare so callers cannot mistake this state for a completed physical
// deletion. A later coordinator must revalidate every remote key and invoke a
// separately fenced finalizer; until then all local secrets, sessions, facts,
// and historical evidence remain available for recovery.
func (r *Channels) Delete(id uint) error {
	return r.DeleteContext(context.Background(), id)
}

// DeleteContext prepares deletion using the caller's cancellation and
// deadline. Delete remains a compatibility wrapper for storage-only callers.
func (r *Channels) DeleteContext(ctx context.Context, id uint) error {
	if r == nil || r.db == nil || id == 0 {
		return errors.New("channel deletion is unavailable")
	}
	if ctx == nil {
		return errors.New("channel deletion context is required")
	}
	if err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var channel Channel
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Select(
				"id", "name", "type", "monitor_enabled", "latency_probe_enabled",
				"credential_mode",
				"upstream_account_identity_revision", "admin_revision",
				"deletion_pending", "deletion_epoch", "deletion_identity_revision",
				"deletion_state", "deletion_requested_at",
			).First(&channel, id).Error; err != nil {
			return err
		}

		if channel.DeletionPending {
			return requirePreparedChannelDeletionIntentTx(tx, channel)
		}
		if channel.DeletionEpoch != 0 || channel.DeletionIdentityRevision != 0 || channel.DeletionState != "" || channel.DeletionRequestedAt != nil {
			return ErrStaleChannelDeletionFence
		}
		if channel.UpstreamAccountIdentityRevision == 0 || channel.UpstreamAccountIdentityRevision > maxUpstreamAccountIdentityRevision {
			return ErrStaleChannelAccountIdentityRevision
		}

		// Match the strongest existing ordering before taking any per-group
		// fences: parent channel -> automatic assignment -> sorted canonical
		// routing domains -> locked group/key rows.
		if err := lockAutomaticAssignmentMutationTx(tx); err != nil {
			return err
		}
		at := time.Now().UTC()
		groups, err := lockChannelDeletionPrepareDomainsTx(tx, channel.ID, at)
		if err != nil {
			return err
		}
		keys, err := lockChannelDeletionUpstreamKeysTx(tx, channel.ID)
		if err != nil {
			return err
		}
		keys, err = deleteDirectChannelLocalRoutingStateForDeletionPrepareTx(tx, channel, keys)
		if err != nil {
			return err
		}

		epoch, err := nextChannelDeletionEpoch(channel.DeletionEpoch)
		if err != nil {
			return err
		}
		adminRevision := channel.AdminRevision
		if adminRevision == 0 {
			adminRevision = 1
		}
		adminRevision, err = nextChannelAdminRevision(adminRevision)
		if err != nil {
			return err
		}
		result := tx.Session(&gorm.Session{SkipHooks: true}).Model(&Channel{}).
			Where(
				"id = ? AND deletion_pending = ? AND deletion_epoch = ? AND deletion_identity_revision = ? AND upstream_account_identity_revision = ? AND admin_revision = ?",
				channel.ID, false, 0, 0, channel.UpstreamAccountIdentityRevision, channel.AdminRevision,
			).
			Updates(map[string]any{
				"deletion_pending":           true,
				"deletion_epoch":             epoch,
				"deletion_identity_revision": channel.UpstreamAccountIdentityRevision,
				"deletion_state":             ChannelDeletionIntentPendingRemoteCleanup,
				"deletion_requested_at":      at,
				"monitor_enabled":            false,
				"latency_probe_enabled":      false,
				"admin_revision":             adminRevision,
				"updated_at":                 at,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrStaleChannelDeletionFence
		}

		intent := ChannelDeletionIntent{
			ChannelID:               channel.ID,
			DeletionEpoch:           epoch,
			ChannelIdentityRevision: channel.UpstreamAccountIdentityRevision,
			ChannelNameSnapshot:     channel.Name,
			ChannelTypeSnapshot:     channel.Type,
			Status:                  ChannelDeletionIntentPendingRemoteCleanup,
			RequestedAt:             at,
			CreatedAt:               at,
			UpdatedAt:               at,
		}
		if err := tx.Create(&intent).Error; err != nil {
			return err
		}
		if err := appendPreparedChannelDeletionKeyAuditsTx(tx, intent, keys, groups, at); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return ErrChannelDeletionPending
}

// RequirePendingDeletionFence is the read-side fence future coordinators use
// before any remote mutation or local finalization. It deliberately locks the
// still-present channel row and rejects a changed identity, epoch, or status.
func (r *Channels) RequirePendingDeletionFence(channelID uint, expectedEpoch, expectedIdentityRevision uint64) (*Channel, error) {
	if r == nil || r.db == nil || channelID == 0 || expectedEpoch == 0 || expectedIdentityRevision == 0 {
		return nil, ErrStaleChannelDeletionFence
	}
	var channel Channel
	err := r.db.Transaction(func(tx *gorm.DB) error {
		var err error
		channel, err = requirePendingChannelDeletionFenceTx(tx, channelID, expectedEpoch, expectedIdentityRevision)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &channel, nil
}

func requirePendingChannelDeletionFenceTx(tx *gorm.DB, channelID uint, expectedEpoch, expectedIdentityRevision uint64) (Channel, error) {
	var channel Channel
	if tx == nil || channelID == 0 || expectedEpoch == 0 || expectedIdentityRevision == 0 {
		return channel, ErrStaleChannelDeletionFence
	}
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&channel, channelID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return channel, ErrStaleChannelDeletionFence
		}
		return channel, err
	}
	if !channel.DeletionPending || channel.DeletionEpoch != expectedEpoch ||
		channel.DeletionIdentityRevision != expectedIdentityRevision ||
		channel.UpstreamAccountIdentityRevision != expectedIdentityRevision ||
		channel.DeletionRequestedAt == nil || channel.DeletionState == "" ||
		channel.MonitorEnabled || channel.LatencyProbeEnabled {
		return channel, ErrStaleChannelDeletionFence
	}
	if err := requirePreparedChannelDeletionIntentTx(tx, channel); err != nil {
		return channel, err
	}
	return channel, nil
}

func requirePreparedChannelDeletionIntentTx(tx *gorm.DB, channel Channel) error {
	if !channel.DeletionPending || channel.DeletionEpoch == 0 || channel.DeletionIdentityRevision == 0 ||
		channel.DeletionIdentityRevision != channel.UpstreamAccountIdentityRevision ||
		channel.DeletionRequestedAt == nil || channel.DeletionState == "" ||
		channel.MonitorEnabled || channel.LatencyProbeEnabled {
		return ErrStaleChannelDeletionFence
	}
	var intent ChannelDeletionIntent
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("channel_id = ? AND deletion_epoch = ?", channel.ID, channel.DeletionEpoch).
		First(&intent).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ErrChannelDeletionIntentMissing
	}
	if err != nil {
		return err
	}
	if intent.ChannelIdentityRevision != channel.DeletionIdentityRevision ||
		intent.ChannelNameSnapshot != channel.Name || intent.ChannelTypeSnapshot != channel.Type ||
		intent.Status == "" || intent.Status == ChannelDeletionIntentFinalized {
		return ErrStaleChannelDeletionFence
	}
	return nil
}

func nextChannelDeletionEpoch(current uint64) (uint64, error) {
	if current != 0 {
		return 0, ErrStaleChannelDeletionFence
	}
	if current >= maxChannelDeletionEpoch {
		return 0, errors.New("channel deletion epoch is exhausted")
	}
	return current + 1, nil
}

func lockChannelDeletionPrepareDomainsTx(tx *gorm.DB, channelID uint, at time.Time) (map[string]GatewayUpstreamGroup, error) {
	if tx == nil || channelID == 0 {
		return nil, errors.New("channel deletion domain is invalid")
	}
	if at.IsZero() {
		at = time.Now().UTC()
	} else {
		at = at.UTC()
	}
	var rateNames []string
	if err := tx.Model(&RateSnapshot{}).Where("channel_id = ?", channelID).Pluck("model_name", &rateNames).Error; err != nil {
		return nil, err
	}
	var groupNames []string
	if err := tx.Model(&GatewayUpstreamGroup{}).Where("channel_id = ?", channelID).Pluck("canonical_group_name", &groupNames).Error; err != nil {
		return nil, err
	}
	var keyNames []string
	if err := tx.Model(&GatewayUpstreamKey{}).Where("channel_id = ?", channelID).Pluck("group_name", &keyNames).Error; err != nil {
		return nil, err
	}
	var removalIntentNames []string
	if err := tx.Model(&GatewayUpstreamGroupChange{}).
		Where("channel_id = ? AND removal_claim_key IS NOT NULL", channelID).
		Pluck("group_name", &removalIntentNames).Error; err != nil {
		return nil, err
	}
	var rebuildIntentNames []string
	if err := tx.Model(&GatewayManagedKeyRebuildIntent{}).
		Where("channel_id = ?", channelID).
		Pluck("canonical_group_name", &rebuildIntentNames).Error; err != nil {
		return nil, err
	}

	canonicalNames := make(map[string]struct{}, len(rateNames)+len(groupNames)+len(keyNames)+len(removalIntentNames)+len(rebuildIntentNames))
	rateCanonicalNames := make(map[string]struct{}, len(rateNames))
	for _, rawName := range rateNames {
		if strings.TrimSpace(rawName) == "" {
			continue
		}
		canonical, err := canonicalUpstreamGroupName(tx, rawName)
		if err != nil {
			return nil, err
		}
		canonicalNames[canonical] = struct{}{}
		rateCanonicalNames[canonical] = struct{}{}
	}
	domainNames := make([]string, 0, len(groupNames)+len(keyNames)+len(removalIntentNames)+len(rebuildIntentNames))
	domainNames = append(domainNames, groupNames...)
	domainNames = append(domainNames, keyNames...)
	domainNames = append(domainNames, removalIntentNames...)
	domainNames = append(domainNames, rebuildIntentNames...)
	for _, rawName := range domainNames {
		if strings.TrimSpace(rawName) == "" {
			continue
		}
		canonical, err := canonicalUpstreamGroupName(tx, rawName)
		if err != nil {
			return nil, err
		}
		canonicalNames[canonical] = struct{}{}
	}
	ordered := make([]string, 0, len(canonicalNames))
	for canonical := range canonicalNames {
		ordered = append(ordered, canonical)
	}
	sort.Strings(ordered)
	for _, canonical := range ordered {
		if _, err := lockRateSnapshotPresenceDomainTx(tx, channelID, canonical); err != nil {
			return nil, err
		}
	}

	var groups []GatewayUpstreamGroup
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("channel_id = ?", channelID).Order("canonical_group_name ASC, id ASC").Find(&groups).Error; err != nil {
		return nil, err
	}
	byCanonical := make(map[string]GatewayUpstreamGroup, len(groups))
	for _, group := range groups {
		byCanonical[group.CanonicalGroupName] = group
	}
	// A rate is the current presence fact even if an older inconsistent row says
	// the group is absent. Establish its current generation before retiring it.
	// Key- or intent-only groups still need a durable absent identity so later
	// claims cannot be confused with a same-name generation after a restart.
	for _, canonical := range ordered {
		_, hasRate := rateCanonicalNames[canonical]
		if _, exists := byCanonical[canonical]; exists && !hasRate {
			continue
		}
		group, _, err := ensureUpstreamGroupIdentityTx(tx, channelID, canonical, hasRate, at)
		if err != nil {
			return nil, err
		}
		byCanonical[canonical] = *group
	}
	if err := tx.Where("channel_id = ?", channelID).Delete(&RateSnapshot{}).Error; err != nil {
		return nil, err
	}
	for _, canonical := range ordered {
		frozen := byCanonical[canonical]
		group, _, err := ensureUpstreamGroupIdentityTx(tx, channelID, canonical, false, at)
		if err != nil {
			return nil, err
		}
		if group.ID != frozen.ID || group.PresenceGeneration != frozen.PresenceGeneration || group.Present {
			return nil, ErrChannelDeletionKeyFreezeChanged
		}
		// Deletion prepare freezes the parent channel and makes this generation
		// absent, but keeps assignment/member/override/claim rows available for
		// recovery and audit. The finalizer removes channel-scoped live state.
		byCanonical[canonical] = *group
	}
	return byCanonical, nil
}

func lockChannelDeletionUpstreamKeysTx(tx *gorm.DB, channelID uint) ([]GatewayUpstreamKey, error) {
	keys := make([]GatewayUpstreamKey, 0)
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("channel_id = ?", channelID).Order("id ASC").Find(&keys).Error
	return keys, err
}

// Direct channels do not own a provider-side API key. Their direct-group key
// is a local routing cache of the encrypted channel credential, so retaining
// it as an unknown-provenance remote-deletion claim would incorrectly require
// manual review. Remove only the exact synthetic row while deletion prepare is
// still atomic; any unexpected row remains frozen for the normal coordinator.
func deleteDirectChannelLocalRoutingStateForDeletionPrepareTx(tx *gorm.DB, channel Channel, keys []GatewayUpstreamKey) ([]GatewayUpstreamKey, error) {
	if tx == nil || channel.ID == 0 || channel.CredentialMode != CredentialModeDirect {
		return keys, nil
	}
	remaining := make([]GatewayUpstreamKey, 0, len(keys))
	removed := false
	for _, key := range keys {
		localDirectKey := key.ChannelID == channel.ID && key.UpstreamKeyID == 0 &&
			(key.DirectCredentialID != nil || (strings.EqualFold(strings.TrimSpace(key.UpstreamKeyName), DirectChannelUpstreamKeyName) &&
				(strings.EqualFold(strings.TrimSpace(key.GroupName), DirectChannelGroupName) || (key.DirectCredentialRevision > 0 && strings.TrimSpace(key.DirectBaseURL) != "")))) &&
			!key.Provisioning && key.ProvisioningStartedAt == nil &&
			key.ChannelDeletionIntentID == nil &&
			key.ChannelDeletionEpoch == nil &&
			key.ChannelDeletionIdentityRevision == nil &&
			key.ChannelDeletionUpstreamGroupID == nil &&
			key.ChannelDeletionPresenceGeneration == nil
		if !localDirectKey || strings.TrimSpace(key.GroupName) == "" {
			remaining = append(remaining, key)
			continue
		}
		result := tx.Where(
			"id = ? AND channel_id = ? AND key_generation = ? AND upstream_key_id = ? AND provisioning = ? AND provisioning_started_at IS NULL AND channel_deletion_intent_id IS NULL AND channel_deletion_epoch IS NULL AND channel_deletion_identity_revision IS NULL AND channel_deletion_upstream_group_id IS NULL AND channel_deletion_presence_generation IS NULL",
			key.ID, channel.ID, key.KeyGeneration, 0, false,
		).Delete(&GatewayUpstreamKey{})
		if result.Error != nil {
			return nil, result.Error
		}
		if result.RowsAffected != 1 {
			return nil, ErrChannelDeletionKeyFreezeChanged
		}
		removed = true
	}
	if !removed {
		return remaining, nil
	}
	if err := tx.Where("channel_id = ?", channel.ID).Delete(&GatewayModelCapability{}).Error; err != nil {
		return nil, err
	}
	if err := tx.Where("channel_id = ?", channel.ID).Delete(&GatewayRoutingFailure{}).Error; err != nil {
		return nil, err
	}
	return remaining, nil
}

func appendPreparedChannelDeletionKeyAuditsTx(tx *gorm.DB, intent ChannelDeletionIntent, keys []GatewayUpstreamKey, groups map[string]GatewayUpstreamGroup, at time.Time) error {
	if tx == nil || intent.ID == 0 || intent.ChannelID == 0 || intent.DeletionEpoch == 0 || intent.ChannelIdentityRevision == 0 {
		return ErrStaleChannelDeletionFence
	}
	for _, key := range keys {
		if key.ID == 0 || key.ChannelID != intent.ChannelID {
			return ErrStaleChannelDeletionFence
		}
		provenance, reason := preparedChannelDeletionKeyProvenance(key.OwnershipProvenance)
		disposition := channelDeletionKeyDispositionForKey(key, provenance)
		localOwnershipClaim := ""
		if disposition == ChannelDeletionKeyDispositionVerifyManagedDelete {
			claim, claimErr := ChannelDeletionManagedOwnershipClaimDigest(key)
			if claimErr != nil {
				reason = "local_ownership_claim_incomplete"
			} else {
				localOwnershipClaim = claim
			}
		}
		canonicalGroupName := ""
		var upstreamGroupID *uint
		var presenceGeneration *uint64
		if strings.TrimSpace(key.GroupName) != "" {
			canonical, err := canonicalUpstreamGroupName(tx, key.GroupName)
			if err != nil {
				return err
			}
			canonicalGroupName = canonical
			if group, ok := groups[canonical]; ok {
				groupID := group.ID
				generation := group.PresenceGeneration
				upstreamGroupID = &groupID
				presenceGeneration = &generation
			}
		}
		audit := ChannelDeletionKeyAudit{
			IntentID:                intent.ID,
			ChannelID:               intent.ChannelID,
			DeletionEpoch:           intent.DeletionEpoch,
			ChannelIdentityRevision: intent.ChannelIdentityRevision,
			LocalKeyID:              key.ID,
			UpstreamGroupID:         upstreamGroupID,
			PresenceGeneration:      presenceGeneration,
			CanonicalGroupName:      canonicalGroupName,
			UpstreamKeyID:           key.UpstreamKeyID,
			UpstreamKeyName:         strings.TrimSpace(key.UpstreamKeyName),
			KeyGeneration:           key.KeyGeneration,
			OwnershipProvenance:     provenance,
			Provisioning:            key.Provisioning,
			LocalOwnershipClaim:     localOwnershipClaim,
			Disposition:             disposition,
			Reason:                  reason,
			CreatedAt:               at,
		}
		if err := tx.Create(&audit).Error; err != nil {
			return err
		}
		if err := appendPreparedChannelDeletionKeyClaimTx(tx, intent, audit, key, at); err != nil {
			return err
		}
	}
	return nil
}

func preparedChannelDeletionKeyProvenance(value string) (string, string) {
	provenance, err := normalizeGatewayUpstreamKeyOwnershipProvenance(value)
	if err != nil {
		return GatewayUpstreamKeyProvenanceUnknown, "invalid_local_ownership_provenance"
	}
	return provenance, "prepared_local_snapshot"
}

func channelDeletionKeyDispositionForKey(key GatewayUpstreamKey, provenance string) ChannelDeletionKeyDisposition {
	if provenance != GatewayUpstreamKeyProvenanceAdopted &&
		gatewayProvisioningKeyNamePattern.MatchString(strings.TrimSpace(key.UpstreamKeyName)) {
		return ChannelDeletionKeyDispositionVerifyManagedDelete
	}
	switch provenance {
	case GatewayUpstreamKeyProvenanceCreated, GatewayUpstreamKeyProvenanceProvisioningClaim:
		return ChannelDeletionKeyDispositionVerifyManagedDelete
	case GatewayUpstreamKeyProvenanceAdopted:
		return ChannelDeletionKeyDispositionRetainAdopted
	default:
		return ChannelDeletionKeyDispositionManualReview
	}
}
func (r *Channels) FindByID(id uint) (*Channel, error) {
	var c Channel
	if err := r.db.First(&c, id).Error; err != nil {
		return nil, err
	}
	return &c, nil
}

// ListByIDs returns the requested channels without the aggregate projections
// used by the administrator list view. Configuration projections use this to
// avoid one FindByID query per candidate member.
func (r *Channels) ListByIDs(ids []uint) ([]Channel, error) {
	list := make([]Channel, 0)
	if len(ids) == 0 {
		return list, nil
	}
	if err := r.db.Where("id IN ?", ids).Order("id ASC").Find(&list).Error; err != nil {
		return nil, err
	}
	return list, nil
}

func (r *Channels) List() ([]Channel, error) {
	var list []Channel
	query, err := r.channelListQuery()
	if err != nil {
		return nil, err
	}
	if err := query.Order("sort_order DESC").Order("id ASC").Find(&list).Error; err != nil {
		return nil, err
	}
	return list, nil
}
func (r *Channels) ListPage(page, pageSize int) ([]Channel, int64, error) {
	if page < 1 {
		page = 1
	}
	if pageSize <= 0 && pageSize != -1 {
		pageSize = 20
	}
	var total int64
	if err := r.db.Model(&Channel{}).Count(&total).Error; err != nil {
		return nil, 0, err
	}
	q, err := r.channelListQuery()
	if err != nil {
		return nil, 0, err
	}
	var list []Channel
	q = q.Order("request_count DESC").Order("sort_order DESC").Order("id ASC")
	if pageSize != -1 {
		q = q.Offset((page - 1) * pageSize).Limit(pageSize)
	}
	if err := q.Find(&list).Error; err != nil {
		return nil, 0, err
	}
	return list, total, nil
}

func (r *Channels) channelListQuery() (*gorm.DB, error) {
	ready, err := requestAggregatesReady(r.db)
	if err != nil {
		return nil, err
	}
	projection := `channels.*,
		COALESCE((SELECT SUM(gateway_request_aggregates.requests) FROM gateway_request_aggregates
			 WHERE gateway_request_aggregates.channel_id = channels.id
			 AND gateway_request_aggregates.version = ?), 0)`
	args := []any{gatewayRequestAggregateVersion}
	if !ready {
		projection += ` +
		COALESCE((SELECT COUNT(*) FROM gateway_request_logs
			 WHERE gateway_request_logs.channel_id = channels.id
			 AND gateway_request_logs.aggregate_version <> ?
			 AND ` + gatewayRequestRecordSQL("gateway_request_logs") + `
			 AND COALESCE(gateway_request_logs.route_reason, '') <> 'latency_probe'), 0)`
		args = append(args, gatewayRequestAggregateVersion)
	}
	projection += ` AS request_count`
	return r.db.Model(&Channel{}).Select(projection, args...), nil
}
func (r *Channels) ListMonitorEnabled() ([]Channel, error) {
	var list []Channel
	if err := r.db.Where("monitor_enabled = ?", true).Order("sort_order DESC").Order("id ASC").Find(&list).Error; err != nil {
		return nil, err
	}
	return list, nil
}

// ListHealthyMonitorEnabled returns channels that can stay on the regular
// rate-refresh cadence. Failed channels are handled by ListDueSyncRetries so
// their recovery backoff is not bypassed by the normal scheduler.
func (r *Channels) ListHealthyMonitorEnabled() ([]Channel, error) {
	var list []Channel
	if err := r.db.Where("monitor_enabled = ? AND last_error = ?", true, "").Order("sort_order DESC").Order("id ASC").Find(&list).Error; err != nil {
		return nil, err
	}
	return list, nil
}

// ListDueSyncRetries returns failed monitor-enabled channels whose persisted
// recovery timer has elapsed. Legacy error rows without a retry timestamp are
// included once so an upgrade can heal them without manual intervention.
func (r *Channels) ListDueSyncRetries(now time.Time) ([]Channel, error) {
	var list []Channel
	if err := r.db.Where("monitor_enabled = ? AND last_error <> ? AND (next_sync_retry_at IS NULL OR next_sync_retry_at <= ?)", true, "", now).
		Order("sort_order DESC").Order("id ASC").Find(&list).Error; err != nil {
		return nil, err
	}
	return list, nil
}
func (r *Channels) UpdateBalance(id uint, balance float64, at any, lastErr string) error {
	updates := map[string]any{
		"last_balance":    balance,
		"last_balance_at": at,
	}
	if message := strings.TrimSpace(lastErr); message != "" {
		updates["balance_error"] = message
		updates["balance_error_at"] = time.Now().UTC()
	}
	return r.db.Model(&Channel{}).Where("id = ?", id).Updates(updates).Error
}

// RecordTrustedBalance projects a verified balance and clears only balance
// telemetry for the same upstream account identity. It deliberately leaves
// last_error and rate-sync recovery untouched.
func (r *Channels) RecordTrustedBalance(id uint, expectedRevision uint64, rawBalance, actualBalance float64, at time.Time) error {
	if r == nil || r.db == nil || id == 0 || expectedRevision == 0 {
		return ErrStaleChannelAccountIdentityRevision
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	result := r.db.Model(&Channel{}).
		Where("id = ? AND upstream_account_identity_revision = ?", id, expectedRevision).
		Updates(map[string]any{
			"raw_last_balance":                               rawBalance,
			"last_balance":                                   actualBalance,
			"last_balance_at":                                at.UTC(),
			"balance_error":                                  "",
			"balance_error_at":                               nil,
			"upstream_account_balance_insufficient":          false,
			"upstream_account_balance_insufficient_at":       nil,
			"upstream_account_balance_insufficient_revision": 0,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrStaleChannelAccountIdentityRevision
	}
	return nil
}

// SetBalanceError records an outcome from the balance/cost collection job
// without changing the independent rate/login sync recovery state. Every write
// is fenced to the account identity that produced the observation.
func (r *Channels) SetBalanceError(id uint, expectedRevision uint64, message string, observedAt time.Time) error {
	if r == nil || r.db == nil || id == 0 || expectedRevision == 0 {
		return ErrStaleChannelAccountIdentityRevision
	}
	message = strings.TrimSpace(message)
	updates := map[string]any{"balance_error": message}
	if message == "" {
		updates["balance_error_at"] = nil
	} else {
		if observedAt.IsZero() {
			observedAt = time.Now().UTC()
		}
		updates["balance_error_at"] = observedAt.UTC()
	}
	result := r.db.Model(&Channel{}).
		Where("id = ? AND upstream_account_identity_revision = ?", id, expectedRevision).
		Updates(updates)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrStaleChannelAccountIdentityRevision
	}
	return nil
}

// MarkUpstreamAccountBalanceInsufficient persists explicit account-balance
// rejection evidence without changing key state or the rate/login error loop.
func (r *Channels) MarkUpstreamAccountBalanceInsufficient(id uint, expectedRevision uint64, observedAt time.Time) error {
	if r == nil || r.db == nil || id == 0 || expectedRevision == 0 {
		return ErrStaleChannelAccountIdentityRevision
	}
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	}
	result := r.db.Model(&Channel{}).
		Where("id = ? AND upstream_account_identity_revision = ?", id, expectedRevision).
		Updates(map[string]any{
			"upstream_account_balance_insufficient":          true,
			"upstream_account_balance_insufficient_at":       observedAt.UTC(),
			"upstream_account_balance_insufficient_revision": expectedRevision,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrStaleChannelAccountIdentityRevision
	}
	return nil
}

// ClearUpstreamAccountBalanceInsufficient clears only the current identity's
// account-balance fact. A stale observation cannot clear a newer identity.
func (r *Channels) ClearUpstreamAccountBalanceInsufficient(id uint, expectedRevision uint64) error {
	if r == nil || r.db == nil || id == 0 || expectedRevision == 0 {
		return ErrStaleChannelAccountIdentityRevision
	}
	result := r.db.Model(&Channel{}).
		Where("id = ? AND upstream_account_identity_revision = ?", id, expectedRevision).
		Updates(map[string]any{
			"upstream_account_balance_insufficient":          false,
			"upstream_account_balance_insufficient_at":       nil,
			"upstream_account_balance_insufficient_revision": 0,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrStaleChannelAccountIdentityRevision
	}
	return nil
}

// MarkBalanceUnavailable records a balance-observation error without inventing
// a numeric value. An invalid or missing provider result is not evidence that
// the account balance is zero, so the last trusted balance snapshot remains
// intact until a new trusted observation replaces it.
func (r *Channels) MarkBalanceUnavailable(id uint, expectedRevision uint64, observedAt time.Time, message string) error {
	if r == nil || r.db == nil || id == 0 || expectedRevision == 0 {
		return ErrStaleChannelAccountIdentityRevision
	}
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	}
	message = strings.TrimSpace(message)
	if message == "" {
		message = "upstream balance observation is invalid"
	}
	result := r.db.Model(&Channel{}).
		Where("id = ? AND upstream_account_identity_revision = ?", id, expectedRevision).
		Updates(map[string]any{
			"balance_error":    message,
			"balance_error_at": observedAt.UTC(),
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrStaleChannelAccountIdentityRevision
	}
	return nil
}

func resetChannelBalanceObservation(updates map[string]any) {
	updates["last_balance"] = nil
	updates["raw_last_balance"] = nil
	updates["last_balance_at"] = nil
	updates["balance_error"] = ""
	updates["balance_error_at"] = nil
	updates["upstream_account_balance_insufficient"] = false
	updates["upstream_account_balance_insufficient_at"] = nil
	updates["upstream_account_balance_insufficient_revision"] = 0
}

// UpdateBalanceCosted projects one canonical cost-state observation onto the
// channel list row. Raw and actual values are written together so readers can
// never observe a mixed cost basis.
func (r *Channels) UpdateBalanceCosted(id uint, rawBalance, actualBalance, fundingFactor float64, factorSource string, at time.Time, lastErr string) error {
	updates := map[string]any{
		"raw_last_balance":      rawBalance,
		"last_balance":          actualBalance,
		"funding_factor":        fundingFactor,
		"funding_factor_source": strings.TrimSpace(factorSource),
		"last_balance_at":       at,
	}
	if message := strings.TrimSpace(lastErr); message != "" {
		updates["balance_error"] = message
		updates["balance_error_at"] = time.Now().UTC()
	}
	return r.db.Model(&Channel{}).Where("id = ?", id).Updates(updates).Error
}

func (r *Channels) UpdateCosts(id uint, todayCost float64, totalCost float64, sampledAt time.Time) error {
	return r.db.Model(&Channel{}).Where("id = ?", id).Updates(map[string]any{
		"today_cost":    todayCost,
		"today_cost_at": sampledAt,
		"total_cost":    totalCost,
	}).Error
}

// UpdateFinancialObservation keeps the legacy non-ledger path from exposing a
// balance collected in a cycle whose cost write later failed.
func (r *Channels) UpdateFinancialObservation(id uint, balance float64, balanceAt *time.Time, todayCost, totalCost float64, costAt time.Time) error {
	return r.updateFinancialObservation(id, 0, balance, balanceAt, todayCost, totalCost, costAt)
}

func (r *Channels) UpdateFinancialObservationForIdentity(id uint, expectedRevision uint64, balance float64, balanceAt *time.Time, todayCost, totalCost float64, costAt time.Time) error {
	if expectedRevision == 0 {
		return ErrStaleChannelAccountIdentityRevision
	}
	return r.updateFinancialObservation(id, expectedRevision, balance, balanceAt, todayCost, totalCost, costAt)
}

func (r *Channels) updateFinancialObservation(id uint, expectedRevision uint64, balance float64, balanceAt *time.Time, todayCost, totalCost float64, costAt time.Time) error {
	if r == nil || r.db == nil || id == 0 {
		return errors.New("channel financial observation is unavailable")
	}
	return r.db.Transaction(func(tx *gorm.DB) error {
		channel, err := requireOrdinaryChannelWriteTx(tx, id)
		if err != nil {
			return err
		}
		if expectedRevision != 0 && channel.UpstreamAccountIdentityRevision != expectedRevision {
			return ErrStaleChannelAccountIdentityRevision
		}
		result := tx.Model(&Channel{}).Where("id = ?", id).Updates(map[string]any{
			"last_balance": balance, "last_balance_at": balanceAt,
			"balance_error": "", "balance_error_at": nil,
			"upstream_account_balance_insufficient":          false,
			"upstream_account_balance_insufficient_at":       nil,
			"upstream_account_balance_insufficient_revision": 0,
			"today_cost": todayCost, "total_cost": totalCost, "today_cost_at": costAt,
		})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return gorm.ErrRecordNotFound
		}
		return nil
	})
}

// UpdateCostsForIdentity rejects a cost response collected for an account
// configuration that has since been replaced.
func (r *Channels) UpdateCostsForIdentity(id uint, expectedRevision uint64, todayCost float64, totalCost float64, sampledAt time.Time) error {
	if r == nil || r.db == nil || id == 0 || expectedRevision == 0 {
		return ErrStaleChannelAccountIdentityRevision
	}
	result := r.db.Model(&Channel{}).
		Where("id = ? AND upstream_account_identity_revision = ?", id, expectedRevision).
		Updates(map[string]any{
			"today_cost":    todayCost,
			"today_cost_at": sampledAt,
			"total_cost":    totalCost,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrStaleChannelAccountIdentityRevision
	}
	return nil
}

// UpdateCostsCosted writes the upstream raw totals and their canonical actual
// money projection in one statement, alongside the cost basis used.
func (r *Channels) UpdateCostsCosted(id uint, rawTodayCost, rawTotalCost, actualTodayCost, actualTotalCost, fundingFactor float64, factorSource string, sampledAt time.Time) error {
	return r.db.Model(&Channel{}).Where("id = ?", id).Updates(map[string]any{
		"raw_today_cost":        rawTodayCost,
		"raw_total_cost":        rawTotalCost,
		"today_cost":            actualTodayCost,
		"total_cost":            actualTotalCost,
		"funding_factor":        fundingFactor,
		"funding_factor_source": strings.TrimSpace(factorSource),
		"today_cost_at":         sampledAt,
	}).Error
}
func (r *Channels) SetLastError(id uint, msg string) error {
	if strings.TrimSpace(msg) == "" {
		return r.db.Transaction(func(tx *gorm.DB) error {
			if _, err := requireOrdinaryChannelWriteTx(tx, id); err != nil {
				return err
			}
			return tx.Model(&Channel{}).
				Where("id = ? AND next_sync_retry_at IS NULL AND COALESCE(last_error, '') <> ''", id).
				Update("last_error", "").Error
		})
	}
	return r.MarkSyncFailure(id, msg)
}

// SetLastErrorForIdentity keeps a login/session result from changing the
// visible recovery state after an administrator selected another account.
func (r *Channels) SetLastErrorForIdentity(id uint, expectedRevision uint64, msg string) error {
	if r == nil || r.db == nil || id == 0 || expectedRevision == 0 {
		return ErrStaleChannelAccountIdentityRevision
	}
	if strings.TrimSpace(msg) != "" {
		return r.updateSyncFailureForIdentity(id, expectedRevision, msg, false)
	}
	return r.db.Transaction(func(tx *gorm.DB) error {
		if err := requireChannelIdentityRevisionTx(tx, id, expectedRevision); err != nil {
			return err
		}
		return tx.Model(&Channel{}).
			Where("id = ? AND next_sync_retry_at IS NULL AND COALESCE(last_error, '') <> ''", id).
			Update("last_error", "").Error
	})
}

// MarkSyncFailure records a fresh monitoring error and makes it eligible for
// an immediate recovery pass. Repeated errors before that pass do not reset
// the timer, preventing a noisy task from defeating the backoff.
func (r *Channels) MarkSyncFailure(id uint, msg string) error {
	return r.updateSyncFailure(id, msg, false)
}

// RecordSyncRetryFailure advances the persistent retry cadence. The initial
// failed sync waits five minutes; then three failed recovery attempts use
// five-minute intervals before later attempts use thirty minutes. A successful
// sync or manual repair clears the state.
func (r *Channels) RecordSyncRetryFailure(id uint, msg string) error {
	return r.updateSyncFailure(id, msg, true)
}

func (r *Channels) RecordSyncRetryFailureForIdentity(id uint, expectedRevision uint64, msg string) error {
	return r.updateSyncFailureForIdentity(id, expectedRevision, msg, true)
}

// RecordSyncRetryFailureAfter schedules a bounded provider-discovery retry
// without advancing the general failure counter. A later successful rate sync
// still clears the complete recovery state through ClearSyncFailure.
func (r *Channels) RecordSyncRetryFailureAfter(id uint, msg string, delay time.Duration) error {
	if r == nil || r.db == nil || id == 0 {
		return errors.New("channel sync retry state is unavailable")
	}
	msg = strings.TrimSpace(msg)
	if msg == "" {
		return r.ClearSyncFailure(id)
	}
	if delay <= 0 {
		return r.RecordSyncRetryFailure(id, msg)
	}
	return r.db.Transaction(func(tx *gorm.DB) error {
		if _, err := requireOrdinaryChannelWriteTx(tx, id); err != nil {
			return err
		}
		return tx.Model(&Channel{}).Where("id = ?", id).Updates(map[string]any{
			"last_error":         msg,
			"next_sync_retry_at": time.Now().Add(delay),
		}).Error
	})
}

func (r *Channels) RecordSyncRetryFailureAfterForIdentity(id uint, expectedRevision uint64, msg string, delay time.Duration) error {
	if r == nil || r.db == nil || id == 0 || expectedRevision == 0 {
		return ErrStaleChannelAccountIdentityRevision
	}
	msg = strings.TrimSpace(msg)
	if msg == "" {
		return r.ClearSyncFailureForIdentity(id, expectedRevision)
	}
	if delay <= 0 {
		return r.RecordSyncRetryFailureForIdentity(id, expectedRevision, msg)
	}
	return r.db.Transaction(func(tx *gorm.DB) error {
		if err := requireChannelIdentityRevisionTx(tx, id, expectedRevision); err != nil {
			return err
		}
		return tx.Model(&Channel{}).Where("id = ?", id).Updates(map[string]any{
			"last_error":         msg,
			"next_sync_retry_at": time.Now().Add(delay),
		}).Error
	})
}

func (r *Channels) updateSyncFailure(id uint, msg string, retry bool) error {
	msg = strings.TrimSpace(msg)
	if msg == "" {
		return r.ClearSyncFailure(id)
	}
	return r.db.Transaction(func(tx *gorm.DB) error {
		channel, err := requireOrdinaryChannelWriteTx(tx, id)
		if err != nil {
			return err
		}

		now := time.Now()
		updates := map[string]any{"last_error": msg}
		if retry {
			if strings.TrimSpace(channel.LastError) == "" {
				updates["sync_failure_count"] = 0
				updates["next_sync_retry_at"] = now.Add(5 * time.Minute)
				return tx.Model(&Channel{}).Where("id = ?", id).Updates(updates).Error
			}
			attempts := channel.SyncFailureCount + 1
			delay := 5 * time.Minute
			if attempts >= 3 {
				delay = 30 * time.Minute
			}
			updates["sync_failure_count"] = attempts
			updates["next_sync_retry_at"] = now.Add(delay)
		} else if strings.TrimSpace(channel.LastError) == msg && channel.NextSyncRetryAt != nil {
			return nil
		} else if strings.TrimSpace(channel.LastError) == "" || channel.NextSyncRetryAt == nil {
			updates["sync_failure_count"] = 0
			updates["next_sync_retry_at"] = now
		}
		return tx.Model(&Channel{}).Where("id = ?", id).Updates(updates).Error
	})
}

func (r *Channels) updateSyncFailureForIdentity(id uint, expectedRevision uint64, msg string, retry bool) error {
	if r == nil || r.db == nil || id == 0 || expectedRevision == 0 {
		return ErrStaleChannelAccountIdentityRevision
	}
	msg = strings.TrimSpace(msg)
	if msg == "" {
		return r.ClearSyncFailureForIdentity(id, expectedRevision)
	}
	return r.db.Transaction(func(tx *gorm.DB) error {
		var channel Channel
		if _, err := requireOrdinaryChannelWriteTx(tx, id); err != nil {
			return err
		}
		if err := tx.
			Select("id", "upstream_account_identity_revision", "last_error", "sync_failure_count", "next_sync_retry_at").
			First(&channel, id).Error; err != nil {
			return err
		}
		if channel.UpstreamAccountIdentityRevision != expectedRevision {
			return ErrStaleChannelAccountIdentityRevision
		}
		now := time.Now()
		updates := map[string]any{"last_error": msg}
		if retry {
			if strings.TrimSpace(channel.LastError) == "" {
				updates["sync_failure_count"] = 0
				updates["next_sync_retry_at"] = now.Add(5 * time.Minute)
				return tx.Model(&Channel{}).Where("id = ?", id).Updates(updates).Error
			}
			attempts := channel.SyncFailureCount + 1
			delay := 5 * time.Minute
			if attempts >= 3 {
				delay = 30 * time.Minute
			}
			updates["sync_failure_count"] = attempts
			updates["next_sync_retry_at"] = now.Add(delay)
		} else if strings.TrimSpace(channel.LastError) == msg && channel.NextSyncRetryAt != nil {
			return nil
		} else if strings.TrimSpace(channel.LastError) == "" || channel.NextSyncRetryAt == nil {
			updates["sync_failure_count"] = 0
			updates["next_sync_retry_at"] = now
		}
		return tx.Model(&Channel{}).Where("id = ?", id).Updates(updates).Error
	})
}

// ClearSyncFailure removes both the visible error and its scheduled recovery
// state. Only a successful rate sync or a confirmed manual repair should use it.
func (r *Channels) ClearSyncFailure(id uint) error {
	if r == nil || r.db == nil || id == 0 {
		return errors.New("channel sync failure state is unavailable")
	}
	return r.db.Transaction(func(tx *gorm.DB) error {
		if _, err := requireOrdinaryChannelWriteTx(tx, id); err != nil {
			return err
		}
		return tx.Model(&Channel{}).
			Where("id = ? AND (last_error <> '' OR sync_failure_count <> 0 OR next_sync_retry_at IS NOT NULL)", id).
			Updates(map[string]any{
				"last_error":         "",
				"sync_failure_count": 0,
				"next_sync_retry_at": nil,
			}).Error
	})
}

func (r *Channels) ClearSyncFailureForIdentity(id uint, expectedRevision uint64) error {
	if r == nil || r.db == nil || id == 0 || expectedRevision == 0 {
		return ErrStaleChannelAccountIdentityRevision
	}
	return r.db.Transaction(func(tx *gorm.DB) error {
		if err := requireChannelIdentityRevisionTx(tx, id, expectedRevision); err != nil {
			return err
		}
		return tx.Model(&Channel{}).
			Where("id = ? AND (last_error <> '' OR sync_failure_count <> 0 OR next_sync_retry_at IS NOT NULL)", id).
			Updates(map[string]any{
				"last_error":         "",
				"sync_failure_count": 0,
				"next_sync_retry_at": nil,
			}).Error
	})
}

func requireChannelIdentityRevisionTx(tx *gorm.DB, id uint, expectedRevision uint64) error {
	channel, err := requireOrdinaryChannelWriteTx(tx, id)
	if err != nil {
		return err
	}
	if channel.UpstreamAccountIdentityRevision != expectedRevision {
		return ErrStaleChannelAccountIdentityRevision
	}
	return nil
}

func (r *Channels) SetLatencyProbeEnabled(id uint, enabled bool) error {
	return r.db.Transaction(func(tx *gorm.DB) error {
		if _, err := requireOrdinaryChannelWriteTx(tx, id); err != nil {
			return err
		}
		return tx.Model(&Channel{}).Where("id = ?", id).Update("latency_probe_enabled", enabled).Error
	})
}
