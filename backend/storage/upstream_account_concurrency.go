package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"net/url"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const UpstreamConcurrencyScopeAccount = "account"

var ErrStaleUpstreamAccountConcurrencyObservation = errors.New("upstream account concurrency observation belongs to an older channel identity revision")

// GatewayUpstreamAccount stores only hashed identity components. Raw upstream
// account IDs and API origins must not leave connector/channel memory or be
// exposed through API responses and logs.
type GatewayUpstreamAccount struct {
	ID                    uint64      `gorm:"primaryKey" json:"-"`
	Provider              ChannelType `gorm:"size:32;not null" json:"-"`
	APIOriginHash         string      `gorm:"size:64;not null" json:"-"`
	ExternalAccountIDHash string      `gorm:"size:64;not null" json:"-"`
	ConcurrencyLimit      *int        `json:"-"`
	ConcurrencyScope      string      `gorm:"size:32;not null" json:"-"`
	ConcurrencySource     *string     `gorm:"size:128" json:"-"`
	ConcurrencyObservedAt *time.Time  `json:"-"`
	CreatedAt             time.Time   `json:"-"`
	UpdatedAt             time.Time   `json:"-"`
}

func (GatewayUpstreamAccount) TableName() string { return "gateway_upstream_accounts" }

type GatewayChannelUpstreamAccountBinding struct {
	ChannelID               uint64      `gorm:"primaryKey" json:"-"`
	Provider                ChannelType `gorm:"size:32;not null" json:"-"`
	ChannelIdentityRevision uint64      `gorm:"not null" json:"-"`
	UpstreamAccountID       uint64      `gorm:"not null" json:"-"`
	Source                  string      `gorm:"size:128;not null" json:"-"`
	ObservedAt              time.Time   `gorm:"not null" json:"-"`
	CreatedAt               time.Time   `json:"-"`
	UpdatedAt               time.Time   `json:"-"`
}

func (GatewayChannelUpstreamAccountBinding) TableName() string {
	return "gateway_channel_upstream_account_bindings"
}

type GatewayChannelUpstreamAccountBindingAudit struct {
	ID                        uint64      `gorm:"primaryKey" json:"-"`
	ChannelID                 uint64      `gorm:"not null" json:"-"`
	Provider                  ChannelType `gorm:"size:32;not null" json:"-"`
	ChannelIdentityRevision   uint64      `gorm:"not null" json:"-"`
	PreviousUpstreamAccountID *uint64     `json:"-"`
	UpstreamAccountID         uint64      `gorm:"not null" json:"-"`
	Source                    string      `gorm:"size:128;not null" json:"-"`
	ObservedAt                time.Time   `gorm:"not null" json:"-"`
	CreatedAt                 time.Time   `json:"-"`

	CostStateCleared                bool       `gorm:"not null;default:false" json:"-"`
	PreviousChannelIdentityRevision *uint64    `json:"-"`
	CostUnitEpochBefore             *uint64    `json:"-"`
	CostUnitEpochAfter              *uint64    `json:"-"`
	CostStateVersionBefore          *uint64    `json:"-"`
	CostStateVersionAfter           *uint64    `json:"-"`
	StateSourceBefore               string     `gorm:"size:32;not null;default:''" json:"-"`
	FundingFactorBefore             *float64   `json:"-"`
	RawBalanceBefore                *float64   `json:"-"`
	ActualInventoryBefore           *float64   `json:"-"`
	RawTodayCostBefore              *float64   `json:"-"`
	ActualTodayCostBefore           *float64   `json:"-"`
	RawTotalCostBefore              *float64   `json:"-"`
	ActualTotalCostBefore           *float64   `json:"-"`
	BalanceSampledAtBefore          *time.Time `json:"-"`
	CostSampledAtBefore             *time.Time `json:"-"`
}

func (GatewayChannelUpstreamAccountBindingAudit) TableName() string {
	return "gateway_channel_upstream_account_binding_audits"
}

// UpstreamAccountConcurrencyObservation contains a provider-authenticated
// account identity and optional limit. Limit nil binds the channel to an
// account whose limit is unknown; 0 means explicitly unlimited and values
// above zero are bounded.
type UpstreamAccountConcurrencyObservation struct {
	ChannelID               uint
	ChannelIdentityRevision uint64
	Provider                ChannelType
	APIOrigin               string
	ExternalAccountID       string
	Limit                   *int
	Scope                   string
	Source                  string
	ObservedAt              time.Time
}

// UpstreamAccountConcurrencySnapshot deliberately omits both the raw and
// hashed account identity. AccountRecordID is internal-only and can serve as
// the stable key for the instance-local tracker.
type UpstreamAccountConcurrencySnapshot struct {
	AccountRecordID         uint64     `json:"-"`
	ChannelIdentityRevision uint64     `json:"-"`
	Limit                   *int       `json:"concurrency_limit,omitempty"`
	Scope                   string     `json:"concurrency_scope"`
	Source                  string     `json:"concurrency_source,omitempty"`
	ObservedAt              *time.Time `json:"concurrency_observed_at,omitempty"`
}

func (s UpstreamAccountConcurrencySnapshot) State() string {
	if s.Limit == nil {
		return "unknown"
	}
	if *s.Limit == 0 {
		return "unlimited"
	}
	return "bounded"
}

// ObserveUpstreamAccountConcurrency merges an explicit fact into the
// provider+origin+account domain and binds the channel to that account. The
// channel row, account row, and binding row are locked in that order. Older
// observations can refresh neither the limit nor a changed channel binding.
func (r *Channels) ObserveUpstreamAccountConcurrency(observation UpstreamAccountConcurrencyObservation) (*UpstreamAccountConcurrencySnapshot, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("channel storage is unavailable")
	}
	prepared, err := prepareUpstreamAccountConcurrencyObservation(observation)
	if err != nil {
		return nil, err
	}

	var snapshot *UpstreamAccountConcurrencySnapshot
	err = r.db.Transaction(func(tx *gorm.DB) error {
		var channel Channel
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Select("id", "type", "upstream_account_identity_revision").
			First(&channel, prepared.ChannelID).Error; err != nil {
			return err
		}
		if channel.Type != prepared.Provider {
			return errors.New("upstream account provider does not match channel provider")
		}
		if channel.UpstreamAccountIdentityRevision != prepared.ChannelIdentityRevision {
			return ErrStaleUpstreamAccountConcurrencyObservation
		}

		identity := GatewayUpstreamAccount{
			Provider:              prepared.Provider,
			APIOriginHash:         prepared.APIOriginHash,
			ExternalAccountIDHash: prepared.ExternalAccountIDHash,
			ConcurrencyScope:      UpstreamConcurrencyScopeAccount,
		}
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "provider"}, {Name: "api_origin_hash"}, {Name: "external_account_id_hash"}},
			DoNothing: true,
		}).Create(&identity).Error; err != nil {
			return err
		}

		var account GatewayUpstreamAccount
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("provider = ? AND api_origin_hash = ? AND external_account_id_hash = ?", prepared.Provider, prepared.APIOriginHash, prepared.ExternalAccountIDHash).
			First(&account).Error; err != nil {
			return err
		}
		if err := mergeUpstreamAccountConcurrencyFact(tx, &account, prepared); err != nil {
			return err
		}
		if err := bindChannelUpstreamAccount(tx, &account, prepared); err != nil {
			return err
		}
		snapshot = upstreamAccountConcurrencySnapshot(account, prepared.ChannelIdentityRevision)
		return nil
	})
	return snapshot, err
}

// UpstreamAccountConcurrencyForChannel returns only public-safe capacity
// metadata plus an internal record key. It never returns identity hashes.
func (r *Channels) UpstreamAccountConcurrencyForChannel(channelID uint) (*UpstreamAccountConcurrencySnapshot, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("channel storage is unavailable")
	}
	if channelID == 0 {
		return nil, errors.New("channel id is required")
	}
	snapshots, err := r.UpstreamAccountConcurrencyForChannels([]uint{channelID})
	if err != nil {
		return nil, err
	}
	snapshot, ok := snapshots[channelID]
	if !ok {
		return nil, nil
	}
	return cloneUpstreamAccountConcurrencySnapshot(snapshot), nil
}

// UpstreamAccountConcurrencyForChannels loads current account-capacity facts
// for an entire candidate set in one query. A channel without a binding, or
// whose binding belongs to an older identity revision, is deliberately absent
// from the result: callers must treat that state as unknown rather than as a
// synthetic zero-slot limit.
func (r *Channels) UpstreamAccountConcurrencyForChannels(channelIDs []uint) (map[uint]UpstreamAccountConcurrencySnapshot, error) {
	out := make(map[uint]UpstreamAccountConcurrencySnapshot)
	if r == nil || r.db == nil {
		return nil, errors.New("channel storage is unavailable")
	}
	unique := make([]uint, 0, len(channelIDs))
	seen := make(map[uint]struct{}, len(channelIDs))
	for _, channelID := range channelIDs {
		if channelID == 0 {
			continue
		}
		if _, exists := seen[channelID]; exists {
			continue
		}
		seen[channelID] = struct{}{}
		unique = append(unique, channelID)
	}
	if len(unique) == 0 {
		return out, nil
	}

	type channelAccountConcurrencyRow struct {
		ChannelID               uint64     `gorm:"column:channel_id"`
		ChannelIdentityRevision uint64     `gorm:"column:channel_identity_revision"`
		AccountRecordID         uint64     `gorm:"column:account_record_id"`
		ConcurrencyLimit        *int       `gorm:"column:concurrency_limit"`
		ConcurrencyScope        string     `gorm:"column:concurrency_scope"`
		ConcurrencySource       *string    `gorm:"column:concurrency_source"`
		ConcurrencyObservedAt   *time.Time `gorm:"column:concurrency_observed_at"`
	}
	var rows []channelAccountConcurrencyRow
	err := r.db.Table("gateway_current_upstream_account_concurrency").
		Select(`channel_id,
			channel_identity_revision,
			account_record_id,
			concurrency_limit,
			concurrency_scope,
			concurrency_source,
			concurrency_observed_at`).
		Where("channel_id IN ?", unique).
		Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		if row.ChannelID == 0 || row.ChannelIdentityRevision == 0 || row.AccountRecordID == 0 {
			continue
		}
		snapshot := UpstreamAccountConcurrencySnapshot{
			AccountRecordID:         row.AccountRecordID,
			ChannelIdentityRevision: row.ChannelIdentityRevision,
			Scope:                   row.ConcurrencyScope,
		}
		if row.ConcurrencyLimit != nil {
			limit := *row.ConcurrencyLimit
			snapshot.Limit = &limit
		}
		if row.ConcurrencySource != nil {
			snapshot.Source = *row.ConcurrencySource
		}
		if row.ConcurrencyObservedAt != nil {
			observedAt := row.ConcurrencyObservedAt.UTC()
			snapshot.ObservedAt = &observedAt
		}
		out[uint(row.ChannelID)] = snapshot
	}
	return out, nil
}

func cloneUpstreamAccountConcurrencySnapshot(snapshot UpstreamAccountConcurrencySnapshot) *UpstreamAccountConcurrencySnapshot {
	copy := snapshot
	if snapshot.Limit != nil {
		limit := *snapshot.Limit
		copy.Limit = &limit
	}
	if snapshot.ObservedAt != nil {
		observedAt := snapshot.ObservedAt.UTC()
		copy.ObservedAt = &observedAt
	}
	return &copy
}

type preparedUpstreamAccountConcurrencyObservation struct {
	ChannelID               uint
	ChannelIdentityRevision uint64
	Provider                ChannelType
	APIOriginHash           string
	ExternalAccountIDHash   string
	Limit                   *int
	Scope                   string
	Source                  string
	ObservedAt              time.Time
}

func prepareUpstreamAccountConcurrencyObservation(observation UpstreamAccountConcurrencyObservation) (preparedUpstreamAccountConcurrencyObservation, error) {
	prepared := preparedUpstreamAccountConcurrencyObservation{}
	if observation.ChannelID == 0 {
		return prepared, errors.New("channel id is required")
	}
	if observation.ChannelIdentityRevision == 0 || observation.ChannelIdentityRevision > maxUpstreamAccountIdentityRevision {
		return prepared, errors.New("channel account identity revision is invalid")
	}
	provider := ChannelType(strings.ToLower(strings.TrimSpace(string(observation.Provider))))
	if provider != ChannelTypeSub2API {
		return prepared, errors.New("provider does not expose a supported account concurrency limit")
	}
	origin, err := normalizeUpstreamAccountAPIOrigin(observation.APIOrigin)
	if err != nil {
		return prepared, err
	}
	externalAccountID := strings.TrimSpace(observation.ExternalAccountID)
	if externalAccountID == "" {
		return prepared, errors.New("explicit upstream account id is required")
	}
	scope := strings.TrimSpace(observation.Scope)
	if scope != UpstreamConcurrencyScopeAccount {
		return prepared, errors.New("upstream concurrency scope must be account")
	}
	source := strings.TrimSpace(observation.Source)
	if source == "" || len(source) > 128 {
		return prepared, errors.New("upstream concurrency source is invalid")
	}
	if observation.ObservedAt.IsZero() {
		return prepared, errors.New("upstream concurrency observation time is required")
	}
	var limit *int
	if observation.Limit != nil {
		value := *observation.Limit
		if value <= 0 {
			value = 0
		}
		limit = &value
	}
	prepared = preparedUpstreamAccountConcurrencyObservation{
		ChannelID:               observation.ChannelID,
		ChannelIdentityRevision: observation.ChannelIdentityRevision,
		Provider:                provider,
		APIOriginHash:           hashUpstreamAccountIdentity(origin),
		ExternalAccountIDHash:   hashUpstreamAccountIdentity(string(provider) + "\n" + origin + "\n" + externalAccountID),
		Limit:                   limit,
		Scope:                   scope,
		Source:                  source,
		ObservedAt:              observation.ObservedAt.UTC().Truncate(time.Microsecond),
	}
	return prepared, nil
}

func normalizeUpstreamAccountAPIOrigin(raw string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed == nil || parsed.Opaque != "" {
		return "", errors.New("upstream API origin is invalid")
	}
	scheme := strings.ToLower(strings.TrimSpace(parsed.Scheme))
	if scheme != "http" && scheme != "https" {
		return "", errors.New("upstream API origin must use http or https")
	}
	hostname := strings.TrimSuffix(strings.ToLower(strings.TrimSpace(parsed.Hostname())), ".")
	if hostname == "" {
		return "", errors.New("upstream API origin host is required")
	}
	port := parsed.Port()
	if (scheme == "https" && port == "443") || (scheme == "http" && port == "80") {
		port = ""
	}
	host := hostname
	if port != "" {
		host = net.JoinHostPort(hostname, port)
	} else if strings.Contains(hostname, ":") {
		host = "[" + hostname + "]"
	}
	return (&url.URL{Scheme: scheme, Host: host}).String(), nil
}

func hashUpstreamAccountIdentity(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func mergeUpstreamAccountConcurrencyFact(tx *gorm.DB, account *GatewayUpstreamAccount, observation preparedUpstreamAccountConcurrencyObservation) error {
	if account == nil || account.ID == 0 {
		return errors.New("upstream account record is required")
	}
	// A missing limit is still useful identity evidence for the channel
	// binding, but it cannot erase a previously authenticated limit for the
	// same account record.
	if observation.Limit == nil {
		return nil
	}
	if account.ConcurrencyObservedAt != nil {
		currentObservedAt := account.ConcurrencyObservedAt.UTC()
		switch {
		case currentObservedAt.After(observation.ObservedAt):
			return nil
		case currentObservedAt.Equal(observation.ObservedAt):
			if account.ConcurrencyLimit == nil || *account.ConcurrencyLimit != *observation.Limit ||
				account.ConcurrencySource == nil || *account.ConcurrencySource != observation.Source {
				return errors.New("conflicting upstream concurrency facts have the same observation time")
			}
			return nil
		}
	}

	limit := *observation.Limit
	source := observation.Source
	observedAt := observation.ObservedAt
	now := time.Now().UTC()
	if err := tx.Model(&GatewayUpstreamAccount{}).Where("id = ?", account.ID).Updates(map[string]any{
		"concurrency_limit":       limit,
		"concurrency_scope":       UpstreamConcurrencyScopeAccount,
		"concurrency_source":      source,
		"concurrency_observed_at": observedAt,
		"updated_at":              now,
	}).Error; err != nil {
		return err
	}
	account.ConcurrencyLimit = &limit
	account.ConcurrencyScope = UpstreamConcurrencyScopeAccount
	account.ConcurrencySource = &source
	account.ConcurrencyObservedAt = &observedAt
	account.UpdatedAt = now
	return nil
}

func bindChannelUpstreamAccount(tx *gorm.DB, account *GatewayUpstreamAccount, observation preparedUpstreamAccountConcurrencyObservation) error {
	var binding GatewayChannelUpstreamAccountBinding
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&binding, "channel_id = ?", observation.ChannelID).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		binding = GatewayChannelUpstreamAccountBinding{
			ChannelID:               uint64(observation.ChannelID),
			Provider:                observation.Provider,
			ChannelIdentityRevision: observation.ChannelIdentityRevision,
			UpstreamAccountID:       account.ID,
			Source:                  observation.Source,
			ObservedAt:              observation.ObservedAt,
		}
		if err := tx.Create(&binding).Error; err != nil {
			return err
		}
		return appendChannelUpstreamAccountBindingAudit(tx, observation, nil, account.ID)
	}
	if err != nil {
		return err
	}
	if binding.Provider != observation.Provider {
		return errors.New("channel upstream account binding provider mismatch")
	}
	if binding.ChannelIdentityRevision != observation.ChannelIdentityRevision {
		previous := binding.UpstreamAccountID
		if err := updateChannelUpstreamAccountBinding(tx, account.ID, observation); err != nil {
			return err
		}
		return appendChannelUpstreamAccountBindingAudit(tx, observation, &previous, account.ID)
	}
	if binding.ObservedAt.After(observation.ObservedAt) {
		return nil
	}
	if binding.ObservedAt.Equal(observation.ObservedAt) {
		if binding.UpstreamAccountID != account.ID || binding.Source != observation.Source {
			return errors.New("conflicting upstream account bindings have the same observation time")
		}
		return nil
	}
	if binding.UpstreamAccountID == account.ID {
		if binding.ObservedAt.Before(observation.ObservedAt) {
			return tx.Model(&GatewayChannelUpstreamAccountBinding{}).
				Where("channel_id = ?", observation.ChannelID).
				Updates(map[string]any{
					"source":      observation.Source,
					"observed_at": observation.ObservedAt,
					"updated_at":  time.Now().UTC(),
				}).Error
		}
		return nil
	}

	previous := binding.UpstreamAccountID
	if err := updateChannelUpstreamAccountBinding(tx, account.ID, observation); err != nil {
		return err
	}
	return appendChannelUpstreamAccountBindingAudit(tx, observation, &previous, account.ID)
}

func updateChannelUpstreamAccountBinding(tx *gorm.DB, accountID uint64, observation preparedUpstreamAccountConcurrencyObservation) error {
	return tx.Model(&GatewayChannelUpstreamAccountBinding{}).
		Where("channel_id = ?", observation.ChannelID).
		Updates(map[string]any{
			"provider":                  observation.Provider,
			"channel_identity_revision": observation.ChannelIdentityRevision,
			"upstream_account_id":       accountID,
			"source":                    observation.Source,
			"observed_at":               observation.ObservedAt,
			"updated_at":                time.Now().UTC(),
		}).Error
}

func appendChannelUpstreamAccountBindingAudit(tx *gorm.DB, observation preparedUpstreamAccountConcurrencyObservation, previous *uint64, current uint64) error {
	return tx.Create(&GatewayChannelUpstreamAccountBindingAudit{
		ChannelID:                 uint64(observation.ChannelID),
		Provider:                  observation.Provider,
		ChannelIdentityRevision:   observation.ChannelIdentityRevision,
		PreviousUpstreamAccountID: previous,
		UpstreamAccountID:         current,
		Source:                    observation.Source,
		ObservedAt:                observation.ObservedAt,
	}).Error
}

func upstreamAccountConcurrencySnapshot(account GatewayUpstreamAccount, channelIdentityRevision uint64) *UpstreamAccountConcurrencySnapshot {
	snapshot := &UpstreamAccountConcurrencySnapshot{
		AccountRecordID:         account.ID,
		ChannelIdentityRevision: channelIdentityRevision,
		Scope:                   account.ConcurrencyScope,
	}
	if account.ConcurrencyLimit != nil {
		limit := *account.ConcurrencyLimit
		snapshot.Limit = &limit
	}
	if account.ConcurrencySource != nil {
		snapshot.Source = *account.ConcurrencySource
	}
	if account.ConcurrencyObservedAt != nil {
		observedAt := account.ConcurrencyObservedAt.UTC()
		snapshot.ObservedAt = &observedAt
	}
	return snapshot
}
