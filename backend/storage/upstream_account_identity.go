package storage

import (
	"errors"
	"net/url"
	"strconv"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrStaleUpstreamAccountObservation          = errors.New("upstream account observation belongs to an older channel identity")
	ErrConflictingUpstreamAccountObservation    = errors.New("upstream account observations conflict")
	ErrUpstreamAccountIdentityRevisionExhausted = errors.New("upstream account identity revision is exhausted")
)

// ErrUpstreamAccountIdentityUnbound distinguishes "this channel has not been
// authenticated against an upstream account yet" from a genuine lookup failure.
// It is the normal state of every newly created channel, so callers that only
// need a fence must treat it as "nothing to fence against" instead of an error.
var ErrUpstreamAccountIdentityUnbound = errors.New("channel has no upstream account identity binding")

type UpstreamAccountIdentityObservation struct {
	ChannelID                uint        `json:"-"`
	ExpectedIdentityRevision uint64      `json:"-"`
	ExpectedAdminRevision    uint64      `json:"-"`
	Provider                 ChannelType `json:"-"`
	APIOrigin                string      `json:"-"`
	AccountID                string      `json:"-"`
	Limit                    *int        `json:"-"`
	Scope                    string      `json:"-"`
	Source                   string      `json:"-"`
	ObservedAt               time.Time   `json:"-"`

	// Deprecated compatibility aliases for callers being migrated to the
	// explicit network-observation fences above.
	ChannelIdentityRevision uint64 `json:"-"`
	ExternalAccountID       string `json:"-"`
}

// UpstreamAccountIdentitySnapshot never exposes a hash through JSON. The
// fingerprint is used only by the locked rebase transaction and its audit.
type UpstreamAccountIdentitySnapshot struct {
	AccountRecordID         uint64      `json:"-"`
	ChannelIdentityRevision uint64      `json:"-"`
	Provider                ChannelType `json:"-"`
	Fingerprint             string      `json:"-"`
	FingerprintVersion      uint64      `json:"-"`
	Source                  string      `json:"-"`
	ObservedAt              time.Time   `json:"-"`
}

func (r *Channels) ObserveUpstreamAccountIdentity(observation UpstreamAccountIdentityObservation) (*UpstreamAccountIdentitySnapshot, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("channel storage is unavailable")
	}
	prepared, err := prepareUpstreamAccountIdentityObservation(observation)
	if err != nil {
		return nil, err
	}
	var snapshot *UpstreamAccountIdentitySnapshot
	err = r.db.Transaction(func(tx *gorm.DB) error {
		channel, err := requireOrdinaryChannelWriteTx(tx, prepared.ChannelID)
		if err != nil {
			return err
		}
		if channel.Type != prepared.Provider || channel.UpstreamAccountIdentityRevision != prepared.ExpectedIdentityRevision ||
			prepared.ExpectedAdminRevision != 0 && channel.AdminRevision != prepared.ExpectedAdminRevision {
			return ErrStaleUpstreamAccountObservation
		}
		configuredAPI := strings.TrimSpace(channel.APIURL)
		if configuredAPI == "" {
			configuredAPI = channel.SiteURL
		}
		configuredOrigin, err := normalizeUpstreamAccountIdentityAPIOrigin(configuredAPI)
		if err != nil || configuredOrigin != prepared.APIOrigin {
			return ErrStaleUpstreamAccountObservation
		}
		identity := GatewayUpstreamAccount{
			Provider: prepared.Provider, APIOriginHash: prepared.APIOriginHash,
			ExternalAccountIDHash: prepared.ExternalAccountIDHash, ConcurrencyScope: UpstreamConcurrencyScopeAccount,
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
		if err := mergeUpstreamAccountLimit(tx, &account, prepared); err != nil {
			return err
		}
		if err := bindChannelUpstreamAccountIdentity(tx, &channel, &account, prepared); err != nil {
			return err
		}
		snapshot, err = loadCurrentUpstreamAccountIdentity(tx, observation.ChannelID, true)
		return err
	})
	return snapshot, err
}

func (r *Channels) CurrentUpstreamAccountIdentity(channelID uint) (*UpstreamAccountIdentitySnapshot, error) {
	if r == nil || r.db == nil || channelID == 0 {
		return nil, errors.New("channel identity is required")
	}
	return loadCurrentUpstreamAccountIdentity(r.db, channelID, false)
}

// providerExposesUpstreamAccountIdentity reports whether a channel type can
// ever produce provider-authenticated account identity evidence.
//
// Only Sub2API does (see prepareUpstreamAccountIdentityObservation, and the
// provider CHECK on gateway_channel_upstream_account_binding_audits). Any
// operation that hard-requires a binding is therefore permanently unavailable
// to the other providers, so identity-fenced code must branch on this instead
// of treating a missing binding as a failure.
func providerExposesUpstreamAccountIdentity(provider ChannelType) bool {
	return ChannelType(strings.ToLower(strings.TrimSpace(string(provider)))) == ChannelTypeSub2API
}

func loadCurrentUpstreamAccountIdentity(tx *gorm.DB, channelID uint, lock bool) (*UpstreamAccountIdentitySnapshot, error) {
	query := tx.Table("gateway_channel_upstream_account_bindings AS binding").
		Select(`binding.upstream_account_id AS account_record_id,
			binding.channel_identity_revision,
			binding.provider,
			account.external_account_id_hash AS fingerprint,
			1 AS fingerprint_version,
			binding.source,
			binding.observed_at`).
		Joins("JOIN channels ON channels.id = binding.channel_id AND channels.type = binding.provider AND channels.upstream_account_identity_revision = binding.channel_identity_revision").
		Joins("JOIN gateway_upstream_accounts AS account ON account.id = binding.upstream_account_id AND account.provider = binding.provider").
		Where("binding.channel_id = ?", channelID)
	if lock {
		query = query.Clauses(clause.Locking{Strength: "UPDATE", Table: clause.Table{Name: "binding"}})
	}
	var snapshot UpstreamAccountIdentitySnapshot
	if err := query.Take(&snapshot).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrUpstreamAccountIdentityUnbound
		}
		return nil, err
	}
	if snapshot.AccountRecordID == 0 || snapshot.ChannelIdentityRevision == 0 || snapshot.FingerprintVersion != 1 ||
		!isLowerHexFingerprint(snapshot.Fingerprint) || strings.TrimSpace(snapshot.Source) == "" || snapshot.ObservedAt.IsZero() {
		return nil, errors.New("current upstream account identity evidence is incomplete")
	}
	return &snapshot, nil
}

type preparedUpstreamAccountIdentityObservation struct {
	ChannelID                uint
	ExpectedIdentityRevision uint64
	ExpectedAdminRevision    uint64
	Provider                 ChannelType
	APIOrigin                string
	APIOriginHash            string
	ExternalAccountIDHash    string
	Limit                    *int
	Source                   string
	ObservedAt               time.Time
}

func prepareUpstreamAccountIdentityObservation(observation UpstreamAccountIdentityObservation) (preparedUpstreamAccountIdentityObservation, error) {
	prepared := preparedUpstreamAccountIdentityObservation{}
	expectedIdentityRevision := observation.ExpectedIdentityRevision
	if expectedIdentityRevision == 0 {
		expectedIdentityRevision = observation.ChannelIdentityRevision
	}
	if observation.ChannelID == 0 || expectedIdentityRevision == 0 ||
		expectedIdentityRevision > maxUpstreamAccountIdentityRevision || observation.ExpectedAdminRevision == 0 ||
		observation.ExpectedAdminRevision > maxChannelAdminRevision {
		return prepared, errors.New("channel account identity revision is required")
	}
	provider := ChannelType(strings.ToLower(strings.TrimSpace(string(observation.Provider))))
	if provider != ChannelTypeSub2API {
		return prepared, errors.New("provider does not expose a supported account identity")
	}
	origin, err := normalizeUpstreamAccountIdentityAPIOrigin(observation.APIOrigin)
	if err != nil {
		return prepared, err
	}
	externalID := strings.TrimSpace(observation.AccountID)
	if externalID == "" {
		externalID = strings.TrimSpace(observation.ExternalAccountID)
	}
	source := strings.TrimSpace(observation.Source)
	if externalID == "" || source == "" || len(source) > 128 || observation.ObservedAt.IsZero() ||
		strings.TrimSpace(observation.Scope) != UpstreamConcurrencyScopeAccount {
		return prepared, errors.New("upstream account identity evidence is incomplete")
	}
	var limit *int
	if observation.Limit != nil {
		value := *observation.Limit
		if value < 0 {
			value = 0
		}
		limit = &value
	}
	return preparedUpstreamAccountIdentityObservation{
		ChannelID: observation.ChannelID, ExpectedIdentityRevision: expectedIdentityRevision,
		ExpectedAdminRevision: observation.ExpectedAdminRevision, Provider: provider, APIOrigin: origin,
		APIOriginHash:         hashUpstreamAccountIdentity(origin),
		ExternalAccountIDHash: hashUpstreamAccountIdentity(string(provider) + "\n" + origin + "\n" + externalID),
		Limit:                 limit, Source: source, ObservedAt: observation.ObservedAt.UTC().Truncate(time.Microsecond),
	}, nil
}

func normalizeUpstreamAccountIdentityAPIOrigin(raw string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed == nil {
		return "", errors.New("upstream API origin is invalid")
	}
	if port := parsed.Port(); port != "" {
		numericPort, err := strconv.Atoi(port)
		if err != nil || numericPort < 1 || numericPort > 65535 {
			return "", errors.New("upstream API origin is invalid")
		}
	}
	return normalizeUpstreamAccountAPIOrigin(raw)
}

func mergeUpstreamAccountLimit(tx *gorm.DB, account *GatewayUpstreamAccount, observation preparedUpstreamAccountIdentityObservation) error {
	if observation.Limit == nil {
		return nil
	}
	if account.ConcurrencyObservedAt != nil {
		currentObservedAt := account.ConcurrencyObservedAt.UTC().Truncate(time.Microsecond)
		if observation.ObservedAt.Before(currentObservedAt) {
			return nil
		}
		if observation.ObservedAt.Equal(currentObservedAt) {
			if account.ConcurrencyLimit == nil || *account.ConcurrencyLimit != *observation.Limit ||
				account.ConcurrencySource == nil || *account.ConcurrencySource != observation.Source {
				return ErrConflictingUpstreamAccountObservation
			}
			return nil
		}
	}
	source := observation.Source
	return tx.Model(&GatewayUpstreamAccount{}).Where("id = ?", account.ID).Updates(map[string]any{
		"concurrency_limit": *observation.Limit, "concurrency_scope": UpstreamConcurrencyScopeAccount,
		"concurrency_source": source, "concurrency_observed_at": observation.ObservedAt, "updated_at": time.Now().UTC(),
	}).Error
}

func bindChannelUpstreamAccountIdentity(tx *gorm.DB, channel *Channel, account *GatewayUpstreamAccount, observation preparedUpstreamAccountIdentityObservation) error {
	if tx == nil || channel == nil || account == nil || channel.ID != observation.ChannelID ||
		channel.UpstreamAccountIdentityRevision != observation.ExpectedIdentityRevision ||
		observation.ExpectedAdminRevision != 0 && channel.AdminRevision != observation.ExpectedAdminRevision {
		return ErrStaleUpstreamAccountObservation
	}
	var binding GatewayChannelUpstreamAccountBinding
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("channel_id = ?", observation.ChannelID).First(&binding).Error
	var previous *uint64
	var clearedCostState *ChannelCostState
	var previousIdentityRevision, previousCostUnitEpoch uint64
	if errors.Is(err, gorm.ErrRecordNotFound) {
		binding = GatewayChannelUpstreamAccountBinding{
			ChannelID: uint64(observation.ChannelID), Provider: observation.Provider,
			ChannelIdentityRevision: observation.ExpectedIdentityRevision, UpstreamAccountID: account.ID,
			Source: observation.Source, ObservedAt: observation.ObservedAt,
		}
		if err := tx.Create(&binding).Error; err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else {
		if binding.Provider != observation.Provider || binding.ChannelIdentityRevision != observation.ExpectedIdentityRevision {
			return ErrStaleUpstreamAccountObservation
		}
		if binding.ObservedAt.After(observation.ObservedAt) {
			return ErrStaleUpstreamAccountObservation
		}
		if binding.ObservedAt.Equal(observation.ObservedAt) && (binding.UpstreamAccountID != account.ID || binding.Source != observation.Source) {
			return ErrConflictingUpstreamAccountObservation
		}
		if binding.UpstreamAccountID == account.ID && binding.Source == observation.Source && binding.ObservedAt.Equal(observation.ObservedAt) {
			return nil
		}
		if binding.UpstreamAccountID == account.ID {
			updated := tx.Model(&GatewayChannelUpstreamAccountBinding{}).
				Where("channel_id = ? AND channel_identity_revision = ? AND upstream_account_id = ?",
					observation.ChannelID, observation.ExpectedIdentityRevision, account.ID).
				Updates(map[string]any{
					"source": observation.Source, "observed_at": observation.ObservedAt, "updated_at": time.Now().UTC(),
				})
			if updated.Error != nil {
				return updated.Error
			}
			if updated.RowsAffected != 1 {
				return ErrStaleUpstreamAccountObservation
			}
			return nil
		}
		value := binding.UpstreamAccountID
		previous = &value
		if binding.UpstreamAccountID != account.ID {
			if channel.UpstreamAccountIdentityRevision >= maxUpstreamAccountIdentityRevision ||
				channel.CostUnitEpoch >= maxUpstreamAccountIdentityRevision {
				return ErrUpstreamAccountIdentityRevisionExhausted
			}
			if err := tx.Where("channel_id = ?", observation.ChannelID).Delete(&GatewayChannelUpstreamAccountBinding{}).Error; err != nil {
				return err
			}
			previousIdentityRevision = channel.UpstreamAccountIdentityRevision
			previousCostUnitEpoch = channel.CostUnitEpoch
			nextRevision := channel.UpstreamAccountIdentityRevision + 1
			nextEpoch := channel.CostUnitEpoch + 1
			var previousCostState ChannelCostState
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
				Where("channel_id = ?", channel.ID).First(&previousCostState).Error; err != nil {
				return err
			}
			clearedCostState = &previousCostState
			if err := resetChannelCostStateForIdentityChangeTx(tx, channel.ID, observation.ObservedAt); err != nil {
				return err
			}
			if err := resetChannelModelFactsForIdentityChangeTx(tx, channel.ID, observation.ObservedAt); err != nil {
				return err
			}
			if err := invalidateManagedKeysForChannelIdentityChangeTx(tx, channel.ID, observation.ObservedAt); err != nil {
				return err
			}
			if err := resetChannelAccountScopedMonitorStateTx(tx, channel.ID); err != nil {
				return err
			}
			if err := tx.Model(&ChannelCostState{}).Where("channel_id = ?", channel.ID).Updates(map[string]any{
				"observed_provider_factor":            nil,
				"observed_provider_factor_source":     "",
				"observed_provider_factor_sampled_at": nil,
				"cost_unit_epoch":                     nextEpoch,
			}).Error; err != nil {
				return err
			}
			updated := tx.Model(&Channel{}).
				Where("id = ? AND upstream_account_identity_revision = ? AND admin_revision = ? AND cost_unit_epoch = ?",
					channel.ID, channel.UpstreamAccountIdentityRevision, channel.AdminRevision, channel.CostUnitEpoch).
				Updates(map[string]any{
					"upstream_account_identity_revision": nextRevision, "cost_unit_epoch": nextEpoch,
					"rate_snapshot_observed_at": nil, "model_price_observed_at": nil,
					"raw_last_balance": nil, "last_balance": nil, "last_balance_at": nil,
					"funding_factor": 1, "funding_factor_source": "default",
					"raw_today_cost": nil, "today_cost": nil, "raw_total_cost": nil,
					"total_cost": nil, "today_cost_at": nil,
				})
			if updated.Error != nil {
				return updated.Error
			}
			if updated.RowsAffected != 1 {
				return ErrStaleChannelAccountIdentityRevision
			}
			observation.ExpectedIdentityRevision = nextRevision
			channel.UpstreamAccountIdentityRevision = nextRevision
			channel.CostUnitEpoch = nextEpoch
			binding = GatewayChannelUpstreamAccountBinding{
				ChannelID: uint64(observation.ChannelID), Provider: observation.Provider,
				ChannelIdentityRevision: nextRevision, UpstreamAccountID: account.ID,
				Source: observation.Source, ObservedAt: observation.ObservedAt,
			}
			if err := tx.Create(&binding).Error; err != nil {
				return err
			}
		} else if err := tx.Model(&GatewayChannelUpstreamAccountBinding{}).Where("channel_id = ?", observation.ChannelID).Updates(map[string]any{
			"source": observation.Source, "observed_at": observation.ObservedAt, "updated_at": time.Now().UTC(),
		}).Error; err != nil {
			return err
		}
	}
	audit := &GatewayChannelUpstreamAccountBindingAudit{
		ChannelID: uint64(observation.ChannelID), Provider: observation.Provider,
		ChannelIdentityRevision: observation.ExpectedIdentityRevision, PreviousUpstreamAccountID: previous,
		UpstreamAccountID: account.ID, Source: observation.Source, ObservedAt: observation.ObservedAt,
	}
	recordClearedChannelCostStateEvidence(audit, clearedCostState, previousIdentityRevision, previousCostUnitEpoch)
	return tx.Create(audit).Error
}

// recordClearedChannelCostStateEvidence freezes the monetary anchors destroyed
// by an account switch into the same append-only row that records the switch.
// Without it the clear leaves no evidence of what was cleared, only that the
// binding moved.
func recordClearedChannelCostStateEvidence(
	audit *GatewayChannelUpstreamAccountBindingAudit,
	cleared *ChannelCostState,
	previousIdentityRevision, previousCostUnitEpoch uint64,
) {
	if audit == nil || cleared == nil || previousIdentityRevision == 0 || previousCostUnitEpoch == 0 {
		return
	}
	nextEpoch := previousCostUnitEpoch + 1
	nextVersion := cleared.Version + 1
	source := strings.TrimSpace(cleared.Source)
	if source == "" {
		source = "default"
	}
	audit.CostStateCleared = true
	audit.PreviousChannelIdentityRevision = &previousIdentityRevision
	audit.CostUnitEpochBefore = &previousCostUnitEpoch
	audit.CostUnitEpochAfter = &nextEpoch
	audit.CostStateVersionBefore = &cleared.Version
	audit.CostStateVersionAfter = &nextVersion
	audit.StateSourceBefore = source
	audit.FundingFactorBefore = &cleared.FundingFactor
	audit.RawBalanceBefore = &cleared.RawBalance
	audit.ActualInventoryBefore = &cleared.ActualInventory
	audit.RawTodayCostBefore = &cleared.RawTodayCost
	audit.ActualTodayCostBefore = &cleared.ActualTodayCost
	audit.RawTotalCostBefore = &cleared.RawTotalCost
	audit.ActualTotalCostBefore = &cleared.ActualTotalCost
	audit.BalanceSampledAtBefore = cleared.BalanceSampledAt
	audit.CostSampledAtBefore = cleared.CostSampledAt
}
