package storage

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/usertime"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Rates struct {
	db       *gorm.DB
	userTime *usertime.Service
}

// WithContext returns a repository view whose SQL observes ctx cancellation.
func (r *Rates) WithContext(ctx context.Context) *Rates {
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

var ErrRateSnapshotCostUnitReview = errors.New("channel cost unit requires review")
var ErrStaleRateSnapshot = errors.New("rate snapshot is older than the committed observation")

// RateCostBasisSnapshot is one rate row and its channel monetary basis read
// by the same SQL statement. Keeping the pair in one PostgreSQL snapshot
// prevents a concurrent cost-unit rebase from exposing old rates with a new
// funding factor (or the reverse) to a gateway request.
type RateCostBasisSnapshot struct {
	ID                  uint
	ChannelID           uint
	ModelName           string
	Description         string
	Ratio               float64
	CompletionRatio     float64
	FirstSeenAt         time.Time
	LastSeenAt          time.Time
	CostUnitEpoch       uint64
	FundingFactor       float64
	CostBasisEpoch      uint64
	CostBasisVersion    uint64
	CostBasisSource     string
	CostBasisStatus     string
	CostBasisStateFound bool
}

// RateSnapshotBatchInput is one complete upstream rate observation. The
// expected epoch is captured before the remote request and checked while the
// channel row is locked, so a cost-unit rebase can never relabel an older
// response as belonging to the new monetary unit.
type RateSnapshotBatchInput struct {
	ChannelID                uint
	ExpectedIdentityRevision uint64
	ExpectedCostUnitEpoch    uint64
	ObservedAt               time.Time
	Items                    []RateSnapshot
	// AllowCostUnitReview is reserved for the authoritative upstream-facts
	// collector. It may refresh group membership while financial evidence is
	// under review; routing and accounting still remain gated elsewhere.
	AllowCostUnitReview bool
}

// RateSnapshotBatchResult contains only committed transitions. Callers may
// safely use it for post-commit notifications and routing cleanup.
type RateSnapshotBatchResult struct {
	CostUnitEpoch    uint64
	CostBasisVersion uint64
	FundingFactor    float64
	IsFirstSync      bool
	Added            []RateSnapshot
	Changed          []RateChangeLog
	Removed          []RateSnapshot
}

func (s RateCostBasisSnapshot) RateSnapshot() RateSnapshot {
	return RateSnapshot{
		ID: s.ID, ChannelID: s.ChannelID, ModelName: s.ModelName,
		Description: s.Description, Ratio: s.Ratio, CompletionRatio: s.CompletionRatio,
		FirstSeenAt: s.FirstSeenAt, LastSeenAt: s.LastSeenAt, CostUnitEpoch: s.CostUnitEpoch,
	}
}

func NewRates(db *gorm.DB, userTimes ...*usertime.Service) *Rates {
	userTime := usertime.Default()
	if len(userTimes) > 0 && userTimes[0] != nil {
		userTime = userTimes[0]
	}
	return &Rates{db: db, userTime: userTime}
}

const unchangedRateSnapshotHeartbeat = 10 * time.Minute

var trendNow = time.Now
var trendLocation = usertime.Default().Location()

// ListByChannel 返回渠道当前所有倍率快照。
func (r *Rates) ListByChannel(channelID uint) ([]RateSnapshot, error) {
	var list []RateSnapshot
	if err := r.db.Table("rate_snapshots AS rs").Select("rs.*").
		Joins("JOIN channels ON channels.id = rs.channel_id AND channels.cost_unit_epoch = rs.cost_unit_epoch").
		Where("rs.channel_id = ?", channelID).Order("rs.model_name ASC").Scan(&list).Error; err != nil {
		return nil, err
	}
	return list, nil
}

func (r *Rates) FindByChannelAndModel(channelID uint, modelName string) (*RateSnapshot, error) {
	modelName = strings.TrimSpace(modelName)
	if modelName == "" {
		return nil, gorm.ErrRecordNotFound
	}
	var snapshot RateSnapshot
	if err := r.db.Table("rate_snapshots AS rs").Select("rs.*").
		Joins("JOIN channels ON channels.id = rs.channel_id AND channels.cost_unit_epoch = rs.cost_unit_epoch").
		Where("rs.channel_id = ? AND lower(btrim(rs.model_name)) = lower(btrim(?))", channelID, modelName).
		Order("rs.last_seen_at DESC, rs.id DESC").Take(&snapshot).Error; err != nil {
		return nil, err
	}
	return &snapshot, nil
}

func (r *Rates) ListModels() ([]RateSnapshot, error) {
	var list []RateSnapshot
	if err := r.db.Table("rate_snapshots AS rs").Select("rs.*").
		Joins("JOIN channels ON channels.id = rs.channel_id AND channels.cost_unit_epoch = rs.cost_unit_epoch").
		Order("rs.model_name ASC").Order("rs.ratio ASC").Scan(&list).Error; err != nil {
		return nil, err
	}
	return list, nil
}

func (r *Rates) ListModelsWithCostBasis() ([]RateCostBasisSnapshot, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("rate store is unavailable")
	}
	var list []RateCostBasisSnapshot
	err := r.db.Table("rate_snapshots AS rs").
		Select(`
			rs.id,
			rs.channel_id,
			rs.model_name,
			rs.description,
			rs.ratio,
			rs.completion_ratio,
			rs.first_seen_at,
				rs.last_seen_at,
				rs.cost_unit_epoch,
				COALESCE(cost.funding_factor, channels.funding_factor, 1) AS funding_factor,
				COALESCE(cost.cost_unit_epoch, 0) AS cost_basis_epoch,
			COALESCE(cost.version, 0) AS cost_basis_version,
			COALESCE(NULLIF(btrim(cost.source), ''), channels.funding_factor_source, '') AS cost_basis_source,
			COALESCE(cost.status, '') AS cost_basis_status,
			(cost.channel_id IS NOT NULL) AS cost_basis_state_found
		`).
		Joins("JOIN channels ON channels.id = rs.channel_id AND channels.cost_unit_epoch = rs.cost_unit_epoch").
		Joins("LEFT JOIN channel_cost_states AS cost ON cost.channel_id = rs.channel_id AND cost.cost_unit_epoch = rs.cost_unit_epoch").
		Order("rs.model_name ASC").
		Order("rs.ratio ASC").
		Scan(&list).Error
	if err != nil {
		return nil, err
	}
	return list, nil
}

// Upsert 更新或插入倍率快照，返回此前的记录（若有），调用方据此判断是否变化。
func (r *Rates) Upsert(snapshot *RateSnapshot) (*RateSnapshot, error) {
	return r.UpsertLoaded(snapshot, nil)
}

// UpsertLoaded applies a snapshot when the caller already loaded the previous
// row. The previous argument remains a caller-side optimization hint only: the
// current row is deliberately re-read after taking the cross-instance
// canonical-group fence. A process-local monitor lock cannot make a detached
// snapshot safe against another service instance restoring the same group.
func (r *Rates) UpsertLoaded(snapshot *RateSnapshot, previous *RateSnapshot) (*RateSnapshot, error) {
	return r.upsertLoadedForChannelIdentity(snapshot, previous, 0)
}

// UpsertLoadedForChannelIdentity rejects a rate response collected before an
// administrator changed the channel's upstream account configuration.
func (r *Rates) UpsertLoadedForChannelIdentity(snapshot *RateSnapshot, previous *RateSnapshot, expectedRevision uint64) (*RateSnapshot, error) {
	if expectedRevision == 0 {
		return nil, ErrStaleChannelAccountIdentityRevision
	}
	return r.upsertLoadedForChannelIdentity(snapshot, previous, expectedRevision)
}

func (r *Rates) upsertLoadedForChannelIdentity(snapshot *RateSnapshot, previous *RateSnapshot, expectedRevision uint64) (*RateSnapshot, error) {
	if snapshot == nil {
		return nil, errors.New("rate snapshot is nil")
	}
	if r == nil || r.db == nil {
		return nil, errors.New("rate snapshot store is unavailable")
	}
	if snapshot.ChannelID == 0 || strings.TrimSpace(snapshot.ModelName) == "" {
		return nil, errors.New("rate snapshot identity is invalid")
	}
	if isReservedInternalUpstreamGroupName(snapshot.ModelName) {
		return nil, ErrReservedInternalUpstreamGroupName
	}
	_ = previous

	var old *RateSnapshot
	err := r.db.Transaction(func(tx *gorm.DB) error {
		if expectedRevision != 0 {
			if err := requireChannelIdentityRevisionTx(tx, snapshot.ChannelID, expectedRevision); err != nil {
				return err
			}
		} else if err := lockUpstreamGroupParentChannelTx(tx, snapshot.ChannelID); err != nil {
			return err
		}
		canonicalGroupName, err := lockRateSnapshotPresenceDomainTx(tx, snapshot.ChannelID, snapshot.ModelName)
		if err != nil {
			return err
		}

		var current RateSnapshot
		findErr := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("channel_id = ? AND model_name = ?", snapshot.ChannelID, snapshot.ModelName).
			First(&current).Error
		if findErr != nil && !errors.Is(findErr, gorm.ErrRecordNotFound) {
			return findErr
		}

		observedAt := snapshot.LastSeenAt
		if observedAt.IsZero() {
			observedAt = time.Now()
		}
		// The RateSnapshot row and the presence generation change under the
		// same canonical advisory lock. If a removed group is restored after
		// retirement, this advances its generation before a stale destructive
		// finalizer can acquire the same fence.
		if err := ensureRateSnapshotPresenceGenerationTx(tx, snapshot.ChannelID, canonicalGroupName, snapshot.ModelName, observedAt); err != nil {
			return err
		}

		if errors.Is(findErr, gorm.ErrRecordNotFound) {
			snapshot.FirstSeenAt = snapshot.LastSeenAt
			return tx.Create(snapshot).Error
		}

		copyOld := current
		old = &copyOld
		unchanged := current.Ratio == snapshot.Ratio &&
			current.CompletionRatio == snapshot.CompletionRatio &&
			current.Description == snapshot.Description
		if unchanged && !current.LastSeenAt.IsZero() && snapshot.LastSeenAt.Before(current.LastSeenAt.Add(unchangedRateSnapshotHeartbeat)) {
			snapshot.ID = current.ID
			snapshot.FirstSeenAt = current.FirstSeenAt
			return nil
		}

		next := current
		next.Ratio = snapshot.Ratio
		next.CompletionRatio = snapshot.CompletionRatio
		next.Description = snapshot.Description
		next.LastSeenAt = snapshot.LastSeenAt
		if err := tx.Save(&next).Error; err != nil {
			return err
		}
		snapshot.ID = next.ID
		snapshot.FirstSeenAt = next.FirstSeenAt
		return nil
	})
	if err != nil {
		return nil, err
	}
	return old, nil
}

func (r *Rates) AppendChange(log *RateChangeLog) error {
	if log.ChangedAt.IsZero() {
		log.ChangedAt = time.Now()
	}
	return r.db.Create(log).Error
}

func (r *Rates) AppendChangeForChannelIdentity(log *RateChangeLog, expectedRevision uint64) error {
	if r == nil || r.db == nil || log == nil || log.ChannelID == 0 || expectedRevision == 0 {
		return ErrStaleChannelAccountIdentityRevision
	}
	if log.ChangedAt.IsZero() {
		log.ChangedAt = time.Now()
	}
	return r.db.Transaction(func(tx *gorm.DB) error {
		if err := requireChannelIdentityRevisionTx(tx, log.ChannelID, expectedRevision); err != nil {
			return err
		}
		return tx.Create(log).Error
	})
}

func (r *Rates) DeleteSnapshot(channelID uint, modelName string) error {
	return r.deleteSnapshotForChannelIdentity(channelID, modelName, 0)
}

func (r *Rates) DeleteSnapshotForChannelIdentity(channelID uint, modelName string, expectedRevision uint64) error {
	if expectedRevision == 0 {
		return ErrStaleChannelAccountIdentityRevision
	}
	return r.deleteSnapshotForChannelIdentity(channelID, modelName, expectedRevision)
}

func (r *Rates) deleteSnapshotForChannelIdentity(channelID uint, modelName string, expectedRevision uint64) error {
	rawModelName := modelName
	modelName = strings.TrimSpace(rawModelName)
	if r == nil || r.db == nil {
		return errors.New("rate snapshot store is unavailable")
	}
	if channelID == 0 || modelName == "" {
		return errors.New("rate snapshot identity is invalid")
	}
	return r.db.Transaction(func(tx *gorm.DB) error {
		if expectedRevision != 0 {
			if err := requireChannelIdentityRevisionTx(tx, channelID, expectedRevision); err != nil {
				return err
			}
		} else if err := lockUpstreamGroupParentChannelTx(tx, channelID); err != nil {
			return err
		}
		if err := lockAutomaticAssignmentMutationTx(tx); err != nil {
			return err
		}
		canonicalGroupName, err := lockRateSnapshotPresenceDomainTx(tx, channelID, modelName)
		if err != nil {
			return err
		}
		result := tx.Where("channel_id = ? AND model_name = ?", channelID, rawModelName).Delete(&RateSnapshot{})
		if result.Error != nil || result.RowsAffected == 0 {
			return result.Error
		}
		return reconcileAutomaticAssignmentPresenceLockedTx(tx, channelID, rawModelName, canonicalGroupName, time.Now())
	})
}

// lockRateSnapshotPresenceDomainTx is the single serialization domain for a
// canonical upstream group's RateSnapshot presence and presence-generation
// transitions. Destructive group retirement must acquire this helper before
// checking canonicalRateSnapshotExistsTx and keep the transaction open while
// marking the captured generation absent.
func lockRateSnapshotPresenceDomainTx(tx *gorm.DB, channelID uint, groupName string) (string, error) {
	return lockUpstreamRoutingHealthDomainAdvisoryTx(tx, channelID, groupName)
}

// lockAllRateSnapshotPresenceDomainsForChannelTx fences every canonical
// RateSnapshot identity that channel deletion can remove. Existing upstream
// group identities are included because a concurrent restore can recreate a
// rate after the initial rate-name query but before the channel row is gone.
func lockAllRateSnapshotPresenceDomainsForChannelTx(tx *gorm.DB, channelID uint) error {
	if tx == nil || channelID == 0 {
		return errors.New("rate snapshot channel identity is invalid")
	}
	var rateNames []string
	if err := tx.Model(&RateSnapshot{}).Where("channel_id = ?", channelID).Pluck("model_name", &rateNames).Error; err != nil {
		return err
	}
	var identityNames []string
	if err := tx.Model(&GatewayUpstreamGroup{}).Where("channel_id = ?", channelID).Pluck("canonical_group_name", &identityNames).Error; err != nil {
		return err
	}
	canonicalNames := make(map[string]struct{}, len(rateNames)+len(identityNames))
	for _, name := range append(rateNames, identityNames...) {
		canonical, err := canonicalUpstreamGroupName(tx, name)
		if err != nil {
			return err
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
			return err
		}
	}
	return nil
}

func ensureRateSnapshotPresenceGenerationTx(tx *gorm.DB, channelID uint, canonicalGroupName, rawGroupName string, observedAt time.Time) error {
	var identity GatewayUpstreamGroup
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Select("id", "present").
		Where("channel_id = ? AND canonical_group_name = ?", channelID, canonicalGroupName).
		First(&identity).Error
	if err == nil && identity.Present {
		return nil
	}
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}
	_, _, err = ensureUpstreamGroupIdentityTx(tx, channelID, rawGroupName, true, observedAt)
	return err
}

// canonicalRateSnapshotExistsTx checks the canonical name returned by
// lockRateSnapshotPresenceDomainTx. Callers must already hold that transaction
// fence, otherwise an absent result is only a racy observation.
func canonicalRateSnapshotExistsTx(tx *gorm.DB, channelID uint, canonicalGroupName string) (bool, error) {
	canonicalGroupName = strings.TrimSpace(canonicalGroupName)
	if tx == nil || channelID == 0 || canonicalGroupName == "" {
		return false, errors.New("rate snapshot presence identity is invalid")
	}
	var count int64
	if err := tx.Model(&RateSnapshot{}).
		Where("channel_id = ? AND lower(btrim(model_name)) = ?", channelID, canonicalGroupName).
		Count(&count).Error; err != nil {
		return false, err
	}
	return count > 0, nil
}

// ReplaceChannelSnapshotForCostUnitEpoch atomically replaces one channel's
// complete rate snapshot and appends every ordinary rate-change audit. The
// channel lock serializes this batch with cost-unit rebases across processes.
func (r *Rates) ReplaceChannelSnapshotForCostUnitEpoch(input RateSnapshotBatchInput) (*RateSnapshotBatchResult, error) {
	if r == nil || r.db == nil || input.ChannelID == 0 || input.ExpectedIdentityRevision == 0 || input.ExpectedCostUnitEpoch == 0 || input.ObservedAt.IsZero() {
		return nil, errors.New("complete rate snapshot identity is required")
	}
	prepared := make([]RateSnapshot, len(input.Items))
	seen := make(map[string]struct{}, len(input.Items))
	for index := range input.Items {
		item := input.Items[index]
		item.ModelName = strings.TrimSpace(item.ModelName)
		if isReservedInternalUpstreamGroupName(item.ModelName) {
			return nil, ErrReservedInternalUpstreamGroupName
		}
		if item.ChannelID != 0 && item.ChannelID != input.ChannelID || item.ModelName == "" ||
			item.Ratio < 0 || math.IsNaN(item.Ratio) || math.IsInf(item.Ratio, 0) ||
			item.CompletionRatio < 0 || math.IsNaN(item.CompletionRatio) || math.IsInf(item.CompletionRatio, 0) {
			return nil, errors.New("complete rate snapshot contains an invalid item")
		}
		canonicalName := strings.ToLower(item.ModelName)
		if _, duplicate := seen[canonicalName]; duplicate {
			return nil, fmt.Errorf("complete rate snapshot contains duplicate model %q", item.ModelName)
		}
		seen[canonicalName] = struct{}{}
		item.ID = 0
		item.ChannelID = input.ChannelID
		item.CostUnitEpoch = input.ExpectedCostUnitEpoch
		item.FirstSeenAt = input.ObservedAt
		item.LastSeenAt = input.ObservedAt
		prepared[index] = item
	}

	committed := &RateSnapshotBatchResult{}
	err := r.db.Transaction(func(tx *gorm.DB) error {
		channel, state, err := lockChannelAndCostStateTx(tx, input.ChannelID)
		if err != nil {
			return err
		}
		if channel.UpstreamAccountIdentityRevision != input.ExpectedIdentityRevision {
			return ErrStaleChannelAccountIdentityRevision
		}
		if state.CostUnitEpoch != input.ExpectedCostUnitEpoch || state.Version == 0 ||
			state.FundingFactor < 0 || math.IsNaN(state.FundingFactor) || math.IsInf(state.FundingFactor, 0) {
			return ErrCostUnitEpochConflict
		}
		if strings.TrimSpace(state.Status) == "needs_review" && !input.AllowCostUnitReview {
			return ErrRateSnapshotCostUnitReview
		}
		if channel.RateSnapshotObservedAt != nil && !input.ObservedAt.After(*channel.RateSnapshotObservedAt) {
			return ErrStaleRateSnapshot
		}

		var stored []RateSnapshot
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("channel_id = ?", input.ChannelID).Order("model_name ASC").Find(&stored).Error; err != nil {
			return err
		}
		current := make(map[string]RateSnapshot, len(stored))
		staleIDs := make([]uint, 0)
		var latestCommittedAt time.Time
		for _, item := range stored {
			if item.CostUnitEpoch != input.ExpectedCostUnitEpoch {
				staleIDs = append(staleIDs, item.ID)
				continue
			}
			current[item.ModelName] = item
			if item.LastSeenAt.After(latestCommittedAt) {
				latestCommittedAt = item.LastSeenAt
			}
		}
		if channel.RateSnapshotObservedAt == nil && !latestCommittedAt.IsZero() && !input.ObservedAt.After(latestCommittedAt) {
			return ErrStaleRateSnapshot
		}
		if len(staleIDs) > 0 {
			result := tx.Where("channel_id = ? AND id IN ?", input.ChannelID, staleIDs).Delete(&RateSnapshot{})
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != int64(len(staleIDs)) {
				return ErrCostUnitEpochConflict
			}
		}

		result := &RateSnapshotBatchResult{
			CostUnitEpoch: input.ExpectedCostUnitEpoch, CostBasisVersion: state.Version,
			FundingFactor: state.FundingFactor, IsFirstSync: len(current) == 0,
			Added: []RateSnapshot{}, Changed: []RateChangeLog{}, Removed: []RateSnapshot{},
		}
		for index := range prepared {
			item := prepared[index]
			previous, exists := current[item.ModelName]
			if !exists {
				if err := tx.Create(&item).Error; err != nil {
					return err
				}
				result.Added = append(result.Added, item)
				continue
			}
			delete(current, item.ModelName)
			item.ID = previous.ID
			item.FirstSeenAt = previous.FirstSeenAt
			unchanged := previous.Ratio == item.Ratio && previous.CompletionRatio == item.CompletionRatio && previous.Description == item.Description
			if !unchanged || previous.LastSeenAt.IsZero() || !input.ObservedAt.Before(previous.LastSeenAt.Add(unchangedRateSnapshotHeartbeat)) {
				updated := tx.Model(&RateSnapshot{}).
					Where("id = ? AND channel_id = ? AND cost_unit_epoch = ?", previous.ID, input.ChannelID, input.ExpectedCostUnitEpoch).
					Updates(map[string]any{
						"ratio": item.Ratio, "completion_ratio": item.CompletionRatio,
						"description": item.Description, "last_seen_at": input.ObservedAt,
					})
				if updated.Error != nil {
					return updated.Error
				}
				if updated.RowsAffected != 1 {
					return ErrCostUnitEpochConflict
				}
			}
			if previous.Ratio == item.Ratio && previous.CompletionRatio == item.CompletionRatio {
				continue
			}
			oldRatio, oldCompletion := previous.Ratio, previous.CompletionRatio
			oldEffective := oldRatio * state.FundingFactor
			newEffective := item.Ratio * state.FundingFactor
			oldEffectiveCompletion := oldCompletion * state.FundingFactor
			newEffectiveCompletion := item.CompletionRatio * state.FundingFactor
			result.Changed = append(result.Changed, RateChangeLog{
				ChannelID: input.ChannelID, ModelName: item.ModelName,
				OldRatio: &oldRatio, NewRatio: item.Ratio,
				OldCompletionRatio: &oldCompletion, NewCompletionRatio: item.CompletionRatio,
				FundingFactorBefore: state.FundingFactor, FundingFactorAfter: state.FundingFactor,
				OldEffectiveRatio: &oldEffective, NewEffectiveRatio: newEffective,
				OldEffectiveCompletionRatio: &oldEffectiveCompletion, NewEffectiveCompletionRatio: newEffectiveCompletion,
				ChangeReason: "upstream_rate", CostUnitEpochBefore: input.ExpectedCostUnitEpoch,
				CostUnitEpochAfter: input.ExpectedCostUnitEpoch, ChangedAt: input.ObservedAt,
			})
		}

		removedIDs := make([]uint, 0, len(current))
		for _, item := range current {
			removedIDs = append(removedIDs, item.ID)
			result.Removed = append(result.Removed, item)
		}
		if len(removedIDs) > 0 {
			deleted := tx.Where("channel_id = ? AND cost_unit_epoch = ? AND id IN ?", input.ChannelID, input.ExpectedCostUnitEpoch, removedIDs).
				Delete(&RateSnapshot{})
			if deleted.Error != nil {
				return deleted.Error
			}
			if deleted.RowsAffected != int64(len(removedIDs)) {
				return ErrCostUnitEpochConflict
			}
		}
		if len(result.Changed) > 0 {
			if err := tx.Create(&result.Changed).Error; err != nil {
				return fmt.Errorf("append rate snapshot batch changes: %w", err)
			}
		}
		watermark := tx.Model(&Channel{}).
			Where("id = ? AND upstream_account_identity_revision = ? AND cost_unit_epoch = ?",
				input.ChannelID, input.ExpectedIdentityRevision, input.ExpectedCostUnitEpoch).
			Update("rate_snapshot_observed_at", input.ObservedAt)
		if watermark.Error != nil {
			return watermark.Error
		}
		if watermark.RowsAffected != 1 {
			return ErrCostUnitEpochConflict
		}
		sort.Slice(result.Removed, func(i, j int) bool { return result.Removed[i].ModelName < result.Removed[j].ModelName })
		committed = result
		return nil
	})
	if err != nil {
		return nil, err
	}
	return committed, nil
}

// ListChanges 倒序拉取倍率变化日志。channelID 为 0 表示不过滤。
func (r *Rates) ListChanges(channelID uint, limit int) ([]RateChangeLog, error) {
	if limit <= 0 {
		limit = 50
	}
	q := r.db.Model(&RateChangeLog{}).Order("changed_at DESC").Limit(limit)
	if channelID != 0 {
		q = q.Where("channel_id = ?", channelID)
	}
	var list []RateChangeLog
	if err := q.Find(&list).Error; err != nil {
		return nil, err
	}
	return list, nil
}

func (r *Rates) ListChangesPage(channelID uint, page, pageSize int) ([]RateChangeLog, int64, error) {
	if page < 1 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 20
	}

	q := r.db.Model(&RateChangeLog{})
	if channelID != 0 {
		q = q.Where("channel_id = ?", channelID)
	}

	var total int64
	if err := q.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	var list []RateChangeLog
	if err := q.Order("changed_at DESC").
		Offset((page - 1) * pageSize).
		Limit(pageSize).
		Find(&list).Error; err != nil {
		return nil, 0, err
	}
	return list, total, nil
}

func (r *Rates) AppendBalance(s *BalanceSnapshot) error {
	if s.SampledAt.IsZero() {
		s.SampledAt = time.Now()
	}
	return r.db.Create(s).Error
}

func (r *Rates) AppendBalanceForChannelIdentity(s *BalanceSnapshot, expectedRevision uint64) error {
	if r == nil || r.db == nil || s == nil || s.ChannelID == 0 || expectedRevision == 0 {
		return ErrStaleChannelAccountIdentityRevision
	}
	if s.SampledAt.IsZero() {
		s.SampledAt = time.Now()
	}
	return r.db.Transaction(func(tx *gorm.DB) error {
		if err := requireChannelIdentityRevisionTx(tx, s.ChannelID, expectedRevision); err != nil {
			return err
		}
		return tx.Create(s).Error
	})
}

func (r *Rates) AppendCost(s *CostSnapshot) error {
	if s.SampledAt.IsZero() {
		s.SampledAt = time.Now()
	}
	return r.db.Create(s).Error
}

func (r *Rates) AppendCostForChannelIdentity(s *CostSnapshot, expectedRevision uint64) error {
	if r == nil || r.db == nil || s == nil || s.ChannelID == 0 || expectedRevision == 0 {
		return ErrStaleChannelAccountIdentityRevision
	}
	if s.SampledAt.IsZero() {
		s.SampledAt = time.Now()
	}
	return r.db.Transaction(func(tx *gorm.DB) error {
		if err := requireChannelIdentityRevisionTx(tx, s.ChannelID, expectedRevision); err != nil {
			return err
		}
		return tx.Create(s).Error
	})
}

// DeleteBalanceSnapshotsBefore 删除 sampled_at < cutoff 的余额快照，返回删除行数。
func (r *Rates) DeleteBalanceSnapshotsBefore(cutoff time.Time) (int64, error) {
	res := r.db.Where("sampled_at < ?", cutoff).Delete(&BalanceSnapshot{})
	return res.RowsAffected, res.Error
}

// DeleteCostSnapshotsBefore 删除 sampled_at < cutoff 的消费快照，返回删除行数。
func (r *Rates) DeleteCostSnapshotsBefore(cutoff time.Time) (int64, error) {
	res := r.db.Where("sampled_at < ?", cutoff).Delete(&CostSnapshot{})
	return res.RowsAffected, res.Error
}

// BalanceHistory 倒序拉取余额历史。
func (r *Rates) BalanceHistory(channelID uint, limit int) ([]BalanceSnapshot, error) {
	if limit <= 0 {
		limit = 100
	}
	var list []BalanceSnapshot
	if err := r.db.
		Where("channel_id = ?", channelID).
		Order("sampled_at DESC").
		Limit(limit).
		Find(&list).Error; err != nil {
		return nil, err
	}
	return list, nil
}

func (r *Rates) CostSnapshotAtOrBefore(channelID uint, at time.Time) (*CostSnapshot, error) {
	var snapshot CostSnapshot
	err := r.db.Table("cost_snapshots AS snapshot").Select("snapshot.*").
		Joins("JOIN channels ON channels.id = snapshot.channel_id AND channels.cost_unit_epoch = snapshot.cost_unit_epoch").
		Where("snapshot.channel_id = ? AND snapshot.sampled_at <= ?", channelID, at).
		Order("snapshot.sampled_at DESC").Take(&snapshot).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &snapshot, nil
}

func (r *Rates) BalanceSnapshotAtOrBefore(channelID uint, at time.Time) (*BalanceSnapshot, error) {
	var snapshot BalanceSnapshot
	err := r.db.Table("balance_snapshots AS snapshot").Select("snapshot.*").
		Joins("JOIN channels ON channels.id = snapshot.channel_id AND channels.cost_unit_epoch = snapshot.cost_unit_epoch").
		Where("snapshot.channel_id = ? AND snapshot.sampled_at <= ?", channelID, at).
		Order("snapshot.sampled_at DESC").Take(&snapshot).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &snapshot, nil
}

// DailyAggregate 一天的聚合余额（所有渠道之和）。
type DailyAggregate struct {
	Day     time.Time `json:"day"`
	Balance *float64  `json:"balance"`
}

// DailyCostAggregate 一天的聚合消费（所有渠道之和）。
type DailyCostAggregate struct {
	Day  time.Time `json:"day"`
	Cost *float64  `json:"cost"`
}

type RateChangeDailyAggregate struct {
	Day   string `json:"day"`
	Up    int64  `json:"up"`
	Down  int64  `json:"down"`
	Other int64  `json:"other"`
}

type RateChangeModelAggregate struct {
	ChannelID   uint   `json:"channel_id"`
	ChannelName string `json:"channel_name"`
	ModelName   string `json:"model_name"`
	Changes     int64  `json:"changes"`
	Up          int64  `json:"up"`
	Down        int64  `json:"down"`
}

type RateChangeAnalytics struct {
	Total  int64                      `json:"total"`
	Up     int64                      `json:"up"`
	Down   int64                      `json:"down"`
	Other  int64                      `json:"other"`
	Daily  []RateChangeDailyAggregate `json:"daily"`
	Models []RateChangeModelAggregate `json:"models"`
	Recent []RateChangeLog            `json:"recent"`
}

const rateChangeAnalyticsMaxRows = 50000

func (r *Rates) RateChangeAnalyticsForDays(channelID uint, days int, recentLimit int) (*RateChangeAnalytics, error) {
	if days <= 0 {
		days = 30
	}
	today := r.dayStart(trendNow())
	return r.RateChangeAnalytics(channelID, today.AddDate(0, 0, -(days-1)), recentLimit)
}

func (r *Rates) RateChangeAnalytics(channelID uint, since time.Time, recentLimit int) (*RateChangeAnalytics, error) {
	query := r.db.Model(&RateChangeLog{}).Where("changed_at >= ?", since)
	if channelID != 0 {
		query = query.Where("channel_id = ?", channelID)
	}
	var changes []RateChangeLog
	if err := query.Order("changed_at DESC").Limit(rateChangeAnalyticsMaxRows + 1).Find(&changes).Error; err != nil {
		return nil, err
	}
	if len(changes) > rateChangeAnalyticsMaxRows {
		return nil, fmt.Errorf("rate change analytics exceeds %d rows; narrow the time window or channel", rateChangeAnalyticsMaxRows)
	}
	if recentLimit <= 0 {
		recentLimit = 20
	}
	channelIDs := make([]uint, 0)
	seenChannelIDs := make(map[uint]struct{})
	for _, change := range changes {
		if _, ok := seenChannelIDs[change.ChannelID]; ok {
			continue
		}
		seenChannelIDs[change.ChannelID] = struct{}{}
		channelIDs = append(channelIDs, change.ChannelID)
	}
	channelNames := make(map[uint]string, len(channelIDs))
	if len(channelIDs) > 0 {
		var channels []Channel
		if err := r.db.Select("id", "name").Where("id IN ?", channelIDs).Find(&channels).Error; err != nil {
			return nil, err
		}
		for _, channel := range channels {
			channelNames[channel.ID] = channel.Name
		}
	}
	result := &RateChangeAnalytics{
		Daily:  []RateChangeDailyAggregate{},
		Models: []RateChangeModelAggregate{},
		Recent: append([]RateChangeLog(nil), changes[:min(recentLimit, len(changes))]...),
		Total:  int64(len(changes)),
	}
	daily := make(map[string]*RateChangeDailyAggregate)
	type modelKey struct {
		ChannelID uint
		ModelName string
	}
	models := make(map[modelKey]*RateChangeModelAggregate)
	for _, change := range changes {
		day := dayKey(r.dayStart(change.ChangedAt))
		if daily[day] == nil {
			daily[day] = &RateChangeDailyAggregate{Day: day}
		}
		modelName := strings.TrimSpace(change.ModelName)
		if modelName == "" {
			modelName = "未命名模型"
		}
		key := modelKey{ChannelID: change.ChannelID, ModelName: modelName}
		if models[key] == nil {
			models[key] = &RateChangeModelAggregate{
				ChannelID:   change.ChannelID,
				ChannelName: channelNames[change.ChannelID],
				ModelName:   modelName,
			}
		}
		models[key].Changes++
		switch rateChangeDirection(change) {
		case 1:
			result.Up++
			daily[day].Up++
			models[key].Up++
		case -1:
			result.Down++
			daily[day].Down++
			models[key].Down++
		default:
			result.Other++
			daily[day].Other++
		}
	}
	for _, item := range daily {
		result.Daily = append(result.Daily, *item)
	}
	sort.Slice(result.Daily, func(i, j int) bool { return result.Daily[i].Day < result.Daily[j].Day })
	for _, item := range models {
		result.Models = append(result.Models, *item)
	}
	sort.Slice(result.Models, func(i, j int) bool {
		if result.Models[i].Changes != result.Models[j].Changes {
			return result.Models[i].Changes > result.Models[j].Changes
		}
		if result.Models[i].ModelName != result.Models[j].ModelName {
			return result.Models[i].ModelName < result.Models[j].ModelName
		}
		if result.Models[i].ChannelName != result.Models[j].ChannelName {
			return result.Models[i].ChannelName < result.Models[j].ChannelName
		}
		return result.Models[i].ChannelID < result.Models[j].ChannelID
	})
	if len(result.Models) > 8 {
		result.Models = result.Models[:8]
	}
	return result, nil
}

func rateChangeDirection(change RateChangeLog) int {
	up, down := false, false
	if change.OldRatio != nil {
		up = up || change.NewRatio > *change.OldRatio
		down = down || change.NewRatio < *change.OldRatio
	}
	if change.OldCompletionRatio != nil {
		up = up || change.NewCompletionRatio > *change.OldCompletionRatio
		down = down || change.NewCompletionRatio < *change.OldCompletionRatio
	}
	if up == down {
		return 0
	}
	if up {
		return 1
	}
	return -1
}

// AggregateBalanceTrend 取最近 N 天的"日内最后一次余额"按渠道之和，作为总余额趋势。
//
// 实现：对每个 (channel_id, day) 取该天最后一次 BalanceSnapshot 的余额，再按 day 求和，
// 然后补齐窗口内缺失的日期。窗口内完全没有采样时返回空数组。
func (r *Rates) AggregateBalanceTrend(days int) ([]DailyAggregate, error) {
	if days <= 0 {
		days = 7
	}
	today := r.dayStart(trendNow())
	since := today.AddDate(0, 0, -(days - 1))

	var snapshots []BalanceSnapshot
	if err := r.db.
		Where("sampled_at >= ?", since).
		Order("sampled_at ASC").
		Find(&snapshots).Error; err != nil {
		return nil, err
	}
	if len(snapshots) == 0 {
		return []DailyAggregate{}, nil
	}

	type key struct {
		ChannelID uint
		Day       time.Time
	}

	latest := make(map[key]BalanceSnapshot, len(snapshots))
	for _, snapshot := range snapshots {
		day := r.dayStart(snapshot.SampledAt)
		latest[key{ChannelID: snapshot.ChannelID, Day: day}] = snapshot
	}

	byDay := make(map[string]float64, days)
	for _, snapshot := range latest {
		day := r.dayStart(snapshot.SampledAt)
		byDay[dayKey(day)] += snapshot.Balance
	}

	out := make([]DailyAggregate, 0, days)
	for day := since; !day.After(today); day = day.AddDate(0, 0, 1) {
		var balance *float64
		if value, ok := byDay[dayKey(day)]; ok {
			valueCopy := value
			balance = &valueCopy
		}
		out = append(out, DailyAggregate{Day: day, Balance: balance})
	}
	return out, nil
}

// AggregateCostTrend 取最近 N 天的"日内最后一次今日消费"按渠道之和，作为总消费趋势。
func (r *Rates) AggregateCostTrend(days int) ([]DailyCostAggregate, error) {
	if days <= 0 {
		days = 7
	}
	today := r.dayStart(trendNow())
	since := today.AddDate(0, 0, -(days - 1))

	var snapshots []CostSnapshot
	if err := r.db.
		Where("sampled_at >= ?", since).
		Order("sampled_at ASC").
		Find(&snapshots).Error; err != nil {
		return nil, err
	}
	if len(snapshots) == 0 {
		return []DailyCostAggregate{}, nil
	}

	type key struct {
		ChannelID uint
		Day       time.Time
	}

	latest := make(map[key]CostSnapshot, len(snapshots))
	for _, snapshot := range snapshots {
		day := r.dayStart(snapshot.SampledAt)
		latest[key{ChannelID: snapshot.ChannelID, Day: day}] = snapshot
	}

	byDay := make(map[string]float64, days)
	for _, snapshot := range latest {
		day := r.dayStart(snapshot.SampledAt)
		byDay[dayKey(day)] += snapshot.TodayCost
	}

	out := make([]DailyCostAggregate, 0, days)
	for day := since; !day.After(today); day = day.AddDate(0, 0, 1) {
		var cost *float64
		if value, ok := byDay[dayKey(day)]; ok {
			valueCopy := value
			cost = &valueCopy
		}
		out = append(out, DailyCostAggregate{Day: day, Cost: cost})
	}
	return out, nil
}

func (r *Rates) dayStart(value time.Time) time.Time {
	return r.userTime.DayStart(value)
}

func dayKey(t time.Time) string {
	return t.Format("2006-01-02")
}
