package storage

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/providerid"
	"github.com/Harvey-Mei/BridgeOps/backend/usertime"
	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Gateway struct {
	db               *gorm.DB
	userTime         *usertime.Service
	databaseLocation *time.Location
	channelLockSlots chan struct{}

	historyFacetCacheMu sync.Mutex
	historyFacetCache   map[string]gatewayUpstreamGroupChangeHistoryFacetCacheEntry

	requestDispatchOwnerMu         sync.Mutex
	requestDispatchOwnerID         string
	requestDispatchOwnerFenceToken uint64
	requestDispatchOwnerLeaseUntil time.Time
	requestDispatchOwnerRegistered bool
	requestDispatchOwnerStopped    bool
	requestDispatchOwnerInitErr    error
}

func (g *Gateway) WithContext(ctx context.Context) *Gateway {
	if g == nil || g.db == nil {
		return g
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// Keep this view limited to the SQL-backed fields. Contextual preflight
	// calls must not copy the gateway's live mutexes or in-memory caches.
	return &Gateway{db: g.db.WithContext(ctx), userTime: g.userTime, databaseLocation: g.databaseLocation, channelLockSlots: g.channelLockSlots}
}

var (
	ErrUpstreamUsageAlreadyAssigned  = errors.New("upstream usage is already assigned to another request")
	ErrRequestCostReconcileLeaseLost = errors.New("request cost reconciliation lease is no longer owned")
	ErrGatewayRequestRecordInvalid   = errors.New("gateway request record is invalid")
)
var ErrCandidateGroupInUse = errors.New("candidate group is assigned to downstream keys")
var ErrCandidateGroupMembershipChanged = errors.New("candidate group membership was updated concurrently")
var ErrKeyGroupInUse = errors.New("key group is assigned to downstream keys")
var ErrUpstreamGroupChangeSuperseded = errors.New("upstream group change is no longer current")
var ErrUpstreamGroupAssignmentKeyUnavailable = errors.New("current upstream group key is unavailable for assignment")
var ErrDownstreamKeyUpdateConflict = errors.New("downstream key was updated concurrently")
var ErrDownstreamKeyModelMappingChanged = errors.New("downstream key model mapping was updated concurrently")
var ErrGatewayRequestLogRouteUnavailable = errors.New("request log has no routed upstream group")
var ErrGatewayRequestLogUpstreamGroupUnavailable = errors.New("request log upstream group is no longer available")
var ErrGatewayRequestLogManualCooldownIneligible = errors.New("request log first-byte latency is not eligible for manual cooldown")
var ErrGatewayManualCooldownIdempotencyConflict = errors.New("manual cooldown idempotency key belongs to another actor")

const upstreamModelSyncHeartbeat = 10 * time.Minute
const upstreamModelErrorHeartbeat = 5 * time.Minute
const (
	GatewayRoutingFailureManualCooldown    = "manual_cooldown"
	GatewayAdaptiveRoutingFailureThreshold = 3
)
const GatewayManualRoutingCooldownMinimumFirstByte = 30 * time.Second
const gatewayChannelAdvisoryLockNamespace int64 = 0x424f000000000000
const gatewayDownstreamKeyAdvisoryLockNamespace int64 = 0x424f100000000000
const gatewayChannelUnlimitedPoolLockSlots = 8

var gatewayChannelLockLimiters sync.Map

func NewGateway(db *gorm.DB, userTimes ...*usertime.Service) *Gateway {
	userTime := usertime.Default()
	if len(userTimes) > 0 && userTimes[0] != nil {
		userTime = userTimes[0]
	}
	ownerID, ownerFenceToken, ownerErr := newGatewayRequestDispatchOwnerIdentity()
	return &Gateway{
		db:               db,
		userTime:         userTime,
		databaseLocation: databaseLocation(db),
		channelLockSlots: sharedGatewayChannelLockLimiter(db),

		requestDispatchOwnerID:         ownerID,
		requestDispatchOwnerFenceToken: ownerFenceToken,
		requestDispatchOwnerInitErr:    ownerErr,
	}
}

func sharedGatewayChannelLockLimiter(db *gorm.DB) chan struct{} {
	if db == nil || db.Dialector == nil || db.Dialector.Name() != string(DBDriverPostgres) {
		return nil
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil
	}
	if existing, ok := gatewayChannelLockLimiters.Load(sqlDB); ok {
		return existing.(chan struct{})
	}
	limiter := make(chan struct{}, gatewayChannelLockSlotCount(sqlDB.Stats().MaxOpenConnections))
	actual, _ := gatewayChannelLockLimiters.LoadOrStore(sqlDB, limiter)
	return actual.(chan struct{})
}

func gatewayChannelLockSlotCount(maxOpenConnections int) int {
	if maxOpenConnections <= 0 {
		return gatewayChannelUnlimitedPoolLockSlots
	}
	slots := maxOpenConnections / 2
	if slots < 1 {
		return 1
	}
	return slots
}

// LockChannelOperation serializes remote key mutation across PostgreSQL
// service instances. The caller must hold the returned lock until all remote
// and local state transitions for the channel are complete.
func (g *Gateway) LockChannelOperation(ctx context.Context, channelID uint) (func(), error) {
	if g == nil {
		return nil, errors.New("gateway channel lock is unavailable")
	}
	return lockGatewayChannelOperation(ctx, g.db, g.channelLockSlots, channelID)
}

// RequireUpstreamAccountIdentityRevision verifies that an inventory or other
// remote observation still belongs to the channel account revision that
// initiated it. Destructive finalizers repeat this check with a row lock.
func (g *Gateway) RequireUpstreamAccountIdentityRevision(channelID uint, expectedRevision uint64) error {
	if g == nil || g.db == nil || channelID == 0 || expectedRevision == 0 {
		return ErrStaleChannelAccountIdentityRevision
	}
	var channel Channel
	err := g.db.Transaction(func(tx *gorm.DB) error {
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

func lockGatewayChannelOperation(ctx context.Context, db *gorm.DB, channelLockSlots chan struct{}, channelID uint) (func(), error) {
	if db == nil || channelID == 0 {
		return nil, errors.New("gateway channel lock is unavailable")
	}
	if db.Dialector == nil || db.Dialector.Name() != string(DBDriverPostgres) {
		return func() {}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if channelLockSlots == nil {
		return nil, errors.New("gateway channel lock limiter is unavailable")
	}
	select {
	case channelLockSlots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	releaseSlot := func() { <-channelLockSlots }
	sqlDB, err := db.DB()
	if err != nil {
		releaseSlot()
		return nil, err
	}
	conn, err := sqlDB.Conn(ctx)
	if err != nil {
		releaseSlot()
		return nil, err
	}
	lockID := gatewayChannelAdvisoryLockNamespace ^ int64(uint64(channelID)&0x00ffffffffffffff)
	if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", lockID); err != nil {
		// The server may have acquired the session lock even when the client
		// failed to receive the result. Discard the connection unconditionally.
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		_ = conn.Close()
		releaseSlot()
		return nil, err
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			defer releaseSlot()
			unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var unlocked bool
			if err := conn.QueryRowContext(unlockCtx, "SELECT pg_advisory_unlock($1)", lockID).Scan(&unlocked); err != nil || !unlocked {
				// Returning ErrBadConn from Raw prevents a session that may still hold
				// the advisory lock from being returned to the connection pool.
				_ = conn.Raw(func(any) error { return driver.ErrBadConn })
			}
			_ = conn.Close()
		})
	}, nil
}

func (g *Gateway) CreateDownstreamKey(key *GatewayDownstreamKey) error {
	if err := normalizeDownstreamCandidateTarget(key); err != nil {
		return err
	}
	err := g.db.Transaction(func(tx *gorm.DB) error {
		if err := lockAutomaticAssignmentMutationTx(tx); err != nil {
			return err
		}
		if err := validateKeyGroupRoutePolicySnapshotTx(tx, key); err != nil {
			return err
		}
		if err := lockCandidateGroup(tx, key.CandidateGroupID); err != nil {
			return err
		}
		if err := lockKeyGroup(tx, key.KeyGroupID); err != nil {
			return err
		}
		if err := ensureUniqueDownstreamKeyName(tx, key); err != nil {
			return err
		}
		if err := ensureManualRouteCandidateMembership(tx, key); err != nil {
			return err
		}
		return tx.Create(key).Error
	})
	if err == nil {
		key.ValidateRoutePolicySnapshot = false
	}
	return err
}

func (g *Gateway) ListDownstreamKeys() ([]GatewayDownstreamKey, error) {
	list := []GatewayDownstreamKey{}
	if err := g.db.Order("id DESC").Find(&list).Error; err != nil {
		return nil, err
	}
	ids := make([]uint, 0, len(list))
	for _, key := range list {
		ids = append(ids, key.ID)
	}
	usage, err := g.SummarizeDownstreamKeyUsage(ids, g.userTime.Now())
	if err != nil {
		return nil, err
	}
	recentRequests, err := g.latestRealRequestsByDownstreamKey(ids)
	if err != nil {
		return nil, err
	}
	groupNames := make(map[uint]string)
	var candidateGroups []GatewayCandidateGroup
	if err := g.db.Select("id", "name").Find(&candidateGroups).Error; err != nil {
		return nil, err
	}
	for _, group := range candidateGroups {
		groupNames[group.ID] = group.Name
	}
	keyGroupNames := make(map[uint]string)
	var keyGroups []GatewayKeyGroup
	if err := g.db.Select("id", "name").Find(&keyGroups).Error; err != nil {
		return nil, err
	}
	for _, group := range keyGroups {
		keyGroupNames[group.ID] = group.Name
	}
	for i := range list {
		list[i].Usage = usage[list[i].ID]
		if recent := recentRequests[list[i].ID]; recent != nil {
			list[i].CandidatePreviewModel = recent.ModelName
			if recent.ChannelID > 0 {
				list[i].RecentRoute = recent
			}
		}
		if list[i].CandidatePreviewModel != "" {
			list[i].CandidatePreviewModelSource = "recent_request"
		}
		list[i].CandidateGroupName = groupNames[list[i].CandidateGroupID]
		list[i].KeyGroupName = keyGroupNames[list[i].KeyGroupID]
		exclusions, err := g.ListRouteExclusions(list[i].ID)
		if err != nil {
			return nil, err
		}
		list[i].RouteExclusions = exclusions
		if list[i].ManualChannelID == 0 {
			continue
		}
		var ch Channel
		if err := g.db.Select("id", "name").First(&ch, list[i].ManualChannelID).Error; err == nil {
			list[i].ManualChannelName = ch.Name
		}
		var upstream GatewayUpstreamKey
		err = g.db.
			Where("channel_id = ? AND lower(btrim(group_name)) = lower(btrim(?)) AND enabled = ? AND provisioning = ? AND btrim(api_key_cipher) <> ''", list[i].ManualChannelID, list[i].ManualGroupName, true, false).
			First(&upstream).Error
		list[i].ManualRouteInvalid = err != nil
	}
	return list, nil
}

// LatestRealRequestModelsByDownstreamKey returns one recent, completed model
// per downstream key. Internal probes/tests are excluded so an administrator
// candidate preview defaults to the model a client actually used.
func (g *Gateway) LatestRealRequestModelsByDownstreamKey(ids []uint) (map[uint]string, error) {
	result := make(map[uint]string, len(ids))
	recent, err := g.latestRealRequestsByDownstreamKey(ids)
	if err != nil {
		return nil, err
	}
	for id, request := range recent {
		result[id] = request.ModelName
	}
	return result, nil
}

// Reuse the model-preview lookup for route display; listing keys must not add
// another request-log scan or one history query per displayed key.
func (g *Gateway) latestRealRequestsByDownstreamKey(ids []uint) (map[uint]*GatewayKeyRecentRoute, error) {
	result := make(map[uint]*GatewayKeyRecentRoute, len(ids))
	if len(ids) == 0 {
		return result, nil
	}
	base := g.db.Model(&GatewayRequestLog{}).
		Select(`downstream_key_id, id,
			ROW_NUMBER() OVER (PARTITION BY downstream_key_id ORDER BY created_at DESC, id DESC) AS row_number`).
		Where("downstream_key_id IN ? AND downstream_key_id > 0", ids).
		Where(performanceGatewayRequestSQL("")).
		Where("COALESCE(route_reason, '') NOT IN ?", []string{gatewayRouteReasonLatencyProbe, gatewayRouteReasonKeyTest, "model_check"}).
		Where("TRIM(COALESCE(model_name, '')) <> ''")
	rows := make([]GatewayKeyRecentRoute, 0, len(ids))
	// Rank narrow IDs first; fetch display fields only for each winning row.
	if err := g.db.Table("(?) AS recent_models", base).
		Select(`recent_models.downstream_key_id, request.id AS request_id,
			request.model_name, request.channel_id, request.channel_name_snapshot AS channel_name,
			request.group_name, request.created_at`).
		Joins("JOIN gateway_request_logs AS request ON request.id = recent_models.id").
		Where("recent_models.row_number = ?", 1).Scan(&rows).Error; err != nil {
		return nil, err
	}
	for i := range rows {
		item := &rows[i]
		if item.DownstreamKeyID == 0 || strings.TrimSpace(item.ModelName) == "" {
			continue
		}
		item.ModelName = strings.TrimSpace(item.ModelName)
		result[item.DownstreamKeyID] = item
	}
	return result, nil
}

// ListDownstreamKeysByCandidateGroup intentionally avoids usage aggregation;
// it is used only while validating a candidate-group edit.
func (g *Gateway) ListDownstreamKeysByCandidateGroup(candidateGroupID uint) ([]GatewayDownstreamKey, error) {
	if candidateGroupID == 0 {
		return []GatewayDownstreamKey{}, nil
	}
	keys := []GatewayDownstreamKey{}
	err := g.db.Where("candidate_group_id = ?", candidateGroupID).Order("id ASC").Find(&keys).Error
	return keys, err
}

func (g *Gateway) ListCandidateGroups() ([]GatewayCandidateGroup, error) {
	var groups []GatewayCandidateGroup
	if err := g.db.Order("name ASC").Order("id ASC").Find(&groups).Error; err != nil {
		return nil, err
	}
	if len(groups) == 0 {
		return groups, nil
	}
	ids := make([]uint, 0, len(groups))
	for _, group := range groups {
		ids = append(ids, group.ID)
	}
	var members []GatewayCandidateGroupMember
	if err := g.db.Where("candidate_group_id IN ?", ids).Order("channel_id ASC").Order("group_name ASC").Find(&members).Error; err != nil {
		return nil, err
	}
	if err := hydrateCandidateMemberPresenceGenerations(g.db, members); err != nil {
		return nil, err
	}
	byGroup := make(map[uint][]GatewayCandidateGroupMember, len(groups))
	for _, member := range members {
		byGroup[member.CandidateGroupID] = append(byGroup[member.CandidateGroupID], member)
	}
	for i := range groups {
		groups[i].Members = byGroup[groups[i].ID]
	}
	return groups, nil
}

func (g *Gateway) FindCandidateGroup(id uint) (*GatewayCandidateGroup, error) {
	if id == 0 {
		return nil, gorm.ErrRecordNotFound
	}
	var group GatewayCandidateGroup
	if err := g.db.First(&group, id).Error; err != nil {
		return nil, err
	}
	if err := g.db.Where("candidate_group_id = ?", id).Order("channel_id ASC").Order("group_name ASC").Find(&group.Members).Error; err != nil {
		return nil, err
	}
	if err := hydrateCandidateMemberPresenceGenerations(g.db, group.Members); err != nil {
		return nil, err
	}
	return &group, nil
}

func hydrateCandidateMemberPresenceGenerations(db *gorm.DB, members []GatewayCandidateGroupMember) error {
	if len(members) == 0 {
		return nil
	}
	ids := make([]uint, 0, len(members))
	seen := make(map[uint]struct{}, len(members))
	for _, member := range members {
		if member.UpstreamGroupID == 0 {
			continue
		}
		if _, exists := seen[member.UpstreamGroupID]; exists {
			continue
		}
		seen[member.UpstreamGroupID] = struct{}{}
		ids = append(ids, member.UpstreamGroupID)
	}
	if len(ids) == 0 {
		return nil
	}
	var groups []GatewayUpstreamGroup
	if err := db.Select("id", "presence_generation").Where("id IN ?", ids).Find(&groups).Error; err != nil {
		return err
	}
	generationByID := make(map[uint]uint64, len(groups))
	for _, group := range groups {
		generationByID[group.ID] = group.PresenceGeneration
	}
	for index := range members {
		members[index].PresenceGeneration = generationByID[members[index].UpstreamGroupID]
	}
	return nil
}

func (g *Gateway) SaveCandidateGroup(group *GatewayCandidateGroup, members []GatewayCandidateGroupMember) error {
	return g.saveCandidateGroupWithOptions(group, members, "administrator", false, nil, nil)
}

// SaveCandidateGroupCurrentGenerationAs requires every submitted member to
// carry the upstream identity and presence generation observed by the caller.
// The transaction rejects a stale editor snapshot instead of silently binding
// the same channel/group name to a newer generation.
func (g *Gateway) SaveCandidateGroupCurrentGenerationAs(group *GatewayCandidateGroup, members []GatewayCandidateGroupMember, actor string) error {
	return g.saveCandidateGroupWithOptions(group, members, actor, true, nil, nil)
}

// SaveCandidateGroupCurrentGenerationVersionAs also fences the candidate-group
// snapshot used by an administrator. This prevents an old full-list editor
// from deleting memberships added after the editor was opened.
func (g *Gateway) SaveCandidateGroupCurrentGenerationVersionAs(group *GatewayCandidateGroup, members []GatewayCandidateGroupMember, expectedMembershipVersion uint64, actor string) error {
	return g.saveCandidateGroupWithOptions(group, members, actor, true, &expectedMembershipVersion, nil)
}

func (g *Gateway) saveCandidateGroup(group *GatewayCandidateGroup, members []GatewayCandidateGroupMember, afterGroupLock func()) error {
	return g.saveCandidateGroupWithOptions(group, members, "administrator", false, nil, afterGroupLock)
}

func (g *Gateway) saveCandidateGroupWithOptions(group *GatewayCandidateGroup, members []GatewayCandidateGroupMember, actor string, requireCurrentGeneration bool, expectedMembershipVersion *uint64, afterGroupLock func()) error {
	if group == nil {
		return errors.New("candidate group is required")
	}
	actor = strings.TrimSpace(actor)
	if actor == "" {
		actor = "administrator"
	}
	group.Name = strings.TrimSpace(group.Name)
	if group.Name == "" {
		return errors.New("candidate group name is required")
	}
	if len(members) == 0 {
		return errors.New("candidate group requires at least one member")
	}
	if expectedMembershipVersion != nil && group.ID > 0 && *expectedMembershipVersion == 0 {
		return ErrCandidateGroupMembershipChanged
	}
	normalizedMembers, err := normalizeCandidateGroupMembers(g.db, members)
	if err != nil {
		return err
	}
	if requireCurrentGeneration {
		for _, member := range normalizedMembers {
			if member.UpstreamGroupID == 0 || member.PresenceGeneration == 0 {
				return ErrUpstreamGroupGenerationChanged
			}
		}
	}
	return g.db.Transaction(func(tx *gorm.DB) error {
		var lockedChannelID uint
		for _, member := range normalizedMembers {
			if member.ChannelID == lockedChannelID {
				continue
			}
			if err := lockUpstreamGroupParentChannelTx(tx, member.ChannelID); err != nil {
				return err
			}
			lockedChannelID = member.ChannelID
		}
		if err := lockAutomaticAssignmentMutationTx(tx); err != nil {
			return err
		}
		if err := ensureUniqueCandidateGroupName(tx, group); err != nil {
			return err
		}
		existingMembers := make(map[uint]GatewayCandidateGroupMember)
		if group.ID == 0 {
			if err := tx.Create(group).Error; err != nil {
				return err
			}
		} else {
			var currentGroup GatewayCandidateGroup
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
				Select("id", "membership_version").
				First(&currentGroup, group.ID).Error; err != nil {
				return err
			}
			if expectedMembershipVersion != nil && currentGroup.MembershipVersion != *expectedMembershipVersion {
				return ErrCandidateGroupMembershipChanged
			}
			var currentMembers []GatewayCandidateGroupMember
			if err := tx.Where("candidate_group_id = ?", group.ID).Find(&currentMembers).Error; err != nil {
				return err
			}
			for _, member := range currentMembers {
				existingMembers[member.UpstreamGroupID] = member
			}
			result := tx.Model(&GatewayCandidateGroup{}).Where("id = ?", group.ID).Updates(map[string]any{
				"name":                 group.Name,
				"model_mapping_family": group.ModelMappingFamily,
			})
			if result.Error != nil {
				return result.Error
			}
		}
		if afterGroupLock != nil {
			afterGroupLock()
		}
		var referencedKeys []GatewayDownstreamKey
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("candidate_group_id = ?", group.ID).
			Order("id ASC").
			Find(&referencedKeys).Error; err != nil {
			return err
		}
		resolvedMembers := make([]GatewayCandidateGroupMember, 0, len(normalizedMembers))
		resolvedIdentities := make(map[uint]struct{}, len(normalizedMembers))
		for index := range normalizedMembers {
			member := &normalizedMembers[index]
			canonicalGroupName, err := lockUpstreamRoutingHealthDomainAdvisoryTx(tx, member.ChannelID, member.GroupName)
			if err != nil {
				return err
			}
			rateExists, err := lockUpstreamGroupRate(tx, member.ChannelID, canonicalGroupName)
			if err != nil {
				return err
			}
			if !rateExists {
				return fmt.Errorf("candidate %d/%s does not exist", member.ChannelID, member.GroupName)
			}
			upstreamGroup, _, err := ensureUpstreamGroupIdentityTx(tx, member.ChannelID, member.GroupName, true, time.Now())
			if err != nil {
				return err
			}
			if requireCurrentGeneration && (member.UpstreamGroupID != upstreamGroup.ID || member.PresenceGeneration != upstreamGroup.PresenceGeneration) {
				return ErrUpstreamGroupGenerationChanged
			}
			member.UpstreamGroupID = upstreamGroup.ID
			member.PresenceGeneration = upstreamGroup.PresenceGeneration
			if _, duplicate := resolvedIdentities[member.UpstreamGroupID]; duplicate {
				continue
			}
			resolvedIdentities[member.UpstreamGroupID] = struct{}{}
			if err := lockCurrentGenerationAssignmentKeyTx(tx, upstreamGroupGeneration(*upstreamGroup), nil); err != nil {
				return fmt.Errorf("candidate %d/%s: %w", member.ChannelID, member.GroupName, err)
			}
			resolvedMembers = append(resolvedMembers, *member)
		}
		normalizedMembers = resolvedMembers
		now := time.Now()
		desiredByIdentity := make(map[uint]GatewayCandidateGroupMember, len(normalizedMembers))
		affectedIdentities := make(map[uint]struct{}, len(normalizedMembers)+len(existingMembers))
		for _, member := range existingMembers {
			if member.UpstreamGroupID > 0 {
				affectedIdentities[member.UpstreamGroupID] = struct{}{}
			}
		}
		for _, member := range normalizedMembers {
			desiredByIdentity[member.UpstreamGroupID] = member
			affectedIdentities[member.UpstreamGroupID] = struct{}{}
		}
		for index := range referencedKeys {
			key := &referencedKeys[index]
			if key.SmartRouting {
				continue
			}
			canonical, err := canonicalUpstreamGroupName(tx, key.ManualGroupName)
			if err != nil {
				return err
			}
			var manualIdentity GatewayUpstreamGroup
			if err := tx.Select("id").Where("channel_id = ? AND canonical_group_name = ?", key.ManualChannelID, canonical).First(&manualIdentity).Error; err != nil {
				if !errors.Is(err, gorm.ErrRecordNotFound) {
					return err
				}
				return fmt.Errorf("candidate group %d must keep manual route %d/%s used by downstream key %q; enable smart routing or choose another manual route first", group.ID, key.ManualChannelID, strings.TrimSpace(key.ManualGroupName), key.Name)
			}
			if _, exists := desiredByIdentity[manualIdentity.ID]; !exists {
				return fmt.Errorf("candidate group %d must keep manual route %d/%s used by downstream key %q; enable smart routing or choose another manual route first", group.ID, key.ManualChannelID, strings.TrimSpace(key.ManualGroupName), key.Name)
			}
		}
		var manualClaims []GatewayCandidateGroupMembershipClaim
		if err := tx.Where("candidate_group_id = ? AND source_kind = ? AND active = ?", group.ID, "manual", true).Find(&manualClaims).Error; err != nil {
			return err
		}
		manualByIdentity := make(map[uint]struct{}, len(manualClaims))
		for _, claim := range manualClaims {
			manualByIdentity[claim.UpstreamGroupID] = struct{}{}
			if _, keep := desiredByIdentity[claim.UpstreamGroupID]; keep {
				continue
			}
			if err := revokeMembershipClaimsTx(tx, "manual", claim.SourceID, now); err != nil {
				return err
			}
		}
		state, err := automaticAssignmentStateTx(tx)
		if err != nil {
			return err
		}
		for _, member := range normalizedMembers {
			if _, existed := existingMembers[member.UpstreamGroupID]; existed {
				continue
			}
			if _, exists := manualByIdentity[member.UpstreamGroupID]; exists {
				continue
			}
			var identity GatewayUpstreamGroup
			if err := tx.First(&identity, member.UpstreamGroupID).Error; err != nil {
				return err
			}
			audit := GatewayUpstreamGroupAssignmentAudit{
				UpstreamGroupID: identity.ID, RawGroupName: member.GroupName, CanonicalGroupName: identity.CanonicalGroupName,
				PresenceGeneration: identity.PresenceGeneration, ClassificationVersion: identity.ClassificationVersion,
				FactsVersion: identity.FactsVersion, RulesetVersion: state.RulesetVersion,
				Action: "manual_assign", TriggerSource: "manual_assign", MatchedRuleIDs: []uint{},
				TargetCandidateGroupIDs: []uint{group.ID}, FactsSnapshot: map[string]any{"facts_digest": identity.SourceRevision},
				Explanation: map[string]any{"reason": "candidate_group_saved_by_administrator"}, Actor: actor, CreatedAt: now,
			}
			if err := tx.Create(&audit).Error; err != nil {
				return err
			}
			if err := ensureMembershipClaimTx(tx, group.ID, &identity, member.GroupName, "manual", audit.ID, now); err != nil {
				return err
			}
		}
		for upstreamGroupID := range affectedIdentities {
			_, desired := desiredByIdentity[upstreamGroupID]
			if _, existed := existingMembers[upstreamGroupID]; existed && desired {
				continue
			}
			if err := syncManualCandidateOverrideTx(tx, upstreamGroupID, group.ID, desired, actor, now); err != nil {
				return err
			}
		}
		for index := range referencedKeys {
			key := &referencedKeys[index]
			mappingEnabled, err := downstreamKeyModelMappingEnabled(key.ModelMappingStrategy)
			if err != nil {
				return err
			}
			if !mappingEnabled {
				continue
			}
			if !candidateGroupMappingFamilyMatchesKey(group.ModelMappingFamily, key) {
				return fmt.Errorf("candidate group is used by mapped key %q and must remain %s", key.Name, key.ModelFamily)
			}
		}
		return nil
	})
}

func candidateMembersSupportManualMappedModel(tx *gorm.DB, members []GatewayCandidateGroupMember, model string) error {
	model = strings.TrimSpace(model)
	hasSynchronizedModels := false
	modelSupported := false
	for _, member := range members {
		upstream, err := candidateMemberCurrentGenerationKey(tx, member)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		var channel Channel
		if err := tx.Clauses(clause.Locking{Strength: "SHARE"}).
			Select("id", "credential_mode", "direct_models").
			First(&channel, upstream.ChannelID).Error; err != nil {
			return err
		}
		if channel.CredentialMode == CredentialModeDirect {
			// Direct credentials are configured per routing group. The channel's
			// legacy aggregate is intentionally not consulted here: doing so lets
			// one group's model allowlist authorize a different group's key. Only
			// an unprojected pre-000070 row may use the single legacy allowlist.
			if upstream.DirectCredentialID == nil && upstream.DirectCredentialRevision == 0 {
				directModels, parseErr := ParseDirectModels(channel.DirectModels)
				if parseErr != nil {
					continue
				}
				for _, candidateModel := range directModels {
					hasSynchronizedModels = true
					if candidateModel == model {
						modelSupported = true
					}
				}
				continue
			}
			if upstream.DirectCredentialID == nil || *upstream.DirectCredentialID == 0 || upstream.DirectCredentialRevision == 0 {
				continue
			}
			for _, candidateModel := range strings.Split(upstream.ModelIDs, ",") {
				candidateModel = strings.TrimSpace(candidateModel)
				if candidateModel == "" {
					continue
				}
				hasSynchronizedModels = true
				if candidateModel == model {
					modelSupported = true
				}
			}
			continue
		}
		for _, candidateModel := range strings.Split(upstream.ModelIDs, ",") {
			candidateModel = strings.TrimSpace(candidateModel)
			if candidateModel == "" {
				continue
			}
			hasSynchronizedModels = true
			if strings.EqualFold(candidateModel, model) {
				modelSupported = true
			}
		}
	}
	if !hasSynchronizedModels {
		return errors.New("candidate group has no synchronized model IDs for manual model mapping")
	}
	if modelSupported {
		return nil
	}
	return fmt.Errorf("candidate group does not support manual mapped model %q", model)
}

func candidateMembersSupportMappingFamily(tx *gorm.DB, members []GatewayCandidateGroupMember, family string) error {
	family = strings.ToLower(strings.TrimSpace(family))
	if family == "" {
		return nil
	}
	for _, member := range members {
		upstream, err := candidateMemberCurrentGenerationKey(tx, member)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		var channel Channel
		if err := tx.Clauses(clause.Locking{Strength: "SHARE"}).
			Select("id", "credential_mode", "gateway_app").
			First(&channel, upstream.ChannelID).Error; err != nil {
			return err
		}
		if channel.CredentialMode == CredentialModeDirect {
			if strings.EqualFold(strings.TrimSpace(string(channel.GatewayApp)), family) {
				return nil
			}
			continue
		}
		for _, candidateFamily := range strings.Split(upstream.ModelFamilies, ",") {
			if strings.EqualFold(strings.TrimSpace(candidateFamily), family) {
				return nil
			}
		}
	}
	return fmt.Errorf("candidate group has no member with synchronized %s radar capability", family)
}

func candidateMemberCurrentGenerationKey(tx *gorm.DB, member GatewayCandidateGroupMember) (*GatewayUpstreamKey, error) {
	if tx == nil {
		return nil, errors.New("gateway store is unavailable")
	}
	var group GatewayUpstreamGroup
	if member.UpstreamGroupID != 0 {
		if err := tx.Clauses(clause.Locking{Strength: "SHARE"}).
			Select("id", "channel_id", "canonical_group_name", "present", "presence_generation", "discovered_at").
			Where("id = ? AND present = ?", member.UpstreamGroupID, true).First(&group).Error; err != nil {
			return nil, err
		}
	} else {
		canonical, err := canonicalUpstreamGroupName(tx, member.GroupName)
		if err != nil {
			return nil, err
		}
		if err := tx.Clauses(clause.Locking{Strength: "SHARE"}).
			Select("id", "channel_id", "canonical_group_name", "present", "presence_generation", "discovered_at").
			Where("channel_id = ? AND canonical_group_name = ? AND present = ?", member.ChannelID, canonical, true).First(&group).Error; err != nil {
			return nil, err
		}
	}
	var upstream GatewayUpstreamKey
	if err := tx.Clauses(clause.Locking{Strength: "SHARE"}).
		Select("id", "channel_id", "direct_credential_id", "direct_credential_revision", "model_families", "model_ids", "models_synced_at", "models_error").
		Where("channel_id = ? AND lower(btrim(group_name)) = ? AND enabled = ? AND models_synced_at IS NOT NULL AND models_synced_at >= ? AND COALESCE(models_error, '') = ''", group.ChannelID, group.CanonicalGroupName, true, group.DiscoveredAt).
		First(&upstream).Error; err != nil {
		return nil, err
	}
	return &upstream, nil
}

func (g *Gateway) CandidateMembersSupportMappingFamily(members []GatewayCandidateGroupMember, family string) error {
	if g == nil || g.db == nil {
		return errors.New("gateway store is unavailable")
	}
	return candidateMembersSupportMappingFamily(g.db, members, family)
}

func (g *Gateway) CandidateMembersSupportManualMappedModel(members []GatewayCandidateGroupMember, model string) error {
	if g == nil || g.db == nil {
		return errors.New("gateway store is unavailable")
	}
	return candidateMembersSupportManualMappedModel(g.db, members, model)
}

func downstreamKeyModelMappingEnabled(strategy string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(strategy)) {
	case "", "off":
		return false, nil
	case "iq", "cost", "time", "balanced", "manual":
		return true, nil
	default:
		return false, errors.New("invalid model mapping strategy")
	}
}

func candidateGroupMappingFamilyMatchesKey(family string, key *GatewayDownstreamKey) bool {
	if key == nil {
		return false
	}
	family = strings.ToLower(strings.TrimSpace(family))
	return (family == string(GatewayAppCodex) || family == string(GatewayAppClaude)) &&
		strings.EqualFold(strings.TrimSpace(string(key.App)), family) &&
		strings.EqualFold(strings.TrimSpace(key.ModelFamily), family)
}

func normalizeCandidateGroupMembers(db *gorm.DB, members []GatewayCandidateGroupMember) ([]GatewayCandidateGroupMember, error) {
	type canonicalMember struct {
		member        GatewayCandidateGroupMember
		canonicalName string
	}
	canonicalMembers := make([]canonicalMember, 0, len(members))
	seen := make(map[string]struct{}, len(members))
	for _, member := range members {
		member.GroupName = strings.TrimSpace(member.GroupName)
		if member.ChannelID == 0 || member.GroupName == "" {
			return nil, errors.New("candidate group member requires channel and upstream group")
		}
		canonicalName, err := canonicalUpstreamGroupName(db, member.GroupName)
		if err != nil {
			return nil, err
		}
		key := candidateGroupMemberKey(member.ChannelID, canonicalName)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		member.ID = 0
		member.CandidateGroupID = 0
		canonicalMembers = append(canonicalMembers, canonicalMember{member: member, canonicalName: canonicalName})
	}
	sort.Slice(canonicalMembers, func(i, j int) bool {
		if canonicalMembers[i].member.ChannelID != canonicalMembers[j].member.ChannelID {
			return canonicalMembers[i].member.ChannelID < canonicalMembers[j].member.ChannelID
		}
		if canonicalMembers[i].canonicalName != canonicalMembers[j].canonicalName {
			return canonicalMembers[i].canonicalName < canonicalMembers[j].canonicalName
		}
		return canonicalMembers[i].member.GroupName < canonicalMembers[j].member.GroupName
	})
	normalized := make([]GatewayCandidateGroupMember, 0, len(canonicalMembers))
	for _, item := range canonicalMembers {
		normalized = append(normalized, item.member)
	}
	return normalized, nil
}

func candidateGroupMemberKey(channelID uint, groupName string) string {
	return fmt.Sprintf("%d\x00%s", channelID, strings.TrimSpace(groupName))
}

// Candidate membership changes and probe-policy updates lock the same rate row.
// Sorting members before locking gives multi-channel candidate saves one stable
// lock order and makes a concurrent disable linearizable.
func lockUpstreamGroupRate(tx *gorm.DB, channelID uint, groupName string) (bool, error) {
	canonical, err := canonicalUpstreamGroupName(tx, groupName)
	if err != nil {
		return false, err
	}
	var rate RateSnapshot
	err = tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Select("id").
		Where("channel_id = ? AND lower(btrim(model_name)) = ?", channelID, canonical).
		Order("last_seen_at DESC, id DESC").
		First(&rate).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	return err == nil, err
}

func upstreamGroupProbeDisabled(tx *gorm.DB, channelID uint, groupName string) (bool, error) {
	canonical, err := canonicalUpstreamGroupName(tx, groupName)
	if err != nil {
		return false, err
	}
	var key GatewayUpstreamKey
	err = tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Select("id", "latency_probe_enabled").
		Where("channel_id = ? AND lower(btrim(group_name)) = ?", channelID, canonical).
		Order("enabled DESC, provisioning ASC, models_synced_at DESC NULLS LAST, id DESC").
		First(&key).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return !key.LatencyProbeEnabled, nil
}

func (g *Gateway) DeleteCandidateGroup(id uint) error {
	rulesChanged := false
	err := g.db.Transaction(func(tx *gorm.DB) error {
		if err := lockAutomaticAssignmentMutationTx(tx); err != nil {
			return err
		}
		state, err := g.lockAutomaticAssignmentState(tx)
		if err != nil {
			return err
		}
		if err := lockCandidateGroup(tx, id); err != nil {
			return err
		}
		var references int64
		if err := tx.Model(&GatewayDownstreamKey{}).Where("candidate_group_id = ?", id).Count(&references).Error; err != nil {
			return err
		}
		if references > 0 {
			return ErrCandidateGroupInUse
		}
		var ruleCount int64
		if err := tx.Model(&GatewayCandidateGroupAutoRule{}).Where("candidate_group_id = ?", id).Count(&ruleCount).Error; err != nil {
			return err
		}
		if err := tx.Where("candidate_group_id = ?", id).Delete(&GatewayCandidateGroupMember{}).Error; err != nil {
			return err
		}
		result := tx.Delete(&GatewayCandidateGroup{}, id)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return gorm.ErrRecordNotFound
		}
		if ruleCount > 0 {
			state.RulesetVersion++
			state.UpdatedAt = time.Now()
			if err := tx.Save(state).Error; err != nil {
				return err
			}
			rulesChanged = true
		}
		return nil
	})
	if err == nil && rulesChanged {
		_, _ = g.PreviewAutomaticAssignments(nil, true)
	}
	return err
}

func (g *Gateway) ListCandidateGroupMembers(id uint) ([]GatewayCandidateGroupMember, error) {
	var members []GatewayCandidateGroupMember
	if err := g.db.Where("candidate_group_id = ?", id).Find(&members).Error; err != nil {
		return nil, err
	}
	if err := hydrateCandidateMemberPresenceGenerations(g.db, members); err != nil {
		return nil, err
	}
	return members, nil
}

// SummarizeDownstreamKeyUsage returns one aggregate row per key. The query
// deliberately uses the same visibility rule as the usage and analytics
// pages, while calculating today's bucket from the server's local day.
func (g *Gateway) SummarizeDownstreamKeyUsage(ids []uint, now time.Time) (map[uint]GatewayDownstreamKeyUsage, error) {
	if usage, used, err := g.summarizeDownstreamKeyUsageFromAggregates(ids, now); used || err != nil {
		return usage, err
	}
	return g.summarizeDownstreamKeyUsageRaw(ids, now)
}

func (g *Gateway) summarizeDownstreamKeyUsageRaw(ids []uint, now time.Time) (map[uint]GatewayDownstreamKeyUsage, error) {
	result := make(map[uint]GatewayDownstreamKeyUsage, len(ids))
	if len(ids) == 0 {
		return result, nil
	}
	dayStart := time.Date(now.In(now.Location()).Year(), now.In(now.Location()).Month(), now.In(now.Location()).Day(), 0, 0, 0, 0, now.Location())
	type usageRow struct {
		DownstreamKeyID          uint    `gorm:"column:downstream_key_id"`
		TotalCost                float64 `gorm:"column:total_cost"`
		TodayCost                float64 `gorm:"column:today_cost"`
		TotalRequests            int64   `gorm:"column:total_requests"`
		TodayRequests            int64   `gorm:"column:today_requests"`
		TotalTokens              int64   `gorm:"column:total_tokens"`
		TodayTokens              int64   `gorm:"column:today_tokens"`
		ActualCost               float64 `gorm:"column:actual_cost"`
		EstimatedCost            float64 `gorm:"column:estimated_cost"`
		ReportedCost             float64 `gorm:"column:reported_cost"`
		ReconciledCost           float64 `gorm:"column:reconciled_cost"`
		UnavailableRequests      int64   `gorm:"column:unavailable_requests"`
		TodayUnavailableRequests int64   `gorm:"column:today_unavailable_requests"`
	}
	rows := []usageRow{}
	estimatedCostSQL := gatewayRequestCostByStatusSQL("", GatewayCostStatusEstimated)
	reportedCostSQL := gatewayRequestCostByStatusSQL("", GatewayCostStatusReported)
	reconciledCostSQL := gatewayRequestCostByStatusSQL("", GatewayCostStatusReconciled)
	err := g.db.Model(&GatewayRequestLog{}).
		Select(`downstream_key_id,
			COALESCE(SUM(`+gatewayRequestBillableCostSQL("")+`), 0) AS total_cost,
			COALESCE(SUM(CASE WHEN created_at >= ? THEN `+gatewayRequestBillableCostSQL("")+` ELSE 0 END), 0) AS today_cost,
			COUNT(*) AS total_requests,
			COALESCE(SUM(CASE WHEN created_at >= ? THEN 1 ELSE 0 END), 0) AS today_requests,
			COALESCE(SUM(total_tokens), 0) AS total_tokens,
			COALESCE(SUM(CASE WHEN created_at >= ? THEN total_tokens ELSE 0 END), 0) AS today_tokens,
			COALESCE(SUM(`+gatewayRequestActualCostSQL("")+`), 0) AS actual_cost,
			COALESCE(SUM(`+estimatedCostSQL+`), 0) AS estimated_cost,
			COALESCE(SUM(`+reportedCostSQL+`), 0) AS reported_cost,
			COALESCE(SUM(`+reconciledCostSQL+`), 0) AS reconciled_cost,
			COALESCE(SUM(`+gatewayRequestUnavailableCountSQL("")+`), 0) AS unavailable_requests,
			COALESCE(SUM(CASE WHEN created_at >= ? THEN `+gatewayRequestUnavailableCountSQL("")+` ELSE 0 END), 0) AS today_unavailable_requests`,
			dayStart, dayStart, dayStart, dayStart).
		Where("downstream_key_id IN ? AND "+userVisibleGatewayRequestSQL(""), ids).
		Group("downstream_key_id").
		Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		result[row.DownstreamKeyID] = GatewayDownstreamKeyUsage{
			TotalCost:                row.TotalCost,
			TodayCost:                row.TodayCost,
			TotalRequests:            row.TotalRequests,
			TodayRequests:            row.TodayRequests,
			TotalTokens:              row.TotalTokens,
			TodayTokens:              row.TodayTokens,
			ActualCost:               row.ActualCost,
			EstimatedCost:            row.EstimatedCost,
			ReportedCost:             row.ReportedCost,
			ReconciledCost:           row.ReconciledCost,
			UnavailableRequests:      row.UnavailableRequests,
			TodayUnavailableRequests: row.TodayUnavailableRequests,
		}
	}
	return result, nil
}

func (g *Gateway) FindDownstreamKey(id uint) (*GatewayDownstreamKey, error) {
	var key GatewayDownstreamKey
	if err := g.db.First(&key, id).Error; err != nil {
		return nil, err
	}
	return &key, nil
}

func (g *Gateway) FindDownstreamKeyByHash(hash string) (*GatewayDownstreamKey, error) {
	var key GatewayDownstreamKey
	if err := g.db.Where("key_hash = ? AND enabled = ?", hash, true).First(&key).Error; err != nil {
		return nil, err
	}
	return &key, nil
}

func (g *Gateway) UpdateDownstreamKey(key *GatewayDownstreamKey) error {
	if key == nil || key.ID == 0 {
		return errors.New("downstream key is required")
	}
	expectedUpdatedAt := key.UpdatedAt
	updated, err := g.patchDownstreamKey(key.ID, func(_ *Gateway, current *GatewayDownstreamKey) error {
		if !expectedUpdatedAt.IsZero() && !sameDownstreamKeyRevision(expectedUpdatedAt, current.UpdatedAt) {
			return ErrDownstreamKeyUpdateConflict
		}
		replacement := *key
		replacement.ID = current.ID
		replacement.CreatedAt = current.CreatedAt
		*current = replacement
		return normalizeDownstreamCandidateTarget(current)
	}, nil)
	if err != nil {
		return err
	}
	*key = *updated
	key.ValidateRoutePolicySnapshot = false
	return nil
}

// PatchDownstreamKey serializes partial key updates and applies them to the
// row version loaded after the lock is acquired. Apply must be deterministic
// because it runs once to establish lock targets and again on the locked row.
// The validator receives a transaction-scoped store so cross-field checks
// observe the same snapshot.
func (g *Gateway) PatchDownstreamKey(
	id uint,
	apply func(*GatewayDownstreamKey) error,
	validate func(*Gateway, *GatewayDownstreamKey) error,
) (*GatewayDownstreamKey, error) {
	return g.patchDownstreamKey(id, func(_ *Gateway, key *GatewayDownstreamKey) error {
		if apply == nil {
			return nil
		}
		return apply(key)
	}, validate)
}

// PatchDownstreamKeyWithStore is the transaction-aware variant used when a
// patch derives its candidate target from a key-group route policy. The policy
// lookup must run in the same protected transaction as the candidate-group
// lock, otherwise a policy edit can leave a stale target on the key.
func (g *Gateway) PatchDownstreamKeyWithStore(
	id uint,
	apply func(*Gateway, *GatewayDownstreamKey) error,
	validate func(*Gateway, *GatewayDownstreamKey) error,
) (*GatewayDownstreamKey, error) {
	return g.patchDownstreamKey(id, apply, validate)
}

func (g *Gateway) patchDownstreamKey(
	id uint,
	apply func(*Gateway, *GatewayDownstreamKey) error,
	validate func(*Gateway, *GatewayDownstreamKey) error,
) (*GatewayDownstreamKey, error) {
	if g == nil || g.db == nil || id == 0 {
		return nil, errors.New("downstream key is required")
	}
	var key GatewayDownstreamKey
	err := g.db.Transaction(func(tx *gorm.DB) error {
		if err := lockAutomaticAssignmentMutationTx(tx); err != nil {
			return err
		}
		if tx.Dialector != nil && tx.Dialector.Name() == string(DBDriverPostgres) {
			lockID := gatewayDownstreamKeyAdvisoryLockNamespace ^ int64(uint64(id)&0x00000fffffffffff)
			if err := tx.Exec("SELECT pg_advisory_xact_lock(?)", lockID).Error; err != nil {
				return err
			}
		}
		transactionStore := &Gateway{
			db:               tx,
			userTime:         g.userTime,
			databaseLocation: g.databaseLocation,
			channelLockSlots: g.channelLockSlots,
		}
		var prospective GatewayDownstreamKey
		if err := tx.First(&prospective, id).Error; err != nil {
			return err
		}
		if apply != nil {
			if err := apply(transactionStore, &prospective); err != nil {
				return err
			}
		}
		prospective.ID = id
		if err := normalizeDownstreamCandidateTarget(&prospective); err != nil {
			return err
		}
		if err := validateKeyGroupRoutePolicySnapshotTx(tx, &prospective); err != nil {
			return err
		}
		if err := lockCandidateGroup(tx, prospective.CandidateGroupID); err != nil {
			return err
		}
		if err := lockKeyGroup(tx, prospective.KeyGroupID); err != nil {
			return err
		}
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&key, id).Error; err != nil {
			return err
		}
		if apply != nil {
			if err := apply(transactionStore, &key); err != nil {
				return err
			}
		}
		key.ID = id
		if err := normalizeDownstreamCandidateTarget(&key); err != nil {
			return err
		}
		if !sameDownstreamCandidateTarget(&key, &prospective) || key.KeyGroupID != prospective.KeyGroupID {
			return ErrDownstreamKeyUpdateConflict
		}
		if err := validateKeyGroupRoutePolicySnapshotTx(tx, &key); err != nil {
			return err
		}
		if validate != nil {
			if err := validate(transactionStore, &key); err != nil {
				return err
			}
		}
		if err := ensureUniqueDownstreamKeyName(tx, &key); err != nil {
			return err
		}
		if err := ensureManualRouteCandidateMembership(tx, &key); err != nil {
			return err
		}
		return tx.Save(&key).Error
	})
	if err != nil {
		return nil, err
	}
	key.ValidateRoutePolicySnapshot = false
	return &key, nil
}

func sameDownstreamKeyRevision(left, right time.Time) bool {
	return left.UTC().Truncate(time.Microsecond).Equal(right.UTC().Truncate(time.Microsecond))
}

func sameDownstreamCandidateTarget(left, right *GatewayDownstreamKey) bool {
	if left == nil || right == nil {
		return left == right
	}
	if left.CandidateGroupID != right.CandidateGroupID ||
		left.CandidateTargetMode != right.CandidateTargetMode ||
		left.CandidateTargetSource != right.CandidateTargetSource {
		return false
	}
	if (left.RoutePolicyID == nil) != (right.RoutePolicyID == nil) ||
		(left.RoutePolicyVersion == nil) != (right.RoutePolicyVersion == nil) {
		return false
	}
	if left.RoutePolicyID != nil && *left.RoutePolicyID != *right.RoutePolicyID {
		return false
	}
	return left.RoutePolicyVersion == nil || *left.RoutePolicyVersion == *right.RoutePolicyVersion
}

func normalizeDownstreamCandidateTarget(key *GatewayDownstreamKey) error {
	if key == nil {
		return errors.New("downstream key is required")
	}
	mode := strings.TrimSpace(key.CandidateTargetMode)
	changed := false
	if key.CandidateGroupID > 0 {
		if mode != "candidate_group" {
			mode = "candidate_group"
			changed = true
		}
	} else if mode == "" || mode == "candidate_group" {
		mode = "all_candidates"
		changed = true
	}
	if mode != "candidate_group" && mode != "all_candidates" && mode != "unset" {
		return errors.New("invalid downstream candidate target mode")
	}
	key.CandidateTargetMode = mode
	if changed {
		key.CandidateTargetSource = "manual"
		key.RoutePolicyID = nil
		key.RoutePolicyVersion = nil
	} else if strings.TrimSpace(key.CandidateTargetSource) == "" {
		key.CandidateTargetSource = "manual"
	}
	return nil
}

func ensureManualRouteCandidateMembership(tx *gorm.DB, key *GatewayDownstreamKey) error {
	if key == nil || key.SmartRouting || key.CandidateGroupID == 0 {
		return nil
	}
	groupName := strings.TrimSpace(key.ManualGroupName)
	if key.ManualChannelID == 0 || groupName == "" {
		return errors.New("manual route requires channel and group")
	}
	canonical, err := canonicalUpstreamGroupName(tx, groupName)
	if err != nil {
		return err
	}
	var identity GatewayUpstreamGroup
	identityErr := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Select("id", "present").
		Where("channel_id = ? AND canonical_group_name = ?", key.ManualChannelID, canonical).
		First(&identity).Error
	if identityErr != nil && !errors.Is(identityErr, gorm.ErrRecordNotFound) {
		return identityErr
	}
	var count int64
	if identityErr == nil && identity.Present {
		if err := tx.Model(&GatewayCandidateGroupMember{}).
			Where("candidate_group_id = ? AND upstream_group_id = ?", key.CandidateGroupID, identity.ID).
			Count(&count).Error; err != nil {
			return err
		}
		if count > 0 {
			return nil
		}
	}
	// Historical members created before stable upstream identities have ID 0.
	// Keep that compatibility path canonical, but never let an old non-zero ID
	// stand in for a recreated generation.
	if err := tx.Model(&GatewayCandidateGroupMember{}).
		Where("candidate_group_id = ? AND channel_id = ? AND upstream_group_id = ? AND lower(btrim(group_name)) = ?", key.CandidateGroupID, key.ManualChannelID, 0, canonical).
		Count(&count).Error; err != nil {
		return err
	}
	if count == 0 {
		return fmt.Errorf("manual route %d/%s is not in candidate group %d", key.ManualChannelID, groupName, key.CandidateGroupID)
	}
	return nil
}

// DisableDownstreamKeyModelMapping leaves the chosen model fields intact for
// later administrator review while making future requests use their own model.
func (g *Gateway) DisableDownstreamKeyModelMapping(id uint) error {
	if id == 0 {
		return errors.New("downstream key id is required")
	}
	result := g.db.Model(&GatewayDownstreamKey{}).
		Where("id = ?", id).
		Update("model_mapping_strategy", "off")
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

// DisableDownstreamKeyModelMappingIfCurrent prevents an old request from
// turning off a newer administrator mapping. When a candidate-group snapshot
// is supplied, it also fences the membership version used to build the
// decision. The lock order matches SaveCandidateGroup: automatic-assignment
// advisory lock, candidate-group row lock, then downstream-key update.
// A zero-row result is reloaded: an already-disabled mapping is idempotent,
// while any newer executable mapping is preserved and reported as a conflict.
func (g *Gateway) DisableDownstreamKeyModelMappingIfCurrent(expected *GatewayDownstreamKey, candidateGroup *GatewayCandidateGroup) error {
	if g == nil || g.db == nil || expected == nil || expected.ID == 0 {
		return errors.New("downstream key mapping snapshot is required")
	}
	if candidateGroup == nil || candidateGroup.ID == 0 || candidateGroup.ID != expected.CandidateGroupID {
		return errors.New("candidate group mapping snapshot is invalid")
	}
	return g.db.Transaction(func(tx *gorm.DB) error {
		if err := lockAutomaticAssignmentMutationTx(tx); err != nil {
			return err
		}
		var currentGroup GatewayCandidateGroup
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&currentGroup, candidateGroup.ID).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return disableDownstreamKeyModelMappingIfAlreadyOffTx(tx, expected.ID)
			}
			return err
		}
		if currentGroup.MembershipVersion != candidateGroup.MembershipVersion {
			return disableDownstreamKeyModelMappingIfAlreadyOffTx(tx, expected.ID)
		}
		result := tx.Model(&GatewayDownstreamKey{}).
			Where(
				"id = ? AND model_mapping_strategy = ? AND model_mapping_model = ? AND model_mapping_reasoning_effort = ? AND candidate_group_id = ? AND candidate_target_mode = ? AND candidate_target_source = ? AND app = ? AND model_family = ? AND smart_routing = ?",
				expected.ID,
				expected.ModelMappingStrategy,
				expected.ModelMappingModel,
				expected.ModelMappingReasoningEffort,
				expected.CandidateGroupID,
				expected.CandidateTargetMode,
				expected.CandidateTargetSource,
				expected.App,
				expected.ModelFamily,
				expected.SmartRouting,
			).
			Update("model_mapping_strategy", "off")
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 1 {
			return nil
		}
		return disableDownstreamKeyModelMappingIfAlreadyOffTx(tx, expected.ID)
	})
}

func disableDownstreamKeyModelMappingIfAlreadyOffTx(tx *gorm.DB, id uint) error {
	if tx == nil || id == 0 {
		return errors.New("downstream key mapping snapshot is required")
	}
	var current GatewayDownstreamKey
	if err := tx.First(&current, id).Error; err != nil {
		return err
	}
	if strings.EqualFold(strings.TrimSpace(current.ModelMappingStrategy), "off") {
		return nil
	}
	return ErrDownstreamKeyModelMappingChanged
}

func (g *Gateway) ListKeyGroups() ([]GatewayKeyGroup, error) {
	groups := []GatewayKeyGroup{}
	if err := g.db.Order("name ASC").Find(&groups).Error; err != nil {
		return nil, err
	}
	return groups, nil
}

func (g *Gateway) SaveKeyGroup(group *GatewayKeyGroup) error {
	if group == nil {
		return errors.New("key group is required")
	}
	group.Name = strings.TrimSpace(group.Name)
	if group.Name == "" {
		return errors.New("key group name is required")
	}
	return g.db.Transaction(func(tx *gorm.DB) error {
		if err := ensureUniqueKeyGroupName(tx, group); err != nil {
			return err
		}
		if group.ID == 0 {
			return tx.Create(group).Error
		}
		result := tx.Model(&GatewayKeyGroup{}).Where("id = ?", group.ID).Update("name", group.Name)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return gorm.ErrRecordNotFound
		}
		return tx.First(group, group.ID).Error
	})
}

func (g *Gateway) DeleteKeyGroup(id uint) error {
	return g.db.Transaction(func(tx *gorm.DB) error {
		if err := lockKeyGroup(tx, id); err != nil {
			return err
		}
		var references int64
		if err := tx.Model(&GatewayDownstreamKey{}).Where("key_group_id = ?", id).Count(&references).Error; err != nil {
			return err
		}
		if references > 0 {
			return ErrKeyGroupInUse
		}
		return tx.Delete(&GatewayKeyGroup{}, id).Error
	})
}

func (g *Gateway) RecordUpstreamGroupChange(channelID uint, groupName, kind string, upstreamKeyID int64, upstreamKeyName string, at time.Time) error {
	groupName = strings.TrimSpace(groupName)
	kind = strings.TrimSpace(kind)
	if channelID == 0 || groupName == "" || (kind != "added" && kind != "removed") {
		return errors.New("invalid upstream group change")
	}
	if at.IsZero() {
		at = time.Now()
	}
	err := g.db.Transaction(func(tx *gorm.DB) error {
		if err := lockUpstreamGroupParentChannelTx(tx, channelID); err != nil {
			return err
		}
		if err := lockAutomaticAssignmentMutationTx(tx); err != nil {
			return err
		}
		if _, err := lockUpstreamRoutingHealthDomainAdvisoryTx(tx, channelID, groupName); err != nil {
			return err
		}
		identity, newGeneration, err := ensureUpstreamGroupIdentityTx(tx, channelID, groupName, kind == "added", at)
		if err != nil {
			return err
		}
		if kind == "removed" {
			if err := retireUpstreamGroupGenerationTx(tx, identity, at); err != nil {
				return err
			}
		}
		if kind == "added" && !newGeneration {
			recorded, err := upstreamGroupGenerationAddedChangeExistsTx(tx, identity.ID, identity.PresenceGeneration)
			if err != nil {
				return err
			}
			if recorded {
				return nil
			}
		}
		var existing GatewayUpstreamGroupChange
		err = tx.Where("upstream_group_id = ?", identity.ID).Order("id DESC").First(&existing).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if err == nil && existing.Kind == kind && !newGeneration {
			return nil
		}
		change := newUpstreamGroupChange(channelID, groupName, kind, upstreamKeyID, upstreamKeyName, at)
		change.UpstreamGroupID = &identity.ID
		generation := identity.PresenceGeneration
		change.PresenceGeneration = &generation
		if kind == "added" {
			change.AssignmentState = "awaiting_facts"
			change.AssignmentReasonCode = "models_not_synced_for_generation"
		}
		return tx.Create(change).Error
	})
	if err == nil && kind == "added" {
		g.ReevaluateUpstreamGroupAutomaticAssignment(channelID, groupName, true)
	}
	return err
}

func upstreamGroupGenerationAddedChangeExistsTx(tx *gorm.DB, upstreamGroupID uint, presenceGeneration uint64) (bool, error) {
	var existing GatewayUpstreamGroupChange
	err := tx.Select("id").
		Where(
			"upstream_group_id = ? AND presence_generation = ? AND kind = ?",
			upstreamGroupID, presenceGeneration, "added",
		).
		Order("id DESC").
		First(&existing).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	return err == nil, err
}

func retireUpstreamGroupGenerationTx(tx *gorm.DB, identity *GatewayUpstreamGroup, at time.Time) error {
	if identity == nil || identity.ID == 0 {
		return errors.New("upstream group identity is required")
	}
	var candidateGroupIDs []uint
	if err := tx.Model(&GatewayCandidateGroupMembershipClaim{}).
		Where("upstream_group_id = ? AND presence_generation <= ? AND active = ?", identity.ID, identity.PresenceGeneration, true).
		Distinct().Pluck("candidate_group_id", &candidateGroupIDs).Error; err != nil {
		return err
	}
	if err := tx.Model(&GatewayCandidateGroupMembershipClaim{}).
		Where("upstream_group_id = ? AND presence_generation <= ? AND active = ?", identity.ID, identity.PresenceGeneration, true).
		Updates(map[string]any{"active": false, "revoked_at": at}).Error; err != nil {
		return err
	}
	if err := tx.Where("upstream_group_id = ?", identity.ID).Delete(&GatewayCandidateGroupMember{}).Error; err != nil {
		return err
	}
	if len(candidateGroupIDs) > 0 {
		if err := tx.Model(&GatewayCandidateGroup{}).Where("id IN ?", candidateGroupIDs).
			UpdateColumn("membership_version", gorm.Expr("membership_version + 1")).Error; err != nil {
			return err
		}
	}
	return supersedePendingUpstreamGroupAssignmentsTx(tx, identity, "group_removed", at)
}

func supersedePendingUpstreamGroupAssignmentsTx(tx *gorm.DB, identity *GatewayUpstreamGroup, reason string, at time.Time) error {
	return tx.Model(&GatewayUpstreamGroupChange{}).
		Where("upstream_group_id = ? AND kind = ? AND status = ? AND presence_generation <= ?", identity.ID, "added", "pending", identity.PresenceGeneration).
		Updates(map[string]any{
			"status":                 "superseded",
			"assignment_state":       "superseded",
			"assignment_reason_code": reason,
			"resolved_at":            at,
			"evaluated_at":           at,
		}).Error
}

func upstreamGroupHasPersistentMembershipThroughCurrentGenerationTx(tx *gorm.DB, channelID uint, canonicalGroupName string) (bool, error) {
	if tx == nil || channelID == 0 || strings.TrimSpace(canonicalGroupName) == "" {
		return false, errors.New("upstream group identity is required")
	}
	var identity GatewayUpstreamGroup
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Select("id", "present", "presence_generation").
		Where("channel_id = ? AND canonical_group_name = ?", channelID, canonicalGroupName).
		First(&identity).Error
	if errors.Is(err, gorm.ErrRecordNotFound) || (err == nil && !identity.Present) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var count int64
	if err := tx.Model(&GatewayCandidateGroupMembershipClaim{}).
		Where(
			"upstream_group_id = ? AND presence_generation <= ? AND active = ? AND source_kind IN ?",
			identity.ID, identity.PresenceGeneration, true, []string{"manual", "override"},
		).
		Count(&count).Error; err != nil {
		return false, err
	}
	return count > 0, nil
}

// retireAutomaticUpstreamGroupGenerationTx drops only the materialized part
// of automatic assignment. Manual claims and persistent assignment overrides
// are administrator configuration and therefore survive a missing pricing
// snapshot. A real group removal still calls retireUpstreamGroupGenerationTx,
// which revokes every claim through the current generation and removes every
// member.
func retireAutomaticUpstreamGroupGenerationTx(tx *gorm.DB, identity *GatewayUpstreamGroup, at time.Time) error {
	if identity == nil || identity.ID == 0 {
		return errors.New("upstream group identity is required")
	}
	var claims []GatewayCandidateGroupMembershipClaim
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where(
			"upstream_group_id = ? AND presence_generation <= ? AND source_kind = ? AND active = ?",
			identity.ID, identity.PresenceGeneration, "auto_rule", true,
		).
		Order("candidate_group_id ASC, id ASC").
		Find(&claims).Error; err != nil {
		return err
	}
	if len(claims) == 0 {
		return supersedePendingUpstreamGroupAssignmentsTx(tx, identity, "group_removed", at)
	}
	candidateGroupIDs := make([]uint, 0, len(claims))
	seenCandidateGroups := make(map[uint]struct{}, len(claims))
	claimIDs := make([]uint, 0, len(claims))
	for _, claim := range claims {
		claimIDs = append(claimIDs, claim.ID)
		if _, seen := seenCandidateGroups[claim.CandidateGroupID]; seen {
			continue
		}
		seenCandidateGroups[claim.CandidateGroupID] = struct{}{}
		candidateGroupIDs = append(candidateGroupIDs, claim.CandidateGroupID)
	}
	if err := tx.Model(&GatewayCandidateGroupMembershipClaim{}).
		Where("id IN ?", claimIDs).
		Updates(map[string]any{"active": false, "revoked_at": at}).Error; err != nil {
		return err
	}
	for _, candidateGroupID := range candidateGroupIDs {
		var remaining int64
		if err := tx.Model(&GatewayCandidateGroupMembershipClaim{}).
			Where(
				"candidate_group_id = ? AND upstream_group_id = ? AND presence_generation <= ? AND active = ?",
				candidateGroupID, identity.ID, identity.PresenceGeneration, true,
			).
			Count(&remaining).Error; err != nil {
			return err
		}
		if remaining == 0 {
			if err := tx.Where("candidate_group_id = ? AND upstream_group_id = ?", candidateGroupID, identity.ID).
				Delete(&GatewayCandidateGroupMember{}).Error; err != nil {
				return err
			}
		}
	}
	if err := tx.Model(&GatewayCandidateGroup{}).Where("id IN ?", candidateGroupIDs).
		UpdateColumn("membership_version", gorm.Expr("membership_version + 1")).Error; err != nil {
		return err
	}
	return supersedePendingUpstreamGroupAssignmentsTx(tx, identity, "group_removed", at)
}

// BeginManagedUpstreamKeyRemoval records durable intent before the caller
// performs the remote deletion. A retry reuses an unfinished intent for the
// same remote key, while distinct orphan keys in one group retain separate
// audit rows.
func (g *Gateway) BeginManagedUpstreamKeyRemoval(channelID uint, groupName string, upstreamKeyID int64, upstreamKeyName string, observedAt time.Time, staleAfter time.Duration) (*GatewayUpstreamGroupChange, bool, error) {
	return nil, false, ErrLegacyManagedUpstreamKeyRemovalUnsupported
}

func (g *Gateway) CompleteManagedUpstreamKeyRemoval(id uint, removed bool, at time.Time) error {
	return ErrLegacyManagedUpstreamKeyRemovalUnsupported
}

func (g *Gateway) ReconcileManagedUpstreamKeyRemovals(channelID uint, presentRemoteIDs map[int64]struct{}, at time.Time) error {
	return ErrLegacyManagedUpstreamKeyRemovalUnsupported
}

func newUpstreamGroupChange(channelID uint, groupName, kind string, upstreamKeyID int64, upstreamKeyName string, at time.Time) *GatewayUpstreamGroupChange {
	change := &GatewayUpstreamGroupChange{
		ChannelID:       channelID,
		GroupName:       groupName,
		Kind:            kind,
		Status:          "pending",
		UpstreamKeyID:   upstreamKeyID,
		UpstreamKeyName: strings.TrimSpace(upstreamKeyName),
		DetectedAt:      at,
	}
	if kind == "removed" {
		change.Status = "acknowledged"
		change.ResolvedAt = &at
	}
	return change
}

func (g *Gateway) ListUpstreamGroupChanges(pendingOnly bool) ([]GatewayUpstreamGroupChange, error) {
	var changes []GatewayUpstreamGroupChange
	query := g.db.Order("detected_at DESC")
	if pendingOnly {
		if err := g.supersedeStalePendingUpstreamGroupChanges(time.Now().UTC()); err != nil {
			return nil, err
		}
		query = query.Where("status = ?", "pending")
	}
	if err := query.Find(&changes).Error; err != nil {
		return nil, err
	}
	return g.withUpstreamGroupChangeChannelNames(changes)
}

func (g *Gateway) ListUpstreamGroupChangesHistoryPage(page, pageSize int) ([]GatewayUpstreamGroupChange, int64, error) {
	if page < 1 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 50
	}
	if pageSize > 100 {
		pageSize = 100
	}
	query := g.db.Model(&GatewayUpstreamGroupChange{}).Where("status <> ?", "pending")
	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	changes := []GatewayUpstreamGroupChange{}
	if err := query.Order("detected_at DESC").Limit(pageSize).Offset((page - 1) * pageSize).Find(&changes).Error; err != nil {
		return nil, 0, err
	}
	changes, err := g.withUpstreamGroupChangeChannelNames(changes)
	return changes, total, err
}

func (g *Gateway) withUpstreamGroupChangeChannelNames(changes []GatewayUpstreamGroupChange) ([]GatewayUpstreamGroupChange, error) {
	if len(changes) == 0 {
		return changes, nil
	}
	ids := make([]uint, 0, len(changes))
	seen := make(map[uint]struct{}, len(changes))
	for _, change := range changes {
		if change.ChannelID == 0 {
			continue
		}
		if _, ok := seen[change.ChannelID]; ok {
			continue
		}
		seen[change.ChannelID] = struct{}{}
		ids = append(ids, change.ChannelID)
	}
	var channels []Channel
	if len(ids) > 0 {
		if err := g.db.Select("id", "name").Where("id IN ?", ids).Find(&channels).Error; err != nil {
			return nil, err
		}
	}
	names := make(map[uint]string, len(channels))
	for _, channel := range channels {
		names[channel.ID] = channel.Name
	}
	for i := range changes {
		changes[i].ChannelName = names[changes[i].ChannelID]
	}
	if err := g.projectUpstreamGroupChangeAssignment(changes); err != nil {
		return nil, err
	}
	return changes, nil
}

func (g *Gateway) ResolveUpstreamGroupChange(id uint, status string, at time.Time) error {
	return g.ResolveUpstreamGroupChangeWithContext(context.Background(), id, status, at)
}

func (g *Gateway) ResolveUpstreamGroupChangeWithContext(ctx context.Context, id uint, status string, at time.Time) error {
	if status != "assigned" && status != "ignored" && status != "acknowledged" && status != "superseded" {
		return errors.New("invalid upstream group change status")
	}
	if at.IsZero() {
		at = time.Now()
	}
	db := g.db
	if ctx != nil {
		db = db.WithContext(ctx)
	}
	var observed GatewayUpstreamGroupChange
	if err := db.Select("id", "channel_id").First(&observed, id).Error; err != nil {
		return err
	}
	return db.Transaction(func(tx *gorm.DB) error {
		if _, err := requireOrdinaryChannelWriteTx(tx, observed.ChannelID); err != nil {
			return err
		}
		result := tx.Model(&GatewayUpstreamGroupChange{}).
			Where("id = ? AND channel_id = ? AND status = ?", id, observed.ChannelID, "pending").
			Updates(map[string]any{"status": status, "resolved_at": at})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return gorm.ErrRecordNotFound
		}
		return nil
	})
}

func (g *Gateway) FindUpstreamGroupChange(id uint) (*GatewayUpstreamGroupChange, error) {
	var change GatewayUpstreamGroupChange
	if err := g.db.First(&change, id).Error; err != nil {
		return nil, err
	}
	return &change, nil
}

func (g *Gateway) AssignUpstreamGroupChange(id uint, candidateGroupIDs []uint, at time.Time) error {
	return g.AssignUpstreamGroupChangeAs(id, candidateGroupIDs, "administrator", at)
}

func (g *Gateway) AssignUpstreamGroupChangeAs(id uint, candidateGroupIDs []uint, actor string, at time.Time) error {
	return g.AssignUpstreamGroupChangeAsWithIdentityRevision(id, candidateGroupIDs, actor, at, 0)
}

func (g *Gateway) AssignUpstreamGroupChangeAsWithIdentityRevision(id uint, candidateGroupIDs []uint, actor string, at time.Time, expectedIdentityRevision uint64) error {
	return g.AssignUpstreamGroupChangeAsWithIdentityRevisionContext(context.Background(), id, candidateGroupIDs, actor, at, expectedIdentityRevision)
}

func (g *Gateway) AssignUpstreamGroupChangeAsWithIdentityRevisionContext(ctx context.Context, id uint, candidateGroupIDs []uint, actor string, at time.Time, expectedIdentityRevision uint64) error {
	candidateGroupIDs, err := normalizeCandidateGroupIDs(candidateGroupIDs)
	if err != nil {
		return err
	}
	actor = strings.TrimSpace(actor)
	if actor == "" {
		actor = "administrator"
	}
	if at.IsZero() {
		at = time.Now()
	}
	// Read the channel identity before entering the transaction so every
	// assignment path can acquire the parent channel row before the global
	// automatic-assignment advisory lock. ExecuteAutomaticAssignment uses the
	// same order; taking these locks in reverse order creates a deterministic
	// deadlock between the two administrative entry points.
	db := g.db
	if ctx != nil {
		db = db.WithContext(ctx)
	}
	var observedChange GatewayUpstreamGroupChange
	if err := db.Select("id", "channel_id").First(&observedChange, id).Error; err != nil {
		return err
	}
	if observedChange.ChannelID == 0 {
		return ErrUpstreamGroupGenerationChanged
	}
	superseded := false
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := lockUpstreamGroupParentChannelTx(tx, observedChange.ChannelID); err != nil {
			return err
		}
		if expectedIdentityRevision != 0 {
			if err := requireUpstreamAccountIdentityRevisionTx(tx, observedChange.ChannelID, expectedIdentityRevision); err != nil {
				return err
			}
		}
		if err := lockAutomaticAssignmentMutationTx(tx); err != nil {
			return err
		}
		var change GatewayUpstreamGroupChange
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ? AND status = ? AND kind = ?", id, "pending", "added").First(&change).Error; err != nil {
			if !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
			if currentErr := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&change, id).Error; currentErr != nil {
				return currentErr
			}
			if change.Kind == "added" && change.Status == "superseded" {
				return ErrUpstreamGroupChangeSuperseded
			}
			return err
		}
		if change.ChannelID != observedChange.ChannelID {
			return ErrUpstreamGroupGenerationChanged
		}
		supersede := func() error {
			superseded = true
			return tx.Model(&change).Updates(map[string]any{"status": "superseded", "resolved_at": at}).Error
		}
		for _, candidateGroupID := range candidateGroupIDs {
			if err := lockCandidateGroup(tx, candidateGroupID); err != nil {
				return err
			}
		}
		canonicalGroupName, err := lockUpstreamRoutingHealthDomainAdvisoryTx(tx, change.ChannelID, change.GroupName)
		if err != nil {
			return err
		}
		rateExists, err := lockUpstreamGroupRate(tx, change.ChannelID, canonicalGroupName)
		if err != nil {
			return err
		}
		var newest GatewayUpstreamGroupChange
		newestErr := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("channel_id = ? AND lower(btrim(group_name)) = ? AND id > ?", change.ChannelID, canonicalGroupName, change.ID).
			Order("id DESC").
			First(&newest).Error
		if newestErr != nil && !errors.Is(newestErr, gorm.ErrRecordNotFound) {
			return newestErr
		}
		if !rateExists || (newestErr == nil && newest.Kind == "removed") {
			return supersede()
		}
		var identity GatewayUpstreamGroup
		err = tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("channel_id = ? AND canonical_group_name = ?", change.ChannelID, canonicalGroupName).
			First(&identity).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return supersede()
		}
		if err != nil {
			return err
		}
		if !identity.Present || change.UpstreamGroupID == nil || change.PresenceGeneration == nil ||
			*change.UpstreamGroupID != identity.ID || *change.PresenceGeneration != identity.PresenceGeneration {
			return supersede()
		}
		if err := lockCurrentGenerationAssignmentKeyTx(tx, upstreamGroupGeneration(identity), &change); err != nil {
			return err
		}
		override, err := saveAssignmentOverrideTx(tx, identity.ID, identity.PresenceGeneration, "assign", candidateGroupIDs, "Assigned from upstream change review", actor, at)
		if err != nil {
			return err
		}
		if err := revokeMembershipClaimsTx(tx, "override", uint64(override.ID), at); err != nil {
			return err
		}
		state, err := automaticAssignmentStateTx(tx)
		if err != nil {
			return err
		}
		preview := &GatewayAutomaticAssignmentPreview{
			ChangeID: change.ID, UpstreamGroupID: identity.ID, ChannelID: identity.ChannelID, GroupName: change.GroupName,
			PresenceGeneration: identity.PresenceGeneration, ModelFamilyCodes: identity.ModelFamilyCodes, ModelIDs: identity.ModelIDs,
			GatewayApps: identity.GatewayApps, ChannelType: string(identity.ChannelType), ServiceTier: identity.ServiceTier,
			ServiceTierSource: identity.ServiceTierSource, State: "ready", ReasonCode: "manual_assign_override",
			SuggestedCandidateGroupIDs: candidateGroupIDs, MatchedRuleIDs: []uint{}, MissingFacts: identity.IncompleteReasons,
			RuleMatches: []GatewayAutomaticRuleMatch{}, EvaluatedAt: at,
		}
		if !identity.FactsComplete {
			preview.ReasonCode = "manual_override_incomplete_facts"
		}
		auditAction := "manual_assign"
		if preview.ReasonCode == "manual_override_incomplete_facts" {
			auditAction = preview.ReasonCode
		}
		if _, err := createAssignmentAuditTx(tx, &change, &identity, state, override.OverrideVersion, auditAction, "manual_assign", preview, actor); err != nil {
			return err
		}
		for _, candidateGroupID := range candidateGroupIDs {
			if err := ensureMembershipClaimTx(tx, candidateGroupID, &identity, change.GroupName, "override", uint64(override.ID), at); err != nil {
				return err
			}
		}
		return tx.Model(&change).Updates(map[string]any{
			"status": "assigned", "assignment_state": "assigned", "assignment_reason_code": preview.ReasonCode,
			"suggested_candidate_group_ids": gatewayJSONB(candidateGroupIDs), "evaluated_at": at, "resolved_at": at,
		}).Error
	})
	if err != nil {
		return err
	}
	if superseded {
		return ErrUpstreamGroupChangeSuperseded
	}
	return nil
}

func normalizeCandidateGroupIDs(ids []uint) ([]uint, error) {
	normalized := make([]uint, 0, len(ids))
	seen := make(map[uint]struct{}, len(ids))
	for _, id := range ids {
		if id == 0 {
			return nil, errors.New("candidate group is required")
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		normalized = append(normalized, id)
	}
	sort.Slice(normalized, func(i, j int) bool { return normalized[i] < normalized[j] })
	return normalized, nil
}

func lockCandidateGroup(tx *gorm.DB, id uint) error {
	if id == 0 {
		return nil
	}
	var group GatewayCandidateGroup
	return tx.Clauses(clause.Locking{Strength: "UPDATE"}).Select("id").First(&group, id).Error
}

func lockKeyGroup(tx *gorm.DB, id uint) error {
	if id == 0 {
		return nil
	}
	var group GatewayKeyGroup
	return tx.Clauses(clause.Locking{Strength: "UPDATE"}).Select("id").First(&group, id).Error
}

func ensureUniqueDownstreamKeyName(tx *gorm.DB, key *GatewayDownstreamKey) error {
	if key == nil {
		return errors.New("downstream key is required")
	}
	key.Name = strings.TrimSpace(key.Name)
	if key.Name == "" {
		return errors.New("key name is required")
	}
	var count int64
	if err := tx.Model(&GatewayDownstreamKey{}).
		Where("key_group_id = ? AND LOWER(name) = LOWER(?) AND id <> ?", key.KeyGroupID, key.Name, key.ID).
		Count(&count).Error; err != nil {
		return err
	}
	if count > 0 {
		return errors.New("key name already exists in this key group")
	}
	return nil
}

func ensureUniqueKeyGroupName(tx *gorm.DB, group *GatewayKeyGroup) error {
	var count int64
	if err := tx.Model(&GatewayKeyGroup{}).Where("LOWER(name) = LOWER(?) AND id <> ?", group.Name, group.ID).Count(&count).Error; err != nil {
		return err
	}
	if count > 0 {
		return errors.New("key group name already exists")
	}
	return nil
}

func ensureUniqueCandidateGroupName(tx *gorm.DB, group *GatewayCandidateGroup) error {
	var count int64
	if err := tx.Model(&GatewayCandidateGroup{}).Where("LOWER(name) = LOWER(?) AND id <> ?", group.Name, group.ID).Count(&count).Error; err != nil {
		return err
	}
	if count > 0 {
		return errors.New("candidate group name already exists")
	}
	return nil
}

func (g *Gateway) DeleteDownstreamKey(id uint) error {
	return g.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("downstream_key_id = ?", id).Delete(&GatewayRouteExclusion{}).Error; err != nil {
			return err
		}
		return tx.Delete(&GatewayDownstreamKey{}, id).Error
	})
}

func (g *Gateway) TouchDownstreamKey(id uint, at time.Time) error {
	return g.db.Model(&GatewayDownstreamKey{}).Where("id = ?", id).Update("last_used_at", at).Error
}

func (g *Gateway) FindUpstreamKey(channelID uint, groupName string) (*GatewayUpstreamKey, error) {
	canonical, err := canonicalUpstreamGroupName(g.db, groupName)
	if err != nil {
		return nil, err
	}
	var key GatewayUpstreamKey
	if err := g.db.Where("channel_id = ? AND lower(btrim(group_name)) = ? AND enabled = ?", channelID, canonical, true).
		Order("provisioning ASC, CASE WHEN btrim(api_key_cipher) <> '' THEN 1 ELSE 0 END DESC, models_synced_at DESC NULLS LAST, id DESC").First(&key).Error; err != nil {
		return nil, err
	}
	return &key, nil
}

func (g *Gateway) FindUpstreamKeyAny(channelID uint, groupName string) (*GatewayUpstreamKey, error) {
	canonical, err := canonicalUpstreamGroupName(g.db, groupName)
	if err != nil {
		return nil, err
	}
	var key GatewayUpstreamKey
	if err := g.db.Where("channel_id = ? AND lower(btrim(group_name)) = ?", channelID, canonical).
		Order(upstreamKeyRoutePreferenceOrder).First(&key).Error; err != nil {
		return nil, err
	}
	return &key, nil
}

// ListUpstreamKeys returns every cached upstream key for the requested
// channels, including disabled keys. Configuration views need the disabled
// rows to explain why a group is not currently eligible without issuing one
// query per rate snapshot.
func (g *Gateway) ListUpstreamKeys(channelIDs []uint) ([]GatewayUpstreamKey, error) {
	list := make([]GatewayUpstreamKey, 0)
	if len(channelIDs) == 0 {
		return list, nil
	}
	if err := g.db.
		Where("channel_id IN ?", channelIDs).
		Order("channel_id ASC").
		Order("group_name ASC").
		Find(&list).Error; err != nil {
		return nil, err
	}
	return list, nil
}

func (g *Gateway) UpsertUpstreamKey(key *GatewayUpstreamKey) error {
	return g.upsertUpstreamKey(key, 0)
}

// UpsertDirectUpstreamKey installs a direct credential only while its channel
// identity is current. An authentication failure for that same identity is a
// durable stop signal; background synchronization must not silently re-enable
// the rejected secret.
func (g *Gateway) UpsertDirectUpstreamKey(key *GatewayUpstreamKey, expectedChannelIdentityRevision uint64) error {
	if expectedChannelIdentityRevision == 0 {
		return ErrStaleChannelAccountIdentityRevision
	}
	return g.upsertUpstreamKey(key, expectedChannelIdentityRevision)
}

func (g *Gateway) upsertUpstreamKey(key *GatewayUpstreamKey, expectedChannelIdentityRevision uint64) error {
	if key == nil || key.ChannelID == 0 || strings.TrimSpace(key.GroupName) == "" {
		return errors.New("invalid upstream key")
	}
	key.GroupName = strings.TrimSpace(key.GroupName)
	requestedProvenance := strings.TrimSpace(key.OwnershipProvenance)
	provenance, err := normalizeGatewayUpstreamKeyOwnershipProvenance(requestedProvenance)
	if err != nil {
		return err
	}
	return g.db.Transaction(func(tx *gorm.DB) error {
		if expectedChannelIdentityRevision > 0 {
			if err := requireUpstreamAccountIdentityRevisionTx(tx, key.ChannelID, expectedChannelIdentityRevision); err != nil {
				return err
			}
		}
		domain, err := lockUpstreamRoutingHealthDomainTx(tx, key.ChannelID, key.GroupName, time.Now())
		if errors.Is(err, gorm.ErrRecordNotFound) {
			key.OwnershipProvenance = provenance
			key.LatencyProbeEnabled = true
			return tx.Create(key).Error
		}
		if err != nil {
			return err
		}
		existing := domain.Key
		sameDirectCredentialRevision := existing.DirectCredentialID != nil && key.DirectCredentialID != nil &&
			*existing.DirectCredentialID == *key.DirectCredentialID &&
			existing.DirectCredentialRevision == key.DirectCredentialRevision
		if expectedChannelIdentityRevision > 0 && !existing.Enabled &&
			existing.AuthenticationInvalidatedChannelIdentityRevision == expectedChannelIdentityRevision &&
			sameDirectCredentialRevision {
			return ErrGatewayUpstreamKeyAuthenticationInvalidated
		}
		if requestedProvenance == "" {
			provenance, err = normalizeGatewayUpstreamKeyOwnershipProvenance(existing.OwnershipProvenance)
			if err != nil {
				return err
			}
		}
		updates := map[string]any{
			"upstream_key_id":            key.UpstreamKeyID,
			"upstream_key_name":          key.UpstreamKeyName,
			"ownership_provenance":       provenance,
			"provisioning":               key.Provisioning,
			"provisioning_started_at":    key.ProvisioningStartedAt,
			"api_key_cipher":             key.APIKeyCipher,
			"api_key_prefix":             key.APIKeyPrefix,
			"group_ratio":                key.GroupRatio,
			"direct_credential_id":       key.DirectCredentialID,
			"direct_credential_revision": key.DirectCredentialRevision,
			"direct_base_url":            key.DirectBaseURL,
			"enabled":                    key.Enabled,
			"last_error":                 key.LastError,
			"authentication_invalidated_channel_identity_revision": 0,
		}
		if key.ModelFamilies != "" {
			updates["model_families"] = key.ModelFamilies
		}
		if key.ModelIDs != "" {
			updates["model_ids"] = key.ModelIDs
		}
		if key.LatencyProbeModel != "" {
			updates["latency_probe_model"] = key.LatencyProbeModel
		}
		if key.ModelsSyncedAt != nil {
			updates["models_synced_at"] = key.ModelsSyncedAt
		}
		if key.ModelsError != "" {
			updates["models_error"] = key.ModelsError
		}
		return tx.Model(&GatewayUpstreamKey{}).Where("id = ?", existing.ID).Updates(updates).Error
	})
}

// ClaimUpstreamKeyProvisioning serializes remote key creation across service
// instances and process restarts. fresh is true only when no earlier remote
// create attempt can exist; a stale claimant must first search by the persisted
// provisioning name before issuing another POST.
func (g *Gateway) ClaimUpstreamKeyProvisioning(channelID uint, groupName, provisioningName string, groupRatio float64, now time.Time, staleAfter time.Duration) (key *GatewayUpstreamKey, claimed bool, fresh bool, err error) {
	return g.claimUpstreamKeyProvisioning(channelID, groupName, provisioningName, groupRatio, now, staleAfter, "")
}

// ClaimUpstreamKeyProvisioningForRebuild atomically replaces the durable
// rebuild marker with a provisioning claim. No ordinary request can treat the
// marker as disposable provisioning evidence.
func (g *Gateway) ClaimUpstreamKeyProvisioningForRebuild(channelID uint, groupName, provisioningName string, groupRatio float64, now time.Time, staleAfter time.Duration, rebuildPendingMessage string) (key *GatewayUpstreamKey, claimed bool, fresh bool, err error) {
	return g.claimUpstreamKeyProvisioning(channelID, groupName, provisioningName, groupRatio, now, staleAfter, strings.TrimSpace(rebuildPendingMessage))
}

func (g *Gateway) claimUpstreamKeyProvisioning(channelID uint, groupName, provisioningName string, groupRatio float64, now time.Time, staleAfter time.Duration, rebuildPendingMessage string) (key *GatewayUpstreamKey, claimed bool, fresh bool, err error) {
	groupName = strings.TrimSpace(groupName)
	provisioningName = strings.TrimSpace(provisioningName)
	if channelID == 0 || groupName == "" || provisioningName == "" {
		return nil, false, false, errors.New("invalid upstream key provisioning claim")
	}
	if now.IsZero() {
		now = time.Now()
	}
	err = g.db.Transaction(func(tx *gorm.DB) error {
		domain, findErr := lockUpstreamRoutingHealthDomainTx(tx, channelID, groupName, now)
		if errors.Is(findErr, gorm.ErrRecordNotFound) {
			current := GatewayUpstreamKey{
				ChannelID: channelID, GroupName: groupName, GroupRatio: groupRatio,
				UpstreamKeyName: provisioningName, OwnershipProvenance: GatewayUpstreamKeyProvenanceProvisioningClaim,
				Provisioning: true, ProvisioningStartedAt: &now,
				APIKeyCipher: "", APIKeyPrefix: "", Enabled: false, LatencyProbeEnabled: true,
			}
			if createErr := tx.Create(&current).Error; createErr != nil {
				return createErr
			}
			if updateErr := tx.Model(&current).Updates(map[string]any{
				"enabled": false, "provisioning": true, "provisioning_started_at": now,
			}).Error; updateErr != nil {
				return updateErr
			}
			current.Enabled = false
			key, claimed, fresh = &current, true, true
			return nil
		}
		if findErr != nil {
			return findErr
		}
		current := *domain.Key
		if current.Enabled && strings.TrimSpace(current.APIKeyCipher) != "" {
			key = &current
			return nil
		}
		if current.Provisioning {
			key = &current
			if current.ProvisioningStartedAt != nil && staleAfter > 0 && !current.ProvisioningStartedAt.Add(staleAfter).After(now) {
				if err := tx.Model(&current).Updates(map[string]any{"provisioning_started_at": now, "last_error": ""}).Error; err != nil {
					return err
				}
				current.ProvisioningStartedAt = &now
				current.LastError = ""
				claimed = true
			}
			return nil
		}
		rebuildPending := rebuildPendingMessage != "" && !current.Enabled && !current.Provisioning &&
			strings.TrimSpace(current.LastError) == rebuildPendingMessage
		if upstreamKeyHasProvisioningEvidence(&current) && !rebuildPending {
			key = &current
			return nil
		}
		updates := map[string]any{
			"upstream_key_id": 0, "upstream_key_name": provisioningName,
			"ownership_provenance": GatewayUpstreamKeyProvenanceProvisioningClaim,
			"provisioning":         true, "provisioning_started_at": now,
			"api_key_cipher": "", "api_key_prefix": "", "group_ratio": groupRatio,
			"enabled": false, "last_error": "",
		}
		if err := tx.Model(&current).Updates(updates).Error; err != nil {
			return err
		}
		current.UpstreamKeyID = 0
		current.UpstreamKeyName = provisioningName
		current.OwnershipProvenance = GatewayUpstreamKeyProvenanceProvisioningClaim
		current.Provisioning = true
		current.ProvisioningStartedAt = &now
		current.APIKeyCipher = ""
		current.APIKeyPrefix = ""
		current.GroupRatio = groupRatio
		current.Enabled = false
		current.LastError = ""
		key, claimed, fresh = &current, true, true
		return nil
	})
	return key, claimed, fresh, err
}

func upstreamKeyHasProvisioningEvidence(key *GatewayUpstreamKey) bool {
	if key == nil {
		return false
	}
	return key.ProvisioningStartedAt != nil || key.UpstreamKeyID > 0 ||
		strings.TrimSpace(key.UpstreamKeyName) != "" || strings.TrimSpace(key.APIKeyCipher) != "" ||
		strings.TrimSpace(key.APIKeyPrefix) != ""
}

// upstreamKeyRoutePreferenceOrder must stay aligned with
// gateway.upstreamKeyRoutePreference. A case-only duplicate can otherwise
// make routing select one row while health updates another.
const upstreamKeyRoutePreferenceOrder = "enabled DESC, provisioning ASC, CASE WHEN btrim(api_key_cipher) <> '' THEN 1 ELSE 0 END DESC, models_synced_at DESC NULLS LAST, id DESC"

type lockedUpstreamRoutingHealthDomain struct {
	ChannelID          uint
	CanonicalGroupName string
	Key                *GatewayUpstreamKey
	HealthFence        *GatewayRoutingHealthFence
}

// lockUpstreamRoutingHealthDomainAdvisoryTx serializes all routing-health
// writes for one database-canonical upstream group. The lock intentionally
// does not include the presence generation: a retired generation must wait
// behind an in-flight write before it can be checked and rejected.
func lockUpstreamRoutingHealthDomainAdvisoryTx(tx *gorm.DB, channelID uint, groupName string) (string, error) {
	if channelID == 0 {
		return "", errors.New("channel id is required")
	}
	canonical, err := canonicalUpstreamGroupName(tx, groupName)
	if err != nil {
		return "", err
	}
	if tx.Dialector != nil && tx.Dialector.Name() == string(DBDriverPostgres) {
		lockName := fmt.Sprintf("bridgeops:routing-health:%d:%s", channelID, canonical)
		if err := tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", lockName).Error; err != nil {
			return "", err
		}
	}
	return canonical, nil
}

func lockCanonicalUpstreamKeyRowsTx(tx *gorm.DB, channelID uint, canonicalGroupName string) ([]GatewayUpstreamKey, error) {
	keys := make([]GatewayUpstreamKey, 0)
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("channel_id = ? AND lower(btrim(group_name)) = ?", channelID, canonicalGroupName).
		Order(upstreamKeyRoutePreferenceOrder).
		Find(&keys).Error
	return keys, err
}

func cloneRoutingHealthTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func routingHealthStatePreferred(candidate, current GatewayUpstreamKey) bool {
	if candidate.RoutingIsolatedAt != nil || current.RoutingIsolatedAt != nil {
		if candidate.RoutingIsolatedAt == nil {
			return false
		}
		if current.RoutingIsolatedAt == nil {
			return true
		}
		if !candidate.RoutingIsolatedAt.Equal(*current.RoutingIsolatedAt) {
			return candidate.RoutingIsolatedAt.After(*current.RoutingIsolatedAt)
		}
	}
	if candidate.LastRoutingFailureAt != nil || current.LastRoutingFailureAt != nil {
		if candidate.LastRoutingFailureAt == nil {
			return false
		}
		if current.LastRoutingFailureAt == nil {
			return true
		}
		if !candidate.LastRoutingFailureAt.Equal(*current.LastRoutingFailureAt) {
			return candidate.LastRoutingFailureAt.After(*current.LastRoutingFailureAt)
		}
	}
	if candidate.RoutingCooldownUntil != nil || current.RoutingCooldownUntil != nil {
		if candidate.RoutingCooldownUntil == nil {
			return false
		}
		if current.RoutingCooldownUntil == nil {
			return true
		}
		if !candidate.RoutingCooldownUntil.Equal(*current.RoutingCooldownUntil) {
			return candidate.RoutingCooldownUntil.After(*current.RoutingCooldownUntil)
		}
	}
	if candidate.RoutingFailureCount != current.RoutingFailureCount {
		return candidate.RoutingFailureCount > current.RoutingFailureCount
	}
	return false
}

func mergedUpstreamRoutingHealthSummary(keys []GatewayUpstreamKey, at time.Time) GatewayUpstreamKey {
	var summary GatewayUpstreamKey
	if at.IsZero() {
		at = time.Now()
	}
	var latest *GatewayUpstreamKey
	var latestSuccess *GatewayUpstreamKey
	var activeManual *GatewayUpstreamKey
	var isolated *GatewayUpstreamKey
	for index := range keys {
		key := &keys[index]
		if key.LastUsedAt != nil && (summary.LastUsedAt == nil || key.LastUsedAt.After(*summary.LastUsedAt)) {
			summary.LastUsedAt = cloneRoutingHealthTime(key.LastUsedAt)
			latestSuccess = key
		}
		if key.LastRoutingFailureKind == GatewayRoutingFailureManualCooldown && key.RoutingCooldownUntil != nil && at.Before(*key.RoutingCooldownUntil) {
			if activeManual == nil || routingHealthStatePreferred(*key, *activeManual) {
				activeManual = key
			}
		}
		if key.RoutingIsolatedAt != nil && (isolated == nil || key.RoutingIsolatedAt.After(*isolated.RoutingIsolatedAt)) {
			isolated = key
		}
		if latest == nil || routingHealthStatePreferred(*key, *latest) {
			latest = key
		}
	}
	owner := latest
	if activeManual != nil {
		owner = activeManual
	}
	if owner == nil {
		if latestSuccess != nil {
			summary.RoutingHealthUpstreamGroupID = latestSuccess.RoutingHealthUpstreamGroupID
			summary.RoutingHealthPresenceGeneration = latestSuccess.RoutingHealthPresenceGeneration
			summary.RoutingHealthChannelIdentityRevision = latestSuccess.RoutingHealthChannelIdentityRevision
		}
		return summary
	}
	summary.RoutingHealthUpstreamGroupID = owner.RoutingHealthUpstreamGroupID
	summary.RoutingHealthPresenceGeneration = owner.RoutingHealthPresenceGeneration
	summary.RoutingHealthChannelIdentityRevision = owner.RoutingHealthChannelIdentityRevision
	// LastUsedAt is the durable success watermark for adaptive routing health.
	// A legacy case-variant row can retain an older failure summary even after
	// another variant recorded a newer success; never let that stale summary be
	// folded onto the canonical survivor. Active administrator cooldowns are an
	// explicit override and intentionally survive ordinary successes.
	adaptiveIsolation := isolated != nil
	if activeManual == nil && !adaptiveIsolation && summary.LastUsedAt != nil &&
		(owner.LastRoutingFailureAt == nil || !owner.LastRoutingFailureAt.After(*summary.LastUsedAt)) {
		if latestSuccess != nil {
			summary.RoutingHealthUpstreamGroupID = latestSuccess.RoutingHealthUpstreamGroupID
			summary.RoutingHealthPresenceGeneration = latestSuccess.RoutingHealthPresenceGeneration
			summary.RoutingHealthChannelIdentityRevision = latestSuccess.RoutingHealthChannelIdentityRevision
		}
		return summary
	}
	summary.LastRoutingFailureError = owner.LastRoutingFailureError
	summary.LastRoutingFailureKind = owner.LastRoutingFailureKind
	summary.LastRoutingFailureStatus = owner.LastRoutingFailureStatus
	summary.LastRoutingFailureAt = cloneRoutingHealthTime(owner.LastRoutingFailureAt)
	summary.RoutingFailureCount = owner.RoutingFailureCount
	summary.RoutingCooldownUntil = cloneRoutingHealthTime(owner.RoutingCooldownUntil)
	if isolated != nil {
		summary.RoutingIsolatedAt = cloneRoutingHealthTime(isolated.RoutingIsolatedAt)
		if summary.RoutingFailureCount < GatewayAdaptiveRoutingFailureThreshold {
			summary.RoutingFailureCount = GatewayAdaptiveRoutingFailureThreshold
		}
	}
	return summary
}

func routingHealthSummaryUpdates(summary GatewayUpstreamKey) map[string]any {
	return map[string]any{
		"last_used_at":                             summary.LastUsedAt,
		"last_routing_failure_error":               summary.LastRoutingFailureError,
		"last_routing_failure_kind":                summary.LastRoutingFailureKind,
		"last_routing_failure_status":              summary.LastRoutingFailureStatus,
		"last_routing_failure_at":                  summary.LastRoutingFailureAt,
		"routing_failure_count":                    summary.RoutingFailureCount,
		"routing_cooldown_until":                   summary.RoutingCooldownUntil,
		"routing_isolated_at":                      summary.RoutingIsolatedAt,
		"routing_health_upstream_group_id":         summary.RoutingHealthUpstreamGroupID,
		"routing_health_presence_generation":       summary.RoutingHealthPresenceGeneration,
		"routing_health_channel_identity_revision": summary.RoutingHealthChannelIdentityRevision,
	}
}

func clearRoutingHealthSummaryUpdates() map[string]any {
	return map[string]any{
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
	}
}

func gatewayRoutingHealthMatchesFence(key GatewayUpstreamKey, fence GatewayRoutingHealthFence) bool {
	return fence.MatchesUpstreamKey(&key)
}

func bindGatewayRoutingHealthSummary(summary *GatewayUpstreamKey, fence GatewayRoutingHealthFence) {
	if summary == nil || !fence.Valid() {
		return
	}
	summary.RoutingHealthUpstreamGroupID = fence.UpstreamGroupID
	summary.RoutingHealthPresenceGeneration = fence.PresenceGeneration
	summary.RoutingHealthChannelIdentityRevision = fence.ChannelIdentityRevision
}

func clearGatewayRoutingHealthSummary(summary *GatewayUpstreamKey) {
	if summary == nil {
		return
	}
	summary.LastUsedAt = nil
	summary.LastRoutingFailureError = ""
	summary.LastRoutingFailureKind = ""
	summary.LastRoutingFailureStatus = 0
	summary.LastRoutingFailureAt = nil
	summary.RoutingFailureCount = 0
	summary.RoutingCooldownUntil = nil
	summary.RoutingIsolatedAt = nil
	summary.RoutingHealthUpstreamGroupID = 0
	summary.RoutingHealthPresenceGeneration = 0
	summary.RoutingHealthChannelIdentityRevision = 0
}

func gatewayRoutingFailureFactsQuery(tx *gorm.DB, channelID uint, canonicalGroupName string, fence *GatewayRoutingHealthFence) *gorm.DB {
	query := tx.Where("channel_id = ? AND lower(btrim(group_name)) = ?", channelID, canonicalGroupName)
	if fence != nil && fence.Valid() {
		query = query.Where(
			"upstream_group_id = ? AND presence_generation = ? AND channel_identity_revision = ?",
			fence.UpstreamGroupID, fence.PresenceGeneration, fence.ChannelIdentityRevision,
		)
	}
	return query
}

func routingFailureIsolationThresholdAtTx(tx *gorm.DB, channelID uint, canonicalGroupName string, after *time.Time, windowStart, latestAt time.Time, fence *GatewayRoutingHealthFence) (*time.Time, error) {
	query := gatewayRoutingFailureFactsQuery(tx.Model(&GatewayRoutingFailure{}), channelID, canonicalGroupName, fence).
		Where("occurred_at >= ? AND occurred_at <= ?", windowStart, latestAt)
	if after != nil {
		query = query.Where("occurred_at > ?", *after)
	}
	var failure GatewayRoutingFailure
	err := query.Select("occurred_at").Order("occurred_at ASC").Order("id ASC").
		Offset(GatewayAdaptiveRoutingFailureThreshold - 1).Limit(1).Take(&failure).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	thresholdAt := failure.OccurredAt.UTC().Truncate(time.Microsecond)
	return &thresholdAt, nil
}

func reconcileMergedRoutingFailureFactsTx(tx *gorm.DB, channelID uint, canonicalGroupName string, at time.Time, summary *GatewayUpstreamKey, fence *GatewayRoutingHealthFence) error {
	if summary == nil || summary.LastUsedAt == nil {
		return nil
	}
	// Once a route is isolated, ordinary inference success is latency evidence
	// only. This check must run before deleting failure facts because an outbox
	// success can be delivered more than once after the health write commits.
	if summary.RoutingIsolatedAt != nil && !summary.LastUsedAt.Before(*summary.RoutingIsolatedAt) {
		return nil
	}
	if err := gatewayRoutingFailureFactsQuery(tx, channelID, canonicalGroupName, fence).
		Where("occurred_at <= ?", *summary.LastUsedAt).
		Delete(&GatewayRoutingFailure{}).Error; err != nil {
		return err
	}
	var remaining struct {
		LatestAt *time.Time `gorm:"column:latest_at"`
	}
	if err := gatewayRoutingFailureFactsQuery(tx.Model(&GatewayRoutingFailure{}), channelID, canonicalGroupName, fence).
		Select("MAX(occurred_at) AS latest_at").Scan(&remaining).Error; err != nil {
		return err
	}
	if remaining.LatestAt == nil {
		summary.RoutingFailureCount = 0
		summary.RoutingIsolatedAt = nil
		return nil
	}
	windowStart := remaining.LatestAt.Add(-10 * time.Minute)
	if err := gatewayRoutingFailureFactsQuery(tx, channelID, canonicalGroupName, fence).
		Where("occurred_at < ?", windowStart).
		Delete(&GatewayRoutingFailure{}).Error; err != nil {
		return err
	}
	var count int64
	if err := gatewayRoutingFailureFactsQuery(tx.Model(&GatewayRoutingFailure{}), channelID, canonicalGroupName, fence).
		Where("occurred_at >= ? AND occurred_at <= ?", windowStart, *remaining.LatestAt).
		Count(&count).Error; err != nil {
		return err
	}
	summary.RoutingFailureCount = int(count)
	if summary.RoutingFailureCount < GatewayAdaptiveRoutingFailureThreshold {
		summary.RoutingIsolatedAt = nil
	} else {
		thresholdAt, err := routingFailureIsolationThresholdAtTx(
			tx, channelID, canonicalGroupName, nil, windowStart, *remaining.LatestAt, fence,
		)
		if err != nil {
			return err
		}
		summary.RoutingIsolatedAt = thresholdAt
	}
	activeManualCooldown := summary.LastRoutingFailureKind == GatewayRoutingFailureManualCooldown &&
		summary.RoutingCooldownUntil != nil && at.Before(*summary.RoutingCooldownUntil)
	if activeManualCooldown {
		return nil
	}
	summary.LastRoutingFailureAt = cloneRoutingHealthTime(remaining.LatestAt)
	summary.RoutingCooldownUntil = nil
	return nil
}

// lockUpstreamRoutingHealthDomainTx locks every case variant before choosing
// the same preferred key that candidate selection uses. It folds any legacy
// per-row summary into that survivor and clears all non-survivor summaries so
// a later preference change cannot resurrect stale health state.
func lockUpstreamRoutingHealthDomainTx(tx *gorm.DB, channelID uint, groupName string, at time.Time, generations ...UpstreamGroupGeneration) (*lockedUpstreamRoutingHealthDomain, error) {
	var expected *UpstreamGroupGeneration
	if len(generations) > 0 {
		generation := generations[0]
		expected = &generation
		channelID = generation.ChannelID
		groupName = generation.CanonicalGroupName
	}
	return lockUpstreamRoutingHealthDomainWithFenceTx(tx, channelID, groupName, at, expected, nil)
}

func lockUpstreamRoutingHealthDomainForFenceTx(tx *gorm.DB, fence GatewayRoutingHealthFence, at time.Time) (*lockedUpstreamRoutingHealthDomain, error) {
	if !fence.Valid() {
		return nil, ErrUpstreamGroupGenerationChanged
	}
	generation := fence.UpstreamGeneration()
	return lockUpstreamRoutingHealthDomainWithFenceTx(tx, fence.ChannelID, fence.CanonicalGroupName, at, &generation, &fence)
}

func lockUpstreamRoutingHealthDomainWithFenceTx(tx *gorm.DB, channelID uint, groupName string, at time.Time, expected *UpstreamGroupGeneration, healthFence *GatewayRoutingHealthFence) (*lockedUpstreamRoutingHealthDomain, error) {
	channel, err := requireOrdinaryChannelWriteTx(tx, channelID)
	if err != nil {
		return nil, err
	}
	if healthFence != nil && channel.UpstreamAccountIdentityRevision != healthFence.ChannelIdentityRevision {
		return nil, ErrStaleChannelAccountIdentityRevision
	}
	canonical, err := lockUpstreamRoutingHealthDomainAdvisoryTx(tx, channelID, groupName)
	if err != nil {
		return nil, err
	}
	if expected != nil {
		if canonical != expected.CanonicalGroupName {
			return nil, ErrUpstreamGroupGenerationChanged
		}
		if err := requireUpstreamGroupGenerationTx(tx, *expected); err != nil {
			return nil, err
		}
	}
	keys, err := lockCanonicalUpstreamKeyRowsTx(tx, channelID, canonical)
	if err != nil {
		return nil, err
	}
	domain := &lockedUpstreamRoutingHealthDomain{ChannelID: channelID, CanonicalGroupName: canonical, HealthFence: healthFence}
	if len(keys) == 0 {
		return domain, gorm.ErrRecordNotFound
	}
	if healthFence != nil {
		for index := range keys {
			if !gatewayRoutingHealthMatchesFence(keys[index], *healthFence) {
				clearGatewayRoutingHealthSummary(&keys[index])
			}
		}
	}
	summary := mergedUpstreamRoutingHealthSummary(keys, at)
	if healthFence != nil {
		bindGatewayRoutingHealthSummary(&summary, *healthFence)
	}
	// Facts at or before the canonical success watermark belong to an older
	// health epoch. Reconcile the retained count and cooldown immediately while
	// the domain lock is held; callers must not briefly observe a stale count.
	if err := reconcileMergedRoutingFailureFactsTx(tx, channelID, canonical, at, &summary, healthFence); err != nil {
		return nil, err
	}
	survivor := keys[0]
	if err := tx.Model(&GatewayUpstreamKey{}).Where("id = ?", survivor.ID).Updates(routingHealthSummaryUpdates(summary)).Error; err != nil {
		return nil, err
	}
	if len(keys) > 1 {
		ids := make([]uint, 0, len(keys)-1)
		for _, key := range keys[1:] {
			ids = append(ids, key.ID)
		}
		if err := tx.Model(&GatewayUpstreamKey{}).Where("id IN ?", ids).Updates(clearRoutingHealthSummaryUpdates()).Error; err != nil {
			return nil, err
		}
	}
	survivor.LastUsedAt = cloneRoutingHealthTime(summary.LastUsedAt)
	survivor.LastRoutingFailureError = summary.LastRoutingFailureError
	survivor.LastRoutingFailureKind = summary.LastRoutingFailureKind
	survivor.LastRoutingFailureStatus = summary.LastRoutingFailureStatus
	survivor.LastRoutingFailureAt = cloneRoutingHealthTime(summary.LastRoutingFailureAt)
	survivor.RoutingFailureCount = summary.RoutingFailureCount
	survivor.RoutingCooldownUntil = cloneRoutingHealthTime(summary.RoutingCooldownUntil)
	survivor.RoutingIsolatedAt = cloneRoutingHealthTime(summary.RoutingIsolatedAt)
	survivor.RoutingHealthUpstreamGroupID = summary.RoutingHealthUpstreamGroupID
	survivor.RoutingHealthPresenceGeneration = summary.RoutingHealthPresenceGeneration
	survivor.RoutingHealthChannelIdentityRevision = summary.RoutingHealthChannelIdentityRevision
	domain.Key = &survivor
	return domain, nil
}

func lockAllUpstreamRoutingHealthDomainsForChannelTx(tx *gorm.DB, channelID uint, at time.Time) error {
	if channelID == 0 {
		return errors.New("channel id is required")
	}
	type canonicalRoute struct {
		CanonicalGroupName string `gorm:"column:canonical_group_name"`
	}
	routes := make([]canonicalRoute, 0)
	if err := tx.Raw(`
		SELECT canonical_group_name
		FROM (
			SELECT DISTINCT lower(btrim(group_name)) AS canonical_group_name
			FROM gateway_upstream_keys
			WHERE channel_id = ? AND btrim(group_name) <> ''
			UNION
			SELECT DISTINCT lower(btrim(group_name)) AS canonical_group_name
			FROM gateway_routing_failures
			WHERE channel_id = ? AND btrim(group_name) <> ''
		) AS routing_health_domains
		ORDER BY canonical_group_name`, channelID, channelID).Scan(&routes).Error; err != nil {
		return err
	}
	for _, route := range routes {
		_, err := lockUpstreamRoutingHealthDomainTx(tx, channelID, route.CanonicalGroupName, at)
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
	}
	return nil
}

func lockUpstreamKeyByRawThenCanonicalTx(tx *gorm.DB, channelID uint, groupName string) (*GatewayUpstreamKey, error) {
	canonical, err := canonicalUpstreamGroupName(tx, groupName)
	if err != nil {
		return nil, err
	}
	keys, err := lockCanonicalUpstreamKeyRowsTx(tx, channelID, canonical)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, gorm.ErrRecordNotFound
	}
	return &keys[0], nil
}

func preferredUpstreamKeyIDQuery(db *gorm.DB, channelID uint, groupName string) *gorm.DB {
	groupName = strings.TrimSpace(groupName)
	return db.Model(&GatewayUpstreamKey{}).
		Select("id").
		Where("channel_id = ? AND lower(btrim(group_name)) = lower(btrim(?))", channelID, groupName).
		Order(upstreamKeyRoutePreferenceOrder).
		Limit(1)
}

// lockModelSyncUpstreamGenerationTx establishes the identity-before-key lock
// order shared with generation-fenced routing health writes. An explicit
// current generation may update model facts for a fixed route whose rate
// snapshot disappeared; legacy callers still require a current rate and must
// never turn a retired identity present again.
func lockModelSyncUpstreamGenerationTx(tx *gorm.DB, channelID uint, groupName string, at time.Time, generations ...UpstreamGroupGeneration) (string, *UpstreamGroupGeneration, error) {
	if len(generations) > 0 {
		expected := generations[0]
		channelID = expected.ChannelID
		groupName = expected.CanonicalGroupName
		canonical, err := lockUpstreamRoutingHealthDomainAdvisoryTx(tx, channelID, groupName)
		if err != nil {
			return "", nil, err
		}
		if canonical != expected.CanonicalGroupName {
			return "", nil, ErrUpstreamGroupGenerationChanged
		}
		if err := requireUpstreamGroupGenerationTx(tx, expected); err != nil {
			return "", nil, err
		}
		return canonical, &expected, nil
	}
	canonical, err := lockUpstreamRoutingHealthDomainAdvisoryTx(tx, channelID, groupName)
	if err != nil {
		return "", nil, err
	}
	hasCurrentRate, err := upstreamGroupHasCurrentRateTx(tx, channelID, canonical)
	if err != nil {
		return "", nil, err
	}
	if !hasCurrentRate {
		return "", nil, ErrUpstreamGroupGenerationChanged
	}
	var identity GatewayUpstreamGroup
	err = tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("channel_id = ? AND canonical_group_name = ?", channelID, canonical).
		First(&identity).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		created, _, createErr := ensureUpstreamGroupIdentityTx(tx, channelID, canonical, true, at)
		if createErr != nil {
			return "", nil, createErr
		}
		generation := upstreamGroupGeneration(*created)
		return canonical, &generation, nil
	}
	if err != nil {
		return "", nil, err
	}
	if !identity.Present {
		return "", nil, ErrUpstreamGroupGenerationChanged
	}
	generation := upstreamGroupGeneration(identity)
	return canonical, &generation, nil
}

func (g *Gateway) UpdateUpstreamKeyModelFamilies(channelID uint, groupName string, families string, modelIDs string, syncedAt time.Time, syncErr string, generations ...UpstreamGroupGeneration) error {
	return g.updateUpstreamKeyModelFamilies(channelID, groupName, families, modelIDs, syncedAt, syncErr, nil, generations...)
}

func (g *Gateway) UpdateUpstreamKeyModelFamiliesForIdentity(fence GatewayModelFactFence, families string, modelIDs string, syncedAt time.Time, syncErr string) error {
	if !fence.Valid() {
		return ErrUpstreamGroupGenerationChanged
	}
	return g.updateUpstreamKeyModelFamilies(fence.ChannelID, fence.CanonicalGroupName, families, modelIDs, syncedAt, syncErr, &fence)
}

func (g *Gateway) updateUpstreamKeyModelFamilies(channelID uint, groupName string, families string, modelIDs string, syncedAt time.Time, syncErr string, fence *GatewayModelFactFence, generations ...UpstreamGroupGeneration) error {
	groupName = strings.TrimSpace(groupName)
	if syncedAt.IsZero() {
		syncedAt = time.Now()
	}
	err := g.db.Transaction(func(tx *gorm.DB) error {
		if err := lockUpstreamGroupParentChannelTx(tx, channelID); err != nil {
			return err
		}
		if err := lockAutomaticAssignmentMutationTx(tx); err != nil {
			return err
		}
		canonical, modelFence, err := lockGatewayModelFactWriteTx(tx, channelID, groupName, syncedAt, fence, generations...)
		if err != nil {
			return err
		}
		groupName = canonical
		channelID = modelFence.ChannelID
		key, err := lockUpstreamKeyByRawThenCanonicalTx(tx, channelID, groupName)
		if err != nil {
			return err
		}
		if key.ModelsSyncedAt != nil && syncedAt.Before(*key.ModelsSyncedAt) {
			return nil
		}
		groupName = key.GroupName
		forceGenerationRefresh := !modelFence.MatchesUpstreamKey(key)
		heartbeat := syncedAt.Add(-upstreamModelSyncHeartbeat)
		update := tx.Model(key)
		if !forceGenerationRefresh {
			update = update.Where("COALESCE(model_families, '') <> ? OR COALESCE(model_ids, '') <> ? OR COALESCE(models_error, '') <> ? OR models_synced_at IS NULL OR models_synced_at < ?", families, modelIDs, syncErr, heartbeat)
		}
		result := update.Updates(map[string]any{
			"model_families":                        families,
			"model_ids":                             modelIDs,
			"models_synced_at":                      syncedAt,
			"models_error":                          syncErr,
			"model_facts_upstream_group_id":         modelFence.UpstreamGroupID,
			"model_facts_presence_generation":       modelFence.PresenceGeneration,
			"model_facts_channel_identity_revision": modelFence.ChannelIdentityRevision,
		})
		if result.Error != nil {
			return result.Error
		}
		_, err = updateUpstreamGroupFactsIfChannelExistsTx(tx, channelID, groupName, strings.Split(modelIDs, ","), syncedAt, syncErr, modelFence)
		return err
	})
	if err == nil {
		g.ReevaluateUpstreamGroupAutomaticAssignment(channelID, groupName, true)
	}
	return err
}

func (g *Gateway) UpdateLatencyProbeModel(channelID uint, groupName string, modelName string) error {
	return g.db.Transaction(func(tx *gorm.DB) error {
		domain, err := lockUpstreamRoutingHealthDomainTx(tx, channelID, groupName, time.Now())
		if err != nil {
			return err
		}
		return tx.Model(&GatewayUpstreamKey{}).
			Where("id = ?", domain.Key.ID).
			Update("latency_probe_model", strings.TrimSpace(modelName)).Error
	})
}

func (g *Gateway) UpdateLatencyProbeEnabled(channelID uint, groupName string, enabled bool) error {
	groupName = strings.TrimSpace(groupName)
	return g.db.Transaction(func(tx *gorm.DB) error {
		domain, domainErr := lockUpstreamRoutingHealthDomainTx(tx, channelID, groupName, time.Now())
		if domainErr == nil {
			return tx.Model(&GatewayUpstreamKey{}).
				Where("id = ?", domain.Key.ID).
				Update("latency_probe_enabled", enabled).Error
		}
		if !errors.Is(domainErr, gorm.ErrRecordNotFound) {
			return domainErr
		}
		if domain == nil || domain.CanonicalGroupName == "" {
			return gorm.ErrRecordNotFound
		}
		// Keep the same advisory-before-rate order as model synchronization.
		// A probe toggle may create a placeholder, so it needs the rate lock only
		// after the canonical routing domain is already serialized.
		rateExists, err := lockUpstreamGroupRate(tx, channelID, domain.CanonicalGroupName)
		if err != nil {
			return err
		}
		if !rateExists {
			return gorm.ErrRecordNotFound
		}
		now := time.Now()
		return tx.Model(&GatewayUpstreamKey{}).Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "channel_id"}, {Name: "group_name"}},
			DoUpdates: clause.Assignments(map[string]any{
				"latency_probe_enabled": enabled,
				"updated_at":            now,
			}),
		}).Create(map[string]any{
			"channel_id": channelID, "group_name": domain.CanonicalGroupName,
			"api_key_cipher": "", "api_key_prefix": "",
			"enabled": false, "latency_probe_enabled": enabled,
			"created_at": now, "updated_at": now,
		}).Error
	})
}

func (g *Gateway) SetUpstreamKeyModelsError(channelID uint, groupName string, syncedAt time.Time, syncErr string, generations ...UpstreamGroupGeneration) error {
	return g.setUpstreamKeyModelsError(channelID, groupName, syncedAt, syncErr, nil, generations...)
}

func (g *Gateway) SetUpstreamKeyModelsErrorForIdentity(fence GatewayModelFactFence, syncedAt time.Time, syncErr string) error {
	if !fence.Valid() {
		return ErrUpstreamGroupGenerationChanged
	}
	return g.setUpstreamKeyModelsError(fence.ChannelID, fence.CanonicalGroupName, syncedAt, syncErr, &fence)
}

func (g *Gateway) setUpstreamKeyModelsError(channelID uint, groupName string, syncedAt time.Time, syncErr string, fence *GatewayModelFactFence, generations ...UpstreamGroupGeneration) error {
	groupName = strings.TrimSpace(groupName)
	if syncedAt.IsZero() {
		syncedAt = time.Now()
	}
	err := g.db.Transaction(func(tx *gorm.DB) error {
		if err := lockUpstreamGroupParentChannelTx(tx, channelID); err != nil {
			return err
		}
		if err := lockAutomaticAssignmentMutationTx(tx); err != nil {
			return err
		}
		canonical, modelFence, err := lockGatewayModelFactWriteTx(tx, channelID, groupName, syncedAt, fence, generations...)
		if err != nil {
			return err
		}
		groupName = canonical
		channelID = modelFence.ChannelID
		key, err := lockUpstreamKeyByRawThenCanonicalTx(tx, channelID, groupName)
		if err != nil {
			return err
		}
		groupName = key.GroupName
		sameIdentity := modelFence.MatchesUpstreamKey(key)
		if sameIdentity && key.ModelsSyncedAt != nil && syncedAt.Before(*key.ModelsSyncedAt) {
			return nil
		}
		heartbeat := syncedAt.Add(-upstreamModelErrorHeartbeat)
		if !sameIdentity || key.ModelsError != syncErr || key.ModelsSyncedAt == nil || key.ModelsSyncedAt.Before(heartbeat) {
			updates := map[string]any{
				"models_synced_at":                      syncedAt,
				"models_error":                          syncErr,
				"model_facts_upstream_group_id":         modelFence.UpstreamGroupID,
				"model_facts_presence_generation":       modelFence.PresenceGeneration,
				"model_facts_channel_identity_revision": modelFence.ChannelIdentityRevision,
			}
			if !sameIdentity {
				updates["model_families"] = ""
				updates["model_ids"] = ""
				key.ModelIDs = ""
			}
			if err := tx.Model(key).Updates(updates).Error; err != nil {
				return err
			}
		}
		_, err = updateUpstreamGroupFactsIfChannelExistsTx(tx, channelID, groupName, strings.Split(key.ModelIDs, ","), syncedAt, syncErr, modelFence)
		return err
	})
	if err == nil {
		g.ReevaluateUpstreamGroupAutomaticAssignment(channelID, groupName, true)
	}
	return err
}

func (g *Gateway) RecordUpstreamKeyModelsError(channelID uint, groupName string, syncedAt time.Time, syncErr string, generations ...UpstreamGroupGeneration) error {
	return g.recordUpstreamKeyModelsError(channelID, groupName, syncedAt, syncErr, nil, generations...)
}

func (g *Gateway) RecordUpstreamKeyModelsErrorForIdentity(fence GatewayModelFactFence, syncedAt time.Time, syncErr string) error {
	if !fence.Valid() {
		return ErrUpstreamGroupGenerationChanged
	}
	return g.recordUpstreamKeyModelsError(fence.ChannelID, fence.CanonicalGroupName, syncedAt, syncErr, &fence)
}

func (g *Gateway) recordUpstreamKeyModelsError(channelID uint, groupName string, syncedAt time.Time, syncErr string, fence *GatewayModelFactFence, generations ...UpstreamGroupGeneration) error {
	groupName = strings.TrimSpace(groupName)
	if syncedAt.IsZero() {
		syncedAt = time.Now()
	}
	err := g.db.Transaction(func(tx *gorm.DB) error {
		if err := lockUpstreamGroupParentChannelTx(tx, channelID); err != nil {
			return err
		}
		if err := lockAutomaticAssignmentMutationTx(tx); err != nil {
			return err
		}
		canonical, modelFence, err := lockGatewayModelFactWriteTx(tx, channelID, groupName, syncedAt, fence, generations...)
		if err != nil {
			return err
		}
		groupName = canonical
		channelID = modelFence.ChannelID
		key, err := lockUpstreamKeyByRawThenCanonicalTx(tx, channelID, groupName)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			key = &GatewayUpstreamKey{
				ChannelID: channelID, GroupName: groupName, Enabled: false,
				LatencyProbeEnabled: true, ModelsSyncedAt: &syncedAt, ModelsError: syncErr,
				ModelFactsUpstreamGroupID: modelFence.UpstreamGroupID, ModelFactsPresenceGeneration: modelFence.PresenceGeneration,
				ModelFactsChannelIdentityRevision: modelFence.ChannelIdentityRevision,
			}
			if err := tx.Model(&GatewayUpstreamKey{}).Create(map[string]any{
				"channel_id": channelID, "group_name": groupName, "enabled": false,
				"latency_probe_enabled": true, "models_synced_at": syncedAt, "models_error": syncErr,
				"model_facts_upstream_group_id": modelFence.UpstreamGroupID, "model_facts_presence_generation": modelFence.PresenceGeneration,
				"model_facts_channel_identity_revision": modelFence.ChannelIdentityRevision,
				"api_key_cipher":                        "", "api_key_prefix": "", "created_at": time.Now(), "updated_at": time.Now(),
			}).Error; err != nil {
				return err
			}
			if err := tx.Where("channel_id = ? AND group_name = ?", channelID, groupName).First(key).Error; err != nil {
				return err
			}
		} else if err != nil {
			return err
		} else {
			groupName = key.GroupName
			sameIdentity := modelFence.MatchesUpstreamKey(key)
			if sameIdentity && key.ModelsSyncedAt != nil && syncedAt.Before(*key.ModelsSyncedAt) {
				return nil
			}
			heartbeat := syncedAt.Add(-upstreamModelErrorHeartbeat)
			if !sameIdentity || key.ModelsError != syncErr || key.ModelsSyncedAt == nil || key.ModelsSyncedAt.Before(heartbeat) {
				updates := map[string]any{
					"models_synced_at": syncedAt, "models_error": syncErr,
					"model_facts_upstream_group_id":         modelFence.UpstreamGroupID,
					"model_facts_presence_generation":       modelFence.PresenceGeneration,
					"model_facts_channel_identity_revision": modelFence.ChannelIdentityRevision,
				}
				if !sameIdentity {
					updates["model_families"] = ""
					updates["model_ids"] = ""
					key.ModelIDs = ""
				}
				if err := tx.Model(key).Updates(updates).Error; err != nil {
					return err
				}
			}
		}
		_, err = updateUpstreamGroupFactsIfChannelExistsTx(tx, channelID, groupName, strings.Split(key.ModelIDs, ","), syncedAt, syncErr, modelFence)
		return err
	})
	if err == nil {
		g.ReevaluateUpstreamGroupAutomaticAssignment(channelID, groupName, true)
	}
	return err
}

func (g *Gateway) TouchUpstreamKey(channelID uint, groupName string, at time.Time, generations ...UpstreamGroupGeneration) error {
	return g.touchUpstreamKey(channelID, groupName, at, nil, generations...)
}

func (g *Gateway) TouchUpstreamKeyForRoutingHealth(fence GatewayRoutingHealthFence, at time.Time) error {
	if !fence.Valid() {
		return ErrUpstreamGroupGenerationChanged
	}
	return g.touchUpstreamKey(fence.ChannelID, fence.CanonicalGroupName, at, &fence)
}

func (g *Gateway) touchUpstreamKey(channelID uint, groupName string, at time.Time, fence *GatewayRoutingHealthFence, generations ...UpstreamGroupGeneration) error {
	if at.IsZero() {
		at = time.Now()
	}
	at = at.UTC().Truncate(time.Microsecond)
	groupName = strings.TrimSpace(groupName)
	return g.db.Transaction(func(tx *gorm.DB) error {
		var domain *lockedUpstreamRoutingHealthDomain
		var err error
		if fence != nil {
			domain, err = lockUpstreamRoutingHealthDomainForFenceTx(tx, *fence, at)
		} else {
			domain, err = lockUpstreamRoutingHealthDomainTx(tx, channelID, groupName, at, generations...)
		}
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		channelID = domain.ChannelID
		key := domain.Key
		canonicalGroupName := domain.CanonicalGroupName
		protectedIsolation := key.RoutingIsolatedAt != nil && !at.Before(*key.RoutingIsolatedAt)
		if !protectedIsolation {
			if err := gatewayRoutingFailureFactsQuery(tx, channelID, canonicalGroupName, domain.HealthFence).
				Where("occurred_at <= ?", at).
				Delete(&GatewayRoutingFailure{}).Error; err != nil {
				return err
			}
		}
		var remaining struct {
			LatestAt *time.Time `gorm:"column:latest_at"`
		}
		if err := gatewayRoutingFailureFactsQuery(tx.Model(&GatewayRoutingFailure{}), channelID, canonicalGroupName, domain.HealthFence).
			Select("MAX(occurred_at) AS latest_at").Scan(&remaining).Error; err != nil {
			return err
		}
		var remainingCount int64
		if remaining.LatestAt != nil {
			windowStart := remaining.LatestAt.Add(-10 * time.Minute)
			if err := gatewayRoutingFailureFactsQuery(tx, channelID, canonicalGroupName, domain.HealthFence).
				Where("occurred_at < ?", windowStart).
				Delete(&GatewayRoutingFailure{}).Error; err != nil {
				return err
			}
			if err := gatewayRoutingFailureFactsQuery(tx.Model(&GatewayRoutingFailure{}), channelID, canonicalGroupName, domain.HealthFence).
				Where("occurred_at >= ? AND occurred_at <= ?", windowStart, *remaining.LatestAt).
				Count(&remainingCount).Error; err != nil {
				return err
			}
		}

		nextIsolationAt := cloneRoutingHealthTime(key.RoutingIsolatedAt)
		if !protectedIsolation {
			if remainingCount >= GatewayAdaptiveRoutingFailureThreshold && remaining.LatestAt != nil {
				thresholdAt, thresholdErr := routingFailureIsolationThresholdAtTx(
					tx, channelID, canonicalGroupName, nil,
					remaining.LatestAt.Add(-10*time.Minute), *remaining.LatestAt, domain.HealthFence,
				)
				if thresholdErr != nil {
					return thresholdErr
				}
				nextIsolationAt = thresholdAt
			} else {
				nextIsolationAt = nil
			}
		}
		if nextIsolationAt != nil && remainingCount < GatewayAdaptiveRoutingFailureThreshold {
			remainingCount = GatewayAdaptiveRoutingFailureThreshold
		}
		activeManualCooldown := key.LastRoutingFailureKind == GatewayRoutingFailureManualCooldown &&
			key.RoutingCooldownUntil != nil && at.Before(*key.RoutingCooldownUntil)
		updates := map[string]any{
			"last_used_at":          gorm.Expr("CASE WHEN last_used_at IS NULL OR last_used_at < ? THEN ? ELSE last_used_at END", at, at),
			"routing_failure_count": remainingCount,
			"routing_isolated_at":   nextIsolationAt,
		}
		if domain.HealthFence != nil {
			updates["routing_health_upstream_group_id"] = domain.HealthFence.UpstreamGroupID
			updates["routing_health_presence_generation"] = domain.HealthFence.PresenceGeneration
			updates["routing_health_channel_identity_revision"] = domain.HealthFence.ChannelIdentityRevision
		}
		switch {
		case activeManualCooldown:
			updates["last_routing_failure_error"] = key.LastRoutingFailureError
			updates["last_routing_failure_kind"] = key.LastRoutingFailureKind
			updates["last_routing_failure_status"] = key.LastRoutingFailureStatus
			updates["last_routing_failure_at"] = key.LastRoutingFailureAt
			updates["routing_cooldown_until"] = key.RoutingCooldownUntil
		case protectedIsolation:
			// A real request that was already in flight when the route crossed the
			// isolation threshold is useful latency evidence, but it cannot bypass
			// the required three-success recovery probe.
			updates["last_routing_failure_error"] = key.LastRoutingFailureError
			updates["last_routing_failure_kind"] = key.LastRoutingFailureKind
			updates["last_routing_failure_status"] = key.LastRoutingFailureStatus
			updates["last_routing_failure_at"] = key.LastRoutingFailureAt
			updates["routing_failure_count"] = key.RoutingFailureCount
			updates["routing_cooldown_until"] = key.RoutingCooldownUntil
		case remainingCount > 0 && remaining.LatestAt != nil && key.LastRoutingFailureAt != nil && key.LastRoutingFailureAt.After(at):
			updates["last_routing_failure_error"] = key.LastRoutingFailureError
			updates["last_routing_failure_kind"] = key.LastRoutingFailureKind
			updates["last_routing_failure_status"] = key.LastRoutingFailureStatus
			updates["last_routing_failure_at"] = *remaining.LatestAt
			updates["routing_cooldown_until"] = nil
		default:
			updates["last_routing_failure_error"] = ""
			updates["last_routing_failure_kind"] = ""
			updates["last_routing_failure_status"] = 0
			updates["last_routing_failure_at"] = nil
			updates["routing_failure_count"] = 0
			updates["routing_cooldown_until"] = nil
			updates["routing_isolated_at"] = nil
		}
		return tx.Model(&GatewayUpstreamKey{}).Where("id = ?", key.ID).Updates(updates).Error
	})
}

// latestConfirmedLatencyProbeRecoveryWatermarkTx returns the newest completed
// three-sample probe recovery for this exact group generation. It is a
// recovery fact for adaptive health, deliberately distinct from LastUsedAt:
// probe traffic is not a real downstream inference success. The separate
// watermark survives later failed/mixed probe outcomes.
//
// Callers must already hold the canonical routing-health advisory lock and
// validate generation, just like the writer below.
func latestConfirmedLatencyProbeRecoveryWatermarkTx(tx *gorm.DB, generation UpstreamGroupGeneration, fences ...*GatewayRoutingHealthFence) (*time.Time, error) {
	if !generation.Valid() {
		return nil, nil
	}
	var state GatewayLatencyProbeState
	query := tx.Table("gateway_latency_probe_states AS states").Select("states.last_confirmed_recovery_at").
		Joins("JOIN channels ON channels.id = states.channel_id AND channels.upstream_account_identity_revision = states.channel_identity_revision").
		Where(
			"states.channel_id = ? AND states.group_name = ? AND states.upstream_group_id = ? AND states.presence_generation = ? AND states.last_confirmed_recovery_at IS NOT NULL",
			generation.ChannelID, generation.CanonicalGroupName, generation.ID, generation.PresenceGeneration,
		)
	if len(fences) > 0 && fences[0] != nil {
		query = query.Where("states.channel_identity_revision = ?", fences[0].ChannelIdentityRevision)
	}
	err := query.Order("states.last_confirmed_recovery_at DESC, states.id DESC").First(&state).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	watermark := state.LastConfirmedRecoveryAt.UTC().Truncate(time.Microsecond)
	return &watermark, nil
}

// RecoverUpstreamKeyRoutingFromConfirmedLatencyProbe records a confirmed
// three-sample latency-probe recovery and clears only adaptive failures at or
// before that recovery event. It never writes LastUsedAt: only an actual
// downstream inference success may advance the cross-key success watermark.
func (g *Gateway) RecoverUpstreamKeyRoutingFromConfirmedLatencyProbe(channelID uint, groupName string, app GatewayApp, at time.Time, generations ...UpstreamGroupGeneration) error {
	if len(generations) != 1 || !generations[0].Valid() {
		return ErrUpstreamGroupGenerationChanged
	}
	app, err := normalizeLatencyProbeStateApp(app)
	if err != nil {
		return err
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	at = at.UTC().Truncate(time.Microsecond)
	return g.recoverUpstreamKeyRoutingFromConfirmedLatencyProbe(channelID, groupName, app, at, nil, generations[0])
}

func (g *Gateway) RecoverUpstreamKeyRoutingFromConfirmedLatencyProbeForRoutingHealth(fence GatewayRoutingHealthFence, app GatewayApp, at time.Time) error {
	if !fence.Valid() {
		return ErrUpstreamGroupGenerationChanged
	}
	return g.recoverUpstreamKeyRoutingFromConfirmedLatencyProbe(fence.ChannelID, fence.CanonicalGroupName, app, at, &fence, fence.UpstreamGeneration())
}

func (g *Gateway) recoverUpstreamKeyRoutingFromConfirmedLatencyProbe(channelID uint, groupName string, app GatewayApp, at time.Time, fence *GatewayRoutingHealthFence, generation UpstreamGroupGeneration) error {
	app, err := normalizeLatencyProbeStateApp(app)
	if err != nil {
		return err
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	at = at.UTC().Truncate(time.Microsecond)
	return g.db.Transaction(func(tx *gorm.DB) error {
		var domain *lockedUpstreamRoutingHealthDomain
		var err error
		if fence != nil {
			domain, err = lockUpstreamRoutingHealthDomainForFenceTx(tx, *fence, at)
		} else {
			domain, err = lockUpstreamRoutingHealthDomainTx(tx, channelID, groupName, at, generation)
		}
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}

		// Persist the recovery fact in the same transaction as health clearing.
		// RunLatencyProbes later upserts scheduling fields with its captured
		// clock; the storage upsert keeps this newer completion watermark.
		recoveryAt := at
		state := &GatewayLatencyProbeState{
			ChannelID:               domain.ChannelID,
			ChannelIdentityRevision: 0,
			GroupName:               domain.CanonicalGroupName,
			GatewayApp:              app,
			UpstreamGroupID:         uint64(generation.ID),
			PresenceGeneration:      generation.PresenceGeneration,
			LastProbeAt:             &recoveryAt,
			ConfirmationOutcome:     "success",
			ConfirmationCount:       3,
			LastConfirmedAt:         &recoveryAt,
			LastConfirmedRecoveryAt: &recoveryAt,
			UpdatedAt:               recoveryAt,
		}
		if domain.HealthFence != nil {
			state.ChannelIdentityRevision = domain.HealthFence.ChannelIdentityRevision
		} else {
			var channel Channel
			if err := tx.Select("upstream_account_identity_revision").First(&channel, domain.ChannelID).Error; err != nil {
				return err
			}
			state.ChannelIdentityRevision = channel.UpstreamAccountIdentityRevision
		}
		if err := upsertLatencyProbeStateTx(tx, state); err != nil {
			return err
		}

		if err := gatewayRoutingFailureFactsQuery(tx, domain.ChannelID, domain.CanonicalGroupName, domain.HealthFence).
			Where("occurred_at <= ?", at).
			Delete(&GatewayRoutingFailure{}).Error; err != nil {
			return err
		}
		latestFailureAt, remainingIsolationAt, remainingFailureCount, err := routingFailureFactsAfterWatermarkTx(tx, domain.ChannelID, domain.CanonicalGroupName, at, domain.HealthFence)
		if err != nil {
			return err
		}
		key := domain.Key
		activeManualCooldown := key.LastRoutingFailureKind == GatewayRoutingFailureManualCooldown &&
			key.RoutingCooldownUntil != nil && at.Before(*key.RoutingCooldownUntil)
		updates := map[string]any{
			"routing_failure_count": remainingFailureCount,
			"routing_isolated_at":   remainingIsolationAt,
		}
		switch {
		case activeManualCooldown:
			updates["last_routing_failure_error"] = key.LastRoutingFailureError
			updates["last_routing_failure_kind"] = key.LastRoutingFailureKind
			updates["last_routing_failure_status"] = key.LastRoutingFailureStatus
			updates["last_routing_failure_at"] = key.LastRoutingFailureAt
			updates["routing_cooldown_until"] = key.RoutingCooldownUntil
		case latestFailureAt != nil:
			lastKind := "transport"
			lastStatus := 0
			lastMessage := "adaptive routing failure after probe recovery"
			if key.LastRoutingFailureAt != nil && key.LastRoutingFailureAt.Equal(*latestFailureAt) &&
				strings.TrimSpace(key.LastRoutingFailureKind) != "" && key.LastRoutingFailureKind != GatewayRoutingFailureManualCooldown {
				lastKind = key.LastRoutingFailureKind
				lastStatus = key.LastRoutingFailureStatus
				lastMessage = key.LastRoutingFailureError
			}
			updates["last_routing_failure_error"] = lastMessage
			updates["last_routing_failure_kind"] = lastKind
			updates["last_routing_failure_status"] = lastStatus
			updates["last_routing_failure_at"] = *latestFailureAt
			updates["routing_cooldown_until"] = nil
		default:
			updates["last_routing_failure_error"] = ""
			updates["last_routing_failure_kind"] = ""
			updates["last_routing_failure_status"] = 0
			updates["last_routing_failure_at"] = nil
			updates["routing_failure_count"] = 0
			updates["routing_cooldown_until"] = nil
		}
		return tx.Model(&GatewayUpstreamKey{}).Where("id = ?", key.ID).Updates(updates).Error
	})
}

func (g *Gateway) SetUpstreamKeyError(channelID uint, groupName string, msg string) error {
	preferred := preferredUpstreamKeyIDQuery(g.db, channelID, groupName)
	return g.db.Model(&GatewayUpstreamKey{}).Where("id = (?)", preferred).Update("last_error", msg).Error
}

// RecordUpstreamKeyRoutingFailure stores routing-only health state separately
// from generic key/model-sync errors. Candidate selection can use this state
// without treating unrelated administrative failures as an outage.
func (g *Gateway) RecordUpstreamKeyRoutingFailure(channelID uint, groupName, kind string, status int, msg string, at time.Time, generations ...UpstreamGroupGeneration) error {
	return g.recordUpstreamKeyRoutingFailure(channelID, groupName, kind, status, msg, at, 1, nil, generations...)
}

func (g *Gateway) RecordUpstreamKeyRoutingFailureForRoutingHealth(fence GatewayRoutingHealthFence, kind string, status int, msg string, at time.Time) error {
	if !fence.Valid() {
		return ErrUpstreamGroupGenerationChanged
	}
	return g.recordUpstreamKeyRoutingFailure(fence.ChannelID, fence.CanonicalGroupName, kind, status, msg, at, 1, &fence)
}

// RecordUpstreamKeyRoutingFailuresForRoutingHealth atomically records a
// confirmed batch of consecutive physical failures for one routing identity.
func (g *Gateway) RecordUpstreamKeyRoutingFailuresForRoutingHealth(fence GatewayRoutingHealthFence, kind string, status int, msg string, at time.Time, occurrences int) error {
	if !fence.Valid() {
		return ErrUpstreamGroupGenerationChanged
	}
	if occurrences <= 0 {
		return errors.New("routing failure occurrence count must be positive")
	}
	return g.recordUpstreamKeyRoutingFailure(fence.ChannelID, fence.CanonicalGroupName, kind, status, msg, at, occurrences, &fence)
}

func (g *Gateway) recordUpstreamKeyRoutingFailure(channelID uint, groupName, kind string, status int, msg string, at time.Time, occurrences int, fence *GatewayRoutingHealthFence, generations ...UpstreamGroupGeneration) error {
	if occurrences <= 0 {
		return errors.New("routing failure occurrence count must be positive")
	}
	if at.IsZero() {
		at = time.Now()
	}
	at = at.UTC().Truncate(time.Microsecond)
	groupName = strings.TrimSpace(groupName)
	return g.db.Transaction(func(tx *gorm.DB) error {
		var domain *lockedUpstreamRoutingHealthDomain
		var err error
		if fence != nil {
			domain, err = lockUpstreamRoutingHealthDomainForFenceTx(tx, *fence, at)
		} else {
			domain, err = lockUpstreamRoutingHealthDomainTx(tx, channelID, groupName, at, generations...)
		}
		if err != nil {
			return err
		}
		channelID = domain.ChannelID
		key := domain.Key
		canonicalGroupName := domain.CanonicalGroupName
		if key.LastUsedAt != nil && !at.After(*key.LastUsedAt) {
			return nil
		}
		if len(generations) > 0 || domain.HealthFence != nil {
			var generation UpstreamGroupGeneration
			if domain.HealthFence != nil {
				generation = domain.HealthFence.UpstreamGeneration()
			} else {
				generation = generations[0]
			}
			if watermark, watermarkErr := latestGatewayRoutingEnableWatermarkTx(tx, generation, domain.HealthFence); watermarkErr != nil {
				return watermarkErr
			} else if watermark != nil && !at.After(*watermark) {
				// The failure occurred before a generation-bound operator enable
				// action but reached this lock afterwards. Do not recreate the
				// adaptive cooldown that action explicitly cleared.
				return nil
			}
			if watermark, watermarkErr := latestConfirmedLatencyProbeRecoveryWatermarkTx(tx, generation, domain.HealthFence); watermarkErr != nil {
				return watermarkErr
			} else if watermark != nil && !at.After(*watermark) {
				// A confirmed latency probe has its own recovery fact. It may
				// retire older adaptive failures without pretending to be a real
				// downstream inference success or touching LastUsedAt.
				return nil
			}
		}
		latestAt := at
		lastKind, lastStatus, lastMessage := strings.TrimSpace(kind), status, msg
		if key.LastRoutingFailureAt != nil && key.LastRoutingFailureAt.After(latestAt) {
			latestAt = *key.LastRoutingFailureAt
			lastKind, lastStatus, lastMessage = key.LastRoutingFailureKind, key.LastRoutingFailureStatus, key.LastRoutingFailureError
		}
		windowStart := latestAt.Add(-10 * time.Minute)
		if err := gatewayRoutingFailureFactsQuery(tx, channelID, canonicalGroupName, domain.HealthFence).
			Where("occurred_at < ?", windowStart).
			Delete(&GatewayRoutingFailure{}).Error; err != nil {
			return err
		}
		failures := make([]GatewayRoutingFailure, occurrences)
		for index := range failures {
			failures[index] = GatewayRoutingFailure{ChannelID: channelID, GroupName: canonicalGroupName, OccurredAt: at}
			if domain.HealthFence != nil {
				failures[index].UpstreamGroupID = domain.HealthFence.UpstreamGroupID
				failures[index].PresenceGeneration = domain.HealthFence.PresenceGeneration
				failures[index].ChannelIdentityRevision = domain.HealthFence.ChannelIdentityRevision
			}
		}
		if err := tx.Create(&failures).Error; err != nil {
			return err
		}
		var count int64
		if err := gatewayRoutingFailureFactsQuery(tx.Model(&GatewayRoutingFailure{}), channelID, canonicalGroupName, domain.HealthFence).
			Where("occurred_at >= ? AND occurred_at <= ?", windowStart, latestAt).
			Count(&count).Error; err != nil {
			return err
		}
		routingFailureCount := int(count)
		isolatedAt := cloneRoutingHealthTime(key.RoutingIsolatedAt)
		if isolatedAt == nil && routingFailureCount >= GatewayAdaptiveRoutingFailureThreshold {
			thresholdAt, thresholdErr := routingFailureIsolationThresholdAtTx(
				tx, channelID, canonicalGroupName, nil, windowStart, latestAt, domain.HealthFence,
			)
			if thresholdErr != nil {
				return thresholdErr
			}
			isolatedAt = thresholdAt
		}
		if isolatedAt != nil && routingFailureCount < GatewayAdaptiveRoutingFailureThreshold {
			routingFailureCount = GatewayAdaptiveRoutingFailureThreshold
		}
		var cooldownUntil *time.Time
		if key.LastRoutingFailureKind == GatewayRoutingFailureManualCooldown && key.RoutingCooldownUntil != nil && at.Before(*key.RoutingCooldownUntil) {
			lastKind = GatewayRoutingFailureManualCooldown
			lastStatus = 0
			lastMessage = key.LastRoutingFailureError
			if key.LastRoutingFailureAt != nil {
				latestAt = *key.LastRoutingFailureAt
			}
			cooldownUntil = cloneRoutingHealthTime(key.RoutingCooldownUntil)
		}
		updates := map[string]any{
			"last_routing_failure_error":  lastMessage,
			"last_routing_failure_kind":   lastKind,
			"last_routing_failure_status": lastStatus,
			"last_routing_failure_at":     latestAt,
			"routing_failure_count":       routingFailureCount,
			"routing_cooldown_until":      cooldownUntil,
			"routing_isolated_at":         isolatedAt,
		}
		if domain.HealthFence != nil {
			updates["routing_health_upstream_group_id"] = domain.HealthFence.UpstreamGroupID
			updates["routing_health_presence_generation"] = domain.HealthFence.PresenceGeneration
			updates["routing_health_channel_identity_revision"] = domain.HealthFence.ChannelIdentityRevision
		}
		result := tx.Model(&GatewayUpstreamKey{}).Where("id = ?", key.ID).Updates(updates)
		return result.Error
	})
}

type GatewayManualRoutingCooldownResult struct {
	RequestLogID     uint      `json:"request_log_id"`
	ChannelID        uint      `json:"channel_id"`
	ChannelName      string    `json:"channel_name"`
	GroupName        string    `json:"group_name"`
	DurationSeconds  int64     `json:"duration_seconds"`
	CooldownUntil    time.Time `json:"cooldown_until"`
	IdempotentReplay bool      `json:"idempotent_replay"`
}

// ApplyRequestLogManualRoutingCooldown resolves the route exclusively from a
// stored request ID. Callers cannot substitute a browser-supplied channel or
// group identity for the route that actually served the request.
func (g *Gateway) ApplyRequestLogManualRoutingCooldown(requestLogID uint, duration time.Duration, actor, idempotencyKey string, at time.Time) (*GatewayManualRoutingCooldownResult, error) {
	if requestLogID == 0 {
		return nil, gorm.ErrRecordNotFound
	}
	if duration <= 0 {
		return nil, errors.New("manual routing cooldown duration must be positive")
	}
	actor = strings.TrimSpace(actor)
	if actor == "" || len(actor) > 128 {
		return nil, errors.New("manual routing cooldown actor is required")
	}
	idempotencyKey = strings.TrimSpace(idempotencyKey)
	if idempotencyKey == "" || len(idempotencyKey) > 128 {
		return nil, errors.New("manual routing cooldown idempotency key is invalid")
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	var applied *GatewayManualRoutingCooldownResult
	err := g.db.Transaction(func(tx *gorm.DB) error {
		var requestLog GatewayRequestLog
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Select("id", "channel_id", "channel_name_snapshot", "group_name", "first_byte_ms", "route_reason", "created_at", "channel_identity_revision", "upstream_group_id", "upstream_group_generation").
			First(&requestLog, requestLogID).Error; err != nil {
			return err
		}
		var existing GatewayManualRoutingCooldownAudit
		err := tx.Where("request_log_id = ? AND idempotency_key = ?", requestLog.ID, idempotencyKey).First(&existing).Error
		if err == nil {
			if existing.Actor != actor {
				return ErrGatewayManualCooldownIdempotencyConflict
			}
			applied = &GatewayManualRoutingCooldownResult{
				RequestLogID:     existing.RequestLogID,
				ChannelID:        existing.ChannelID,
				ChannelName:      existing.ChannelNameSnapshot,
				GroupName:        existing.GroupName,
				DurationSeconds:  existing.DurationSeconds,
				CooldownUntil:    existing.CooldownUntil,
				IdempotentReplay: true,
			}
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		routeReason := strings.ToLower(strings.TrimSpace(requestLog.RouteReason))
		if requestLog.FirstByteMs <= GatewayManualRoutingCooldownMinimumFirstByte.Milliseconds() || !gatewayManualCooldownEligibleRouteReason(routeReason) {
			return ErrGatewayRequestLogManualCooldownIneligible
		}
		groupName := strings.TrimSpace(requestLog.GroupName)
		if requestLog.ChannelID == 0 || groupName == "" || requestLog.ChannelIdentityRevision == 0 ||
			requestLog.UpstreamGroupID == 0 || requestLog.UpstreamGroupGeneration == 0 {
			return ErrGatewayRequestLogRouteUnavailable
		}
		canonicalGroupName, err := canonicalUpstreamGroupName(tx, groupName)
		if err != nil {
			return ErrGatewayRequestLogRouteUnavailable
		}
		fence := GatewayRoutingHealthFence{
			ChannelID: requestLog.ChannelID, ChannelIdentityRevision: requestLog.ChannelIdentityRevision,
			UpstreamGroupID: requestLog.UpstreamGroupID, PresenceGeneration: requestLog.UpstreamGroupGeneration,
			CanonicalGroupName: canonicalGroupName,
		}
		domain, err := lockUpstreamRoutingHealthDomainForFenceTx(tx, fence, at)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrGatewayRequestLogUpstreamGroupUnavailable
		}
		if err != nil {
			return err
		}
		key := domain.Key
		if !requestLog.CreatedAt.IsZero() && key.CreatedAt.After(requestLog.CreatedAt) {
			return ErrGatewayRequestLogUpstreamGroupUnavailable
		}
		groupName = key.GroupName
		cooldownUntil := at.Add(duration)
		if key.RoutingCooldownUntil != nil && key.RoutingCooldownUntil.After(cooldownUntil) {
			cooldownUntil = *key.RoutingCooldownUntil
		}
		result := tx.Model(&GatewayUpstreamKey{}).
			Where("id = ?", key.ID).
			Updates(map[string]any{
				"last_routing_failure_error":               "administrator manual cooldown",
				"last_routing_failure_kind":                GatewayRoutingFailureManualCooldown,
				"last_routing_failure_status":              0,
				"last_routing_failure_at":                  at,
				"routing_cooldown_until":                   cooldownUntil,
				"routing_health_upstream_group_id":         fence.UpstreamGroupID,
				"routing_health_presence_generation":       fence.PresenceGeneration,
				"routing_health_channel_identity_revision": fence.ChannelIdentityRevision,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return ErrGatewayRequestLogUpstreamGroupUnavailable
		}
		audit := &GatewayManualRoutingCooldownAudit{
			RequestLogID:            requestLog.ID,
			RequestFirstByteMs:      requestLog.FirstByteMs,
			RequestCreatedAt:        requestLog.CreatedAt,
			ChannelID:               requestLog.ChannelID,
			ChannelNameSnapshot:     strings.TrimSpace(requestLog.ChannelNameSnapshot),
			GroupName:               groupName,
			UpstreamGroupID:         fence.UpstreamGroupID,
			PresenceGeneration:      fence.PresenceGeneration,
			ChannelIdentityRevision: fence.ChannelIdentityRevision,
			Actor:                   actor,
			IdempotencyKey:          idempotencyKey,
			DurationSeconds:         int64(duration / time.Second),
			PreviousCooldownUntil:   key.RoutingCooldownUntil,
			CooldownUntil:           cooldownUntil,
			CreatedAt:               at,
		}
		if err := tx.Create(audit).Error; err != nil {
			return err
		}
		applied = &GatewayManualRoutingCooldownResult{
			RequestLogID:    requestLog.ID,
			ChannelID:       requestLog.ChannelID,
			ChannelName:     strings.TrimSpace(requestLog.ChannelNameSnapshot),
			GroupName:       groupName,
			DurationSeconds: int64(duration / time.Second),
			CooldownUntil:   cooldownUntil,
		}
		return nil
	})
	return applied, err
}

func gatewayManualCooldownEligibleRouteReason(routeReason string) bool {
	switch strings.ToLower(strings.TrimSpace(routeReason)) {
	case "smart_primary", "fallback", "session_affinity", "model_filtered":
		return true
	default:
		return false
	}
}

func (g *Gateway) DeleteUpstreamKey(channelID uint, groupName string) error {
	return g.db.Transaction(func(tx *gorm.DB) error {
		domain, err := lockUpstreamRoutingHealthDomainTx(tx, channelID, groupName, time.Now())
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if err := tx.Where("channel_id = ? AND lower(btrim(group_name)) = ?", channelID, domain.CanonicalGroupName).Delete(&GatewayUpstreamKey{}).Error; err != nil {
			return err
		}
		return tx.Where("channel_id = ? AND lower(btrim(group_name)) = ?", channelID, domain.CanonicalGroupName).Delete(&GatewayRoutingFailure{}).Error
	})
}

// PrepareChannelUpstreamKeysForRebuild blocks cached key reuse before the
// caller starts deleting remote keys. Remote identities and encrypted local
// secrets remain available so a crash or partial failure can reverify
// ownership before retrying any DELETE.
func (g *Gateway) PrepareChannelUpstreamKeysForRebuild(channelID uint, channelIdentityRevision uint64, pendingMessage string) error {
	if channelID == 0 || channelIdentityRevision == 0 {
		return errors.New("channel id and account identity revision are required")
	}
	return g.db.Transaction(func(tx *gorm.DB) error {
		if err := requireUpstreamAccountIdentityRevisionTx(tx, channelID, channelIdentityRevision); err != nil {
			return err
		}
		if err := lockAllUpstreamRoutingHealthDomainsForChannelTx(tx, channelID, time.Now()); err != nil {
			return err
		}
		if err := tx.Where("channel_id = ?", channelID).Delete(&GatewayRoutingFailure{}).Error; err != nil {
			return err
		}
		return tx.Model(&GatewayUpstreamKey{}).Where("channel_id = ?", channelID).Updates(map[string]any{
			"enabled":                     false,
			"last_error":                  strings.TrimSpace(pendingMessage),
			"last_routing_failure_error":  "",
			"last_routing_failure_kind":   "",
			"last_routing_failure_status": 0,
			"last_routing_failure_at":     nil,
			"routing_failure_count":       0,
			"routing_cooldown_until":      nil,
			"routing_isolated_at":         nil,
			"updated_at":                  time.Now(),
		}).Error
	})
}

func (g *Gateway) ResetChannelUpstreamKeysForRebuild(channelID uint, channelIdentityRevision uint64) error {
	if channelID == 0 || channelIdentityRevision == 0 {
		return errors.New("channel id and account identity revision are required")
	}
	return g.db.Transaction(func(tx *gorm.DB) error {
		if err := requireUpstreamAccountIdentityRevisionTx(tx, channelID, channelIdentityRevision); err != nil {
			return err
		}
		if err := lockAllUpstreamRoutingHealthDomainsForChannelTx(tx, channelID, time.Now()); err != nil {
			return err
		}
		return tx.Model(&GatewayUpstreamKey{}).Where("channel_id = ?", channelID).Updates(map[string]any{
			"upstream_key_id":             0,
			"upstream_key_name":           "",
			"ownership_provenance":        GatewayUpstreamKeyProvenanceUnknown,
			"provisioning":                false,
			"provisioning_started_at":     nil,
			"api_key_cipher":              "",
			"api_key_prefix":              "",
			"enabled":                     false,
			"last_used_at":                nil,
			"last_routing_failure_error":  "",
			"last_routing_failure_kind":   "",
			"last_routing_failure_status": 0,
			"last_routing_failure_at":     nil,
			"routing_failure_count":       0,
			"routing_cooldown_until":      nil,
			"routing_isolated_at":         nil,
			"updated_at":                  time.Now(),
		}).Error
	})
}

// CanonicalizeChannelUpstreamGroupsForRebuild collapses case-only duplicate
// local key rows before a destructive rebuild. Candidate references are moved
// to the chosen survivor; request history is intentionally untouched.
func (g *Gateway) CanonicalizeChannelUpstreamGroupsForRebuild(channelID uint, channelIdentityRevision uint64, preferredGroupNames []string) error {
	if channelID == 0 || channelIdentityRevision == 0 {
		return errors.New("channel id and account identity revision are required")
	}
	return g.db.Transaction(func(tx *gorm.DB) error {
		if err := requireUpstreamAccountIdentityRevisionTx(tx, channelID, channelIdentityRevision); err != nil {
			return err
		}
		if err := lockAllUpstreamRoutingHealthDomainsForChannelTx(tx, channelID, time.Now()); err != nil {
			return err
		}
		preferred := make(map[string]string, len(preferredGroupNames))
		for _, name := range preferredGroupNames {
			name = strings.TrimSpace(name)
			if name == "" {
				continue
			}
			canonical, err := canonicalUpstreamGroupName(tx, name)
			if err != nil {
				return err
			}
			preferred[canonical] = name
		}
		var keys []GatewayUpstreamKey
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("channel_id = ?", channelID).Order(upstreamKeyRoutePreferenceOrder).Find(&keys).Error; err != nil {
			return err
		}
		byCanonicalGroup := make(map[string][]GatewayUpstreamKey, len(keys))
		for _, key := range keys {
			groupKey, err := canonicalUpstreamGroupName(tx, key.GroupName)
			if err != nil {
				return err
			}
			byCanonicalGroup[groupKey] = append(byCanonicalGroup[groupKey], key)
		}
		for groupKey, duplicates := range byCanonicalGroup {
			if len(duplicates) < 2 {
				continue
			}
			survivor := duplicates[0]
			if preferredName := preferred[groupKey]; preferredName != "" {
				for _, candidate := range duplicates {
					if strings.TrimSpace(candidate.GroupName) == preferredName {
						survivor = candidate
						break
					}
				}
			}
			if err := tx.Where("channel_id = ? AND lower(btrim(group_name)) = ?", channelID, groupKey).Delete(&GatewayRoutingFailure{}).Error; err != nil {
				return err
			}
			for _, duplicate := range duplicates {
				if duplicate.ID == survivor.ID {
					continue
				}
				if err := tx.Model(&GatewayDownstreamKey{}).
					Where("manual_channel_id = ? AND manual_group_name = ?", channelID, duplicate.GroupName).
					Update("manual_group_name", survivor.GroupName).Error; err != nil {
					return err
				}
				var exclusions []GatewayRouteExclusion
				if err := tx.Where("channel_id = ? AND group_name = ?", channelID, duplicate.GroupName).Find(&exclusions).Error; err != nil {
					return err
				}
				for _, exclusion := range exclusions {
					var existing int64
					if err := tx.Model(&GatewayRouteExclusion{}).Where(
						"downstream_key_id = ? AND channel_id = ? AND group_name = ?",
						exclusion.DownstreamKeyID, channelID, survivor.GroupName,
					).Count(&existing).Error; err != nil {
						return err
					}
					if existing > 0 {
						if err := tx.Delete(&GatewayRouteExclusion{}, exclusion.ID).Error; err != nil {
							return err
						}
					} else if err := tx.Model(&exclusion).Update("group_name", survivor.GroupName).Error; err != nil {
						return err
					}
				}
				var members []GatewayCandidateGroupMember
				if err := tx.Where("channel_id = ? AND group_name = ?", channelID, duplicate.GroupName).Find(&members).Error; err != nil {
					return err
				}
				for _, member := range members {
					var existing int64
					if err := tx.Model(&GatewayCandidateGroupMember{}).Where(
						"candidate_group_id = ? AND channel_id = ? AND group_name = ?",
						member.CandidateGroupID, channelID, survivor.GroupName,
					).Count(&existing).Error; err != nil {
						return err
					}
					if existing > 0 {
						if err := tx.Delete(&GatewayCandidateGroupMember{}, member.ID).Error; err != nil {
							return err
						}
					} else if err := tx.Model(&member).Update("group_name", survivor.GroupName).Error; err != nil {
						return err
					}
				}
				if err := tx.Where("channel_id = ? AND group_name = ?", channelID, duplicate.GroupName).Delete(&GatewayModelCapability{}).Error; err != nil {
					return err
				}
				if err := tx.Delete(&GatewayUpstreamKey{}, duplicate.ID).Error; err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// DeleteGroupRoutingState removes only live routing state. Request logs are
// historical accounting records and intentionally remain untouched.
func (g *Gateway) DeleteGroupRoutingState(channelID uint, groupName string) error {
	return g.DeleteGroupUpstreamState(channelID, groupName, false)
}

func (g *Gateway) DeleteGroupUpstreamState(channelID uint, groupName string, preserveCandidates bool) error {
	groupName = strings.TrimSpace(groupName)
	return g.db.Transaction(func(tx *gorm.DB) error {
		domain, err := lockUpstreamRoutingHealthDomainTx(tx, channelID, groupName, time.Now())
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		canonicalGroupName := domain.CanonicalGroupName
		if !preserveCandidates {
			if err := tx.Where("channel_id = ? AND lower(btrim(group_name)) = ?", channelID, canonicalGroupName).Delete(&GatewayCandidateGroupMember{}).Error; err != nil {
				return err
			}
		}
		if err := tx.Where("channel_id = ? AND lower(btrim(group_name)) = ?", channelID, canonicalGroupName).Delete(&GatewayUpstreamKey{}).Error; err != nil {
			return err
		}
		if err := tx.Where("channel_id = ? AND lower(btrim(group_name)) = ?", channelID, canonicalGroupName).Delete(&GatewayModelCapability{}).Error; err != nil {
			return err
		}
		return tx.Where("channel_id = ? AND lower(btrim(group_name)) = ?", channelID, canonicalGroupName).Delete(&GatewayRoutingFailure{}).Error
	})
}

func (g *Gateway) ListRoutingGroupNames(channelID uint) ([]string, error) {
	seen := make(map[string]struct{})
	for _, model := range []any{&GatewayUpstreamKey{}, &GatewayModelCapability{}} {
		var names []string
		if err := g.db.Model(model).Where("channel_id = ? AND group_name <> ?", channelID, "").Distinct().Pluck("group_name", &names).Error; err != nil {
			return nil, err
		}
		for _, name := range names {
			if name = strings.TrimSpace(name); name != "" {
				seen[name] = struct{}{}
			}
		}
	}
	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}
	sort.Strings(out)
	return out, nil
}

func (g *Gateway) DisableUpstreamKey(channelID uint, groupName string, msg string, generations ...UpstreamGroupGeneration) error {
	return g.db.Transaction(func(tx *gorm.DB) error {
		if _, err := requireOrdinaryChannelWriteTx(tx, channelID); err != nil {
			return err
		}
		if len(generations) > 0 {
			if err := requireUpstreamGroupGenerationTx(tx, generations[0]); err != nil {
				return err
			}
			groupName = generations[0].CanonicalGroupName
		}
		preferred := preferredUpstreamKeyIDQuery(tx, channelID, groupName)
		return tx.Model(&GatewayUpstreamKey{}).
			Where("id = (?)", preferred).
			Updates(map[string]any{
				"api_key_cipher":          "",
				"api_key_prefix":          "",
				"ownership_provenance":    GatewayUpstreamKeyProvenanceUnknown,
				"enabled":                 false,
				"provisioning":            false,
				"provisioning_started_at": nil,
				"last_error":              msg,
			}).Error
	})
}

// ResetUpstreamKeyAfterManagedReconcile clears the remote identity as well as
// the secret. Unlike ordinary authentication invalidation, a management sync
// has already decided that the prefixed remote key must be replaced, so stale
// ID/name evidence must not send provisioning back through the recovery gate.
func (g *Gateway) ResetUpstreamKeyAfterManagedReconcile(channelID uint, expectedIdentityRevision uint64, groupName, msg string) error {
	return g.db.Transaction(func(tx *gorm.DB) error {
		if err := requireUpstreamAccountIdentityRevisionTx(tx, channelID, expectedIdentityRevision); err != nil {
			return err
		}
		preferred := preferredUpstreamKeyIDQuery(tx, channelID, groupName)
		return tx.Model(&GatewayUpstreamKey{}).
			Where("id = (?)", preferred).
			Updates(map[string]any{
				"upstream_key_id":         0,
				"upstream_key_name":       "",
				"api_key_cipher":          "",
				"api_key_prefix":          "",
				"ownership_provenance":    GatewayUpstreamKeyProvenanceUnknown,
				"enabled":                 false,
				"provisioning":            false,
				"provisioning_started_at": nil,
				"last_error":              strings.TrimSpace(msg),
				"updated_at":              time.Now(),
			}).Error
	})
}

func (g *Gateway) SetRouteExclusion(downstreamKeyID uint, channelID uint, groupName string, disabled bool) error {
	groupName = strings.TrimSpace(groupName)
	if downstreamKeyID == 0 || channelID == 0 || groupName == "" {
		return errors.New("route exclusion requires downstream key, channel, and group")
	}
	canonical, err := canonicalUpstreamGroupName(g.db, groupName)
	if err != nil {
		return err
	}
	if !disabled {
		return g.db.Where("downstream_key_id = ? AND channel_id = ? AND lower(btrim(group_name)) = ?", downstreamKeyID, channelID, canonical).
			Delete(&GatewayRouteExclusion{}).Error
	}
	var existing GatewayRouteExclusion
	err = g.db.Where("downstream_key_id = ? AND channel_id = ? AND lower(btrim(group_name)) = ?", downstreamKeyID, channelID, canonical).First(&existing).Error
	switch err {
	case nil:
		return nil
	case gorm.ErrRecordNotFound:
		return g.db.Create(&GatewayRouteExclusion{
			DownstreamKeyID: downstreamKeyID,
			ChannelID:       channelID,
			GroupName:       canonical,
		}).Error
	default:
		return err
	}
}

func (g *Gateway) ListRouteExclusions(downstreamKeyID uint) ([]GatewayRouteExclusion, error) {
	var list []GatewayRouteExclusion
	if downstreamKeyID == 0 {
		return list, nil
	}
	err := g.db.Where("downstream_key_id = ?", downstreamKeyID).Find(&list).Error
	return list, err
}

const (
	gatewayCostSourceNewAPIQuota                  = "newapi_quota"
	gatewayCostSourceSub2APIActualCost            = "sub2api_actual_cost"
	gatewayCostSourceGroupRatioDefaultQuota       = "group_ratio_default_quota"
	gatewayCostSourceGatewayEstimate              = "gateway_estimate"
	gatewayReconciledCostSourcesSQLList           = "'" + gatewayCostSourceNewAPIQuota + "', '" + gatewayCostSourceSub2APIActualCost + "'"
	gatewayLegacyUnverifiedEstimateSourcesSQLList = "'" + gatewayCostSourceGroupRatioDefaultQuota + "', '" + gatewayCostSourceGatewayEstimate + "'"
	gatewayReportedCostSourcesSQLList             = "'cost', 'total_cost', 'cost_usd', 'actual_cost', " + gatewayReconciledCostSourcesSQLList
	gatewayCostReconcileConflictError             = "upstream usage record is already assigned to another request"
	gatewayCostReconcileIneligibleError           = "request is not eligible for upstream cost reconciliation"
	gatewayCostReconcilePricingUnavailableError   = "request pricing basis is unavailable"
)

func isLegacyUnverifiedGatewayCostEstimate(log *GatewayRequestLog) bool {
	if !isGatewayRequestRecord(log) || log.PriceFactID != nil {
		return false
	}
	status := GatewayCostStatus(strings.ToLower(strings.TrimSpace(string(log.CostStatus))))
	if status == GatewayCostStatusReported || status == GatewayCostStatusReconciled {
		return false
	}
	hasUpstreamUsageID := log.UpstreamUsageID != nil && strings.TrimSpace(*log.UpstreamUsageID) != ""
	if hasUpstreamUsageID || isReconciledGatewayCostSource(log.CostSource) {
		return false
	}
	if status == GatewayCostStatusUnavailable &&
		strings.EqualFold(strings.TrimSpace(log.CostUnavailableReason), GatewayPricingUnavailableLegacyUnverifiedEstimate) {
		return true
	}
	if status == GatewayCostStatusEstimated || log.CostEstimated {
		return true
	}
	switch strings.ToLower(strings.TrimSpace(log.CostSource)) {
	case gatewayCostSourceGroupRatioDefaultQuota, gatewayCostSourceGatewayEstimate:
		return true
	default:
		return false
	}
}

func gatewayRequestCostStatus(log *GatewayRequestLog) GatewayCostStatus {
	if log == nil {
		return GatewayCostStatusNone
	}
	status := GatewayCostStatus(strings.ToLower(strings.TrimSpace(string(log.CostStatus))))
	if status == GatewayCostStatusUnavailable {
		return status
	}
	if isLegacyUnverifiedGatewayCostEstimate(log) {
		return GatewayCostStatusUnavailable
	}
	switch status {
	case GatewayCostStatusEstimated, GatewayCostStatusReported, GatewayCostStatusReconciled:
		return status
	case GatewayCostStatusNone:
		if log.UpstreamUsageID == nil && !log.CostEstimated && log.Cost == 0 && strings.TrimSpace(log.CostSource) == "" {
			return GatewayCostStatusNone
		}
	}
	hasUpstreamUsageID := log.UpstreamUsageID != nil && strings.TrimSpace(*log.UpstreamUsageID) != ""
	if log.CostReconcileLegacyPending && !hasUpstreamUsageID && !log.CostEstimated && strings.TrimSpace(log.CostSource) == "" {
		return GatewayCostStatusNone
	}
	if hasUpstreamUsageID || isReconciledGatewayCostSource(log.CostSource) {
		return GatewayCostStatusReconciled
	}
	if log.CostEstimated {
		return GatewayCostStatusEstimated
	}
	if log.Cost != 0 || strings.TrimSpace(log.CostSource) != "" {
		return GatewayCostStatusReported
	}
	return GatewayCostStatusNone
}

func normalizeGatewayRequestCost(log *GatewayRequestLog) {
	if log == nil {
		return
	}
	legacyUnverifiedEstimate := isLegacyUnverifiedGatewayCostEstimate(log)
	log.CostStatus = gatewayRequestCostStatus(log)
	log.CostEstimated = log.CostStatus == GatewayCostStatusEstimated
	if log.CostStatus == GatewayCostStatusUnavailable && legacyUnverifiedEstimate {
		log.CostUnavailableReason = GatewayPricingUnavailableLegacyUnverifiedEstimate
	}
}

func normalizeGatewayRequestCostReconcileLifecycle(log *GatewayRequestLog) {
	if log == nil {
		return
	}
	// A request is never born with an owned worker claim. Claims are acquired
	// only after the log is durable, under the billing-identity lease.
	log.CostReconcileIdentityLeaseID = nil
	log.CostReconcileClaimEpoch = 0
	log.CostReconcileClaimToken = ""
	log.CostReconcileClaimedAt = nil
	log.CostReconcileLeaseExpiresAt = nil
	if log.CostReconcileAt == nil {
		log.CostReconcileState = GatewayRequestCostReconcileIdle
		log.CostReconcileOutcome = GatewayRequestCostReconcileOutcomeNone
		return
	}
	log.CostReconcileState = GatewayRequestCostReconcilePending
	log.CostReconcileOutcome = GatewayRequestCostReconcileOutcomeNone
}

func isReconciledGatewayCostSource(source string) bool {
	switch strings.ToLower(strings.TrimSpace(source)) {
	case gatewayCostSourceNewAPIQuota, gatewayCostSourceSub2APIActualCost:
		return true
	default:
		return false
	}
}

func isProviderReportedGatewayCostSource(source string) bool {
	switch strings.ToLower(strings.TrimSpace(source)) {
	case "cost", "total_cost", "cost_usd", "actual_cost", gatewayCostSourceNewAPIQuota, gatewayCostSourceSub2APIActualCost:
		return true
	default:
		return false
	}
}

// completeGatewayRequestCostBasis freezes the effective monetary values passed
// to AppendRequestLog. A missing factor means the caller supplied effective
// values without metadata; it does not mean the values should be multiplied
// again during a later startup migration.
func completeGatewayRequestCostBasis(tx *gorm.DB, log *GatewayRequestLog, lockState bool) error {
	if tx == nil || log == nil || gatewayRequestCostReconcileBlockedByPricingFence(log) {
		return nil
	}
	factor := 1.0
	version := uint64(0)
	source := "default"
	if log.ChannelID > 0 && (lockState || log.FundingFactor == nil) {
		if lockState {
			// Funding writers lock channel then cost state. Direct appends take
			// compatible locks in the same order so neither side can observe an
			// old basis and commit a new row outside the correction scan.
			var channel Channel
			if err := tx.Clauses(clause.Locking{Strength: "SHARE"}).
				Select("id", "deletion_pending").First(&channel, log.ChannelID).Error; err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
			if channel.ID != 0 && channel.DeletionPending {
				return ErrChannelDeletionPending
			}
		}
		var state ChannelCostState
		stateQuery := tx.Select("funding_factor", "version", "source")
		if lockState {
			// Direct appends hold this basis row through INSERT. Funding writers
			// lock it for update before scanning logs, closing the old-read/new-row
			// gap without blocking unrelated channels.
			stateQuery = stateQuery.Clauses(clause.Locking{Strength: "SHARE"})
		}
		err := stateQuery.First(&state, "channel_id = ?", log.ChannelID).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if err == nil {
			if state.FundingFactor >= 0 && !math.IsNaN(state.FundingFactor) && !math.IsInf(state.FundingFactor, 0) {
				factor = state.FundingFactor
			}
			version = state.Version
			if value := strings.TrimSpace(state.Source); value != "" {
				source = value
			}
		}
	}
	if log.FundingFactor != nil {
		if *log.FundingFactor < 0 || math.IsNaN(*log.FundingFactor) || math.IsInf(*log.FundingFactor, 0) {
			return ErrCostUnitRequestRestatementConflict
		}
		factor = *log.FundingFactor
	} else {
		log.FundingFactor = &factor
		if log.CostBasisVersion == 0 {
			log.CostBasisVersion = version
		}
		if strings.TrimSpace(log.CostBasisSource) == "" {
			log.CostBasisSource = source
		}
	}
	if log.FundingBasisAt == nil {
		basisAt := log.CreatedAt.UTC()
		log.FundingBasisAt = &basisAt
	} else {
		basisAt := log.FundingBasisAt.UTC()
		log.FundingBasisAt = &basisAt
	}
	if factor > 0 && log.RawGroupRatio == nil && log.GroupRatio > 0 {
		raw := log.GroupRatio / factor
		log.RawGroupRatio = &raw
	}
	if factor > 0 && log.RawCompletionRatio == nil && log.CompletionRatio > 0 {
		raw := log.CompletionRatio / factor
		log.RawCompletionRatio = &raw
	}
	if factor > 0 && log.RawCost == nil && log.Cost > 0 {
		raw := log.Cost / factor
		log.RawCost = &raw
	}
	if log.ChannelID == 0 || log.FundingBasisAt == nil || log.FundingBasisAt.IsZero() {
		return nil
	}
	// Migration 45 makes a trusted price-fact snapshot, including its frozen
	// funding basis and ratios, immutable. Historical funding correction only
	// applies to rows whose financial basis remains mutable.
	if log.PriceFactID != nil {
		return nil
	}
	correction, err := gatewayRequestFundingCorrectionAtTx(tx, log.ChannelID, *log.FundingBasisAt, log.CostBasisVersion)
	if err != nil {
		return err
	}
	if correction == nil {
		return nil
	}
	correctedFactor := correction.FundingFactor
	if isGatewayDispatchCostEvidenceRecord(log) && gatewayRequestCostStatus(log) == GatewayCostStatusNone &&
		log.RawCost == nil && log.UpstreamUsageID == nil {
		correctedVersion := correction.CostBasisVersion
		correctedSource := correction.CostBasisSource
		log.DeferredFundingFactor = &correctedFactor
		log.DeferredCostBasisVersion = &correctedVersion
		log.DeferredCostBasisSource = &correctedSource
		return nil
	}
	log.FundingFactor = &correctedFactor
	log.CostBasisVersion = correction.CostBasisVersion
	log.CostBasisSource = correction.CostBasisSource
	if log.RawGroupRatio != nil {
		log.GroupRatio = *log.RawGroupRatio * correctedFactor
	}
	if log.RawCompletionRatio != nil {
		log.CompletionRatio = *log.RawCompletionRatio * correctedFactor
	}
	if log.RawCost != nil {
		log.Cost = *log.RawCost * correctedFactor
	}
	return nil
}

func gatewayRequestFundingCorrectionAtTx(tx *gorm.DB, channelID uint, at time.Time, frozenVersion uint64) (*GatewayRequestFundingCorrection, error) {
	if tx == nil || channelID == 0 || at.IsZero() {
		return nil, nil
	}
	var correction GatewayRequestFundingCorrection
	err := tx.Where("channel_id = ? AND interval_start <= ? AND (interval_end IS NULL OR interval_end > ?) AND cost_basis_version > ?", channelID, at.UTC(), at.UTC(), frozenVersion).
		Order("cost_basis_version DESC").Order("id DESC").First(&correction).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if correction.FundingFactor < 0 || math.IsNaN(correction.FundingFactor) || math.IsInf(correction.FundingFactor, 0) ||
		correction.CostBasisVersion == 0 || strings.TrimSpace(correction.CostBasisSource) == "" {
		return nil, ErrCostUnitRequestRestatementConflict
	}
	return &correction, nil
}

func (g *Gateway) AppendRequestLog(log *GatewayRequestLog) error {
	if err := normalizeGatewayRequestRecord(log); err != nil {
		return err
	}
	if log.CreatedAt.IsZero() {
		log.CreatedAt = time.Now().UTC()
	} else {
		log.CreatedAt = log.CreatedAt.UTC()
	}
	if log.CostReconcileAt != nil {
		value := log.CostReconcileAt.UTC()
		log.CostReconcileAt = &value
	}
	return g.db.Transaction(func(tx *gorm.DB) error {
		return g.appendRequestLogTx(tx, log, true)
	})
}

// UpdateRequestLogDownstreamFirstVisible fills the downstream visibility fact
// after a terminal JSON body has actually been written. The logical trace and
// telemetry version fence the update so an ID mix-up cannot modify a legacy or
// unrelated request. The first observed value is immutable.
func (g *Gateway) UpdateRequestLogDownstreamFirstVisible(id uint, logicalTraceID string, latencyMs int64) error {
	if g == nil || g.db == nil {
		return errors.New("gateway store is unavailable")
	}
	if id == 0 {
		return fmt.Errorf("%w: request log id is required", ErrGatewayRequestRecordInvalid)
	}
	traceID, err := validateGatewayLogicalRequestTraceID(logicalTraceID)
	if err != nil || traceID == "" {
		if err == nil {
			err = errors.New("logical request trace id is required")
		}
		return fmt.Errorf("%w: %w", ErrGatewayRequestRecordInvalid, err)
	}
	if latencyMs < 0 {
		return fmt.Errorf("%w: downstream first visible latency must not be negative", ErrGatewayRequestRecordInvalid)
	}
	result := g.db.Model(&GatewayRequestLog{}).
		Where("id = ? AND record_kind = ? AND telemetry_version = ? AND logical_request_trace_id = ? AND downstream_first_visible_ms IS NULL",
			id, GatewayRequestRecordKindRequest, GatewayRequestTelemetryVersion, traceID).
		UpdateColumn("downstream_first_visible_ms", latencyMs)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("%w: downstream first visible telemetry update did not match", ErrGatewayRequestRecordInvalid)
	}
	return nil
}

// appendRequestLogTx is the single transactional append path for request logs
// and their aggregate ledgers. Dispatch finalization reuses it so the durable
// lifecycle terminal and the visible request log cannot commit separately.
func (g *Gateway) appendRequestLogTx(tx *gorm.DB, log *GatewayRequestLog, lockState bool) error {
	if err := normalizeGatewayRequestRecord(log); err != nil {
		return err
	}
	if log.DeferredFundingFactor != nil || log.DeferredCostBasisVersion != nil || log.DeferredCostBasisSource != nil {
		return ErrCostUnitRequestRestatementConflict
	}
	if err := completeGatewayRequestCostBasis(tx, log, lockState); err != nil {
		return err
	}
	normalizeGatewayRequestCost(log)
	normalizeGatewayRequestCostReconcileLifecycle(log)
	log.AggregateVersion = gatewayRequestAggregateVersion
	log.GroupCacheAggregateVersion = 0
	state, err := gatewayRequestGroupCacheBackfillStateTx(tx)
	if err != nil {
		return err
	}
	if state.TargetVersion > gatewayRequestGroupCacheAggregateVersion {
		return errGatewayRequestGroupCacheAggregateVersionConflict
	}
	if state.TargetVersion != gatewayRequestGroupCacheAggregateVersion || !state.Completed {
		state, err = lockGatewayRequestGroupCacheBackfillStateTx(tx)
		if err != nil {
			return err
		}
	}
	if err := snapshotGatewayRequestLogChannelName(tx, log); err != nil {
		return err
	}
	if err := validateGatewayRequestLogPriceFactSnapshotTx(tx, log); err != nil {
		return err
	}
	hasDeferredFundingCorrection := log.DeferredFundingFactor != nil || log.DeferredCostBasisVersion != nil || log.DeferredCostBasisSource != nil
	if hasDeferredFundingCorrection && (log.DeferredFundingFactor == nil || log.DeferredCostBasisVersion == nil || log.DeferredCostBasisSource == nil ||
		!isGatewayDispatchCostEvidenceRecord(log) || gatewayRequestCostStatus(log) != GatewayCostStatusNone ||
		log.RawCost != nil || log.UpstreamUsageID != nil) {
		return ErrCostUnitRequestRestatementConflict
	}
	deferredFactor, deferredVersion, deferredSource := log.DeferredFundingFactor, log.DeferredCostBasisVersion, log.DeferredCostBasisSource
	if hasDeferredFundingCorrection {
		// Migration 66 deliberately forbids a deferred tuple on INSERT. Preserve
		// the immutable dispatch basis first, then attach the correction intent
		// through the guarded pending-evidence transition in the same transaction.
		log.DeferredFundingFactor = nil
		log.DeferredCostBasisVersion = nil
		log.DeferredCostBasisSource = nil
	}
	if err := tx.Create(log).Error; err != nil {
		log.DeferredFundingFactor = deferredFactor
		log.DeferredCostBasisVersion = deferredVersion
		log.DeferredCostBasisSource = deferredSource
		return err
	}
	if hasDeferredFundingCorrection {
		result := tx.Model(&GatewayRequestLog{}).Where("id = ?", log.ID).Updates(map[string]any{
			"deferred_funding_factor":     *deferredFactor,
			"deferred_cost_basis_version": *deferredVersion,
			"deferred_cost_basis_source":  strings.TrimSpace(*deferredSource),
		})
		if result.Error != nil {
			log.DeferredFundingFactor = deferredFactor
			log.DeferredCostBasisVersion = deferredVersion
			log.DeferredCostBasisSource = deferredSource
			return result.Error
		}
		if result.RowsAffected != 1 {
			log.DeferredFundingFactor = deferredFactor
			log.DeferredCostBasisVersion = deferredVersion
			log.DeferredCostBasisSource = deferredSource
			return ErrCostUnitRequestRestatementConflict
		}
		log.DeferredFundingFactor = deferredFactor
		log.DeferredCostBasisVersion = deferredVersion
		log.DeferredCostBasisSource = deferredSource
	}
	if row, ok := requestAggregateFromLog(log); ok {
		if err := upsertRequestAggregateTx(tx, row); err != nil {
			return err
		}
	}
	if state.TargetVersion == gatewayRequestGroupCacheAggregateVersion && state.Completed {
		if row, ok := requestGroupCacheAggregateFromLog(log); ok {
			if err := upsertGatewayRequestGroupCacheAggregateTx(tx, row); err != nil {
				return err
			}
		}
		return markGatewayRequestGroupCacheLogsProcessedTx(tx, []uint{log.ID})
	}
	return nil
}

// FindRequestLogClientIP selects only the fields needed by the administrator
// local-geolocation lookup. Keeping this narrow prevents the lookup endpoint
// from accidentally loading or returning other request-log details.
func (g *Gateway) FindRequestLogClientIP(id uint) (*GatewayRequestLog, error) {
	if id == 0 {
		return nil, gorm.ErrRecordNotFound
	}
	var log GatewayRequestLog
	if err := g.db.Select("id", "client_ip", "client_ip_source").Where("id = ? AND "+gatewayRequestRecordSQL(""), id).First(&log).Error; err != nil {
		return nil, err
	}
	return &log, nil
}

func (g *Gateway) UpdateRequestLogActualCost(id uint, usageID string, cost float64, source string, cacheCreationTokens int64, cacheReadTokens int64, hasCacheTokenMetrics bool) error {
	if id == 0 || cost < 0 {
		return nil
	}
	actual := cost
	var snapshot GatewayRequestLog
	if err := g.db.Select("funding_factor").First(&snapshot, id).Error; err == nil && snapshot.FundingFactor != nil && *snapshot.FundingFactor >= 0 {
		actual = cost * *snapshot.FundingFactor
	}
	return g.UpdateRequestLogActualCostWithRaw(id, usageID, cost, actual, source, cacheCreationTokens, cacheReadTokens, hasCacheTokenMetrics)
}

type GatewayRequestUsageMetrics struct {
	PromptTokens         int64
	CompletionTokens     int64
	TotalTokens          int64
	CacheCreationTokens  int64
	CacheReadTokens      int64
	UpstreamModelName    string
	HasTokenMetrics      bool
	HasCacheTokenMetrics bool
}

// UpdateRequestLogActualCostWithRaw stores both the upstream-reported raw
// amount and the normalized amount paid by the operator. Keeping both values
// makes historical logs independent of later funding-factor changes.
func (g *Gateway) UpdateRequestLogActualCostWithRaw(id uint, usageID string, rawCost, actualCost float64, source string, cacheCreationTokens int64, cacheReadTokens int64, hasCacheTokenMetrics bool) error {
	return g.UpdateRequestLogActualCostWithUsage(id, usageID, rawCost, actualCost, source, GatewayRequestUsageMetrics{
		CacheCreationTokens:  cacheCreationTokens,
		CacheReadTokens:      cacheReadTokens,
		HasCacheTokenMetrics: hasCacheTokenMetrics,
	})
}

func (g *Gateway) UpdateRequestLogActualCostWithUsage(id uint, usageID string, rawCost, actualCost float64, source string, usage GatewayRequestUsageMetrics) error {
	return g.updateRequestLogActualCostWithUsage(id, nil, nil, usageID, rawCost, actualCost, source, usage)
}

func (g *Gateway) UpdateClaimedRequestLogActualCostWithUsage(id uint, leaseUntil time.Time, usageID string, rawCost, actualCost float64, source string, usage GatewayRequestUsageMetrics) error {
	if leaseUntil.IsZero() {
		return ErrRequestCostReconcileLeaseLost
	}
	leaseUntil = leaseUntil.UTC()
	return g.updateRequestLogActualCostWithUsage(id, &leaseUntil, nil, usageID, rawCost, actualCost, source, usage)
}

func (g *Gateway) UpdateClaimedRequestLogActualCostWithUsageDurable(fence GatewayRequestCostReconcileFence, usageID string, rawCost, actualCost float64, source string, usage GatewayRequestUsageMetrics) error {
	if !fence.valid() {
		return ErrRequestCostReconcileLeaseLost
	}
	return g.updateRequestLogActualCostWithUsage(fence.RequestLogID, nil, &fence, usageID, rawCost, actualCost, source, usage)
}

func (g *Gateway) updateRequestLogActualCostWithUsage(id uint, expectedLease *time.Time, expectedFence *GatewayRequestCostReconcileFence, usageID string, rawCost, actualCost float64, source string, usage GatewayRequestUsageMetrics) error {
	if id == 0 || rawCost < 0 || actualCost < 0 {
		return nil
	}
	trimmedUsageID := strings.TrimSpace(usageID)
	return g.db.Transaction(func(tx *gorm.DB) error {
		// Completed is a read-mostly state. Only the incomplete path takes the
		// singleton lock before the request row, preserving the backfill lock
		// order without serializing normal cost reconciliations.
		state, err := gatewayRequestGroupCacheBackfillStateTx(tx)
		if err != nil {
			return err
		}
		if state.TargetVersion > gatewayRequestGroupCacheAggregateVersion {
			return errGatewayRequestGroupCacheAggregateVersionConflict
		}
		if state.TargetVersion != gatewayRequestGroupCacheAggregateVersion || !state.Completed {
			if _, err := lockGatewayRequestGroupCacheBackfillStateTx(tx); err != nil {
				return err
			}
		}
		// Cost-unit restatements take the aggregate singleton and then an
		// EXCLUSIVE table lock before selecting historical request rows. Declare
		// this writer intent before locking a request row so neither side can
		// wait on a table-lock upgrade while holding the row the other needs.
		if err := tx.Exec("LOCK TABLE public.gateway_request_logs IN ROW EXCLUSIVE MODE").Error; err != nil {
			return err
		}
		var reconcileIdentityLease *GatewayRequestCostReconcileIdentityLease
		var databaseNow time.Time
		if expectedFence != nil {
			reconcileIdentityLease, _, err = lockGatewayRequestCostReconcileFenceTx(tx, *expectedFence)
			if err != nil {
				return err
			}
		}
		var current GatewayRequestLog
		currentQuery := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", id)
		if expectedLease != nil {
			currentQuery = currentQuery.Where("cost_reconcile_at = ?", *expectedLease)
		}
		if err := currentQuery.First(&current).Error; err != nil {
			if (expectedLease != nil || expectedFence != nil) && errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrRequestCostReconcileLeaseLost
			}
			return err
		}
		if expectedFence != nil {
			databaseNow, err = databaseClockNowTx(tx)
			if err != nil {
				return err
			}
			if !requestCostReconcileClaimMatches(&current, *expectedFence) ||
				!gatewayRequestCostReconcileIdentityLeaseMatchesFence(reconcileIdentityLease, *expectedFence, databaseNow) {
				return ErrRequestCostReconcileLeaseLost
			}
		} else {
			// Unfenced compatibility writers must not bypass a durable claim.
			if current.CostReconcileState == GatewayRequestCostReconcileClaimed {
				return ErrRequestCostReconcileLeaseLost
			}
			databaseNow, err = databaseClockNowTx(tx)
			if err != nil {
				return err
			}
		}
		if gatewayRequestCostReconcileBlockedByPricingFence(&current) {
			updates := map[string]any{
				"cost_reconcile_at":            nil,
				"cost_reconcile_tries":         gorm.Expr("cost_reconcile_tries + 1"),
				"cost_reconcile_error":         gatewayCostReconcilePricingUnavailableError,
				"cost_reconcile_state":         GatewayRequestCostReconcileTerminal,
				"cost_reconcile_outcome":       GatewayRequestCostReconcileOutcomePricingUnavailable,
				"cost_reconcile_last_event_at": databaseNow,
			}
			mergeGatewayRequestCostReconcileUpdates(updates, clearGatewayRequestCostReconcileClaimUpdates())
			updateQuery := tx.Model(&GatewayRequestLog{}).Where("id = ?", id)
			if expectedLease != nil {
				updateQuery = updateQuery.Where("cost_reconcile_at = ?", *expectedLease)
			}
			if expectedFence != nil {
				updateQuery = updateQuery.Where("cost_reconcile_state = ? AND cost_reconcile_identity_lease_id = ? AND cost_reconcile_claim_epoch = ? AND cost_reconcile_claim_token = ?",
					GatewayRequestCostReconcileClaimed, expectedFence.IdentityLeaseID, expectedFence.ClaimEpoch, expectedFence.ClaimToken)
			}
			result := updateQuery.Updates(updates)
			if result.Error != nil {
				return result.Error
			}
			if (expectedLease != nil || expectedFence != nil) && result.RowsAffected != 1 {
				return ErrRequestCostReconcileLeaseLost
			}
			if expectedFence != nil {
				if err := releaseGatewayRequestCostReconcileIdentityLeaseTx(tx, reconcileIdentityLease, *expectedFence, databaseNow); err != nil {
					return err
				}
			}
			return appendGatewayRequestCostReconcileEventTx(tx, current, expectedFence, "outcome", current.CostReconcileState, GatewayRequestCostReconcileTerminal, GatewayRequestCostReconcileOutcomePricingUnavailable, GatewayRequestCostReconcileReasonPricingUnavailable, gatewayCostReconcilePricingUnavailableError, nil, gatewayRequestCostReconcileFenceLease(expectedFence))
		}
		hasDeferredFundingCorrection := current.DeferredFundingFactor != nil || current.DeferredCostBasisVersion != nil || current.DeferredCostBasisSource != nil
		if hasDeferredFundingCorrection && (current.DeferredFundingFactor == nil || current.DeferredCostBasisVersion == nil || current.DeferredCostBasisSource == nil ||
			!isGatewayDispatchCostEvidenceRecord(&current) || *current.DeferredFundingFactor < 0 || math.IsNaN(*current.DeferredFundingFactor) ||
			math.IsInf(*current.DeferredFundingFactor, 0) || *current.DeferredCostBasisVersion == 0 || strings.TrimSpace(*current.DeferredCostBasisSource) == "" ||
			current.PriceFactID != nil) {
			return ErrCostUnitRequestRestatementConflict
		}
		actualCost, err = requestLogActualCostForCurrentBasis(&current, rawCost, actualCost)
		if err != nil {
			return err
		}
		if trimmedUsageID != "" {
			var owner GatewayRequestLog
			err := tx.Select("id").Where("channel_id = ? AND upstream_usage_id = ? AND id <> ?", current.ChannelID, trimmedUsageID, id).First(&owner).Error
			if err == nil {
				return ErrUpstreamUsageAlreadyAssigned
			}
			if !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
		}
		var usageIDValue any
		if trimmedUsageID != "" {
			usageIDValue = trimmedUsageID
		}
		updates := map[string]any{
			"cost":                         actualCost,
			"raw_cost":                     rawCost,
			"cost_estimated":               false,
			"cost_source":                  source,
			"cost_status":                  GatewayCostStatusReconciled,
			"cost_unavailable_reason":      "",
			"upstream_usage_id":            usageIDValue,
			"cost_reconcile_at":            nil,
			"cost_reconcile_tries":         gorm.Expr("cost_reconcile_tries + 1"),
			"cost_reconcile_error":         "",
			"cost_reconcile_state":         GatewayRequestCostReconcileSucceeded,
			"cost_reconcile_outcome":       GatewayRequestCostReconcileOutcomeSucceeded,
			"cost_reconcile_last_event_at": databaseNow,
		}
		if hasDeferredFundingCorrection {
			updates["deferred_funding_factor"] = nil
			updates["deferred_cost_basis_version"] = nil
			updates["deferred_cost_basis_source"] = nil
		}
		mergeGatewayRequestCostReconcileUpdates(updates, clearGatewayRequestCostReconcileClaimUpdates())
		after := current
		after.Cost = actualCost
		after.RawCost = &rawCost
		after.CostEstimated = false
		after.CostSource = source
		after.CostStatus = GatewayCostStatusReconciled
		after.CostUnavailableReason = ""
		after.CostReconcileError = ""
		after.DeferredFundingFactor = nil
		after.DeferredCostBasisVersion = nil
		after.DeferredCostBasisSource = nil
		if trimmedUsageID != "" {
			after.UpstreamUsageID = &trimmedUsageID
		} else {
			after.UpstreamUsageID = nil
		}
		applyGatewayRequestUsageMetricsUpdate(&current, &after, updates, usage)
		updateQuery := tx.Model(&GatewayRequestLog{}).Where("id = ?", id)
		if expectedLease != nil {
			updateQuery = updateQuery.Where("cost_reconcile_at = ?", *expectedLease)
		}
		if expectedFence != nil {
			updateQuery = updateQuery.Where("cost_reconcile_state = ? AND cost_reconcile_identity_lease_id = ? AND cost_reconcile_claim_epoch = ? AND cost_reconcile_claim_token = ?",
				GatewayRequestCostReconcileClaimed, expectedFence.IdentityLeaseID, expectedFence.ClaimEpoch, expectedFence.ClaimToken)
		}
		result := updateQuery.Updates(updates)
		if result.Error != nil {
			if gatewayUsageAssignmentConflict(result.Error) {
				return ErrUpstreamUsageAlreadyAssigned
			}
			return result.Error
		}
		if (expectedLease != nil || expectedFence != nil) && result.RowsAffected != 1 {
			return ErrRequestCostReconcileLeaseLost
		}
		if hasDeferredFundingCorrection {
			deferredFactor := *current.DeferredFundingFactor
			deferredActualCost := rawCost * deferredFactor
			if deferredActualCost < 0 || math.IsNaN(deferredActualCost) || math.IsInf(deferredActualCost, 0) {
				return ErrCostUnitRequestRestatementConflict
			}
			deferredSource := strings.TrimSpace(*current.DeferredCostBasisSource)
			deferredResult := tx.Model(&GatewayRequestLog{}).Where("id = ?", id).Updates(map[string]any{
				"cost":               deferredActualCost,
				"group_ratio":        gorm.Expr("CASE WHEN raw_group_ratio IS NOT NULL THEN raw_group_ratio * ? ELSE group_ratio END", deferredFactor),
				"completion_ratio":   gorm.Expr("CASE WHEN raw_completion_ratio IS NOT NULL THEN raw_completion_ratio * ? ELSE completion_ratio END", deferredFactor),
				"funding_factor":     deferredFactor,
				"cost_basis_version": *current.DeferredCostBasisVersion,
				"cost_basis_source":  deferredSource,
			})
			if deferredResult.Error != nil {
				return deferredResult.Error
			}
			if deferredResult.RowsAffected != 1 {
				return ErrCostUnitRequestRestatementConflict
			}
			actualCost = deferredActualCost
			after.Cost = deferredActualCost
			after.FundingFactor = &deferredFactor
			after.CostBasisVersion = *current.DeferredCostBasisVersion
			after.CostBasisSource = deferredSource
			if after.RawGroupRatio != nil {
				after.GroupRatio = *after.RawGroupRatio * deferredFactor
			}
			if after.RawCompletionRatio != nil {
				after.CompletionRatio = *after.RawCompletionRatio * deferredFactor
			}
		}
		if err := replaceRequestAggregateTx(tx, &current, &after); err != nil {
			return err
		}
		if err := replaceGatewayRequestGroupCacheAggregateTx(tx, &current, &after); err != nil {
			return err
		}
		if expectedFence != nil {
			if err := releaseGatewayRequestCostReconcileIdentityLeaseTx(tx, reconcileIdentityLease, *expectedFence, databaseNow); err != nil {
				return err
			}
		}
		return appendGatewayRequestCostReconcileEventTx(tx, current, expectedFence, "outcome", current.CostReconcileState, GatewayRequestCostReconcileSucceeded, GatewayRequestCostReconcileOutcomeSucceeded, GatewayRequestCostReconcileReasonSucceeded, "", nil, gatewayRequestCostReconcileFenceLease(expectedFence))
	})
}

func requestLogActualCostForCurrentBasis(current *GatewayRequestLog, rawCost, suppliedActualCost float64) (float64, error) {
	if current == nil {
		return 0, ErrCostUnitRequestRestatementConflict
	}
	if current.FundingFactor == nil {
		return suppliedActualCost, nil
	}
	if *current.FundingFactor < 0 || math.IsNaN(*current.FundingFactor) || math.IsInf(*current.FundingFactor, 0) {
		return 0, ErrCostUnitRequestRestatementConflict
	}
	actualCost := rawCost * *current.FundingFactor
	if actualCost < 0 || math.IsNaN(actualCost) || math.IsInf(actualCost, 0) {
		return 0, ErrCostUnitRequestRestatementConflict
	}
	return actualCost, nil
}

func applyGatewayRequestUsageMetricsUpdate(current, after *GatewayRequestLog, updates map[string]any, usage GatewayRequestUsageMetrics) {
	if current == nil || after == nil || updates == nil || isGatewayDispatchCostEvidenceRecord(current) {
		return
	}
	tokenPresenceAllowed := current.UsageSchemaVersion == 1 &&
		current.PriceOperation == GatewayPricingOperationTextGeneration &&
		current.UsageSchema == GatewayPricingUsageSchemaTokensV1 &&
		gatewayRequestPricingProtocolSupported(current.PriceOperation, current.PriceProtocol)
	inputOnlyPresenceAllowed := current.UsageSchemaVersion == 1 &&
		current.PriceOperation == GatewayPricingOperationEmbedding &&
		current.UsageSchema == GatewayPricingUsageSchemaInputTokensV1 &&
		gatewayRequestPricingProtocolSupported(current.PriceOperation, current.PriceProtocol)
	if usage.HasTokenMetrics {
		promptTokens := usage.PromptTokens
		completionTokens := usage.CompletionTokens
		totalTokens := usage.TotalTokens
		tokenMetricsValid := promptTokens >= 0 && completionTokens >= 0 && totalTokens >= 0
		if totalTokens == 0 {
			var ok bool
			totalTokens, ok = checkedGatewayRequestPricingAdd(promptTokens, completionTokens)
			tokenMetricsValid = tokenMetricsValid && ok
		}
		if tokenMetricsValid {
			updates["prompt_tokens"] = promptTokens
			updates["completion_tokens"] = completionTokens
			updates["total_tokens"] = totalTokens
			after.PromptTokens = promptTokens
			after.CompletionTokens = completionTokens
			after.TotalTokens = totalTokens
			// Presence bits are part of the frozen pricing contract. Legacy rows
			// have no immutable usage schema (and /v1/responses is ambiguous), so
			// an exact-cost reconciliation may refresh their numeric telemetry but
			// must not invent v1 component provenance that violates the database
			// shape. New rows already froze a supported contract before dispatch.
			if tokenPresenceAllowed || inputOnlyPresenceAllowed {
				updates["usage_input_present"] = true
				after.UsageInputPresent = true
			}
			if tokenPresenceAllowed {
				updates["usage_output_present"] = true
				after.UsageOutputPresent = true
			}
		}
	}
	if usage.HasCacheTokenMetrics {
		cacheCreationTokens := usage.CacheCreationTokens
		cacheReadTokens := usage.CacheReadTokens
		if cacheCreationTokens >= 0 && cacheReadTokens >= 0 {
			updates["cache_creation_tokens"] = cacheCreationTokens
			updates["cache_read_tokens"] = cacheReadTokens
			after.CacheCreationTokens = cacheCreationTokens
			after.CacheReadTokens = cacheReadTokens
			if tokenPresenceAllowed {
				updates["usage_cache_write_present"] = true
				updates["usage_cache_read_present"] = true
				after.UsageCacheWritePresent = true
				after.UsageCacheReadPresent = true
			}
		}
	}
	if upstreamModelName := strings.TrimSpace(usage.UpstreamModelName); upstreamModelName != "" {
		updates["upstream_model_name"] = upstreamModelName
		after.UpstreamModelName = upstreamModelName
	}
}

func gatewayUsageAssignmentConflict(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == "idx_gateway_channel_usage"
}

func (g *Gateway) ScheduleRequestCostReconcile(id uint, at time.Time) error {
	if g == nil || g.db == nil || id == 0 || at.IsZero() {
		return errors.New("request cost reconcile schedule is invalid")
	}
	at = at.UTC()
	return g.db.Transaction(func(tx *gorm.DB) error {
		var log GatewayRequestLog
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&log, id).Error; err != nil {
			return err
		}
		if log.CostReconcileState == GatewayRequestCostReconcileClaimed {
			return ErrRequestCostReconcileLeaseLost
		}
		databaseNow, err := databaseClockNowTx(tx)
		if err != nil {
			return err
		}
		updates := map[string]any{
			"cost_reconcile_at":            at,
			"cost_reconcile_state":         GatewayRequestCostReconcilePending,
			"cost_reconcile_outcome":       GatewayRequestCostReconcileOutcomeNone,
			"cost_reconcile_last_event_at": databaseNow,
		}
		mergeGatewayRequestCostReconcileUpdates(updates, clearGatewayRequestCostReconcileClaimUpdates())
		if err := tx.Model(&GatewayRequestLog{}).Where("id = ?", id).Updates(updates).Error; err != nil {
			return err
		}
		return appendGatewayRequestCostReconcileEventTx(tx, log, nil, "scheduled", log.CostReconcileState, GatewayRequestCostReconcilePending, GatewayRequestCostReconcileOutcomeNone, GatewayRequestCostReconcileReasonScheduled, "", &at, nil)
	})
}

func (g *Gateway) ReleaseRequestCostReconcileClaim(id uint, leaseUntil time.Time, at time.Time) error {
	result := g.db.Model(&GatewayRequestLog{}).
		Where("id = ? AND cost_reconcile_at = ? AND cost_reconcile_state <> ?", id, leaseUntil.UTC(), GatewayRequestCostReconcileClaimed).
		Update("cost_reconcile_at", at)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrRequestCostReconcileLeaseLost
	}
	return nil
}

func (g *Gateway) MarkRequestCostReconcileRetry(id uint, at time.Time, reconcileErrors ...string) error {
	return g.markRequestCostReconcileRetry(id, nil, at, reconcileErrors...)
}

func (g *Gateway) MarkClaimedRequestCostReconcileRetry(id uint, leaseUntil time.Time, at time.Time, reconcileErrors ...string) error {
	leaseUntil = leaseUntil.UTC()
	return g.markRequestCostReconcileRetry(id, &leaseUntil, at, reconcileErrors...)
}

func (g *Gateway) markRequestCostReconcileRetry(id uint, expectedLease *time.Time, at time.Time, reconcileErrors ...string) error {
	updates := map[string]any{
		"cost_reconcile_at":            at,
		"cost_reconcile_tries":         gorm.Expr("cost_reconcile_tries + 1"),
		"cost_reconcile_state":         GatewayRequestCostReconcileRetryScheduled,
		"cost_reconcile_outcome":       GatewayRequestCostReconcileOutcomeRetry,
		"cost_reconcile_last_event_at": gorm.Expr("clock_timestamp()"),
	}
	if len(reconcileErrors) > 0 {
		updates["cost_reconcile_error"] = strings.TrimSpace(reconcileErrors[0])
	}
	mergeGatewayRequestCostReconcileUpdates(updates, clearGatewayRequestCostReconcileClaimUpdates())
	query := g.db.Model(&GatewayRequestLog{}).Where("id = ? AND cost_reconcile_state <> ?", id, GatewayRequestCostReconcileClaimed)
	if expectedLease != nil {
		query = query.Where("cost_reconcile_at = ?", *expectedLease)
	}
	result := query.Updates(updates)
	if result.Error != nil {
		return result.Error
	}
	if expectedLease != nil && result.RowsAffected != 1 {
		return ErrRequestCostReconcileLeaseLost
	}
	return nil
}

func (g *Gateway) MarkRequestCostReconcileFailed(id uint, reconcileError string) error {
	return g.markRequestCostReconcileFailed(id, nil, GatewayRequestCostReconcileReasonLegacyTerminalFailure, reconcileError)
}

func (g *Gateway) MarkRequestCostReconcileFailedTyped(id uint, reasonCode, reconcileError string) error {
	return g.markRequestCostReconcileFailed(id, nil, reasonCode, reconcileError)
}

func (g *Gateway) MarkClaimedRequestCostReconcileFailed(id uint, leaseUntil time.Time, reconcileError string) error {
	return g.MarkClaimedRequestCostReconcileFailedTyped(id, leaseUntil, GatewayRequestCostReconcileReasonLegacyTerminalFailure, reconcileError)
}

func (g *Gateway) MarkClaimedRequestCostReconcileFailedTyped(id uint, leaseUntil time.Time, reasonCode, reconcileError string) error {
	leaseUntil = leaseUntil.UTC()
	return g.markRequestCostReconcileFailed(id, &leaseUntil, reasonCode, reconcileError)
}

func (g *Gateway) markRequestCostReconcileFailed(id uint, expectedLease *time.Time, reasonCode, reconcileError string) error {
	return g.db.Transaction(func(tx *gorm.DB) error {
		query := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ? AND cost_reconcile_state <> ?", id, GatewayRequestCostReconcileClaimed)
		if expectedLease != nil {
			query = query.Where("cost_reconcile_at = ?", *expectedLease)
		}
		var log GatewayRequestLog
		if err := query.First(&log).Error; err != nil {
			if expectedLease != nil && errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrRequestCostReconcileLeaseLost
			}
			return err
		}
		databaseNow, err := databaseClockNowTx(tx)
		if err != nil {
			return err
		}
		updates := map[string]any{
			"cost_reconcile_at":            nil,
			"cost_reconcile_tries":         gorm.Expr("cost_reconcile_tries + 1"),
			"cost_reconcile_error":         strings.TrimSpace(reconcileError),
			"cost_reconcile_state":         GatewayRequestCostReconcileTerminal,
			"cost_reconcile_outcome":       GatewayRequestCostReconcileOutcomeTerminalFailure,
			"cost_reconcile_last_event_at": databaseNow,
		}
		mergeGatewayRequestCostReconcileUpdates(updates, clearGatewayRequestCostReconcileClaimUpdates())
		result := tx.Model(&GatewayRequestLog{}).Where("id = ?", id).Updates(updates)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrRequestCostReconcileLeaseLost
		}
		return appendGatewayRequestCostReconcileEventTx(tx, log, nil, "outcome", log.CostReconcileState, GatewayRequestCostReconcileTerminal, GatewayRequestCostReconcileOutcomeTerminalFailure, reasonCode, reconcileError, nil, nil)
	})
}

func (g *Gateway) MarkRequestCostReconcileConflict(id uint, retryAt time.Time, maxConflicts int) (bool, error) {
	return g.markRequestCostReconcileConflict(id, nil, retryAt, maxConflicts)
}

func (g *Gateway) MarkClaimedRequestCostReconcileConflict(id uint, leaseUntil time.Time, retryAt time.Time, maxConflicts int) (bool, error) {
	leaseUntil = leaseUntil.UTC()
	return g.markRequestCostReconcileConflict(id, &leaseUntil, retryAt, maxConflicts)
}

func (g *Gateway) markRequestCostReconcileConflict(id uint, expectedLease *time.Time, retryAt time.Time, maxConflicts int) (bool, error) {
	if id == 0 || maxConflicts <= 0 {
		return false, nil
	}
	abandoned := false
	err := g.db.Transaction(func(tx *gorm.DB) error {
		query := tx.Model(&GatewayRequestLog{}).Where("id = ? AND cost_reconcile_state <> ?", id, GatewayRequestCostReconcileClaimed)
		if expectedLease != nil {
			query = query.Where("cost_reconcile_at = ?", *expectedLease)
		}
		result := query.Updates(map[string]any{
			"cost_reconcile_at":            retryAt,
			"cost_reconcile_tries":         gorm.Expr("cost_reconcile_tries + 1"),
			"cost_reconcile_conflicts":     gorm.Expr("cost_reconcile_conflicts + 1"),
			"cost_reconcile_state":         GatewayRequestCostReconcileRetryScheduled,
			"cost_reconcile_outcome":       GatewayRequestCostReconcileOutcomeConflictRetry,
			"cost_reconcile_last_event_at": gorm.Expr("clock_timestamp()"),
		})
		if result.Error != nil {
			return result.Error
		}
		if expectedLease != nil && result.RowsAffected != 1 {
			return ErrRequestCostReconcileLeaseLost
		}
		var current GatewayRequestLog
		if err := tx.Select("cost_reconcile_conflicts").First(&current, id).Error; err != nil {
			return err
		}
		if current.CostReconcileConflicts < maxConflicts {
			return nil
		}
		abandoned = true
		return tx.Model(&GatewayRequestLog{}).Where("id = ?", id).Updates(map[string]any{
			"cost_reconcile_at":            nil,
			"cost_reconcile_error":         gatewayCostReconcileConflictError,
			"cost_reconcile_state":         GatewayRequestCostReconcileTerminal,
			"cost_reconcile_outcome":       GatewayRequestCostReconcileOutcomeTerminalConflict,
			"cost_reconcile_last_event_at": gorm.Expr("clock_timestamp()"),
		}).Error
	})
	return abandoned, err
}

func (g *Gateway) ClaimRequestCostReconcile(id uint, now time.Time, leaseUntil time.Time) (bool, error) {
	result := g.db.Model(&GatewayRequestLog{}).
		Where("id = ? AND cost_reconcile_state <> ? AND cost_reconcile_at IS NOT NULL AND cost_reconcile_at <= ?", id, GatewayRequestCostReconcileClaimed, now).
		Update("cost_reconcile_at", leaseUntil)
	return result.RowsAffected == 1, result.Error
}

func (g *Gateway) ListPendingRequestCostReconciles(limit int, now time.Time) ([]GatewayRequestLog, error) {
	if limit <= 0 {
		limit = 20
	}
	var logs []GatewayRequestLog
	err := g.db.Where("channel_id > 0 AND cost_reconcile_at IS NOT NULL AND cost_reconcile_at <= ? AND "+gatewayRequestCostReconcileEligibleSQL(""), now).
		Order("cost_reconcile_at ASC, id ASC").Limit(limit).Find(&logs).Error
	return logs, err
}

// ClearIneligibleRequestCostReconciles repairs rows that were scheduled by an
// older writer even though the completed request cannot enter the reconciler's
// successful-request queue. The bounded batch keeps startup and the worker
// loop from turning historical cleanup into an unbounded table update.
func (g *Gateway) ClearIneligibleRequestCostReconciles(limit int) (int64, error) {
	if g == nil || g.db == nil {
		return 0, errors.New("gateway store is unavailable")
	}
	if limit <= 0 {
		limit = 100
	}
	var cleared int64
	err := g.db.Transaction(func(tx *gorm.DB) error {
		var logs []GatewayRequestLog
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where("cost_reconcile_at IS NOT NULL AND cost_reconcile_state IN (?, ?) AND NOT ("+gatewayRequestCostReconcileEligibleSQL("")+")", GatewayRequestCostReconcilePending, GatewayRequestCostReconcileRetryScheduled).
			Order("id ASC").Limit(limit).Find(&logs).Error; err != nil || len(logs) == 0 {
			return err
		}
		databaseNow, err := databaseClockNowTx(tx)
		if err != nil {
			return err
		}
		for _, log := range logs {
			outcome := GatewayRequestCostReconcileOutcomeIneligible
			reasonCode := GatewayRequestCostReconcileReasonIneligible
			errorSummary := strings.TrimSpace(log.CostReconcileError)
			if gatewayRequestCostReconcileBlockedByPricingFence(&log) {
				outcome = GatewayRequestCostReconcileOutcomePricingUnavailable
				reasonCode = GatewayRequestCostReconcileReasonPricingUnavailable
				errorSummary = gatewayCostReconcilePricingUnavailableError
			} else if errorSummary == "" {
				errorSummary = gatewayCostReconcileIneligibleError
			}
			updates := map[string]any{
				"cost_reconcile_at":            nil,
				"cost_reconcile_state":         GatewayRequestCostReconcileTerminal,
				"cost_reconcile_outcome":       outcome,
				"cost_reconcile_last_event_at": databaseNow,
				"cost_reconcile_error":         errorSummary,
			}
			mergeGatewayRequestCostReconcileUpdates(updates, clearGatewayRequestCostReconcileClaimUpdates())
			result := tx.Model(&GatewayRequestLog{}).
				Where("id = ? AND cost_reconcile_state <> ?", log.ID, GatewayRequestCostReconcileClaimed).
				Updates(updates)
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				continue
			}
			if err := appendGatewayRequestCostReconcileEventTx(tx, log, nil, "outcome", log.CostReconcileState, GatewayRequestCostReconcileTerminal, outcome, reasonCode, errorSummary, nil, nil); err != nil {
				return err
			}
			cleared++
		}
		return nil
	})
	return cleared, err
}

// RequeueHistoricalRequestCostReconciles gives old, conservatively terminated
// usage lookups one bounded recovery pass after the reconciler's retention
// window is extended. Only provider lookup incompleteness is eligible; a
// duplicate usage identity remains terminal because retrying it cannot make
// the billing assignment safe. The append-only event fence prevents the same
// historical row from being requeued repeatedly on every worker tick.
func (g *Gateway) RequeueHistoricalRequestCostReconciles(limit int, since, before time.Time) (int64, error) {
	if g == nil || g.db == nil {
		return 0, errors.New("gateway store is unavailable")
	}
	if limit <= 0 {
		limit = 100
	}
	if before.IsZero() {
		before = time.Now().UTC()
	}
	if since.IsZero() {
		since = before.Add(-7 * 24 * time.Hour)
	}
	since = since.UTC()
	before = before.UTC()
	if !since.Before(before) {
		return 0, nil
	}

	var requeued int64
	err := g.db.Transaction(func(tx *gorm.DB) error {
		var logs []GatewayRequestLog
		err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where("cost_reconcile_at IS NULL AND cost_reconcile_state = ? AND created_at >= ? AND created_at < ?", GatewayRequestCostReconcileTerminal, since, before).
			Where("(LOWER(COALESCE(cost_reconcile_error, '')) LIKE ? OR LOWER(COALESCE(cost_reconcile_error, '')) LIKE ?)", "%usage record not found%", "%usage scan incomplete%").
			Where("NOT EXISTS (SELECT 1 FROM gateway_request_cost_reconcile_events AS history WHERE history.request_log_id = gateway_request_logs.id AND history.reason_code = ?)", GatewayRequestCostReconcileReasonHistoricalBackfill).
			Where(gatewayRequestCostReconcileEligibleSQL("")).
			Order("created_at ASC, id ASC").Limit(limit).Find(&logs).Error
		if err != nil || len(logs) == 0 {
			return err
		}
		databaseNow, err := databaseClockNowTx(tx)
		if err != nil {
			return err
		}
		for _, log := range logs {
			updates := map[string]any{
				"cost_reconcile_at":            databaseNow,
				"cost_reconcile_tries":         0,
				"cost_reconcile_error":         "",
				"cost_reconcile_state":         GatewayRequestCostReconcilePending,
				"cost_reconcile_outcome":       GatewayRequestCostReconcileOutcomeNone,
				"cost_reconcile_last_event_at": databaseNow,
			}
			mergeGatewayRequestCostReconcileUpdates(updates, clearGatewayRequestCostReconcileClaimUpdates())
			result := tx.Model(&GatewayRequestLog{}).
				Where("id = ? AND cost_reconcile_state = ? AND cost_reconcile_at IS NULL", log.ID, GatewayRequestCostReconcileTerminal).
				Updates(updates)
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				continue
			}
			if err := appendGatewayRequestCostReconcileEventTx(tx, log, nil, "scheduled", GatewayRequestCostReconcileTerminal, GatewayRequestCostReconcilePending, GatewayRequestCostReconcileOutcomeNone, GatewayRequestCostReconcileReasonHistoricalBackfill, "", &databaseNow, nil); err != nil {
				return err
			}
			requeued++
		}
		return nil
	})
	return requeued, err
}

type GatewayRequestLogItem struct {
	ID                                    uint              `json:"id"`
	DownstreamKeyID                       uint              `json:"downstream_key_id"`
	DownstreamKeyName                     string            `json:"downstream_key_name"`
	DownstreamKeyGroupName                string            `json:"downstream_key_group_name"`
	ChannelID                             uint              `json:"channel_id"`
	ChannelName                           string            `json:"channel_name"`
	GroupName                             string            `json:"group_name"`
	GroupRatio                            float64           `json:"group_ratio"`
	CompletionRatio                       float64           `json:"completion_ratio"`
	RawGroupRatio                         *float64          `json:"raw_group_ratio,omitempty"`
	RawCompletionRatio                    *float64          `json:"raw_completion_ratio,omitempty"`
	FundingFactor                         *float64          `json:"funding_factor,omitempty"`
	CostBasisVersion                      uint64            `json:"cost_basis_version"`
	CostBasisSource                       string            `json:"cost_basis_source,omitempty"`
	ModelName                             string            `json:"model_name"`
	UpstreamModelName                     string            `json:"upstream_model_name,omitempty"`
	UpstreamResponseModel                 string            `json:"upstream_response_model,omitempty"`
	ReasoningEffort                       string            `json:"reasoning_effort"`
	RequestedModelName                    string            `json:"requested_model_name,omitempty"`
	RequestedReasoningEffort              string            `json:"requested_reasoning_effort,omitempty"`
	ModelMappingStrategy                  string            `json:"model_mapping_strategy,omitempty"`
	ModelMappingDetails                   string            `json:"model_mapping_details,omitempty"`
	Method                                string            `json:"method"`
	Path                                  string            `json:"path"`
	StatusCode                            int               `json:"status_code"`
	FirstByteMs                           int64             `json:"first_byte_ms"`
	DownstreamFirstVisibleMs              *int64            `json:"downstream_first_visible_ms,omitempty"`
	LatencyMs                             int64             `json:"latency_ms"`
	PromptTokens                          int64             `json:"prompt_tokens"`
	CompletionTokens                      int64             `json:"completion_tokens"`
	CacheCreationTokens                   int64             `json:"cache_creation_tokens"`
	CacheReadTokens                       int64             `json:"cache_read_tokens"`
	TotalTokens                           int64             `json:"total_tokens"`
	Cost                                  float64           `json:"cost"`
	RawCost                               *float64          `json:"raw_cost,omitempty"`
	CostEstimated                         bool              `json:"cost_estimated"`
	CostSource                            string            `json:"cost_source"`
	CostStatus                            GatewayCostStatus `json:"cost_status"`
	CostUnavailableReason                 string            `json:"cost_unavailable_reason,omitempty"`
	CalculatedCost                        *float64          `json:"calculated_cost,omitempty"`
	CalculatedCostStatus                  GatewayCostStatus `json:"calculated_cost_status,omitempty"`
	CalculatedCostUnavailableReason       string            `json:"calculated_cost_unavailable_reason,omitempty"`
	UpstreamCost                          *float64          `json:"upstream_cost,omitempty"`
	UpstreamCostStatus                    GatewayCostStatus `json:"upstream_cost_status,omitempty"`
	DisplayCost                           *float64          `json:"display_cost,omitempty"`
	CostDifference                        *float64          `json:"cost_difference,omitempty"`
	CostDifferenceRatio                   *float64          `json:"cost_difference_ratio,omitempty"`
	CostDifferenceMaterial                bool              `json:"cost_difference_material"`
	PriceFactID                           *uint64           `json:"price_fact_id,omitempty"`
	PriceFactVersion                      uint64            `json:"price_fact_version,omitempty"`
	PriceFactSchemaVersion                int               `json:"price_fact_schema_version,omitempty"`
	PriceCostUnitEpoch                    uint64            `json:"price_cost_unit_epoch,omitempty"`
	PriceModelName                        string            `json:"price_model_name,omitempty"`
	PriceOperation                        string            `json:"price_operation,omitempty"`
	PriceProtocol                         string            `json:"price_protocol,omitempty"`
	PriceInputUnit                        *float64          `json:"-"`
	PriceOutputUnit                       *float64          `json:"-"`
	PriceCacheReadUnit                    *float64          `json:"-"`
	PriceCacheWriteUnit                   *float64          `json:"-"`
	PriceLongContextThresholdTokens       *int64            `json:"-"`
	PriceLongContextInputUnit             *float64          `json:"-"`
	PriceLongContextOutputUnit            *float64          `json:"-"`
	PriceLongContextCacheReadUnit         *float64          `json:"-"`
	PriceLongContextCacheWriteUnit        *float64          `json:"-"`
	PriceOperationUnit                    *float64          `json:"-"`
	PriceCurrency                         string            `json:"price_currency,omitempty"`
	PriceUnit                             string            `json:"price_unit,omitempty"`
	PriceSource                           string            `json:"price_source,omitempty"`
	PriceObservedAt                       *time.Time        `json:"price_observed_at,omitempty"`
	UsageSchema                           string            `json:"usage_schema,omitempty"`
	UsageSchemaVersion                    int               `json:"usage_schema_version,omitempty"`
	UsageInputPresent                     bool              `json:"-"`
	UsageOutputPresent                    bool              `json:"-"`
	UsageCacheReadPresent                 bool              `json:"-"`
	UsageCacheWritePresent                bool              `json:"-"`
	UsageOperationPresent                 bool              `json:"-"`
	OperationUnits                        *int64            `json:"-"`
	CostBillable                          bool              `json:"cost_billable"`
	CostReconcileAt                       *time.Time        `json:"cost_reconcile_at,omitempty"`
	CostReconcileTries                    int               `json:"cost_reconcile_tries"`
	CostReconcileError                    string            `json:"cost_reconcile_error,omitempty"`
	UpstreamKeyID                         int64             `json:"upstream_key_id,omitempty"`
	UpstreamKeyName                       string            `json:"upstream_key_name,omitempty"`
	UpstreamRequestID                     string            `json:"upstream_request_id,omitempty"`
	UpstreamAccepted                      bool              `json:"upstream_accepted"`
	BillingCommitted                      bool              `json:"billing_committed"`
	ClientIP                              string            `json:"client_ip,omitempty"`
	ClientIPSource                        string            `json:"client_ip_source,omitempty"`
	RouteReason                           string            `json:"route_reason"`
	RouteSelectionRank                    uint              `json:"route_selection_rank,omitempty"`
	SessionAffinityOriginRequestID        uint              `json:"session_affinity_origin_request_id,omitempty"`
	SessionAffinityExpiresAt              *time.Time        `json:"session_affinity_expires_at,omitempty"`
	SessionAffinityOverriddenCandidates   string            `json:"-"`
	RouteDetails                          string            `json:"route_details"`
	RouteAttemptedChannels                string            `json:"route_attempted_channels,omitempty"`
	LogicalRequestTraceID                 string            `json:"logical_request_trace_id,omitempty"`
	TelemetryVersion                      int               `json:"telemetry_version"`
	RoutingMode                           string            `json:"routing_mode,omitempty"`
	GatewayApp                            string            `json:"gateway_app,omitempty"`
	TerminalFailureCode                   string            `json:"terminal_failure_code,omitempty"`
	PriorPhysicallyDispatchedFailureCount int               `json:"prior_physically_dispatched_failure_count"`
	RealFallback                          bool              `json:"real_fallback"`
	RouteExclusionSnapshotVersion         int               `json:"route_exclusion_snapshot_version"`
	RouteExclusionsSnapshot               string            `json:"-"`
	Error                                 string            `json:"error,omitempty"`
	CreatedAt                             time.Time         `json:"created_at"`
}

type GatewayRequestLogSummary struct {
	TotalRequests              int64      `json:"total_requests"`
	Success                    int64      `json:"success"`
	Failed                     int64      `json:"failed"`
	Canceled                   int64      `json:"canceled"`
	TotalTokens                int64      `json:"total_tokens"`
	PromptTokens               int64      `json:"prompt_tokens"`
	CompletionTokens           int64      `json:"completion_tokens"`
	CacheCreationTokens        int64      `json:"cache_creation_tokens"`
	CacheReadTokens            int64      `json:"cache_read_tokens"`
	TotalCost                  float64    `json:"total_cost"`
	EstimatedCost              float64    `json:"estimated_cost"`
	ReportedCost               float64    `json:"reported_cost"`
	ReconciledCost             float64    `json:"reconciled_cost"`
	ActualCost                 float64    `json:"actual_cost"`
	UnavailableRequests        int64      `json:"unavailable_requests"`
	DispatchEvidencePending    int64      `json:"dispatch_evidence_pending"`
	DispatchEvidenceActual     float64    `json:"dispatch_evidence_actual"`
	DispatchEvidenceReported   float64    `json:"dispatch_evidence_reported"`
	DispatchEvidenceReconciled float64    `json:"dispatch_evidence_reconciled"`
	AvgLatency                 float64    `json:"avg_latency_ms"`
	WindowSeconds              float64    `json:"window_seconds"`
	PerformanceWindowSeconds   float64    `json:"performance_window_seconds"`
	RPM                        float64    `json:"rpm"`
	TPM                        float64    `json:"tpm"`
	PerformanceRequests        int64      `json:"performance_requests"`
	PerformanceTokens          int64      `json:"performance_tokens"`
	PerformanceAvgLatency      float64    `json:"performance_avg_latency_ms"`
	EstimatedConcurrency       float64    `json:"estimated_concurrency"`
	CurrentConcurrency         int64      `json:"current_concurrency"`
	CurrentConcurrencyScope    string     `json:"current_concurrency_scope"`
	FirstRequestAt             *time.Time `json:"first_request_at,omitempty"`
	LastRequestAt              *time.Time `json:"last_request_at,omitempty"`
}

type gatewayRequestLogSummaryScan struct {
	TotalRequests              int64
	Success                    int64
	Failed                     int64
	Canceled                   int64
	TotalTokens                int64
	PromptTokens               int64
	CompletionTokens           int64
	CacheCreationTokens        int64
	CacheReadTokens            int64
	TotalCost                  float64
	EstimatedCost              float64
	ReportedCost               float64
	ReconciledCost             float64
	ActualCost                 float64
	UnavailableRequests        int64
	DispatchEvidencePending    int64
	DispatchEvidenceActual     float64
	DispatchEvidenceReported   float64
	DispatchEvidenceReconciled float64
	AvgLatency                 float64
	WindowSeconds              float64
	PerformanceWindowSeconds   float64
	RPM                        float64
	TPM                        float64
	PerformanceRequests        int64
	PerformanceTokens          int64
	PerformanceAvgLatency      float64
	EstimatedConcurrency       float64
	CurrentConcurrency         int64
	FirstRequestAt             string
	LastRequestAt              string
}

type gatewayRequestLogPerformanceScan struct {
	Requests   int64
	Tokens     int64
	AvgLatency float64
}

type GatewayRequestLogFilter struct {
	Start             *time.Time
	End               *time.Time
	TimeZone          string
	DownstreamKeyID   uint
	ModelName         string
	ChannelID         uint
	GroupName         string
	GroupUnassigned   bool
	UpstreamRequestID string
	UpstreamKeyName   string
	Result            string
	RouteReason       string
	SortBy            string
	SortOrder         string
}

type GatewayRequestLogOptions struct {
	Filter GatewayRequestLogFilter
}

type GatewayRequestLogTrendPoint struct {
	Bucket                     string  `json:"bucket"`
	Requests                   int64   `json:"requests"`
	Success                    int64   `json:"success"`
	Failed                     int64   `json:"failed"`
	Canceled                   int64   `json:"canceled"`
	TotalTokens                int64   `json:"total_tokens"`
	PromptTokens               int64   `json:"prompt_tokens"`
	CompletionTokens           int64   `json:"completion_tokens"`
	CacheCreationTokens        int64   `json:"cache_creation_tokens"`
	CacheReadTokens            int64   `json:"cache_read_tokens"`
	TotalCost                  float64 `json:"total_cost"`
	EstimatedCost              float64 `json:"estimated_cost"`
	ReportedCost               float64 `json:"reported_cost"`
	ReconciledCost             float64 `json:"reconciled_cost"`
	ActualCost                 float64 `json:"actual_cost"`
	UnavailableRequests        int64   `json:"unavailable_requests"`
	DispatchEvidencePending    int64   `json:"dispatch_evidence_pending"`
	DispatchEvidenceActual     float64 `json:"dispatch_evidence_actual"`
	DispatchEvidenceReported   float64 `json:"dispatch_evidence_reported"`
	DispatchEvidenceReconciled float64 `json:"dispatch_evidence_reconciled"`
	AvgLatency                 float64 `json:"avg_latency_ms"`
}

const (
	GatewayTokenPriceInput         = "input"
	GatewayTokenPriceOutput        = "output"
	GatewayTokenPriceCacheCreation = "cache_creation"
	GatewayTokenPriceCacheRead     = "cache_read"
)

// GatewayRequestLogModelUnitPriceTrendPoint reports one token kind for one
// actual gateway model in one time bucket. Cost fields are nullable because
// cache usage can be reported without an independent cache charge; callers
// must not manufacture a cache price from the request's mixed total cost.
type GatewayRequestLogModelUnitPriceTrendPoint struct {
	Bucket              string   `json:"bucket"`
	ModelFamily         string   `json:"model_family"`
	ModelName           string   `json:"model_name"`
	TokenType           string   `json:"token_type"`
	TotalTokens         int64    `json:"total_tokens"`
	TotalCost           *float64 `json:"total_cost"`
	CostPerMillionToken *float64 `json:"cost_per_million_tokens"`
}

type GatewayRequestLogBreakdownItem struct {
	Name                       string  `json:"name"`
	ChannelID                  uint    `json:"channel_id,omitempty"`
	DownstreamKeyID            uint    `json:"downstream_key_id,omitempty"`
	Requests                   int64   `json:"requests"`
	Success                    int64   `json:"success"`
	Failed                     int64   `json:"failed"`
	Canceled                   int64   `json:"canceled"`
	TotalTokens                int64   `json:"total_tokens"`
	PromptTokens               int64   `json:"prompt_tokens"`
	CompletionTokens           int64   `json:"completion_tokens"`
	CacheCreationTokens        int64   `json:"cache_creation_tokens"`
	CacheReadTokens            int64   `json:"cache_read_tokens"`
	TotalCost                  float64 `json:"total_cost"`
	EstimatedCost              float64 `json:"estimated_cost"`
	ReportedCost               float64 `json:"reported_cost"`
	ReconciledCost             float64 `json:"reconciled_cost"`
	ActualCost                 float64 `json:"actual_cost"`
	UnavailableRequests        int64   `json:"unavailable_requests"`
	DispatchEvidencePending    int64   `json:"dispatch_evidence_pending"`
	DispatchEvidenceActual     float64 `json:"dispatch_evidence_actual"`
	DispatchEvidenceReported   float64 `json:"dispatch_evidence_reported"`
	DispatchEvidenceReconciled float64 `json:"dispatch_evidence_reconciled"`
	AvgLatency                 float64 `json:"avg_latency_ms"`
}

// GatewayRequestLogGroupCacheRatio contains weighted cache metrics for one
// channel and upstream group. Ratio is nil when no request in the selected
// scope supplied a positive input-token count; the group remains visible so a
// missing denominator is never presented as a misleading zero percent.
type GatewayRequestLogGroupCacheRatio struct {
	ChannelID           uint     `json:"channel_id"`
	ChannelName         string   `json:"channel_name"`
	GroupName           string   `json:"group_name"`
	CacheReadTokens     int64    `json:"cache_read_tokens"`
	CacheCreationTokens int64    `json:"cache_creation_tokens"`
	PromptTokens        int64    `json:"prompt_tokens"`
	ValidRequests       int64    `json:"valid_requests"`
	Ratio               *float64 `json:"ratio"`
	channelNameSnapshot string
}

type GatewayRequestLogAnalytics struct {
	Trend            []GatewayRequestLogTrendPoint               `json:"trend"`
	ModelUnitPrices  []GatewayRequestLogModelUnitPriceTrendPoint `json:"model_unit_prices"`
	Channels         []GatewayRequestLogBreakdownItem            `json:"channels"`
	Models           []GatewayRequestLogBreakdownItem            `json:"models"`
	ModelsByTokens   []GatewayRequestLogBreakdownItem            `json:"models_by_tokens"`
	Keys             []GatewayRequestLogBreakdownItem            `json:"keys"`
	CostSources      []GatewayRequestLogBreakdownItem            `json:"cost_sources"`
	RouteReasons     []GatewayRequestLogBreakdownItem            `json:"route_reasons"`
	GroupCacheRatios []GatewayRequestLogGroupCacheRatio          `json:"group_cache_ratios"`
}

type GatewayUsageSummary struct {
	IsActive            bool       `json:"is_active"`
	Used                float64    `json:"used"`
	Total               float64    `json:"total"`
	Remaining           float64    `json:"remaining"`
	Unit                string     `json:"unit"`
	Requests            int64      `json:"requests"`
	Success             int64      `json:"success"`
	Failed              int64      `json:"failed"`
	AvgLatency          float64    `json:"avg_latency_ms"`
	LastUsedAt          *time.Time `json:"last_used_at,omitempty"`
	TotalTokens         int64      `json:"total_tokens"`
	PromptTokens        int64      `json:"prompt_tokens"`
	CompletionTokens    int64      `json:"completion_tokens"`
	TotalCost           float64    `json:"total_cost"`
	ActualCost          float64    `json:"actual_cost"`
	EstimatedCost       float64    `json:"estimated_cost"`
	ReportedCost        float64    `json:"reported_cost"`
	ReconciledCost      float64    `json:"reconciled_cost"`
	UnavailableRequests int64      `json:"unavailable_requests"`
	Extra               string     `json:"extra"`
}

type GatewayLatencyStat struct {
	ChannelID  uint
	GroupName  string
	AvgLatency float64
	Requests   int64
}

type GatewayLatencyLog struct {
	ID        uint
	ChannelID uint
	GroupName string
	LatencyMs int64
	CreatedAt time.Time
}

type GatewayRecentRoute struct {
	ID                     uint
	ChannelID              uint
	GroupName              string
	RouteReason            string
	SelectionRank          uint
	RouteDetails           string
	RouteAttemptedChannels string
	CreatedAt              time.Time
}

const (
	gatewayRouteReasonClientCanceled     = "client_canceled"
	gatewayRouteReasonLatencyProbe       = "latency_probe"
	gatewayRouteReasonKeyTest            = "api_test"
	gatewayPerformanceWindow             = 5 * time.Minute
	GatewayRequestResultSuccess          = "success"
	GatewayRequestResultFailed           = "failed"
	GatewayRequestResultCanceled         = "canceled"
	gatewayRequestResultDispatchEvidence = "evidence"
	GatewayRequestLogSortCreatedAt       = "created_at"
	GatewayRequestLogSortModelName       = "model_name"
	GatewayRequestLogSortLatencyMs       = "latency_ms"
	GatewayRequestLogSortTotalTokens     = "total_tokens"
	GatewayRequestLogSortCost            = "cost"
	GatewayRequestLogSortStatusCode      = "status_code"
	GatewayRequestLogSortAscending       = "asc"
	GatewayRequestLogSortDescending      = "desc"
)

func normalizedGatewayRequestRecordKind(log *GatewayRequestLog) GatewayRequestRecordKind {
	if log == nil {
		return ""
	}
	kind := GatewayRequestRecordKind(strings.ToLower(strings.TrimSpace(string(log.RecordKind))))
	if kind == "" {
		return GatewayRequestRecordKindRequest
	}
	return kind
}

func isGatewayRequestRecord(log *GatewayRequestLog) bool {
	return normalizedGatewayRequestRecordKind(log) == GatewayRequestRecordKindRequest
}

func isGatewayDispatchCostEvidenceRecord(log *GatewayRequestLog) bool {
	return normalizedGatewayRequestRecordKind(log) == GatewayRequestRecordKindDispatchCostEvidence
}

func gatewayRequestRecordSQL(alias string) string {
	prefix := ""
	if strings.TrimSpace(alias) != "" {
		prefix = strings.TrimSpace(alias) + "."
	}
	return "COALESCE(" + prefix + "record_kind, '" + string(GatewayRequestRecordKindRequest) + "') = '" + string(GatewayRequestRecordKindRequest) + "'"
}

func gatewayDispatchCostEvidenceRecordSQL(alias string) string {
	prefix := ""
	if strings.TrimSpace(alias) != "" {
		prefix = strings.TrimSpace(alias) + "."
	}
	return "COALESCE(" + prefix + "record_kind, '" + string(GatewayRequestRecordKindRequest) + "') = '" + string(GatewayRequestRecordKindDispatchCostEvidence) + "'"
}

func gatewayPendingDispatchCostEvidenceSQL(alias string) string {
	prefix := ""
	if strings.TrimSpace(alias) != "" {
		prefix = strings.TrimSpace(alias) + "."
	}
	status := "LOWER(TRIM(COALESCE(" + prefix + "cost_status, '')))"
	usageID := "TRIM(COALESCE(" + prefix + "upstream_usage_id, ''))"
	return "(" + gatewayDispatchCostEvidenceRecordSQL(alias) +
		" AND " + status + " = '" + string(GatewayCostStatusNone) + "'" +
		" AND " + usageID + " = '' AND " + prefix + "raw_cost IS NULL)"
}

// gatewayRequestFundingCorrectionEligibleSQL keeps unreconciled dispatch
// evidence frozen. Once exact provider reconciliation has supplied immutable
// raw cost evidence, later funding corrections may safely re-convert it.
func gatewayRequestFundingCorrectionEligibleSQL(alias string) string {
	prefix := ""
	if strings.TrimSpace(alias) != "" {
		prefix = strings.TrimSpace(alias) + "."
	}
	status := "LOWER(TRIM(COALESCE(" + prefix + "cost_status, '')))"
	usageID := "TRIM(COALESCE(" + prefix + "upstream_usage_id, ''))"
	return "((" + gatewayRequestRecordSQL(alias) + " OR (" + gatewayDispatchCostEvidenceRecordSQL(alias) +
		" AND " + status + " = '" + string(GatewayCostStatusReconciled) + "'" +
		" AND " + usageID + " <> '' AND " + prefix + "raw_cost IS NOT NULL))" +
		" AND " + prefix + "price_fact_id IS NULL)"
}

func normalizeGatewayRequestRecord(log *GatewayRequestLog) error {
	if log == nil {
		return fmt.Errorf("%w: request log is required", ErrGatewayRequestRecordInvalid)
	}
	if err := normalizeGatewayRequestLogTelemetry(log); err != nil {
		return fmt.Errorf("%w: %w", ErrGatewayRequestRecordInvalid, err)
	}
	if err := normalizeGatewayRequestPricingSnapshot(log); err != nil {
		return fmt.Errorf("%w: %w", ErrGatewayRequestRecordInvalid, err)
	}
	providerRequestID, err := providerid.Normalize(log.UpstreamRequestID)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrGatewayRequestRecordInvalid, err)
	}
	log.UpstreamRequestID = providerRequestID
	log.RecordKind = normalizedGatewayRequestRecordKind(log)
	switch log.RecordKind {
	case GatewayRequestRecordKindRequest:
		return nil
	case GatewayRequestRecordKindDispatchCostEvidence:
		if log.UpstreamRequestID == "" {
			return fmt.Errorf("%w: dispatch cost evidence requires a provider request ID", ErrGatewayRequestRecordInvalid)
		}
		if log.UpstreamAccepted || log.BillingCommitted {
			return fmt.Errorf("%w: provider request ID evidence cannot claim upstream acceptance or billing commitment", ErrGatewayRequestRecordInvalid)
		}
		// Header-only provider IDs are exact-reconciliation clues, not local
		// charges. Only the cost worker may later replace these zero values with
		// provider-matched usage.
		log.Cost = 0
		log.RawCost = nil
		log.CostEstimated = false
		log.CostSource = ""
		log.CostStatus = GatewayCostStatusNone
		log.CostReconcileLegacyPending = false
		log.UpstreamUsageID = nil
		log.StatusCode = 0
		log.FirstByteMs = 0
		log.LatencyMs = 0
		log.PromptTokens = 0
		log.CompletionTokens = 0
		log.CacheCreationTokens = 0
		log.CacheReadTokens = 0
		log.TotalTokens = 0
		log.Error = ""
		log.CostReconcileTries = 0
		log.CostReconcileConflicts = 0
		log.CostReconcileError = ""
		if log.TelemetryVersion != 0 {
			log.DownstreamFirstVisibleMs = nil
			log.TerminalFailureCode = "dispatch_cost_evidence"
			log.PriorPhysicallyDispatchedFailureCount = 0
			log.RealFallback = false
		}
		return nil
	default:
		return fmt.Errorf("%w: unsupported record kind %q", ErrGatewayRequestRecordInvalid, log.RecordKind)
	}
}

func normalizeGatewayRequestLogTelemetry(log *GatewayRequestLog) error {
	if log == nil {
		return errors.New("request telemetry log is required")
	}
	if log.DownstreamFirstVisibleMs != nil {
		if *log.DownstreamFirstVisibleMs < 0 {
			return errors.New("downstream first visible latency must not be negative")
		}
		visible := *log.DownstreamFirstVisibleMs
		log.DownstreamFirstVisibleMs = &visible
	}
	if log.SessionAffinityExpiresAt != nil {
		expiresAt := log.SessionAffinityExpiresAt.UTC()
		if expiresAt.IsZero() {
			return errors.New("session affinity expiry is invalid")
		}
		log.SessionAffinityExpiresAt = &expiresAt
	}
	affinityCandidates, err := normalizeGatewayRequestDispatchAffinityCandidates(log.SessionAffinityOverriddenCandidates)
	if err != nil {
		return err
	}
	log.SessionAffinityOverriddenCandidates = affinityCandidates
	if log.SessionAffinityOriginRequestID == 0 && (log.SessionAffinityExpiresAt != nil || affinityCandidates != "") {
		return errors.New("session affinity audit requires an origin request")
	}
	version, traceID, routingMode, gatewayApp, _, err := normalizeGatewayRequestTelemetry(
		log.TelemetryVersion,
		log.LogicalRequestTraceID,
		log.RoutingMode,
		log.GatewayApp,
		0,
	)
	if err != nil {
		return err
	}
	log.TelemetryVersion = version
	log.LogicalRequestTraceID = traceID
	log.RoutingMode = routingMode
	log.GatewayApp = gatewayApp
	log.RouteExclusionSnapshotVersion, log.RouteExclusionsSnapshot, err = normalizeGatewayRouteExclusionSnapshot(
		log.RouteExclusionSnapshotVersion,
		log.RouteExclusionsSnapshot,
	)
	if err != nil {
		return err
	}
	if version == 0 && log.RouteExclusionSnapshotVersion != 0 ||
		version != 0 && log.RouteExclusionSnapshotVersion != GatewayRouteExclusionSnapshotVersion {
		return errors.New("route exclusion snapshot coverage does not match request telemetry")
	}
	log.TerminalFailureCode = strings.ToLower(strings.TrimSpace(log.TerminalFailureCode))
	if len(log.TerminalFailureCode) > 64 {
		return errors.New("terminal failure code exceeds its safe length")
	}
	for _, value := range log.TerminalFailureCode {
		if (value < 'a' || value > 'z') && (value < '0' || value > '9') && value != '_' {
			return errors.New("terminal failure code is invalid")
		}
	}
	if log.PriorPhysicallyDispatchedFailureCount < 0 {
		return errors.New("prior physically dispatched failure count must not be negative")
	}
	if version == 0 {
		if log.DownstreamFirstVisibleMs != nil || log.TerminalFailureCode != "" || log.PriorPhysicallyDispatchedFailureCount != 0 || log.RealFallback {
			return errors.New("request telemetry facts require a telemetry version")
		}
		return nil
	}
	if log.RealFallback && routingMode != GatewayRoutingModeSmart {
		return errors.New("real fallback requires smart routing telemetry")
	}
	return nil
}

func IsSuccessfulGatewayRequest(log *GatewayRequestLog) bool {
	if !isGatewayRequestRecord(log) || log.StatusCode < 200 || log.StatusCode >= 400 {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(log.RouteReason), gatewayRouteReasonClientCanceled) {
		return false
	}
	errorText := strings.TrimSpace(log.Error)
	if errorText != "" {
		return false
	}
	return true
}

func isGatewayRequestCountTokens(log *GatewayRequestLog) bool {
	return log != nil && strings.EqualFold(strings.TrimSpace(log.Path), "/v1/messages/count_tokens")
}

func gatewayRequestNotCountTokensSQL(alias string) string {
	prefix := ""
	if strings.TrimSpace(alias) != "" {
		prefix = strings.TrimSpace(alias) + "."
	}
	return "LOWER(TRIM(COALESCE(" + prefix + "path, ''))) <> '/v1/messages/count_tokens'"
}

// IsGatewayRequestCostReconcileEligible is deliberately broader than the
// success predicate used by routing and reporting. A provider may accept or
// commit billing for a stream or image task before the downstream request later
// fails or is canceled; those explicit facts still require upstream cost
// reconciliation. Ordinary unaccepted failures remain excluded.
func IsGatewayRequestCostReconcileEligible(log *GatewayRequestLog) bool {
	if log == nil || isGatewayRequestCountTokens(log) || gatewayRequestCostReconcileBlockedByPricingFence(log) {
		return false
	}
	hasProviderRequestID := log.UpstreamRequestID != ""
	if isGatewayDispatchCostEvidenceRecord(log) {
		return hasProviderRequestID
	}
	return isGatewayRequestRecord(log) && (IsSuccessfulGatewayRequest(log) || log.BillingCommitted || log.UpstreamAccepted || log.CostReconcileLegacyPending || hasProviderRequestID)
}

func successfulGatewayRequestSQL(alias string) string {
	prefix := ""
	if strings.TrimSpace(alias) != "" {
		prefix = strings.TrimSpace(alias) + "."
	}
	return "(" + gatewayRequestRecordSQL(alias) + " AND " + prefix + "status_code >= 200 AND " + prefix + "status_code < 400 AND LOWER(TRIM(COALESCE(" + prefix + "route_reason, ''))) <> '" + gatewayRouteReasonClientCanceled + "' AND TRIM(COALESCE(" + prefix + "error, '')) = '')"
}

func gatewayRequestHasProviderRequestIDSQL(alias string) string {
	prefix := ""
	if strings.TrimSpace(alias) != "" {
		prefix = strings.TrimSpace(alias) + "."
	}
	return "COALESCE(" + prefix + "upstream_request_id, '') <> ''"
}

func gatewayRequestCostReconcileEligibleSQL(alias string) string {
	prefix := ""
	if strings.TrimSpace(alias) != "" {
		prefix = strings.TrimSpace(alias) + "."
	}
	providerRequestID := gatewayRequestHasProviderRequestIDSQL(alias)
	return "(" + gatewayRequestNotCountTokensSQL(alias) + " AND NOT " + gatewayRequestCostReconcileBlockedByPricingFenceSQL(alias) + " AND ((" + gatewayRequestRecordSQL(alias) + " AND (" + successfulGatewayRequestSQL(alias) +
		" OR COALESCE(" + prefix + "billing_committed, FALSE)" +
		" OR COALESCE(" + prefix + "upstream_accepted, FALSE)" +
		" OR COALESCE(" + prefix + "cost_reconcile_legacy_pending, FALSE)" +
		" OR " + providerRequestID + ")) OR (" + gatewayDispatchCostEvidenceRecordSQL(alias) + " AND " + providerRequestID + ")))"
}

// IsGatewayRequestPricingFenceUnavailable identifies requests whose
// local monetary basis was deliberately removed before physical dispatch. The
// provider request ID remains durable evidence, but a later current-account
// lookup cannot safely convert its raw amount into this request's local cost.
func IsGatewayRequestPricingFenceUnavailable(log *GatewayRequestLog) bool {
	if log == nil || gatewayRequestCostStatus(log) != GatewayCostStatusUnavailable {
		return false
	}
	return gatewayRequestPricingFenceUnavailableReason(log.CostUnavailableReason)
}

func gatewayRequestCostReconcileBlockedByPricingFence(log *GatewayRequestLog) bool {
	return IsGatewayRequestPricingFenceUnavailable(log)
}

func gatewayRequestCostReconcileBlockedByPricingFenceSQL(alias string) string {
	prefix := ""
	if strings.TrimSpace(alias) != "" {
		prefix = strings.TrimSpace(alias) + "."
	}
	status := "LOWER(TRIM(COALESCE(" + prefix + "cost_status, '')))"
	reason := "LOWER(TRIM(COALESCE(" + prefix + "cost_unavailable_reason, '')))"
	return "(" + status + " = '" + string(GatewayCostStatusUnavailable) + "' AND " + reason + " IN ('" +
		GatewayPricingUnavailableScopeIncomplete + "', '" + GatewayPricingUnavailableIdentityChanged + "'))"
}

func gatewayRequestHasReliableCostEvidence(log *GatewayRequestLog) bool {
	if log == nil {
		return false
	}
	status := GatewayCostStatus(strings.ToLower(strings.TrimSpace(string(log.CostStatus))))
	if status == GatewayCostStatusReported || status == GatewayCostStatusReconciled {
		return true
	}
	if log.UpstreamUsageID != nil && strings.TrimSpace(*log.UpstreamUsageID) != "" {
		return true
	}
	return isProviderReportedGatewayCostSource(log.CostSource)
}

func gatewayRequestHasReliableCostEvidenceSQL(alias string) string {
	prefix := ""
	if strings.TrimSpace(alias) != "" {
		prefix = strings.TrimSpace(alias) + "."
	}
	status := "LOWER(TRIM(COALESCE(" + prefix + "cost_status, '')))"
	return "(" + status + " IN ('" + string(GatewayCostStatusReported) + "', '" + string(GatewayCostStatusReconciled) + "')" +
		" OR (" + prefix + "upstream_usage_id IS NOT NULL AND TRIM(" + prefix + "upstream_usage_id) <> '')" +
		" OR LOWER(TRIM(COALESCE(" + prefix + "cost_source, ''))) IN (" + gatewayReportedCostSourcesSQLList + "))"
}

// IsGatewayRequestCostBillable controls monetary contributions without hiding
// request/result counters. Explicit current-release acceptance facts are
// sufficient; the migration-only legacy marker additionally requires durable
// cost evidence so an unexplained residual numeric value is never promoted to
// a bill.
func IsGatewayRequestCostBillable(log *GatewayRequestLog) bool {
	if !isGatewayRequestRecord(log) || isGatewayRequestCountTokens(log) {
		return false
	}
	if IsSuccessfulGatewayRequest(log) || log.BillingCommitted || log.UpstreamAccepted {
		return true
	}
	if log.UpstreamRequestID != "" && gatewayRequestHasReliableCostEvidence(log) {
		return true
	}
	return log.CostReconcileLegacyPending && gatewayRequestHasReliableCostEvidence(log)
}

func gatewayRequestCostBillableSQL(alias string) string {
	prefix := ""
	if strings.TrimSpace(alias) != "" {
		prefix = strings.TrimSpace(alias) + "."
	}
	return "(" + gatewayRequestRecordSQL(alias) + " AND " + gatewayRequestNotCountTokensSQL(alias) + " AND (" + successfulGatewayRequestSQL(alias) +
		" OR COALESCE(" + prefix + "billing_committed, FALSE)" +
		" OR COALESCE(" + prefix + "upstream_accepted, FALSE)" +
		" OR (" + gatewayRequestHasProviderRequestIDSQL(alias) + " AND " + gatewayRequestHasReliableCostEvidenceSQL(alias) + ")" +
		" OR (COALESCE(" + prefix + "cost_reconcile_legacy_pending, FALSE) AND " + gatewayRequestHasReliableCostEvidenceSQL(alias) + ")))"
}

func isGatewayDispatchCostEvidencePending(log *GatewayRequestLog) bool {
	return isGatewayDispatchCostEvidenceRecord(log) && log.UpstreamRequestID != "" && !gatewayRequestHasReliableCostEvidence(log)
}

func gatewayDispatchCostEvidenceActual(log *GatewayRequestLog) float64 {
	if !isGatewayDispatchCostEvidenceRecord(log) || !gatewayRequestHasReliableCostEvidence(log) || log.Cost < 0 {
		return 0
	}
	status := gatewayRequestCostStatus(log)
	if status != GatewayCostStatusReported && status != GatewayCostStatusReconciled {
		return 0
	}
	return log.Cost
}

func gatewayDispatchCostEvidenceByStatus(log *GatewayRequestLog, status GatewayCostStatus) float64 {
	if gatewayDispatchCostEvidenceActual(log) == 0 || gatewayRequestCostStatus(log) != status {
		return 0
	}
	return log.Cost
}

func gatewayDispatchCostEvidencePendingSQL(alias string) string {
	return "CASE WHEN " + gatewayDispatchCostEvidenceRecordSQL(alias) + " AND " + gatewayRequestHasProviderRequestIDSQL(alias) + " AND NOT " + gatewayRequestHasReliableCostEvidenceSQL(alias) + " THEN 1 ELSE 0 END"
}

func gatewayDispatchCostEvidenceActualSQL(alias string) string {
	prefix := ""
	if strings.TrimSpace(alias) != "" {
		prefix = strings.TrimSpace(alias) + "."
	}
	status := gatewayRequestEffectiveCostStatusSQL(alias)
	return "CASE WHEN " + gatewayDispatchCostEvidenceRecordSQL(alias) + " AND " + gatewayRequestHasReliableCostEvidenceSQL(alias) + " AND (" + status + ") IN ('" + string(GatewayCostStatusReported) + "', '" + string(GatewayCostStatusReconciled) + "') THEN COALESCE(" + prefix + "cost, 0) ELSE 0 END"
}

func gatewayDispatchCostEvidenceByStatusSQL(alias string, status GatewayCostStatus) string {
	prefix := ""
	if strings.TrimSpace(alias) != "" {
		prefix = strings.TrimSpace(alias) + "."
	}
	return "CASE WHEN " + gatewayDispatchCostEvidenceRecordSQL(alias) + " AND " + gatewayRequestHasReliableCostEvidenceSQL(alias) + " AND (" + gatewayRequestEffectiveCostStatusSQL(alias) + ") = '" + string(status) + "' THEN COALESCE(" + prefix + "cost, 0) ELSE 0 END"
}

func canceledGatewayRequestSQL(alias string) string {
	prefix := ""
	if strings.TrimSpace(alias) != "" {
		prefix = strings.TrimSpace(alias) + "."
	}
	return "(" + gatewayRequestRecordSQL(alias) + " AND (LOWER(TRIM(COALESCE(" + prefix + "route_reason, ''))) = '" + gatewayRouteReasonClientCanceled + "' OR LOWER(TRIM(COALESCE(" + prefix + "error, ''))) = 'context canceled' OR TRIM(COALESCE(" + prefix + "error, '')) = '客户端取消'))"
}

func failedGatewayRequestSQL(alias string) string {
	return "(" + gatewayRequestRecordSQL(alias) + " AND NOT (" + successfulGatewayRequestSQL(alias) + ") AND NOT " + canceledGatewayRequestSQL(alias) + ")"
}

// performanceGatewayRequestSQL matches completed model requests that can be
// compared with Sub2API's usage dashboard. Management calls such as
// /v1/models and latency probes are intentionally excluded from RPM/TPM.
func performanceGatewayRequestSQL(alias string) string {
	prefix := ""
	if strings.TrimSpace(alias) != "" {
		prefix = strings.TrimSpace(alias) + "."
	}
	return successfulGatewayRequestSQL(alias) +
		" AND " + prefix + "method = 'POST'" +
		" AND " + prefix + "path IN (" +
		"'/v1/chat/completions', '/v1/completions', '/v1/embeddings', " +
		"'/v1/responses', '/v1/messages', '/v1/images/generations')"
}

func performanceInputTokensSQL(alias string) string {
	prefix := ""
	if strings.TrimSpace(alias) != "" {
		prefix = strings.TrimSpace(alias) + "."
	}
	// Request-log prompt_tokens is kept in the reconciliation-friendly form
	// (which may include cache tokens). Sub2API's dashboard TPM uses billable
	// input_tokens, so remove the separately recorded cache portions here.
	return "CASE WHEN COALESCE(" + prefix + "prompt_tokens, 0) > (COALESCE(" + prefix + "cache_creation_tokens, 0) + COALESCE(" + prefix + "cache_read_tokens, 0)) THEN " +
		"COALESCE(" + prefix + "prompt_tokens, 0) - COALESCE(" + prefix + "cache_creation_tokens, 0) - COALESCE(" + prefix + "cache_read_tokens, 0) ELSE 0 END"
}

func userVisibleGatewayRequestSQL(alias string) string {
	prefix := ""
	if strings.TrimSpace(alias) != "" {
		prefix = strings.TrimSpace(alias) + "."
	}
	return gatewayRequestRecordSQL(alias) + " AND COALESCE(" + prefix + "route_reason, '') <> '" + gatewayRouteReasonLatencyProbe + "'"
}

func gatewayRequestCostByStatusSQL(alias string, status GatewayCostStatus) string {
	prefix := ""
	if strings.TrimSpace(alias) != "" {
		prefix = strings.TrimSpace(alias) + "."
	}
	return "CASE WHEN " + gatewayRequestCostBillableSQL(alias) + " AND (" + gatewayRequestEffectiveCostStatusSQL(alias) + ") = '" + string(status) + "' THEN COALESCE(" + prefix + "cost, 0) ELSE 0 END"
}

func gatewayRequestUnavailableCountSQL(alias string) string {
	return "CASE WHEN " + gatewayRequestRecordSQL(alias) + " AND (" + gatewayRequestEffectiveCostStatusSQL(alias) + ") = '" + string(GatewayCostStatusUnavailable) + "' THEN 1 ELSE 0 END"
}

func gatewayRequestActualCostSQL(alias string) string {
	prefix := ""
	if strings.TrimSpace(alias) != "" {
		prefix = strings.TrimSpace(alias) + "."
	}
	return "CASE WHEN " + gatewayRequestCostBillableSQL(alias) + " AND (" + gatewayRequestEffectiveCostStatusSQL(alias) + ") IN ('" + string(GatewayCostStatusReported) + "', '" + string(GatewayCostStatusReconciled) + "') THEN COALESCE(" + prefix + "cost, 0) ELSE 0 END"
}

func gatewayRequestBillableCostSQL(alias string) string {
	prefix := ""
	if strings.TrimSpace(alias) != "" {
		prefix = strings.TrimSpace(alias) + "."
	}
	return "CASE WHEN " + gatewayRequestCostBillableSQL(alias) +
		" AND (" + gatewayRequestEffectiveCostStatusSQL(alias) + ") IN ('" + string(GatewayCostStatusEstimated) + "', '" +
		string(GatewayCostStatusReported) + "', '" + string(GatewayCostStatusReconciled) + "')" +
		" THEN COALESCE(" + prefix + "cost, 0) ELSE 0 END"
}

func (g *Gateway) RequestLogSummary() (*GatewayRequestLogSummary, error) {
	return g.RequestLogSummaryFiltered(GatewayRequestLogFilter{})
}

// RequestAggregatesReady reports whether every request log has been folded
// into the incremental aggregate ledger. Management caches must stay disabled
// until this is true so an in-progress backfill cannot serve stale summaries.
func (g *Gateway) RequestAggregatesReady() (bool, error) {
	if g == nil || g.db == nil {
		return false, errors.New("gateway store is not initialized")
	}
	return requestAggregatesReady(g.db)
}

func (g *Gateway) RequestLogSummaryFiltered(filter GatewayRequestLogFilter) (*GatewayRequestLogSummary, error) {
	return g.requestLogSummaryFilteredAt(filter, time.Now())
}

func (g *Gateway) requestLogSummaryFilteredAt(filter GatewayRequestLogFilter, now time.Time) (*GatewayRequestLogSummary, error) {
	if summary, used, err := g.requestLogSummaryFromAggregates(filter, now); used || err != nil {
		return summary, err
	}
	return g.requestLogSummaryFilteredAtRaw(filter, now)
}

func (g *Gateway) requestLogSummaryFilteredAtRaw(filter GatewayRequestLogFilter, now time.Time) (*GatewayRequestLogSummary, error) {
	var scan gatewayRequestLogSummaryScan
	requestSQL := gatewayRequestRecordSQL("")
	successSQL := successfulGatewayRequestSQL("")
	failedSQL := failedGatewayRequestSQL("")
	canceledSQL := canceledGatewayRequestSQL("")
	estimatedCostSQL := gatewayRequestCostByStatusSQL("", GatewayCostStatusEstimated)
	reportedCostSQL := gatewayRequestCostByStatusSQL("", GatewayCostStatusReported)
	reconciledCostSQL := gatewayRequestCostByStatusSQL("", GatewayCostStatusReconciled)
	query := g.applyRequestLedgerFilter(g.db.Model(&GatewayRequestLog{}), "", filter)
	err := query.
		Select(`COALESCE(SUM(CASE WHEN ` + requestSQL + ` THEN 1 ELSE 0 END), 0) AS total_requests,
			COALESCE(SUM(CASE WHEN ` + successSQL + ` THEN 1 ELSE 0 END), 0) AS success,
			COALESCE(SUM(CASE WHEN ` + failedSQL + ` THEN 1 ELSE 0 END), 0) AS failed,
			COALESCE(SUM(CASE WHEN ` + canceledSQL + ` THEN 1 ELSE 0 END), 0) AS canceled,
			COALESCE(SUM(CASE WHEN ` + requestSQL + ` THEN total_tokens ELSE 0 END), 0) AS total_tokens,
			COALESCE(SUM(CASE WHEN ` + requestSQL + ` THEN prompt_tokens ELSE 0 END), 0) AS prompt_tokens,
			COALESCE(SUM(CASE WHEN ` + requestSQL + ` THEN completion_tokens ELSE 0 END), 0) AS completion_tokens,
			COALESCE(SUM(CASE WHEN ` + requestSQL + ` THEN cache_creation_tokens ELSE 0 END), 0) AS cache_creation_tokens,
			COALESCE(SUM(CASE WHEN ` + requestSQL + ` THEN cache_read_tokens ELSE 0 END), 0) AS cache_read_tokens,
			COALESCE(SUM(` + gatewayRequestBillableCostSQL("") + `), 0) AS total_cost,
			COALESCE(SUM(` + estimatedCostSQL + `), 0) AS estimated_cost,
			COALESCE(SUM(` + reportedCostSQL + `), 0) AS reported_cost,
			COALESCE(SUM(` + reconciledCostSQL + `), 0) AS reconciled_cost,
			COALESCE(SUM(` + gatewayRequestActualCostSQL("") + `), 0) AS actual_cost,
			COALESCE(SUM(` + gatewayRequestUnavailableCountSQL("") + `), 0) AS unavailable_requests,
			COALESCE(SUM(` + gatewayDispatchCostEvidencePendingSQL("") + `), 0) AS dispatch_evidence_pending,
			COALESCE(SUM(` + gatewayDispatchCostEvidenceActualSQL("") + `), 0) AS dispatch_evidence_actual,
			COALESCE(SUM(` + gatewayDispatchCostEvidenceByStatusSQL("", GatewayCostStatusReported) + `), 0) AS dispatch_evidence_reported,
			COALESCE(SUM(` + gatewayDispatchCostEvidenceByStatusSQL("", GatewayCostStatusReconciled) + `), 0) AS dispatch_evidence_reconciled,
			COALESCE(AVG(CASE WHEN ` + successSQL + ` THEN latency_ms END), 0) AS avg_latency,
			MIN(CASE WHEN ` + requestSQL + ` THEN created_at END) AS first_request_at,
			MAX(CASE WHEN ` + requestSQL + ` THEN created_at END) AS last_request_at`).
		Scan(&scan).Error
	if err != nil {
		return nil, err
	}
	row := GatewayRequestLogSummary{
		TotalRequests:              scan.TotalRequests,
		Success:                    scan.Success,
		Failed:                     scan.Failed,
		Canceled:                   scan.Canceled,
		TotalTokens:                scan.TotalTokens,
		PromptTokens:               scan.PromptTokens,
		CompletionTokens:           scan.CompletionTokens,
		CacheCreationTokens:        scan.CacheCreationTokens,
		CacheReadTokens:            scan.CacheReadTokens,
		TotalCost:                  scan.TotalCost,
		EstimatedCost:              scan.EstimatedCost,
		ReportedCost:               scan.ReportedCost,
		ReconciledCost:             scan.ReconciledCost,
		ActualCost:                 scan.ActualCost,
		UnavailableRequests:        scan.UnavailableRequests,
		DispatchEvidencePending:    scan.DispatchEvidencePending,
		DispatchEvidenceActual:     scan.DispatchEvidenceActual,
		DispatchEvidenceReported:   scan.DispatchEvidenceReported,
		DispatchEvidenceReconciled: scan.DispatchEvidenceReconciled,
		AvgLatency:                 scan.AvgLatency,
		CurrentConcurrencyScope:    "instance",
	}
	if first, ok := g.parseRequestLogAggregateTime(scan.FirstRequestAt); ok {
		row.FirstRequestAt = &first
	}
	if last, ok := g.parseRequestLogAggregateTime(scan.LastRequestAt); ok {
		row.LastRequestAt = &last
	}
	row.WindowSeconds = requestLogRateWindowSeconds(row.FirstRequestAt, row.LastRequestAt, filter)
	performance, err := g.requestLogPerformanceFilteredAt(filter, now)
	if err != nil {
		return nil, err
	}
	applyPerformanceSummary(&row, performance)
	return &row, nil
}

func (g *Gateway) requestLogPerformanceFilteredAt(filter GatewayRequestLogFilter, now time.Time) (*gatewayRequestLogPerformanceScan, error) {
	// Sub2API keeps RPM/TPM live even when a dashboard date range is selected;
	// dimension filters still apply, but historical Start/End bounds do not.
	performanceFilter := filter
	performanceFilter.Start = nil
	performanceFilter.End = nil
	query := g.applyRequestLogFilter(g.db.Model(&GatewayRequestLog{}), "", performanceFilter).
		Where("created_at >= ? AND created_at < ?", g.requestLogTimeBoundary(now.Add(-gatewayPerformanceWindow)), g.requestLogTimeBoundary(now)).
		Where(performanceGatewayRequestSQL(""))
	var scan gatewayRequestLogPerformanceScan
	if err := query.Select(`COUNT(*) AS requests,
			COALESCE(SUM(` + performanceInputTokensSQL("") + ` + completion_tokens), 0) AS tokens,
			COALESCE(AVG(latency_ms), 0) AS avg_latency`).Scan(&scan).Error; err != nil {
		return nil, err
	}
	return &scan, nil
}

func requestLogRateWindowSeconds(first *time.Time, last *time.Time, filter GatewayRequestLogFilter) float64 {
	if filter.Start != nil && filter.End != nil && filter.End.After(*filter.Start) {
		return filter.End.Sub(*filter.Start).Seconds()
	}
	if first != nil && last != nil && last.After(*first) {
		return last.Sub(*first).Seconds()
	}
	return 60
}

func (g *Gateway) parseRequestLogAggregateTime(raw string) (time.Time, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false
	}
	for _, layout := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999-07:00",
	} {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return parsed, true
		}
	}
	location := g.databaseLocation
	if location == nil {
		location = time.UTC
	}
	for _, layout := range []string{"2006-01-02 15:04:05.999999999", "2006-01-02 15:04:05"} {
		if parsed, err := time.ParseInLocation(layout, raw, location); err == nil {
			return parsed, true
		}
	}
	return time.Time{}, false
}

func (g *Gateway) ListRecentSuccessfulLatencyLogsSince(since time.Time, limit int) ([]GatewayLatencyLog, error) {
	if limit <= 0 {
		limit = 5000
	}
	var list []GatewayLatencyLog
	err := g.db.Model(&GatewayRequestLog{}).
		Select("id, channel_id, group_name, COALESCE(NULLIF(first_byte_ms, 0), latency_ms) AS latency_ms, created_at").
		Where("created_at >= ? AND channel_id > 0 AND latency_ms > 0 AND "+successfulGatewayRequestSQL(""), g.requestLogTimeBoundary(since)).
		Where("COALESCE(route_details, '') <> ? AND COALESCE(route_details, '') NOT LIKE ?", GatewayWaitingProbeDetail, GatewayWaitingProbeDetail+"\n%").
		Where("COALESCE(route_reason, '') <> ?", "model_check").
		Order("created_at DESC, id DESC").
		Limit(limit).
		Scan(&list).Error
	if err != nil {
		return nil, err
	}
	return list, nil
}

func (g *Gateway) ListSuccessfulLatencyStatsSince(since time.Time) ([]GatewayLatencyStat, error) {
	var list []GatewayLatencyStat
	err := g.db.Model(&GatewayRequestLog{}).
		Select("channel_id, group_name, COALESCE(AVG(COALESCE(NULLIF(first_byte_ms, 0), latency_ms)), 0) AS avg_latency, COUNT(*) AS requests").
		Where("created_at >= ? AND channel_id > 0 AND latency_ms > 0 AND "+successfulGatewayRequestSQL(""), g.requestLogTimeBoundary(since)).
		Where("COALESCE(route_details, '') <> ? AND COALESCE(route_details, '') NOT LIKE ?", GatewayWaitingProbeDetail, GatewayWaitingProbeDetail+"\n%").
		Where("COALESCE(route_reason, '') <> ?", "model_check").
		Group("channel_id, group_name").
		Scan(&list).Error
	if err != nil {
		return nil, err
	}
	return list, nil
}

func (g *Gateway) LastSuccessfulRoute(downstreamKeyID uint, sessionHash string, model string, since time.Time) (*GatewayRecentRoute, error) {
	if downstreamKeyID == 0 || strings.TrimSpace(sessionHash) == "" || strings.TrimSpace(model) == "" {
		return nil, nil
	}
	var latest GatewayRequestLog
	query := g.db.Model(&GatewayRequestLog{}).
		Where("downstream_key_id = ? AND session_hash = ? AND model_name = ? AND channel_id > 0 AND group_name <> ? AND created_at >= ? AND COALESCE(route_reason, '') <> ? AND "+gatewayRequestRecordSQL("")+" AND NOT "+canceledGatewayRequestSQL(""),
			downstreamKeyID, strings.TrimSpace(sessionHash), strings.TrimSpace(model), "", g.requestLogTimeBoundary(since), "session_affinity").
		Where(`NOT EXISTS (
			SELECT 1 FROM gateway_request_logs AS revoked
			WHERE revoked.downstream_key_id = gateway_request_logs.downstream_key_id
				AND revoked.session_hash = gateway_request_logs.session_hash
				AND revoked.model_name = gateway_request_logs.model_name
				AND revoked.id > gateway_request_logs.id
				AND NOT (` + successfulGatewayRequestSQL("revoked") + `)
				AND (
					(
						revoked.channel_id = gateway_request_logs.channel_id
						AND revoked.group_name = gateway_request_logs.group_name
						AND COALESCE(revoked.upstream_accepted, FALSE) = TRUE
					)
					OR revoked.session_affinity_origin_request_id = gateway_request_logs.id
				)
		)`)
	err := query.Order("id DESC").First(&latest).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !IsSuccessfulGatewayRequest(&latest) {
		return nil, nil
	}
	return &GatewayRecentRoute{
		ID:                     latest.ID,
		ChannelID:              latest.ChannelID,
		GroupName:              latest.GroupName,
		RouteReason:            latest.RouteReason,
		SelectionRank:          latest.RouteSelectionRank,
		RouteDetails:           latest.RouteDetails,
		RouteAttemptedChannels: latest.RouteAttemptedChannels,
		CreatedAt:              latest.CreatedAt,
	}, nil
}

func (g *Gateway) ListRequestLogsPage(page int, pageSize int) ([]GatewayRequestLogItem, int64, *GatewayRequestLogSummary, error) {
	return g.ListRequestLogsPageFiltered(page, pageSize, GatewayRequestLogFilter{})
}

func (g *Gateway) ListRequestLogsPageFiltered(page int, pageSize int, filter GatewayRequestLogFilter) ([]GatewayRequestLogItem, int64, *GatewayRequestLogSummary, error) {
	summary, err := g.RequestLogSummaryFiltered(filter)
	if err != nil {
		return nil, 0, nil, err
	}
	total := summary.TotalRequests
	list, err := g.ListRequestLogPageItemsFiltered(page, pageSize, filter)
	if err != nil {
		return nil, 0, nil, err
	}
	return list, total, summary, nil
}

// ListRequestLogPageItemsFiltered reads only the requested raw-log page. It
// deliberately does not calculate totals or summaries so the API can serve a
// cached aggregate while keeping request details live and uncached.
func (g *Gateway) ListRequestLogPageItemsFiltered(page int, pageSize int, filter GatewayRequestLogFilter) ([]GatewayRequestLogItem, error) {
	list := []GatewayRequestLogItem{}
	listQuery := g.applyRequestLogFilter(g.db.Table("gateway_request_logs AS logs"), "logs", filter)
	err := listQuery.
		Select(`logs.id, logs.downstream_key_id, COALESCE(keys.name, '') AS downstream_key_name, COALESCE(key_groups.name, '') AS downstream_key_group_name,
			logs.channel_id, COALESCE(NULLIF(TRIM(logs.channel_name_snapshot), ''), channels.name, '') AS channel_name,
			logs.group_name, logs.group_ratio, logs.completion_ratio, logs.raw_group_ratio, logs.raw_completion_ratio, logs.funding_factor, logs.cost_basis_version, logs.cost_basis_source, logs.model_name, logs.reasoning_effort,
			logs.upstream_model_name, logs.upstream_response_model,
			logs.requested_model_name, logs.requested_reasoning_effort, logs.model_mapping_strategy, logs.model_mapping_details,
				logs.method, logs.path, logs.status_code, logs.first_byte_ms, logs.downstream_first_visible_ms, logs.latency_ms,
			logs.prompt_tokens, logs.completion_tokens, logs.cache_creation_tokens, logs.cache_read_tokens, logs.total_tokens,
				logs.cost, logs.raw_cost, logs.cost_estimated, logs.cost_source, ` + gatewayRequestEffectiveCostStatusSQL("logs") + ` AS cost_status,
				logs.cost_unavailable_reason, logs.price_fact_id, logs.price_fact_version, logs.price_fact_schema_version, logs.price_cost_unit_epoch, logs.price_model_name,
				logs.price_operation, logs.price_protocol, logs.price_input_unit, logs.price_output_unit, logs.price_cache_read_unit, logs.price_cache_write_unit,
				logs.price_long_context_threshold_tokens, logs.price_long_context_input_unit, logs.price_long_context_output_unit,
				logs.price_long_context_cache_read_unit, logs.price_long_context_cache_write_unit, logs.price_operation_unit,
				logs.price_currency, logs.price_unit, logs.price_source, logs.price_observed_at, logs.usage_schema, logs.usage_schema_version,
				logs.usage_input_present, logs.usage_output_present, logs.usage_cache_read_present, logs.usage_cache_write_present, logs.usage_operation_present, logs.operation_units,
				` + gatewayRequestCostBillableSQL("logs") + ` AS cost_billable, logs.cost_reconcile_at, logs.cost_reconcile_tries, logs.cost_reconcile_error,
			logs.upstream_key_id, logs.upstream_key_name, logs.upstream_request_id, logs.upstream_accepted, logs.billing_committed, logs.client_ip, logs.client_ip_source,
			logs.route_reason, logs.route_selection_rank, logs.session_affinity_origin_request_id, logs.session_affinity_expires_at, logs.session_affinity_overridden_candidates, logs.route_details, logs.route_attempted_channels,
				logs.logical_request_trace_id, logs.telemetry_version, logs.routing_mode, logs.gateway_app, logs.terminal_failure_code,
				logs.prior_physically_dispatched_failure_count, logs.real_fallback, logs.route_exclusion_snapshot_version, logs.route_exclusions_snapshot,
				logs.error, logs.created_at`).
		Joins("LEFT JOIN gateway_downstream_keys AS keys ON keys.id = logs.downstream_key_id").
		Joins("LEFT JOIN gateway_key_groups AS key_groups ON key_groups.id = keys.key_group_id").
		Joins("LEFT JOIN channels ON channels.id = logs.channel_id").
		Order(gatewayRequestLogOrder("logs", filter.SortBy, filter.SortOrder)).
		Limit(pageSize).
		Offset((page - 1) * pageSize).
		Scan(&list).Error
	if err != nil {
		return nil, err
	}
	populateGatewayRequestLogCostComparisons(list)
	return list, nil
}

func (g *Gateway) ListRequestLogsFiltered(filter GatewayRequestLogFilter, limit int) ([]GatewayRequestLogItem, error) {
	if limit <= 0 {
		limit = 10000
	}
	list := []GatewayRequestLogItem{}
	listQuery := g.applyRequestLogFilter(g.db.Table("gateway_request_logs AS logs"), "logs", filter)
	err := listQuery.
		Select(`logs.id, logs.downstream_key_id, COALESCE(keys.name, '') AS downstream_key_name, COALESCE(key_groups.name, '') AS downstream_key_group_name,
			logs.channel_id, COALESCE(NULLIF(TRIM(logs.channel_name_snapshot), ''), channels.name, '') AS channel_name,
			logs.group_name, logs.group_ratio, logs.completion_ratio, logs.raw_group_ratio, logs.raw_completion_ratio, logs.funding_factor, logs.cost_basis_version, logs.cost_basis_source, logs.model_name, logs.reasoning_effort,
			logs.upstream_model_name, logs.upstream_response_model,
			logs.requested_model_name, logs.requested_reasoning_effort, logs.model_mapping_strategy, logs.model_mapping_details,
				logs.method, logs.path, logs.status_code, logs.first_byte_ms, logs.downstream_first_visible_ms, logs.latency_ms,
			logs.prompt_tokens, logs.completion_tokens, logs.cache_creation_tokens, logs.cache_read_tokens, logs.total_tokens,
				logs.cost, logs.raw_cost, logs.cost_estimated, logs.cost_source, ` + gatewayRequestEffectiveCostStatusSQL("logs") + ` AS cost_status,
				logs.cost_unavailable_reason, logs.price_fact_id, logs.price_fact_version, logs.price_fact_schema_version, logs.price_cost_unit_epoch, logs.price_model_name,
				logs.price_operation, logs.price_protocol, logs.price_input_unit, logs.price_output_unit, logs.price_cache_read_unit, logs.price_cache_write_unit,
				logs.price_long_context_threshold_tokens, logs.price_long_context_input_unit, logs.price_long_context_output_unit,
				logs.price_long_context_cache_read_unit, logs.price_long_context_cache_write_unit, logs.price_operation_unit,
				logs.price_currency, logs.price_unit, logs.price_source, logs.price_observed_at, logs.usage_schema, logs.usage_schema_version,
				logs.usage_input_present, logs.usage_output_present, logs.usage_cache_read_present, logs.usage_cache_write_present, logs.usage_operation_present, logs.operation_units,
				` + gatewayRequestCostBillableSQL("logs") + ` AS cost_billable, logs.cost_reconcile_at, logs.cost_reconcile_tries, logs.cost_reconcile_error,
			logs.upstream_key_id, logs.upstream_key_name, logs.upstream_request_id, logs.upstream_accepted, logs.billing_committed, logs.client_ip, logs.client_ip_source,
			logs.route_reason, logs.route_selection_rank, logs.session_affinity_origin_request_id, logs.session_affinity_expires_at, logs.session_affinity_overridden_candidates, logs.route_details, logs.route_attempted_channels,
				logs.logical_request_trace_id, logs.telemetry_version, logs.routing_mode, logs.gateway_app, logs.terminal_failure_code,
				logs.prior_physically_dispatched_failure_count, logs.real_fallback, logs.route_exclusion_snapshot_version, logs.route_exclusions_snapshot,
				logs.error, logs.created_at`).
		Joins("LEFT JOIN gateway_downstream_keys AS keys ON keys.id = logs.downstream_key_id").
		Joins("LEFT JOIN gateway_key_groups AS key_groups ON key_groups.id = keys.key_group_id").
		Joins("LEFT JOIN channels ON channels.id = logs.channel_id").
		Order(gatewayRequestLogOrder("logs", filter.SortBy, filter.SortOrder)).
		Limit(limit).
		Scan(&list).Error
	if err != nil {
		return nil, err
	}
	populateGatewayRequestLogCostComparisons(list)
	return list, nil
}

const (
	gatewayRequestCostDifferenceMinimumRatio = 0.05
)

func populateGatewayRequestLogCostComparisons(items []GatewayRequestLogItem) {
	for index := range items {
		populateGatewayRequestLogCostComparison(&items[index])
	}
}

func populateGatewayRequestLogCostComparison(item *GatewayRequestLogItem) {
	if item == nil {
		return
	}
	item.CalculatedCost = nil
	item.CalculatedCostStatus = GatewayCostStatusUnavailable
	item.CalculatedCostUnavailableReason = GatewayPricingUnavailableFactMissing
	item.UpstreamCost = nil
	item.UpstreamCostStatus = GatewayCostStatusNone
	item.DisplayCost = nil
	item.CostDifference = nil
	item.CostDifferenceRatio = nil
	item.CostDifferenceMaterial = false
	if item.PriceFactID != nil {
		fact := &GatewayRequestPriceFact{
			SchemaVersion:                  item.PriceFactSchemaVersion,
			Operation:                      item.PriceOperation,
			Protocol:                       item.PriceProtocol,
			InputUnitPrice:                 cloneGatewayRequestDispatchNumber(item.PriceInputUnit),
			OutputUnitPrice:                cloneGatewayRequestDispatchNumber(item.PriceOutputUnit),
			CacheReadUnitPrice:             cloneGatewayRequestDispatchNumber(item.PriceCacheReadUnit),
			CacheWriteUnitPrice:            cloneGatewayRequestDispatchNumber(item.PriceCacheWriteUnit),
			LongContextThresholdTokens:     cloneGatewayRequestPricingInt64(item.PriceLongContextThresholdTokens),
			LongContextInputUnitPrice:      cloneGatewayRequestDispatchNumber(item.PriceLongContextInputUnit),
			LongContextOutputUnitPrice:     cloneGatewayRequestDispatchNumber(item.PriceLongContextOutputUnit),
			LongContextCacheReadUnitPrice:  cloneGatewayRequestDispatchNumber(item.PriceLongContextCacheReadUnit),
			LongContextCacheWriteUnitPrice: cloneGatewayRequestDispatchNumber(item.PriceLongContextCacheWriteUnit),
			OperationUnitPrice:             cloneGatewayRequestDispatchNumber(item.PriceOperationUnit),
			Unit:                           item.PriceUnit,
			Currency:                       item.PriceCurrency,
		}
		if fact.SchemaVersion == GatewayRequestPriceFactTieredSchemaVersion {
			groupRatio := item.GroupRatio
			completionRatio := item.CompletionRatio
			fact.GroupRatio = &groupRatio
			fact.CompletionRatio = &completionRatio
		}
		usage, usageReason := gatewayRequestPricingUsageFromLogItem(item)
		if usageReason != "" {
			item.CalculatedCostUnavailableReason = usageReason
		} else if !strings.EqualFold(strings.TrimSpace(fact.Currency), gatewayRequestPriceSupportedCurrency) {
			item.CalculatedCostUnavailableReason = GatewayPricingUnavailableCurrencyUnsupported
		} else {
			estimate := EvaluateGatewayRequestPriceFact(fact, usage)
			item.CalculatedCost = estimate.Cost
			item.CalculatedCostStatus = estimate.Status
			item.CalculatedCostUnavailableReason = estimate.UnavailableReason
		}
	}

	switch item.CostStatus {
	case GatewayCostStatusReported, GatewayCostStatusReconciled:
		if item.Cost >= 0 && !math.IsNaN(item.Cost) && !math.IsInf(item.Cost, 0) {
			upstreamCost := item.Cost
			item.UpstreamCost = &upstreamCost
			item.UpstreamCostStatus = item.CostStatus
		}
	}
	if item.UpstreamCost != nil {
		item.DisplayCost = cloneGatewayRequestDispatchNumber(item.UpstreamCost)
	} else if item.CalculatedCost != nil {
		item.DisplayCost = cloneGatewayRequestDispatchNumber(item.CalculatedCost)
	}
	if item.UpstreamCost == nil || item.CalculatedCost == nil || *item.CalculatedCost < 0 {
		return
	}
	difference := *item.UpstreamCost - *item.CalculatedCost
	item.CostDifference = &difference
	if *item.CalculatedCost == 0 {
		if difference == 0 {
			ratio := 0.0
			item.CostDifferenceRatio = &ratio
		}
		item.CostDifferenceMaterial = difference != 0
		return
	}
	ratio := math.Abs(difference) / *item.CalculatedCost
	item.CostDifferenceRatio = &ratio
	item.CostDifferenceMaterial = gatewayRequestCostDifferenceExceedsThreshold(
		*item.CalculatedCost,
		*item.UpstreamCost,
	)
}

func gatewayRequestCostDifferenceExceedsThreshold(calculated, upstream float64) bool {
	difference := math.Abs(upstream - calculated)
	if calculated == 0 {
		return difference != 0
	}
	limit := calculated * gatewayRequestCostDifferenceMinimumRatio
	// Treat a handful of binary floating-point ULPs as the exact boundary. This
	// keeps decimal amounts such as 0.000065 and 0.00006825 at exactly 5%, while
	// preserving genuinely greater differences.
	tolerance := 32 * (math.Nextafter(1, 2) - 1) * math.Max(difference, limit)
	return difference > limit+tolerance
}

func gatewayRequestPricingUsageFromLogItem(item *GatewayRequestLogItem) (GatewayRequestPricingUsage, string) {
	if item == nil {
		return GatewayRequestPricingUsage{}, GatewayPricingUnavailableUsageIncomplete
	}
	usage := GatewayRequestPricingUsage{}
	if item.UsageInputPresent {
		if item.PromptTokens < 0 {
			return GatewayRequestPricingUsage{}, GatewayPricingUnavailableUsageInvalid
		}
		value := item.PromptTokens
		if item.UsageSchema != GatewayPricingUsageSchemaInputTokensV1 {
			if item.CacheCreationTokens < 0 || item.CacheReadTokens < 0 {
				return GatewayRequestPricingUsage{}, GatewayPricingUnavailableUsageInvalid
			}
			var ok bool
			value, ok = checkedGatewayRequestPricingSub(value, item.CacheCreationTokens)
			if !ok {
				return GatewayRequestPricingUsage{}, GatewayPricingUnavailableUsageInvalid
			}
			value, ok = checkedGatewayRequestPricingSub(value, item.CacheReadTokens)
			if !ok || value < 0 {
				return GatewayRequestPricingUsage{}, GatewayPricingUnavailableUsageInvalid
			}
		}
		usage.InputTokens = &value
	}
	if item.UsageOutputPresent {
		value := item.CompletionTokens
		if value < 0 {
			return GatewayRequestPricingUsage{}, GatewayPricingUnavailableUsageInvalid
		}
		usage.OutputTokens = &value
	}
	if item.UsageCacheReadPresent {
		value := item.CacheReadTokens
		if value < 0 {
			return GatewayRequestPricingUsage{}, GatewayPricingUnavailableUsageInvalid
		}
		usage.CacheReadTokens = &value
	}
	if item.UsageCacheWritePresent {
		value := item.CacheCreationTokens
		if value < 0 {
			return GatewayRequestPricingUsage{}, GatewayPricingUnavailableUsageInvalid
		}
		usage.CacheWriteTokens = &value
	}
	if item.UsageOperationPresent {
		if item.OperationUnits == nil || *item.OperationUnits < 0 {
			return GatewayRequestPricingUsage{}, GatewayPricingUnavailableUsageInvalid
		}
		usage.OperationUnits = cloneGatewayRequestPricingInt64(item.OperationUnits)
	}
	return usage, ""
}

func (g *Gateway) ListRequestLogModels(filter GatewayRequestLogFilter) ([]string, error) {
	names := []string{}
	query := g.applyRequestLogFilter(g.db.Model(&GatewayRequestLog{}), "", filter)
	if err := query.Where("model_name <> ?", "").Distinct().Order("model_name ASC").Pluck("model_name", &names).Error; err != nil {
		return nil, err
	}
	return names, nil
}

// ListRequestLogGroups returns distinct upstream groups available under the
// supplied request-log dimensions.  The empty string represents legacy or
// unassigned requests and is kept so the UI can expose that scope explicitly.
func (g *Gateway) ListRequestLogGroups(filter GatewayRequestLogFilter) ([]string, error) {
	names := []string{}
	const groupNameSQL = "COALESCE(NULLIF(TRIM(group_name), ''), '')"
	query := g.applyRequestLogFilter(g.db.Model(&GatewayRequestLog{}), "", filter)
	if err := query.Select(groupNameSQL + " AS group_name").Group(groupNameSQL).Order(groupNameSQL + " ASC").Scan(&names).Error; err != nil {
		return nil, err
	}
	return names, nil
}

func (g *Gateway) RequestLogAnalytics(filter GatewayRequestLogFilter, limit int) (*GatewayRequestLogAnalytics, error) {
	if analytics, used, err := g.requestLogAnalyticsFromAggregates(filter, limit); used || err != nil {
		return analytics, err
	}
	return g.requestLogAnalyticsRaw(filter, limit)
}

// RequestLogChannelCosts returns complete per-channel costs for one filter. It
// uses the incremental aggregate path when ready and only groups raw request
// logs while aggregates are rebuilding or for the two partial edge buckets.
func (g *Gateway) RequestLogChannelCosts(filter GatewayRequestLogFilter) (map[uint]float64, error) {
	if costs, used, err := g.requestLogChannelCostsFromAggregates(filter); used || err != nil {
		return costs, err
	}
	return g.requestLogChannelCostsRaw(filter)
}

func (g *Gateway) requestLogChannelCostsRaw(filter GatewayRequestLogFilter) (map[uint]float64, error) {
	type channelCost struct {
		ChannelID uint
		TotalCost float64
	}
	rows := []channelCost{}
	if err := g.applyRequestLogFilter(g.db.Model(&GatewayRequestLog{}), "", filter).
		Select("channel_id, COALESCE(SUM(" + gatewayRequestBillableCostSQL("") + "), 0) AS total_cost").
		Group("channel_id").Scan(&rows).Error; err != nil {
		return nil, err
	}
	costs := make(map[uint]float64, len(rows))
	for _, row := range rows {
		if row.ChannelID > 0 {
			costs[row.ChannelID] += row.TotalCost
		}
	}
	return costs, nil
}

func (g *Gateway) requestLogAnalyticsRaw(filter GatewayRequestLogFilter, limit int) (*GatewayRequestLogAnalytics, error) {
	return g.requestLogAnalyticsRawBounded(filter, limit, 0)
}

func (g *Gateway) requestLogAnalyticsRawBounded(filter GatewayRequestLogFilter, limit int, modelUnitPriceLimit int) (*GatewayRequestLogAnalytics, error) {
	return g.requestLogAnalyticsRawBoundedWithGranularity(filter, limit, modelUnitPriceLimit, requestLogUsesHourlyBuckets(filter), false)
}

func gatewayRequestLedgerMetricsSelectSQL(alias string) string {
	prefix := ""
	if strings.TrimSpace(alias) != "" {
		prefix = strings.TrimSpace(alias) + "."
	}
	requestSQL := gatewayRequestRecordSQL(alias)
	successSQL := successfulGatewayRequestSQL(alias)
	failedSQL := failedGatewayRequestSQL(alias)
	canceledSQL := canceledGatewayRequestSQL(alias)
	return `COALESCE(SUM(CASE WHEN ` + requestSQL + ` THEN 1 ELSE 0 END), 0) AS requests,
		COALESCE(SUM(CASE WHEN ` + successSQL + ` THEN 1 ELSE 0 END), 0) AS success,
		COALESCE(SUM(CASE WHEN ` + failedSQL + ` THEN 1 ELSE 0 END), 0) AS failed,
		COALESCE(SUM(CASE WHEN ` + canceledSQL + ` THEN 1 ELSE 0 END), 0) AS canceled,
		COALESCE(SUM(CASE WHEN ` + requestSQL + ` THEN ` + prefix + `total_tokens ELSE 0 END), 0) AS total_tokens,
		COALESCE(SUM(CASE WHEN ` + requestSQL + ` THEN ` + prefix + `prompt_tokens ELSE 0 END), 0) AS prompt_tokens,
		COALESCE(SUM(CASE WHEN ` + requestSQL + ` THEN ` + prefix + `completion_tokens ELSE 0 END), 0) AS completion_tokens,
		COALESCE(SUM(CASE WHEN ` + requestSQL + ` THEN ` + prefix + `cache_creation_tokens ELSE 0 END), 0) AS cache_creation_tokens,
		COALESCE(SUM(CASE WHEN ` + requestSQL + ` THEN ` + prefix + `cache_read_tokens ELSE 0 END), 0) AS cache_read_tokens,
		COALESCE(SUM(` + gatewayRequestBillableCostSQL(alias) + `), 0) AS total_cost,
		COALESCE(SUM(` + gatewayRequestCostByStatusSQL(alias, GatewayCostStatusEstimated) + `), 0) AS estimated_cost,
		COALESCE(SUM(` + gatewayRequestCostByStatusSQL(alias, GatewayCostStatusReported) + `), 0) AS reported_cost,
		COALESCE(SUM(` + gatewayRequestCostByStatusSQL(alias, GatewayCostStatusReconciled) + `), 0) AS reconciled_cost,
		COALESCE(SUM(` + gatewayRequestActualCostSQL(alias) + `), 0) AS actual_cost,
		COALESCE(SUM(` + gatewayRequestUnavailableCountSQL(alias) + `), 0) AS unavailable_requests,
		COALESCE(SUM(` + gatewayDispatchCostEvidencePendingSQL(alias) + `), 0) AS dispatch_evidence_pending,
		COALESCE(SUM(` + gatewayDispatchCostEvidenceActualSQL(alias) + `), 0) AS dispatch_evidence_actual,
		COALESCE(SUM(` + gatewayDispatchCostEvidenceByStatusSQL(alias, GatewayCostStatusReported) + `), 0) AS dispatch_evidence_reported,
		COALESCE(SUM(` + gatewayDispatchCostEvidenceByStatusSQL(alias, GatewayCostStatusReconciled) + `), 0) AS dispatch_evidence_reconciled,
		COALESCE(AVG(CASE WHEN ` + successSQL + ` THEN ` + prefix + `latency_ms END), 0) AS avg_latency`
}

func (g *Gateway) requestLogAnalyticsRawBoundedWithGranularity(filter GatewayRequestLogFilter, limit int, modelUnitPriceLimit int, hourly bool, includeEvidenceOnlyBreakdowns bool) (*GatewayRequestLogAnalytics, error) {
	if limit <= 0 {
		limit = 8
	}
	trend, err := g.requestLogTrendWithGranularity(filter, hourly)
	if err != nil {
		return nil, err
	}
	modelUnitPrices, err := g.requestLogModelUnitPriceTrendWithGranularity(filter, modelUnitPriceLimit, hourly)
	if err != nil {
		return nil, err
	}

	channels := []GatewayRequestLogBreakdownItem{}
	channelQuery := g.applyRequestLedgerFilter(g.db.Table("gateway_request_logs AS logs"), "logs", filter)
	channelQuery = channelQuery.
		Select(`COALESCE(NULLIF(TRIM(logs.channel_name_snapshot), ''), channels.name, '未知渠道') AS name,
			logs.channel_id AS channel_id,
			` + gatewayRequestLedgerMetricsSelectSQL("logs")).
		Joins("LEFT JOIN channels ON channels.id = logs.channel_id").
		Group("logs.channel_id, NULLIF(TRIM(logs.channel_name_snapshot), ''), channels.name")
	if !includeEvidenceOnlyBreakdowns {
		channelQuery = channelQuery.Having("SUM(CASE WHEN " + gatewayRequestRecordSQL("logs") + " THEN 1 ELSE 0 END) > 0")
	}
	if err := channelQuery.
		Order("total_cost DESC, requests DESC").
		Limit(limit).
		Scan(&channels).Error; err != nil {
		return nil, err
	}

	models, err := g.requestLogModelBreakdown(filter, limit, false, includeEvidenceOnlyBreakdowns)
	if err != nil {
		return nil, err
	}
	modelsByToken, err := g.requestLogModelBreakdown(filter, limit, true, includeEvidenceOnlyBreakdowns)
	if err != nil {
		return nil, err
	}

	keys := []GatewayRequestLogBreakdownItem{}
	keyQuery := g.applyRequestLedgerFilter(g.db.Table("gateway_request_logs AS logs"), "logs", filter)
	keyQuery = keyQuery.
		Select(`COALESCE(NULLIF(keys.name, ''), '未知 Key') AS name,
			logs.downstream_key_id AS downstream_key_id,
			` + gatewayRequestLedgerMetricsSelectSQL("logs")).
		Joins("LEFT JOIN gateway_downstream_keys AS keys ON keys.id = logs.downstream_key_id").
		Group("logs.downstream_key_id, keys.name")
	if !includeEvidenceOnlyBreakdowns {
		keyQuery = keyQuery.Having("SUM(CASE WHEN " + gatewayRequestRecordSQL("logs") + " THEN 1 ELSE 0 END) > 0")
	}
	if err := keyQuery.
		Order("total_cost DESC, requests DESC").
		Limit(limit).
		Scan(&keys).Error; err != nil {
		return nil, err
	}

	costSources := []GatewayRequestLogBreakdownItem{}
	costSourceQuery := g.applyRequestLedgerFilter(g.db.Table("gateway_request_logs AS logs"), "logs", filter)
	costSourceQuery = costSourceQuery.
		Select(`COALESCE(NULLIF(TRIM(logs.cost_source), ''), '未提供费用来源') AS name,
			` + gatewayRequestLedgerMetricsSelectSQL("logs")).
		Group("COALESCE(NULLIF(TRIM(logs.cost_source), ''), '未提供费用来源')")
	if !includeEvidenceOnlyBreakdowns {
		costSourceQuery = costSourceQuery.Having("SUM(CASE WHEN " + gatewayRequestRecordSQL("logs") + " THEN 1 ELSE 0 END) > 0")
	}
	if err := costSourceQuery.
		Order("total_cost DESC, requests DESC").
		Limit(limit).
		Scan(&costSources).Error; err != nil {
		return nil, err
	}

	routeReasons := []GatewayRequestLogBreakdownItem{}
	routeReasonQuery := g.applyRequestLedgerFilter(g.db.Table("gateway_request_logs AS logs"), "logs", filter)
	routeReasonQuery = routeReasonQuery.
		Select(`COALESCE(NULLIF(TRIM(logs.route_reason), ''), 'smart_primary') AS name,
			` + gatewayRequestLedgerMetricsSelectSQL("logs")).
		Group("COALESCE(NULLIF(TRIM(logs.route_reason), ''), 'smart_primary')")
	if !includeEvidenceOnlyBreakdowns {
		routeReasonQuery = routeReasonQuery.Having("SUM(CASE WHEN " + gatewayRequestRecordSQL("logs") + " THEN 1 ELSE 0 END) > 0")
	}
	if err := routeReasonQuery.
		Order("total_cost DESC, requests DESC").
		Limit(limit).
		Scan(&routeReasons).Error; err != nil {
		return nil, err
	}
	return &GatewayRequestLogAnalytics{
		Trend:            trend,
		ModelUnitPrices:  modelUnitPrices,
		Channels:         channels,
		Models:           models,
		ModelsByTokens:   modelsByToken,
		Keys:             keys,
		CostSources:      costSources,
		RouteReasons:     routeReasons,
		GroupCacheRatios: []GatewayRequestLogGroupCacheRatio{},
	}, nil
}

func (g *Gateway) requestLogModelBreakdown(filter GatewayRequestLogFilter, limit int, byTokens bool, includeEvidenceOnly bool) ([]GatewayRequestLogBreakdownItem, error) {
	order := "total_cost DESC, requests DESC"
	if byTokens {
		order = "total_tokens DESC, requests DESC"
	}
	items := []GatewayRequestLogBreakdownItem{}
	query := g.applyRequestLedgerFilter(g.db.Model(&GatewayRequestLog{}), "", filter)
	query = query.
		Select(`COALESCE(NULLIF(model_name, ''), '未知模型') AS name,
			` + gatewayRequestLedgerMetricsSelectSQL("")).
		Group("model_name")
	if !includeEvidenceOnly {
		query = query.Having("SUM(CASE WHEN " + gatewayRequestRecordSQL("") + " THEN 1 ELSE 0 END) > 0")
	}
	if err := query.
		Order(order).
		Limit(limit).
		Scan(&items).Error; err != nil {
		return nil, err
	}
	return items, nil
}

func (g *Gateway) requestLogTrend(filter GatewayRequestLogFilter) ([]GatewayRequestLogTrendPoint, error) {
	return g.requestLogTrendWithGranularity(filter, requestLogUsesHourlyBuckets(filter))
}

func requestLogUsesHourlyBuckets(filter GatewayRequestLogFilter) bool {
	return filter.Start != nil && filter.End != nil && filter.End.After(*filter.Start) && filter.End.Sub(*filter.Start) <= 48*time.Hour
}

func (g *Gateway) requestLogTrendWithGranularity(filter GatewayRequestLogFilter, hourly bool) ([]GatewayRequestLogTrendPoint, error) {
	if hourly {
		bucketSQL, bucketArgs := g.requestLogHourBucketSQL()
		return g.requestLogTrendWithBucket(filter, bucketSQL, true, bucketArgs...)
	}

	location := time.UTC
	if name := strings.TrimSpace(filter.TimeZone); name != "" {
		if configured, err := time.LoadLocation(name); err == nil {
			location = configured
		}
	}
	start, end, ok, err := g.requestLogTrendRange(filter)
	if err != nil || !ok {
		return []GatewayRequestLogTrendPoint{}, err
	}
	first := start.In(location)
	last := end.Add(-time.Nanosecond).In(location)
	day := time.Date(first.Year(), first.Month(), first.Day(), 0, 0, 0, 0, location)
	rangeEnd := time.Date(last.Year(), last.Month(), last.Day(), 0, 0, 0, 0, location).AddDate(0, 0, 1)
	trend := make([]GatewayRequestLogTrendPoint, 0)
	for day.Before(rangeEnd) {
		batchEnd := day.AddDate(0, 0, 31)
		if batchEnd.After(rangeEnd) {
			batchEnd = rangeEnd
		}
		bucketSQL, bucketArgs := g.requestLogDailyBucketSQL(day, batchEnd)
		batchFilter := requestLogBatchFilter(filter, day.UTC(), batchEnd.UTC())
		batch, err := g.requestLogTrendWithBucket(batchFilter, bucketSQL, false, bucketArgs...)
		if err != nil {
			return nil, err
		}
		trend = append(trend, batch...)
		day = batchEnd
	}
	return trend, nil
}

func (g *Gateway) requestLogModelUnitPriceTrend(filter GatewayRequestLogFilter, rowLimit int) ([]GatewayRequestLogModelUnitPriceTrendPoint, error) {
	return g.requestLogModelUnitPriceTrendWithGranularity(filter, rowLimit, requestLogUsesHourlyBuckets(filter))
}

func (g *Gateway) requestLogModelUnitPriceTrendWithGranularity(filter GatewayRequestLogFilter, rowLimit int, hourly bool) ([]GatewayRequestLogModelUnitPriceTrendPoint, error) {
	if hourly {
		bucketSQL, bucketArgs := g.requestLogHourBucketSQL()
		return g.requestLogModelUnitPriceTrendWithBucket(filter, bucketSQL, true, rowLimit, bucketArgs...)
	}

	location := time.UTC
	if name := strings.TrimSpace(filter.TimeZone); name != "" {
		if configured, err := time.LoadLocation(name); err == nil {
			location = configured
		}
	}
	start, end, ok, err := g.requestLogTrendRange(filter)
	if err != nil || !ok {
		return []GatewayRequestLogModelUnitPriceTrendPoint{}, err
	}
	first := start.In(location)
	last := end.Add(-time.Nanosecond).In(location)
	day := time.Date(first.Year(), first.Month(), first.Day(), 0, 0, 0, 0, location)
	rangeEnd := time.Date(last.Year(), last.Month(), last.Day(), 0, 0, 0, 0, location).AddDate(0, 0, 1)
	trend := make([]GatewayRequestLogModelUnitPriceTrendPoint, 0)
	for day.Before(rangeEnd) {
		batchEnd := day.AddDate(0, 0, 31)
		if batchEnd.After(rangeEnd) {
			batchEnd = rangeEnd
		}
		bucketSQL, bucketArgs := g.requestLogDailyBucketSQL(day, batchEnd)
		batchFilter := requestLogBatchFilter(filter, day.UTC(), batchEnd.UTC())
		batch, err := g.requestLogModelUnitPriceTrendWithBucket(batchFilter, bucketSQL, false, rowLimit, bucketArgs...)
		if err != nil {
			return nil, err
		}
		trend = append(trend, batch...)
		day = batchEnd
	}
	return trend, nil
}

func (g *Gateway) requestLogModelUnitPriceTrendWithBucket(filter GatewayRequestLogFilter, bucketSQL string, hourly bool, rowLimit int, bucketArgs ...any) ([]GatewayRequestLogModelUnitPriceTrendPoint, error) {
	type modelBucket struct {
		Bucket              string
		ModelName           string
		InputTokens         int64
		OutputTokens        int64
		InputPricedTokens   int64
		OutputPricedTokens  int64
		CacheCreationTokens int64
		CacheReadTokens     int64
		InputCost           float64
		OutputCost          float64
	}
	inputTokensSQL := performanceInputTokensSQL("")
	outputTokensSQL := "CASE WHEN COALESCE(completion_tokens, 0) > 0 THEN COALESCE(completion_tokens, 0) ELSE 0 END"
	cacheCreationTokensSQL := "CASE WHEN COALESCE(cache_creation_tokens, 0) > 0 THEN COALESCE(cache_creation_tokens, 0) ELSE 0 END"
	cacheReadTokensSQL := "CASE WHEN COALESCE(cache_read_tokens, 0) > 0 THEN COALESCE(cache_read_tokens, 0) ELSE 0 END"
	costStatusSQL := "(" + gatewayRequestEffectiveCostStatusSQL("") + ")"
	attributionAvailableSQL := "(" + gatewayRequestCostBillableSQL("") +
		" AND " + costStatusSQL + " NOT IN ('" + string(GatewayCostStatusNone) + "', '" + string(GatewayCostStatusUnavailable) + "')" +
		" AND COALESCE(cost, 0) >= 0" +
		" AND (COALESCE(cost, 0) = 0 OR ((COALESCE(group_ratio, 0) > 0 OR COALESCE(completion_ratio, 0) > 0)" +
		" AND (" + costStatusSQL + " = '" + string(GatewayCostStatusEstimated) + "' OR (" + cacheCreationTokensSQL + " = 0 AND " + cacheReadTokensSQL + " = 0)))))"
	inputPricedTokensSQL := "CASE WHEN " + attributionAvailableSQL + " THEN " + inputTokensSQL + " ELSE 0 END"
	outputPricedTokensSQL := "CASE WHEN " + attributionAvailableSQL + " THEN " + outputTokensSQL + " ELSE 0 END"
	inputRatioSQL := "CASE WHEN COALESCE(group_ratio, 0) > 0 THEN group_ratio WHEN COALESCE(completion_ratio, 0) > 0 THEN completion_ratio ELSE 0 END"
	outputRatioSQL := "CASE WHEN COALESCE(completion_ratio, 0) > 0 THEN completion_ratio WHEN COALESCE(group_ratio, 0) > 0 THEN group_ratio ELSE 0 END"
	inputWeightSQL := "((" + inputTokensSQL + ") * (" + inputRatioSQL + "))"
	outputWeightSQL := "((" + outputTokensSQL + ") * (" + outputRatioSQL + "))"
	totalWeightSQL := "((" + inputWeightSQL + ") + (" + outputWeightSQL + "))"
	inputCostSQL := "CASE WHEN " + attributionAvailableSQL + " AND COALESCE(cost, 0) > 0 AND " + totalWeightSQL + " > 0 THEN cost * " + inputWeightSQL + " / " + totalWeightSQL + " ELSE 0 END"
	outputCostSQL := "CASE WHEN " + attributionAvailableSQL + " AND COALESCE(cost, 0) > 0 AND " + totalWeightSQL + " > 0 THEN cost * " + outputWeightSQL + " / " + totalWeightSQL + " ELSE 0 END"
	rows := []modelBucket{}
	query := g.applyRequestLogFilter(g.db.Model(&GatewayRequestLog{}), "", filter).
		Where("downstream_key_id > 0").
		Where(performanceGatewayRequestSQL("")).
		Where("TRIM(model_name) <> ''").
		Select(bucketSQL+` AS bucket,
			model_name,
			COALESCE(SUM(`+inputTokensSQL+`), 0) AS input_tokens,
			COALESCE(SUM(`+outputTokensSQL+`), 0) AS output_tokens,
			COALESCE(SUM(`+inputPricedTokensSQL+`), 0) AS input_priced_tokens,
			COALESCE(SUM(`+outputPricedTokensSQL+`), 0) AS output_priced_tokens,
			COALESCE(SUM(`+cacheCreationTokensSQL+`), 0) AS cache_creation_tokens,
			COALESCE(SUM(`+cacheReadTokensSQL+`), 0) AS cache_read_tokens,
			COALESCE(SUM(`+inputCostSQL+`), 0) AS input_cost,
			COALESCE(SUM(`+outputCostSQL+`), 0) AS output_cost`, bucketArgs...).
		Group("bucket, model_name").
		Order("bucket ASC, model_name ASC")
	if rowLimit > 0 {
		query = query.Limit(rowLimit)
	}
	if err := query.Scan(&rows).Error; err != nil {
		return nil, err
	}
	if rowLimit > 0 && len(rows) >= rowLimit {
		return nil, errGatewayRequestAggregateGroupedLimit
	}

	trend := make([]GatewayRequestLogModelUnitPriceTrendPoint, 0, len(rows)*4)
	for _, row := range rows {
		modelName := strings.TrimSpace(row.ModelName)
		family := gatewayUnitPriceModelFamily(modelName)
		if family == "" {
			continue
		}
		bucket := row.Bucket
		if hourly {
			parsed, ok := g.parseRequestLogHourBucket(bucket)
			if !ok {
				continue
			}
			bucket = parsed.UTC().Format("2006-01-02T15:00:00Z")
		}
		trend = appendGatewayUnitPricePoints(trend, bucket, family, modelName, gatewayRequestUnitPriceContribution{
			InputTokens:         row.InputTokens,
			OutputTokens:        row.OutputTokens,
			InputPricedTokens:   row.InputPricedTokens,
			OutputPricedTokens:  row.OutputPricedTokens,
			CacheCreationTokens: row.CacheCreationTokens,
			CacheReadTokens:     row.CacheReadTokens,
			InputCost:           row.InputCost,
			OutputCost:          row.OutputCost,
		})
	}
	return trend, nil
}

func appendGatewayUnitPricePoints(points []GatewayRequestLogModelUnitPriceTrendPoint, bucket, family, modelName string, value gatewayRequestUnitPriceContribution) []GatewayRequestLogModelUnitPriceTrendPoint {
	appendPriced := func(tokenType string, tokens int64, cost float64) {
		if tokens <= 0 {
			return
		}
		price := cost * 1_000_000 / float64(tokens)
		costCopy := cost
		points = append(points, GatewayRequestLogModelUnitPriceTrendPoint{
			Bucket: bucket, ModelFamily: family, ModelName: modelName, TokenType: tokenType,
			TotalTokens: tokens, TotalCost: &costCopy, CostPerMillionToken: &price,
		})
	}
	appendUnpriced := func(tokenType string, tokens int64) {
		if tokens <= 0 {
			return
		}
		points = append(points, GatewayRequestLogModelUnitPriceTrendPoint{
			Bucket: bucket, ModelFamily: family, ModelName: modelName, TokenType: tokenType, TotalTokens: tokens,
		})
	}
	appendComponent := func(tokenType string, tokens, pricedTokens int64, cost float64) {
		if tokens <= 0 {
			return
		}
		if pricedTokens != tokens {
			appendUnpriced(tokenType, tokens)
			return
		}
		appendPriced(tokenType, tokens, cost)
	}
	appendComponent(GatewayTokenPriceInput, value.InputTokens, value.InputPricedTokens, value.InputCost)
	appendComponent(GatewayTokenPriceOutput, value.OutputTokens, value.OutputPricedTokens, value.OutputCost)
	appendUnpriced(GatewayTokenPriceCacheCreation, value.CacheCreationTokens)
	appendUnpriced(GatewayTokenPriceCacheRead, value.CacheReadTokens)
	return points
}

func gatewayUnitPriceModelFamily(modelName string) string {
	model := strings.ToLower(strings.TrimSpace(modelName))
	if model == "" || strings.Contains(model, "test") {
		return ""
	}
	if strings.HasPrefix(model, "claude") {
		return "claude"
	}
	if strings.HasPrefix(model, "gpt") || strings.HasPrefix(model, "chatgpt") || strings.HasPrefix(model, "codex") || model == "o1" || strings.HasPrefix(model, "o1-") || model == "o3" || strings.HasPrefix(model, "o3-") || model == "o4" || strings.HasPrefix(model, "o4-") {
		return "gpt"
	}
	return ""
}

func (g *Gateway) requestLogTrendWithBucket(filter GatewayRequestLogFilter, bucketSQL string, hourly bool, bucketArgs ...any) ([]GatewayRequestLogTrendPoint, error) {
	trend := []GatewayRequestLogTrendPoint{}
	query := g.applyRequestLedgerFilter(g.db.Model(&GatewayRequestLog{}), "", filter)
	if err := query.
		Select(bucketSQL+` AS bucket,
			`+gatewayRequestLedgerMetricsSelectSQL(""), bucketArgs...).
		Group("bucket").
		Order("bucket ASC").
		Scan(&trend).Error; err != nil {
		return nil, err
	}
	if hourly {
		for index := range trend {
			parsed, ok := g.parseRequestLogHourBucket(trend[index].Bucket)
			if !ok {
				continue
			}
			trend[index].Bucket = parsed.UTC().Format("2006-01-02T15:00:00Z")
		}
	}
	return trend, nil
}

func (g *Gateway) requestLogTrendRange(filter GatewayRequestLogFilter) (time.Time, time.Time, bool, error) {
	var first GatewayRequestLog
	if err := g.applyRequestLedgerFilter(g.db.Model(&GatewayRequestLog{}), "", filter).
		Select("created_at").Order("created_at ASC").Take(&first).Error; errors.Is(err, gorm.ErrRecordNotFound) {
		return time.Time{}, time.Time{}, false, nil
	} else if err != nil {
		return time.Time{}, time.Time{}, false, err
	}
	var last GatewayRequestLog
	if err := g.applyRequestLedgerFilter(g.db.Model(&GatewayRequestLog{}), "", filter).
		Select("created_at").Order("created_at DESC").Take(&last).Error; err != nil {
		return time.Time{}, time.Time{}, false, err
	}
	start := first.CreatedAt
	end := last.CreatedAt.Add(time.Nanosecond)
	return start, end, end.After(start), nil
}

func requestLogBatchFilter(filter GatewayRequestLogFilter, start, end time.Time) GatewayRequestLogFilter {
	if filter.Start == nil || start.After(*filter.Start) {
		filter.Start = &start
	}
	if filter.End == nil || end.Before(*filter.End) {
		filter.End = &end
	}
	return filter
}

func (g *Gateway) requestLogDailyBucketSQL(day, end time.Time) (string, []any) {
	var bucket strings.Builder
	bucket.WriteString("CASE")
	args := make([]any, 0)
	for day.Before(end) {
		next := day.AddDate(0, 0, 1)
		bucket.WriteString(" WHEN created_at >= ? AND created_at < ? THEN ?")
		args = append(args, g.requestLogTimeBoundary(day.UTC()), g.requestLogTimeBoundary(next.UTC()), day.Format("2006-01-02"))
		day = next
	}
	bucket.WriteString(" END")
	return bucket.String(), args
}

func (g *Gateway) requestLogHourBucketSQL() (string, []any) {
	return `TO_CHAR(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:00:00')`, nil
}

func (g *Gateway) parseRequestLogHourBucket(value string) (time.Time, bool) {
	parsed, err := time.ParseInLocation("2006-01-02T15:00:00", value, time.UTC)
	return parsed, err == nil
}

func (g *Gateway) applyRequestLogFilter(query *gorm.DB, alias string, filter GatewayRequestLogFilter) *gorm.DB {
	return g.applyRequestLogDimensions(query.Where(userVisibleGatewayRequestSQL(alias)), alias, filter)
}

// applyRequestLedgerFilter reads the user-visible logical request ledger plus
// hidden physical-dispatch cost evidence. Result filters remain logical-request
// filters, so specifying success/failed/canceled always excludes evidence.
func (g *Gateway) applyRequestLedgerFilter(query *gorm.DB, alias string, filter GatewayRequestLogFilter) *gorm.DB {
	scope := "(" + userVisibleGatewayRequestSQL(alias) + " OR " + gatewayDispatchCostEvidenceRecordSQL(alias) + ")"
	return g.applyRequestLogDimensions(query.Where(scope), alias, filter)
}

func (g *Gateway) applyRequestLogDimensions(query *gorm.DB, alias string, filter GatewayRequestLogFilter) *gorm.DB {
	prefix := ""
	if strings.TrimSpace(alias) != "" {
		prefix = strings.TrimSpace(alias) + "."
	}
	if filter.Start != nil {
		query = query.Where(prefix+"created_at >= ?", g.requestLogTimeBoundary(*filter.Start))
	}
	if filter.End != nil {
		query = query.Where(prefix+"created_at < ?", g.requestLogTimeBoundary(*filter.End))
	}
	if filter.DownstreamKeyID > 0 {
		query = query.Where(prefix+"downstream_key_id = ?", filter.DownstreamKeyID)
	}
	if strings.TrimSpace(filter.ModelName) != "" {
		query = query.Where(prefix+"model_name = ?", strings.TrimSpace(filter.ModelName))
	}
	if filter.ChannelID > 0 {
		query = query.Where(prefix+"channel_id = ?", filter.ChannelID)
	}
	if groupName := strings.TrimSpace(filter.GroupName); groupName != "" {
		query = query.Where("TRIM(COALESCE("+prefix+"group_name, '')) = ?", groupName)
	}
	if filter.GroupUnassigned {
		if strings.TrimSpace(filter.GroupName) != "" {
			return query.Where("1 = 0")
		}
		query = query.Where("TRIM(COALESCE(" + prefix + "group_name, '')) = ''")
	}
	if upstreamRequestID := filter.UpstreamRequestID; upstreamRequestID != "" {
		query = query.Where(prefix+"upstream_request_id = ?", upstreamRequestID)
	}
	if upstreamKeyName := strings.TrimSpace(filter.UpstreamKeyName); upstreamKeyName != "" {
		query = query.Where(prefix+"upstream_key_name = ?", upstreamKeyName)
	}
	if routeReason := strings.TrimSpace(filter.RouteReason); routeReason != "" {
		query = query.Where("COALESCE(NULLIF(TRIM("+prefix+"route_reason), ''), 'smart_primary') = ?", routeReason)
	}
	switch strings.ToLower(strings.TrimSpace(filter.Result)) {
	case "":
	case GatewayRequestResultSuccess:
		query = query.Where(successfulGatewayRequestSQL(alias))
	case GatewayRequestResultFailed:
		query = query.Where(failedGatewayRequestSQL(alias))
	case GatewayRequestResultCanceled:
		query = query.Where(canceledGatewayRequestSQL(alias))
	default:
		// API callers reject unknown values. Keep direct storage callers fail-closed.
		query = query.Where("1 = 0")
	}
	return query
}

func (g *Gateway) requestLogTimeBoundary(value time.Time) time.Time {
	return value.UTC()
}

func gatewayRequestFiniteNonnegativeFloatSQL(value string) string {
	return "(" + value + " >= 0 AND " + value + " NOT IN ('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision))"
}

// gatewayRequestCalculatedCostSQL mirrors EvaluateGatewayRequestPriceFact for
// the immutable pricing snapshot stored on a request log. It returns numeric
// so extreme but finite float8 unit prices cannot overflow during ordering.
func gatewayRequestCalculatedCostSQL(alias string) string {
	prefix := ""
	if strings.TrimSpace(alias) != "" {
		prefix = strings.TrimSpace(alias) + "."
	}
	column := func(name string) string { return prefix + name }
	promptTokens := "COALESCE(" + column("prompt_tokens") + ", 0)::numeric"
	inputTokens := "(" + promptTokens + " - COALESCE(" + column("cache_creation_tokens") + ", 0)::numeric - COALESCE(" + column("cache_read_tokens") + ", 0)::numeric)"
	completionTokens := "COALESCE(" + column("completion_tokens") + ", 0)::numeric"
	cacheReadTokens := "COALESCE(" + column("cache_read_tokens") + ", 0)::numeric"
	cacheWriteTokens := "COALESCE(" + column("cache_creation_tokens") + ", 0)::numeric"
	operationUnits := "COALESCE(" + column("operation_units") + ", 0)::numeric"
	validPrice := func(name string) string {
		value := column(name)
		return "(" + value + " IS NULL OR " + gatewayRequestFiniteNonnegativeFloatSQL(value) + ")"
	}
	validPrices := validPrice("price_input_unit") + " AND " + validPrice("price_output_unit") +
		" AND " + validPrice("price_cache_read_unit") + " AND " + validPrice("price_cache_write_unit") +
		" AND " + validPrice("price_long_context_input_unit") + " AND " + validPrice("price_long_context_output_unit") +
		" AND " + validPrice("price_long_context_cache_read_unit") + " AND " + validPrice("price_long_context_cache_write_unit") +
		" AND " + validPrice("group_ratio") + " AND " + validPrice("completion_ratio") +
		" AND " + validPrice("price_operation_unit")
	base := column("price_fact_id") + " IS NOT NULL" +
		" AND " + column("price_fact_schema_version") + " IN (" + strconv.Itoa(GatewayRequestPriceFactSchemaVersion) + ", " + strconv.Itoa(GatewayRequestPriceFactTieredSchemaVersion) + ")" +
		" AND UPPER(TRIM(COALESCE(" + column("price_currency") + ", ''))) = '" + gatewayRequestPriceSupportedCurrency + "'" +
		" AND " + validPrices
	flatTokenCost := "(" +
		inputTokens + " * COALESCE(" + column("price_input_unit") + ", 0)::numeric + " +
		completionTokens + " * COALESCE(" + column("price_output_unit") + ", 0)::numeric + " +
		cacheReadTokens + " * COALESCE(" + column("price_cache_read_unit") + ", 0)::numeric + " +
		cacheWriteTokens + " * COALESCE(" + column("price_cache_write_unit") + ", 0)::numeric) / 1000000::numeric"
	useLongTier := promptTokens + " > " + column("price_long_context_threshold_tokens") + "::numeric"
	priceForTier := func(shortColumn, longColumn string) string {
		return "CASE WHEN " + useLongTier + " THEN " + column(longColumn) + " ELSE " + column(shortColumn) + " END"
	}
	tieredTokenCost := "(" +
		inputTokens + " * " + priceForTier("price_input_unit", "price_long_context_input_unit") + "::numeric * " + column("group_ratio") + "::numeric + " +
		completionTokens + " * " + priceForTier("price_output_unit", "price_long_context_output_unit") + "::numeric * " + column("completion_ratio") + "::numeric + " +
		cacheReadTokens + " * COALESCE(" + priceForTier("price_cache_read_unit", "price_long_context_cache_read_unit") + ", 0)::numeric * " + column("group_ratio") + "::numeric + " +
		cacheWriteTokens + " * COALESCE(" + priceForTier("price_cache_write_unit", "price_long_context_cache_write_unit") + ", 0)::numeric * " + column("group_ratio") + "::numeric) / 1000000::numeric"

	textProtocols := "('" + GatewayPricingProtocolOpenAIChatCompletions + "', '" + GatewayPricingProtocolOpenAICompletions +
		"', '" + GatewayPricingProtocolOpenAIResponses + "', '" + GatewayPricingProtocolAnthropicMessages + "')"
	textCommonValid := column("price_operation") + " = '" + GatewayPricingOperationTextGeneration + "'" +
		" AND " + column("price_protocol") + " IN " + textProtocols +
		" AND " + column("price_unit") + " = '" + GatewayPricingUnitPerMillionTokens + "'" +
		" AND " + column("price_operation_unit") + " IS NULL" +
		" AND COALESCE(" + column("usage_input_present") + ", FALSE)" +
		" AND COALESCE(" + column("usage_output_present") + ", FALSE)" +
		" AND COALESCE(" + column("usage_cache_read_present") + ", FALSE)" +
		" AND COALESCE(" + column("usage_cache_write_present") + ", FALSE)" +
		" AND " + inputTokens + " >= 0 AND " + completionTokens + " >= 0 AND " + cacheReadTokens + " >= 0 AND " + cacheWriteTokens + " >= 0"
	flatTextValid := column("price_fact_schema_version") + " = " + strconv.Itoa(GatewayRequestPriceFactSchemaVersion) +
		" AND (" + column("price_input_unit") + " IS NOT NULL OR " + column("price_output_unit") + " IS NOT NULL OR " +
		column("price_cache_read_unit") + " IS NOT NULL OR " + column("price_cache_write_unit") + " IS NOT NULL)" +
		" AND " + column("price_long_context_threshold_tokens") + " IS NULL" +
		" AND " + column("price_long_context_input_unit") + " IS NULL AND " + column("price_long_context_output_unit") + " IS NULL" +
		" AND " + column("price_long_context_cache_read_unit") + " IS NULL AND " + column("price_long_context_cache_write_unit") + " IS NULL" +
		" AND (" + inputTokens + " = 0 OR " + column("price_input_unit") + " IS NOT NULL)" +
		" AND (" + completionTokens + " = 0 OR " + column("price_output_unit") + " IS NOT NULL)" +
		" AND (" + cacheReadTokens + " = 0 OR " + column("price_cache_read_unit") + " IS NOT NULL)" +
		" AND (" + cacheWriteTokens + " = 0 OR " + column("price_cache_write_unit") + " IS NOT NULL)"
	tieredTextValid := column("price_fact_schema_version") + " = " + strconv.Itoa(GatewayRequestPriceFactTieredSchemaVersion) +
		" AND " + column("price_input_unit") + " IS NOT NULL AND " + column("price_output_unit") + " IS NOT NULL" +
		" AND " + column("price_long_context_threshold_tokens") + " > 0" +
		" AND " + column("price_long_context_input_unit") + " IS NOT NULL AND " + column("price_long_context_output_unit") + " IS NOT NULL" +
		" AND " + column("group_ratio") + " IS NOT NULL AND " + column("completion_ratio") + " IS NOT NULL" +
		" AND (" + cacheReadTokens + " = 0 OR " + priceForTier("price_cache_read_unit", "price_long_context_cache_read_unit") + " IS NOT NULL)" +
		" AND (" + cacheWriteTokens + " = 0 OR " + priceForTier("price_cache_write_unit", "price_long_context_cache_write_unit") + " IS NOT NULL)"
	embeddingValid := column("price_fact_schema_version") + " = " + strconv.Itoa(GatewayRequestPriceFactSchemaVersion) +
		" AND " + column("price_operation") + " = '" + GatewayPricingOperationEmbedding + "'" +
		" AND " + column("price_protocol") + " = '" + GatewayPricingProtocolOpenAIEmbeddings + "'" +
		" AND " + column("price_unit") + " = '" + GatewayPricingUnitPerMillionTokens + "'" +
		" AND " + column("price_input_unit") + " IS NOT NULL" +
		" AND " + column("price_output_unit") + " IS NULL AND " + column("price_cache_read_unit") + " IS NULL" +
		" AND " + column("price_cache_write_unit") + " IS NULL AND " + column("price_operation_unit") + " IS NULL" +
		" AND COALESCE(" + column("usage_input_present") + ", FALSE) AND " + inputTokens + " >= 0"
	imageValid := column("price_fact_schema_version") + " = " + strconv.Itoa(GatewayRequestPriceFactSchemaVersion) +
		" AND " + column("price_operation") + " = '" + GatewayImageOperationGeneration + "'" +
		" AND " + column("price_protocol") + " IN ('" + GatewayImageProtocolImages + "', '" + GatewayImageProtocolResponses + "')" +
		" AND " + column("price_unit") + " = '" + GatewayPricingUnitPerOperation + "'" +
		" AND " + column("price_input_unit") + " IS NULL AND " + column("price_output_unit") + " IS NULL" +
		" AND " + column("price_cache_read_unit") + " IS NULL AND " + column("price_cache_write_unit") + " IS NULL" +
		" AND " + column("price_operation_unit") + " IS NOT NULL" +
		" AND COALESCE(" + column("usage_operation_present") + ", FALSE)" +
		" AND " + column("operation_units") + " IS NOT NULL AND " + operationUnits + " >= 0"

	return "CASE WHEN " + base + " AND " + textCommonValid + " AND " + flatTextValid + " THEN " + flatTokenCost +
		" WHEN " + base + " AND " + textCommonValid + " AND " + tieredTextValid + " THEN " + tieredTokenCost +
		" WHEN " + base + " AND " + embeddingValid + " THEN " + promptTokens + " * " + column("price_input_unit") + "::numeric / 1000000::numeric" +
		" WHEN " + base + " AND " + imageValid + " THEN " + operationUnits + " * " + column("price_operation_unit") + "::numeric" +
		" ELSE NULL END"
}

func gatewayRequestDisplayCostSQL(alias string) string {
	prefix := ""
	if strings.TrimSpace(alias) != "" {
		prefix = strings.TrimSpace(alias) + "."
	}
	calculated := "(" + gatewayRequestCalculatedCostSQL(alias) + ")"
	status := "(" + gatewayRequestEffectiveCostStatusSQL(alias) + ")"
	finiteCost := gatewayRequestFiniteNonnegativeFloatSQL(prefix + "cost")
	return "CASE" +
		" WHEN " + status + " IN ('" + string(GatewayCostStatusReported) + "', '" + string(GatewayCostStatusReconciled) + "') AND " + finiteCost + " THEN " + prefix + "cost::numeric" +
		" WHEN " + calculated + " >= 0 AND " + calculated + " <= 1.7976931348623157e308::numeric THEN " + calculated +
		" WHEN " + prefix + "price_fact_id IS NULL AND " + status + " = '" + string(GatewayCostStatusEstimated) + "' AND " + finiteCost + " THEN " + prefix + "cost::numeric" +
		" ELSE NULL END"
}

func gatewayRequestLogOrder(alias string, sortBy string, sortOrder string) string {
	prefix := ""
	if strings.TrimSpace(alias) != "" {
		prefix = strings.TrimSpace(alias) + "."
	}
	column := "created_at"
	switch strings.ToLower(strings.TrimSpace(sortBy)) {
	case "", GatewayRequestLogSortCreatedAt:
	case GatewayRequestLogSortModelName:
		column = "model_name"
	case GatewayRequestLogSortLatencyMs:
		column = "latency_ms"
	case GatewayRequestLogSortTotalTokens:
		column = "total_tokens"
	case GatewayRequestLogSortCost:
		direction := "DESC"
		if strings.EqualFold(strings.TrimSpace(sortOrder), GatewayRequestLogSortAscending) {
			direction = "ASC"
		}
		return gatewayRequestDisplayCostSQL(alias) + " " + direction + " NULLS LAST, " + prefix + "id " + direction
	case GatewayRequestLogSortStatusCode:
		column = "status_code"
	}
	direction := "DESC"
	if strings.EqualFold(strings.TrimSpace(sortOrder), GatewayRequestLogSortAscending) {
		direction = "ASC"
	}
	return prefix + column + " " + direction + ", " + prefix + "id " + direction
}

func (g *Gateway) SummarizeUsage(downstreamKeyID uint) (*GatewayUsageSummary, error) {
	if summary, used, err := g.summarizeUsageFromAggregates(downstreamKeyID); used || err != nil {
		return summary, err
	}
	return g.summarizeUsageRaw(downstreamKeyID)
}

func (g *Gateway) summarizeUsageRaw(downstreamKeyID uint) (*GatewayUsageSummary, error) {
	var row struct {
		TotalRequests       int64
		Success             int64
		Failed              int64
		AvgLatency          float64
		LastUsedAt          string
		TotalTokens         int64
		PromptTokens        int64
		CompletionTokens    int64
		TotalCost           float64
		EstimatedCost       float64
		ReportedCost        float64
		ReconciledCost      float64
		ActualCost          float64
		UnavailableRequests int64
	}
	successSQL := successfulGatewayRequestSQL("")
	estimatedCostSQL := gatewayRequestCostByStatusSQL("", GatewayCostStatusEstimated)
	reportedCostSQL := gatewayRequestCostByStatusSQL("", GatewayCostStatusReported)
	reconciledCostSQL := gatewayRequestCostByStatusSQL("", GatewayCostStatusReconciled)
	err := g.db.Model(&GatewayRequestLog{}).
		Select(`COUNT(*) AS total_requests,
			COALESCE(SUM(CASE WHEN `+successSQL+` THEN 1 ELSE 0 END), 0) AS success,
			COALESCE(SUM(CASE WHEN NOT (`+successSQL+`) THEN 1 ELSE 0 END), 0) AS failed,
			COALESCE(AVG(CASE WHEN `+successSQL+` THEN latency_ms END), 0) AS avg_latency,
			MAX(created_at) AS last_used_at,
			COALESCE(SUM(total_tokens), 0) AS total_tokens,
			COALESCE(SUM(prompt_tokens), 0) AS prompt_tokens,
			COALESCE(SUM(completion_tokens), 0) AS completion_tokens,
			COALESCE(SUM(`+gatewayRequestBillableCostSQL("")+`), 0) AS total_cost,
			COALESCE(SUM(`+estimatedCostSQL+`), 0) AS estimated_cost,
			COALESCE(SUM(`+reportedCostSQL+`), 0) AS reported_cost,
			COALESCE(SUM(`+reconciledCostSQL+`), 0) AS reconciled_cost,
			COALESCE(SUM(`+gatewayRequestActualCostSQL("")+`), 0) AS actual_cost,
			COALESCE(SUM(`+gatewayRequestUnavailableCountSQL("")+`), 0) AS unavailable_requests`).
		Where("downstream_key_id = ? AND "+userVisibleGatewayRequestSQL(""), downstreamKeyID).
		Scan(&row).Error
	if err != nil {
		return nil, err
	}
	var lastUsedAt *time.Time
	if parsed, ok := g.parseRequestLogAggregateTime(row.LastUsedAt); ok {
		lastUsedAt = &parsed
	}
	unit := "requests"
	used := float64(row.TotalRequests)
	total := float64(row.TotalRequests)
	if row.TotalCost > 0 {
		unit = "estimated_usd"
		if row.ActualCost > 0 && row.EstimatedCost == 0 {
			unit = "USD"
		}
		used = row.TotalCost
		total = row.TotalCost
	} else if row.TotalTokens > 0 {
		unit = "tokens"
		used = float64(row.TotalTokens)
		total = float64(row.TotalTokens)
	}
	return &GatewayUsageSummary{
		IsActive:            true,
		Used:                used,
		Total:               total,
		Remaining:           0,
		Unit:                unit,
		Requests:            row.TotalRequests,
		Success:             row.Success,
		Failed:              row.Failed,
		AvgLatency:          row.AvgLatency,
		LastUsedAt:          lastUsedAt,
		TotalTokens:         row.TotalTokens,
		PromptTokens:        row.PromptTokens,
		CompletionTokens:    row.CompletionTokens,
		TotalCost:           row.TotalCost,
		ActualCost:          row.ActualCost,
		EstimatedCost:       row.EstimatedCost,
		ReportedCost:        row.ReportedCost,
		ReconciledCost:      row.ReconciledCost,
		UnavailableRequests: row.UnavailableRequests,
		Extra:               "requests, tokens, and cost totals",
	}, nil
}

func (g *Gateway) IsModelUnsupported(channelID uint, groupName string, modelName string, maxAge time.Duration) (bool, error) {
	var capability GatewayModelCapability
	err := currentGatewayModelCapabilityScope(g.db).Where(
		"channel_id = ? AND lower(btrim(group_name)) = lower(btrim(?)) AND model_name = ?",
		channelID,
		groupName,
		modelName,
	).First(&capability).Error
	switch err {
	case nil:
		if capability.Supported {
			return false, nil
		}
		return time.Since(capability.LastCheckedAt) <= maxAge, nil
	case gorm.ErrRecordNotFound:
		return false, nil
	default:
		return false, err
	}
}

func (g *Gateway) IsModelUnsupportedForIdentity(fence GatewayModelFactFence, modelName string, maxAge time.Duration) (bool, error) {
	capability, err := g.FindModelCapabilityForIdentity(fence, modelName)
	if err != nil || capability == nil || capability.Supported {
		return false, err
	}
	return time.Since(capability.LastCheckedAt) <= maxAge, nil
}

func (g *Gateway) FindModelCapability(channelID uint, groupName string, modelName string) (*GatewayModelCapability, error) {
	var capability GatewayModelCapability
	err := currentGatewayModelCapabilityScope(g.db).Where(
		"channel_id = ? AND lower(btrim(group_name)) = lower(btrim(?)) AND model_name = ?",
		channelID,
		strings.TrimSpace(groupName),
		strings.TrimSpace(modelName),
	).First(&capability).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &capability, nil
}

func (g *Gateway) FindModelCapabilityForIdentity(fence GatewayModelFactFence, modelName string) (*GatewayModelCapability, error) {
	if !fence.Valid() || strings.TrimSpace(modelName) == "" {
		return nil, nil
	}
	var capability GatewayModelCapability
	err := currentGatewayModelCapabilityScope(g.db).Where(
		"channel_id = ? AND lower(btrim(group_name)) = ? AND model_name = ? AND upstream_group_id = ? AND presence_generation = ? AND channel_identity_revision = ?",
		fence.ChannelID, fence.CanonicalGroupName, strings.TrimSpace(modelName),
		fence.UpstreamGroupID, fence.PresenceGeneration, fence.ChannelIdentityRevision,
	).First(&capability).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &capability, nil
}

func (g *Gateway) ObserveModelContextLimit(channelID uint, groupName string, modelName string, contextWindowTokens, safeInputTokens, contextSafetyTokens int64, observedAt time.Time, generations ...UpstreamGroupGeneration) error {
	return g.observeModelContextLimit(channelID, groupName, modelName, contextWindowTokens, safeInputTokens, contextSafetyTokens, observedAt, nil, generations...)
}

func (g *Gateway) ObserveModelContextLimitForIdentity(fence GatewayModelFactFence, modelName string, contextWindowTokens, safeInputTokens, contextSafetyTokens int64, observedAt time.Time) error {
	if !fence.Valid() {
		return ErrUpstreamGroupGenerationChanged
	}
	return g.observeModelContextLimit(fence.ChannelID, fence.CanonicalGroupName, modelName, contextWindowTokens, safeInputTokens, contextSafetyTokens, observedAt, &fence)
}

func (g *Gateway) observeModelContextLimit(channelID uint, groupName string, modelName string, contextWindowTokens, safeInputTokens, contextSafetyTokens int64, observedAt time.Time, fence *GatewayModelFactFence, generations ...UpstreamGroupGeneration) error {
	groupName = strings.TrimSpace(groupName)
	modelName = strings.TrimSpace(modelName)
	if channelID == 0 || groupName == "" || modelName == "" || safeInputTokens <= 0 {
		return nil
	}
	if observedAt.IsZero() {
		observedAt = time.Now()
	}
	updates := map[string]any{
		"supported":                 true,
		"last_checked_at":           observedAt,
		"last_error":                "",
		"context_window_tokens":     max(contextWindowTokens, 0),
		"safe_input_tokens":         safeInputTokens,
		"context_safety_tokens":     max(contextSafetyTokens, 0),
		"context_limit_observed_at": observedAt,
	}
	err := g.db.Transaction(func(tx *gorm.DB) error {
		if err := lockUpstreamGroupParentChannelTx(tx, channelID); err != nil {
			return err
		}
		if err := lockAutomaticAssignmentMutationTx(tx); err != nil {
			return err
		}
		canonical, modelFence, err := lockGatewayModelFactWriteTx(tx, channelID, groupName, observedAt, fence, generations...)
		if err != nil {
			return err
		}
		channelID = modelFence.ChannelID
		groupName = canonical
		updates["upstream_group_id"] = modelFence.UpstreamGroupID
		updates["presence_generation"] = modelFence.PresenceGeneration
		updates["channel_identity_revision"] = modelFence.ChannelIdentityRevision
		var existing GatewayModelCapability
		err = tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("channel_id = ? AND lower(btrim(group_name)) = lower(btrim(?)) AND model_name = ?", channelID, groupName, modelName).
			Order("last_checked_at DESC, id DESC").First(&existing).Error
		if err == nil {
			return tx.Model(&existing).Updates(updates).Error
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		return tx.Create(&GatewayModelCapability{
			ChannelID: channelID, GroupName: groupName, ModelName: modelName,
			UpstreamGroupID: modelFence.UpstreamGroupID, PresenceGeneration: modelFence.PresenceGeneration,
			ChannelIdentityRevision: modelFence.ChannelIdentityRevision,
			Supported:               true, LastCheckedAt: observedAt,
			ContextWindowTokens: max(contextWindowTokens, 0), SafeInputTokens: safeInputTokens,
			ContextSafetyTokens: max(contextSafetyTokens, 0), ContextLimitObserved: &observedAt,
		}).Error
	})
	if err == nil {
		err = g.refreshUpstreamGroupFactsFromKey(channelID, groupName, observedAt)
	}
	return err
}

func (g *Gateway) ListUnsupportedModelCapabilities(modelName string, maxAge time.Duration) ([]GatewayModelCapability, error) {
	modelName = strings.TrimSpace(modelName)
	if modelName == "" {
		return []GatewayModelCapability{}, nil
	}
	query := currentGatewayModelCapabilityScope(g.db).Where("model_name = ? AND supported = ?", modelName, false)
	if maxAge > 0 {
		query = query.Where("last_checked_at >= ?", time.Now().Add(-maxAge))
	}
	items := []GatewayModelCapability{}
	err := query.Find(&items).Error
	return items, err
}

// ListModelCapabilities returns both positive and negative observations for a
// model. Callers that need a current-generation negative gate must retain the
// latest observation for each channel/group instead of treating an older
// negative row as authoritative after a later positive observation.
func (g *Gateway) ListModelCapabilities(modelName string, maxAge time.Duration) ([]GatewayModelCapability, error) {
	modelName = strings.TrimSpace(modelName)
	items := make([]GatewayModelCapability, 0)
	if modelName == "" {
		return items, nil
	}
	query := currentGatewayModelCapabilityScope(g.db).Where("model_name = ?", modelName)
	if maxAge > 0 {
		query = query.Where("last_checked_at >= ?", time.Now().Add(-maxAge))
	}
	err := query.Order("channel_id ASC").Order("group_name ASC").Order("last_checked_at DESC").Order("id DESC").Find(&items).Error
	return items, err
}

// GatewayModelCapabilityRoute identifies one exact channel/group route for a
// bounded capability lookup. GroupName is intentionally kept as the original
// route spelling: PostgreSQL applies lower(btrim(...)) to both operands so its
// canonical-group semantics remain the authority, including for non-ASCII
// names.
type GatewayModelCapabilityRoute struct {
	ChannelID uint
	GroupName string
}

// ListModelCapabilitiesForRoutes returns observations for one model only on
// the requested channel/group pairs. It deliberately keeps each channel and
// group in the same SQL predicate; independent channel and group IN filters
// would admit their cross product and can read an unrelated candidate route.
//
// This is one bounded PostgreSQL query for the supplied route set. It is used
// by fixed-route configuration projections, which must not scan capability
// facts for every route in the installation just to evaluate one candidate
// group.
func (g *Gateway) ListModelCapabilitiesForRoutes(modelName string, routes []GatewayModelCapabilityRoute, maxAge time.Duration) ([]GatewayModelCapability, error) {
	modelName = strings.TrimSpace(modelName)
	items := make([]GatewayModelCapability, 0)
	if modelName == "" {
		return items, nil
	}
	predicate, arguments := modelCapabilityRoutePredicate(routes)
	if predicate == "" {
		return items, nil
	}
	query := currentGatewayModelCapabilityScope(g.db).Where("model_name = ?", modelName).Where(predicate, arguments...)
	if maxAge > 0 {
		query = query.Where("last_checked_at >= ?", time.Now().Add(-maxAge))
	}
	err := query.Order("channel_id ASC").Order("group_name ASC").Order("last_checked_at DESC").Order("id DESC").Find(&items).Error
	return items, err
}

func modelCapabilityRoutePredicate(routes []GatewayModelCapabilityRoute) (string, []any) {
	clauses := make([]string, 0, len(routes))
	arguments := make([]any, 0, len(routes)*2)
	seen := make(map[GatewayModelCapabilityRoute]struct{}, len(routes))
	for _, route := range routes {
		route.GroupName = strings.TrimSpace(route.GroupName)
		if route.ChannelID == 0 || route.GroupName == "" {
			continue
		}
		if _, exists := seen[route]; exists {
			continue
		}
		seen[route] = struct{}{}
		clauses = append(clauses, "(channel_id = ? AND lower(btrim(group_name)) = lower(btrim(?)))")
		arguments = append(arguments, route.ChannelID, route.GroupName)
	}
	if len(clauses) == 0 {
		return "", nil
	}
	return "(" + strings.Join(clauses, " OR ") + ")", arguments
}

func (g *Gateway) MarkModelCapability(channelID uint, groupName string, modelName string, supported bool, lastError string, generations ...UpstreamGroupGeneration) error {
	return g.markModelCapability(channelID, groupName, modelName, supported, lastError, nil, generations...)
}

func (g *Gateway) MarkModelCapabilityForIdentity(fence GatewayModelFactFence, modelName string, supported bool, lastError string) error {
	if !fence.Valid() {
		return ErrUpstreamGroupGenerationChanged
	}
	return g.markModelCapability(fence.ChannelID, fence.CanonicalGroupName, modelName, supported, lastError, &fence)
}

func (g *Gateway) markModelCapability(channelID uint, groupName string, modelName string, supported bool, lastError string, fence *GatewayModelFactFence, generations ...UpstreamGroupGeneration) error {
	now := time.Now()
	heartbeat := now.Add(-10 * time.Minute)
	err := g.db.Transaction(func(tx *gorm.DB) error {
		if err := lockUpstreamGroupParentChannelTx(tx, channelID); err != nil {
			return err
		}
		if err := lockAutomaticAssignmentMutationTx(tx); err != nil {
			return err
		}
		canonical, modelFence, err := lockGatewayModelFactWriteTx(tx, channelID, groupName, now, fence, generations...)
		if err != nil {
			return err
		}
		channelID = modelFence.ChannelID
		groupName = canonical
		var capability GatewayModelCapability
		err = tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("channel_id = ? AND lower(btrim(group_name)) = lower(btrim(?)) AND model_name = ?", channelID, groupName, modelName).
			Order("last_checked_at DESC, id DESC").First(&capability).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return tx.Create(&GatewayModelCapability{
				ChannelID: channelID, GroupName: groupName, ModelName: modelName,
				UpstreamGroupID: modelFence.UpstreamGroupID, PresenceGeneration: modelFence.PresenceGeneration,
				ChannelIdentityRevision: modelFence.ChannelIdentityRevision,
				Supported:               supported, LastCheckedAt: now, LastError: lastError,
			}).Error
		}
		if err != nil {
			return err
		}
		if modelFence.MatchesCapability(&capability) && capability.Supported == supported && capability.LastError == lastError && capability.LastCheckedAt.After(heartbeat) {
			return nil
		}
		return tx.Model(&capability).Updates(map[string]any{
			"supported": supported, "last_checked_at": now, "last_error": lastError,
			"upstream_group_id": modelFence.UpstreamGroupID, "presence_generation": modelFence.PresenceGeneration,
			"channel_identity_revision": modelFence.ChannelIdentityRevision,
		}).Error
	})
	if err == nil {
		err = g.refreshUpstreamGroupFactsFromKey(channelID, groupName, now)
	}
	return err
}

func (g *Gateway) MarkModelCapabilityForApp(channelID uint, groupName, modelName string, app GatewayApp, supported bool, lastError string, generations ...UpstreamGroupGeneration) error {
	if err := g.MarkModelCapability(channelID, groupName, modelName, supported, lastError, generations...); err != nil {
		return err
	}
	if !supported || strings.TrimSpace(lastError) != "" {
		return nil
	}
	return g.markGatewayAppProbed(channelID, groupName, app, time.Now(), generations...)
}

func (g *Gateway) MarkModelCapabilityForAppForIdentity(fence GatewayModelFactFence, modelName string, app GatewayApp, supported bool, lastError string) error {
	if err := g.MarkModelCapabilityForIdentity(fence, modelName, supported, lastError); err != nil {
		return err
	}
	if !supported || strings.TrimSpace(lastError) != "" {
		return nil
	}
	return g.markGatewayAppProbedForIdentity(fence, app, time.Now())
}

func (g *Gateway) markGatewayAppProbed(channelID uint, groupName string, app GatewayApp, at time.Time, generations ...UpstreamGroupGeneration) error {
	return g.markGatewayAppProbedInternal(channelID, groupName, app, at, nil, generations...)
}

func (g *Gateway) markGatewayAppProbedForIdentity(fence GatewayModelFactFence, app GatewayApp, at time.Time) error {
	if !fence.Valid() {
		return ErrUpstreamGroupGenerationChanged
	}
	return g.markGatewayAppProbedInternal(fence.ChannelID, fence.CanonicalGroupName, app, at, &fence)
}

func (g *Gateway) markGatewayAppProbedInternal(channelID uint, groupName string, app GatewayApp, at time.Time, fence *GatewayModelFactFence, generations ...UpstreamGroupGeneration) error {
	appName := strings.ToLower(strings.TrimSpace(string(app)))
	if !stringIn(appName, "codex", "claude", "gemini") {
		return fmt.Errorf("invalid probed gateway app %q", appName)
	}
	canonical, err := canonicalUpstreamGroupName(g.db, groupName)
	if err != nil {
		return err
	}
	if at.IsZero() {
		at = time.Now()
	}
	err = g.db.Transaction(func(tx *gorm.DB) error {
		if err := lockUpstreamGroupParentChannelTx(tx, channelID); err != nil {
			return err
		}
		if err := lockAutomaticAssignmentMutationTx(tx); err != nil {
			return err
		}
		var modelFence GatewayModelFactFence
		canonical, modelFence, err = lockGatewayModelFactWriteTx(tx, channelID, canonical, at, fence, generations...)
		if err != nil {
			return err
		}
		var group GatewayUpstreamGroup
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("channel_id = ? AND canonical_group_name = ?", channelID, canonical).
			First(&group).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		if !group.Present || at.Before(group.DiscoveredAt) {
			return nil
		}
		hasCurrentRate, err := upstreamGroupHasCurrentRateTx(tx, channelID, groupName)
		if err != nil {
			return err
		}
		if !hasCurrentRate {
			return nil
		}
		var key GatewayUpstreamKey
		if err := tx.Where("channel_id = ? AND lower(btrim(group_name)) = ?", channelID, canonical).Order("id ASC").First(&key).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		if key.ModelsSyncedAt == nil || !modelFence.MatchesUpstreamKey(&key) {
			return nil
		}
		probed := normalizeCodes(append(group.ProbedGatewayApps, appName))
		if err := tx.Model(&group).Update("probed_gateway_apps", gatewayJSONB(probed)).Error; err != nil {
			return err
		}
		_, err = rebuildUpstreamGroupFactsIfChannelExistsTx(tx, channelID, canonical, strings.Split(key.ModelIDs, ","), *key.ModelsSyncedAt, at, key.ModelsError, modelFence)
		return err
	})
	if err == nil {
		g.ReevaluateUpstreamGroupAutomaticAssignment(channelID, groupName, true)
	}
	return err
}

func (g *Gateway) refreshUpstreamGroupFactsFromKey(channelID uint, groupName string, at time.Time) error {
	err := g.db.Transaction(func(tx *gorm.DB) error {
		if err := lockUpstreamGroupParentChannelTx(tx, channelID); err != nil {
			return err
		}
		if err := lockAutomaticAssignmentMutationTx(tx); err != nil {
			return err
		}
		canonical, err := lockUpstreamRoutingHealthDomainAdvisoryTx(tx, channelID, groupName)
		if err != nil {
			return err
		}
		var key GatewayUpstreamKey
		if err := tx.Where("channel_id = ? AND lower(btrim(group_name)) = ?", channelID, canonical).
			Order(upstreamKeyRoutePreferenceOrder).First(&key).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		if key.ModelsSyncedAt == nil {
			return nil
		}
		_, err = rebuildUpstreamGroupFactsIfChannelExistsTx(tx, channelID, canonical, strings.Split(key.ModelIDs, ","), *key.ModelsSyncedAt, at, key.ModelsError)
		return err
	})
	if err == nil {
		g.ReevaluateUpstreamGroupAutomaticAssignment(channelID, groupName, true)
	}
	return err
}
