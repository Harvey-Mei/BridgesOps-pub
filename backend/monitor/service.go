// Package monitor 周期性扫描渠道，采集余额 / 倍率并写入快照、变化日志和通知。
package monitor

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/channel"
	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	"github.com/Harvey-Mei/BridgeOps/backend/costing"
	"github.com/Harvey-Mei/BridgeOps/backend/notify"
	"github.com/Harvey-Mei/BridgeOps/backend/progress"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"gorm.io/gorm"
)

const (
	rateSuccessLogInterval         = 15 * time.Minute
	announcementSyncInterval       = 15 * time.Minute
	channelIdentityDispatchTimeout = 30 * time.Second
	managedKeyMaintenanceTimeout   = 8 * time.Second
)

// Service 监控扫描服务。
type Service struct {
	channels                *storage.Channels
	announcements           *storage.UpstreamAnnouncements
	rates                   *storage.Rates
	monitorLogs             *storage.MonitorLogs
	channelSvc              *channel.Service
	dispatcher              *notify.Dispatcher
	log                     *slog.Logger
	gatewaySync             gatewaySynchronizer
	costing                 *costing.Service
	refreshMu               sync.Mutex
	refreshLocks            map[uint]chan struct{}
	cadenceMu               sync.Mutex
	rateSuccessAt           map[uint]time.Time
	announcementAt          map[channelIdentityRevisionKey]time.Time
	identityDispatchTimeout time.Duration
	managedKeyTimeout       time.Duration
	scanConcurrency         int
}

type channelIdentityRevisionKey struct {
	ChannelID uint
	Revision  uint64
}

type gatewaySynchronizer interface {
	SyncChannelModelFamilies(context.Context, uint, bool) error
	DeleteGroupRoutingState(context.Context, uint, string) error
}

type upstreamGroupFactsConsumer interface {
	ApplyUpstreamGroupFacts(context.Context, uint) error
}

type partialModelSyncError interface {
	ModelFamilySyncPartial() bool
}

func isStaleChannelAccountIdentity(err error) bool {
	return errors.Is(err, storage.ErrStaleChannelAccountIdentityRevision) ||
		errors.Is(err, storage.ErrStaleUpstreamAccountConcurrencyObservation)
}

type SessionKeepAliveResult struct {
	Total   int
	Success int
	Failed  int
}

func (s *Service) SetGatewaySync(syncer gatewaySynchronizer) {
	s.gatewaySync = syncer
}

// SetCosting enables canonical raw-to-actual money conversion. Keeping it
// optional preserves the legacy constructor contract for embedders and tests.
func (s *Service) SetCosting(service *costing.Service) {
	s.costing = service
}

// SetFundingDetectionThreshold updates the shared ledger decision point after
// a scheduler configuration has started successfully.
func (s *Service) SetFundingDetectionThreshold(value float64) {
	if s == nil || s.costing == nil {
		return
	}
	s.costing.SetFundingDetectionThreshold(value)
}

// RebaseCostUnit applies an administrator-approved provider monetary-unit
// change. The command intentionally contains no raw values; this method owns
// both trusted upstream sampling and the per-channel refresh lock.
func (s *Service) RebaseCostUnit(ctx context.Context, channelID uint, command costing.CostUnitRebaseCommand) (*costing.CostUnitRebaseResult, error) {
	if s == nil || s.channels == nil || s.costing == nil {
		return nil, errors.New("cost unit rebase is unavailable")
	}
	if replay, err := s.costing.ReplayCostUnitRebase(channelID, command); err == nil {
		return replay, nil
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}

	unlock, err := s.lockChannelRefresh(ctx, channelID)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if replay, err := s.costing.ReplayCostUnitRebase(channelID, command); err == nil {
		return replay, nil
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}

	channelRecord, err := s.channels.FindByID(channelID)
	if err != nil {
		return nil, err
	}
	resolved, conn, session, err := s.prepare(ctx, channelRecord)
	if err != nil {
		return nil, fmt.Errorf("%w: prepare channel: %v", costing.ErrCostUnitRebaseUpstream, err)
	}
	if resolved.Type != connector.TypeSub2API || resolved.RechargeMultiplier != nil {
		return nil, fmt.Errorf("%w: only an authoritative Sub2API advertised multiplier can be rebased", costing.ErrCostUnitRebaseEvidenceMismatch)
	}
	expectedIdentity, err := s.channels.CurrentUpstreamAccountIdentity(channelID)
	if err != nil {
		return nil, fmt.Errorf("%w: current authenticated upstream account is unavailable: %v", costing.ErrCostUnitRebaseEvidenceMismatch, err)
	}
	if expectedIdentity.ChannelIdentityRevision != channelRecord.UpstreamAccountIdentityRevision {
		return nil, storage.ErrStaleChannelAccountIdentityRevision
	}
	mode := connector.NormalizeRechargeMultiplierMode(resolved.RechargeMultiplierMode)
	expectedFactor := connector.RechargeCostFactor(&command.CurrentMultiplier, mode)
	priorState, err := s.costing.State(channelID)
	if err != nil {
		return nil, err
	}
	if priorState.Version != command.ExpectedVersion {
		return nil, storage.ErrCostStateConflict
	}
	if priorState.ObservedProviderFactor == nil || !finitePositiveFactor(*priorState.ObservedProviderFactor) {
		return nil, fmt.Errorf("%w: persisted previous provider factor is missing", costing.ErrCostUnitRebaseEvidenceMismatch)
	}
	rawScale := *priorState.ObservedProviderFactor / expectedFactor
	if !finitePositiveFactor(rawScale) || sameFinancialFactor(rawScale, 1) {
		return nil, fmt.Errorf("%w: provider factor transition is not evidenced", costing.ErrCostUnitRebaseEvidenceMismatch)
	}

	var selectedBalance *connector.BalanceResult
	var selectedCosts *connector.CostResult
	var selectedRates *connector.RateSnapshotResult
	var selectedRateAt time.Time
	var selectedCostAt time.Time
	for sample := 0; sample < 2; sample++ {
		balance, balanceErr := conn.GetBalance(ctx, resolved, session)
		if balanceErr != nil {
			return nil, fmt.Errorf("%w: sample balance: %v", costing.ErrCostUnitRebaseUpstream, balanceErr)
		}
		if balance == nil {
			return nil, fmt.Errorf("%w: balance sample is empty", costing.ErrCostUnitRebaseEvidenceMismatch)
		}
		if balance.AccountConcurrency == nil {
			return nil, fmt.Errorf("%w: balance sample has no authenticated account identity", costing.ErrCostUnitRebaseEvidenceMismatch)
		}
		if err := s.channelSvc.PersistAccountIdentityFact(channelRecord, resolved, balance.AccountConcurrency); err != nil {
			return nil, fmt.Errorf("%w: persist sampled account identity: %v", costing.ErrCostUnitRebaseEvidenceMismatch, err)
		}
		sampledIdentity, identityErr := s.channels.CurrentUpstreamAccountIdentity(channelID)
		if identityErr != nil {
			return nil, fmt.Errorf("%w: reload sampled account identity: %v", costing.ErrCostUnitRebaseEvidenceMismatch, identityErr)
		}
		if !sameUpstreamAccountIdentity(expectedIdentity, sampledIdentity) {
			return nil, fmt.Errorf("%w: authenticated upstream account changed during rebase sampling", costing.ErrCostUnitRebaseEvidenceMismatch)
		}
		rates, rateErr := conn.GetRates(ctx, resolved, session)
		rateAt := time.Now().UTC()
		if rateErr != nil {
			return nil, fmt.Errorf("%w: sample rates: %v", costing.ErrCostUnitRebaseUpstream, rateErr)
		}
		if err := validateRateSnapshot(rates, 1); err != nil {
			return nil, fmt.Errorf("%w: %v", costing.ErrCostUnitRebaseEvidenceMismatch, err)
		}
		costs, costErr := conn.GetCosts(ctx, resolved, session)
		costAt := time.Now()
		if costErr != nil {
			return nil, fmt.Errorf("%w: sample costs: %v", costing.ErrCostUnitRebaseUpstream, costErr)
		}
		if costs == nil {
			return nil, fmt.Errorf("%w: cost sample is empty", costing.ErrCostUnitRebaseEvidenceMismatch)
		}
		if costs.CumulativeCostUnavailable {
			return nil, fmt.Errorf("%w: upstream cumulative cost is unavailable", costing.ErrCostUnitRebaseEvidenceMismatch)
		}
		if strings.TrimSpace(balance.FactorSource) != "advertised" || strings.TrimSpace(costs.FactorSource) != "advertised" ||
			!sameFinancialFactor(balance.AppliedFactor, expectedFactor) || !sameFinancialFactor(costs.AppliedFactor, expectedFactor) {
			return nil, fmt.Errorf("%w: provider did not confirm the current advertised multiplier", costing.ErrCostUnitRebaseEvidenceMismatch)
		}
		if selectedCosts != nil && costs.RawTotalCost < selectedCosts.RawTotalCost {
			return nil, fmt.Errorf("%w: repeated cumulative cost samples moved backwards", costing.ErrCostUnitRebaseEvidenceMismatch)
		}
		if selectedRates != nil && !sameCostUnitRateSnapshot(selectedRates, rates) {
			return nil, fmt.Errorf("%w: repeated rate snapshots changed during sampling", costing.ErrCostUnitRebaseEvidenceMismatch)
		}
		selectedBalance, selectedCosts, selectedRates, selectedRateAt, selectedCostAt = balance, costs, rates, rateAt, costAt
	}

	balanceAt := selectedBalance.SampledAt
	if balanceAt.IsZero() {
		balanceAt = time.Now()
	}
	rateEvidence, err := s.costUnitRateEvidence(channelID, rawScale, selectedRates, priorState.CostSampledAt, selectedRateAt, selectedCostAt)
	if err != nil {
		return nil, err
	}
	result, err := s.costing.RebaseCostUnit(command, costing.CostUnitRebaseObservation{
		ChannelID: channelID, ExpectedIdentityRevision: expectedIdentity.ChannelIdentityRevision,
		ExpectedAccountRecordID:    expectedIdentity.AccountRecordID,
		ExpectedAccountFingerprint: expectedIdentity.Fingerprint, MultiplierMode: mode,
		ObservedFactor: selectedBalance.AppliedFactor, FactorSource: selectedBalance.FactorSource,
		RawBalance: selectedBalance.RawBalance, RawTodayCost: selectedCosts.RawTodayCost,
		RawTotalCost: selectedCosts.RawTotalCost, BalanceSampledAt: balanceAt, CostSampledAt: selectedCostAt,
		RateEvidence: rateEvidence,
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func NewService(
	channels *storage.Channels,
	announcements *storage.UpstreamAnnouncements,
	rates *storage.Rates,
	monitorLogs *storage.MonitorLogs,
	channelSvc *channel.Service,
	dispatcher *notify.Dispatcher,
	log *slog.Logger,
) *Service {
	return &Service{
		channels:                channels,
		announcements:           announcements,
		rates:                   rates,
		monitorLogs:             monitorLogs,
		channelSvc:              channelSvc,
		dispatcher:              dispatcher,
		log:                     log,
		refreshLocks:            make(map[uint]chan struct{}),
		rateSuccessAt:           make(map[uint]time.Time),
		announcementAt:          make(map[channelIdentityRevisionKey]time.Time),
		identityDispatchTimeout: channelIdentityDispatchTimeout,
		managedKeyTimeout:       managedKeyMaintenanceTimeout,
		scanConcurrency:         1,
	}
}

// SetScanConcurrency bounds concurrent upstream collection within one
// scheduler pass. The bound keeps network work responsive without allowing an
// unbounded burst of database writes when many channels are configured.
func (s *Service) SetScanConcurrency(value int) {
	if s == nil {
		return
	}
	if value < 1 {
		value = 1
	}
	if value > 8 {
		value = 8
	}
	s.scanConcurrency = value
}

// ScanAllBalances 扫描所有启用监控的渠道余额。单个失败不影响其他。
func (s *Service) ScanAllBalances(ctx context.Context) {
	list, err := s.channels.ListMonitorEnabled()
	if err != nil {
		s.log.Error("list channels", "err", err)
		return
	}
	s.scanChannels(ctx, list, func(c *storage.Channel) {
		if err := s.RefreshBalance(ctx, c); err != nil {
			s.log.Warn("refresh balance failed", "channel", c.Name, "err", err)
			// An accounting gate only refuses to move the money ledger; the
			// upstream session is still usable. Subscription quota and expiry
			// alerts carry no cost basis, so suppressing them here would let one
			// unverifiable financial fact silently disable an unrelated alarm.
			if !isCostUnitAccountingGate(err) {
				return
			}
		}
		if err := s.CheckSubscriptionUsageAlerts(ctx, c); err != nil {
			s.log.Warn("check subscription usage failed", "channel", c.Name, "err", err)
		}
	})
}

// ScanAllRates 扫描所有启用监控的渠道倍率。
func (s *Service) ScanAllRates(ctx context.Context) {
	list, err := s.channels.ListHealthyMonitorEnabled()
	if err != nil {
		s.log.Error("list channels", "err", err)
		return
	}
	s.scanChannels(ctx, list, func(c *storage.Channel) {
		if err := s.RefreshRates(ctx, c); err != nil {
			s.log.Warn("refresh rates failed", "channel", c.Name, "err", err)
		}
	})
}

// RecoverDueRateSyncs retries only channels with a visible failure state.
// The cadence is persisted by Channels, so this loop remains correct across
// process restarts and never lets the normal one-minute rate scan bypass it.
func (s *Service) RecoverDueRateSyncs(ctx context.Context) {
	list, err := s.channels.ListDueSyncRetries(time.Now())
	if err != nil {
		s.log.Error("list due channel sync retries", "err", err)
		return
	}
	// Use the same bounded worker pool as normal scans. RefreshRates also owns
	// a per-channel lock, so recovering a failed channel cannot serialize or
	// block unrelated healthy-channel monitoring.
	s.scanChannels(ctx, list, func(c *storage.Channel) {
		if err := s.RefreshRates(ctx, c); err != nil {
			s.log.Warn("recover channel rate sync failed", "channel", c.Name, "err", err)
		}
	})
}

func (s *Service) scanChannels(ctx context.Context, list []storage.Channel, visit func(*storage.Channel)) {
	if len(list) == 0 {
		return
	}
	workers := s.scanConcurrency
	if workers < 1 {
		workers = 1
	}
	if workers > len(list) {
		workers = len(list)
	}

	jobs := make(chan storage.Channel)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for channel := range jobs {
				if ctx.Err() != nil {
					return
				}
				visit(&channel)
			}
		}()
	}

enqueue:
	for i := range list {
		select {
		case jobs <- list[i]:
		case <-ctx.Done():
			break enqueue
		}
	}
	close(jobs)
	wg.Wait()
}

// KeepAliveSessions 定期校验所有渠道登录态，并在需要时刷新 token 或重新登录。
// 这不是余额/倍率监控：即使渠道关闭了 monitor_enabled，也会尽量保持 API 网关可用。
func (s *Service) KeepAliveSessions(ctx context.Context) SessionKeepAliveResult {
	result := SessionKeepAliveResult{}
	if s == nil || s.channels == nil || s.channelSvc == nil {
		return result
	}
	list, err := s.channels.List()
	if err != nil {
		if s.log != nil {
			s.log.Error("list channels for session keepalive", "err", err)
		}
		return result
	}
	result.Total = len(list)
	for i := range list {
		if ctx.Err() != nil {
			return result
		}
		c := list[i]
		if c.DeletionPending || c.CredentialMode == storage.CredentialModeDirect {
			continue
		}
		started := time.Now()
		err := s.keepAliveSession(ctx, &c)
		finished := time.Now()
		if s.monitorLogs != nil {
			_ = s.monitorLogs.Append(&storage.MonitorLog{
				ChannelID:    c.ID,
				Job:          storage.MonitorJobLogin,
				Success:      err == nil,
				ErrorMessage: errString(err),
				StartedAt:    started,
				FinishedAt:   finished,
			})
		}
		if err != nil {
			if isStaleChannelAccountIdentity(err) {
				continue
			}
			result.Failed++
			if connector.IsAccountBalanceHTTPError(err) {
				_ = s.channels.MarkUpstreamAccountBalanceInsufficient(c.ID, c.UpstreamAccountIdentityRevision, finished)
				if s.log != nil {
					s.log.Warn("channel session keepalive blocked by upstream account balance", "channel", c.Name)
				}
				continue
			}
			_ = s.channels.SetLastErrorForIdentity(c.ID, c.UpstreamAccountIdentityRevision, err.Error())
			if s.log != nil {
				s.log.Warn("channel session keepalive failed", "channel", c.Name, "err", err)
			}
			continue
		}
		if clearErr := s.channels.SetLastErrorForIdentity(c.ID, c.UpstreamAccountIdentityRevision, ""); clearErr != nil {
			if !isStaleChannelAccountIdentity(clearErr) && s.log != nil {
				s.log.Warn("clear channel session keepalive error", "channel", c.Name, "err", clearErr)
			}
			continue
		}
		result.Success++
	}
	return result
}

func (s *Service) keepAliveSession(ctx context.Context, c *storage.Channel) error {
	if c != nil && c.CredentialMode == storage.CredentialModeDirect {
		return nil
	}
	resolved, err := s.channelSvc.Resolve(ctx, c)
	if err != nil {
		return err
	}
	conn, err := connector.For(resolved.Type)
	if err != nil {
		return err
	}
	s.channelSvc.ApplyHTTPConfig(conn)
	if err := s.channelSvc.ApplyProxy(conn, resolved); err != nil {
		return err
	}
	_, err = s.channelSvc.EnsureSession(ctx, c, resolved, conn)
	return err
}

// RefreshBalance 单个渠道余额刷新，可被 API 手动触发。
func (s *Service) RefreshBalance(ctx context.Context, c *storage.Channel) (err error) {
	unlock, lockErr := s.lockChannelRefresh(ctx, c.ID)
	if lockErr != nil {
		return lockErr
	}
	defer unlock()
	if c.CredentialMode == storage.CredentialModeDirect {
		progress.OK(ctx, progress.StageBalance, "直连 API 渠道无需采集余额")
		return nil
	}
	defer func() {
		if c == nil {
			return
		}
		if err != nil {
			if isCostUnitAccountingGate(err) {
				_ = s.channels.ClearSyncFailureForIdentity(c.ID, c.UpstreamAccountIdentityRevision)
				return
			}
			if isStaleChannelAccountIdentity(err) {
				return
			}
			if !errors.Is(err, context.Canceled) {
				if connector.IsAccountBalanceHTTPError(err) {
					_ = s.channels.MarkUpstreamAccountBalanceInsufficient(c.ID, c.UpstreamAccountIdentityRevision, time.Now().UTC())
				}
				if persistErr := s.channels.SetBalanceError(c.ID, c.UpstreamAccountIdentityRevision, err.Error(), time.Now().UTC()); persistErr != nil && !errors.Is(persistErr, storage.ErrStaleChannelAccountIdentityRevision) {
					err = errors.Join(err, fmt.Errorf("persist balance refresh error: %w", persistErr))
				}
			}
			return
		}
	}()

	return s.refreshBalance(ctx, c)
}

func (s *Service) refreshBalance(ctx context.Context, c *storage.Channel) error {
	if c != nil && c.CredentialMode == storage.CredentialModeDirect {
		return nil
	}
	resolved, conn, session, err := s.prepare(ctx, c)
	if err != nil {
		if !isStaleChannelAccountIdentity(err) {
			s.notifyError(ctx, c, storage.EventLoginFailed, "登录失败", err)
		}
		return err
	}
	var expectedCostState *storage.ChannelCostState
	var expectedAccountIdentity *storage.UpstreamAccountIdentitySnapshot
	freezeStarted := time.Now()
	if s.costing != nil {
		expectedCostState, err = s.costing.State(c.ID)
		if err != nil {
			return s.failBalanceSampling(ctx, c.ID, freezeStarted,
				fmt.Errorf("freeze channel cost state before financial sampling: %w", err))
		}
		if costing.CanRecoverConfirmedFundingLegacyReview(expectedCostState) {
			expectedCostState, err = s.costing.RecoverConfirmedFundingLegacyReview(
				c.ID, expectedCostState.Version, c.UpstreamAccountIdentityRevision,
			)
			if err != nil {
				return s.failBalanceSampling(ctx, c.ID, freezeStarted,
					fmt.Errorf("recover confirmed funding legacy review: %w", err))
			}
		}
	}
	// frozenIdentityRevision is read before any binding write, because
	// PersistAccountIdentityFact rewrites c.UpstreamAccountIdentityRevision in
	// place when it advances the revision.
	frozenIdentityRevision := c.UpstreamAccountIdentityRevision
	if c.Type == storage.ChannelTypeSub2API {
		expectedAccountIdentity, err = s.channels.CurrentUpstreamAccountIdentity(c.ID)
		if err != nil && !errors.Is(err, storage.ErrUpstreamAccountIdentityUnbound) {
			return s.failBalanceSampling(ctx, c.ID, freezeStarted,
				fmt.Errorf("freeze authenticated upstream account before financial sampling: %w", err))
		}
		// An unbound channel is the normal state of every newly created Sub2API
		// channel: the first successful sample is what creates the binding.
		// Refusing here made the very first refresh of such a channel
		// impossible, which is a bootstrap deadlock, not a fence.
		err = nil
	}
	progress.Start(ctx, progress.StageBalance, "拉取余额…")
	compensated := false
	started := time.Now()
	res, err := conn.GetBalance(ctx, resolved, session)
	if err == nil && res == nil {
		err = errors.New("余额采集返回空结果")
	}
	finished := time.Now()
	if err == nil {
		if validationErr := connector.ValidateBalanceResult(res); validationErr != nil {
			return s.markInvalidBalanceObservation(ctx, c, res, started, finished, validationErr)
		}
	}
	_ = s.monitorLogs.Append(&storage.MonitorLog{
		ChannelID:    c.ID,
		Job:          storage.MonitorJobBalance,
		Success:      err == nil,
		ErrorMessage: errString(err),
		StartedAt:    started,
		FinishedAt:   finished,
	})
	if err != nil {
		initialErr := err
		var used bool
		session, used, err = s.compensateCollectionFailure(ctx, c, resolved, conn, session, err)
		compensated = used
		if err == nil {
			retryStarted := time.Now()
			retryRes, retryErr := conn.GetBalance(ctx, resolved, session)
			if retryErr == nil && retryRes == nil {
				retryErr = errors.New("余额采集返回空结果")
			}
			retryFinished := time.Now()
			if retryErr == nil {
				if validationErr := connector.ValidateBalanceResult(retryRes); validationErr != nil {
					return errors.Join(initialErr, s.markInvalidBalanceObservation(ctx, c, retryRes, retryStarted, retryFinished, validationErr))
				}
			}
			_ = s.monitorLogs.Append(&storage.MonitorLog{
				ChannelID:    c.ID,
				Job:          storage.MonitorJobBalance,
				Success:      retryErr == nil,
				ErrorMessage: errString(retryErr),
				StartedAt:    retryStarted,
				FinishedAt:   retryFinished,
			})
			if retryErr == nil {
				res = retryRes
			} else {
				err = errors.Join(initialErr, fmt.Errorf("补偿同步后余额采集失败：%w", retryErr))
			}
		}
		if err != nil {
			progress.Fail(ctx, progress.StageBalance, err.Error())
			s.notifyError(ctx, c, storage.EventMonitorFailed, "余额采集失败", err)
			return err
		}
	}
	if c.Type == storage.ChannelTypeSub2API {
		identityStarted := time.Now()
		if res.AccountConcurrency == nil {
			return s.failBalanceSampling(ctx, c.ID, identityStarted,
				fmt.Errorf("%w: balance sample has no authenticated account identity", costing.ErrCostUnitEvidenceUnavailable))
		}
		if err := s.channelSvc.PersistAccountIdentityFact(c, resolved, res.AccountConcurrency); err != nil {
			return s.failBalanceSampling(ctx, c.ID, identityStarted,
				fmt.Errorf("persist balance account identity: %w", err))
		}
		currentIdentity, identityErr := s.channels.CurrentUpstreamAccountIdentity(c.ID)
		if identityErr != nil {
			return s.failBalanceSampling(ctx, c.ID, identityStarted,
				fmt.Errorf("reload balance account identity: %w", identityErr))
		}
		if expectedAccountIdentity == nil {
			// First binding for this channel. There is no prior identity to be
			// stale against, but the race is still fenced: had a concurrent
			// observation bound a different account first, our own persist
			// would have advanced the channel identity revision past the value
			// frozen before sampling.
			if currentIdentity == nil || currentIdentity.ChannelIdentityRevision != frozenIdentityRevision {
				return s.failBalanceSampling(ctx, c.ID, identityStarted, storage.ErrStaleUpstreamAccountObservation)
			}
			expectedAccountIdentity = currentIdentity
		} else if !sameUpstreamAccountIdentity(expectedAccountIdentity, currentIdentity) {
			return s.failBalanceSampling(ctx, c.ID, identityStarted, storage.ErrStaleUpstreamAccountObservation)
		}
	}

	sampledAt := res.SampledAt
	if sampledAt.IsZero() {
		sampledAt = time.Now()
	}
	var rechargeRecords []connector.RechargeRecord
	if history, ok := conn.(connector.RechargeHistory); ok {
		records, historyErr := history.GetRechargeHistory(ctx, resolved, session)
		if historyErr == nil {
			rechargeRecords = records
			if c.Type == storage.ChannelTypeNewAPI && resolved.RechargeMultiplier == nil && res.FactorSource != "manual" {
				if observedFactor, ok := consistentRechargeHistoryFactor(records); ok {
					res.AppliedFactor = observedFactor
					res.FactorSource = "recharge_history"
					res.Balance = res.RawBalance * observedFactor
				}
			}
		} else if !errors.Is(historyErr, connector.ErrUnsupported) && s.log != nil {
			s.log.Warn("load recharge history failed", "channel", c.Name, "err", historyErr)
		}
	}

	progress.Start(ctx, progress.StageCost, "拉取消费…")
	costStarted := time.Now()
	costRes, err := conn.GetCosts(ctx, resolved, session)
	costSampledAt := time.Time{}
	if costRes != nil {
		costSampledAt = costRes.SampledAt
	}
	if costSampledAt.IsZero() {
		costSampledAt = time.Now().UTC()
	}
	if err == nil && costRes == nil {
		err = errors.New("消费采集返回空结果")
	}
	if err != nil {
		initialErr := err
		if !compensated {
			var used bool
			session, used, err = s.compensateCollectionFailure(ctx, c, resolved, conn, session, err)
			compensated = used
			if err == nil {
				retryRes, retryErr := conn.GetCosts(ctx, resolved, session)
				retrySampledAt := time.Time{}
				if retryRes != nil {
					retrySampledAt = retryRes.SampledAt
				}
				if retrySampledAt.IsZero() {
					retrySampledAt = time.Now().UTC()
				}
				if retryErr == nil && retryRes == nil {
					retryErr = errors.New("消费采集返回空结果")
				}
				if retryErr == nil {
					costRes = retryRes
					costSampledAt = retrySampledAt
				} else {
					err = errors.Join(initialErr, fmt.Errorf("补偿同步后消费采集失败：%w", retryErr))
				}
			}
		}
		if err != nil {
			progress.Fail(ctx, progress.StageCost, err.Error())
			s.notifyError(ctx, c, storage.EventMonitorFailed, "消费采集失败", err)
			_ = s.monitorLogs.Append(&storage.MonitorLog{
				ChannelID: c.ID, Job: storage.MonitorJobCost, Success: false,
				ErrorMessage: err.Error(), StartedAt: costStarted, FinishedAt: time.Now(),
			})
			return err
		}
	}
	if costRes.CumulativeCostUnavailable {
		historicalSampler, _ := conn.(connector.HistoricalCostSampler)
		if recoveryErr := recoverCumulativeCostFromDaily(ctx, historicalSampler, resolved, session, expectedCostState, costRes, costSampledAt, resolved.TimeZone); recoveryErr != nil {
			gateErr := fmt.Errorf("%w: %v", costing.ErrCostUnitEvidenceUnavailable, recoveryErr)
			if s.persistSafeProviderFactorEvidence(c, resolved, expectedCostState, res, costRes, costSampledAt) == nil {
				// Keep the last trusted financial anchors. A later run can retry
				// the completed-day query without losing the previous high-water
				// mark, while the newly persisted unit evidence unblocks rates.
				_ = s.monitorLogs.Append(&storage.MonitorLog{
					ChannelID: c.ID, Job: storage.MonitorJobCost, Success: false,
					ErrorMessage: gateErr.Error(), StartedAt: costStarted, FinishedAt: time.Now(),
				})
				progress.OK(ctx, progress.StageCost, "累计消费历史暂不可恢复，沿用上次可信账务基准并等待重试", map[string]any{"accounting_warning": gateErr.Error()})
				return nil
			}
			reportMonitorStageFailure(ctx, progress.StageCost, gateErr)
			_ = s.monitorLogs.Append(&storage.MonitorLog{
				ChannelID: c.ID, Job: storage.MonitorJobCost, Success: false,
				ErrorMessage: gateErr.Error(), StartedAt: costStarted, FinishedAt: time.Now(),
			})
			return gateErr
		}
	}
	if err := normalizeFinancialFactorEvidence(c.Type, res, costRes); err != nil {
		gateErr := fmt.Errorf("%w: %v", costing.ErrCostUnitEvidenceUnavailable, err)
		if s.persistSafeProviderFactorEvidence(c, resolved, expectedCostState, res, costRes, costSampledAt) == nil {
			_ = s.monitorLogs.Append(&storage.MonitorLog{
				ChannelID: c.ID, Job: storage.MonitorJobCost, Success: false,
				ErrorMessage: gateErr.Error(), StartedAt: costStarted, FinishedAt: time.Now(),
			})
			progress.OK(ctx, progress.StageCost, "充值倍率证据暂不可用，沿用上次可信账务基准并等待重试", map[string]any{"accounting_warning": gateErr.Error()})
			return nil
		}
		reportMonitorStageFailure(ctx, progress.StageCost, gateErr)
		_ = s.monitorLogs.Append(&storage.MonitorLog{
			ChannelID: c.ID, Job: storage.MonitorJobCost, Success: false,
			ErrorMessage: gateErr.Error(), StartedAt: costStarted, FinishedAt: time.Now(),
		})
		return gateErr
	}
	actualBalance := res.Balance
	rawBalance := res.RawBalance
	actualTodayCost := costRes.TodayCost
	actualTotalCost := costRes.TotalCost
	if s.costing != nil {
		state := expectedCostState
		if state == nil {
			return s.failBalanceSampling(ctx, c.ID, costStarted, errors.New("frozen channel cost state is unavailable"))
		}
		if costing.LegacyAdvertisedFactorEvidenceMissing(state) &&
			!costing.CanRecoverLegacyAdvertisedFactorEvidence(state, res.AppliedFactor, res.FactorSource) {
			return s.failBalanceSampling(ctx, c.ID, costStarted,
				fmt.Errorf("%w: persisted advertised factor evidence is incomplete; refusing to rewrite the financial basis", costing.ErrCostUnitEvidenceUnavailable))
		}
		if c.Type == storage.ChannelTypeSub2API && resolved.RechargeMultiplier == nil &&
			strings.TrimSpace(state.Source) == "advertised" &&
			(state.ObservedProviderFactor == nil || !finitePositiveFactor(*state.ObservedProviderFactor) ||
				strings.TrimSpace(state.ObservedProviderFactorSource) != "advertised" ||
				state.ObservedProviderFactorSampledAt == nil || state.ObservedProviderFactorSampledAt.IsZero()) &&
			!costing.CanRecoverAdvertisedFactorEvidence(state, res.AppliedFactor, res.FactorSource) {
			return s.failBalanceSampling(ctx, c.ID, costStarted,
				fmt.Errorf("%w: persisted advertised factor evidence is incomplete; refusing to rewrite the financial basis", costing.ErrCostUnitEvidenceUnavailable))
		}
		if c.Type == storage.ChannelTypeSub2API && resolved.RechargeMultiplier == nil &&
			strings.TrimSpace(res.FactorSource) != "advertised" {
			return s.failBalanceSampling(ctx, c.ID, costStarted,
				fmt.Errorf("%w: Sub2API 账务采样缺少 advertised 充值倍率证据，本轮同步已拒绝", costing.ErrCostUnitEvidenceUnavailable))
		}
		balanceDelta := rawBalance - state.RawBalance
		var fundingCandidate *costing.FundingCandidateInput
		if state.BalanceSampledAt != nil && len(rechargeRecords) > 0 {
			records := rechargeRecords
			sort.Slice(records, func(i, j int) bool { return records[i].CompletedAt.Before(records[j].CompletedAt) })
			var creditedTotal, paidTotal float64
			paidKnown := true
			var sourceIDs []string
			var occurredAt *time.Time
			for _, record := range records {
				if !record.CompletedAt.After(*state.BalanceSampledAt) || record.CompletedAt.After(sampledAt.Add(time.Minute)) {
					continue
				}
				if !s.costing.IsFundingCandidateAmount(record.CreditedAmount) {
					continue
				}
				creditedTotal += record.CreditedAmount
				if record.PaidAmount == nil {
					paidKnown = false
				} else {
					paidTotal += *record.PaidAmount
				}
				sourceIDs = append(sourceIDs, record.SourceID)
				if occurredAt == nil || record.CompletedAt.Before(*occurredAt) {
					value := record.CompletedAt
					occurredAt = &value
				}
			}
			changesFactor := !paidKnown || math.Abs(paidTotal-creditedTotal) > 1e-9 || math.Abs(state.FundingFactor-1) > 1e-9
			if creditedTotal > 0 && changesFactor {
				source := "newapi_topup"
				if c.Type == storage.ChannelTypeSub2API {
					source = "sub2api_payment"
				}
				digest := sha256.Sum256([]byte(strings.Join(sourceIDs, "\x00")))
				fundingCandidate = &costing.FundingCandidateInput{
					ChannelID: c.ID, Source: source, ExternalID: fmt.Sprintf("batch:%x", digest[:12]),
					CreditedAmount: creditedTotal, RawBalanceBefore: state.RawBalance, RawBalanceDetected: rawBalance,
					FundingFactorBefore: state.FundingFactor, RawTodayCostBefore: state.RawTodayCost,
					RawTotalCostBefore: state.RawTotalCost, OccurredAt: occurredAt,
					DetectedAt: sampledAt, Note: fmt.Sprintf("上游充值订单 %d 笔，待确认实际付款信息", len(sourceIDs)),
				}
				if paidKnown {
					fundingCandidate.PaidAmount = &paidTotal
				}
			}
		}
		observedNonUnit := res.AppliedFactor > 0 && !math.IsNaN(res.AppliedFactor) && !math.IsInf(res.AppliedFactor, 0) && math.Abs(res.AppliedFactor-1) > 1e-9
		stateNonUnit := state.FundingFactor >= 0 && !math.IsNaN(state.FundingFactor) && !math.IsInf(state.FundingFactor, 0) && math.Abs(state.FundingFactor-1) > 1e-9
		if fundingCandidate == nil && state.BalanceSampledAt != nil && s.costing.IsFundingCandidateAmount(balanceDelta) && (observedNonUnit || stateNonUnit) {
			fundingCandidate = &costing.FundingCandidateInput{
				ChannelID: c.ID, Source: "balance_increase",
				ExternalID:          fmt.Sprintf("balance:%.9g:%.9g", state.RawBalance, rawBalance),
				CreditedAmount:      balanceDelta,
				RawBalanceBefore:    state.RawBalance,
				RawBalanceDetected:  rawBalance,
				FundingFactorBefore: state.FundingFactor,
				RawTodayCostBefore:  state.RawTodayCost, RawTotalCostBefore: state.RawTotalCost,
				DetectedAt: sampledAt,
				Note:       "上游余额上涨，待核对实际付款金额",
			}
		}
		balanceInput := costing.BalanceInput{
			ChannelID: c.ID, ExpectedVersion: state.Version, ExpectedIdentityRevision: frozenIdentityRevision,
			RawBalance: rawBalance, ObservedFactor: res.AppliedFactor,
			SampledAt: sampledAt, Source: res.FactorSource,
		}
		if expectedAccountIdentity != nil {
			balanceInput.ExpectedIdentityRevision = expectedAccountIdentity.ChannelIdentityRevision
			balanceInput.ExpectedAccountRecordID = expectedAccountIdentity.AccountRecordID
		}
		state, _, stateErr := s.costing.RecordObservation(costing.ObservationInput{
			Balance: balanceInput,
			Costs: costing.CostInput{
				ChannelID: c.ID, ExpectedVersion: state.Version, ExpectedIdentityRevision: frozenIdentityRevision,
				RawTodayCost: costRes.RawTodayCost, RawTotalCost: costRes.RawTotalCost,
				SampledAt: costSampledAt,
			},
			Funding: fundingCandidate,
		})
		if stateErr != nil {
			err = fmt.Errorf("record channel financial observation: %w", stateErr)
			reportMonitorStageFailure(ctx, progress.StageCost, err)
			_ = s.monitorLogs.Append(&storage.MonitorLog{
				ChannelID: c.ID, Job: storage.MonitorJobCost, Success: false,
				ErrorMessage: err.Error(), StartedAt: costStarted, FinishedAt: time.Now(),
			})
			return err
		}
		actualBalance = state.ActualInventory
		actualTodayCost = state.ActualTodayCost
		actualTotalCost = state.ActualTotalCost
	} else if err := s.channels.UpdateFinancialObservationForIdentity(c.ID, frozenIdentityRevision, actualBalance, &sampledAt, actualTodayCost, actualTotalCost, costSampledAt); err != nil {
		reportMonitorStageFailure(ctx, progress.StageCost, err)
		return err
	}
	if s.costing == nil {
		if err := s.rates.AppendBalance(&storage.BalanceSnapshot{
			ChannelID: c.ID, Balance: actualBalance, CostUnitEpoch: c.CostUnitEpoch, SampledAt: sampledAt,
		}); err != nil {
			return fmt.Errorf("append legacy balance snapshot: %w", err)
		}
		if err := s.rates.AppendCost(&storage.CostSnapshot{
			ChannelID: c.ID, TodayCost: actualTodayCost, TotalCost: actualTotalCost,
			CostUnitEpoch: c.CostUnitEpoch, SampledAt: costSampledAt,
		}); err != nil {
			return fmt.Errorf("append legacy cost snapshot: %w", err)
		}
	}
	progress.OK(ctx, progress.StageBalance, fmt.Sprintf("当前余额 %.4f", actualBalance),
		map[string]any{"balance": actualBalance, "raw_balance": rawBalance})
	_ = s.monitorLogs.Append(&storage.MonitorLog{
		ChannelID: c.ID, Job: storage.MonitorJobCost, Success: true,
		StartedAt: costStarted, FinishedAt: time.Now(),
	})
	progress.OK(ctx, progress.StageCost, fmt.Sprintf("今日 %0.4f / 累计 %0.4f", actualTodayCost, actualTotalCost),
		map[string]any{"today_cost": actualTodayCost, "total_cost": actualTotalCost, "raw_today_cost": costRes.RawTodayCost})

	balanceThreshold := c.EffectiveBalanceThreshold()
	if actualBalance < balanceThreshold {
		body := fmt.Sprintf("当前余额: %.4f，阈值: %.4f；该渠道全部分组已暂停路由，余额恢复后自动启用", actualBalance, balanceThreshold)
		if s.dispatcher != nil {
			err := s.dispatchForChannelIdentity(ctx, c, notify.Message{
				Event:     storage.EventBalanceLow,
				ChannelID: c.ID,
				Subject:   fmt.Sprintf("%s 余额低于阈值", c.Name),
				Body:      body,
			})
			if isStaleChannelAccountIdentity(err) {
				return err
			}
		}
	}
	return nil
}

func recoverCumulativeCostFromDaily(ctx context.Context, historicalSampler connector.HistoricalCostSampler, channel *connector.Channel, session *connector.AuthSession, state *storage.ChannelCostState, result *connector.CostResult, sampledAt time.Time, timeZone string) error {
	if result == nil || !result.CumulativeCostUnavailable || state == nil || state.CostSampledAt == nil {
		return nil
	}
	if result.AppliedFactor <= 0 || math.IsNaN(result.AppliedFactor) || math.IsInf(result.AppliedFactor, 0) {
		return errors.New("cannot recover cumulative cost without a valid provider factor")
	}
	if result.RawTodayCost < 0 || math.IsNaN(result.RawTodayCost) || math.IsInf(result.RawTodayCost, 0) {
		return errors.New("cannot recover cumulative cost from an invalid daily sample")
	}
	location := time.UTC
	if timeZone = strings.TrimSpace(timeZone); timeZone != "" {
		var err error
		location, err = time.LoadLocation(timeZone)
		if err != nil {
			return fmt.Errorf("cannot recover cumulative cost with invalid timezone %q: %w", timeZone, err)
		}
	}
	previous := state.CostSampledAt.In(location)
	current := sampledAt.In(location)
	previousDay := time.Date(previous.Year(), previous.Month(), previous.Day(), 0, 0, 0, 0, location)
	currentDay := time.Date(current.Year(), current.Month(), current.Day(), 0, 0, 0, 0, location)
	if currentDay.Before(previousDay) {
		return errors.New("cannot recover cumulative cost from an older daily sample")
	}
	sameDay := currentDay.Equal(previousDay)
	delta := result.RawTodayCost
	if sameDay {
		delta -= state.RawTodayCost
		if delta < 0 {
			result.RawTodayCost = state.RawTodayCost
			result.TodayCost = state.ActualTodayCost
			delta = 0
		}
	} else {
		if historicalSampler == nil {
			return errors.New("cannot recover cumulative cost across calendar days without historical cost support")
		}
		historical, err := historicalSampler.GetHistoricalCosts(ctx, channel, session, previousDay, currentDay)
		if err != nil {
			return fmt.Errorf("cannot recover cumulative cost from completed calendar days: %w", err)
		}
		if historical == nil || historical.RawCost < 0 || math.IsNaN(historical.RawCost) || math.IsInf(historical.RawCost, 0) {
			return errors.New("cannot recover cumulative cost from invalid historical cost evidence")
		}
		tolerance := math.Max(1e-9, math.Max(math.Abs(historical.RawCost), math.Abs(state.RawTodayCost))*1e-9)
		if historical.RawCost+tolerance < state.RawTodayCost {
			return errors.New("cannot recover cumulative cost because completed-day history is below the previous daily high-water mark")
		}
		completedDelta := historical.RawCost - state.RawTodayCost
		if completedDelta < 0 {
			completedDelta = 0
		}
		delta += completedDelta
	}
	result.RawTotalCost = state.RawTotalCost + delta
	result.TotalCost = state.ActualTotalCost + delta*result.AppliedFactor
	return nil
}

func (s *Service) markInvalidBalanceObservation(ctx context.Context, c *storage.Channel, result *connector.BalanceResult, started, finished time.Time, validationErr error) error {
	observationErr := fmt.Errorf("余额观测无效：%w", validationErr)
	_ = s.monitorLogs.Append(&storage.MonitorLog{
		ChannelID:    c.ID,
		Job:          storage.MonitorJobBalance,
		Success:      false,
		ErrorMessage: observationErr.Error(),
		StartedAt:    started,
		FinishedAt:   finished,
	})
	sampledAt := result.SampledAt
	if sampledAt.IsZero() {
		sampledAt = finished
	}
	persistErr := s.channels.MarkBalanceUnavailable(c.ID, c.UpstreamAccountIdentityRevision, sampledAt, observationErr.Error())
	if persistErr != nil {
		if isStaleChannelAccountIdentity(persistErr) {
			return persistErr
		}
		observationErr = errors.Join(observationErr, fmt.Errorf("保存不可用余额状态失败：%w", persistErr))
	}
	if err := s.channels.RequireUpstreamAccountIdentityRevision(c.ID, c.UpstreamAccountIdentityRevision); err != nil {
		return err
	}
	progress.Fail(ctx, progress.StageBalance, observationErr.Error())
	s.notifyError(ctx, c, storage.EventMonitorFailed, "余额采集失败", observationErr)
	return observationErr
}

// RefreshRates 单个渠道倍率刷新，可被 API 手动触发。
func (s *Service) RefreshRates(ctx context.Context, c *storage.Channel) (err error) {
	unlock, lockErr := s.lockChannelRefresh(ctx, c.ID)
	if lockErr != nil {
		return lockErr
	}
	defer unlock()
	defer func() {
		if err != nil {
			if isCostUnitAccountingGate(err) {
				_ = s.channels.ClearSyncFailureForIdentity(c.ID, c.UpstreamAccountIdentityRevision)
				return
			}
			if isStaleChannelAccountIdentity(err) {
				return
			}
			if !errors.Is(err, context.Canceled) {
				if connector.IsAccountBalanceHTTPError(err) {
					_ = s.channels.MarkUpstreamAccountBalanceInsufficient(c.ID, c.UpstreamAccountIdentityRevision, time.Now().UTC())
					return
				}
				if delay, ok := connector.TransientRetryDelay(err); ok {
					_ = s.channels.RecordSyncRetryFailureAfterForIdentity(c.ID, c.UpstreamAccountIdentityRevision, err.Error(), delay)
				} else {
					_ = s.channels.RecordSyncRetryFailureForIdentity(c.ID, c.UpstreamAccountIdentityRevision, err.Error())
				}
			}
			return
		}
		_ = s.channels.ClearSyncFailureForIdentity(c.ID, c.UpstreamAccountIdentityRevision)
	}()
	if c.CredentialMode == storage.CredentialModeDirect {
		return s.refreshDirectRates(ctx, c)
	}

	if s.costing != nil && c.Type == storage.ChannelTypeSub2API {
		state, stateErr := s.costing.State(c.ID)
		if stateErr != nil {
			return stateErr
		}
		if costing.CanRecoverConfirmedFundingLegacyReview(state) {
			state, stateErr = s.costing.RecoverConfirmedFundingLegacyReview(
				c.ID, state.Version, c.UpstreamAccountIdentityRevision,
			)
			if stateErr != nil {
				return stateErr
			}
		}
		if shouldBootstrapAdvertisedFactor(state) &&
			(!costing.LegacyAdvertisedFactorEvidenceMissing(state) || c.RechargeMultiplier == nil) {
			if err := s.refreshBalance(ctx, c); err != nil {
				return err
			}
		}
	}
	resolved, conn, session, err := s.prepare(ctx, c)
	if err != nil {
		if !isStaleChannelAccountIdentity(err) {
			s.notifyError(ctx, c, storage.EventLoginFailed, "登录失败", err)
		}
		return err
	}
	progress.Start(ctx, progress.StageRates, "拉取分组倍率…")
	started := time.Now()
	snapshot, expectedCostUnitEpoch, rateObservedAt, err := s.getRatesWithCostUnitGuard(ctx, c, resolved, conn, session)
	if err == nil && snapshot == nil {
		err = errors.New("倍率采集返回空结果")
	}
	finished := time.Now()
	if err != nil {
		s.appendRateMonitorLog(c.ID, started, finished, err)
		initialErr := err
		session, _, err = s.compensateCollectionFailure(ctx, c, resolved, conn, session, err)
		if err == nil {
			retryStarted := time.Now()
			retrySnapshot, retryEpoch, retryObservedAt, retryErr := s.getRatesWithCostUnitGuard(ctx, c, resolved, conn, session)
			if retryErr == nil && retrySnapshot == nil {
				retryErr = errors.New("倍率采集返回空结果")
			}
			retryFinished := time.Now()
			if retryErr == nil {
				snapshot = retrySnapshot
				expectedCostUnitEpoch = retryEpoch
				rateObservedAt = retryObservedAt
				finished = retryFinished
			} else {
				err = errors.Join(initialErr, fmt.Errorf("补偿同步后倍率采集失败：%w", retryErr))
				s.appendRateMonitorLog(c.ID, retryStarted, retryFinished, err)
			}
		}
		if err != nil {
			reportMonitorStageFailure(ctx, progress.StageRates, err)
			if !isStaleChannelAccountIdentity(err) && !isCostUnitAccountingGate(err) {
				s.notifyError(ctx, c, storage.EventMonitorFailed, "倍率采集失败", err)
			}
			return err
		}
	}
	if err := s.channels.RequireUpstreamAccountIdentityRevision(c.ID, c.UpstreamAccountIdentityRevision); err != nil {
		return err
	}

	existing, err := s.rates.ListByChannel(c.ID)
	if err != nil {
		s.appendRateMonitorLog(c.ID, started, time.Now(), err)
		return err
	}
	if err := validateRateSnapshot(snapshot, len(existing)); err != nil {
		s.appendRateMonitorLog(c.ID, started, finished, err)
		progress.Fail(ctx, progress.StageRates, err.Error())
		return err
	}
	results := snapshot.Items
	batchItems := make([]storage.RateSnapshot, 0, len(results))
	for _, item := range results {
		batchItems = append(batchItems, storage.RateSnapshot{
			ChannelID: c.ID, ModelName: strings.TrimSpace(item.ModelName), Description: item.Description,
			Ratio: item.Ratio, CompletionRatio: item.CompletionRatio,
		})
	}
	batch, err := s.rates.ReplaceChannelSnapshotForCostUnitEpoch(storage.RateSnapshotBatchInput{
		ChannelID: c.ID, ExpectedIdentityRevision: c.UpstreamAccountIdentityRevision,
		ExpectedCostUnitEpoch: expectedCostUnitEpoch,
		ObservedAt:            rateObservedAt, Items: batchItems,
	})
	if err != nil {
		s.log.Warn("rate batch commit failed", "channel", c.Name, "err", err)
		s.appendRateMonitorLog(c.ID, started, time.Now(), err)
		progress.Fail(ctx, progress.StageRates, err.Error())
		return err
	}
	if provider, ok := conn.(connector.ModelPricingProvider); ok {
		if err := s.refreshChannelModelPrices(
			ctx, c, resolved, session, provider, c.UpstreamAccountIdentityRevision, batch.CostUnitEpoch,
		); err != nil {
			return err
		}
	}
	fundingFactor := batch.FundingFactor
	isFirstSync := batch.IsFirstSync
	changes := make([]notify.RateChange, 0, len(batch.Changed))
	for _, change := range batch.Changed {
		if change.OldRatio == nil || change.OldCompletionRatio == nil || change.OldEffectiveRatio == nil || change.OldEffectiveCompletionRatio == nil {
			continue
		}
		changes = append(changes, notify.RateChange{
			GroupName: change.ModelName, OldRatio: *change.OldRatio, NewRatio: change.NewRatio,
			OldComp: *change.OldCompletionRatio, NewComp: change.NewCompletionRatio,
			FundingFactor: fundingFactor, OldEffectiveRatio: *change.OldEffectiveRatio,
			NewEffectiveRatio: change.NewEffectiveRatio,
			OldEffectiveComp:  *change.OldEffectiveCompletionRatio,
			NewEffectiveComp:  change.NewEffectiveCompletionRatio, ChangedAt: change.ChangedAt,
		})
	}
	added := make([]notify.RateChange, 0, len(batch.Added))
	if !isFirstSync {
		for _, item := range batch.Added {
			added = append(added, notify.RateChange{
				GroupName: item.ModelName, NewRatio: item.Ratio, NewComp: item.CompletionRatio,
				FundingFactor: fundingFactor, NewEffectiveRatio: item.Ratio * fundingFactor,
				NewEffectiveComp: item.CompletionRatio * fundingFactor, ChangedAt: rateObservedAt,
			})
		}
	}
	removed := make([]notify.RateChange, 0, len(batch.Removed))
	for _, snapshot := range batch.Removed {
		removed = append(removed, notify.RateChange{
			GroupName:         snapshot.ModelName,
			OldRatio:          snapshot.Ratio,
			OldComp:           snapshot.CompletionRatio,
			FundingFactor:     fundingFactor,
			OldEffectiveRatio: snapshot.Ratio * fundingFactor,
			OldEffectiveComp:  snapshot.CompletionRatio * fundingFactor,
			ChangedAt:         rateObservedAt,
		})
	}
	// 一次扫描的所有变化打包推送：去抖策略（合并 / 涨跌幅过滤）由 Dispatcher.Policy 决定。
	if len(changes) > 0 {
		err := s.dispatchWithChannelIdentity(ctx, c, func(dispatchCtx context.Context) error {
			return s.dispatcher.DispatchRateBatch(dispatchCtx, c, changes)
		})
		if isStaleChannelAccountIdentity(err) {
			return err
		}
	}
	if len(added)+len(removed) > 0 {
		err := s.dispatchWithChannelIdentity(ctx, c, func(dispatchCtx context.Context) error {
			return s.dispatcher.DispatchRateStructureBatch(dispatchCtx, c, notify.RateStructureChange{
				Added:   added,
				Removed: removed,
			})
		})
		if isStaleChannelAccountIdentity(err) {
			return err
		}
	}
	if err := s.syncAnnouncements(ctx, c, resolved, conn, session); err != nil {
		if isStaleChannelAccountIdentity(err) {
			return err
		}
		s.log.Warn("sync announcements failed", "channel", c.Name, "err", err)
	}
	ratesProgressReported := false
	if s.gatewaySync != nil {
		if consumer, ok := s.gatewaySync.(upstreamGroupFactsConsumer); ok {
			// The rate snapshot is the authoritative group fact. Managed-key
			// reconciliation consumes it here, while the independent model-sync
			// scheduler owns potentially slow per-group capability probes.
			progress.OK(ctx, progress.StageRates, fmt.Sprintf("拉到 %d 个分组", len(results)),
				map[string]any{"count": len(results)})
			ratesProgressReported = true
			progress.Start(ctx, progress.StageAPIKeys, "维护受管 API Key…")
			if consumeErr := s.applyUpstreamGroupFacts(ctx, consumer, c.ID); consumeErr != nil {
				s.log.Warn("apply upstream group facts failed", "channel", c.Name, "err", consumeErr)
				message := "受管 API Key 维护未完成，已保留状态供自动重试"
				if errors.Is(consumeErr, context.DeadlineExceeded) {
					message = "受管 API Key 维护超时，已保留状态供自动重试"
				}
				progress.Fail(ctx, progress.StageAPIKeys, message)
			} else {
				progress.OK(ctx, progress.StageAPIKeys, "受管 API Key 维护完成")
			}
		} else {
			// Preserve compatibility for embedders that have not implemented the
			// group-fact consumer yet.
			forceModelSync := strings.TrimSpace(c.LastError) != "" || isFirstSync || len(changes)+len(added)+len(removed) > 0
			if err := s.gatewaySync.SyncChannelModelFamilies(ctx, c.ID, forceModelSync); err != nil {
				s.log.Warn("sync gateway model families failed", "channel", c.Name, "err", err)
				var partial partialModelSyncError
				if !errors.As(err, &partial) || !partial.ModelFamilySyncPartial() {
					s.appendRateMonitorLog(c.ID, started, time.Now(), err)
					progress.Fail(ctx, progress.StageRates, "倍率已刷新，但模型能力同步无法完成："+err.Error())
					return err
				}
				progress.OK(ctx, progress.StageRates, fmt.Sprintf("拉到 %d 个分组；部分模型能力待自动重试", len(results)),
					map[string]any{"count": len(results), "model_sync_warning": err.Error()})
				s.appendRateMonitorLog(c.ID, started, finished, nil)
				return nil
			}
		}
	}
	s.appendRateMonitorLog(c.ID, started, finished, nil)
	if !ratesProgressReported {
		progress.OK(ctx, progress.StageRates, fmt.Sprintf("拉到 %d 个分组", len(results)),
			map[string]any{"count": len(results)})
	}
	return nil
}

// refreshChannelModelPrices is deliberately post-commit and best effort. A
// provider or payload failure must not roll back a trusted rate refresh, while
// cancellation and CAS failures stop work that is now bound to stale state.
func (s *Service) refreshChannelModelPrices(
	ctx context.Context,
	channelRecord *storage.Channel,
	resolved *connector.Channel,
	session *connector.AuthSession,
	provider connector.ModelPricingProvider,
	expectedIdentityRevision uint64,
	expectedCostUnitEpoch uint64,
) error {
	if provider == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	snapshot, err := provider.GetModelPrices(ctx, resolved, session)
	if err == nil && ctx.Err() != nil {
		err = ctx.Err()
	}
	if err == nil && (snapshot == nil || !snapshot.Complete) {
		err = errors.New("upstream model price snapshot is incomplete")
	}
	if err == nil {
		items := make([]storage.GatewayChannelModelPriceSnapshotItem, 0, len(snapshot.Items))
		for _, item := range snapshot.Items {
			items = append(items, storage.GatewayChannelModelPriceSnapshotItem{
				UpstreamGroupID: item.UpstreamGroupID, UpstreamGroupName: item.UpstreamGroupName,
				GroupRatio: item.GroupRatio, ModelName: item.ModelName,
				InputUnitPriceUSD: item.InputUnitPriceUSD, OutputUnitPriceUSD: item.OutputUnitPriceUSD,
				CacheReadUnitPriceUSD: item.CacheReadUnitPriceUSD, CacheWriteUnitPriceUSD: item.CacheWriteUnitPriceUSD,
				LongContextBasis: item.LongContextBasis, LongContextThresholdTokens: item.LongContextThresholdTokens,
				LongContextInputUnitPriceUSD:      item.LongContextInputUnitPriceUSD,
				LongContextOutputUnitPriceUSD:     item.LongContextOutputUnitPriceUSD,
				LongContextCacheReadUnitPriceUSD:  item.LongContextCacheReadUnitPriceUSD,
				LongContextCacheWriteUnitPriceUSD: item.LongContextCacheWriteUnitPriceUSD,
			})
		}
		err = s.rates.ReplaceChannelModelPriceSnapshot(ctx, storage.GatewayChannelModelPriceSnapshotInput{
			ChannelID: channelRecord.ID, ExpectedIdentityRevision: expectedIdentityRevision,
			ExpectedCostUnitEpoch: expectedCostUnitEpoch, SourceVersion: snapshot.SourceVersion,
			SourceChecksum: snapshot.SourceChecksum, ObservedAt: snapshot.ObservedAt, Items: items,
		})
	}
	if err == nil {
		return nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if isStaleChannelAccountIdentity(err) || errors.Is(err, storage.ErrStaleUpstreamAccountObservation) ||
		errors.Is(err, storage.ErrCostUnitEpochConflict) {
		return err
	}
	if s.log != nil {
		s.log.Warn("sync channel model prices failed", "channel", channelRecord.Name, "err", err)
	}
	return nil
}

// refreshDirectRates projects each enabled administrator-owned direct group
// into the ordinary rate snapshot pipeline. Direct providers expose only an
// inference API, so this path avoids login, billing and key management calls.
func (s *Service) refreshDirectRates(ctx context.Context, c *storage.Channel) error {
	progress.Start(ctx, progress.StageRates, "更新直连 API 路由…")
	started := time.Now().UTC()
	current, err := s.channels.FindByID(c.ID)
	if err != nil {
		return err
	}
	if current.UpstreamAccountIdentityRevision != c.UpstreamAccountIdentityRevision {
		return storage.ErrStaleChannelAccountIdentityRevision
	}
	if current.CostUnitEpoch == 0 {
		return storage.ErrCostUnitEpochConflict
	}
	credentials, err := s.channels.ListDirectCredentials(c.ID)
	if err != nil {
		return err
	}
	items := make([]storage.RateSnapshot, 0, len(credentials))
	modelCount := 0
	for _, credential := range credentials {
		models, modelErr := storage.ParseDirectModels(credential.Models)
		if modelErr != nil {
			progress.Fail(ctx, progress.StageRates, modelErr.Error())
			return fmt.Errorf("直连分组 %q：%w", credential.GroupName, modelErr)
		}
		modelCount += len(models)
		if !credential.Enabled {
			continue
		}
		items = append(items, storage.RateSnapshot{
			ChannelID: c.ID, ModelName: credential.GroupName, Description: "直连 API",
			Ratio: credential.Ratio, CompletionRatio: credential.CompletionRatio,
		})
	}
	_, err = s.rates.ReplaceChannelSnapshotForCostUnitEpoch(storage.RateSnapshotBatchInput{
		ChannelID:                c.ID,
		ExpectedIdentityRevision: current.UpstreamAccountIdentityRevision,
		ExpectedCostUnitEpoch:    current.CostUnitEpoch,
		ObservedAt:               started,
		Items:                    items,
		AllowCostUnitReview:      true,
	})
	if err != nil {
		s.appendRateMonitorLog(c.ID, started, time.Now().UTC(), err)
		progress.Fail(ctx, progress.StageRates, err.Error())
		return err
	}
	if s.gatewaySync != nil {
		if err := s.gatewaySync.SyncChannelModelFamilies(ctx, c.ID, true); err != nil {
			s.appendRateMonitorLog(c.ID, started, time.Now().UTC(), err)
			progress.Fail(ctx, progress.StageRates, "直连 API 路由同步失败："+err.Error())
			return err
		}
	}
	s.appendRateMonitorLog(c.ID, started, time.Now().UTC(), nil)
	progress.OK(ctx, progress.StageRates, fmt.Sprintf("已配置 %d 个直连分组、%d 个模型", len(items), modelCount), map[string]any{
		"count": modelCount, "groups": len(items),
	})
	return nil
}

func shouldBootstrapAdvertisedFactor(state *storage.ChannelCostState) bool {
	return costing.LegacyAdvertisedFactorEvidenceMissing(state) || state != nil && strings.TrimSpace(state.Source) == "default" &&
		strings.TrimSpace(state.Status) == costing.StateStatusReady &&
		state.ObservedProviderFactor == nil && strings.TrimSpace(state.ObservedProviderFactorSource) == "" &&
		state.ObservedProviderFactorSampledAt == nil && state.BalanceSampledAt == nil && state.CostSampledAt == nil &&
		state.RawBalance == 0 && state.ActualInventory == 0 && state.FundingFactor == 1 &&
		state.RawTodayCost == 0 && state.ActualTodayCost == 0 && state.RawTotalCost == 0 && state.ActualTotalCost == 0
}

func costBasisRequiresAdvertisedRateFence(state *storage.ChannelCostState) bool {
	if state == nil {
		return false
	}
	if costing.ManualCostBasisOverrideActive(state) {
		return false
	}
	if costing.LegacyAdvertisedFactorEvidenceMissing(state) {
		return true
	}
	// Funding source describes how the local basis was confirmed; it does not
	// invalidate an independently observed provider cost unit. Once advertised
	// evidence exists, every rate snapshot remains fenced to it regardless of
	// whether the funding basis came from a redeem code or another confirmation.
	if state.ObservedProviderFactor != nil || strings.TrimSpace(state.ObservedProviderFactorSource) != "" || state.ObservedProviderFactorSampledAt != nil {
		return true
	}
	switch strings.TrimSpace(state.Source) {
	case "advertised", "cost_unit_rebase":
		return true
	default:
		return false
	}
}

func (s *Service) getRatesWithCostUnitGuard(ctx context.Context, channelRecord *storage.Channel, resolved *connector.Channel, conn connector.Connector, session *connector.AuthSession) (*connector.RateSnapshotResult, uint64, time.Time, error) {
	if channelRecord == nil || channelRecord.ID == 0 || resolved == nil || conn == nil {
		return nil, 0, time.Time{}, errors.New("rate collection is unavailable")
	}
	if s == nil {
		return nil, 0, time.Time{}, errors.New("rate collection service is unavailable")
	}
	var expectedIdentity *storage.UpstreamAccountIdentitySnapshot
	if s.costing != nil && resolved.Type == connector.TypeSub2API {
		if session == nil {
			return nil, 0, time.Time{}, fmt.Errorf("%w: authenticated session is unavailable", costing.ErrCostUnitEvidenceUnavailable)
		}
		// A stored password session can be refreshed without carrying the
		// provider account fact in the local AuthSession. Re-authenticate only
		// that exceptional path so the rates request is tied to this token's
		// account before any cost state is frozen.
		if session.AccountConcurrency == nil {
			identityRevisionBeforeSessionProof := channelRecord.UpstreamAccountIdentityRevision
			if err := conn.CheckAuth(ctx, resolved, session); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || connector.IsAuthHTTPError(err) {
					return nil, 0, time.Time{}, err
				}
				return nil, 0, time.Time{}, fmt.Errorf("%w: refreshed session identity check failed: %v", costing.ErrCostUnitEvidenceUnavailable, err)
			}
			if session.AccountConcurrency == nil {
				return nil, 0, time.Time{}, fmt.Errorf("%w: authenticated session has no upstream account identity", costing.ErrCostUnitEvidenceUnavailable)
			}
			if s.channelSvc == nil {
				return nil, 0, time.Time{}, fmt.Errorf("%w: account identity persistence is unavailable", costing.ErrCostUnitEvidenceUnavailable)
			}
			if err := s.channelSvc.PersistAccountIdentityFact(channelRecord, resolved, session.AccountConcurrency); err != nil {
				if errors.Is(err, storage.ErrStaleUpstreamAccountObservation) || errors.Is(err, storage.ErrStaleChannelAccountIdentityRevision) {
					return nil, 0, time.Time{}, err
				}
				return nil, 0, time.Time{}, fmt.Errorf("%w: persist authenticated session identity: %v", costing.ErrCostUnitEvidenceUnavailable, err)
			}
			if channelRecord.UpstreamAccountIdentityRevision != identityRevisionBeforeSessionProof {
				return nil, 0, time.Time{}, fmt.Errorf("%w: refreshed session bound to a different upstream account", costing.ErrCostUnitEvidenceUnavailable)
			}
		}
		var err error
		expectedIdentity, err = s.channels.CurrentUpstreamAccountIdentity(channelRecord.ID)
		if err != nil {
			return nil, 0, time.Time{}, err
		}
		if expectedIdentity.ChannelIdentityRevision != channelRecord.UpstreamAccountIdentityRevision {
			return nil, 0, time.Time{}, storage.ErrStaleUpstreamAccountObservation
		}
	}
	var state *storage.ChannelCostState
	var err error
	expectedEpoch := channelRecord.CostUnitEpoch
	if s.costing != nil {
		state, err = s.costing.State(channelRecord.ID)
		if err != nil {
			return nil, 0, time.Time{}, err
		}
		expectedEpoch = state.CostUnitEpoch
	}
	if expectedEpoch == 0 {
		return nil, 0, time.Time{}, storage.ErrCostUnitEpochConflict
	}
	if expectedIdentity != nil {
		// The identity can change after prepare() returns but before the cost
		// state read. Refuse that window before making any upstream rate call;
		// the final epoch CAS protects the later interval.
		currentIdentity, identityErr := s.channels.CurrentUpstreamAccountIdentity(channelRecord.ID)
		if identityErr != nil {
			return nil, expectedEpoch, time.Time{}, identityErr
		}
		if !sameUpstreamAccountIdentity(expectedIdentity, currentIdentity) {
			return nil, expectedEpoch, time.Time{}, storage.ErrStaleUpstreamAccountObservation
		}
	}
	if state == nil || resolved.Type != connector.TypeSub2API || resolved.RechargeMultiplier != nil ||
		!costBasisRequiresAdvertisedRateFence(state) {
		snapshot, rateErr := conn.GetRates(ctx, resolved, session)
		return snapshot, expectedEpoch, time.Now().UTC(), rateErr
	}
	if state.ObservedProviderFactor == nil || !finitePositiveFactor(*state.ObservedProviderFactor) ||
		state.ObservedProviderFactorSampledAt == nil || state.ObservedProviderFactorSampledAt.IsZero() ||
		strings.TrimSpace(state.ObservedProviderFactorSource) != "advertised" {
		return nil, expectedEpoch, time.Time{}, fmt.Errorf("%w: persisted advertised factor evidence is incomplete", costing.ErrCostUnitEvidenceUnavailable)
	}
	factorSampler, ok := conn.(connector.ProviderFactorSampler)
	if !ok {
		return nil, expectedEpoch, time.Time{}, fmt.Errorf(
			"%w: connector cannot sample provider factor independently",
			costing.ErrCostUnitEvidenceUnavailable,
		)
	}
	expected := *state.ObservedProviderFactor
	verifyFactor := func(position string) error {
		observation, factorErr := factorSampler.GetProviderFactor(ctx, resolved, session)
		if factorErr != nil {
			if errors.Is(factorErr, context.Canceled) || errors.Is(factorErr, context.DeadlineExceeded) {
				return factorErr
			}
			wrapped := fmt.Errorf("sample provider factor %s rate snapshot: %w", position, factorErr)
			if connector.IsAuthHTTPError(factorErr) {
				return wrapped
			}
			return fmt.Errorf("%w: %v", costing.ErrCostUnitEvidenceUnavailable, wrapped)
		}
		if observation == nil || observation.SampledAt.IsZero() ||
			strings.TrimSpace(observation.FactorSource) != "advertised" ||
			!finitePositiveFactor(observation.AppliedFactor) {
			return fmt.Errorf("%w: provider factor evidence is incomplete during rate refresh", costing.ErrCostUnitEvidenceUnavailable)
		}
		// prepare() has already authenticated this exact session and persisted its
		// provider account identity. Reloading the durable binding here catches a
		// concurrent identity revision without issuing another /auth/me request;
		// the rate commit's epoch CAS closes the remaining interval.
		currentIdentity, err := s.channels.CurrentUpstreamAccountIdentity(channelRecord.ID)
		if err != nil {
			return err
		}
		if !sameUpstreamAccountIdentity(expectedIdentity, currentIdentity) {
			return storage.ErrStaleUpstreamAccountObservation
		}
		if !sameFinancialFactor(observation.AppliedFactor, expected) {
			return s.costing.FlagCostUnitReview(
				channelRecord.ID, state.Version, observation.AppliedFactor,
				expectedIdentity.ChannelIdentityRevision, expectedIdentity.AccountRecordID,
			)
		}
		return nil
	}
	if err := verifyFactor("before"); err != nil {
		return nil, expectedEpoch, time.Time{}, err
	}
	snapshot, err := conn.GetRates(ctx, resolved, session)
	observedAt := time.Now().UTC()
	if err != nil {
		return nil, expectedEpoch, observedAt, err
	}
	if err := verifyFactor("after"); err != nil {
		return nil, expectedEpoch, observedAt, err
	}
	return snapshot, expectedEpoch, observedAt, nil
}

func (s *Service) lockChannelRefresh(ctx context.Context, channelID uint) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	s.refreshMu.Lock()
	if s.refreshLocks == nil {
		s.refreshLocks = make(map[uint]chan struct{})
	}
	lock := s.refreshLocks[channelID]
	if lock == nil {
		lock = make(chan struct{}, 1)
		lock <- struct{}{}
		s.refreshLocks[channelID] = lock
	}
	s.refreshMu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-lock:
		return func() { lock <- struct{}{} }, nil
	}
}

func (s *Service) applyUpstreamGroupFacts(ctx context.Context, consumer upstreamGroupFactsConsumer, channelID uint) error {
	if consumer == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	timeout := s.managedKeyTimeout
	if timeout <= 0 {
		timeout = managedKeyMaintenanceTimeout
	}
	maintenanceCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return consumer.ApplyUpstreamGroupFacts(maintenanceCtx, channelID)
}

// RefreshUpstreamGroupFacts refreshes the authoritative upstream group
// snapshot and then lets the normal consumer reconcile managed keys. The
// assignment preflight uses RefreshUpstreamGroupFactsForAssignment instead so
// an administrator adding one group does not synchronously reconcile the
// whole channel inventory.
func (s *Service) RefreshUpstreamGroupFacts(ctx context.Context, c *storage.Channel) error {
	return s.refreshUpstreamGroupFacts(ctx, c, true)
}

// RefreshUpstreamGroupFactsForAssignment refreshes only the authoritative
// group snapshot. Managed-key reconciliation is intentionally left to the
// caller for the requested group; this keeps candidate-group assignment
// bounded and avoids a duplicate full-channel control-plane pass.
func (s *Service) RefreshUpstreamGroupFactsForAssignment(ctx context.Context, c *storage.Channel) error {
	return s.refreshUpstreamGroupFacts(ctx, c, false)
}

func (s *Service) refreshUpstreamGroupFacts(ctx context.Context, c *storage.Channel, applyManagedKeys bool) error {
	if s == nil || c == nil || c.ID == 0 {
		return errors.New("upstream group fact refresh is unavailable")
	}
	unlock, lockErr := s.lockChannelRefresh(ctx, c.ID)
	if lockErr != nil {
		return lockErr
	}
	defer unlock()
	resolved, conn, session, err := s.prepare(ctx, c)
	if err != nil {
		return err
	}
	channels := s.channels.WithContext(ctx)
	rates := s.rates.WithContext(ctx)
	current, err := channels.FindByID(c.ID)
	if err != nil {
		return err
	}
	if current.UpstreamAccountIdentityRevision != c.UpstreamAccountIdentityRevision {
		return storage.ErrStaleChannelAccountIdentityRevision
	}
	snapshot, err := conn.GetRates(ctx, resolved, session)
	if err != nil {
		return err
	}
	if snapshot == nil {
		return errors.New("upstream group fact refresh returned an empty snapshot")
	}
	existing, err := rates.ListByChannel(c.ID)
	if err != nil {
		return err
	}
	if err := validateRateSnapshot(snapshot, len(existing)); err != nil {
		return err
	}
	items := make([]storage.RateSnapshot, 0, len(snapshot.Items))
	for _, item := range snapshot.Items {
		items = append(items, storage.RateSnapshot{
			ChannelID: c.ID, ModelName: strings.TrimSpace(item.ModelName), Description: item.Description,
			Ratio: item.Ratio, CompletionRatio: item.CompletionRatio,
		})
	}
	if current.CostUnitEpoch == 0 {
		return storage.ErrCostUnitEpochConflict
	}
	_, err = rates.ReplaceChannelSnapshotForCostUnitEpoch(storage.RateSnapshotBatchInput{
		ChannelID: c.ID, ExpectedIdentityRevision: current.UpstreamAccountIdentityRevision,
		ExpectedCostUnitEpoch: current.CostUnitEpoch, ObservedAt: time.Now().UTC(), Items: items,
		AllowCostUnitReview: true,
	})
	if err != nil {
		return err
	}
	if applyManagedKeys {
		consumer, ok := s.gatewaySync.(upstreamGroupFactsConsumer)
		if !ok {
			return nil
		}
		if consumeErr := s.applyUpstreamGroupFacts(ctx, consumer, c.ID); consumeErr != nil {
			s.log.Warn("apply refreshed upstream group facts failed", "channel", c.Name, "err", consumeErr)
		}
	}
	return nil
}

func shouldCompensateCollection(err error) bool {
	return err != nil &&
		!errors.Is(err, context.Canceled) &&
		!errors.Is(err, context.DeadlineExceeded) &&
		!connector.IsAccountBalanceHTTPError(err) &&
		!connector.IsAPIOriginMismatch(err) &&
		!isCostUnitAccountingGate(err)
}

// failBalanceSampling keeps a refused financial observation durably visible.
func (s *Service) failBalanceSampling(ctx context.Context, channelID uint, started time.Time, err error) error {
	if err == nil {
		return nil
	}
	reportMonitorStageFailure(ctx, progress.StageBalance, err)
	if s != nil && s.monitorLogs != nil {
		if started.IsZero() {
			started = time.Now()
		}
		_ = s.monitorLogs.Append(&storage.MonitorLog{
			ChannelID: channelID, Job: storage.MonitorJobBalance, Success: false,
			ErrorMessage: err.Error(), StartedAt: started, FinishedAt: time.Now(),
		})
	}
	return err
}

func isCostUnitAccountingGate(err error) bool {
	return costing.IsAccountingGate(err)
}

func reportMonitorStageFailure(ctx context.Context, stage progress.Stage, err error) {
	if err == nil {
		return
	}
	if isCostUnitAccountingGate(err) {
		progress.OK(ctx, stage, "账务采集已暂停，等待重试", map[string]any{"accounting_warning": err.Error()})
		return
	}
	progress.Fail(ctx, stage, err.Error())
}

func sameUpstreamAccountIdentity(left, right *storage.UpstreamAccountIdentitySnapshot) bool {
	return left != nil && right != nil && left.AccountRecordID != 0 &&
		left.AccountRecordID == right.AccountRecordID &&
		left.ChannelIdentityRevision == right.ChannelIdentityRevision &&
		left.Provider == right.Provider && left.Fingerprint == right.Fingerprint
}

// compensateCollectionFailure refreshes only the existing session. The caller
// owns the single retry budget and must not call this helper again for the
// same refresh operation.
func (s *Service) compensateCollectionFailure(
	ctx context.Context,
	c *storage.Channel,
	resolved *connector.Channel,
	conn connector.Connector,
	session *connector.AuthSession,
	collectionErr error,
) (*connector.AuthSession, bool, error) {
	if !shouldCompensateCollection(collectionErr) {
		return session, false, collectionErr
	}
	if s.channelSvc == nil {
		return session, true, errors.Join(collectionErr, errors.New("补偿同步不可用"))
	}
	refreshed, err := s.channelSvc.EnsureSessionWithoutLogin(ctx, c, resolved, conn)
	if err != nil {
		return session, true, errors.Join(collectionErr, fmt.Errorf("补偿同步失败：%w", err))
	}
	if refreshed == nil {
		return session, true, errors.Join(collectionErr, errors.New("补偿同步返回空会话"))
	}
	return refreshed, true, nil
}

func (s *Service) appendRateMonitorLog(channelID uint, started, finished time.Time, err error) {
	if s == nil || s.monitorLogs == nil {
		return
	}
	if err == nil {
		s.cadenceMu.Lock()
		last := s.rateSuccessAt[channelID]
		if !last.IsZero() && last.Add(rateSuccessLogInterval).After(finished) {
			s.cadenceMu.Unlock()
			return
		}
		s.rateSuccessAt[channelID] = finished
		s.cadenceMu.Unlock()
	} else {
		s.cadenceMu.Lock()
		delete(s.rateSuccessAt, channelID)
		s.cadenceMu.Unlock()
	}
	appendErr := s.monitorLogs.Append(&storage.MonitorLog{
		ChannelID:    channelID,
		Job:          storage.MonitorJobRates,
		Success:      err == nil,
		ErrorMessage: errString(err),
		StartedAt:    started,
		FinishedAt:   finished,
	})
	if appendErr != nil && err == nil {
		s.cadenceMu.Lock()
		if recorded := s.rateSuccessAt[channelID]; recorded.Equal(finished) {
			delete(s.rateSuccessAt, channelID)
		}
		s.cadenceMu.Unlock()
	}
}

func validateRateSnapshot(snapshot *connector.RateSnapshotResult, existingCount int) error {
	if snapshot == nil || !snapshot.Complete {
		return errors.New("上游分组快照不完整，保留现有分组")
	}
	results := snapshot.Items
	if len(results) == 0 && existingCount > 0 {
		return errors.New("上游返回空分组快照，保留现有分组")
	}
	seen := make(map[string]struct{}, len(results))
	for _, result := range results {
		name := strings.TrimSpace(result.ModelName)
		if name == "" {
			return errors.New("上游分组快照包含空名称")
		}
		if _, ok := seen[name]; ok {
			return fmt.Errorf("上游分组快照包含重复分组 %q", name)
		}
		seen[name] = struct{}{}
		if result.Ratio < 0 || math.IsNaN(result.Ratio) || math.IsInf(result.Ratio, 0) ||
			result.CompletionRatio < 0 || math.IsNaN(result.CompletionRatio) || math.IsInf(result.CompletionRatio, 0) {
			return fmt.Errorf("上游分组 %q 返回无效倍率", name)
		}
	}
	return nil
}

func sameCostUnitRateSnapshot(left, right *connector.RateSnapshotResult) bool {
	if left == nil || right == nil || !left.Complete || !right.Complete || len(left.Items) != len(right.Items) {
		return false
	}
	values := make(map[string]connector.RateResult, len(left.Items))
	for _, item := range left.Items {
		name := strings.ToLower(strings.TrimSpace(item.ModelName))
		if name == "" {
			return false
		}
		values[name] = item
	}
	if len(values) != len(left.Items) {
		return false
	}
	for _, item := range right.Items {
		name := strings.ToLower(strings.TrimSpace(item.ModelName))
		previous, ok := values[name]
		if !ok || !sameRateValue(previous.Ratio, item.Ratio) || !sameRateValue(previous.CompletionRatio, item.CompletionRatio) {
			return false
		}
		delete(values, name)
	}
	return len(values) == 0
}

func (s *Service) costUnitRateEvidence(channelID uint, rawScale float64, live *connector.RateSnapshotResult, previousCostAt *time.Time, rateSampledAt, observationFinishedAt time.Time) (costing.CostUnitRateEvidence, error) {
	var zero costing.CostUnitRateEvidence
	if s == nil || s.rates == nil || channelID == 0 || !finitePositiveFactor(rawScale) ||
		live == nil || !live.Complete || len(live.Items) == 0 || previousCostAt == nil || previousCostAt.IsZero() ||
		rateSampledAt.IsZero() || observationFinishedAt.IsZero() || !rateSampledAt.After(*previousCostAt) || rateSampledAt.After(observationFinishedAt) {
		return zero, fmt.Errorf("%w: complete rate-transition inputs are required", costing.ErrCostUnitRebaseEvidenceMismatch)
	}
	persisted, err := s.rates.ListByChannel(channelID)
	if err != nil {
		return zero, err
	}
	if len(persisted) != len(live.Items) {
		return zero, fmt.Errorf("%w: persisted and live rate snapshots differ in size", costing.ErrCostUnitRebaseEvidenceMismatch)
	}
	liveByName := make(map[string]connector.RateResult, len(live.Items))
	for _, item := range live.Items {
		name := strings.ToLower(strings.TrimSpace(item.ModelName))
		if name == "" {
			return zero, fmt.Errorf("%w: live rate snapshot contains an empty group", costing.ErrCostUnitRebaseEvidenceMismatch)
		}
		if _, duplicate := liveByName[name]; duplicate {
			return zero, fmt.Errorf("%w: live rate snapshot contains a duplicate group", costing.ErrCostUnitRebaseEvidenceMismatch)
		}
		liveByName[name] = item
	}
	if evidence, ok := buildCostUnitRateEvidence(persisted, liveByName, rawScale, rateSampledAt); ok {
		return evidence, nil
	}
	return zero, fmt.Errorf("%w: the complete live rate snapshot did not uniformly follow the provider factor change", costing.ErrCostUnitRebaseEvidenceMismatch)
}

func buildCostUnitRateEvidence(persisted []storage.RateSnapshot, live map[string]connector.RateResult, rawScale float64, sampledAt time.Time) (costing.CostUnitRateEvidence, bool) {
	var zero costing.CostUnitRateEvidence
	if len(persisted) != len(live) || len(persisted) == 0 || !finitePositiveFactor(rawScale) || sampledAt.IsZero() {
		return zero, false
	}
	items := make([]storage.CostUnitRateTransitionItem, 0, len(persisted))
	seen := make(map[string]struct{}, len(persisted))
	for _, previous := range persisted {
		name := strings.ToLower(strings.TrimSpace(previous.ModelName))
		if name == "" {
			return zero, false
		}
		if _, duplicate := seen[name]; duplicate {
			return zero, false
		}
		current, ok := live[name]
		if !ok || !sameScaledRateValue(previous.Ratio, current.Ratio, rawScale) ||
			!sameScaledRateValue(previous.CompletionRatio, current.CompletionRatio, rawScale) {
			return zero, false
		}
		seen[name] = struct{}{}
		items = append(items, storage.CostUnitRateTransitionItem{
			ModelName:         previous.ModelName,
			DescriptionBefore: previous.Description, DescriptionAfter: current.Description,
			RatioBefore: previous.Ratio, RatioAfter: current.Ratio,
			CompletionRatioBefore: previous.CompletionRatio, CompletionRatioAfter: current.CompletionRatio,
		})
	}
	if len(seen) != len(live) {
		return zero, false
	}
	digest, err := storage.CostUnitRateEvidenceDigest(items)
	if err != nil {
		return zero, false
	}
	return costing.CostUnitRateEvidence{
		TransitionAt: sampledAt, RawScale: rawScale,
		GroupCount: len(items), SnapshotDigest: digest, Items: items,
	}, true
}

func sameScaledRateValue(previous, current, scale float64) bool {
	if previous == 0 && current == 0 {
		return true
	}
	return sameRateValue(current, previous*scale)
}

func sameRateValue(left, right float64) bool {
	if left < 0 || right < 0 || math.IsNaN(left) || math.IsNaN(right) || math.IsInf(left, 0) || math.IsInf(right, 0) {
		return false
	}
	tolerance := math.Max(1e-9, math.Max(math.Abs(left), math.Abs(right))*1e-9)
	return math.Abs(left-right) <= tolerance
}

func (s *Service) CheckSubscriptionUsageAlerts(ctx context.Context, c *storage.Channel) error {
	if c == nil || c.CredentialMode == storage.CredentialModeDirect || !c.MonitorEnabled || !c.SubscriptionEnabled || (c.Type != storage.ChannelTypeSub2API && c.Type != storage.ChannelTypeNewAPI) {
		return nil
	}
	policy := s.dispatcher.Policy()
	if policy.SubscriptionDailyRemainingThresholdPct <= 0 &&
		policy.SubscriptionWeeklyRemainingThresholdPct <= 0 &&
		policy.SubscriptionMonthlyRemainingThresholdPct <= 0 &&
		policy.SubscriptionExpiryThreshold <= 0 {
		return nil
	}
	info, err := s.channelSvc.GetSubscriptionUsageForIdentity(ctx, c.ID, c.UpstreamAccountIdentityRevision)
	if err != nil {
		progress.Fail(ctx, progress.StageSubscription, err.Error())
		s.notifyError(ctx, c, storage.EventMonitorFailed, "订阅用量采集失败", err)
		return err
	}
	if err := s.dispatchSubscriptionWindowAlert(ctx, c, storage.EventSubscriptionDailyLow, "每日", policy.SubscriptionDailyRemainingThresholdPct, info.Items, func(item connector.SubscriptionUsage) *connector.SubscriptionUsageWindow {
		return item.Daily
	}); err != nil {
		return err
	}
	if err := s.dispatchSubscriptionWindowAlert(ctx, c, storage.EventSubscriptionWeeklyLow, "每周", policy.SubscriptionWeeklyRemainingThresholdPct, info.Items, func(item connector.SubscriptionUsage) *connector.SubscriptionUsageWindow {
		return item.Weekly
	}); err != nil {
		return err
	}
	if err := s.dispatchSubscriptionWindowAlert(ctx, c, storage.EventSubscriptionMonthlyLow, "每月", policy.SubscriptionMonthlyRemainingThresholdPct, info.Items, func(item connector.SubscriptionUsage) *connector.SubscriptionUsageWindow {
		return item.Monthly
	}); err != nil {
		return err
	}
	if err := s.dispatchSubscriptionExpiryAlert(ctx, c, policy.SubscriptionExpiryThreshold, info.Items); err != nil {
		return err
	}
	progress.OK(ctx, progress.StageSubscription, fmt.Sprintf("检查订阅用量 %d 项", len(info.Items)),
		map[string]any{"count": len(info.Items)})
	return nil
}

func (s *Service) dispatchSubscriptionWindowAlert(ctx context.Context, c *storage.Channel, event storage.NotificationEvent, label string, threshold float64, items []connector.SubscriptionUsage, pick func(connector.SubscriptionUsage) *connector.SubscriptionUsageWindow) error {
	if threshold <= 0 {
		return nil
	}
	lines := make([]string, 0)
	for _, item := range items {
		w := pick(item)
		if w == nil || w.LimitUSD <= 0 || w.RemainingPercent > threshold {
			continue
		}
		reset := "—"
		if w.ResetsAt != nil && !w.ResetsAt.IsZero() {
			reset = w.ResetsAt.Format("01-02 15:04")
		}
		lines = append(lines, fmt.Sprintf("· %s：已用 $%.4f / $%.4f，剩余 $%.4f（%.1f%%），重置 %s",
			subscriptionGroupName(item), w.UsedUSD, w.LimitUSD, w.RemainingUSD, w.RemainingPercent, reset))
	}
	if len(lines) == 0 {
		return nil
	}
	body := fmt.Sprintf("渠道：%s\n维度：%s\n阈值：剩余 %.1f%%\n%s", c.Name, label, threshold, strings.Join(lines, "\n"))
	return s.dispatchForChannelIdentity(ctx, c, notify.Message{
		Event:     event,
		ChannelID: c.ID,
		Subject:   fmt.Sprintf("%s 订阅%s剩余额度不足", c.Name, label),
		Body:      body,
	})
}

func (s *Service) dispatchSubscriptionExpiryAlert(ctx context.Context, c *storage.Channel, threshold time.Duration, items []connector.SubscriptionUsage) error {
	if threshold <= 0 {
		return nil
	}
	now := time.Now()
	lines := make([]string, 0)
	for _, item := range items {
		if item.ExpiresAt == nil || item.ExpiresAt.IsZero() {
			continue
		}
		remaining := item.ExpiresAt.Sub(now)
		if remaining > threshold {
			continue
		}
		lines = append(lines, fmt.Sprintf("· %s：到期 %s，剩余 %s",
			subscriptionGroupName(item), item.ExpiresAt.Format("2006-01-02 15:04"), formatDurationHours(remaining)))
	}
	if len(lines) == 0 {
		return nil
	}
	body := fmt.Sprintf("渠道：%s\n类型：订阅即将到期\n阈值：剩余 %.0f 小时\n%s", c.Name, threshold.Hours(), strings.Join(lines, "\n"))
	return s.dispatchForChannelIdentity(ctx, c, notify.Message{
		Event:     storage.EventSubscriptionExpiring,
		ChannelID: c.ID,
		Subject:   fmt.Sprintf("%s 订阅即将到期", c.Name),
		Body:      body,
	})
}

func subscriptionGroupName(item connector.SubscriptionUsage) string {
	if strings.TrimSpace(item.GroupName) != "" {
		return strings.TrimSpace(item.GroupName)
	}
	if item.GroupID > 0 {
		return fmt.Sprintf("分组 %d", item.GroupID)
	}
	return fmt.Sprintf("订阅 %d", item.ID)
}

func formatDurationHours(d time.Duration) string {
	if d <= 0 {
		return "已到期"
	}
	hours := d.Hours()
	if hours < 1 {
		return fmt.Sprintf("%.0f 分钟", d.Minutes())
	}
	return fmt.Sprintf("%.1f 小时", hours)
}

func (s *Service) prepare(ctx context.Context, c *storage.Channel) (*connector.Channel, connector.Connector, *connector.AuthSession, error) {
	if c != nil && c.CredentialMode == storage.CredentialModeDirect {
		return nil, nil, nil, connector.ErrUnsupported
	}
	resolved, err := s.channelSvc.Resolve(ctx, c)
	if err != nil {
		return nil, nil, nil, err
	}
	conn, err := connector.For(resolved.Type)
	if err != nil {
		return nil, nil, nil, err
	}
	s.channelSvc.ApplyHTTPConfig(conn)
	if err := s.channelSvc.ApplyProxy(conn, resolved); err != nil {
		return nil, nil, nil, err
	}
	session, err := s.channelSvc.EnsureSession(ctx, c, resolved, conn)
	if err != nil {
		return nil, nil, nil, err
	}
	return resolved, conn, session, nil
}

func (s *Service) notifyError(ctx context.Context, c *storage.Channel, event storage.NotificationEvent, subject string, err error) {
	if err == nil {
		return
	}
	_ = s.dispatchForChannelIdentity(ctx, c, notify.Message{
		Event:     event,
		ChannelID: c.ID,
		Subject:   fmt.Sprintf("%s %s", c.Name, subject),
		Body:      err.Error(),
	})
}

func (s *Service) requireChannelIdentity(c *storage.Channel) error {
	if s == nil || s.channels == nil || c == nil {
		return storage.ErrStaleChannelAccountIdentityRevision
	}
	return s.channels.RequireUpstreamAccountIdentityRevision(c.ID, c.UpstreamAccountIdentityRevision)
}

func (s *Service) dispatchForChannelIdentity(ctx context.Context, c *storage.Channel, msg notify.Message) error {
	if s == nil || s.dispatcher == nil {
		return nil
	}
	return s.dispatchWithChannelIdentity(ctx, c, func(dispatchCtx context.Context) error {
		return s.dispatcher.Dispatch(dispatchCtx, msg)
	})
}

func (s *Service) dispatchWithChannelIdentity(ctx context.Context, c *storage.Channel, dispatch func(context.Context) error) error {
	if s == nil || s.dispatcher == nil || dispatch == nil {
		return nil
	}
	if s.channels == nil || c == nil {
		return storage.ErrStaleChannelAccountIdentityRevision
	}
	if ctx == nil {
		ctx = context.Background()
	}
	timeout := s.identityDispatchTimeout
	if timeout <= 0 {
		timeout = channelIdentityDispatchTimeout
	}
	dispatchCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	unlock, err := s.channels.LockChannelOperation(dispatchCtx, c.ID)
	if err != nil {
		return err
	}
	defer unlock()
	if err := s.requireChannelIdentity(c); err != nil {
		return err
	}
	dispatchCtx = storage.WithChannelOperationLockHeld(dispatchCtx, c.ID)
	return dispatch(dispatchCtx)
}

func (s *Service) syncAnnouncements(ctx context.Context, c *storage.Channel, resolved *connector.Channel, conn connector.Connector, session *connector.AuthSession) error {
	if c != nil && c.CredentialMode == storage.CredentialModeDirect {
		return nil
	}
	if s.announcements == nil {
		return nil
	}
	if c.IgnoreAnnouncements {
		return nil
	}
	now := time.Now()
	key := channelIdentityRevisionKey{ChannelID: c.ID, Revision: c.UpstreamAccountIdentityRevision}
	s.cadenceMu.Lock()
	for existingKey, recordedAt := range s.announcementAt {
		if (existingKey.ChannelID == c.ID && existingKey != key) || recordedAt.Add(announcementSyncInterval).Before(now) {
			delete(s.announcementAt, existingKey)
		}
	}
	last := s.announcementAt[key]
	if !last.IsZero() && last.Add(announcementSyncInterval).After(now) {
		s.cadenceMu.Unlock()
		return nil
	}
	s.announcementAt[key] = now
	s.cadenceMu.Unlock()
	clearCadence := func() {
		s.cadenceMu.Lock()
		delete(s.announcementAt, key)
		s.cadenceMu.Unlock()
	}
	items, err := conn.GetAnnouncements(ctx, resolved, session)
	if err != nil {
		clearCadence()
		return err
	}
	if err := s.requireChannelIdentity(c); err != nil {
		clearCadence()
		return err
	}
	if len(items) == 0 {
		return nil
	}
	records := make([]storage.UpstreamAnnouncement, 0, len(items))
	for _, item := range items {
		records = append(records, storage.UpstreamAnnouncement{
			ChannelID:       c.ID,
			SourceKey:       item.SourceKey,
			Title:           item.Title,
			Content:         item.Content,
			Type:            item.Type,
			Link:            item.Link,
			PublishedAt:     item.PublishedAt,
			SourceUpdatedAt: item.SourceUpdatedAt,
		})
	}
	newRecords, existingCount, err := s.announcements.SyncForChannelIdentity(c.ID, c.UpstreamAccountIdentityRevision, records)
	if err != nil {
		clearCadence()
		return err
	}
	if existingCount == 0 {
		return nil
	}
	for i := range newRecords {
		rec := newRecords[i]
		if err := s.dispatchForChannelIdentity(ctx, c, notify.Message{
			Event:     storage.EventAnnouncement,
			ChannelID: c.ID,
			Subject:   announcementSubject(c, rec),
			Body:      announcementBody(c, rec),
			Extra: map[string]any{
				"announcement_id": rec.ID,
				"source_key":      rec.SourceKey,
				"title":           rec.Title,
				"type":            rec.Type,
				"link":            rec.Link,
			},
		}); err != nil {
			return err
		}
	}
	return nil
}

func announcementSubject(c *storage.Channel, a storage.UpstreamAnnouncement) string {
	title := strings.TrimSpace(a.Title)
	if title == "" {
		title = strings.TrimSpace(a.Content)
	}
	if title == "" {
		title = "上游公告"
	}
	if len([]rune(title)) > 40 {
		title = string([]rune(title)[:40])
	}
	return fmt.Sprintf("%s 公告 · %s", c.Name, title)
}

func announcementBody(c *storage.Channel, a storage.UpstreamAnnouncement) string {
	var b strings.Builder
	if a.Content != "" {
		b.WriteString(a.Content)
	}
	if a.Link != "" {
		if b.Len() > 0 {
			b.WriteString("\n")
		}
		b.WriteString("来源：")
		b.WriteString(a.Link)
	}
	if a.PublishedAt != nil {
		if b.Len() > 0 {
			b.WriteString("\n")
		}
		b.WriteString("发布时间：")
		b.WriteString(a.PublishedAt.Format("2006-01-02 15:04"))
	}
	return b.String()
}

// persistSafeProviderFactorEvidence records a matching provider unit without
// touching financial counters. It is intentionally limited to Sub2API's
// advertised checkout factor, which is the evidence required by the rate
// snapshot fence.
func (s *Service) persistSafeProviderFactorEvidence(c *storage.Channel, resolved *connector.Channel, state *storage.ChannelCostState, balance *connector.BalanceResult, costs *connector.CostResult, sampledAt time.Time) error {
	if costing.ConfirmedFundingBasisWithoutProviderEvidence(state) || costing.ManualCostBasisOverrideActive(state) {
		// An explicit funding confirmation or manual override is already the
		// trusted local cost basis. Keep incomplete provider history retryable
		// without pretending the advertised unit equals that independent basis.
		return nil
	}
	if s == nil || s.costing == nil || s.channels == nil || c == nil || resolved == nil || state == nil || balance == nil || costs == nil ||
		c.Type != storage.ChannelTypeSub2API || resolved.Type != connector.TypeSub2API || resolved.RechargeMultiplier != nil {
		return errors.New("provider factor evidence recovery is unavailable")
	}
	if strings.TrimSpace(balance.FactorSource) != "advertised" || strings.TrimSpace(costs.FactorSource) != "advertised" ||
		!sameFinancialFactor(balance.AppliedFactor, costs.AppliedFactor) || !finitePositiveFactor(balance.AppliedFactor) ||
		!sameFinancialFactor(balance.AppliedFactor, state.FundingFactor) {
		return fmt.Errorf("%w: current advertised factor does not match the persisted funding factor", costing.ErrCostUnitEvidenceUnavailable)
	}
	identity, err := s.channels.CurrentUpstreamAccountIdentity(c.ID)
	if err != nil || identity == nil || identity.ChannelIdentityRevision == 0 {
		if err != nil {
			return err
		}
		return storage.ErrUpstreamAccountIdentityUnbound
	}
	if sampledAt.IsZero() {
		sampledAt = balance.SampledAt
	}
	if sampledAt.IsZero() {
		sampledAt = time.Now().UTC()
	}
	_, err = s.costing.RecordProviderFactorEvidence(costing.ProviderFactorEvidenceInput{
		ChannelID: c.ID, ExpectedVersion: state.Version,
		ExpectedIdentityRevision: identity.ChannelIdentityRevision,
		ObservedFactor:           balance.AppliedFactor, Source: "advertised", SampledAt: sampledAt,
	})
	return err
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func normalizeFinancialFactorEvidence(channelType storage.ChannelType, balance *connector.BalanceResult, costs *connector.CostResult) error {
	if balance == nil || costs == nil {
		return errors.New("余额和消费倍率证据必须同时存在")
	}
	if channelType == storage.ChannelTypeNewAPI &&
		strings.TrimSpace(balance.FactorSource) == "recharge_history" &&
		strings.TrimSpace(costs.FactorSource) == "default" {
		costs.AppliedFactor = balance.AppliedFactor
		costs.FactorSource = balance.FactorSource
		costs.TodayCost = costs.RawTodayCost * balance.AppliedFactor
		costs.TotalCost = costs.RawTotalCost * balance.AppliedFactor
	}
	if !finitePositiveFactor(balance.AppliedFactor) || !finitePositiveFactor(costs.AppliedFactor) {
		return errors.New("余额或消费返回了无效的充值倍率")
	}
	balanceSource := strings.TrimSpace(balance.FactorSource)
	costSource := strings.TrimSpace(costs.FactorSource)
	if balanceSource == "" || costSource == "" || balanceSource != costSource {
		return fmt.Errorf("余额与消费的充值倍率来源不一致：%q / %q", balanceSource, costSource)
	}
	if !sameFinancialFactor(balance.AppliedFactor, costs.AppliedFactor) {
		return fmt.Errorf("余额与消费的充值倍率不一致：%.12g / %.12g", balance.AppliedFactor, costs.AppliedFactor)
	}
	return nil
}

func finitePositiveFactor(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

func sameFinancialFactor(left, right float64) bool {
	if !finitePositiveFactor(left) || !finitePositiveFactor(right) {
		return false
	}
	tolerance := math.Max(1e-12, math.Max(math.Abs(left), math.Abs(right))*1e-9)
	return math.Abs(left-right) <= tolerance
}

func consistentRechargeHistoryFactor(records []connector.RechargeRecord) (float64, bool) {
	var paidTotal, creditedTotal, reference float64
	usable := 0
	for _, record := range records {
		if record.CreditedAmount <= 0 || math.IsNaN(record.CreditedAmount) || math.IsInf(record.CreditedAmount, 0) {
			continue
		}
		if record.PaidAmount == nil || *record.PaidAmount <= 0 ||
			math.IsNaN(*record.PaidAmount) || math.IsInf(*record.PaidAmount, 0) ||
			strings.TrimSpace(record.SourceID) == "" {
			return 0, false
		}
		ratio := *record.PaidAmount / record.CreditedAmount
		if ratio <= 0 || math.IsNaN(ratio) || math.IsInf(ratio, 0) {
			continue
		}
		if usable == 0 {
			reference = ratio
		} else if math.Abs(ratio-reference) > math.Max(1e-6, reference*0.005) {
			return 0, false
		}
		paidTotal += *record.PaidAmount
		creditedTotal += record.CreditedAmount
		usable++
	}
	if usable == 0 || creditedTotal <= 0 {
		return 0, false
	}
	factor := paidTotal / creditedTotal
	return factor, factor > 0 && !math.IsNaN(factor) && !math.IsInf(factor, 0)
}
