// Package costing provides the canonical conversion between upstream credits
// and actual money paid by the operator.
package costing

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/config"
	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"gorm.io/gorm"
)

const (
	StateStatusReady                           = "ready"
	StateStatusError                           = "error"
	StateStatusNeedsReview                     = "needs_review"
	FundingKindRecharge                        = "recharge"
	LegacyAdvertisedFactorEvidenceReviewReason = "legacy advertised provider factor requires administrator evidence adoption"
	UpstreamCostCounterResetWarning            = "upstream cost counter decrease observed; reset confirmation pending"
)

var ErrFundingCandidateBelowThreshold = errors.New("observed funding amount is below the detection threshold")
var ErrCostUnitRebaseCommandInvalid = errors.New("cost unit rebase command is invalid")
var ErrCostUnitRebaseEvidenceMismatch = errors.New("cost unit rebase evidence does not match the requested provider multiplier")
var ErrCostUnitRebaseNotRequired = errors.New("cost unit rebase requires complete prior financial anchors")
var ErrCostUnitRebaseUnexplainedRegression = errors.New("cost unit rebase does not match a recognized upstream counter transition")
var ErrCostUnitRebaseUpstream = errors.New("cost unit rebase upstream collection failed")
var ErrCostBasisOverrideCommandInvalid = errors.New("cost basis override command is invalid")
var ErrCostUnitChangeRequiresRebase = errors.New("observed provider factor changed; administrator cost-unit rebase is required")
var ErrCostUnitEvidenceUnavailable = errors.New("trusted provider cost-unit evidence is temporarily unavailable")

// IsAccountingGate reports whether an error is a refusal to touch a channel's
// monetary state, as opposed to a connectivity, credential or provider failure.
//
// An accounting gate is deliberately narrow: it pauses the money ledger for one
// channel and nothing else. Callers must not use it to suppress unrelated
// non-financial work (subscription quota alerts, expiry warnings, announcement
// sync); refusing to sample a balance is not evidence that the upstream account
// is unreachable, and letting the gate cascade turns one accounting anomaly into
// a silent outage of every other monitoring signal the operator relies on.
func IsAccountingGate(err error) bool {
	return errors.Is(err, ErrCostUnitChangeRequiresRebase) ||
		errors.Is(err, ErrCostUnitEvidenceUnavailable) ||
		errors.Is(err, storage.ErrCostStateConflict) ||
		errors.Is(err, storage.ErrCostUnitEpochConflict) ||
		errors.Is(err, storage.ErrRateSnapshotCostUnitReview) ||
		errors.Is(err, storage.ErrStaleRateSnapshot) ||
		errors.Is(err, storage.ErrStaleChannelAccountIdentityRevision) ||
		errors.Is(err, storage.ErrStaleUpstreamAccountObservation) ||
		errors.Is(err, storage.ErrUpstreamAccountIdentityUnbound)
}

// trustedProviderFactorSource reports whether a funding-factor observation is
// provider evidence of the channel's monetary denomination.
//
// "advertised" is the Sub2API checkout multiplier; "recharge_history" is the
// NewAPI paid/credited ratio derived from the provider's own top-up records.
// Both are collected from the upstream account, so a change in either means the
// denomination itself moved and the price basis must be paused for review.
// "default", "legacy_unknown" and "manual" are not provider evidence: the first
// two are the absence of evidence, and the last is an administrator assertion
// that is already an explicit, reviewed decision.
//
// This predicate deliberately governs only the needs_review state machine. The
// cost-unit *rebase* command stays Sub2API/advertised-only, because rewriting a
// cumulative ledger needs a provider-served multiplier with an explicit
// divide/multiply mode, which recharge history does not supply.
func trustedProviderFactorSource(source string) bool {
	switch strings.TrimSpace(source) {
	case "advertised", "recharge_history":
		return true
	}
	return false
}

// LegacyAdvertisedFactorEvidenceMissing identifies a pre-cost-unit state whose
// financial basis was advertised but whose provider observation predates the
// durable evidence columns. A fresh full financial observation may recover the
// row only when it independently reports this exact persisted basis.
func LegacyAdvertisedFactorEvidenceMissing(state *storage.ChannelCostState) bool {
	return state != nil && strings.TrimSpace(state.Source) == "advertised" &&
		state.Status == StateStatusNeedsReview &&
		state.LastError == LegacyAdvertisedFactorEvidenceReviewReason &&
		state.ObservedProviderFactor == nil && state.ObservedProviderFactorSource == "" &&
		state.ObservedProviderFactorSampledAt == nil
}

func confirmedFundingSource(source string) bool {
	switch strings.TrimSpace(source) {
	case "confirmed_balance_increase", "confirmed_newapi_topup", "confirmed_sub2api_payment", "confirmed_redeem_code":
		return true
	default:
		return false
	}
}

// CanRecoverConfirmedFundingLegacyReview identifies a legacy migration marker
// that became stale after an administrator confirmed the channel's funding
// basis. The confirmed basis is independently auditable and does not require
// inventing advertised-factor evidence. Every other review state remains
// fenced for explicit repair.
func CanRecoverConfirmedFundingLegacyReview(state *storage.ChannelCostState) bool {
	return state != nil && confirmedFundingSource(state.Source) &&
		state.Status == StateStatusNeedsReview &&
		state.LastError == LegacyAdvertisedFactorEvidenceReviewReason &&
		state.ObservedProviderFactor == nil && state.ObservedProviderFactorSource == "" &&
		state.ObservedProviderFactorSampledAt == nil &&
		finite(state.FundingFactor) && state.FundingFactor > 0
}

// ConfirmedFundingBasisWithoutProviderEvidence reports whether the channel has
// an administrator-confirmed funding basis and no advertised-factor evidence.
// Missing provider evidence in this state must not rewrite cost anchors, but it
// also must not block independent group/rate synchronization.
func ConfirmedFundingBasisWithoutProviderEvidence(state *storage.ChannelCostState) bool {
	return state != nil && confirmedFundingSource(state.Source) &&
		state.Status == StateStatusReady && state.LastError == "" &&
		state.ObservedProviderFactor == nil && state.ObservedProviderFactorSource == "" &&
		state.ObservedProviderFactorSampledAt == nil &&
		finite(state.FundingFactor) && state.FundingFactor > 0
}

// ManualCostBasisOverrideActive reports an administrator-owned funding basis.
// Provider observations remain useful audit evidence, but they must not
// overwrite or re-fence an explicit manual assertion on a later sync.
func ManualCostBasisOverrideActive(state *storage.ChannelCostState) bool {
	return state != nil && strings.TrimSpace(state.Source) == storage.CostBasisOverrideSource &&
		finite(state.FundingFactor) && state.FundingFactor >= 0
}

// CanRecoverLegacyAdvertisedFactorEvidence requires current provider evidence
// to match the frozen financial basis. A changed factor still needs the
// administrator repair/rebase workflow because its historical boundary is
// unknowable.
func CanRecoverLegacyAdvertisedFactorEvidence(state *storage.ChannelCostState, observedFactor float64, observedSource string) bool {
	return LegacyAdvertisedFactorEvidenceMissing(state) &&
		strings.TrimSpace(observedSource) == "advertised" &&
		finite(state.FundingFactor) && state.FundingFactor > 0 &&
		finite(observedFactor) && observedFactor > 0 && sameFactor(state.FundingFactor, observedFactor)
}

// CanRecoverAdvertisedFactorEvidence covers both legacy needs_review rows and
// ready rows created before the observed-factor columns were populated. The
// current provider assertion is accepted only when it exactly matches the
// existing funding factor; a mismatch still requires the explicit rebase
// workflow.
func CanRecoverAdvertisedFactorEvidence(state *storage.ChannelCostState, observedFactor float64, observedSource string) bool {
	return state != nil && strings.TrimSpace(state.Source) == "advertised" &&
		strings.TrimSpace(observedSource) == "advertised" &&
		(state.ObservedProviderFactor == nil ||
			strings.TrimSpace(state.ObservedProviderFactorSource) != "advertised" ||
			state.ObservedProviderFactorSampledAt == nil || state.ObservedProviderFactorSampledAt.IsZero()) &&
		finite(state.FundingFactor) && state.FundingFactor > 0 &&
		finite(observedFactor) && observedFactor > 0 && sameFactor(state.FundingFactor, observedFactor)
}

func providerFactorChangeReviewMarkerMatches(state *storage.ChannelCostState) bool {
	if state == nil || state.Status != StateStatusNeedsReview || state.ObservedProviderFactor == nil ||
		!finite(*state.ObservedProviderFactor) || *state.ObservedProviderFactor <= 0 {
		return false
	}
	prefix := fmt.Sprintf("%s: persisted factor %.12g, observed factor ",
		ErrCostUnitChangeRequiresRebase, *state.ObservedProviderFactor)
	if !strings.HasPrefix(state.LastError, prefix) {
		return false
	}
	observed, err := strconv.ParseFloat(strings.TrimPrefix(state.LastError, prefix), 64)
	return err == nil && finite(observed) && observed > 0 &&
		!sameFactor(*state.ObservedProviderFactor, observed) &&
		state.LastError == fmt.Sprintf("%s%.12g", prefix, observed)
}

func finite(value float64) bool { return !math.IsNaN(value) && !math.IsInf(value, 0) }

// IsUpstreamCostCounterResetWarning identifies a non-fatal provider counter
// reset recorded on the cost state. It is deliberately separate from channel
// connectivity errors: a provider can reset its usage statistics while groups,
// keys, and probes remain fully usable.
func IsUpstreamCostCounterResetWarning(message string) bool {
	return strings.HasPrefix(strings.TrimSpace(message), UpstreamCostCounterResetWarning)
}

type upstreamCostCounterResetEvidence struct {
	PreviousRawTotal float64 `json:"previous_raw_total"`
	ResetBaseline    float64 `json:"reset_baseline"`
}

func costCounterResetWarning(previous, observed float64) string {
	evidence, _ := json.Marshal(upstreamCostCounterResetEvidence{PreviousRawTotal: previous, ResetBaseline: observed})
	return UpstreamCostCounterResetWarning + ": " + string(evidence)
}

func costCounterResetEvidence(message string, currentRawTotal float64) (upstreamCostCounterResetEvidence, bool) {
	payload := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(message), UpstreamCostCounterResetWarning+":"))
	if payload == "" || payload == strings.TrimSpace(message) {
		return upstreamCostCounterResetEvidence{}, false
	}
	var evidence upstreamCostCounterResetEvidence
	if err := json.Unmarshal([]byte(payload), &evidence); err != nil ||
		!finite(evidence.PreviousRawTotal) || !finite(evidence.ResetBaseline) ||
		evidence.PreviousRawTotal < 0 || evidence.ResetBaseline < 0 ||
		evidence.PreviousRawTotal <= evidence.ResetBaseline ||
		math.Abs(evidence.ResetBaseline-currentRawTotal) > 1e-9*math.Max(1, math.Abs(currentRawTotal)) {
		return upstreamCostCounterResetEvidence{}, false
	}
	return evidence, true
}

// applyRawTotalCostObservation requires a second observation before treating a
// decrease as a provider reset. A one-sample stale response that returns to the
// previous high-water mark therefore cannot replay already-accounted usage.
func applyRawTotalCostObservation(currentRawTotal, currentActualTotal, observedRawTotal, factor float64, sampled bool, lastError string) (float64, string, error) {
	if !sampled {
		actual, err := Actual(observedRawTotal, factor)
		return actual, "", err
	}
	pending, hasPending := costCounterResetEvidence(lastError, currentRawTotal)
	if observedRawTotal < currentRawTotal {
		previousHigh := currentRawTotal
		if hasPending && pending.PreviousRawTotal > previousHigh {
			previousHigh = pending.PreviousRawTotal
		}
		return currentActualTotal, costCounterResetWarning(previousHigh, observedRawTotal), nil
	}
	baseline := currentRawTotal
	if hasPending && observedRawTotal >= pending.PreviousRawTotal {
		// The counter recovered immediately: count only usage above the value
		// observed before the suspicious decrease.
		baseline = pending.PreviousRawTotal
	}
	delta, err := Actual(observedRawTotal-baseline, factor)
	if err != nil {
		return 0, "", err
	}
	return currentActualTotal + delta, "", nil
}

// Actual converts an upstream raw monetary value to actual money.
func Actual(raw, factor float64) (float64, error) {
	if !finite(raw) || raw < 0 {
		return 0, errors.New("raw amount must be finite and non-negative")
	}
	if !finite(factor) || factor < 0 {
		return 0, errors.New("funding factor must be finite and non-negative")
	}
	actual := raw * factor
	if !finite(actual) {
		return 0, errors.New("actual amount exceeds numeric range")
	}
	return actual, nil
}

// ActualBalance converts a signed upstream balance while retaining the same
// factor validation used by non-negative monetary counters.
func ActualBalance(raw, factor float64) (float64, error) {
	if !finite(raw) {
		return 0, errors.New("raw balance must be finite")
	}
	if !finite(factor) || factor < 0 {
		return 0, errors.New("funding factor must be finite and non-negative")
	}
	actual := raw * factor
	if !finite(actual) {
		return 0, errors.New("actual balance exceeds numeric range")
	}
	return actual, nil
}

// WeightedFactor blends the actual value of the remaining raw balance with a
// new recharge. paid and credited are the amounts entered by the operator.
func WeightedFactor(preRaw, oldFactor, paid, credited float64) (float64, error) {
	preActual, err := Actual(preRaw, oldFactor)
	if err != nil {
		return 0, err
	}
	if !finite(paid) || paid < 0 {
		return 0, errors.New("paid amount must be finite and non-negative")
	}
	if !finite(credited) || credited <= 0 {
		return 0, errors.New("credited amount must be finite and positive")
	}
	combinedRaw := preRaw + credited
	combinedActual := preActual + paid
	if !finite(combinedRaw) || !finite(combinedActual) {
		return 0, errors.New("combined amount exceeds numeric range")
	}
	factor := combinedActual / combinedRaw
	if !finite(factor) || factor < 0 {
		return 0, errors.New("calculated funding factor is invalid")
	}
	return factor, nil
}

type Store interface {
	FindState(channelID uint) (*storage.ChannelCostState, error)
	ListStates() ([]storage.ChannelCostState, error)
	UpdateState(next *storage.ChannelCostState, expectedVersion uint64) error
	UpdateStateForChannelIdentity(next *storage.ChannelCostState, expectedVersion, expectedIdentityRevision uint64, clearBalanceFact bool) error
	UpdateBalanceState(next *storage.ChannelCostState, expectedVersion uint64) error
	UpdateBalanceStateForChannelIdentity(next *storage.ChannelCostState, expectedVersion, expectedIdentityRevision uint64) error
	UpdateStateWithFundingCandidate(next *storage.ChannelCostState, candidate *storage.ChannelFundingCandidate, expectedVersion uint64) error
	UpdateStateWithFundingCandidateForChannelIdentity(next *storage.ChannelCostState, candidate *storage.ChannelFundingCandidate, expectedVersion, expectedIdentityRevision uint64) error
	AppendFundingEvent(next *storage.ChannelCostState, event *storage.ChannelFundingEvent, expectedVersion uint64) error
	ListFundingEvents(channelID uint, limit, offset int) ([]storage.ChannelFundingEvent, int64, error)
	UpsertFundingCandidate(candidate *storage.ChannelFundingCandidate) (*storage.ChannelFundingCandidate, error)
	FindFundingCandidate(channelID uint, candidateID uint64) (*storage.ChannelFundingCandidate, error)
	ListFundingCandidates(channelID uint, status string, limit, offset int) ([]storage.ChannelFundingCandidate, int64, error)
	ConfirmFundingCandidate(next *storage.ChannelCostState, event *storage.ChannelFundingEvent, candidateID uint64, expectedVersion uint64) error
	PendingFundingCounts() (map[uint]int64, error)
	FindCostUnitRebase(channelID uint, idempotencyKey string) (*storage.ChannelCostUnitRebase, error)
	ApplyCostUnitRebaseForIdentity(next *storage.ChannelCostState, audit *storage.ChannelCostUnitRebase, expectedVersion, expectedIdentityRevision, expectedAccountRecordID uint64) (bool, error)
	FindCostBasisOverride(channelID uint, idempotencyKey string) (*storage.ChannelCostBasisOverride, error)
	ApplyCostBasisOverride(next *storage.ChannelCostState, audit *storage.ChannelCostBasisOverride, expectedVersion, expectedIdentityRevision, expectedCostUnitEpoch uint64) (bool, error)
	MarkCostUnitReview(channelID uint, expectedVersion uint64, message string) error
	MarkCostUnitReviewForIdentity(channelID uint, expectedVersion, expectedIdentityRevision, expectedAccountRecordID uint64, message string) error
}

type Service struct {
	store Store

	mu                        sync.RWMutex
	fundingDetectionThreshold float64
}

func NewService(store Store) *Service {
	service := &Service{store: store}
	service.SetFundingDetectionThreshold(config.MinimumFundingDetectionThreshold)
	return service
}

// SetFundingDetectionThreshold updates the single runtime decision point used
// by every observed-credit path. The config package owns the hard floor.
func (s *Service) SetFundingDetectionThreshold(value float64) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.fundingDetectionThreshold = config.NormalizeFundingDetectionThreshold(value)
	s.mu.Unlock()
}

func (s *Service) FundingDetectionThreshold() float64 {
	if s == nil {
		return config.MinimumFundingDetectionThreshold
	}
	s.mu.RLock()
	value := s.fundingDetectionThreshold
	s.mu.RUnlock()
	return config.NormalizeFundingDetectionThreshold(value)
}

// IsFundingCandidateAmount only classifies a positive observed credit. It
// deliberately does not decide whether a particular upstream source needs a
// reconciliation candidate; callers keep their existing source semantics.
func (s *Service) IsFundingCandidateAmount(amount float64) bool {
	return finite(amount) && amount >= s.FundingDetectionThreshold()
}

func isObservedFundingSource(source string) bool {
	switch strings.TrimSpace(source) {
	case "balance_increase", "newapi_topup", "sub2api_payment", "redeem_code":
		return true
	default:
		return false
	}
}

func (s *Service) State(channelID uint) (*storage.ChannelCostState, error) {
	if channelID == 0 {
		return nil, errors.New("channel id is required")
	}
	return s.store.FindState(channelID)
}

func (s *Service) ListStates() ([]storage.ChannelCostState, error) { return s.store.ListStates() }

func (s *Service) FundingEvents(channelID uint, limit, offset int) ([]storage.ChannelFundingEvent, int64, error) {
	if channelID == 0 {
		return nil, 0, errors.New("channel id is required")
	}
	return s.store.ListFundingEvents(channelID, limit, offset)
}

func (s *Service) FundingCandidates(channelID uint, status string, limit, offset int) ([]storage.ChannelFundingCandidate, int64, error) {
	if channelID == 0 {
		return nil, 0, errors.New("channel id is required")
	}
	return s.store.ListFundingCandidates(channelID, status, limit, offset)
}

func (s *Service) PendingFundingCounts() (map[uint]int64, error) {
	return s.store.PendingFundingCounts()
}

// CostUnitRebaseCommand contains only administrator-controlled intent. Raw
// monetary observations are deliberately absent and must be sampled by the
// monitor service while it owns the channel refresh lock.
type CostUnitRebaseCommand struct {
	ExpectedVersion    uint64
	PreviousMultiplier float64
	CurrentMultiplier  float64
	IdempotencyKey     string
	Actor              string
	Note               string
	RequestRestatement *storage.CostUnitRequestRestatementEvidence
}

type CostUnitRebaseObservation struct {
	ChannelID                  uint
	ExpectedIdentityRevision   uint64
	ExpectedAccountRecordID    uint64
	ExpectedAccountFingerprint string
	MultiplierMode             string
	ObservedFactor             float64
	FactorSource               string
	RawBalance                 float64
	RawTodayCost               float64
	RawTotalCost               float64
	BalanceSampledAt           time.Time
	CostSampledAt              time.Time
	RateEvidence               CostUnitRateEvidence
}

type CostUnitRateEvidence struct {
	TransitionAt   time.Time
	RawScale       float64
	GroupCount     int
	SnapshotDigest string
	Items          []storage.CostUnitRateTransitionItem
}

type CostUnitRebaseResult struct {
	Audit            *storage.ChannelCostUnitRebase `json:"audit"`
	State            *storage.ChannelCostState      `json:"state,omitempty"`
	IdempotentReplay bool                           `json:"idempotent_replay"`
}

type CostBasisOverrideCommand struct {
	ExpectedVersion          uint64
	ExpectedIdentityRevision uint64
	ExpectedCostUnitEpoch    uint64
	FundingFactor            float64
	IdempotencyKey           string
	Actor                    string
	Note                     string
}

type CostBasisOverrideResult struct {
	Audit            *storage.ChannelCostBasisOverride `json:"audit"`
	State            *storage.ChannelCostState         `json:"state,omitempty"`
	IdempotentReplay bool                              `json:"idempotent_replay"`
}

// OverrideCostBasis records an administrator assertion for the current and
// future raw-credit conversion. Provider evidence and historical request costs
// remain untouched; the epoch advance fences every live price projection.
func (s *Service) OverrideCostBasis(channelID uint, command CostBasisOverrideCommand) (*CostBasisOverrideResult, error) {
	if s == nil || s.store == nil {
		return nil, errors.New("costing service is unavailable")
	}
	command, err := normalizeCostBasisOverrideCommand(channelID, command)
	if err != nil {
		return nil, err
	}
	fingerprint, err := costBasisOverrideFingerprint(channelID, command)
	if err != nil {
		return nil, err
	}
	if existing, findErr := s.store.FindCostBasisOverride(channelID, command.IdempotencyKey); findErr == nil {
		if existing.RequestFingerprint != fingerprint || existing.Actor != command.Actor {
			return nil, storage.ErrCostBasisOverrideIdempotencyConflict
		}
		return &CostBasisOverrideResult{Audit: existing, IdempotentReplay: true}, nil
	} else if !errors.Is(findErr, gorm.ErrRecordNotFound) {
		return nil, findErr
	}

	state, err := s.State(channelID)
	if err != nil {
		return nil, err
	}
	if state.Version != command.ExpectedVersion {
		return nil, storage.ErrCostStateConflict
	}
	if state.CostUnitEpoch != command.ExpectedCostUnitEpoch || state.CostUnitEpoch == math.MaxUint64 {
		return nil, storage.ErrCostUnitEpochConflict
	}
	if !finite(state.FundingFactor) || state.FundingFactor < 0 {
		return nil, ErrCostBasisOverrideCommandInvalid
	}
	actualInventory, err := ActualBalance(state.RawBalance, command.FundingFactor)
	if err != nil {
		return nil, ErrCostBasisOverrideCommandInvalid
	}
	next := *state
	next.ActualInventory = actualInventory
	next.FundingFactor = command.FundingFactor
	next.CostUnitEpoch = state.CostUnitEpoch + 1
	next.Source = storage.CostBasisOverrideSource
	next.Status = StateStatusReady
	next.LastError = ""
	audit := &storage.ChannelCostBasisOverride{
		ChannelID: channelID, IdempotencyKey: command.IdempotencyKey,
		RequestFingerprint: fingerprint, Actor: command.Actor, Note: command.Note,
	}
	replayed, err := s.store.ApplyCostBasisOverride(
		&next, audit, command.ExpectedVersion, command.ExpectedIdentityRevision, command.ExpectedCostUnitEpoch,
	)
	if err != nil {
		return nil, err
	}
	if replayed {
		return &CostBasisOverrideResult{Audit: audit, IdempotentReplay: true}, nil
	}
	return &CostBasisOverrideResult{Audit: audit, State: &next}, nil
}

func normalizeCostBasisOverrideCommand(channelID uint, command CostBasisOverrideCommand) (CostBasisOverrideCommand, error) {
	command.IdempotencyKey = strings.TrimSpace(command.IdempotencyKey)
	command.Actor = strings.TrimSpace(command.Actor)
	command.Note = strings.TrimSpace(command.Note)
	if channelID == 0 || command.ExpectedVersion == 0 || command.ExpectedIdentityRevision == 0 || command.ExpectedCostUnitEpoch == 0 ||
		!finite(command.FundingFactor) || command.FundingFactor < 0 ||
		command.IdempotencyKey == "" || len(command.IdempotencyKey) > 128 ||
		command.Actor == "" || len(command.Actor) > 128 || command.Note == "" || len(command.Note) > 1024 {
		return command, ErrCostBasisOverrideCommandInvalid
	}
	return command, nil
}

func costBasisOverrideFingerprint(channelID uint, command CostBasisOverrideCommand) (string, error) {
	payload := struct {
		ChannelID                uint    `json:"channel_id"`
		ExpectedVersion          uint64  `json:"expected_version"`
		ExpectedIdentityRevision uint64  `json:"expected_identity_revision"`
		ExpectedCostUnitEpoch    uint64  `json:"expected_cost_unit_epoch"`
		FundingFactor            float64 `json:"funding_factor"`
		Actor                    string  `json:"actor"`
		Note                     string  `json:"note"`
	}{channelID, command.ExpectedVersion, command.ExpectedIdentityRevision, command.ExpectedCostUnitEpoch, command.FundingFactor, command.Actor, command.Note}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func (s *Service) ReplayCostUnitRebase(channelID uint, command CostUnitRebaseCommand) (*CostUnitRebaseResult, error) {
	if s == nil || s.store == nil {
		return nil, errors.New("costing service is unavailable")
	}
	command.IdempotencyKey = strings.TrimSpace(command.IdempotencyKey)
	if channelID == 0 || command.IdempotencyKey == "" || len(command.IdempotencyKey) > 128 {
		return nil, ErrCostUnitRebaseCommandInvalid
	}
	audit, err := s.store.FindCostUnitRebase(channelID, command.IdempotencyKey)
	if err != nil {
		return nil, err
	}
	command, mode, err := normalizeCostUnitRebaseCommand(channelID, audit.CurrentMultiplierMode, command)
	if err != nil {
		return nil, err
	}
	fingerprint, err := costUnitRebaseFingerprint(channelID, audit.ChannelIdentityRevision, audit.AccountIdentityFingerprint, mode, command)
	if err != nil {
		return nil, err
	}
	if audit.RequestFingerprint != fingerprint || audit.Actor != command.Actor {
		return nil, storage.ErrCostUnitRebaseIdempotencyConflict
	}
	return &CostUnitRebaseResult{Audit: audit, IdempotentReplay: true}, nil
}

func (s *Service) RebaseCostUnit(command CostUnitRebaseCommand, observation CostUnitRebaseObservation) (*CostUnitRebaseResult, error) {
	if s == nil || s.store == nil {
		return nil, errors.New("costing service is unavailable")
	}
	command, mode, err := normalizeCostUnitRebaseCommand(observation.ChannelID, observation.MultiplierMode, command)
	if err != nil {
		return nil, err
	}
	if !finite(observation.ObservedFactor) || observation.ObservedFactor <= 0 ||
		observation.ExpectedIdentityRevision == 0 || observation.ExpectedAccountRecordID == 0 ||
		!validSHA256Hex(observation.ExpectedAccountFingerprint) ||
		!finite(observation.RawBalance) ||
		!finite(observation.RawTodayCost) || observation.RawTodayCost < 0 ||
		!finite(observation.RawTotalCost) || observation.RawTotalCost < 0 ||
		observation.BalanceSampledAt.IsZero() || observation.CostSampledAt.IsZero() ||
		strings.TrimSpace(observation.FactorSource) != "advertised" {
		return nil, fmt.Errorf("%w: sampled monetary values are incomplete", ErrCostUnitRebaseEvidenceMismatch)
	}

	currentProviderFactor := connector.RechargeCostFactor(&command.CurrentMultiplier, mode)
	if !sameFactor(observation.ObservedFactor, currentProviderFactor) {
		return nil, fmt.Errorf("%w: observed factor %.12g, expected %.12g", ErrCostUnitRebaseEvidenceMismatch, observation.ObservedFactor, currentProviderFactor)
	}

	state, err := s.State(observation.ChannelID)
	if err != nil {
		return nil, err
	}
	if state.Version != command.ExpectedVersion {
		return nil, storage.ErrCostStateConflict
	}
	if state.CostUnitEpoch == 0 || state.CostUnitEpoch == math.MaxUint64 {
		return nil, storage.ErrCostUnitEpochConflict
	}
	if state.CostSampledAt == nil || state.BalanceSampledAt == nil {
		return nil, fmt.Errorf("%w: a complete prior cost state is required", ErrCostUnitRebaseNotRequired)
	}
	if !observation.BalanceSampledAt.After(*state.BalanceSampledAt) ||
		!observation.CostSampledAt.After(*state.CostSampledAt) {
		return nil, fmt.Errorf("%w: current samples must follow the persisted anchors", ErrCostUnitRebaseEvidenceMismatch)
	}
	if state.ObservedProviderFactor == nil || state.ObservedProviderFactorSampledAt == nil ||
		strings.TrimSpace(state.ObservedProviderFactorSource) != "advertised" {
		return nil, fmt.Errorf("%w: a persisted advertised previous factor is required", ErrCostUnitRebaseEvidenceMismatch)
	}
	previousProviderFactor := *state.ObservedProviderFactor
	previousMode, err := historicalMultiplierMode(command.PreviousMultiplier, previousProviderFactor, mode)
	if err != nil {
		return nil, err
	}
	rawScale := previousProviderFactor / currentProviderFactor
	if !finite(rawScale) || rawScale <= 0 || sameFactor(rawScale, 1) {
		return nil, fmt.Errorf("%w: multiplier change does not alter the raw monetary unit", ErrCostUnitRebaseCommandInvalid)
	}
	if observation.RateEvidence.TransitionAt.IsZero() ||
		!observation.RateEvidence.TransitionAt.After(*state.CostSampledAt) ||
		observation.RateEvidence.TransitionAt.After(observation.CostSampledAt) ||
		observation.RateEvidence.GroupCount < 1 ||
		len(observation.RateEvidence.Items) != observation.RateEvidence.GroupCount ||
		!sameFactor(observation.RateEvidence.RawScale, rawScale) ||
		!validSHA256Hex(observation.RateEvidence.SnapshotDigest) {
		return nil, fmt.Errorf("%w: a complete multi-group rate transition is required", ErrCostUnitRebaseEvidenceMismatch)
	}

	oldPriorRaw, err := nonNegativeDifference(state.RawTotalCost, state.RawTodayCost)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid previous cost anchors: %v", ErrCostUnitRebaseEvidenceMismatch, err)
	}
	newPriorRaw, err := nonNegativeDifference(observation.RawTotalCost, observation.RawTodayCost)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid current cost anchors: %v", ErrCostUnitRebaseEvidenceMismatch, err)
	}
	counterTransition := "prior_unchanged"
	if !sameMonetaryCounter(newPriorRaw, oldPriorRaw) {
		counterTransition = "prior_scaled"
	}
	if counterTransition == "prior_scaled" && !sameMonetaryCounter(newPriorRaw, oldPriorRaw*rawScale) {
		return nil, fmt.Errorf(
			"%w: prior raw cost %.12g is neither unchanged from %.12g nor scaled to %.12g",
			ErrCostUnitRebaseUnexplainedRegression, newPriorRaw, oldPriorRaw, oldPriorRaw*rawScale,
		)
	}

	newFundingFactor := state.FundingFactor * currentProviderFactor / previousProviderFactor
	if !finite(newFundingFactor) || newFundingFactor < 0 {
		return nil, fmt.Errorf("%w: transformed funding factor is invalid", ErrCostUnitRebaseEvidenceMismatch)
	}
	priorActualCost, err := nonNegativeDifference(state.ActualTotalCost, state.ActualTodayCost)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid previous actual cost anchors: %v", ErrCostUnitRebaseEvidenceMismatch, err)
	}
	actualTodayCost, err := Actual(observation.RawTodayCost, newFundingFactor)
	if err != nil {
		return nil, err
	}
	actualTotalCost := priorActualCost + actualTodayCost
	if !finite(actualTotalCost) || actualTotalCost < 0 {
		return nil, fmt.Errorf("%w: transformed total cost is invalid", ErrCostUnitRebaseEvidenceMismatch)
	}
	actualInventory, err := ActualBalance(observation.RawBalance, newFundingFactor)
	if err != nil {
		return nil, err
	}

	next := *state
	next.RawBalance = observation.RawBalance
	next.ActualInventory = actualInventory
	next.FundingFactor = newFundingFactor
	next.RawTodayCost = observation.RawTodayCost
	next.ActualTodayCost = actualTodayCost
	next.RawTotalCost = observation.RawTotalCost
	next.ActualTotalCost = actualTotalCost
	next.BalanceSampledAt = &observation.BalanceSampledAt
	next.CostSampledAt = &observation.CostSampledAt
	observedFactor := observation.ObservedFactor
	next.ObservedProviderFactor = &observedFactor
	next.ObservedProviderFactorSource = strings.TrimSpace(observation.FactorSource)
	next.ObservedProviderFactorSampledAt = &observation.CostSampledAt
	next.CostUnitEpoch = state.CostUnitEpoch + 1
	next.Source = "cost_unit_rebase"
	next.Status = StateStatusReady
	next.LastError = ""

	fingerprint, err := costUnitRebaseFingerprint(observation.ChannelID, observation.ExpectedIdentityRevision, observation.ExpectedAccountFingerprint, mode, command)
	if err != nil {
		return nil, err
	}
	audit := &storage.ChannelCostUnitRebase{
		ChannelID: observation.ChannelID, IdempotencyKey: command.IdempotencyKey,
		RequestFingerprint: fingerprint, Actor: command.Actor,
		PreviousMultiplier: command.PreviousMultiplier, CurrentMultiplier: command.CurrentMultiplier,
		PreviousMultiplierMode: previousMode, CurrentMultiplierMode: mode,
		PreviousProviderFactor: previousProviderFactor, CurrentProviderFactor: currentProviderFactor,
		RateTransitionAt: observation.RateEvidence.TransitionAt,
		RateRawScale:     observation.RateEvidence.RawScale, RateEvidenceGroupCount: observation.RateEvidence.GroupCount,
		RateEvidenceDigest: observation.RateEvidence.SnapshotDigest, CounterTransition: counterTransition,
		RateTransitionItems: append([]storage.CostUnitRateTransitionItem(nil), observation.RateEvidence.Items...),
		RequestRestatement:  command.RequestRestatement,
		FactorSource:        strings.TrimSpace(observation.FactorSource), Note: command.Note,
		AppliedAt: time.Now().UTC(),
	}
	idempotentReplay, err := s.store.ApplyCostUnitRebaseForIdentity(
		&next, audit, command.ExpectedVersion, observation.ExpectedIdentityRevision, observation.ExpectedAccountRecordID,
	)
	if err != nil {
		return nil, err
	}
	if idempotentReplay {
		return &CostUnitRebaseResult{Audit: audit, IdempotentReplay: true}, nil
	}
	return &CostUnitRebaseResult{Audit: audit, State: &next}, nil
}

func normalizeCostUnitRebaseCommand(channelID uint, mode string, command CostUnitRebaseCommand) (CostUnitRebaseCommand, string, error) {
	mode = strings.TrimSpace(mode)
	command.IdempotencyKey = strings.TrimSpace(command.IdempotencyKey)
	command.Actor = strings.TrimSpace(command.Actor)
	command.Note = strings.TrimSpace(command.Note)
	var err error
	command.RequestRestatement, err = storage.NormalizeCostUnitRequestRestatementEvidence(command.RequestRestatement)
	if err != nil {
		return command, mode, ErrCostUnitRebaseCommandInvalid
	}
	if channelID == 0 || command.ExpectedVersion == 0 ||
		!finite(command.PreviousMultiplier) || command.PreviousMultiplier <= 0 ||
		!finite(command.CurrentMultiplier) || command.CurrentMultiplier <= 0 ||
		command.PreviousMultiplier == command.CurrentMultiplier ||
		len(command.IdempotencyKey) < 1 || len(command.IdempotencyKey) > 128 ||
		command.Actor == "" || len(command.Actor) > 128 || len(command.Note) > 1024 {
		return command, mode, ErrCostUnitRebaseCommandInvalid
	}
	if mode != connector.RechargeMultiplierModeDivide && mode != connector.RechargeMultiplierModeMultiply {
		return command, mode, ErrCostUnitRebaseCommandInvalid
	}
	return command, mode, nil
}

func costUnitRebaseFingerprint(channelID uint, identityRevision uint64, accountFingerprint, mode string, command CostUnitRebaseCommand) (string, error) {
	payload := struct {
		ChannelID          uint                                        `json:"channel_id"`
		IdentityRevision   uint64                                      `json:"identity_revision"`
		AccountFingerprint string                                      `json:"account_fingerprint"`
		ExpectedVersion    uint64                                      `json:"expected_version"`
		PreviousMultiplier float64                                     `json:"previous_multiplier"`
		CurrentMultiplier  float64                                     `json:"current_multiplier"`
		MultiplierMode     string                                      `json:"multiplier_mode"`
		Actor              string                                      `json:"actor"`
		Note               string                                      `json:"note"`
		RequestRestatement *storage.CostUnitRequestRestatementEvidence `json:"request_restatement,omitempty"`
	}{channelID, identityRevision, accountFingerprint, command.ExpectedVersion, command.PreviousMultiplier, command.CurrentMultiplier, mode, command.Actor, command.Note, command.RequestRestatement}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func historicalMultiplierMode(multiplier, observedFactor float64, currentMode string) (string, error) {
	divideFactor := connector.RechargeCostFactor(&multiplier, connector.RechargeMultiplierModeDivide)
	multiplyFactor := connector.RechargeCostFactor(&multiplier, connector.RechargeMultiplierModeMultiply)
	divideMatches := sameFactor(observedFactor, divideFactor)
	multiplyMatches := sameFactor(observedFactor, multiplyFactor)
	switch {
	case divideMatches && !multiplyMatches:
		return connector.RechargeMultiplierModeDivide, nil
	case multiplyMatches && !divideMatches:
		return connector.RechargeMultiplierModeMultiply, nil
	case divideMatches && multiplyMatches:
		return currentMode, nil
	default:
		return "", fmt.Errorf(
			"%w: persisted previous factor %.12g does not match multiplier %.12g",
			ErrCostUnitRebaseEvidenceMismatch, observedFactor, multiplier,
		)
	}
}

func validSHA256Hex(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && value == strings.ToLower(value)
}

func nonNegativeDifference(total, part float64) (float64, error) {
	if !finite(total) || !finite(part) || total < 0 || part < 0 {
		return 0, errors.New("counter values must be finite and non-negative")
	}
	difference := total - part
	tolerance := monetaryCounterTolerance(total, part)
	if difference < -tolerance {
		return 0, fmt.Errorf("total %.12g is below its current-period component %.12g", total, part)
	}
	if difference < 0 {
		return 0, nil
	}
	return difference, nil
}

func sameMonetaryCounter(left, right float64) bool {
	if !finite(left) || !finite(right) || left < 0 || right < 0 {
		return false
	}
	return math.Abs(left-right) <= monetaryCounterTolerance(left, right)
}

func monetaryCounterTolerance(left, right float64) float64 {
	return math.Max(1e-7, math.Max(math.Abs(left), math.Abs(right))*1e-9)
}

func sameFactor(left, right float64) bool {
	if !finite(left) || !finite(right) || left <= 0 || right <= 0 {
		return false
	}
	tolerance := math.Max(1e-12, math.Max(math.Abs(left), math.Abs(right))*1e-9)
	return math.Abs(left-right) <= tolerance
}

type FundingCandidateInput struct {
	ChannelID           uint
	Source              string
	ExternalID          string
	PaidAmount          *float64
	CreditedAmount      float64
	RawBalanceBefore    float64
	RawBalanceDetected  float64
	FundingFactorBefore float64
	RawTodayCostBefore  float64
	RawTotalCostBefore  float64
	OccurredAt          *time.Time
	DetectedAt          time.Time
	Note                string
}

func (s *Service) DetectFunding(in FundingCandidateInput) (*storage.ChannelFundingCandidate, error) {
	candidate, err := s.buildFundingCandidate(in)
	if err != nil {
		return nil, err
	}
	return s.store.UpsertFundingCandidate(candidate)
}

func (s *Service) buildFundingCandidate(in FundingCandidateInput) (*storage.ChannelFundingCandidate, error) {
	if in.ChannelID == 0 || strings.TrimSpace(in.Source) == "" || strings.TrimSpace(in.ExternalID) == "" {
		return nil, errors.New("channel, source and external id are required")
	}
	if isObservedFundingSource(in.Source) && !s.IsFundingCandidateAmount(in.CreditedAmount) {
		return nil, ErrFundingCandidateBelowThreshold
	}
	if !finite(in.CreditedAmount) || in.CreditedAmount <= 0 || !finite(in.RawBalanceBefore) || in.RawBalanceBefore < 0 || !finite(in.RawBalanceDetected) || in.RawBalanceDetected < 0 || !finite(in.FundingFactorBefore) || in.FundingFactorBefore < 0 || !finite(in.RawTodayCostBefore) || in.RawTodayCostBefore < 0 || !finite(in.RawTotalCostBefore) || in.RawTotalCostBefore < 0 {
		return nil, errors.New("candidate balances must be finite and non-negative")
	}
	if in.PaidAmount != nil && (!finite(*in.PaidAmount) || *in.PaidAmount < 0) {
		return nil, errors.New("candidate paid amount must be finite and non-negative")
	}
	if in.DetectedAt.IsZero() {
		in.DetectedAt = time.Now()
	}
	return &storage.ChannelFundingCandidate{
		ChannelID: in.ChannelID, Source: strings.TrimSpace(in.Source), ExternalID: strings.TrimSpace(in.ExternalID), Status: storage.FundingCandidatePending,
		PaidAmount: in.PaidAmount, CreditedAmount: in.CreditedAmount,
		RawBalanceBefore: in.RawBalanceBefore, RawBalanceDetected: in.RawBalanceDetected,
		FundingFactorBefore: in.FundingFactorBefore,
		RawTodayCostBefore:  in.RawTodayCostBefore, RawTotalCostBefore: in.RawTotalCostBefore,
		OccurredAt: in.OccurredAt, DetectedAt: in.DetectedAt, Note: strings.TrimSpace(in.Note),
	}, nil
}

func safeAutomaticFundingBalances(previousRaw, observedRaw float64, candidate FundingCandidateInput) bool {
	return finite(previousRaw) && previousRaw >= 0 &&
		finite(observedRaw) && observedRaw >= 0 &&
		finite(candidate.RawBalanceBefore) && candidate.RawBalanceBefore >= 0 &&
		finite(candidate.RawBalanceDetected) && candidate.RawBalanceDetected >= 0
}

func (s *Service) ConfirmFundingCandidate(channelID uint, candidateID uint64, paidAmount float64, expectedVersion uint64, note string) (*storage.ChannelCostState, *storage.ChannelFundingEvent, error) {
	candidate, err := s.store.FindFundingCandidate(channelID, candidateID)
	if err != nil {
		return nil, nil, err
	}
	if candidate.Status != storage.FundingCandidatePending {
		return nil, nil, errors.New("funding candidate is no longer pending")
	}
	state, err := s.State(channelID)
	if err != nil {
		return nil, nil, err
	}
	if state.Version != expectedVersion {
		return nil, nil, storage.ErrCostStateConflict
	}
	effectiveAt := fundingCandidateTime(*candidate)
	factor, err := WeightedFactor(candidate.RawBalanceBefore, candidate.FundingFactorBefore, paidAmount, candidate.CreditedAmount)
	if err != nil {
		return nil, nil, err
	}
	actualBefore, err := Actual(candidate.RawBalanceBefore, candidate.FundingFactorBefore)
	if err != nil {
		return nil, nil, err
	}
	eventActualAfter, err := Actual(candidate.RawBalanceDetected, factor)
	if err != nil {
		return nil, nil, err
	}
	currentActual, err := ActualBalance(state.RawBalance, factor)
	if err != nil {
		return nil, nil, err
	}
	next := *state
	next.RawBalance = state.RawBalance
	next.ActualInventory = currentActual
	next.FundingFactor = factor
	pending, _, pendingErr := s.FundingCandidates(channelID, storage.FundingCandidatePending, 500, 0)
	if pendingErr != nil {
		return nil, nil, pendingErr
	}
	endRawToday, endRawTotal := state.RawTodayCost, state.RawTotalCost
	candidateTime := fundingCandidateTime(*candidate)
	var nextCandidate *storage.ChannelFundingCandidate
	for _, later := range pending {
		laterTime := fundingCandidateTime(later)
		if later.ID == candidate.ID || laterTime.Before(candidateTime) || laterTime.Equal(candidateTime) && later.ID < candidate.ID {
			continue
		}
		if nextCandidate == nil || laterTime.Before(fundingCandidateTime(*nextCandidate)) || laterTime.Equal(fundingCandidateTime(*nextCandidate)) && later.ID < nextCandidate.ID {
			copy := later
			nextCandidate = &copy
		}
	}
	if nextCandidate != nil {
		endRawToday, endRawTotal = nextCandidate.RawTodayCostBefore, nextCandidate.RawTotalCostBefore
	}
	if rawDelta := endRawTotal - candidate.RawTotalCostBefore; rawDelta > 0 {
		next.ActualTotalCost += rawDelta * (factor - candidate.FundingFactorBefore)
	}
	if rawDelta := endRawToday - candidate.RawTodayCostBefore; rawDelta >= 0 {
		next.ActualTodayCost += rawDelta * (factor - candidate.FundingFactorBefore)
	}
	next.Source = "confirmed_" + candidate.Source
	// Resolving a funding candidate can advance the ledger, but it cannot prove
	// that an independently observed provider unit change was resolved. Only a
	// completed cost-unit rebase, or a full observation of the persisted factor,
	// may clear needs_review. The exact legacy migration marker is different:
	// this confirmation supersedes its unconfirmed advertised basis without
	// manufacturing any provider evidence.
	if state.Status != StateStatusNeedsReview || CanRecoverConfirmedFundingLegacyReview(&next) {
		next.Status = StateStatusReady
		next.LastError = ""
	}
	event := &storage.ChannelFundingEvent{
		ChannelID: channelID, Kind: FundingKindRecharge, PaidAmount: paidAmount, CreditedAmount: candidate.CreditedAmount,
		RawBalanceBefore: candidate.RawBalanceBefore, RawBalanceAfter: candidate.RawBalanceDetected,
		ActualInventoryBefore: actualBefore, ActualInventoryAfter: eventActualAfter,
		FundingFactorBefore: candidate.FundingFactorBefore, FundingFactorAfter: factor,
		Note: strings.TrimSpace(note), EffectiveAt: effectiveAt,
	}
	if err := s.store.ConfirmFundingCandidate(&next, event, candidateID, expectedVersion); err != nil {
		return nil, nil, err
	}
	return &next, event, nil
}

func fundingCandidateTime(candidate storage.ChannelFundingCandidate) time.Time {
	if candidate.OccurredAt != nil && !candidate.OccurredAt.IsZero() {
		return *candidate.OccurredAt
	}
	return candidate.DetectedAt
}

type BalanceInput struct {
	ChannelID                uint
	ExpectedVersion          uint64
	ExpectedIdentityRevision uint64
	ExpectedAccountRecordID  uint64
	RawBalance               float64
	ObservedFactor           float64
	SampledAt                time.Time
	Source                   string
}

// ProviderFactorEvidenceInput records a provider-served monetary factor
// without advancing any balance or cost anchors. It is used when a connector
// can prove the current unit but cannot safely reconstruct a missing lifetime
// counter; the next complete cost sample can then resume normal accounting.
type ProviderFactorEvidenceInput struct {
	ChannelID                uint
	ExpectedVersion          uint64
	ExpectedIdentityRevision uint64
	ObservedFactor           float64
	Source                   string
	SampledAt                time.Time
}

type CostInput struct {
	ChannelID                uint
	ExpectedVersion          uint64
	ExpectedIdentityRevision uint64
	RawTodayCost             float64
	RawTotalCost             float64
	SampledAt                time.Time
}

// ObservationInput is one monetary observation collected from a channel.
// Balance, costs and an optional funding candidate advance through one state
// version so a failed cost validation can never leave a newer balance behind.
type ObservationInput struct {
	Balance BalanceInput
	Costs   CostInput
	Funding *FundingCandidateInput
}

func (s *Service) RecordObservation(in ObservationInput) (*storage.ChannelCostState, *storage.ChannelFundingCandidate, error) {
	if in.Balance.ChannelID == 0 || in.Costs.ChannelID != in.Balance.ChannelID {
		return nil, nil, errors.New("matching balance and cost channel ids are required")
	}
	if in.Costs.ExpectedVersion != in.Balance.ExpectedVersion {
		return nil, nil, errors.New("matching balance and cost state versions are required")
	}
	if in.Costs.ExpectedIdentityRevision != 0 && in.Costs.ExpectedIdentityRevision != in.Balance.ExpectedIdentityRevision {
		return nil, nil, errors.New("matching balance and cost identity revisions are required")
	}
	if in.Funding != nil && in.Funding.ChannelID != in.Balance.ChannelID {
		return nil, nil, errors.New("funding candidate channel does not match observation")
	}

	state, err := s.State(in.Balance.ChannelID)
	if err != nil {
		return nil, nil, err
	}
	if state.Version != in.Balance.ExpectedVersion {
		return nil, nil, storage.ErrCostStateConflict
	}
	observedSource := strings.TrimSpace(in.Balance.Source)
	if !ManualCostBasisOverrideActive(state) && state.ObservedProviderFactor != nil &&
		trustedProviderFactorSource(state.ObservedProviderFactorSource) &&
		strings.TrimSpace(state.ObservedProviderFactorSource) == observedSource &&
		finite(in.Balance.ObservedFactor) && in.Balance.ObservedFactor > 0 &&
		!sameFactor(*state.ObservedProviderFactor, in.Balance.ObservedFactor) {
		if state.Status == StateStatusNeedsReview && !providerFactorChangeReviewMarkerMatches(state) {
			return nil, nil, fmt.Errorf("%w: existing cost-unit review remains unresolved", ErrCostUnitChangeRequiresRebase)
		}
		return nil, nil, s.flagCostUnitReview(
			state, in.Balance.ObservedFactor,
			in.Balance.ExpectedIdentityRevision, in.Balance.ExpectedAccountRecordID,
		)
	}
	next, err := nextBalanceStateFrom(state, in.Balance)
	if err != nil {
		return nil, nil, err
	}
	if in.Funding != nil && !safeAutomaticFundingBalances(state.RawBalance, next.RawBalance, *in.Funding) {
		in.Funding = nil
	}

	var candidate *storage.ChannelFundingCandidate
	if in.Funding != nil {
		if math.Abs(in.Funding.FundingFactorBefore-state.FundingFactor) > 1e-9 {
			return nil, nil, storage.ErrFundingCandidateCostBasis
		}
		if err := preserveFundingBasis(state, next); err != nil {
			return nil, nil, err
		}
		candidate, err = s.buildFundingCandidate(*in.Funding)
		if err != nil {
			return nil, nil, err
		}
	} else if _, pending, pendingErr := s.FundingCandidates(in.Balance.ChannelID, storage.FundingCandidatePending, 1, 0); pendingErr != nil {
		return nil, nil, pendingErr
	} else if pending > 0 {
		if err := preserveFundingBasis(state, next); err != nil {
			return nil, nil, err
		}
	}

	if err := applyCosts(state, next, in.Costs); err != nil {
		return nil, nil, err
	}
	if state.Status == StateStatusNeedsReview &&
		CanRecoverLegacyAdvertisedFactorEvidence(state, in.Balance.ObservedFactor, observedSource) {
		next.Status = StateStatusReady
		next.LastError = ""
	}
	if observedSource == "" {
		observedSource = "default"
	}
	if (state.Status != StateStatusNeedsReview || next.Status == StateStatusReady) &&
		finite(in.Balance.ObservedFactor) && in.Balance.ObservedFactor > 0 &&
		(observedSource != "default" || state.ObservedProviderFactor == nil) {
		observedFactor := in.Balance.ObservedFactor
		next.ObservedProviderFactor = &observedFactor
		next.ObservedProviderFactorSource = observedSource
		next.ObservedProviderFactorSampledAt = next.BalanceSampledAt
	}

	if candidate != nil {
		var updateErr error
		if in.Balance.ExpectedIdentityRevision != 0 {
			updateErr = s.store.UpdateStateWithFundingCandidateForChannelIdentity(next, candidate, in.Balance.ExpectedVersion, in.Balance.ExpectedIdentityRevision)
		} else {
			updateErr = s.store.UpdateStateWithFundingCandidate(next, candidate, in.Balance.ExpectedVersion)
		}
		if updateErr != nil {
			return nil, nil, updateErr
		}
		return next, candidate, nil
	}
	var updateErr error
	if in.Balance.ExpectedIdentityRevision != 0 {
		updateErr = s.store.UpdateStateForChannelIdentity(next, in.Balance.ExpectedVersion, in.Balance.ExpectedIdentityRevision, true)
	} else {
		updateErr = s.store.UpdateState(next, in.Balance.ExpectedVersion)
	}
	if updateErr != nil {
		return nil, nil, updateErr
	}
	return next, nil, nil
}

// RecordProviderFactorEvidence persists only an independently advertised
// provider factor. It deliberately leaves raw/actual balances, costs and
// their sample times untouched, so incomplete historical usage cannot rewrite
// the financial basis merely because unit evidence is available.
func (s *Service) RecordProviderFactorEvidence(in ProviderFactorEvidenceInput) (*storage.ChannelCostState, error) {
	if s == nil || s.store == nil || in.ChannelID == 0 || in.ExpectedVersion == 0 ||
		in.ExpectedIdentityRevision == 0 || !finite(in.ObservedFactor) || in.ObservedFactor <= 0 {
		return nil, errors.New("provider factor evidence is invalid")
	}
	source := strings.TrimSpace(in.Source)
	if !trustedProviderFactorSource(source) {
		return nil, fmt.Errorf("%w: provider factor source %q is not trusted", ErrCostUnitEvidenceUnavailable, source)
	}
	if in.SampledAt.IsZero() {
		in.SampledAt = time.Now().UTC()
	}
	state, err := s.State(in.ChannelID)
	if err != nil {
		return nil, err
	}
	if state.Version != in.ExpectedVersion {
		return nil, storage.ErrCostStateConflict
	}
	if state.ObservedProviderFactor != nil &&
		strings.TrimSpace(state.ObservedProviderFactorSource) == source &&
		finite(*state.ObservedProviderFactor) &&
		!sameFactor(*state.ObservedProviderFactor, in.ObservedFactor) {
		return nil, fmt.Errorf("%w: persisted factor %.12g, observed factor %.12g", ErrCostUnitChangeRequiresRebase, *state.ObservedProviderFactor, in.ObservedFactor)
	}
	if !finite(state.FundingFactor) || state.FundingFactor <= 0 || !sameFactor(state.FundingFactor, in.ObservedFactor) {
		return nil, fmt.Errorf("%w: observed factor %.12g does not match persisted funding factor %.12g", ErrCostUnitEvidenceUnavailable, in.ObservedFactor, state.FundingFactor)
	}
	if state.ObservedProviderFactor != nil &&
		strings.TrimSpace(state.ObservedProviderFactorSource) == source &&
		state.ObservedProviderFactorSampledAt != nil &&
		!state.ObservedProviderFactorSampledAt.IsZero() {
		return state, nil
	}
	next := *state
	factor := in.ObservedFactor
	next.ObservedProviderFactor = &factor
	next.ObservedProviderFactorSource = source
	next.ObservedProviderFactorSampledAt = &in.SampledAt
	if LegacyAdvertisedFactorEvidenceMissing(state) {
		next.Status = StateStatusReady
		next.LastError = ""
	}
	if err := s.store.UpdateStateForChannelIdentity(&next, in.ExpectedVersion, in.ExpectedIdentityRevision, false); err != nil {
		return nil, err
	}
	next.Version = in.ExpectedVersion + 1
	return &next, nil
}

// RecoverConfirmedFundingLegacyReview clears only the stale migration marker
// left behind after an administrator-confirmed funding event. It advances the
// state version under the channel identity fence while preserving every
// financial anchor and leaving provider-factor evidence empty.
func (s *Service) RecoverConfirmedFundingLegacyReview(channelID uint, expectedVersion, expectedIdentityRevision uint64) (*storage.ChannelCostState, error) {
	if s == nil || s.store == nil || channelID == 0 || expectedVersion == 0 || expectedIdentityRevision == 0 {
		return nil, errors.New("confirmed funding legacy review recovery is invalid")
	}
	state, err := s.State(channelID)
	if err != nil {
		return nil, err
	}
	if state.Version != expectedVersion {
		return nil, storage.ErrCostStateConflict
	}
	if !CanRecoverConfirmedFundingLegacyReview(state) {
		return nil, fmt.Errorf("%w: legacy review is not superseded by a confirmed funding basis", ErrCostUnitEvidenceUnavailable)
	}
	next := *state
	next.Status = StateStatusReady
	next.LastError = ""
	if err := s.store.UpdateStateForChannelIdentity(&next, expectedVersion, expectedIdentityRevision, false); err != nil {
		return nil, err
	}
	next.Version = expectedVersion + 1
	return &next, nil
}

// FlagCostUnitReview is used by collection paths which can prove an
// advertised-factor change before they have a complete monetary observation.
// It changes only the review status; all financial anchors remain frozen.
func (s *Service) FlagCostUnitReview(channelID uint, expectedVersion uint64, observedFactor float64, identityFence ...uint64) error {
	if s == nil || s.store == nil || channelID == 0 || !finite(observedFactor) || observedFactor <= 0 {
		return errors.New("cost unit review evidence is invalid")
	}
	state, err := s.State(channelID)
	if err != nil {
		return err
	}
	if state.Version != expectedVersion {
		return storage.ErrCostStateConflict
	}
	if state.ObservedProviderFactor == nil ||
		!trustedProviderFactorSource(state.ObservedProviderFactorSource) ||
		sameFactor(*state.ObservedProviderFactor, observedFactor) {
		return errors.New("cost unit review requires a changed persisted provider factor")
	}
	return s.flagCostUnitReview(state, observedFactor, identityFence...)
}

func (s *Service) flagCostUnitReview(state *storage.ChannelCostState, observedFactor float64, identityFence ...uint64) error {
	if state == nil || state.ChannelID == 0 || state.ObservedProviderFactor == nil ||
		!finite(*state.ObservedProviderFactor) || *state.ObservedProviderFactor <= 0 ||
		!finite(observedFactor) || observedFactor <= 0 {
		return errors.New("cost unit review evidence is invalid")
	}
	reviewErr := fmt.Errorf(
		"%w: persisted factor %.12g, observed factor %.12g",
		ErrCostUnitChangeRequiresRebase, *state.ObservedProviderFactor, observedFactor,
	)
	if state.Status != StateStatusNeedsReview || state.LastError != reviewErr.Error() {
		var err error
		if len(identityFence) == 2 && identityFence[0] != 0 && identityFence[1] != 0 {
			err = s.store.MarkCostUnitReviewForIdentity(state.ChannelID, state.Version, identityFence[0], identityFence[1], reviewErr.Error())
		} else {
			err = s.store.MarkCostUnitReview(state.ChannelID, state.Version, reviewErr.Error())
		}
		if err != nil {
			return err
		}
	}
	return reviewErr
}

func (s *Service) RecordCosts(in CostInput) (*storage.ChannelCostState, error) {
	state, err := s.State(in.ChannelID)
	if err != nil {
		return nil, err
	}
	if state.Version != in.ExpectedVersion {
		return nil, storage.ErrCostStateConflict
	}
	if !finite(in.RawTodayCost) || in.RawTodayCost < 0 || !finite(in.RawTotalCost) || in.RawTotalCost < 0 {
		return nil, errors.New("raw costs must be finite and non-negative")
	}
	if in.SampledAt.IsZero() {
		in.SampledAt = time.Now()
	}
	next := *state
	if state.CostSampledAt == nil {
		next.ActualTodayCost, err = Actual(in.RawTodayCost, state.FundingFactor)
		if err != nil {
			return nil, err
		}
	} else {
		todayDelta := in.RawTodayCost - state.RawTodayCost
		if todayDelta >= 0 {
			actualDelta, conversionErr := Actual(todayDelta, state.FundingFactor)
			if conversionErr != nil {
				return nil, conversionErr
			}
			next.ActualTodayCost = state.ActualTodayCost + actualDelta
		} else {
			next.ActualTodayCost, err = Actual(in.RawTodayCost, state.FundingFactor)
			if err != nil {
				return nil, err
			}
		}
	}
	actualTotalCost, counterWarning, err := applyRawTotalCostObservation(
		state.RawTotalCost, state.ActualTotalCost, in.RawTotalCost, state.FundingFactor,
		state.CostSampledAt != nil, state.LastError,
	)
	if err != nil {
		return nil, err
	}
	next.ActualTotalCost = actualTotalCost
	if (counterWarning != "" && state.Status == StateStatusReady) || IsUpstreamCostCounterResetWarning(state.LastError) {
		next.LastError = counterWarning
	}
	next.RawTodayCost = in.RawTodayCost
	next.RawTotalCost = in.RawTotalCost
	next.CostSampledAt = &in.SampledAt
	var updateErr error
	if in.ExpectedIdentityRevision != 0 {
		updateErr = s.store.UpdateStateForChannelIdentity(&next, in.ExpectedVersion, in.ExpectedIdentityRevision, false)
	} else {
		updateErr = s.store.UpdateState(&next, in.ExpectedVersion)
	}
	if updateErr != nil {
		return nil, updateErr
	}
	return &next, nil
}

func applyCosts(current, next *storage.ChannelCostState, in CostInput) error {
	if current == nil || next == nil || current.ChannelID == 0 || current.ChannelID != next.ChannelID || in.ChannelID != current.ChannelID {
		return errors.New("matching current and next cost state are required")
	}
	if current.Version != in.ExpectedVersion {
		return storage.ErrCostStateConflict
	}
	if !finite(in.RawTodayCost) || in.RawTodayCost < 0 || !finite(in.RawTotalCost) || in.RawTotalCost < 0 {
		return errors.New("raw costs must be finite and non-negative")
	}
	if in.SampledAt.IsZero() {
		in.SampledAt = time.Now()
	}
	if current.CostSampledAt == nil {
		var err error
		next.ActualTodayCost, err = Actual(in.RawTodayCost, next.FundingFactor)
		if err != nil {
			return err
		}
	} else {
		todayDelta := in.RawTodayCost - current.RawTodayCost
		if todayDelta >= 0 {
			actualDelta, err := Actual(todayDelta, next.FundingFactor)
			if err != nil {
				return err
			}
			next.ActualTodayCost += actualDelta
		} else {
			var err error
			next.ActualTodayCost, err = Actual(in.RawTodayCost, next.FundingFactor)
			if err != nil {
				return err
			}
		}
	}
	actualTotalCost, counterWarning, err := applyRawTotalCostObservation(
		current.RawTotalCost, current.ActualTotalCost, in.RawTotalCost, next.FundingFactor,
		current.CostSampledAt != nil, current.LastError,
	)
	if err != nil {
		return err
	}
	next.ActualTotalCost = actualTotalCost
	if (counterWarning != "" && current.Status == StateStatusReady) || IsUpstreamCostCounterResetWarning(current.LastError) {
		next.LastError = counterWarning
	}
	next.RawTodayCost = in.RawTodayCost
	next.RawTotalCost = in.RawTotalCost
	next.CostSampledAt = &in.SampledAt
	return nil
}
func (s *Service) RecordBalance(in BalanceInput) (*storage.ChannelCostState, error) {
	if len(strings.TrimSpace(in.Source)) > 32 {
		return nil, errors.New("source must not exceed 32 bytes")
	}
	_, next, err := s.nextBalanceState(in)
	if err != nil {
		return nil, err
	}
	var updateErr error
	if in.ExpectedIdentityRevision != 0 {
		updateErr = s.store.UpdateBalanceStateForChannelIdentity(next, in.ExpectedVersion, in.ExpectedIdentityRevision)
	} else {
		updateErr = s.store.UpdateBalanceState(next, in.ExpectedVersion)
	}
	if updateErr != nil {
		return nil, updateErr
	}
	return next, nil
}

func (s *Service) RecordBalanceWithFunding(in BalanceInput, funding FundingCandidateInput) (*storage.ChannelCostState, *storage.ChannelFundingCandidate, error) {
	if funding.ChannelID != in.ChannelID {
		return nil, nil, errors.New("funding candidate channel does not match balance")
	}
	state, next, err := s.nextBalanceState(in)
	if err != nil {
		return nil, nil, err
	}
	if !safeAutomaticFundingBalances(state.RawBalance, next.RawBalance, funding) {
		var updateErr error
		if in.ExpectedIdentityRevision != 0 {
			updateErr = s.store.UpdateBalanceStateForChannelIdentity(next, in.ExpectedVersion, in.ExpectedIdentityRevision)
		} else {
			updateErr = s.store.UpdateBalanceState(next, in.ExpectedVersion)
		}
		if updateErr != nil {
			return nil, nil, updateErr
		}
		return next, nil, nil
	}
	if math.Abs(funding.FundingFactorBefore-state.FundingFactor) > 1e-9 {
		return nil, nil, storage.ErrFundingCandidateCostBasis
	}
	if err := preserveFundingBasis(state, next); err != nil {
		return nil, nil, err
	}
	candidate, err := s.buildFundingCandidate(funding)
	if err != nil {
		return nil, nil, err
	}
	var updateErr error
	if in.ExpectedIdentityRevision != 0 {
		updateErr = s.store.UpdateStateWithFundingCandidateForChannelIdentity(next, candidate, in.ExpectedVersion, in.ExpectedIdentityRevision)
	} else {
		updateErr = s.store.UpdateStateWithFundingCandidate(next, candidate, in.ExpectedVersion)
	}
	if updateErr != nil {
		return nil, nil, updateErr
	}
	return next, candidate, nil
}

func preserveFundingBasis(current, next *storage.ChannelCostState) error {
	if current == nil || next == nil || current.ChannelID == 0 || current.ChannelID != next.ChannelID {
		return errors.New("matching current and next cost state are required")
	}
	actual, err := ActualBalance(next.RawBalance, current.FundingFactor)
	if err != nil {
		return err
	}
	next.ActualInventory = actual
	next.FundingFactor = current.FundingFactor
	next.ActualTodayCost = current.ActualTodayCost
	next.ActualTotalCost = current.ActualTotalCost
	next.Source = current.Source
	next.Status = current.Status
	return nil
}

func (s *Service) nextBalanceState(in BalanceInput) (*storage.ChannelCostState, *storage.ChannelCostState, error) {
	state, err := s.State(in.ChannelID)
	if err != nil {
		return nil, nil, err
	}
	next, err := nextBalanceStateFrom(state, in)
	if err != nil {
		return nil, nil, err
	}
	return state, next, nil
}

func nextBalanceStateFrom(state *storage.ChannelCostState, in BalanceInput) (*storage.ChannelCostState, error) {
	if state == nil || state.ChannelID == 0 || in.ChannelID != state.ChannelID {
		return nil, errors.New("matching channel cost state and balance are required")
	}
	if state.Version != in.ExpectedVersion {
		return nil, storage.ErrCostStateConflict
	}
	factor := state.FundingFactor
	observedSource := strings.TrimSpace(in.Source)
	if observedSource == "" {
		observedSource = "default"
	}
	adoptObservedFactor := state.Status != StateStatusNeedsReview &&
		(((state.Source == "default" || state.Source == "legacy_unknown") && observedSource != "default") ||
			(state.Source == "advertised" && (observedSource == "default" || observedSource == "recharge_history")))
	// A review is resolved when the same provider evidence source reports the
	// persisted factor again. Restricting this to "advertised" left every
	// NewAPI channel with no exit from needs_review at all, because its only
	// provider factor evidence is the recharge history ratio.
	reviewResolved := providerFactorChangeReviewMarkerMatches(state) &&
		trustedProviderFactorSource(state.ObservedProviderFactorSource) &&
		strings.TrimSpace(state.ObservedProviderFactorSource) == observedSource &&
		finite(in.ObservedFactor) && in.ObservedFactor > 0 && sameFactor(*state.ObservedProviderFactor, in.ObservedFactor)
	if adoptObservedFactor && finite(in.ObservedFactor) && in.ObservedFactor > 0 {
		factor = in.ObservedFactor
	}
	actual, err := ActualBalance(in.RawBalance, factor)
	if err != nil {
		return nil, err
	}
	if in.SampledAt.IsZero() {
		in.SampledAt = time.Now()
	}
	next := *state
	next.RawBalance = in.RawBalance
	next.ActualInventory = actual
	next.FundingFactor = factor
	next.BalanceSampledAt = &in.SampledAt
	if adoptObservedFactor && finite(in.ObservedFactor) && in.ObservedFactor > 0 {
		next.Source = observedSource
		if state.Source == "advertised" {
			next.ActualTodayCost, err = Actual(state.RawTodayCost, factor)
			if err != nil {
				return nil, err
			}
			next.ActualTotalCost, err = Actual(state.RawTotalCost, factor)
			if err != nil {
				return nil, err
			}
		}
	}
	if state.Status != StateStatusNeedsReview || reviewResolved {
		next.Status = StateStatusReady
		next.LastError = ""
	}
	return &next, nil
}

type FundingInput struct {
	ChannelID           uint
	ExpectedVersion     uint64
	PaidAmount          float64
	CreditedAmount      float64
	CurrentRawBalance   *float64
	ProjectedRawBalance *float64
	RawTodayCostBefore  *float64
	RawTotalCostBefore  *float64
	Kind                string
	Note                string
	EffectiveAt         time.Time
	Source              string
}

func (s *Service) RecordFunding(in FundingInput) (*storage.ChannelCostState, *storage.ChannelFundingEvent, error) {
	if len(strings.TrimSpace(in.Kind)) > 32 {
		return nil, nil, errors.New("funding kind must not exceed 32 bytes")
	}
	if len(strings.TrimSpace(in.Note)) > 1024 {
		return nil, nil, errors.New("funding note must not exceed 1024 bytes")
	}
	if len(strings.TrimSpace(in.Source)) > 32 {
		return nil, nil, errors.New("source must not exceed 32 bytes")
	}
	state, err := s.State(in.ChannelID)
	if err != nil {
		return nil, nil, err
	}
	if state.Version != in.ExpectedVersion {
		return nil, nil, storage.ErrCostStateConflict
	}
	preRaw := state.RawBalance
	if in.CurrentRawBalance != nil {
		preRaw = *in.CurrentRawBalance
	}
	beforeActual, err := Actual(preRaw, state.FundingFactor)
	if err != nil {
		return nil, nil, fmt.Errorf("validate balance before funding: %w", err)
	}
	factor, err := WeightedFactor(preRaw, state.FundingFactor, in.PaidAmount, in.CreditedAmount)
	if err != nil {
		return nil, nil, err
	}
	eventAfterRaw := preRaw + in.CreditedAmount
	eventAfterActual, err := Actual(eventAfterRaw, factor)
	if err != nil {
		return nil, nil, err
	}
	if in.EffectiveAt.IsZero() {
		in.EffectiveAt = time.Now()
	}
	if events, _, listErr := s.store.ListFundingEvents(in.ChannelID, 1, 0); listErr != nil {
		return nil, nil, listErr
	} else if len(events) > 0 && in.EffectiveAt.Before(events[0].EffectiveAt) {
		return nil, nil, errors.New("funding event cannot be inserted before the latest confirmed funding")
	}
	kind := strings.TrimSpace(in.Kind)
	if kind == "" {
		kind = FundingKindRecharge
	}
	next := *state
	projectedRaw := eventAfterRaw
	if in.ProjectedRawBalance != nil {
		projectedRaw = *in.ProjectedRawBalance
	}
	projectedActual, err := Actual(projectedRaw, factor)
	if err != nil {
		return nil, nil, err
	}
	rawTodayBefore, rawTotalBefore := state.RawTodayCost, state.RawTotalCost
	if in.RawTodayCostBefore != nil {
		rawTodayBefore = *in.RawTodayCostBefore
	}
	if in.RawTotalCostBefore != nil {
		rawTotalBefore = *in.RawTotalCostBefore
	}
	next.RawBalance = projectedRaw
	next.ActualInventory = projectedActual
	next.FundingFactor = factor
	if rawDelta := state.RawTotalCost - rawTotalBefore; rawDelta > 0 {
		next.ActualTotalCost += rawDelta * (factor - state.FundingFactor)
	}
	if rawDelta := state.RawTodayCost - rawTodayBefore; rawDelta >= 0 {
		next.ActualTodayCost += rawDelta * (factor - state.FundingFactor)
	}
	next.Source = normalizedSource(in.Source)
	if state.Status != StateStatusNeedsReview {
		next.Status = StateStatusReady
		next.LastError = ""
	}
	event := &storage.ChannelFundingEvent{
		ChannelID: in.ChannelID, Kind: kind,
		PaidAmount: in.PaidAmount, CreditedAmount: in.CreditedAmount,
		RawBalanceBefore: preRaw, RawBalanceAfter: eventAfterRaw,
		ActualInventoryBefore: beforeActual, ActualInventoryAfter: eventAfterActual,
		FundingFactorBefore: state.FundingFactor, FundingFactorAfter: factor,
		RawTodayCostBefore: rawTodayBefore, RawTotalCostBefore: rawTotalBefore,
		Note: strings.TrimSpace(in.Note), EffectiveAt: in.EffectiveAt,
	}
	if err := s.store.AppendFundingEvent(&next, event, in.ExpectedVersion); err != nil {
		return nil, nil, err
	}
	return &next, event, nil
}

func (s *Service) RecordError(channelID uint, expectedVersion uint64, source string, cause error) (*storage.ChannelCostState, error) {
	if len(strings.TrimSpace(source)) > 32 {
		return nil, errors.New("source must not exceed 32 bytes")
	}
	state, err := s.State(channelID)
	if err != nil {
		return nil, err
	}
	if state.Version != expectedVersion {
		return nil, storage.ErrCostStateConflict
	}
	// A generic refresh error carries no evidence about the provider monetary
	// unit. Keep the stronger review gate and its frozen evidence intact.
	if state.Status == StateStatusNeedsReview {
		return state, nil
	}
	next := *state
	next.Source = normalizedSource(source)
	next.Status = StateStatusError
	if cause != nil {
		next.LastError = cause.Error()
	} else {
		next.LastError = "cost state refresh failed"
	}
	if err := s.store.UpdateState(&next, expectedVersion); err != nil {
		return nil, err
	}
	return &next, nil
}

func normalizedSource(source string) string {
	if source = strings.TrimSpace(source); source != "" {
		return source
	}
	return "manual"
}
