package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	GatewayRequestPriceSourceAdministrator = "administrator"
	GatewayRequestPriceImportMaxItems      = 500
	gatewayRequestPriceImportDigestVersion = 1
)

var (
	ErrGatewayRequestPriceImportInvalid             = errors.New("request price import is invalid")
	ErrGatewayRequestPriceImportUnavailable         = errors.New("request price import target is unavailable")
	ErrGatewayRequestPriceImportIdempotencyConflict = errors.New("request price import idempotency key conflicts with an existing command")
	ErrGatewayRequestPriceImportCommitUnknown       = errors.New("request price import commit outcome is unknown")

	gatewayRequestPriceImportIdempotencyPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)
)

// GatewayAdministratorPriceObservation contains only administrator-attested
// actual USD prices. The storage layer resolves every mutable identity field
// from the current database rows while holding locks; callers cannot select a
// channel, group, or key generation directly.
type GatewayAdministratorPriceObservation struct {
	ChannelID                       uint
	GroupName                       string
	ModelName                       string
	Operation                       string
	Protocol                        string
	ExpectedIdentityRevision        uint64
	ExpectedAdminRevision           uint64
	ExpectedProvider                ChannelType
	ExpectedAPIOrigin               string
	ExpectedCostUnitEpoch           uint64
	ExpectedGroupID                 uint
	ExpectedGroupPresenceGeneration uint64
	ExpectedUpstreamKeyID           uint
	ExpectedUpstreamKeyGeneration   uint64
	ObservedAt                      time.Time
	InputUnitPriceUSD               *float64
	OutputUnitPriceUSD              *float64
	CacheReadUnitPriceUSD           *float64
	CacheWriteUnitPriceUSD          *float64
	OperationUnitPriceUSD           *float64
	ValidUntil                      time.Time
}

type preparedGatewayAdministratorPriceObservation struct {
	GatewayAdministratorPriceObservation
	index              int
	canonicalGroupName string
	unit               string
}

// GatewayRequestPriceImportResult is stable across retries of one command.
// Items retain the caller's original order even though writes are lock-ordered.
type GatewayRequestPriceImportResult struct {
	CommandID        uint64
	Items            []GatewayRequestPriceFact
	IdempotentReplay bool
}

// GatewayAdministratorPriceTarget is the current compare-only identity fence
// an authenticated administrator must carry into a later price observation.
// It intentionally contains no account identifier or key material.
type GatewayAdministratorPriceTarget struct {
	ChannelID                       uint        `json:"channel_id"`
	GroupName                       string      `json:"group_name"`
	ModelName                       string      `json:"model_name"`
	ExpectedIdentityRevision        uint64      `json:"expected_identity_revision"`
	ExpectedAdminRevision           uint64      `json:"expected_admin_revision"`
	ExpectedProvider                ChannelType `json:"expected_provider"`
	ExpectedAPIOrigin               string      `json:"expected_api_origin"`
	ExpectedCostUnitEpoch           uint64      `json:"expected_cost_unit_epoch"`
	ExpectedGroupID                 uint        `json:"expected_group_id"`
	ExpectedGroupPresenceGeneration uint64      `json:"expected_group_presence_generation"`
	ExpectedUpstreamKeyID           uint        `json:"expected_upstream_key_id"`
	ExpectedUpstreamKeyGeneration   uint64      `json:"expected_upstream_key_generation"`
	CapturedAt                      time.Time   `json:"captured_at"`
}

type gatewayRequestPriceImportCommand struct {
	ID                   uint64
	Actor                string
	IdempotencyKey       string
	PayloadDigest        string
	PayloadDigestVersion int
	ItemCount            int
	ImportedAt           time.Time
	CreatedAt            time.Time
}

func (gatewayRequestPriceImportCommand) TableName() string {
	return "gateway_request_price_import_commands"
}

type gatewayRequestPriceImportItemReceipt struct {
	CommandID   uint64
	ItemIndex   int
	PriceFactID uint64
	CreatedAt   time.Time
}

func (gatewayRequestPriceImportItemReceipt) TableName() string {
	return "gateway_request_price_import_items"
}

type gatewayRequestPriceImportDigestPayload struct {
	Version int                                          `json:"version"`
	Items   []gatewayRequestPriceImportDigestPayloadItem `json:"items"`
}

type gatewayRequestPriceImportDigestPayloadItem struct {
	ChannelID                       uint     `json:"channel_id"`
	CanonicalGroupName              string   `json:"canonical_group_name"`
	ModelName                       string   `json:"model_name"`
	Operation                       string   `json:"operation"`
	Protocol                        string   `json:"protocol"`
	ExpectedIdentityRevision        uint64   `json:"expected_identity_revision"`
	ExpectedAdminRevision           uint64   `json:"expected_admin_revision"`
	ExpectedProvider                string   `json:"expected_provider"`
	ExpectedAPIOrigin               string   `json:"expected_api_origin"`
	ExpectedCostUnitEpoch           uint64   `json:"expected_cost_unit_epoch"`
	ExpectedGroupID                 uint     `json:"expected_group_id"`
	ExpectedGroupPresenceGeneration uint64   `json:"expected_group_presence_generation"`
	ExpectedUpstreamKeyID           uint     `json:"expected_upstream_key_id"`
	ExpectedUpstreamKeyGeneration   uint64   `json:"expected_upstream_key_generation"`
	ObservedAt                      string   `json:"observed_at"`
	InputUnitPriceUSD               *float64 `json:"input_unit_price_usd"`
	OutputUnitPriceUSD              *float64 `json:"output_unit_price_usd"`
	CacheReadUnitPriceUSD           *float64 `json:"cache_read_unit_price_usd"`
	CacheWriteUnitPriceUSD          *float64 `json:"cache_write_unit_price_usd"`
	OperationUnitPriceUSD           *float64 `json:"operation_unit_price_usd"`
	ValidUntil                      string   `json:"valid_until"`
}

// ImportAdministratorRequestPriceFacts atomically appends one finite-lived
// fact for every observation and an immutable receipt for the command. A
// retry with the same actor, idempotency key, and normalized payload returns
// the original facts without consulting mutable channel identity again.
func (g *Gateway) ImportAdministratorRequestPriceFacts(ctx context.Context, actor, idempotencyKey string, observations []GatewayAdministratorPriceObservation) (*GatewayRequestPriceImportResult, error) {
	if g == nil || g.db == nil {
		return nil, errors.New("gateway storage is required")
	}
	actor = strings.TrimSpace(actor)
	idempotencyKey = strings.TrimSpace(idempotencyKey)
	if actor == "" || strings.ContainsRune(actor, '\x00') || !utf8.ValidString(actor) || utf8.RuneCountInString(actor) > 128 {
		return nil, fmt.Errorf("%w: administrator actor is invalid", ErrGatewayRequestPriceImportInvalid)
	}
	if !gatewayRequestPriceImportIdempotencyPattern.MatchString(idempotencyKey) {
		return nil, fmt.Errorf("%w: idempotency key is invalid", ErrGatewayRequestPriceImportInvalid)
	}
	if len(observations) == 0 || len(observations) > GatewayRequestPriceImportMaxItems {
		return nil, fmt.Errorf("%w: batch must contain between 1 and %d observations", ErrGatewayRequestPriceImportInvalid, GatewayRequestPriceImportMaxItems)
	}
	prepared := make([]preparedGatewayAdministratorPriceObservation, len(observations))
	for index := range observations {
		item := observations[index]
		unit, err := normalizeGatewayAdministratorPriceObservation(&item)
		if err != nil {
			return nil, fmt.Errorf("%w: item %d: %v", ErrGatewayRequestPriceImportInvalid, index, err)
		}
		prepared[index] = preparedGatewayAdministratorPriceObservation{
			GatewayAdministratorPriceObservation: item,
			index:                                index,
			unit:                                 unit,
		}
	}
	if ctx == nil {
		ctx = context.Background()
	}

	var result *GatewayRequestPriceImportResult
	var payloadDigest string
	transactionBodyComplete := false
	err := g.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if tx.Dialector != nil && tx.Dialector.Name() == string(DBDriverPostgres) {
			if err := tx.Exec("LOCK TABLE public.gateway_request_price_import_commands IN SHARE ROW EXCLUSIVE MODE").Error; err != nil {
				return gatewayRequestPriceImportIdentityLockError(err)
			}
		}
		for index := range prepared {
			canonical, err := canonicalUpstreamGroupName(tx, prepared[index].GroupName)
			if err != nil {
				return fmt.Errorf("canonicalize item %d group_name: %w", prepared[index].index, err)
			}
			prepared[index].canonicalGroupName = canonical
		}
		var err error
		payloadDigest, err = gatewayRequestPriceImportPayloadDigest(prepared)
		if err != nil {
			return fmt.Errorf("%w: canonical payload: %v", ErrGatewayRequestPriceImportInvalid, err)
		}
		replay, err := replayGatewayRequestPriceImportTx(tx, actor, idempotencyKey, payloadDigest, len(prepared))
		if err != nil {
			return err
		}
		if replay != nil {
			result = replay
			transactionBodyComplete = true
			return nil
		}
		sort.SliceStable(prepared, func(i, j int) bool {
			left, right := prepared[i], prepared[j]
			if left.ChannelID != right.ChannelID {
				return left.ChannelID < right.ChannelID
			}
			leftKey := left.canonicalGroupName + "\x00" + left.ModelName + "\x00" + left.Operation + "\x00" + left.Protocol
			rightKey := right.canonicalGroupName + "\x00" + right.ModelName + "\x00" + right.Operation + "\x00" + right.Protocol
			return leftKey < rightKey
		})
		for index := 1; index < len(prepared); index++ {
			previous, current := prepared[index-1], prepared[index]
			if previous.ChannelID == current.ChannelID &&
				previous.canonicalGroupName == current.canonicalGroupName &&
				previous.ModelName == current.ModelName &&
				previous.Operation == current.Operation &&
				previous.Protocol == current.Protocol {
				return fmt.Errorf("%w: items %d and %d contain the same price scope", ErrGatewayRequestPriceImportInvalid, previous.index, current.index)
			}
		}
		// Keep the append lock before every mutable route-identity lock. The
		// generic fact writer uses the same order, which prevents a concurrent
		// channel/key mutation from closing a price-table/channel deadlock cycle.
		if tx.Dialector != nil && tx.Dialector.Name() == string(DBDriverPostgres) {
			if err := tx.Exec("LOCK TABLE public.gateway_request_price_facts IN SHARE ROW EXCLUSIVE MODE").Error; err != nil {
				return gatewayRequestPriceImportIdentityLockError(err)
			}
		}
		// A normalized upstream group can have multiple legacy raw-name key rows.
		// Freeze that whole candidate set before choosing a key so an unselected
		// row cannot be inserted or refreshed into first place mid-batch.
		if tx.Dialector != nil && tx.Dialector.Name() == string(DBDriverPostgres) {
			if err := tx.Exec("LOCK TABLE public.gateway_upstream_keys IN SHARE MODE").Error; err != nil {
				return gatewayRequestPriceImportIdentityLockError(err)
			}
		}
		var lockedChannelID uint
		for _, item := range prepared {
			if item.ChannelID == lockedChannelID {
				continue
			}
			var channelID uint
			if err := tx.Model(&Channel{}).Clauses(clause.Locking{Strength: "SHARE", Options: "NOWAIT"}).
				Select("id").Where("id = ?", item.ChannelID).Scan(&channelID).Error; err != nil {
				return gatewayRequestPriceImportIdentityLockError(err)
			}
			if channelID == 0 {
				return fmt.Errorf("%w: channel %d does not exist", ErrGatewayRequestPriceImportUnavailable, item.ChannelID)
			}
			lockedChannelID = item.ChannelID
		}
		importedAt, err := gatewayRequestPriceImportServerTimeTx(tx)
		if err != nil {
			return err
		}
		identities := make([]gatewayAdministratorPriceIdentity, len(prepared))
		for _, item := range prepared {
			if item.ObservedAt.After(importedAt) {
				return fmt.Errorf("%w: item %d observed_at is in the future", ErrGatewayRequestPriceImportInvalid, item.index)
			}
			identity, err := resolveGatewayAdministratorPriceIdentityTx(tx, item)
			if err != nil {
				return fmt.Errorf("item %d: %w", item.index, gatewayRequestPriceImportIdentityLockError(err))
			}
			identities[item.index] = identity
		}
		for index := range prepared {
			if !prepared[index].ValidUntil.After(importedAt) {
				return fmt.Errorf("%w: item %d valid_until must be after the server import time", ErrGatewayRequestPriceImportInvalid, prepared[index].index)
			}
		}

		command := gatewayRequestPriceImportCommand{
			Actor: actor, IdempotencyKey: idempotencyKey, PayloadDigest: payloadDigest,
			PayloadDigestVersion: gatewayRequestPriceImportDigestVersion,
			ItemCount:            len(prepared), ImportedAt: importedAt, CreatedAt: importedAt,
		}
		if err := tx.Create(&command).Error; err != nil {
			return err
		}
		facts := make([]GatewayRequestPriceFact, len(prepared))
		for _, item := range prepared {
			identity := identities[item.index]
			validUntil := item.ValidUntil
			fact := GatewayRequestPriceFact{
				ChannelID:                       identity.channel.ID,
				ChannelAccountIdentityRevision:  identity.channel.UpstreamAccountIdentityRevision,
				CostUnitEpoch:                   item.ExpectedCostUnitEpoch,
				UpstreamGroupID:                 identity.group.ID,
				CanonicalGroupName:              identity.group.CanonicalGroupName,
				UpstreamGroupPresenceGeneration: identity.group.PresenceGeneration,
				GatewayUpstreamKeyID:            identity.key.ID,
				UpstreamKeyGeneration:           identity.key.KeyGeneration,
				ModelName:                       item.ModelName,
				Operation:                       item.Operation,
				Protocol:                        item.Protocol,
				InputUnitPrice:                  item.InputUnitPriceUSD,
				OutputUnitPrice:                 item.OutputUnitPriceUSD,
				CacheReadUnitPrice:              item.CacheReadUnitPriceUSD,
				CacheWriteUnitPrice:             item.CacheWriteUnitPriceUSD,
				OperationUnitPrice:              item.OperationUnitPriceUSD,
				Unit:                            item.unit,
				Currency:                        gatewayRequestPriceSupportedCurrency,
				Source:                          GatewayRequestPriceSourceAdministrator,
				ObservedAt:                      item.ObservedAt,
				ValidFrom:                       importedAt,
				ValidUntil:                      &validUntil,
				SchemaVersion:                   GatewayRequestPriceFactSchemaVersion,
			}
			if err := appendRequestPriceFactTx(tx, &fact, importedAt); err != nil {
				if errors.Is(err, ErrGatewayRequestPriceObservationStale) || errors.Is(err, ErrGatewayRequestPriceObservationConflict) {
					return fmt.Errorf("%w: item %d observation ordering: %v", ErrGatewayRequestPriceImportUnavailable, item.index, err)
				}
				return fmt.Errorf("item %d: %w", item.index, err)
			}
			facts[item.index] = fact
			receipt := gatewayRequestPriceImportItemReceipt{
				CommandID: command.ID, ItemIndex: item.index, PriceFactID: fact.ID, CreatedAt: importedAt,
			}
			if err := tx.Create(&receipt).Error; err != nil {
				return err
			}
		}
		if tx.Dialector != nil && tx.Dialector.Name() == string(DBDriverPostgres) {
			if err := tx.Exec(`SET CONSTRAINTS
				public.gateway_request_price_import_command_complete,
				public.gateway_request_price_administrator_receipt IMMEDIATE`).Error; err != nil {
				return err
			}
		}
		result = &GatewayRequestPriceImportResult{CommandID: command.ID, Items: facts}
		transactionBodyComplete = true
		return nil
	})
	if err != nil {
		transactionErr := gatewayRequestPriceImportTransactionError(err, transactionBodyComplete)
		if errors.Is(transactionErr, ErrGatewayRequestPriceImportCommitUnknown) && payloadDigest != "" {
			replay, replayErr := replayGatewayRequestPriceImportTx(g.db.WithContext(ctx), actor, idempotencyKey, payloadDigest, len(prepared))
			if replayErr == nil && replay != nil {
				return replay, nil
			}
			if errors.Is(replayErr, ErrGatewayRequestPriceImportIdempotencyConflict) {
				return nil, replayErr
			}
		}
		return nil, transactionErr
	}
	return result, nil
}

func gatewayRequestPriceImportServerTimeTx(tx *gorm.DB) (time.Time, error) {
	if tx == nil {
		return time.Time{}, errors.New("gateway storage transaction is required")
	}
	if tx.Dialector == nil || tx.Dialector.Name() != string(DBDriverPostgres) {
		return normalizeGatewayRequestPricingTime(time.Now()), nil
	}
	var now time.Time
	if err := tx.Raw("SELECT clock_timestamp()").Scan(&now).Error; err != nil {
		return time.Time{}, err
	}
	return normalizeGatewayRequestPricingTime(now), nil
}

func gatewayRequestPriceImportTransactionError(err error, transactionBodyComplete bool) error {
	if err == nil {
		return nil
	}
	if transactionBodyComplete {
		return fmt.Errorf("%w: %v", ErrGatewayRequestPriceImportCommitUnknown, err)
	}
	return err
}

func gatewayRequestPriceImportIdentityLockError(err error) error {
	if err == nil {
		return nil
	}
	var state interface{ SQLState() string }
	if errors.As(err, &state) && state.SQLState() == "55P03" {
		return fmt.Errorf("%w: target identity is changing; retry the import", ErrGatewayRequestPriceImportUnavailable)
	}
	return err
}

func gatewayRequestPriceImportPayloadDigest(prepared []preparedGatewayAdministratorPriceObservation) (string, error) {
	items := make([]gatewayRequestPriceImportDigestPayloadItem, len(prepared))
	for _, item := range prepared {
		if item.index < 0 || item.index >= len(items) || item.canonicalGroupName == "" {
			return "", errors.New("normalized item ordering is invalid")
		}
		items[item.index] = gatewayRequestPriceImportDigestPayloadItem{
			ChannelID: item.ChannelID, CanonicalGroupName: item.canonicalGroupName,
			ModelName: item.ModelName, Operation: item.Operation, Protocol: item.Protocol,
			ExpectedIdentityRevision: item.ExpectedIdentityRevision, ExpectedAdminRevision: item.ExpectedAdminRevision,
			ExpectedProvider: string(item.ExpectedProvider), ExpectedAPIOrigin: item.ExpectedAPIOrigin,
			ExpectedCostUnitEpoch: item.ExpectedCostUnitEpoch,
			ExpectedGroupID:       item.ExpectedGroupID, ExpectedGroupPresenceGeneration: item.ExpectedGroupPresenceGeneration,
			ExpectedUpstreamKeyID: item.ExpectedUpstreamKeyID, ExpectedUpstreamKeyGeneration: item.ExpectedUpstreamKeyGeneration,
			ObservedAt:        item.ObservedAt.Format(time.RFC3339Nano),
			InputUnitPriceUSD: item.InputUnitPriceUSD, OutputUnitPriceUSD: item.OutputUnitPriceUSD,
			CacheReadUnitPriceUSD: item.CacheReadUnitPriceUSD, CacheWriteUnitPriceUSD: item.CacheWriteUnitPriceUSD,
			OperationUnitPriceUSD: item.OperationUnitPriceUSD,
			ValidUntil:            item.ValidUntil.Format(time.RFC3339Nano),
		}
	}
	encoded, err := json.Marshal(gatewayRequestPriceImportDigestPayload{Version: gatewayRequestPriceImportDigestVersion, Items: items})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func replayGatewayRequestPriceImportTx(tx *gorm.DB, actor, idempotencyKey, payloadDigest string, itemCount int) (*GatewayRequestPriceImportResult, error) {
	var command gatewayRequestPriceImportCommand
	err := tx.Where("idempotency_key = ?", idempotencyKey).Take(&command).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if command.Actor != actor || command.PayloadDigest != payloadDigest ||
		command.PayloadDigestVersion != gatewayRequestPriceImportDigestVersion || command.ItemCount != itemCount {
		return nil, ErrGatewayRequestPriceImportIdempotencyConflict
	}
	var receipts []gatewayRequestPriceImportItemReceipt
	if err := tx.Where("command_id = ?", command.ID).Order("item_index ASC").Find(&receipts).Error; err != nil {
		return nil, err
	}
	if len(receipts) != command.ItemCount {
		return nil, errors.New("request price import receipt is incomplete")
	}
	factIDs := make([]uint64, len(receipts))
	for index, receipt := range receipts {
		if receipt.ItemIndex != index || receipt.PriceFactID == 0 {
			return nil, errors.New("request price import receipt ordering is invalid")
		}
		factIDs[index] = receipt.PriceFactID
	}
	var storedFacts []GatewayRequestPriceFact
	if err := tx.Where("id IN ?", factIDs).Find(&storedFacts).Error; err != nil {
		return nil, err
	}
	byID := make(map[uint64]GatewayRequestPriceFact, len(storedFacts))
	for _, fact := range storedFacts {
		byID[fact.ID] = fact
	}
	facts := make([]GatewayRequestPriceFact, len(factIDs))
	for index, factID := range factIDs {
		fact, ok := byID[factID]
		if !ok {
			return nil, errors.New("request price import receipt references a missing fact")
		}
		facts[index] = fact
	}
	return &GatewayRequestPriceImportResult{
		CommandID: command.ID, Items: facts, IdempotentReplay: true,
	}, nil
}

type gatewayAdministratorPriceIdentity struct {
	channel Channel
	group   GatewayUpstreamGroup
	key     GatewayUpstreamKey
}

// AdministratorRequestPriceTarget returns a usable current identity fence for
// one model. POST import still revalidates every field under lock.
func (g *Gateway) AdministratorRequestPriceTarget(ctx context.Context, channelID uint, groupName, modelName string) (*GatewayAdministratorPriceTarget, error) {
	if g == nil || g.db == nil {
		return nil, errors.New("gateway storage is required")
	}
	if channelID == 0 {
		return nil, fmt.Errorf("%w: channel_id is required", ErrGatewayRequestPriceImportInvalid)
	}
	groupName, err := validateUpstreamGroupName(groupName)
	if err != nil || strings.ContainsRune(groupName, '\x00') {
		return nil, fmt.Errorf("%w: group_name is invalid", ErrGatewayRequestPriceImportInvalid)
	}
	modelName = strings.TrimSpace(modelName)
	if modelName == "" || strings.ContainsRune(modelName, '\x00') || !utf8.ValidString(modelName) || utf8.RuneCountInString(modelName) > 256 {
		return nil, fmt.Errorf("%w: model_name is invalid", ErrGatewayRequestPriceImportInvalid)
	}

	var target *GatewayAdministratorPriceTarget
	err = g.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		capturedAt, err := gatewayRequestPriceImportServerTimeTx(tx)
		if err != nil {
			return err
		}
		canonicalGroupName, err := canonicalUpstreamGroupName(tx, groupName)
		if err != nil {
			return fmt.Errorf("%w: group_name is invalid", ErrGatewayRequestPriceImportInvalid)
		}
		channel, err := loadGatewayRequestPriceChannelIdentityTx(tx, channelID)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("%w: channel %d does not exist", ErrGatewayRequestPriceImportUnavailable, channelID)
			}
			return err
		}
		if channel.CostUnitEpoch == 0 {
			return fmt.Errorf("%w: channel cost-unit epoch is unavailable", ErrGatewayRequestPriceImportUnavailable)
		}
		origin, err := gatewayRequestPriceChannelAPIOrigin(channel)
		if err != nil {
			return fmt.Errorf("%w: channel API origin is unavailable", ErrGatewayRequestPriceImportUnavailable)
		}
		identity, err := loadGatewayAdministratorPriceRouteIdentityTx(tx, channel, groupName, canonicalGroupName, modelName, capturedAt)
		if err != nil {
			return err
		}
		target = &GatewayAdministratorPriceTarget{
			ChannelID: channel.ID, GroupName: identity.group.CanonicalGroupName, ModelName: modelName,
			ExpectedIdentityRevision: channel.UpstreamAccountIdentityRevision,
			ExpectedAdminRevision:    channel.AdminRevision, ExpectedProvider: channel.Provider,
			ExpectedAPIOrigin: origin, ExpectedCostUnitEpoch: channel.CostUnitEpoch,
			ExpectedGroupID: identity.group.ID, ExpectedGroupPresenceGeneration: identity.group.PresenceGeneration,
			ExpectedUpstreamKeyID: identity.key.ID, ExpectedUpstreamKeyGeneration: identity.key.KeyGeneration,
			CapturedAt: capturedAt,
		}
		return nil
	})
	return target, gatewayRequestPriceImportIdentityLockError(err)
}

func resolveGatewayAdministratorPriceIdentityTx(tx *gorm.DB, item preparedGatewayAdministratorPriceObservation) (gatewayAdministratorPriceIdentity, error) {
	var identity gatewayAdministratorPriceIdentity
	channel, err := loadGatewayRequestPriceChannelIdentityTx(tx, item.ChannelID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return identity, fmt.Errorf("%w: channel %d does not exist", ErrGatewayRequestPriceImportUnavailable, item.ChannelID)
		}
		return identity, err
	}
	if channel.UpstreamAccountIdentityRevision != item.ExpectedIdentityRevision || channel.AdminRevision != item.ExpectedAdminRevision ||
		channel.Provider != item.ExpectedProvider || channel.CostUnitEpoch != item.ExpectedCostUnitEpoch {
		return identity, fmt.Errorf("%w: channel price observation fence changed", ErrGatewayRequestPriceImportUnavailable)
	}
	configuredOrigin, err := gatewayRequestPriceChannelAPIOrigin(channel)
	if err != nil || configuredOrigin != item.ExpectedAPIOrigin {
		return identity, fmt.Errorf("%w: channel API origin changed", ErrGatewayRequestPriceImportUnavailable)
	}
	identity, err = loadGatewayAdministratorPriceRouteIdentityTx(tx, channel, item.GroupName, item.canonicalGroupName, item.ModelName, item.ObservedAt)
	if err != nil {
		return identity, err
	}
	if identity.group.ID != item.ExpectedGroupID || identity.group.PresenceGeneration != item.ExpectedGroupPresenceGeneration {
		return identity, fmt.Errorf("%w: upstream group price observation fence changed", ErrGatewayRequestPriceImportUnavailable)
	}
	if identity.key.ID != item.ExpectedUpstreamKeyID || identity.key.KeyGeneration != item.ExpectedUpstreamKeyGeneration {
		return identity, fmt.Errorf("%w: upstream key price observation fence changed", ErrGatewayRequestPriceImportUnavailable)
	}
	return identity, nil
}

func gatewayRequestPriceChannelAPIOrigin(channel gatewayRequestPriceChannelIdentity) (string, error) {
	configuredOrigin := strings.TrimSpace(channel.APIURL)
	if configuredOrigin == "" {
		configuredOrigin = channel.SiteURL
	}
	return normalizeUpstreamAccountAPIOrigin(configuredOrigin)
}

func loadGatewayAdministratorPriceRouteIdentityTx(tx *gorm.DB, channel gatewayRequestPriceChannelIdentity, groupName, canonicalGroupName, modelName string, observedAt time.Time) (gatewayAdministratorPriceIdentity, error) {
	var identity gatewayAdministratorPriceIdentity
	if err := requireGatewayRequestPriceAccountBindingTx(tx, channel, observedAt); err != nil {
		return identity, fmt.Errorf("%w: %v", ErrGatewayRequestPriceImportUnavailable, err)
	}
	identity.channel = Channel{
		ID: channel.ID, Type: channel.Provider, SiteURL: channel.SiteURL, APIURL: channel.APIURL,
		UpstreamAccountIdentityRevision: channel.UpstreamAccountIdentityRevision, AdminRevision: channel.AdminRevision,
	}
	if err := tx.Clauses(clause.Locking{Strength: "SHARE", Options: "NOWAIT"}).
		Select("id", "channel_id", "canonical_group_name", "present", "presence_generation", "discovered_at", "model_ids", "models_synced_at").
		Where("channel_id = ? AND canonical_group_name = ? AND present = ?", channel.ID, canonicalGroupName, true).
		First(&identity.group).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return identity, fmt.Errorf("%w: upstream group %q is not currently present", ErrGatewayRequestPriceImportUnavailable, groupName)
		}
		return identity, err
	}
	if identity.group.PresenceGeneration == 0 || identity.group.DiscoveredAt.After(observedAt) || identity.group.ModelsSyncedAt == nil ||
		identity.group.ModelsSyncedAt.Before(identity.group.DiscoveredAt) || identity.group.ModelsSyncedAt.After(observedAt) ||
		!containsString(identity.group.ModelIDs, modelName) {
		return identity, fmt.Errorf("%w: model %q is not in the synchronized group identity at observed_at", ErrGatewayRequestPriceImportUnavailable, modelName)
	}
	if err := tx.Clauses(clause.Locking{Strength: "SHARE", Options: "NOWAIT"}).
		Select("id", "channel_id", "group_name", "key_generation", "enabled", "provisioning", "api_key_cipher", "model_ids", "models_synced_at", "models_error", "created_at").
		Where("channel_id = ? AND lower(btrim(group_name)) = ? AND enabled = ? AND provisioning = ?", channel.ID, canonicalGroupName, true, false).
		Order("models_synced_at DESC NULLS LAST, id DESC").
		First(&identity.key).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return identity, fmt.Errorf("%w: upstream group %q has no enabled current key", ErrGatewayRequestPriceImportUnavailable, groupName)
		}
		return identity, err
	}
	if identity.key.KeyGeneration == 0 || strings.TrimSpace(identity.key.APIKeyCipher) == "" ||
		identity.key.CreatedAt.After(observedAt) || identity.key.ModelsSyncedAt == nil ||
		identity.key.ModelsSyncedAt.Before(identity.group.DiscoveredAt) || identity.key.ModelsSyncedAt.After(observedAt) ||
		strings.TrimSpace(identity.key.ModelsError) != "" || !gatewayPriceModelCSVContains(identity.key.ModelIDs, modelName) {
		return identity, fmt.Errorf("%w: upstream group %q key is not synchronized for model %q at observed_at", ErrGatewayRequestPriceImportUnavailable, groupName, modelName)
	}
	return identity, nil
}

func normalizeGatewayAdministratorPriceObservation(item *GatewayAdministratorPriceObservation) (string, error) {
	if item == nil {
		return "", errors.New("observation is required")
	}
	if item.ChannelID == 0 {
		return "", errors.New("channel_id is required")
	}
	groupName, err := validateUpstreamGroupName(item.GroupName)
	if err != nil {
		return "", fmt.Errorf("group_name: %w", err)
	}
	if strings.ContainsRune(groupName, '\x00') {
		return "", errors.New("group_name is invalid")
	}
	item.GroupName = groupName
	item.ModelName = strings.TrimSpace(item.ModelName)
	if item.ModelName == "" || strings.ContainsRune(item.ModelName, '\x00') || !utf8.ValidString(item.ModelName) || utf8.RuneCountInString(item.ModelName) > 256 {
		return "", errors.New("model_name is invalid")
	}
	item.Operation = strings.ToLower(strings.TrimSpace(item.Operation))
	item.Protocol = strings.ToLower(strings.TrimSpace(item.Protocol))
	if !gatewayRequestPricingProtocolSupported(item.Operation, item.Protocol) {
		return "", errors.New("operation or protocol is unsupported")
	}
	if item.ExpectedIdentityRevision == 0 || item.ExpectedAdminRevision == 0 || item.ExpectedCostUnitEpoch == 0 || item.ExpectedGroupID == 0 ||
		item.ExpectedGroupPresenceGeneration == 0 || item.ExpectedUpstreamKeyID == 0 || item.ExpectedUpstreamKeyGeneration == 0 {
		return "", errors.New("price observation identity fence is incomplete")
	}
	item.ExpectedProvider = ChannelType(strings.ToLower(strings.TrimSpace(string(item.ExpectedProvider))))
	if item.ExpectedProvider == "" {
		return "", errors.New("expected_provider is required")
	}
	expectedOrigin, err := normalizeUpstreamAccountAPIOrigin(item.ExpectedAPIOrigin)
	if err != nil {
		return "", fmt.Errorf("expected_api_origin: %w", err)
	}
	item.ExpectedAPIOrigin = expectedOrigin
	if item.ObservedAt.IsZero() {
		return "", errors.New("observed_at is required")
	}
	item.ObservedAt = normalizeGatewayRequestPricingTime(item.ObservedAt)
	if item.ValidUntil.IsZero() {
		return "", errors.New("valid_until is required")
	}
	item.ValidUntil = normalizeGatewayRequestPricingTime(item.ValidUntil)
	for _, priceField := range []**float64{
		&item.InputUnitPriceUSD, &item.OutputUnitPriceUSD, &item.CacheReadUnitPriceUSD,
		&item.CacheWriteUnitPriceUSD, &item.OperationUnitPriceUSD,
	} {
		if *priceField == nil {
			continue
		}
		value := **priceField
		if value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
			return "", errors.New("unit prices must be finite and non-negative")
		}
		// IEEE negative zero is the same monetary value as positive zero. Keep
		// equivalent JSON payloads on one canonical idempotency digest.
		if value == 0 {
			value = 0
		}
		*priceField = &value
	}
	switch item.Operation {
	case GatewayPricingOperationTextGeneration:
		if item.OperationUnitPriceUSD != nil ||
			item.InputUnitPriceUSD == nil && item.OutputUnitPriceUSD == nil && item.CacheReadUnitPriceUSD == nil && item.CacheWriteUnitPriceUSD == nil {
			return "", errors.New("text generation requires at least one token price and forbids an operation price")
		}
		return GatewayPricingUnitPerMillionTokens, nil
	case GatewayPricingOperationEmbedding:
		if item.InputUnitPriceUSD == nil || item.OutputUnitPriceUSD != nil || item.CacheReadUnitPriceUSD != nil || item.CacheWriteUnitPriceUSD != nil || item.OperationUnitPriceUSD != nil {
			return "", errors.New("embedding requires exactly one input token price")
		}
		return GatewayPricingUnitPerMillionTokens, nil
	case GatewayImageOperationGeneration:
		if item.OperationUnitPriceUSD == nil || item.InputUnitPriceUSD != nil || item.OutputUnitPriceUSD != nil || item.CacheReadUnitPriceUSD != nil || item.CacheWriteUnitPriceUSD != nil {
			return "", errors.New("image generation requires exactly one operation price")
		}
		return GatewayPricingUnitPerOperation, nil
	default:
		return "", errors.New("operation is unsupported")
	}
}

func gatewayPriceModelCSVContains(raw, model string) bool {
	for _, item := range strings.Split(raw, ",") {
		if strings.TrimSpace(item) == model {
			return true
		}
	}
	return false
}
