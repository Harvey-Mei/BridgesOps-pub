package storage

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"
)

func gatewayAdministratorPriceObservationForTest(channel *Channel, group *GatewayUpstreamGroup, key *GatewayUpstreamKey, observedAt time.Time, item GatewayAdministratorPriceObservation) GatewayAdministratorPriceObservation {
	item.ExpectedIdentityRevision = channel.UpstreamAccountIdentityRevision
	item.ExpectedAdminRevision = channel.AdminRevision
	item.ExpectedProvider = channel.Type
	item.ExpectedCostUnitEpoch = 1
	item.ExpectedAPIOrigin = channel.SiteURL
	if strings.TrimSpace(channel.APIURL) != "" {
		item.ExpectedAPIOrigin = channel.APIURL
	}
	item.ExpectedGroupID = group.ID
	item.ExpectedGroupPresenceGeneration = group.PresenceGeneration
	item.ExpectedUpstreamKeyID = key.ID
	item.ExpectedUpstreamKeyGeneration = key.KeyGeneration
	item.ObservedAt = observedAt
	return item
}

func TestNormalizeGatewayAdministratorPriceObservationFailsClosed(t *testing.T) {
	zero := 0.0
	negativeZero := math.Copysign(0, -1)
	one := 1.0
	validUntil := time.Now().UTC().Add(time.Hour)
	tests := []struct {
		name     string
		item     GatewayAdministratorPriceObservation
		wantUnit string
		wantErr  string
	}{
		{
			name: "complete text prices preserve explicit zero",
			item: GatewayAdministratorPriceObservation{
				ChannelID: 1, GroupName: " Primary ", ModelName: " model-a ",
				Operation: " TEXT_GENERATION ", Protocol: " OPENAI_RESPONSES ",
				InputUnitPriceUSD: &zero, OutputUnitPriceUSD: &one,
				CacheReadUnitPriceUSD: &zero, CacheWriteUnitPriceUSD: &one,
				ValidUntil: validUntil,
			},
			wantUnit: GatewayPricingUnitPerMillionTokens,
		},
		{
			name: "text prices may be partially observed",
			item: GatewayAdministratorPriceObservation{
				ChannelID: 1, GroupName: "primary", ModelName: "model-a",
				Operation: GatewayPricingOperationTextGeneration, Protocol: GatewayPricingProtocolOpenAIResponses,
				InputUnitPriceUSD: &one, OutputUnitPriceUSD: &one, CacheReadUnitPriceUSD: &zero,
				ValidUntil: validUntil,
			},
			wantUnit: GatewayPricingUnitPerMillionTokens,
		},
		{
			name: "text prices all missing",
			item: GatewayAdministratorPriceObservation{
				ChannelID: 1, GroupName: "primary", ModelName: "model-a",
				Operation: GatewayPricingOperationTextGeneration, Protocol: GatewayPricingProtocolOpenAIResponses,
				ValidUntil: validUntil,
			},
			wantErr: "at least one token price",
		},
		{
			name: "embedding input price",
			item: GatewayAdministratorPriceObservation{
				ChannelID: 1, GroupName: "primary", ModelName: "model-a",
				Operation: GatewayPricingOperationEmbedding, Protocol: GatewayPricingProtocolOpenAIEmbeddings,
				InputUnitPriceUSD: &zero, ValidUntil: validUntil,
			},
			wantUnit: GatewayPricingUnitPerMillionTokens,
		},
		{
			name: "embedding negative zero is canonicalized",
			item: GatewayAdministratorPriceObservation{
				ChannelID: 1, GroupName: "primary", ModelName: "model-a",
				Operation: GatewayPricingOperationEmbedding, Protocol: GatewayPricingProtocolOpenAIEmbeddings,
				InputUnitPriceUSD: &negativeZero, ValidUntil: validUntil,
			},
			wantUnit: GatewayPricingUnitPerMillionTokens,
		},
		{
			name: "embedding has output price",
			item: GatewayAdministratorPriceObservation{
				ChannelID: 1, GroupName: "primary", ModelName: "model-a",
				Operation: GatewayPricingOperationEmbedding, Protocol: GatewayPricingProtocolOpenAIEmbeddings,
				InputUnitPriceUSD: &one, OutputUnitPriceUSD: &zero, ValidUntil: validUntil,
			},
			wantErr: "exactly one input",
		},
		{
			name: "group contains postgres nul",
			item: GatewayAdministratorPriceObservation{
				ChannelID: 1, GroupName: "pri\x00mary", ModelName: "model-a",
				Operation: GatewayPricingOperationEmbedding, Protocol: GatewayPricingProtocolOpenAIEmbeddings,
				InputUnitPriceUSD: &one, ValidUntil: validUntil,
			},
			wantErr: "group_name is invalid",
		},
		{
			name: "model contains postgres nul",
			item: GatewayAdministratorPriceObservation{
				ChannelID: 1, GroupName: "primary", ModelName: "model\x00a",
				Operation: GatewayPricingOperationEmbedding, Protocol: GatewayPricingProtocolOpenAIEmbeddings,
				InputUnitPriceUSD: &one, ValidUntil: validUntil,
			},
			wantErr: "model_name is invalid",
		},
		{
			name: "image operation price",
			item: GatewayAdministratorPriceObservation{
				ChannelID: 1, GroupName: "primary", ModelName: "model-a",
				Operation: GatewayImageOperationGeneration, Protocol: GatewayImageProtocolImages,
				OperationUnitPriceUSD: &zero, ValidUntil: validUntil,
			},
			wantUnit: GatewayPricingUnitPerOperation,
		},
		{
			name: "image has token price",
			item: GatewayAdministratorPriceObservation{
				ChannelID: 1, GroupName: "primary", ModelName: "model-a",
				Operation: GatewayImageOperationGeneration, Protocol: GatewayImageProtocolImages,
				InputUnitPriceUSD: &zero, OperationUnitPriceUSD: &one, ValidUntil: validUntil,
			},
			wantErr: "exactly one operation",
		},
		{
			name: "valid until missing",
			item: GatewayAdministratorPriceObservation{
				ChannelID: 1, GroupName: "primary", ModelName: "model-a",
				Operation: GatewayPricingOperationEmbedding, Protocol: GatewayPricingProtocolOpenAIEmbeddings,
				InputUnitPriceUSD: &one,
			},
			wantErr: "valid_until is required",
		},
		{
			name: "non finite price",
			item: GatewayAdministratorPriceObservation{
				ChannelID: 1, GroupName: "primary", ModelName: "model-a",
				Operation: GatewayPricingOperationEmbedding, Protocol: GatewayPricingProtocolOpenAIEmbeddings,
				InputUnitPriceUSD: func() *float64 { value := math.NaN(); return &value }(), ValidUntil: validUntil,
			},
			wantErr: "finite and non-negative",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.item.ExpectedIdentityRevision = 1
			test.item.ExpectedAdminRevision = 1
			test.item.ExpectedProvider = ChannelTypeSub2API
			test.item.ExpectedCostUnitEpoch = 1
			test.item.ExpectedAPIOrigin = "https://price.invalid"
			test.item.ExpectedGroupID = 1
			test.item.ExpectedGroupPresenceGeneration = 1
			test.item.ExpectedUpstreamKeyID = 1
			test.item.ExpectedUpstreamKeyGeneration = 1
			test.item.ObservedAt = validUntil.Add(-time.Hour)
			unit, err := normalizeGatewayAdministratorPriceObservation(&test.item)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("normalize error = %v, want %q", err, test.wantErr)
				}
				return
			}
			if err != nil || unit != test.wantUnit {
				t.Fatalf("normalize unit=%q err=%v", unit, err)
			}
			if strings.TrimSpace(test.item.GroupName) == "" || test.item.ModelName != "model-a" {
				t.Fatalf("normalized observation = %+v", test.item)
			}
			if test.name == "complete text prices preserve explicit zero" &&
				(test.item.GroupName != "Primary" || test.item.Operation != GatewayPricingOperationTextGeneration ||
					test.item.Protocol != GatewayPricingProtocolOpenAIResponses || test.item.InputUnitPriceUSD == nil || *test.item.InputUnitPriceUSD != 0) {
				t.Fatalf("normalized text observation = %+v", test.item)
			}
			if test.name == "embedding negative zero is canonicalized" &&
				(test.item.InputUnitPriceUSD == nil || math.Signbit(*test.item.InputUnitPriceUSD)) {
				t.Fatalf("negative zero was not canonicalized: %+v", test.item)
			}
			if test.name == "embedding negative zero is canonicalized" && !math.Signbit(negativeZero) {
				t.Fatal("normalization mutated the caller's negative-zero value")
			}
		})
	}
}

func TestGatewayRequestPriceImportPayloadDigestUsesCanonicalOriginalOrder(t *testing.T) {
	firstPrice, secondPrice := 1.25, 2.5
	validUntil := time.Date(2030, 1, 2, 3, 4, 5, 678901000, time.UTC)
	first := preparedGatewayAdministratorPriceObservation{
		GatewayAdministratorPriceObservation: GatewayAdministratorPriceObservation{
			ChannelID: 2, GroupName: "Primary", ModelName: "model-a",
			Operation: GatewayPricingOperationEmbedding, Protocol: GatewayPricingProtocolOpenAIEmbeddings,
			InputUnitPriceUSD: &firstPrice, ValidUntil: validUntil,
		},
		index: 0, canonicalGroupName: "primary", unit: GatewayPricingUnitPerMillionTokens,
	}
	second := preparedGatewayAdministratorPriceObservation{
		GatewayAdministratorPriceObservation: GatewayAdministratorPriceObservation{
			ChannelID: 1, GroupName: "Secondary", ModelName: "model-b",
			Operation: GatewayImageOperationGeneration, Protocol: GatewayImageProtocolImages,
			OperationUnitPriceUSD: &secondPrice, ValidUntil: validUntil.Add(time.Hour),
		},
		index: 1, canonicalGroupName: "secondary", unit: GatewayPricingUnitPerOperation,
	}

	digest, err := gatewayRequestPriceImportPayloadDigest([]preparedGatewayAdministratorPriceObservation{first, second})
	if err != nil {
		t.Fatalf("canonical payload digest: %v", err)
	}
	reorderedInternal, err := gatewayRequestPriceImportPayloadDigest([]preparedGatewayAdministratorPriceObservation{second, first})
	if err != nil {
		t.Fatalf("reordered internal payload digest: %v", err)
	}
	if len(digest) != sha256.Size*2 || digest != reorderedInternal {
		t.Fatalf("canonical digest=%q reordered=%q", digest, reorderedInternal)
	}

	first.index, second.index = 1, 0
	reorderedRequest, err := gatewayRequestPriceImportPayloadDigest([]preparedGatewayAdministratorPriceObservation{first, second})
	if err != nil {
		t.Fatalf("reordered request payload digest: %v", err)
	}
	if reorderedRequest == digest {
		t.Fatalf("request item order was omitted from payload digest %q", digest)
	}
}

func TestImportAdministratorRequestPriceFactsRejectsInvalidActorBeforeTransaction(t *testing.T) {
	store := &Gateway{db: &gorm.DB{}}
	if _, err := store.ImportAdministratorRequestPriceFacts(context.Background(), "admin\x00user", "price-import-key", []GatewayAdministratorPriceObservation{{}}); !errors.Is(err, ErrGatewayRequestPriceImportInvalid) {
		t.Fatalf("invalid administrator actor error = %v", err)
	}
}

func TestGatewayRequestPriceImportTransactionErrorMarksOnlyCommitFailuresUnknown(t *testing.T) {
	cause := errors.New("connection lost")
	if got := gatewayRequestPriceImportTransactionError(cause, false); !errors.Is(got, cause) || errors.Is(got, ErrGatewayRequestPriceImportCommitUnknown) {
		t.Fatalf("pre-commit error = %v", got)
	}
	if got := gatewayRequestPriceImportTransactionError(cause, true); !errors.Is(got, ErrGatewayRequestPriceImportCommitUnknown) || !strings.Contains(got.Error(), cause.Error()) {
		t.Fatalf("commit error = %v", got)
	}
}

type gatewayRequestPriceImportTestSQLStateError struct{ state string }

func (err gatewayRequestPriceImportTestSQLStateError) Error() string {
	return "database lock unavailable"
}
func (err gatewayRequestPriceImportTestSQLStateError) SQLState() string { return err.state }

func TestGatewayRequestPriceImportIdentityLockErrorMapsOnlyLockContention(t *testing.T) {
	lockErr := gatewayRequestPriceImportIdentityLockError(gatewayRequestPriceImportTestSQLStateError{state: "55P03"})
	if !errors.Is(lockErr, ErrGatewayRequestPriceImportUnavailable) {
		t.Fatalf("lock contention error = %v", lockErr)
	}
	cause := errors.New("query failed")
	if got := gatewayRequestPriceImportIdentityLockError(cause); got != cause {
		t.Fatalf("ordinary identity error = %v, want original %v", got, cause)
	}
}

func TestImportAdministratorRequestPriceFactsBindsCurrentIdentity(t *testing.T) {
	if strings.TrimSpace(os.Getenv("BRIDGEOPS_TEST_POSTGRES_DSN")) == "" {
		t.Skip("dedicated PostgreSQL test DSN is not configured")
	}
	db := openTestDB(t)
	store, channel, group, key := gatewayAdministratorPriceImportFixture(t, db, "administrator-price-import")
	inputPrice, outputPrice, cacheReadPrice, cacheWritePrice := 1.0, 2.0, 0.0, 3.0
	operationPrice := 0.04
	observedAt := time.Now().UTC().Add(-time.Minute)
	validUntil := time.Now().UTC().Add(2 * time.Hour)
	result, err := store.ImportAdministratorRequestPriceFacts(context.Background(), "admin-user", "administrator-price-import-1", []GatewayAdministratorPriceObservation{
		gatewayAdministratorPriceObservationForTest(channel, group, key, observedAt, GatewayAdministratorPriceObservation{
			ChannelID: channel.ID, GroupName: " PRIMARY ", ModelName: "model-a",
			Operation: GatewayPricingOperationTextGeneration, Protocol: GatewayPricingProtocolOpenAIResponses,
			InputUnitPriceUSD: &inputPrice, OutputUnitPriceUSD: &outputPrice,
			CacheReadUnitPriceUSD: &cacheReadPrice, CacheWriteUnitPriceUSD: &cacheWritePrice,
			ValidUntil: validUntil,
		}),
		gatewayAdministratorPriceObservationForTest(channel, group, key, observedAt, GatewayAdministratorPriceObservation{
			ChannelID: channel.ID, GroupName: "primary", ModelName: "model-b",
			Operation: GatewayImageOperationGeneration, Protocol: GatewayImageProtocolImages,
			OperationUnitPriceUSD: &operationPrice, ValidUntil: validUntil,
		}),
	})
	if err != nil {
		t.Fatalf("import administrator prices: %v", err)
	}
	facts := result.Items
	if result.CommandID == 0 || result.IdempotentReplay {
		t.Fatalf("new import result = %+v", result)
	}
	if len(facts) != 2 || facts[0].ID == 0 || facts[1].ID == 0 || facts[0].ValidFrom != facts[1].ValidFrom {
		t.Fatalf("imported facts = %+v", facts)
	}
	for _, fact := range facts {
		if fact.ChannelID != channel.ID || fact.ChannelAccountIdentityRevision != channel.UpstreamAccountIdentityRevision ||
			fact.CostUnitEpoch != 1 ||
			fact.UpstreamGroupID != group.ID || fact.CanonicalGroupName != group.CanonicalGroupName ||
			fact.UpstreamGroupPresenceGeneration != group.PresenceGeneration || fact.GatewayUpstreamKeyID != key.ID ||
			fact.UpstreamKeyGeneration != key.KeyGeneration || fact.Source != GatewayRequestPriceSourceAdministrator ||
			fact.Currency != "USD" || !fact.ObservedAt.Equal(observedAt.Truncate(time.Microsecond)) || !fact.ValidFrom.After(fact.ObservedAt) ||
			fact.ValidUntil == nil || !fact.ValidUntil.Equal(validUntil.Truncate(time.Microsecond)) ||
			fact.FactVersion != 1 || fact.SchemaVersion != GatewayRequestPriceFactSchemaVersion || len(fact.Checksum) != 64 {
			t.Fatalf("fact did not bind server identity = %+v", fact)
		}
	}
	if facts[0].CacheReadUnitPrice == nil || *facts[0].CacheReadUnitPrice != 0 {
		t.Fatalf("explicit zero cache price was lost: %+v", facts[0])
	}
}

func TestImportAdministratorRequestPriceFactsRejectsEveryStaleTargetFence(t *testing.T) {
	if strings.TrimSpace(os.Getenv("BRIDGEOPS_TEST_POSTGRES_DSN")) == "" {
		t.Skip("dedicated PostgreSQL test DSN is not configured")
	}
	db := openTestDB(t)
	store, channel, group, key := gatewayAdministratorPriceImportFixture(t, db, "administrator-price-stale-fences")
	price := 1.0
	base := gatewayAdministratorPriceObservationForTest(channel, group, key, time.Now().UTC().Add(-time.Minute), GatewayAdministratorPriceObservation{
		ChannelID: channel.ID, GroupName: "primary", ModelName: "model-a",
		Operation: GatewayPricingOperationEmbedding, Protocol: GatewayPricingProtocolOpenAIEmbeddings,
		InputUnitPriceUSD: &price, ValidUntil: time.Now().UTC().Add(time.Hour),
	})
	tests := []struct {
		name   string
		mutate func(*GatewayAdministratorPriceObservation)
	}{
		{name: "identity revision", mutate: func(item *GatewayAdministratorPriceObservation) { item.ExpectedIdentityRevision++ }},
		{name: "admin revision", mutate: func(item *GatewayAdministratorPriceObservation) { item.ExpectedAdminRevision++ }},
		{name: "provider", mutate: func(item *GatewayAdministratorPriceObservation) { item.ExpectedProvider = ChannelTypeNewAPI }},
		{name: "api origin", mutate: func(item *GatewayAdministratorPriceObservation) { item.ExpectedAPIOrigin = "https://other.invalid" }},
		{name: "cost unit epoch", mutate: func(item *GatewayAdministratorPriceObservation) { item.ExpectedCostUnitEpoch++ }},
		{name: "group id", mutate: func(item *GatewayAdministratorPriceObservation) { item.ExpectedGroupID++ }},
		{name: "group generation", mutate: func(item *GatewayAdministratorPriceObservation) { item.ExpectedGroupPresenceGeneration++ }},
		{name: "key id", mutate: func(item *GatewayAdministratorPriceObservation) { item.ExpectedUpstreamKeyID++ }},
		{name: "key generation", mutate: func(item *GatewayAdministratorPriceObservation) { item.ExpectedUpstreamKeyGeneration++ }},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			item := base
			test.mutate(&item)
			_, err := store.ImportAdministratorRequestPriceFacts(context.Background(), "admin-user", fmt.Sprintf("administrator-price-stale-fence-%d", index), []GatewayAdministratorPriceObservation{item})
			if !errors.Is(err, ErrGatewayRequestPriceImportUnavailable) {
				t.Fatalf("stale %s fence error = %v", test.name, err)
			}
		})
	}
	var count int64
	if err := db.Model(&GatewayRequestPriceFact{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("stale fences persisted facts = %d, err=%v", count, err)
	}
}

func TestImportAdministratorRequestPriceFactsRejectsUnprovenOrChangedAccount(t *testing.T) {
	if strings.TrimSpace(os.Getenv("BRIDGEOPS_TEST_POSTGRES_DSN")) == "" {
		t.Skip("dedicated PostgreSQL test DSN is not configured")
	}
	t.Run("missing authenticated binding", func(t *testing.T) {
		db := openTestDB(t)
		store, channel, group, key := gatewayAdministratorPriceImportFixture(t, db, "administrator-price-no-binding")
		if err := db.Where("channel_id = ?", channel.ID).Delete(&GatewayChannelUpstreamAccountBinding{}).Error; err != nil {
			t.Fatalf("remove account binding: %v", err)
		}
		if target, err := store.AdministratorRequestPriceTarget(context.Background(), channel.ID, "primary", "model-a"); target != nil || !errors.Is(err, ErrGatewayRequestPriceImportUnavailable) {
			t.Fatalf("unbound administrator price target=%+v err=%v", target, err)
		}
		price := 1.0
		item := gatewayAdministratorPriceObservationForTest(channel, group, key, time.Now().UTC().Add(-time.Minute), GatewayAdministratorPriceObservation{
			ChannelID: channel.ID, GroupName: "primary", ModelName: "model-a",
			Operation: GatewayPricingOperationEmbedding, Protocol: GatewayPricingProtocolOpenAIEmbeddings,
			InputUnitPriceUSD: &price, ValidUntil: time.Now().UTC().Add(time.Hour),
		})
		if _, err := store.ImportAdministratorRequestPriceFacts(context.Background(), "admin-user", "administrator-price-no-binding", []GatewayAdministratorPriceObservation{item}); !errors.Is(err, ErrGatewayRequestPriceImportUnavailable) {
			t.Fatalf("missing account binding error = %v", err)
		}
	})

	t.Run("binding observed after price", func(t *testing.T) {
		db := openTestDB(t)
		store, channel, group, key := gatewayAdministratorPriceImportFixture(t, db, "administrator-price-late-binding")
		// The fixture binds the account one minute before group discovery. Place
		// the price before that first binding so this remains valid after 000059
		// makes binding audits append-only.
		priceObservedAt := group.DiscoveredAt.Add(-2 * time.Minute)
		price := 1.0
		item := gatewayAdministratorPriceObservationForTest(channel, group, key, priceObservedAt, GatewayAdministratorPriceObservation{
			ChannelID: channel.ID, GroupName: "primary", ModelName: "model-a",
			Operation: GatewayPricingOperationEmbedding, Protocol: GatewayPricingProtocolOpenAIEmbeddings,
			InputUnitPriceUSD: &price, ValidUntil: time.Now().UTC().Add(time.Hour),
		})
		if _, err := store.ImportAdministratorRequestPriceFacts(context.Background(), "admin-user", "administrator-price-late-binding", []GatewayAdministratorPriceObservation{item}); !errors.Is(err, ErrGatewayRequestPriceImportUnavailable) || !strings.Contains(err.Error(), "not bound at observed_at") {
			t.Fatalf("late account binding error = %v", err)
		}
	})

	t.Run("delayed account A observation after switch to B", func(t *testing.T) {
		db := openTestDB(t)
		store, channel, group, key := gatewayAdministratorPriceImportFixture(t, db, "administrator-price-account-switch")
		price := 1.0
		oldItem := gatewayAdministratorPriceObservationForTest(channel, group, key, time.Now().UTC().Add(-time.Minute), GatewayAdministratorPriceObservation{
			ChannelID: channel.ID, GroupName: "primary", ModelName: "model-a",
			Operation: GatewayPricingOperationEmbedding, Protocol: GatewayPricingProtocolOpenAIEmbeddings,
			InputUnitPriceUSD: &price, ValidUntil: time.Now().UTC().Add(time.Hour),
		})
		if _, err := NewChannels(db).ObserveUpstreamAccountIdentity(UpstreamAccountIdentityObservation{
			ChannelID: channel.ID, ExpectedIdentityRevision: channel.UpstreamAccountIdentityRevision,
			ExpectedAdminRevision: channel.AdminRevision, Provider: channel.Type, APIOrigin: channel.SiteURL,
			AccountID: "replacement-account-b", Scope: UpstreamConcurrencyScopeAccount,
			Source: "pricing-test.authenticated-account", ObservedAt: time.Now().UTC(),
		}); err != nil {
			t.Fatalf("switch authenticated account: %v", err)
		}
		if _, err := store.ImportAdministratorRequestPriceFacts(context.Background(), "admin-user", "administrator-price-account-switch", []GatewayAdministratorPriceObservation{oldItem}); !errors.Is(err, ErrGatewayRequestPriceImportUnavailable) {
			t.Fatalf("delayed account A price error = %v", err)
		}
	})
}

func TestImportAdministratorRequestPriceFactsRollsBackWholeBatch(t *testing.T) {
	if strings.TrimSpace(os.Getenv("BRIDGEOPS_TEST_POSTGRES_DSN")) == "" {
		t.Skip("dedicated PostgreSQL test DSN is not configured")
	}
	db := openTestDB(t)
	store, channel, group, key := gatewayAdministratorPriceImportFixture(t, db, "administrator-price-rollback")
	price := 1.0
	validUntil := time.Now().UTC().Add(time.Hour)
	_, err := store.ImportAdministratorRequestPriceFacts(context.Background(), "admin-user", "administrator-price-rollback-1", []GatewayAdministratorPriceObservation{
		gatewayAdministratorPriceObservationForTest(channel, group, key, time.Now().UTC().Add(-time.Minute), GatewayAdministratorPriceObservation{
			ChannelID: channel.ID, GroupName: "primary", ModelName: "model-a",
			Operation: GatewayPricingOperationEmbedding, Protocol: GatewayPricingProtocolOpenAIEmbeddings,
			InputUnitPriceUSD: &price, ValidUntil: validUntil,
		}),
		gatewayAdministratorPriceObservationForTest(channel, group, key, time.Now().UTC().Add(-time.Minute), GatewayAdministratorPriceObservation{
			ChannelID: channel.ID, GroupName: "primary", ModelName: "model-z",
			Operation: GatewayPricingOperationEmbedding, Protocol: GatewayPricingProtocolOpenAIEmbeddings,
			InputUnitPriceUSD: &price, ValidUntil: validUntil,
		}),
	})
	if !errors.Is(err, ErrGatewayRequestPriceImportUnavailable) {
		t.Fatalf("batch error = %v, want unavailable", err)
	}
	var count int64
	if countErr := db.Model(&GatewayRequestPriceFact{}).Count(&count).Error; countErr != nil {
		t.Fatalf("count price facts: %v", countErr)
	}
	if count != 0 {
		t.Fatalf("failed batch retained %d price facts", count)
	}
	var commandCount, itemCount int64
	if err := db.Table("gateway_request_price_import_commands").Count(&commandCount).Error; err != nil {
		t.Fatalf("count rolled-back price import commands: %v", err)
	}
	if err := db.Table("gateway_request_price_import_items").Count(&itemCount).Error; err != nil {
		t.Fatalf("count rolled-back price import items: %v", err)
	}
	if commandCount != 0 || itemCount != 0 {
		t.Fatalf("failed batch retained command=%d items=%d", commandCount, itemCount)
	}
}

func TestImportAdministratorRequestPriceFactsReplaysExactReceiptAfterTargetChanges(t *testing.T) {
	if strings.TrimSpace(os.Getenv("BRIDGEOPS_TEST_POSTGRES_DSN")) == "" {
		t.Skip("dedicated PostgreSQL test DSN is not configured")
	}
	db := openTestDB(t)
	store, channel, group, key := gatewayAdministratorPriceImportFixture(t, db, "administrator-price-replay")
	price := 1.25
	observedAt := time.Now().UTC().Add(-time.Minute)
	validUntil := time.Now().UTC().Add(2 * time.Second).Truncate(time.Microsecond)
	first, err := store.ImportAdministratorRequestPriceFacts(context.Background(), "admin-user", "administrator-price-replay-1", []GatewayAdministratorPriceObservation{gatewayAdministratorPriceObservationForTest(channel, group, key, observedAt, GatewayAdministratorPriceObservation{
		ChannelID: channel.ID, GroupName: " PRIMARY ", ModelName: "model-a",
		Operation: " EMBEDDING ", Protocol: " OPENAI_EMBEDDINGS ",
		InputUnitPriceUSD: &price, ValidUntil: validUntil,
	})})
	if err != nil {
		t.Fatalf("create price import command: %v", err)
	}
	if first.IdempotentReplay || first.CommandID == 0 || len(first.Items) != 1 {
		t.Fatalf("first price import result = %+v", first)
	}
	if err := db.Model(&GatewayUpstreamKey{}).Where("id = ?", key.ID).Update("enabled", false).Error; err != nil {
		t.Fatalf("disable imported target key: %v", err)
	}
	if wait := time.Until(validUntil.Add(100 * time.Millisecond)); wait > 0 {
		time.Sleep(wait)
	}

	replay, err := store.ImportAdministratorRequestPriceFacts(context.Background(), "admin-user", "administrator-price-replay-1", []GatewayAdministratorPriceObservation{gatewayAdministratorPriceObservationForTest(channel, group, key, observedAt, GatewayAdministratorPriceObservation{
		ChannelID: channel.ID, GroupName: "primary", ModelName: "model-a",
		Operation: GatewayPricingOperationEmbedding, Protocol: GatewayPricingProtocolOpenAIEmbeddings,
		InputUnitPriceUSD: &price, ValidUntil: validUntil,
	})})
	if err != nil {
		t.Fatalf("replay price import command: %v", err)
	}
	if !replay.IdempotentReplay || replay.CommandID != first.CommandID || len(replay.Items) != 1 ||
		replay.Items[0].ID != first.Items[0].ID || replay.Items[0].Checksum != first.Items[0].Checksum ||
		!replay.Items[0].ValidFrom.Equal(first.Items[0].ValidFrom) {
		t.Fatalf("replayed price import = %+v, first = %+v", replay, first)
	}
	var factCount, commandCount, itemCount int64
	if err := db.Model(&GatewayRequestPriceFact{}).Count(&factCount).Error; err != nil {
		t.Fatalf("count replayed price facts: %v", err)
	}
	if err := db.Table("gateway_request_price_import_commands").Count(&commandCount).Error; err != nil {
		t.Fatalf("count replayed price commands: %v", err)
	}
	if err := db.Table("gateway_request_price_import_items").Count(&itemCount).Error; err != nil {
		t.Fatalf("count replayed price items: %v", err)
	}
	if factCount != 1 || commandCount != 1 || itemCount != 1 {
		t.Fatalf("replay row counts facts=%d commands=%d items=%d", factCount, commandCount, itemCount)
	}
}

func TestImportAdministratorRequestPriceFactsRejectsActorAndPayloadConflicts(t *testing.T) {
	if strings.TrimSpace(os.Getenv("BRIDGEOPS_TEST_POSTGRES_DSN")) == "" {
		t.Skip("dedicated PostgreSQL test DSN is not configured")
	}
	db := openTestDB(t)
	store, channel, group, key := gatewayAdministratorPriceImportFixture(t, db, "administrator-price-conflict")
	price := 1.0
	validUntil := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
	observation := gatewayAdministratorPriceObservationForTest(channel, group, key, time.Now().UTC().Add(-time.Minute), GatewayAdministratorPriceObservation{
		ChannelID: channel.ID, GroupName: "primary", ModelName: "model-a",
		Operation: GatewayPricingOperationEmbedding, Protocol: GatewayPricingProtocolOpenAIEmbeddings,
		InputUnitPriceUSD: &price, ValidUntil: validUntil,
	})
	first, err := store.ImportAdministratorRequestPriceFacts(context.Background(), "admin-user", "administrator-price-conflict-1", []GatewayAdministratorPriceObservation{observation})
	if err != nil {
		t.Fatalf("create conflict fixture: %v", err)
	}
	if _, err := store.ImportAdministratorRequestPriceFacts(context.Background(), "other-admin", "administrator-price-conflict-1", []GatewayAdministratorPriceObservation{observation}); !errors.Is(err, ErrGatewayRequestPriceImportIdempotencyConflict) {
		t.Fatalf("actor conflict = %v", err)
	}
	changedPrice := 2.0
	changed := observation
	changed.InputUnitPriceUSD = &changedPrice
	if _, err := store.ImportAdministratorRequestPriceFacts(context.Background(), "admin-user", "administrator-price-conflict-1", []GatewayAdministratorPriceObservation{changed}); !errors.Is(err, ErrGatewayRequestPriceImportIdempotencyConflict) {
		t.Fatalf("payload conflict = %v", err)
	}
	var facts []GatewayRequestPriceFact
	if err := db.Find(&facts).Error; err != nil {
		t.Fatalf("read facts after conflicts: %v", err)
	}
	if len(facts) != 1 || facts[0].ID != first.Items[0].ID {
		t.Fatalf("conflicts changed facts: %+v", facts)
	}
}

func TestImportAdministratorRequestPriceFactsConcurrentReplayCreatesOneCommand(t *testing.T) {
	if strings.TrimSpace(os.Getenv("BRIDGEOPS_TEST_POSTGRES_DSN")) == "" {
		t.Skip("dedicated PostgreSQL test DSN is not configured")
	}
	db := openTestDB(t)
	store, channel, group, key := gatewayAdministratorPriceImportFixture(t, db, "administrator-price-concurrent-replay")
	price := 1.0
	observation := gatewayAdministratorPriceObservationForTest(channel, group, key, time.Now().UTC().Add(-time.Minute), GatewayAdministratorPriceObservation{
		ChannelID: channel.ID, GroupName: "primary", ModelName: "model-a",
		Operation: GatewayPricingOperationEmbedding, Protocol: GatewayPricingProtocolOpenAIEmbeddings,
		InputUnitPriceUSD: &price, ValidUntil: time.Now().UTC().Add(time.Hour),
	})

	start := make(chan struct{})
	results := make(chan *GatewayRequestPriceImportResult, 2)
	errorsSeen := make(chan error, 2)
	var workers sync.WaitGroup
	for range 2 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			result, err := store.ImportAdministratorRequestPriceFacts(context.Background(), "admin-user", "administrator-price-concurrent-replay-1", []GatewayAdministratorPriceObservation{observation})
			if err != nil {
				errorsSeen <- err
				return
			}
			results <- result
		}()
	}
	close(start)
	workers.Wait()
	close(results)
	close(errorsSeen)
	for err := range errorsSeen {
		t.Fatalf("concurrent replay: %v", err)
	}
	var imported []*GatewayRequestPriceImportResult
	for result := range results {
		imported = append(imported, result)
	}
	if len(imported) != 2 || imported[0].CommandID != imported[1].CommandID ||
		len(imported[0].Items) != 1 || len(imported[1].Items) != 1 || imported[0].Items[0].ID != imported[1].Items[0].ID ||
		imported[0].IdempotentReplay == imported[1].IdempotentReplay {
		t.Fatalf("concurrent replay results = %+v", imported)
	}
}

func TestImportAdministratorRequestPriceFactsUsesPostLockServerTime(t *testing.T) {
	if strings.TrimSpace(os.Getenv("BRIDGEOPS_TEST_POSTGRES_DSN")) == "" {
		t.Skip("dedicated PostgreSQL test DSN is not configured")
	}
	db := openTestDB(t)
	store, channel, group, key := gatewayAdministratorPriceImportFixture(t, db, "administrator-price-post-lock-time")
	blocker := db.Begin()
	if blocker.Error != nil {
		t.Fatalf("begin price import lock blocker: %v", blocker.Error)
	}
	defer blocker.Rollback()
	if err := blocker.Exec("LOCK TABLE public.gateway_request_price_import_commands IN SHARE ROW EXCLUSIVE MODE").Error; err != nil {
		t.Fatalf("lock price import command table: %v", err)
	}

	price := 1.0
	validUntil := time.Now().UTC().Add(2 * time.Second).Truncate(time.Microsecond)
	type importResult struct {
		result *GatewayRequestPriceImportResult
		err    error
	}
	resultSeen := make(chan importResult, 1)
	go func() {
		result, err := store.ImportAdministratorRequestPriceFacts(context.Background(), "admin-user", "administrator-price-post-lock-time-1", []GatewayAdministratorPriceObservation{gatewayAdministratorPriceObservationForTest(channel, group, key, time.Now().UTC().Add(-time.Minute), GatewayAdministratorPriceObservation{
			ChannelID: channel.ID, GroupName: "primary", ModelName: "model-a",
			Operation: GatewayPricingOperationEmbedding, Protocol: GatewayPricingProtocolOpenAIEmbeddings,
			InputUnitPriceUSD: &price, ValidUntil: validUntil,
		})})
		resultSeen <- importResult{result: result, err: err}
	}()
	waitForGatewayRequestPriceImportLock(t, db, "lock table public.gateway_request_price_import_commands")
	if wait := time.Until(validUntil.Add(100 * time.Millisecond)); wait > 0 {
		time.Sleep(wait)
	}
	if err := blocker.Rollback().Error; err != nil {
		t.Fatalf("release price import command lock: %v", err)
	}

	select {
	case imported := <-resultSeen:
		if imported.result != nil || !errors.Is(imported.err, ErrGatewayRequestPriceImportInvalid) {
			t.Fatalf("post-lock expired import result=%+v err=%v", imported.result, imported.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("price import did not finish after command lock release")
	}
	var commandCount int64
	if err := db.Table("gateway_request_price_import_commands").
		Where("idempotency_key = ?", "administrator-price-post-lock-time-1").Count(&commandCount).Error; err != nil || commandCount != 0 {
		t.Fatalf("expired price import command count = %d, err=%v", commandCount, err)
	}
}

func TestImportAdministratorRequestPriceFactsFreezesNormalizedKeyCandidates(t *testing.T) {
	if strings.TrimSpace(os.Getenv("BRIDGEOPS_TEST_POSTGRES_DSN")) == "" {
		t.Skip("dedicated PostgreSQL test DSN is not configured")
	}
	db := openTestDB(t)
	store, channel, group, currentKey := gatewayAdministratorPriceImportFixture(t, db, "administrator-price-key-candidates")
	olderSync := group.DiscoveredAt.Add(time.Microsecond)
	alternateKey := &GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: " PRIMARY ", UpstreamKeyID: 8, UpstreamKeyName: "managed-alternate",
		APIKeyCipher: "cipher-v2", APIKeyPrefix: "sk-test-v2", ModelFamilies: "codex", ModelIDs: "model-a,model-b",
		ModelsSyncedAt: &olderSync, Enabled: true,
	}
	// Bypass the current normalized upsert path to model a legacy raw-name
	// duplicate that predates canonical routing-health ownership.
	if err := db.Create(alternateKey).Error; err != nil {
		t.Fatalf("create alternate normalized upstream key: %v", err)
	}
	if alternateKey.ID == 0 || alternateKey.ID == currentKey.ID {
		t.Fatalf("alternate upstream key = %+v, current=%d", alternateKey, currentKey.ID)
	}

	newestSync := currentKey.ModelsSyncedAt.Add(time.Minute)
	blocker := db.Begin()
	if blocker.Error != nil {
		t.Fatalf("begin alternate key refresh: %v", blocker.Error)
	}
	defer blocker.Rollback()
	if err := blocker.Model(&GatewayUpstreamKey{}).Where("id = ?", alternateKey.ID).
		Update("models_synced_at", newestSync).Error; err != nil {
		t.Fatalf("stage alternate key refresh: %v", err)
	}

	price := 1.0
	resultSeen := make(chan struct {
		result *GatewayRequestPriceImportResult
		err    error
	}, 1)
	go func() {
		result, err := store.ImportAdministratorRequestPriceFacts(context.Background(), "admin-user", "administrator-price-key-candidates-1", []GatewayAdministratorPriceObservation{gatewayAdministratorPriceObservationForTest(channel, group, currentKey, time.Now().UTC().Add(-time.Minute), GatewayAdministratorPriceObservation{
			ChannelID: channel.ID, GroupName: "primary", ModelName: "model-a",
			Operation: GatewayPricingOperationEmbedding, Protocol: GatewayPricingProtocolOpenAIEmbeddings,
			InputUnitPriceUSD: &price, ValidUntil: time.Now().UTC().Add(time.Hour),
		})})
		resultSeen <- struct {
			result *GatewayRequestPriceImportResult
			err    error
		}{result: result, err: err}
	}()
	waitForGatewayRequestPriceImportLock(t, db, "lock table public.gateway_upstream_keys in share mode")
	if err := blocker.Commit().Error; err != nil {
		t.Fatalf("commit alternate key refresh: %v", err)
	}

	select {
	case imported := <-resultSeen:
		if imported.result != nil || !errors.Is(imported.err, ErrGatewayRequestPriceImportUnavailable) {
			t.Fatalf("stale key-fenced import result=%+v err=%v, alternate=%d", imported.result, imported.err, alternateKey.ID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("price import did not finish after alternate key refresh")
	}
}

func TestImportAdministratorRequestPriceFactsSelectsNewestVersionAndFailsClosedAfterExpiry(t *testing.T) {
	if strings.TrimSpace(os.Getenv("BRIDGEOPS_TEST_POSTGRES_DSN")) == "" {
		t.Skip("dedicated PostgreSQL test DSN is not configured")
	}
	db := openTestDB(t)
	store, channel, group, key := gatewayAdministratorPriceImportFixture(t, db, "administrator-price-version")
	now := time.Now().UTC()
	firstPrice, secondPrice := 1.0, 2.0
	firstResult, err := store.ImportAdministratorRequestPriceFacts(context.Background(), "admin-user", "administrator-price-version-1", []GatewayAdministratorPriceObservation{gatewayAdministratorPriceObservationForTest(channel, group, key, now.Add(-2*time.Minute), GatewayAdministratorPriceObservation{
		ChannelID: channel.ID, GroupName: "primary", ModelName: "model-a",
		Operation: GatewayPricingOperationEmbedding, Protocol: GatewayPricingProtocolOpenAIEmbeddings,
		InputUnitPriceUSD: &firstPrice, ValidUntil: now.Add(3 * time.Hour),
	})})
	if err != nil {
		t.Fatalf("import first version: %v", err)
	}
	secondResult, err := store.ImportAdministratorRequestPriceFacts(context.Background(), "admin-user", "administrator-price-version-2", []GatewayAdministratorPriceObservation{gatewayAdministratorPriceObservationForTest(channel, group, key, now.Add(-time.Minute), GatewayAdministratorPriceObservation{
		ChannelID: channel.ID, GroupName: "primary", ModelName: "model-a",
		Operation: GatewayPricingOperationEmbedding, Protocol: GatewayPricingProtocolOpenAIEmbeddings,
		InputUnitPriceUSD: &secondPrice, ValidUntil: now.Add(time.Hour),
	})})
	if err != nil {
		t.Fatalf("import second version: %v", err)
	}
	first, second := firstResult.Items, secondResult.Items
	if first[0].FactVersion != 1 || second[0].FactVersion != 2 {
		t.Fatalf("fact versions = %d, %d", first[0].FactVersion, second[0].FactVersion)
	}
	for index, observedAt := range []time.Time{now.Add(-3 * time.Minute), now.Add(-time.Minute)} {
		_, err := store.ImportAdministratorRequestPriceFacts(context.Background(), "admin-user", fmt.Sprintf("administrator-price-version-rejected-%d", index), []GatewayAdministratorPriceObservation{gatewayAdministratorPriceObservationForTest(channel, group, key, observedAt, GatewayAdministratorPriceObservation{
			ChannelID: channel.ID, GroupName: "primary", ModelName: "model-a",
			Operation: GatewayPricingOperationEmbedding, Protocol: GatewayPricingProtocolOpenAIEmbeddings,
			InputUnitPriceUSD: &secondPrice, ValidUntil: now.Add(2 * time.Hour),
		})})
		if !errors.Is(err, ErrGatewayRequestPriceImportUnavailable) {
			t.Fatalf("rejected observed_at %s error = %v", observedAt, err)
		}
	}

	scope := gatewayRequestPriceScopeFromFact(&second[0])
	current, err := store.FindCurrentRequestPriceFact(scope, now.Add(30*time.Minute))
	if err != nil || current.Fact == nil || current.Fact.ID != second[0].ID || current.UnavailableReason != "" {
		t.Fatalf("current price fact = %+v, err=%v", current, err)
	}
	expired, err := store.FindCurrentRequestPriceFact(scope, now.Add(90*time.Minute))
	if err != nil || expired.Fact != nil || expired.UnavailableReason != GatewayPricingUnavailableFactExpired {
		t.Fatalf("expired newest fact revived an older version: %+v, err=%v", expired, err)
	}
}

func TestImportAdministratorRequestPriceFactsAllocatesVersionsConcurrently(t *testing.T) {
	if strings.TrimSpace(os.Getenv("BRIDGEOPS_TEST_POSTGRES_DSN")) == "" {
		t.Skip("dedicated PostgreSQL test DSN is not configured")
	}
	db := openTestDB(t)
	store, channel, group, key := gatewayAdministratorPriceImportFixture(t, db, "administrator-price-concurrent")
	price := 1.0
	observation := gatewayAdministratorPriceObservationForTest(channel, group, key, time.Now().UTC().Add(-time.Minute), GatewayAdministratorPriceObservation{
		ChannelID: channel.ID, GroupName: "primary", ModelName: "model-a",
		Operation: GatewayPricingOperationEmbedding, Protocol: GatewayPricingProtocolOpenAIEmbeddings,
		InputUnitPriceUSD: &price, ValidUntil: time.Now().UTC().Add(time.Hour),
	})

	start := make(chan struct{})
	type result struct {
		version uint64
		err     error
	}
	results := make(chan result, 2)
	var workers sync.WaitGroup
	keys := []string{"administrator-price-concurrent-1", "administrator-price-concurrent-2"}
	for worker := range 2 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			imported, err := store.ImportAdministratorRequestPriceFacts(context.Background(), "admin-user", keys[worker], []GatewayAdministratorPriceObservation{observation})
			if err != nil {
				results <- result{err: err}
				return
			}
			results <- result{version: imported.Items[0].FactVersion}
		}()
	}
	close(start)
	workers.Wait()
	close(results)

	versions := make([]uint64, 0, 2)
	conflicts := 0
	for item := range results {
		if item.err != nil {
			if errors.Is(item.err, ErrGatewayRequestPriceImportUnavailable) {
				conflicts++
				continue
			}
			t.Fatalf("concurrent import: %v", item.err)
		}
		versions = append(versions, item.version)
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i] < versions[j] })
	if len(versions) != 1 || versions[0] != 1 || conflicts != 1 {
		t.Fatalf("concurrent same-observation results versions=%v conflicts=%d", versions, conflicts)
	}
}

func TestImportAdministratorRequestPriceFactsBindsCurrentCostUnitEpoch(t *testing.T) {
	if strings.TrimSpace(os.Getenv("BRIDGEOPS_TEST_POSTGRES_DSN")) == "" {
		t.Skip("dedicated PostgreSQL test DSN is not configured")
	}
	db := openTestDB(t)
	if err := db.Exec("ALTER TABLE public.channels ADD COLUMN IF NOT EXISTS cost_unit_epoch bigint NOT NULL DEFAULT 1").Error; err != nil {
		t.Fatalf("install cost-unit epoch integration fixture: %v", err)
	}
	store, channel, group, key := gatewayAdministratorPriceImportFixture(t, db, "administrator-price-cost-epoch")
	price := 1.0
	observation := gatewayAdministratorPriceObservationForTest(channel, group, key, time.Now().UTC().Add(-2*time.Minute), GatewayAdministratorPriceObservation{
		ChannelID: channel.ID, GroupName: "primary", ModelName: "model-a",
		Operation: GatewayPricingOperationEmbedding, Protocol: GatewayPricingProtocolOpenAIEmbeddings,
		InputUnitPriceUSD: &price, ValidUntil: time.Now().UTC().Add(time.Hour),
	})
	firstResult, err := store.ImportAdministratorRequestPriceFacts(context.Background(), "admin-user", "administrator-price-cost-epoch-1", []GatewayAdministratorPriceObservation{observation})
	if err != nil {
		t.Fatalf("import epoch 1 fact: %v", err)
	}
	first := firstResult.Items
	if first[0].CostUnitEpoch != 1 || first[0].FactVersion != 1 {
		t.Fatalf("epoch 1 fact = %+v", first[0])
	}
	if err := db.Exec("UPDATE public.channels SET cost_unit_epoch = 2 WHERE id = ?", channel.ID).Error; err != nil {
		t.Fatalf("advance channel cost-unit epoch: %v", err)
	}
	observation.ObservedAt = time.Now().UTC().Add(-time.Minute)
	stale, err := store.FindCurrentRequestPriceFact(gatewayRequestPriceScopeFromFact(&first[0]), time.Now().UTC())
	if err != nil || stale.Fact != nil || stale.UnavailableReason != GatewayPricingUnavailableCostBasisChanged {
		t.Fatalf("stale cost-unit price fact = %+v, err=%v", stale, err)
	}

	if _, err := store.ImportAdministratorRequestPriceFacts(context.Background(), "admin-user", "administrator-price-cost-epoch-stale", []GatewayAdministratorPriceObservation{observation}); !errors.Is(err, ErrGatewayRequestPriceImportUnavailable) {
		t.Fatalf("stale expected cost-unit epoch import error = %v", err)
	}
	observation.ExpectedCostUnitEpoch = 2
	secondResult, err := store.ImportAdministratorRequestPriceFacts(context.Background(), "admin-user", "administrator-price-cost-epoch-2", []GatewayAdministratorPriceObservation{observation})
	if err != nil {
		t.Fatalf("import epoch 2 fact: %v", err)
	}
	second := secondResult.Items
	if second[0].CostUnitEpoch != 2 || second[0].FactVersion != 1 {
		t.Fatalf("epoch 2 fact = %+v", second[0])
	}
	current, err := store.FindCurrentRequestPriceFact(gatewayRequestPriceScopeFromFact(&second[0]), time.Now().UTC())
	if err != nil || current.Fact == nil || current.Fact.ID != second[0].ID || current.UnavailableReason != "" {
		t.Fatalf("current epoch price fact = %+v, err=%v", current, err)
	}
}

func gatewayAdministratorPriceImportFixture(t *testing.T, db *gorm.DB, name string) (*Gateway, *Channel, *GatewayUpstreamGroup, *GatewayUpstreamKey) {
	t.Helper()
	store := NewGateway(db)
	channel := &Channel{
		Name: name, Type: ChannelTypeSub2API, GatewayApp: GatewayAppCodex,
		SiteURL: "https://" + name + ".invalid", PasswordCipher: "cipher", MonitorEnabled: true,
	}
	if err := NewChannels(db).Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	now := time.Now().UTC().Add(-5 * time.Minute)
	if _, err := NewChannels(db).ObserveUpstreamAccountIdentity(UpstreamAccountIdentityObservation{
		ChannelID: channel.ID, ExpectedIdentityRevision: channel.UpstreamAccountIdentityRevision,
		ExpectedAdminRevision: channel.AdminRevision, Provider: channel.Type, APIOrigin: channel.SiteURL,
		AccountID: name + "-account", Scope: UpstreamConcurrencyScopeAccount,
		Source: "pricing-test.authenticated-account", ObservedAt: now.Add(-time.Minute),
	}); err != nil {
		t.Fatalf("bind authenticated pricing account: %v", err)
	}
	if _, err := NewRates(db).Upsert(&RateSnapshot{ChannelID: channel.ID, ModelName: "primary", Ratio: 1, LastSeenAt: now}); err != nil {
		t.Fatalf("create current rate: %v", err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "added", 7, "managed", now); err != nil {
		t.Fatalf("create group identity: %v", err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary")
	if err != nil {
		t.Fatalf("capture group generation: %v", err)
	}
	syncedAt := now.Add(time.Second)
	key := &GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "primary", UpstreamKeyID: 7, UpstreamKeyName: "managed",
		APIKeyCipher: "cipher-v1", APIKeyPrefix: "sk-test", ModelFamilies: "codex", ModelIDs: "model-a,model-b",
		ModelsSyncedAt: &syncedAt, Enabled: true, CreatedAt: now,
	}
	if err := store.UpsertUpstreamKey(key); err != nil {
		t.Fatalf("create upstream key: %v", err)
	}
	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "primary", "codex", "model-a,model-b", syncedAt, "", generation); err != nil {
		t.Fatalf("save synchronized model list: %v", err)
	}
	if err := db.Select("id", "upstream_account_identity_revision").First(channel, channel.ID).Error; err != nil {
		t.Fatalf("reload channel identity: %v", err)
	}
	var group GatewayUpstreamGroup
	if err := db.First(&group, generation.ID).Error; err != nil {
		t.Fatalf("reload group identity: %v", err)
	}
	if err := db.Where("channel_id = ? AND group_name = ?", channel.ID, "primary").First(key).Error; err != nil {
		t.Fatalf("reload upstream key: %v", err)
	}
	return store, channel, &group, key
}

func waitForGatewayRequestPriceImportLock(t *testing.T, db *gorm.DB, queryFragment string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiting int64
		if err := db.Raw(`
			SELECT count(*)
			FROM pg_catalog.pg_stat_activity
			WHERE datname = current_database()
			  AND pid <> pg_backend_pid()
			  AND wait_event_type = 'Lock'
			  AND position(lower(?) in lower(query)) > 0
		`, queryFragment).Scan(&waiting).Error; err != nil {
			t.Fatalf("inspect price import lock wait: %v", err)
		}
		if waiting > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("price import did not wait on %q", queryFragment)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
