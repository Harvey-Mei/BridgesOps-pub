package storage

import (
	"errors"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestCostUnitRequestRestatementMixedBucketAggregateRebuildFailsClosed(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *gorm.DB, *GatewayRequestLog)
		want   error
	}{
		{
			name: "pending main log marker",
			mutate: func(t *testing.T, db *gorm.DB, log *GatewayRequestLog) {
				t.Helper()
				if err := db.Model(&GatewayRequestLog{}).Where("id = ?", log.ID).Update("aggregate_version", 0).Error; err != nil {
					t.Fatal(err)
				}
			},
			want: ErrCostUnitRequestRestatementConflict,
		},
		{
			name: "pending group log marker",
			mutate: func(t *testing.T, db *gorm.DB, log *GatewayRequestLog) {
				t.Helper()
				if err := db.Model(&GatewayRequestLog{}).Where("id = ?", log.ID).Update("group_cache_aggregate_version", 0).Error; err != nil {
					t.Fatal(err)
				}
			},
			want: ErrCostUnitRequestRestatementConflict,
		},
		{
			name: "future main log marker",
			mutate: func(t *testing.T, db *gorm.DB, log *GatewayRequestLog) {
				t.Helper()
				if err := db.Model(&GatewayRequestLog{}).Where("id = ?", log.ID).
					Update("aggregate_version", gatewayRequestAggregateVersion+1).Error; err != nil {
					t.Fatal(err)
				}
			},
			want: errGatewayRequestAggregateVersionConflict,
		},
		{
			name: "future group log marker",
			mutate: func(t *testing.T, db *gorm.DB, log *GatewayRequestLog) {
				t.Helper()
				if err := db.Model(&GatewayRequestLog{}).Where("id = ?", log.ID).
					Update("group_cache_aggregate_version", gatewayRequestGroupCacheAggregateVersion+1).Error; err != nil {
					t.Fatal(err)
				}
			},
			want: errGatewayRequestGroupCacheAggregateVersionConflict,
		},
		{
			name: "future main ledger",
			mutate: func(t *testing.T, db *gorm.DB, log *GatewayRequestLog) {
				t.Helper()
				if err := db.Model(&GatewayRequestAggregate{}).Where("channel_id = ?", log.ChannelID).
					Update("version", gatewayRequestAggregateVersion+1).Error; err != nil {
					t.Fatal(err)
				}
			},
			want: errGatewayRequestAggregateVersionConflict,
		},
		{
			name: "future group ledger",
			mutate: func(t *testing.T, db *gorm.DB, log *GatewayRequestLog) {
				t.Helper()
				if err := db.Model(&GatewayRequestGroupCacheAggregate{}).Where("channel_id = ?", log.ChannelID).
					Update("version", gatewayRequestGroupCacheAggregateVersion+1).Error; err != nil {
					t.Fatal(err)
				}
			},
			want: errGatewayRequestGroupCacheAggregateVersionConflict,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, channel, log, evidence := seedCostUnitAggregateRebuildFenceFixture(t, test.name)
			var requestAggregatesBefore, groupAggregatesBefore int64
			if err := db.Model(&GatewayRequestAggregate{}).Where("channel_id = ?", channel.ID).Count(&requestAggregatesBefore).Error; err != nil {
				t.Fatal(err)
			}
			if err := db.Model(&GatewayRequestGroupCacheAggregate{}).Where("channel_id = ?", channel.ID).Count(&groupAggregatesBefore).Error; err != nil {
				t.Fatal(err)
			}
			test.mutate(t, db, log)
			err := db.Transaction(func(tx *gorm.DB) error {
				return rebuildCostUnitRequestRestatementAggregatesTx(tx, channel.ID, evidence)
			})
			if !errors.Is(err, test.want) {
				t.Fatalf("aggregate rebuild error = %v, want %v", err, test.want)
			}
			var requestAggregatesAfter, groupAggregatesAfter int64
			if err := db.Model(&GatewayRequestAggregate{}).Where("channel_id = ?", channel.ID).Count(&requestAggregatesAfter).Error; err != nil {
				t.Fatal(err)
			}
			if err := db.Model(&GatewayRequestGroupCacheAggregate{}).Where("channel_id = ?", channel.ID).Count(&groupAggregatesAfter).Error; err != nil {
				t.Fatal(err)
			}
			if requestAggregatesAfter != requestAggregatesBefore || groupAggregatesAfter != groupAggregatesBefore {
				t.Fatalf("failed rebuild changed ledger counts: request %d/%d group %d/%d",
					requestAggregatesBefore, requestAggregatesAfter, groupAggregatesBefore, groupAggregatesAfter)
			}
		})
	}
}

func seedCostUnitAggregateRebuildFenceFixture(t *testing.T, name string) (*gorm.DB, *Channel, *GatewayRequestLog, *CostUnitRequestRestatementEvidence) {
	t.Helper()
	db := openTestDB(t)
	channel := &Channel{
		Name: "aggregate-fence-" + name, Type: ChannelTypeSub2API,
		SiteURL: "https://example.com", PasswordCipher: "cipher",
	}
	if err := NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC().Truncate(gatewayRequestAggregateBucket).Add(time.Minute).Truncate(time.Microsecond)
	rawCost, factor := 1.25, 1.0
	log := &GatewayRequestLog{
		RecordKind: GatewayRequestRecordKindRequest, DownstreamKeyID: 1,
		ChannelID: channel.ID, GroupName: "plus", ModelName: "shared-model",
		Method: "POST", Path: "/v1/responses", StatusCode: 200,
		RawCost: &rawCost, Cost: rawCost, FundingFactor: &factor,
		RawGroupRatio: &factor, GroupRatio: factor, RawCompletionRatio: &factor, CompletionRatio: factor,
		CostBasisVersion: 1, CostBasisSource: "confirmed_redeem_code",
		CostSource: "reported", CostStatus: GatewayCostStatusReported, CreatedAt: at,
	}
	if err := NewGateway(db).AppendRequestLog(log); err != nil {
		t.Fatal(err)
	}
	if err := migrateGatewayRequestAggregates(db); err != nil {
		t.Fatal(err)
	}
	if err := db.First(log, log.ID).Error; err != nil {
		t.Fatal(err)
	}
	if log.AggregateVersion != gatewayRequestAggregateVersion || log.GroupCacheAggregateVersion != gatewayRequestGroupCacheAggregateVersion {
		t.Fatalf("aggregate fence fixture markers = %d/%d", log.AggregateVersion, log.GroupCacheAggregateVersion)
	}
	return db, channel, log, &CostUnitRequestRestatementEvidence{AffectedStartedAt: at, AffectedEndedAt: at}
}
