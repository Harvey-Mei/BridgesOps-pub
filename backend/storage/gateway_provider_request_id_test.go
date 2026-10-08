package storage

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/providerid"
)

func TestGatewayProviderRequestIDSchemaContractUses512Bytes(t *testing.T) {
	if providerid.MaxBytes != 512 {
		t.Fatalf("provider request ID maximum = %d, want 512", providerid.MaxBytes)
	}
	if GatewayProviderRequestIDMaxLength != providerid.MaxBytes {
		t.Fatalf("gateway provider request ID maximum = %d, want %d", GatewayProviderRequestIDMaxLength, providerid.MaxBytes)
	}
	for _, item := range []struct {
		model any
		field string
	}{
		{model: GatewayRequestLog{}, field: "UpstreamRequestID"},
		{model: GatewayRequestDispatchAudit{}, field: "ProviderRequestID"},
	} {
		field, ok := reflect.TypeOf(item.model).FieldByName(item.field)
		if !ok {
			t.Fatalf("%T missing %s", item.model, item.field)
		}
		if tag := field.Tag.Get("gorm"); !strings.Contains(tag, "size:512") {
			t.Fatalf("%T.%s gorm tag = %q, want size:512", item.model, item.field, tag)
		}
	}
}

func TestGatewayProviderRequestIDNormalizersPreserveBoundaryWithoutTruncation(t *testing.T) {
	maximumUTF8 := strings.Repeat("界", 170) + "aa"
	valid := []string{
		strings.Repeat("a", 128),
		strings.Repeat("a", GatewayProviderRequestIDMaxLength),
		maximumUTF8,
		" provider-id ",
		" ",
	}
	for _, value := range valid {
		log := &GatewayRequestLog{UpstreamRequestID: value}
		if err := normalizeGatewayRequestRecord(log); err != nil {
			t.Fatalf("normalize %d-byte request-log provider ID: %v", len(value), err)
		}
		if log.UpstreamRequestID != value {
			t.Fatalf("request-log provider ID changed at %d bytes", len(value))
		}
		facts, err := normalizeGatewayRequestDispatchFinalizationFacts(
			GatewayRequestDispatchFinalizationFacts{ProviderRequestID: value}, time.Unix(1, 0),
		)
		if err != nil {
			t.Fatalf("normalize %d-byte dispatch provider ID: %v", len(value), err)
		}
		if facts.ProviderRequestID != value {
			t.Fatalf("dispatch provider ID changed at %d bytes", len(value))
		}
	}

	for _, value := range []string{
		strings.Repeat("a", GatewayProviderRequestIDMaxLength+1),
		strings.Repeat("a", GatewayProviderRequestIDMaxLength) + " ",
		strings.Repeat("界", 171),
		string([]byte{'a', 0xff, 'b'}),
	} {
		log := &GatewayRequestLog{UpstreamRequestID: value}
		if err := normalizeGatewayRequestRecord(log); err == nil || log.UpstreamRequestID != value {
			t.Fatalf("request-log provider ID bytes=%d was not rejected intact: value=%q err=%v", len(value), log.UpstreamRequestID, err)
		}
		facts := GatewayRequestDispatchFinalizationFacts{ProviderRequestID: value}
		got, err := normalizeGatewayRequestDispatchFinalizationFacts(facts, time.Unix(1, 0))
		if err == nil || got.ProviderRequestID != "" {
			t.Fatalf("dispatch provider ID bytes=%d was not explicitly rejected: value=%q err=%v", len(value), got.ProviderRequestID, err)
		}
	}
}

func TestGatewayProviderRequestIDCommonPrefixesRemainDistinct(t *testing.T) {
	prefix := strings.Repeat("p", GatewayProviderRequestIDMaxLength-1)
	left := &GatewayRequestLog{UpstreamRequestID: prefix + "a"}
	right := &GatewayRequestLog{UpstreamRequestID: prefix + "b"}
	if err := normalizeGatewayRequestRecord(left); err != nil {
		t.Fatal(err)
	}
	if err := normalizeGatewayRequestRecord(right); err != nil {
		t.Fatal(err)
	}
	if left.UpstreamRequestID == right.UpstreamRequestID {
		t.Fatal("distinct 512-byte provider IDs with a common prefix collapsed")
	}
}

func TestGatewayProviderRequestIDPresenceAndCopyRemainByteExact(t *testing.T) {
	const whitespaceID = " \t "

	failed := &GatewayRequestLog{
		StatusCode:        502,
		Error:             "upstream failure",
		UpstreamRequestID: whitespaceID,
	}
	if !IsGatewayRequestCostReconcileEligible(failed) {
		t.Fatal("non-empty provider request ID was treated as absent")
	}
	evidence := &GatewayRequestLog{
		RecordKind:        GatewayRequestRecordKindDispatchCostEvidence,
		UpstreamRequestID: whitespaceID,
	}
	if !isGatewayDispatchCostEvidencePending(evidence) {
		t.Fatal("non-empty dispatch evidence provider request ID was treated as absent")
	}

	audit := &GatewayRequestDispatchAudit{ProviderRequestID: "provider-from-audit"}
	log := &GatewayRequestLog{UpstreamRequestID: whitespaceID}
	applyGatewayRequestDispatchSnapshotToLog(audit, log)
	if log.UpstreamRequestID != whitespaceID {
		t.Fatalf("request-log provider ID = %q, want exact %q", log.UpstreamRequestID, whitespaceID)
	}
	emptyLog := &GatewayRequestLog{}
	applyGatewayRequestDispatchSnapshotToLog(audit, emptyLog)
	if emptyLog.UpstreamRequestID != audit.ProviderRequestID {
		t.Fatalf("empty request-log provider ID = %q, want audit value %q", emptyLog.UpstreamRequestID, audit.ProviderRequestID)
	}

	presenceSQL := strings.ToLower(gatewayRequestHasProviderRequestIDSQL("logs"))
	if strings.Contains(presenceSQL, "trim(") || !strings.Contains(presenceSQL, "coalesce(logs.upstream_request_id, '') <> ''") {
		t.Fatalf("provider request ID presence SQL is not byte-exact: %s", presenceSQL)
	}
}

func TestGatewayProviderRequestIDFilterPresenceAndCacheKeyRemainByteExact(t *testing.T) {
	plain := GatewayRequestLogFilter{UpstreamRequestID: "provider-id"}
	boundaryWhitespace := GatewayRequestLogFilter{UpstreamRequestID: " provider-id "}
	whitespaceOnly := GatewayRequestLogFilter{UpstreamRequestID: " "}

	for _, filter := range []GatewayRequestLogFilter{plain, boundaryWhitespace, whitespaceOnly} {
		if groupCacheAggregateFilterSupported(filter) {
			t.Fatalf("provider request ID %q was treated as absent by group-cache filter gate", filter.UpstreamRequestID)
		}
		rows, handled, err := (&Gateway{}).requestAggregateRows(filter)
		if err != nil || handled || rows != nil {
			t.Fatalf("provider request ID %q aggregate route = rows:%v handled:%v err:%v, want raw route", filter.UpstreamRequestID, rows, handled, err)
		}
	}

	gateway := &Gateway{}
	plainKey := gateway.requestAggregateMigrationCacheKey(1, plain, 0, "")
	if plainKey == gateway.requestAggregateMigrationCacheKey(1, boundaryWhitespace, 0, "") {
		t.Fatal("provider request ID boundary whitespace collapsed in migration cache key")
	}
	if plainKey == gateway.requestAggregateMigrationCacheKey(1, whitespaceOnly, 0, "") {
		t.Fatal("whitespace-only provider request ID collapsed in migration cache key")
	}
}

func TestGatewayProviderRequestIDFiltersRemainByteExact(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Now().UTC()
	identifiers := []string{"provider-id", " provider-id", "provider-id\u00a0"}
	for index, identifier := range identifiers {
		log := &GatewayRequestLog{
			DownstreamKeyID:   1,
			ChannelID:         uint(index + 1),
			Method:            "POST",
			Path:              "/v1/responses",
			StatusCode:        200,
			UpstreamRequestID: identifier,
			CreatedAt:         now.Add(time.Duration(index) * time.Second),
		}
		if err := store.AppendRequestLog(log); err != nil {
			t.Fatalf("append %q: %v", identifier, err)
		}
		items, total, _, err := store.ListRequestLogsPageFiltered(1, 20, GatewayRequestLogFilter{UpstreamRequestID: identifier})
		if err != nil || total != 1 || len(items) != 1 || items[0].ID != log.ID {
			t.Fatalf("exact filter %q returned total=%d items=%+v err=%v", identifier, total, items, err)
		}
	}
	_, total, _, err := store.ListRequestLogsPageFiltered(1, 20, GatewayRequestLogFilter{UpstreamRequestID: "provider-id "})
	if err != nil || total != 0 {
		t.Fatalf("unrecorded whitespace variant returned total=%d err=%v", total, err)
	}
}
