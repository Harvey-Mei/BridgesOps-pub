package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const (
	gatewayModelsAggregateMaxItems        = 4096
	gatewayModelsAggregateMaxEncodedBytes = 4 << 20
	gatewayModelIDMaxBytes                = 512
	gatewayModelObjectMaxBytes            = 64
	gatewayModelOwnerMaxBytes             = 256
)

var errGatewayModelsAggregateLimit = errors.New("gateway models aggregate exceeds configured limit")

type gatewayBoundedModelItem struct {
	ID      string `json:"id"`
	Object  string `json:"object,omitempty"`
	Created int64  `json:"created,omitempty"`
	OwnedBy string `json:"owned_by,omitempty"`
}

func parseGatewayBoundedModelItems(raw []byte) ([]map[string]any, error) {
	var parsed struct {
		Data []gatewayBoundedModelItem `json:"data"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, err
	}
	if len(parsed.Data) > gatewayModelsAggregateMaxItems {
		return nil, fmt.Errorf("%w: one response returned more than %d models", errGatewayModelsAggregateLimit, gatewayModelsAggregateMaxItems)
	}
	items := make([]map[string]any, 0, len(parsed.Data))
	for _, item := range parsed.Data {
		item.ID = strings.TrimSpace(item.ID)
		item.Object = strings.TrimSpace(item.Object)
		item.OwnedBy = strings.TrimSpace(item.OwnedBy)
		if item.ID == "" {
			continue
		}
		if len(item.ID) > gatewayModelIDMaxBytes || len(item.Object) > gatewayModelObjectMaxBytes || len(item.OwnedBy) > gatewayModelOwnerMaxBytes {
			return nil, fmt.Errorf("%w: model metadata field is too large", errGatewayModelsAggregateLimit)
		}
		bounded := map[string]any{"id": item.ID}
		if item.Object != "" {
			bounded["object"] = item.Object
		}
		if item.Created != 0 {
			bounded["created"] = item.Created
		}
		if item.OwnedBy != "" {
			bounded["owned_by"] = item.OwnedBy
		}
		items = append(items, bounded)
	}
	return items, nil
}

func appendGatewayBoundedUniqueModelItem(data []map[string]any, seen map[string]struct{}, encodedBytes int, item map[string]any) ([]map[string]any, int, error) {
	id, _ := item["id"].(string)
	if id == "" {
		return data, encodedBytes, nil
	}
	if _, exists := seen[id]; exists {
		return data, encodedBytes, nil
	}
	if len(data) >= gatewayModelsAggregateMaxItems {
		return data, encodedBytes, fmt.Errorf("%w: more than %d unique models", errGatewayModelsAggregateLimit, gatewayModelsAggregateMaxItems)
	}
	encoded, err := json.Marshal(item)
	if err != nil {
		return data, encodedBytes, err
	}
	// Include one comma per item. The outer object/list syntax is a small fixed
	// overhead below the configured aggregate cap.
	nextBytes := encodedBytes + len(encoded) + 1
	if nextBytes > gatewayModelsAggregateMaxEncodedBytes {
		return data, encodedBytes, fmt.Errorf("%w: encoded model list exceeds %d bytes", errGatewayModelsAggregateLimit, gatewayModelsAggregateMaxEncodedBytes)
	}
	seen[id] = struct{}{}
	return append(data, item), nextBytes, nil
}
