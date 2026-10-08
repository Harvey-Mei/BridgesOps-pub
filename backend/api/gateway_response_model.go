package api

import "strings"

// The reported model is audit evidence, not proof of the model's identity.
// Ignore malformed/oversized metadata without changing response delivery.
func gatewayResponseModel(value any) string {
	model, ok := value.(string)
	if !ok || len(model) > 256 {
		return ""
	}
	model = strings.TrimSpace(model)
	if strings.HasPrefix(strings.ToLower(model), "sk-") {
		return ""
	}
	for _, c := range model {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("-._:/@", c)) {
			return ""
		}
	}
	return model
}

func sanitizeGatewayUsageModel(metrics gatewayUsageMetrics, secrets ...string) gatewayUsageMetrics {
	for _, secret := range secrets {
		if secret != "" && strings.Contains(metrics.ResponseModel, secret) {
			metrics.ResponseModel = ""
			break
		}
	}
	return metrics
}

func gatewayResponseModelFromObject(root map[string]any) string {
	for _, envelope := range []string{"response", "message"} {
		if nested, ok := objectValue(root, envelope); ok {
			if model := gatewayResponseModel(nested["model"]); model != "" {
				return model
			}
		}
	}
	return gatewayResponseModel(root["model"])
}

func gatewayResponseModelPath(path []string) bool {
	return len(path) == 0 || gatewayProjectionPathEqual(path, "response") || gatewayProjectionPathEqual(path, "message")
}
