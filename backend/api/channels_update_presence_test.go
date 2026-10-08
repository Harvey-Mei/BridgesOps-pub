package api

import (
	"encoding/json"
	"testing"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

func TestChannelUpdateInputDistinguishesMissingAndExplicitNull(t *testing.T) {
	var missing channelUpdateInput
	if err := json.Unmarshal([]byte(`{"expected_admin_revision":7}`), &missing); err != nil {
		t.Fatal(err)
	}
	if missing.networkProxyIDPresent || missing.captchaConfigIDPresent || missing.rechargeMultiplierPresent {
		t.Fatalf("missing nullable fields became present: %+v", missing)
	}

	var cleared channelUpdateInput
	if err := json.Unmarshal([]byte(`{"expected_admin_revision":7,"network_proxy_id":null,"captcha_config_id":null,"recharge_multiplier":null}`), &cleared); err != nil {
		t.Fatal(err)
	}
	if !cleared.networkProxyIDPresent || !cleared.captchaConfigIDPresent || !cleared.rechargeMultiplierPresent {
		t.Fatalf("explicit null presence was lost: %+v", cleared)
	}
	if cleared.NetworkProxyID != nil || cleared.CaptchaConfigID != nil || cleared.RechargeMultiplier != nil {
		t.Fatalf("explicit null decoded as a value: %+v", cleared)
	}
}

func TestSavedDirectModelsProbeInputDistinguishesMissingAndExplicitNullProxy(t *testing.T) {
	var missing savedDirectModelsProbeInput
	if err := json.Unmarshal([]byte(`{"base_url":"https://example.test"}`), &missing); err != nil {
		t.Fatal(err)
	}
	if missing.networkProxyIDPresent {
		t.Fatal("missing network_proxy_id became present")
	}

	var cleared savedDirectModelsProbeInput
	if err := json.Unmarshal([]byte(`{"base_url":"https://example.test","network_proxy_id":null}`), &cleared); err != nil {
		t.Fatal(err)
	}
	if !cleared.networkProxyIDPresent || cleared.NetworkProxyID != nil {
		t.Fatalf("explicit null proxy was not preserved: %+v", cleared)
	}
}

func TestSavedDirectModelsProbeTargetRejectsCredentialForwardingOverrides(t *testing.T) {
	proxyID := uint(7)
	ch := &storage.Channel{ID: 3, SiteURL: "https://saved.example", NetworkProxyID: &proxyID}

	baseURL, gotProxyID, err := savedDirectModelsProbeTarget(ch, savedDirectModelsProbeInput{})
	if err != nil || baseURL != ch.SiteURL || gotProxyID == nil || *gotProxyID != proxyID {
		t.Fatalf("saved target = %q proxy=%v err=%v", baseURL, gotProxyID, err)
	}
	if _, _, err := savedDirectModelsProbeTarget(ch, savedDirectModelsProbeInput{BaseURL: "https://attacker.invalid"}); err == nil {
		t.Fatal("saved API key could be redirected to an overridden base URL")
	}
	if _, _, err := savedDirectModelsProbeTarget(ch, savedDirectModelsProbeInput{networkProxyIDPresent: true}); err == nil {
		t.Fatal("saved API key could be redirected through an overridden proxy")
	}
}
