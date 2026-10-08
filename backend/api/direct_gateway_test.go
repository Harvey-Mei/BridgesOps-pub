package api

import "testing"

func TestGatewayDirectModelIDMatchIsCaseSensitive(t *testing.T) {
	if !gatewayModelIDExact([]string{"Vendor-Alias"}, "Vendor-Alias") {
		t.Fatal("exact direct model ID was rejected")
	}
	if gatewayModelIDExact([]string{"Vendor-Alias"}, "vendor-alias") {
		t.Fatal("case-insensitive direct model ID match was accepted")
	}
}
