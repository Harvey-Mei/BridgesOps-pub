package api

import (
	"testing"
	"time"
)

func TestParseGatewayRequestLogTimeNormalizesRFC3339Offset(t *testing.T) {
	got, err := parseGatewayRequestLogTime("2026-07-19T16:30:00+08:00", time.UTC)
	if err != nil {
		t.Fatalf("parse RFC3339 offset: %v", err)
	}
	want := time.Date(2026, 7, 19, 8, 30, 0, 0, time.UTC)
	if !got.Equal(want) || got.Location() != time.UTC {
		t.Fatalf("parsed time = %s (%s), want %s UTC", got, got.Location(), want)
	}
}

func TestParseGatewayRequestLogDateUsesConfiguredTimeZone(t *testing.T) {
	location, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	got, err := parseGatewayRequestLogTime("2026-07-19", location)
	if err != nil {
		t.Fatalf("parse configured date: %v", err)
	}
	if got.Location() != location || got.Hour() != 0 || got.Day() != 19 {
		t.Fatalf("parsed date = %s (%s)", got, got.Location())
	}
}
