package usertime

import (
	"testing"
	"time"
)

func TestDayBoundsUTCUseConfiguredTimeZone(t *testing.T) {
	service, err := New("Asia/Shanghai")
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	now := time.Date(2026, 7, 18, 1, 30, 0, 0, time.FixedZone("server", -5*60*60))
	start, end := service.DayBoundsUTC(now)
	if want := time.Date(2026, 7, 17, 16, 0, 0, 0, time.UTC); !start.Equal(want) {
		t.Fatalf("start = %s, want %s", start, want)
	}
	if want := time.Date(2026, 7, 18, 16, 0, 0, 0, time.UTC); !end.Equal(want) {
		t.Fatalf("end = %s, want %s", end, want)
	}
}

func TestSetChangesBusinessDayWithoutServerTimeZone(t *testing.T) {
	service, err := New("UTC")
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	instant := time.Date(2026, 7, 18, 16, 30, 0, 0, time.UTC)
	if got := service.DayStart(instant).Format(time.DateOnly); got != "2026-07-18" {
		t.Fatalf("UTC day = %s", got)
	}
	if err := service.Set("Asia/Shanghai"); err != nil {
		t.Fatalf("set timezone: %v", err)
	}
	if got := service.DayStart(instant).Format(time.DateOnly); got != "2026-07-19" {
		t.Fatalf("Shanghai day = %s", got)
	}
}
