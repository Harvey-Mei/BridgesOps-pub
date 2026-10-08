package usertime

import (
	"fmt"
	"strings"
	"sync"
	"time"
	_ "time/tzdata"
)

const DefaultTimeZone = "Asia/Shanghai"

var defaultService = func() *Service {
	service, err := New(DefaultTimeZone)
	if err != nil {
		panic(err)
	}
	return service
}()

// Service owns the single user-facing timezone used for business-day
// boundaries. Stored timestamps remain absolute instants; callers convert the
// resulting local boundaries to UTC before querying.
type Service struct {
	mu       sync.RWMutex
	name     string
	location *time.Location
}

func New(name string) (*Service, error) {
	service := &Service{}
	if err := service.Set(name); err != nil {
		return nil, err
	}
	return service, nil
}

func Default() *Service {
	return defaultService
}

func (s *Service) Set(name string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		name = DefaultTimeZone
	}
	location, err := time.LoadLocation(name)
	if err != nil {
		return fmt.Errorf("load user timezone %q: %w", name, err)
	}
	if s == nil {
		return fmt.Errorf("user timezone service is nil")
	}
	s.mu.Lock()
	s.name = name
	s.location = location
	s.mu.Unlock()
	return nil
}

func (s *Service) Name() string {
	if s == nil {
		return DefaultTimeZone
	}
	s.mu.RLock()
	name := s.name
	s.mu.RUnlock()
	if name == "" {
		return DefaultTimeZone
	}
	return name
}

func (s *Service) Location() *time.Location {
	if s == nil {
		location, _ := time.LoadLocation(DefaultTimeZone)
		return location
	}
	s.mu.RLock()
	location := s.location
	s.mu.RUnlock()
	if location != nil {
		return location
	}
	location, _ = time.LoadLocation(DefaultTimeZone)
	return location
}

func (s *Service) Now() time.Time {
	return time.Now().In(s.Location())
}

func (s *Service) DayStart(value time.Time) time.Time {
	location := s.Location()
	local := value.In(location)
	year, month, day := local.Date()
	return time.Date(year, month, day, 0, 0, 0, 0, location)
}

func (s *Service) DayBoundsUTC(value time.Time) (time.Time, time.Time) {
	start := s.DayStart(value)
	return start.UTC(), start.AddDate(0, 0, 1).UTC()
}

func (s *Service) IsToday(value *time.Time, now time.Time) bool {
	if value == nil {
		return false
	}
	return s.DayStart(*value).Equal(s.DayStart(now))
}
