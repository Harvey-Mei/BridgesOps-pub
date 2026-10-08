package storage

import (
	"errors"
	"fmt"
	"log"
	"math"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	postgresDriver "gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type DBDriver string

const (
	DBDriverPostgres DBDriver = "postgres"
)

type DBConfig struct {
	Driver       DBDriver
	Host         string
	Port         int
	User         string
	Password     string
	Name         string
	TimeZone     string
	SSLMode      string
	MaxOpenConns int
	MaxIdleConns int
}

// PostgresDSN keeps PostgreSQL storage timestamps in UTC. Business-day and
// presentation time zones are handled separately by usertime.Service.
func (c DBConfig) PostgresDSN() (string, error) {
	if _, err := c.postgresLocation(); err != nil {
		return "", err
	}
	host := strings.TrimSpace(c.Host)
	user := strings.TrimSpace(c.User)
	name := strings.TrimSpace(c.Name)
	if host == "" || user == "" || name == "" {
		return "", errors.New("postgres host, user and database name are required")
	}
	port := c.Port
	if port <= 0 {
		port = 5432
	}
	sslMode := strings.ToLower(strings.TrimSpace(c.SSLMode))
	if sslMode == "" {
		sslMode = "disable"
	}
	switch sslMode {
	case "disable", "allow", "prefer", "require", "verify-ca", "verify-full":
	default:
		return "", fmt.Errorf("postgres sslMode %q is invalid", c.SSLMode)
	}
	connection := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(user, c.Password),
		Host:   net.JoinHostPort(host, strconv.Itoa(port)),
		Path:   "/" + name,
	}
	params := url.Values{}
	params.Set("sslmode", sslMode)
	params.Set("TimeZone", "UTC")
	params.Set("search_path", "public")
	connection.RawQuery = params.Encode()
	return connection.String(), nil
}

func (c DBConfig) postgresLocation() (*time.Location, error) {
	name := strings.TrimSpace(c.TimeZone)
	if name == "" || strings.EqualFold(name, "UTC") {
		return time.UTC, nil
	}
	return nil, errors.New("database.timeZone must be UTC for PostgreSQL")
}

const databaseLocationSetting = "bridgeops:database-location"

func databaseLocation(db *gorm.DB) *time.Location {
	if db != nil {
		if configured, ok := db.Get(databaseLocationSetting); ok {
			if location, valid := configured.(*time.Location); valid && location != nil {
				return location
			}
		}
	}
	return time.UTC
}

// newGormLogger 关掉 GORM 默认 logger 对 ErrRecordNotFound 的告警噪音。
//
// 业务代码（如 Rates.Upsert）显式处理了"找不到就插入"，这种情况下 GORM 默认仍会
// 把 record not found 当 Warn 打出来，造成日志看起来满是错误其实没问题。
// IgnoreRecordNotFoundError = true 可以静默这类预期内的"未找到"。
func newGormLogger() logger.Interface {
	return logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags),
		logger.Config{
			SlowThreshold:             200 * time.Millisecond,
			LogLevel:                  logger.Warn,
			IgnoreRecordNotFoundError: true,
			Colorful:                  true,
		},
	)
}

func Open(cfg DBConfig) (*gorm.DB, error) {
	driver := DBDriver(strings.ToLower(string(cfg.Driver)))
	if driver == "" {
		driver = DBDriverPostgres
	}
	if driver != DBDriverPostgres {
		return nil, fmt.Errorf("unsupported database driver %q: BridgeOps requires PostgreSQL", cfg.Driver)
	}
	if err := validateDBConnectionPool(cfg.MaxOpenConns); err != nil {
		return nil, err
	}

	dsn, err := cfg.PostgresDSN()
	if err != nil {
		return nil, err
	}
	databaseTimeZone := time.UTC
	dialector := postgresDriver.Open(dsn)

	db, err := gorm.Open(dialector, &gorm.Config{
		Logger:  newGormLogger(),
		NowFunc: func() time.Time { return time.Now().UTC() },
	})
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("get sql.DB: %w", err)
	}
	sqlDB.SetMaxOpenConns(cfg.MaxOpenConns)
	sqlDB.SetMaxIdleConns(cfg.MaxIdleConns)

	return db.Set(databaseLocationSetting, databaseTimeZone).Session(&gorm.Session{NewDB: true}), nil
}

func validateDBConnectionPool(maxOpenConns int) error {
	if maxOpenConns == 1 {
		return errors.New("postgres database.maxOpenConns must be 0 (unlimited) or at least 2 so gateway advisory locks leave query capacity")
	}
	return nil
}

func gatewayRequestCostStatusSQLParts(alias string) (status string, reconciledEvidence string, anyEvidence string) {
	prefix := ""
	if strings.TrimSpace(alias) != "" {
		prefix = strings.TrimSpace(alias) + "."
	}
	status = "LOWER(TRIM(COALESCE(" + prefix + "cost_status, '')))"
	reconciledEvidence = "((" + prefix + "upstream_usage_id IS NOT NULL AND TRIM(" + prefix + "upstream_usage_id) <> '') OR LOWER(TRIM(COALESCE(" + prefix + "cost_source, ''))) IN (" + gatewayReconciledCostSourcesSQLList + "))"
	anyEvidence = "(" + reconciledEvidence + " OR " + prefix + "cost_estimated OR " + prefix + "cost <> 0 OR TRIM(COALESCE(" + prefix + "cost_source, '')) <> '')"
	return status, reconciledEvidence, anyEvidence
}

func gatewayRequestLegacyUnverifiedEstimateSQL(alias string) string {
	status, reconciledEvidence, _ := gatewayRequestCostStatusSQLParts(alias)
	prefix := ""
	if strings.TrimSpace(alias) != "" {
		prefix = strings.TrimSpace(alias) + "."
	}
	return "(" + gatewayRequestRecordSQL(alias) +
		" AND " + prefix + "price_fact_id IS NULL" +
		" AND NOT " + reconciledEvidence +
		" AND " + status + " NOT IN ('" + string(GatewayCostStatusReported) + "', '" + string(GatewayCostStatusReconciled) + "')" +
		" AND (" + status + " = '" + string(GatewayCostStatusEstimated) + "'" +
		" OR COALESCE(" + prefix + "cost_estimated, FALSE)" +
		" OR LOWER(TRIM(COALESCE(" + prefix + "cost_source, ''))) IN (" + gatewayLegacyUnverifiedEstimateSourcesSQLList + ")))"
}

func gatewayRequestEffectiveCostStatusSQL(alias string) string {
	status, reconciledEvidence, anyEvidence := gatewayRequestCostStatusSQLParts(alias)
	prefix := ""
	if strings.TrimSpace(alias) != "" {
		prefix = strings.TrimSpace(alias) + "."
	}
	legacyUnclassified := "(COALESCE(" + prefix + "cost_reconcile_legacy_pending, FALSE)" +
		" AND NOT " + reconciledEvidence +
		" AND NOT COALESCE(" + prefix + "cost_estimated, FALSE)" +
		" AND TRIM(COALESCE(" + prefix + "cost_source, '')) = '')"
	return "CASE" +
		" WHEN " + status + " = '" + string(GatewayCostStatusUnavailable) + "' THEN " + status +
		" WHEN " + gatewayRequestLegacyUnverifiedEstimateSQL(alias) + " THEN '" + string(GatewayCostStatusUnavailable) + "'" +
		" WHEN " + status + " IN ('" + string(GatewayCostStatusEstimated) + "', '" + string(GatewayCostStatusReported) + "', '" + string(GatewayCostStatusReconciled) + "') THEN " + status +
		" WHEN " + legacyUnclassified + " THEN '" + string(GatewayCostStatusNone) + "'" +
		" WHEN " + status + " = '" + string(GatewayCostStatusNone) + "' AND NOT " + anyEvidence + " THEN '" + string(GatewayCostStatusNone) + "'" +
		" WHEN " + reconciledEvidence + " THEN '" + string(GatewayCostStatusReconciled) + "'" +
		" WHEN " + prefix + "cost_estimated THEN '" + string(GatewayCostStatusEstimated) + "'" +
		" WHEN " + prefix + "cost <> 0 OR TRIM(COALESCE(" + prefix + "cost_source, '')) <> '' THEN '" + string(GatewayCostStatusReported) + "'" +
		" ELSE '" + string(GatewayCostStatusNone) + "' END"
}

func legacyFundingFactor(multiplier *float64, mode string) float64 {
	if multiplier == nil || *multiplier <= 0 || math.IsNaN(*multiplier) || math.IsInf(*multiplier, 0) {
		return 1
	}
	factor := *multiplier
	if !strings.EqualFold(strings.TrimSpace(mode), "multiply") {
		factor = 1 / *multiplier
	}
	if factor <= 0 || math.IsNaN(factor) || math.IsInf(factor, 0) {
		return 1
	}
	return factor
}
