package testdb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/migrations"
	"github.com/Harvey-Mei/BridgeOps/backend/testpostgres"
	postgresDriver "gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const testTemplateHarnessVersion = "postgres-v2"

func Open(t testing.TB) *gorm.DB {
	t.Helper()
	fingerprint, err := migrations.SchemaFingerprint()
	if err != nil {
		t.Fatalf("fingerprint PostgreSQL migrations: %v", err)
	}
	templateHash := sha256.Sum256([]byte(testTemplateHarnessVersion + "\n" + fingerprint))
	sqlDB, dsn := testpostgres.Open(t, hex.EncodeToString(templateHash[:8]), migrations.Apply)
	database, err := gorm.Open(postgresDriver.New(postgresDriver.Config{Conn: sqlDB, DSN: dsn}), &gorm.Config{
		Logger:  logger.Default.LogMode(logger.Silent),
		NowFunc: func() time.Time { return time.Now().UTC() },
	})
	if err != nil {
		t.Fatalf("open migrated PostgreSQL test database with GORM: %v", err)
	}
	return database.Set("bridgeops:database-location", time.UTC).Session(&gorm.Session{NewDB: true, Context: context.Background()})
}

func OpenWithError(t testing.TB) (*gorm.DB, error) {
	t.Helper()
	return Open(t), nil
}

func ReopenWithError(t testing.TB, database *gorm.DB) (*gorm.DB, error) {
	t.Helper()
	dialector, ok := database.Dialector.(*postgresDriver.Dialector)
	if !ok || dialector.Config == nil || dialector.Config.DSN == "" {
		return nil, gorm.ErrInvalidDB
	}
	reopened, err := gorm.Open(postgresDriver.Open(dialector.Config.DSN), &gorm.Config{
		Logger:  logger.Default.LogMode(logger.Silent),
		NowFunc: func() time.Time { return time.Now().UTC() },
	})
	if err != nil {
		return nil, err
	}
	sqlDB, err := reopened.DB()
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	return reopened.Set("bridgeops:database-location", time.UTC).Session(&gorm.Session{NewDB: true, Context: context.Background()}), nil
}
