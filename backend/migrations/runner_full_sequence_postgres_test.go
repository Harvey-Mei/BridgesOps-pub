package migrations

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/testpostgres"
)

func TestApplyEmbeddedMigrationsFullSequenceOnEmptyPostgres(t *testing.T) {
	if strings.TrimSpace(os.Getenv(testpostgres.EnvironmentDSN)) == "" {
		t.Skipf("%s is not configured", testpostgres.EnvironmentDSN)
	}

	expected, err := loadEmbeddedMigrations()
	if err != nil {
		t.Fatalf("load embedded migrations: %v", err)
	}
	db, _ := testpostgres.OpenEmpty(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	if err := Apply(ctx, db); err != nil {
		t.Fatalf("apply full embedded migration sequence: %v", err)
	}

	var count, minimumVersion, maximumVersion int64
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*), MIN(version), MAX(version)
		FROM public.schema_migrations
	`).Scan(&count, &minimumVersion, &maximumVersion); err != nil {
		t.Fatalf("read migration manifest bounds: %v", err)
	}
	if count != int64(len(expected)) {
		t.Fatalf("applied migration count = %d, want %d", count, len(expected))
	}
	if minimumVersion != 1 {
		t.Fatalf("minimum applied migration version = %06d, want 000001", minimumVersion)
	}
	if maximumVersion != expected[len(expected)-1].version {
		t.Fatalf("maximum applied migration version = %06d, want %06d", maximumVersion, expected[len(expected)-1].version)
	}

	rows, err := db.QueryContext(ctx, `
		SELECT version, checksum
		FROM public.schema_migrations
		ORDER BY version
	`)
	if err != nil {
		t.Fatalf("read applied migration manifest: %v", err)
	}
	defer rows.Close()

	index := 0
	for rows.Next() {
		if index >= len(expected) {
			t.Fatalf("database returned more than %d applied migrations", len(expected))
		}
		var version int64
		var checksum string
		if err := rows.Scan(&version, &checksum); err != nil {
			t.Fatalf("scan applied migration %d: %v", index, err)
		}
		want := expected[index]
		if version != want.version || checksum != want.checksum {
			t.Fatalf("applied migration %d = (%06d, %q), want (%06d, %q)", index, version, checksum, want.version, want.checksum)
		}
		index++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate applied migration manifest: %v", err)
	}
	if index != len(expected) {
		t.Fatalf("applied migration rows = %d, want %d", index, len(expected))
	}
}
