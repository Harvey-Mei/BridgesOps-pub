package testpostgres

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const (
	EnvironmentDSN   = "BRIDGEOPS_TEST_POSTGRES_DSN"
	adminDatabase    = "bridgeops_test_admin"
	templateLockID   = int64(4992331798257801)
	managedMarker    = "bridgeops managed test database v1"
	staleDatabaseAge = 24 * time.Hour
)

var forbiddenConnectionOverrides = map[string]struct{}{
	"database":    {},
	"dbname":      {},
	"host":        {},
	"hostaddr":    {},
	"options":     {},
	"port":        {},
	"service":     {},
	"servicefile": {},
	"user":        {},
}

var databaseSequence atomic.Uint64

type Initializer func(context.Context, *sql.DB) error

func Open(t testing.TB, templateID string, initialize Initializer) (*sql.DB, string) {
	t.Helper()
	if initialize == nil {
		t.Fatal("PostgreSQL test initializer is required")
	}
	templateName := templateDatabaseName(t, templateID)
	adminURL := testAdminURL(t)
	admin, err := sql.Open("pgx", adminURL.String())
	if err != nil {
		t.Fatalf("open PostgreSQL test admin database: %v", err)
	}
	// Register teardown before any step that can t.Fatalf. Cleanups run LIFO,
	// so the admin pool closes after the cloned database is dropped.
	t.Cleanup(func() {
		_ = admin.Close()
	})
	if err := admin.PingContext(context.Background()); err != nil {
		t.Fatalf("connect PostgreSQL test admin database: %v", err)
	}
	verifyConnectedDatabase(t, admin, adminDatabase)
	databaseName := uniqueDatabaseName(t)
	t.Cleanup(func() {
		if err := dropDatabase(admin, databaseName); err != nil {
			t.Errorf("drop PostgreSQL test database %s: %v", databaseName, err)
		}
	})
	prepareTemplateClone(t, admin, adminURL, templateName, databaseName, initialize)
	databaseURL := cloneURL(adminURL, databaseName)
	database, err := sql.Open("pgx", databaseURL.String())
	if err != nil {
		t.Fatalf("open cloned PostgreSQL test database: %v", err)
	}
	t.Cleanup(func() {
		_ = database.Close()
	})
	if err := database.PingContext(context.Background()); err != nil {
		t.Fatalf("connect cloned PostgreSQL test database: %v", err)
	}
	verifyConnectedDatabase(t, database, databaseName)
	return database, databaseURL.String()
}

func OpenEmpty(t testing.TB) (*sql.DB, string) {
	t.Helper()
	adminURL := testAdminURL(t)
	admin, err := sql.Open("pgx", adminURL.String())
	if err != nil {
		t.Fatalf("open PostgreSQL test admin database: %v", err)
	}
	t.Cleanup(func() {
		_ = admin.Close()
	})
	if err := admin.PingContext(context.Background()); err != nil {
		t.Fatalf("connect PostgreSQL test admin database: %v", err)
	}
	verifyConnectedDatabase(t, admin, adminDatabase)
	databaseName := uniqueDatabaseName(t)
	t.Cleanup(func() {
		if err := dropDatabase(admin, databaseName); err != nil {
			t.Errorf("drop empty PostgreSQL test database %s: %v", databaseName, err)
		}
	})
	prepareEmptyClone(t, admin, databaseName)
	databaseURL := cloneURL(adminURL, databaseName)
	database, err := sql.Open("pgx", databaseURL.String())
	if err != nil {
		t.Fatalf("open empty PostgreSQL test database: %v", err)
	}
	t.Cleanup(func() {
		_ = database.Close()
	})
	if err := database.PingContext(context.Background()); err != nil {
		t.Fatalf("connect empty PostgreSQL test database: %v", err)
	}
	verifyConnectedDatabase(t, database, databaseName)
	return database, databaseURL.String()
}

func prepareTemplateClone(t testing.TB, admin *sql.DB, adminURL *url.URL, templateName, databaseName string, initialize Initializer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	connection, err := admin.Conn(ctx)
	if err != nil {
		t.Fatalf("acquire PostgreSQL template lock connection: %v", err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(ctx, "SELECT pg_advisory_lock($1)", templateLockID); err != nil {
		t.Fatalf("lock PostgreSQL test template: %v", err)
	}
	defer func() {
		_, _ = connection.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1)", templateLockID)
	}()
	now := time.Now().UTC()
	cleanupStaleManagedDatabases(t, ctx, connection, admin, templateName, now)

	var exists bool
	var comment sql.NullString
	if err := connection.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1), COALESCE((SELECT shobj_description(oid, 'pg_database') FROM pg_database WHERE datname = $1), '')", templateName).Scan(&exists, &comment); err != nil {
		t.Fatalf("inspect PostgreSQL test template: %v", err)
	}
	if exists && (!comment.Valid || !validManagedDatabaseComment(comment.String, "template", templateName)) {
		if comment.Valid && validManagedDatabaseComment(comment.String, "template_initializing", templateName) {
			dropped, err := dropManagedDatabase(admin, templateName)
			if err != nil {
				t.Fatalf("replace interrupted PostgreSQL test template: %v", err)
			}
			if !dropped {
				t.Fatalf("refusing to replace interrupted PostgreSQL test template %s while it is in use", templateName)
			}
			exists = false
		} else {
			t.Fatalf("refusing to replace unmanaged PostgreSQL test template %s", templateName)
		}
	}
	if !exists {
		if _, err := connection.ExecContext(ctx, "CREATE DATABASE "+quoteIdentifier(templateName)+" TEMPLATE template0"); err != nil {
			t.Fatalf("create PostgreSQL test template: %v", err)
		}
		if err := markManagedDatabase(ctx, connection, "template_initializing", templateName, now); err != nil {
			dropDatabase(admin, templateName)
			t.Fatalf("mark initializing PostgreSQL test template: %v", err)
		}
	}
	templateURL := cloneURL(adminURL, templateName)
	template, err := sql.Open("pgx", templateURL.String())
	if err != nil {
		t.Fatalf("open PostgreSQL test template: %v", err)
	}
	if err := template.PingContext(ctx); err != nil {
		_ = template.Close()
		t.Fatalf("connect PostgreSQL test template: %v", err)
	}
	verifyConnectedDatabase(t, template, templateName)
	if err := initialize(ctx, template); err != nil {
		_ = template.Close()
		t.Fatalf("initialize PostgreSQL test template: %v", err)
	}
	if err := template.Close(); err != nil {
		t.Fatalf("close PostgreSQL test template: %v", err)
	}
	if err := markManagedDatabase(ctx, connection, "template", templateName, now); err != nil {
		t.Fatalf("mark PostgreSQL test template initialized: %v", err)
	}
	if _, err := connection.ExecContext(ctx, "CREATE DATABASE "+quoteIdentifier(databaseName)+" TEMPLATE "+quoteIdentifier(templateName)); err != nil {
		t.Fatalf("clone PostgreSQL test database: %v", err)
	}
	if err := markManagedDatabase(ctx, connection, "clone", databaseName, now); err != nil {
		dropDatabase(admin, databaseName)
		t.Fatalf("mark cloned PostgreSQL test database: %v", err)
	}
}

func prepareEmptyClone(t testing.TB, admin *sql.DB, databaseName string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	connection, err := admin.Conn(ctx)
	if err != nil {
		t.Fatalf("acquire PostgreSQL empty clone lock connection: %v", err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(ctx, "SELECT pg_advisory_lock($1)", templateLockID); err != nil {
		t.Fatalf("lock PostgreSQL empty test clone: %v", err)
	}
	defer func() {
		_, _ = connection.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1)", templateLockID)
	}()
	now := time.Now().UTC()
	cleanupStaleManagedDatabases(t, ctx, connection, admin, "", now)
	if _, err := connection.ExecContext(ctx, "CREATE DATABASE "+quoteIdentifier(databaseName)+" TEMPLATE template0"); err != nil {
		t.Fatalf("create empty PostgreSQL test database: %v", err)
	}
	if err := markManagedDatabase(ctx, connection, "clone", databaseName, now); err != nil {
		dropDatabase(admin, databaseName)
		t.Fatalf("mark empty PostgreSQL test database: %v", err)
	}
}

func cleanupStaleManagedDatabases(t testing.TB, ctx context.Context, connection *sql.Conn, admin *sql.DB, currentTemplate string, now time.Time) {
	t.Helper()
	rows, err := connection.QueryContext(ctx, `SELECT d.datname,
COALESCE(shobj_description(d.oid, 'pg_database'), ''),
EXISTS (SELECT 1 FROM pg_stat_activity a WHERE a.datname = d.datname)
FROM pg_database d WHERE left(d.datname, length($1)) = $1`, "bridgeops_test_")
	if err != nil {
		t.Fatalf("inspect stale PostgreSQL test databases: %v", err)
	}
	var stale []string
	for rows.Next() {
		var name, comment string
		var hasConnections bool
		if err := rows.Scan(&name, &comment, &hasConnections); err != nil {
			_ = rows.Close()
			t.Fatalf("read stale PostgreSQL test database: %v", err)
		}
		if shouldCleanupManagedDatabase(name, comment, hasConnections, currentTemplate, now) {
			stale = append(stale, name)
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		t.Fatalf("inspect stale PostgreSQL test databases: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close stale PostgreSQL test database rows: %v", err)
	}
	for _, name := range stale {
		if _, err := dropManagedDatabase(admin, name); err != nil {
			t.Fatalf("drop stale PostgreSQL test database %s: %v", name, err)
		}
	}
}

func markManagedDatabase(ctx context.Context, connection *sql.Conn, kind, name string, now time.Time) error {
	comment := managedDatabaseComment(kind, name, now)
	_, err := connection.ExecContext(ctx, "COMMENT ON DATABASE "+quoteIdentifier(name)+" IS "+quoteLiteral(comment))
	return err
}

func managedDatabaseComment(kind, name string, now time.Time) string {
	return managedMarker + " kind=" + kind + " name=" + name + " touched_at=" + now.UTC().Format(time.RFC3339Nano)
}

func validManagedDatabaseComment(comment, kind, name string) bool {
	_, ok := managedDatabaseTimestamp(comment, kind, name)
	return ok
}

func managedDatabaseTimestamp(comment, kind, name string) (time.Time, bool) {
	prefix := managedMarker + " kind=" + kind + " name=" + name + " touched_at="
	if !strings.HasPrefix(comment, prefix) {
		return time.Time{}, false
	}
	raw := strings.TrimPrefix(comment, prefix)
	if raw == "" || strings.ContainsAny(raw, " \t\r\n") {
		return time.Time{}, false
	}
	timestamp, err := time.Parse(time.RFC3339Nano, raw)
	return timestamp, err == nil
}

func shouldCleanupManagedDatabase(name, comment string, hasConnections bool, currentTemplate string, now time.Time) bool {
	if name == adminDatabase || name == currentTemplate || hasConnections {
		return false
	}
	kind := managedDatabaseKind(name)
	if kind == "" {
		return false
	}
	markerKind := kind
	touchedAt, ok := managedDatabaseTimestamp(comment, markerKind, name)
	if !ok && kind == "template" {
		markerKind = "template_initializing"
		touchedAt, ok = managedDatabaseTimestamp(comment, markerKind, name)
	}
	if !ok || touchedAt.After(now) {
		return false
	}
	return now.Sub(touchedAt) >= staleDatabaseAge
}

func managedDatabaseKind(name string) string {
	if suffix, ok := strings.CutPrefix(name, "bridgeops_test_template_"); ok {
		if validLowerAlphanumeric(suffix, 1, 24) {
			return "template"
		}
		return ""
	}
	suffix, ok := strings.CutPrefix(name, "bridgeops_test_")
	if !ok {
		return ""
	}
	parts := strings.Split(suffix, "_")
	if len(parts) != 3 || !validPositiveDecimal(parts[0]) || !validPositiveDecimal(parts[1]) || len(parts[2]) != 8 {
		return ""
	}
	for _, character := range parts[2] {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return ""
		}
	}
	return "clone"
}

func validLowerAlphanumeric(value string, minimum, maximum int) bool {
	if len(value) < minimum || len(value) > maximum {
		return false
	}
	for _, character := range value {
		if (character < 'a' || character > 'z') && (character < '0' || character > '9') {
			return false
		}
	}
	return true
}

func validPositiveDecimal(value string) bool {
	if value == "" || value[0] < '1' || value[0] > '9' {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return false
		}
	}
	return true
}

func testAdminURL(t testing.TB) *url.URL {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv(EnvironmentDSN))
	if raw == "" {
		t.Fatalf("%s is required and must point to a dedicated %s database", EnvironmentDSN, adminDatabase)
	}
	parsed, err := parseTestAdminURL(raw)
	if err != nil {
		t.Fatalf("validate %s: %v", EnvironmentDSN, err)
	}
	return parsed
}

func parseTestAdminURL(raw string) (*url.URL, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("parse URL: %w", err)
	}
	if parsed.Scheme != "postgres" && parsed.Scheme != "postgresql" {
		return nil, errors.New("must use postgres scheme")
	}
	if strings.TrimPrefix(parsed.EscapedPath(), "/") != adminDatabase {
		return nil, fmt.Errorf("must point to database %q", adminDatabase)
	}
	host := parsed.Hostname()
	if host == "" {
		return nil, errors.New("must include an explicit host")
	}
	if address := net.ParseIP(host); address != nil && address.IsUnspecified() {
		return nil, errors.New("cannot use an unspecified host")
	}
	portText := parsed.Port()
	if portText == "" {
		return nil, errors.New("must include an explicit port")
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil || port == 0 {
		return nil, errors.New("must include a valid explicit port")
	}
	if parsed.User == nil || strings.TrimSpace(parsed.User.Username()) == "" {
		return nil, errors.New("must include an explicit user")
	}
	for key := range parsed.Query() {
		if _, forbidden := forbiddenConnectionOverrides[strings.ToLower(strings.TrimSpace(key))]; forbidden {
			return nil, fmt.Errorf("query parameter %q can override the reviewed connection target", key)
		}
	}
	config, err := pgx.ParseConfig(raw)
	if err != nil {
		return nil, fmt.Errorf("parse final pgx config: %w", err)
	}
	if config.Database != adminDatabase || config.Host != host || config.Port != uint16(port) || config.User != parsed.User.Username() {
		return nil, errors.New("final pgx connection target does not match the explicit URL")
	}
	return parsed, nil
}

func cloneURL(source *url.URL, databaseName string) *url.URL {
	cloned := *source
	cloned.Path = "/" + databaseName
	cloned.RawPath = ""
	return &cloned
}

func uniqueDatabaseName(t testing.TB) string {
	t.Helper()
	randomBytes := make([]byte, 4)
	if _, err := rand.Read(randomBytes); err != nil {
		t.Fatalf("generate PostgreSQL test database suffix: %v", err)
	}
	return "bridgeops_test_" + strconv.Itoa(os.Getpid()) + "_" + strconv.FormatUint(databaseSequence.Add(1), 10) + "_" + hex.EncodeToString(randomBytes)
}

func templateDatabaseName(t testing.TB, templateID string) string {
	t.Helper()
	templateID = strings.ToLower(strings.TrimSpace(templateID))
	if templateID == "" || len(templateID) > 24 {
		t.Fatal("PostgreSQL test template ID must contain 1-24 lowercase letters or digits")
	}
	for _, character := range templateID {
		if (character < 'a' || character > 'z') && (character < '0' || character > '9') {
			t.Fatal("PostgreSQL test template ID must contain only lowercase letters or digits")
		}
	}
	return "bridgeops_test_template_" + templateID
}

func verifyConnectedDatabase(t testing.TB, database *sql.DB, expected string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var actual string
	if err := database.QueryRowContext(ctx, "SELECT current_database()").Scan(&actual); err != nil {
		t.Fatalf("verify connected PostgreSQL database: %v", err)
	}
	if actual != expected {
		t.Fatalf("refusing PostgreSQL test connection to database %q; expected %q", actual, expected)
	}
}

func dropDatabase(admin *sql.DB, databaseName string) error {
	if err := validateDropTarget(admin, databaseName); err != nil {
		return err
	}
	// Ordinary DROP handles autovacuum internally. FORCE additionally checks
	// signal privileges on its worker and can fail for our non-superuser role.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for {
		_, err := admin.ExecContext(ctx, "DROP DATABASE IF EXISTS "+quoteIdentifier(databaseName))
		var postgresError *pgconn.PgError
		if !errors.As(err, &postgresError) || postgresError.Code != "55006" {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for test database connections to close: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func dropManagedDatabase(admin *sql.DB, databaseName string) (bool, error) {
	if err := validateDropTarget(admin, databaseName); err != nil {
		return false, err
	}
	_, err := admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+quoteIdentifier(databaseName))
	if err == nil {
		return true, nil
	}
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) && postgresError.Code == "55006" {
		return false, nil
	}
	return false, err
}

func validateDropTarget(admin *sql.DB, databaseName string) error {
	if admin == nil || managedDatabaseKind(databaseName) == "" || databaseName == adminDatabase {
		return errors.New("refusing to drop non-test PostgreSQL database")
	}
	return nil
}

func quoteIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func quoteLiteral(value string) string {
	return `'` + strings.ReplaceAll(value, `'`, `''`) + `'`
}
