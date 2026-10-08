package testpostgres

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestParseTestAdminURLRejectsConnectionTargetOverrides(t *testing.T) {
	base := "postgres://bridgeops_test@127.0.0.1:55432/bridgeops_test_admin"
	for _, query := range []string{
		"database=bridgeops",
		"dbname=bridgeops",
		"host=production.example",
		"hostaddr=203.0.113.10",
		"port=5432",
		"user=postgres",
		"service=production",
		"servicefile=/tmp/pg_service.conf",
		"options=-csearch_path%3Dpublic",
	} {
		t.Run(query, func(t *testing.T) {
			if _, err := parseTestAdminURL(base + "?" + query); err == nil || !strings.Contains(err.Error(), "override") {
				t.Fatalf("expected target override rejection for %q, got %v", query, err)
			}
		})
	}
}

func TestParseTestAdminURLRequiresExplicitReviewedTarget(t *testing.T) {
	for _, dsn := range []string{
		"postgres://bridgeops_test@127.0.0.1/bridgeops_test_admin",
		"postgres://127.0.0.1:55432/bridgeops_test_admin",
		"postgres://bridgeops_test@0.0.0.0:55432/bridgeops_test_admin",
		"postgres://bridgeops_test@127.0.0.1:55432/bridgeops",
	} {
		if _, err := parseTestAdminURL(dsn); err == nil {
			t.Fatalf("expected unsafe DSN rejection for %q", dsn)
		}
	}
}

func TestParseTestAdminURLAcceptsSafeConnectionOptions(t *testing.T) {
	parsed, err := parseTestAdminURL("postgres://bridgeops_test@127.0.0.1:55432/bridgeops_test_admin?sslmode=disable&connect_timeout=5")
	if err != nil {
		t.Fatalf("expected safe test DSN: %v", err)
	}
	if parsed.Path != "/bridgeops_test_admin" {
		t.Fatalf("unexpected parsed database path %q", parsed.Path)
	}
}

func TestTemplatePreparationAndCloneAreSerialized(t *testing.T) {
	const templateID = "concurrencyv1"
	templateName := templateDatabaseName(t, templateID)
	t.Cleanup(func() {
		adminURL := testAdminURL(t)
		admin, err := sql.Open("pgx", adminURL.String())
		if err != nil {
			t.Errorf("open admin database for template cleanup: %v", err)
			return
		}
		defer admin.Close()
		if err := dropDatabase(admin, templateName); err != nil {
			t.Errorf("drop concurrent test template: %v", err)
		}
	})

	for index := 0; index < 4; index++ {
		t.Run(string(rune('a'+index)), func(t *testing.T) {
			t.Parallel()
			database, _ := Open(t, templateID, func(ctx context.Context, database *sql.DB) error {
				if _, err := database.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS template_concurrency_probe (id integer PRIMARY KEY)"); err != nil {
					return err
				}
				time.Sleep(50 * time.Millisecond)
				return nil
			})
			var tableName string
			if err := database.QueryRowContext(context.Background(), "SELECT to_regclass('public.template_concurrency_probe')::text").Scan(&tableName); err != nil {
				t.Fatalf("inspect cloned template table: %v", err)
			}
			if tableName != "template_concurrency_probe" {
				t.Fatalf("cloned template table = %q", tableName)
			}
		})
	}
}

func TestStaleDatabaseCleanupRequiresExactMarkerNameAgeAndNoConnections(t *testing.T) {
	now := time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC)
	clone := "bridgeops_test_123_4_abcdef01"
	template := "bridgeops_test_template_deadbeef"
	oldCloneComment := managedDatabaseComment("clone", clone, now.Add(-staleDatabaseAge))
	oldTemplateComment := managedDatabaseComment("template", template, now.Add(-staleDatabaseAge-time.Second))
	for _, test := range []struct {
		name           string
		database       string
		comment        string
		hasConnections bool
		current        string
		want           bool
	}{
		{name: "old clone", database: clone, comment: oldCloneComment, want: true},
		{name: "old template", database: template, comment: oldTemplateComment, want: true},
		{name: "interrupted template", database: template, comment: managedDatabaseComment("template_initializing", template, now.Add(-staleDatabaseAge)), want: true},
		{name: "active clone", database: clone, comment: oldCloneComment, hasConnections: true},
		{name: "current template", database: template, comment: oldTemplateComment, current: template},
		{name: "fresh clone", database: clone, comment: managedDatabaseComment("clone", clone, now.Add(-time.Hour))},
		{name: "unmarked clone", database: clone, comment: ""},
		{name: "forged marker name", database: clone, comment: managedDatabaseComment("clone", clone+"x", now.Add(-staleDatabaseAge))},
		{name: "invalid clone name", database: "bridgeops_test_manual", comment: managedDatabaseComment("clone", "bridgeops_test_manual", now.Add(-staleDatabaseAge))},
		{name: "admin database", database: adminDatabase, comment: managedDatabaseComment("clone", adminDatabase, now.Add(-staleDatabaseAge))},
		{name: "future marker", database: clone, comment: managedDatabaseComment("clone", clone, now.Add(time.Second))},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := shouldCleanupManagedDatabase(test.database, test.comment, test.hasConnections, test.current, now)
			if got != test.want {
				t.Fatalf("shouldCleanupManagedDatabase() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestOpenEmptyRemovesOnlyMarkedStaleDatabase(t *testing.T) {
	adminURL := testAdminURL(t)
	admin, err := sql.Open("pgx", adminURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	verifyConnectedDatabase(t, admin, adminDatabase)
	staleName := uniqueDatabaseName(t)
	if _, err := admin.Exec("CREATE DATABASE " + quoteIdentifier(staleName) + " TEMPLATE template0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dropDatabase(admin, staleName) })
	comment := managedDatabaseComment("clone", staleName, time.Now().UTC().Add(-staleDatabaseAge-time.Minute))
	if _, err := admin.Exec("COMMENT ON DATABASE " + quoteIdentifier(staleName) + " IS " + quoteLiteral(comment)); err != nil {
		t.Fatal(err)
	}

	OpenEmpty(t)
	var exists bool
	if err := admin.QueryRow("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)", staleName).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatalf("marked stale database %s was not removed", staleName)
	}
}

func TestOpenEmptyPreservesMarkedStaleDatabaseWithActiveConnection(t *testing.T) {
	adminURL := testAdminURL(t)
	admin, err := sql.Open("pgx", adminURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	verifyConnectedDatabase(t, admin, adminDatabase)
	staleName := uniqueDatabaseName(t)
	if _, err := admin.Exec("CREATE DATABASE " + quoteIdentifier(staleName) + " TEMPLATE template0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dropDatabase(admin, staleName) })
	comment := managedDatabaseComment("clone", staleName, time.Now().UTC().Add(-staleDatabaseAge-time.Minute))
	if _, err := admin.Exec("COMMENT ON DATABASE " + quoteIdentifier(staleName) + " IS " + quoteLiteral(comment)); err != nil {
		t.Fatal(err)
	}

	database, err := sql.Open("pgx", cloneURL(adminURL, staleName).String())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	activeConnection, err := database.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer activeConnection.Close()
	if err := activeConnection.PingContext(context.Background()); err != nil {
		t.Fatal(err)
	}

	OpenEmpty(t)
	var currentDatabase string
	if err := activeConnection.QueryRowContext(context.Background(), "SELECT current_database()").Scan(&currentDatabase); err != nil {
		t.Fatalf("active stale database connection was terminated: %v", err)
	}
	if currentDatabase != staleName {
		t.Fatalf("active stale database connection moved to %q", currentDatabase)
	}
	var exists bool
	if err := admin.QueryRow("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)", staleName).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatalf("active stale database %s was removed", staleName)
	}
}

func TestManagedDatabaseDropSkipsNewActiveConnection(t *testing.T) {
	adminURL := testAdminURL(t)
	admin, err := sql.Open("pgx", adminURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	verifyConnectedDatabase(t, admin, adminDatabase)
	databaseName := uniqueDatabaseName(t)
	if _, err := admin.Exec("CREATE DATABASE " + quoteIdentifier(databaseName) + " TEMPLATE template0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dropDatabase(admin, databaseName) })

	database, err := sql.Open("pgx", cloneURL(adminURL, databaseName).String())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	activeConnection, err := database.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer activeConnection.Close()
	if err := activeConnection.PingContext(context.Background()); err != nil {
		t.Fatal(err)
	}

	dropped, err := dropManagedDatabase(admin, databaseName)
	if err != nil {
		t.Fatalf("drop active managed database: %v", err)
	}
	if dropped {
		t.Fatal("active managed database was dropped")
	}
	var exists bool
	if err := admin.QueryRow("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)", databaseName).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatalf("active managed database %s was not preserved", databaseName)
	}
}

func TestOpenRefusesAndPreservesUnmanagedTemplate(t *testing.T) {
	const helperEnvironment = "BRIDGEOPS_TEST_UNMANAGED_TEMPLATE_ID"
	if templateID := os.Getenv(helperEnvironment); templateID != "" {
		Open(t, templateID, func(context.Context, *sql.DB) error { return nil })
		return
	}

	templateID := "unmanaged" + strconv.Itoa(os.Getpid()) + strconv.FormatUint(databaseSequence.Add(1), 10)
	if len(templateID) > 24 {
		templateID = templateID[:24]
	}
	templateName := templateDatabaseName(t, templateID)
	adminURL := testAdminURL(t)
	admin, err := sql.Open("pgx", adminURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	verifyConnectedDatabase(t, admin, adminDatabase)
	if _, err := admin.Exec("CREATE DATABASE " + quoteIdentifier(templateName) + " TEMPLATE template0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dropDatabase(admin, templateName) })

	command := exec.Command(os.Args[0], "-test.run=^TestOpenRefusesAndPreservesUnmanagedTemplate$")
	command.Env = append(os.Environ(), helperEnvironment+"="+templateID)
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatal("unmanaged template helper unexpectedly succeeded")
	}
	if !strings.Contains(string(output), fmt.Sprintf("refusing to replace unmanaged PostgreSQL test template %s", templateName)) {
		t.Fatalf("helper did not report unmanaged template refusal: %s", output)
	}
	var exists bool
	if err := admin.QueryRow("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)", templateName).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatalf("unmanaged template %s was deleted", templateName)
	}
}

func TestOpenEmptyWaitsForSharedTemplateLock(t *testing.T) {
	const helperEnvironment = "BRIDGEOPS_TEST_OPEN_EMPTY_LOCK_HELPER"
	if os.Getenv(helperEnvironment) == "1" {
		OpenEmpty(t)
		return
	}

	adminURL := testAdminURL(t)
	admin, err := sql.Open("pgx", adminURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	verifyConnectedDatabase(t, admin, adminDatabase)
	lockConnection, err := admin.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer lockConnection.Close()
	if _, err := lockConnection.ExecContext(context.Background(), "SELECT pg_advisory_lock($1)", templateLockID); err != nil {
		t.Fatal(err)
	}
	locked := true
	defer func() {
		if locked {
			_, _ = lockConnection.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1)", templateLockID)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestOpenEmptyWaitsForSharedTemplateLock$")
	command.Env = append(os.Environ(), helperEnvironment+"=1")
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	completed := make(chan error, 1)
	go func() { completed <- command.Wait() }()
	select {
	case err := <-completed:
		t.Fatalf("OpenEmpty completed before the shared advisory lock was released: %v: %s", err, output.String())
	case <-time.After(300 * time.Millisecond):
	}
	if _, err := lockConnection.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1)", templateLockID); err != nil {
		t.Fatal(err)
	}
	locked = false
	select {
	case err := <-completed:
		if err != nil {
			t.Fatalf("OpenEmpty failed after the shared advisory lock was released: %v: %s", err, output.String())
		}
	case <-ctx.Done():
		t.Fatalf("OpenEmpty did not complete after the shared advisory lock was released: %s", output.String())
	}
}
