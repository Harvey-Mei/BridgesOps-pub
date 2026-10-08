package migrations

import (
	"context"
	"database/sql"
	"os"
	"regexp"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/Harvey-Mei/BridgeOps/backend/testpostgres"
)

type priceFactConstraintValidation struct {
	tableName      string
	constraintName string
	constraintType string
}

var priceFactConstraintValidations = []priceFactConstraintValidation{
	{tableName: "gateway_request_logs", constraintName: "fk_gateway_request_logs_price_fact", constraintType: "f"},
	{tableName: "gateway_request_logs", constraintName: "chk_gateway_request_logs_price_identity_nonnegative", constraintType: "c"},
	{tableName: "gateway_request_logs", constraintName: "chk_gateway_request_logs_price_values", constraintType: "c"},
	{tableName: "gateway_request_logs", constraintName: "chk_gateway_request_logs_price_fact_shape", constraintType: "c"},
	{tableName: "gateway_request_logs", constraintName: "chk_gateway_request_logs_usage_schema_shape", constraintType: "c"},
	{tableName: "gateway_request_logs", constraintName: "chk_gateway_request_logs_unavailable_cost_shape", constraintType: "c"},
	{tableName: "gateway_request_dispatch_audits", constraintName: "fk_gateway_request_dispatch_price_fact", constraintType: "f"},
	{tableName: "gateway_request_dispatch_audits", constraintName: "chk_gateway_request_dispatch_price_identity_nonnegative", constraintType: "c"},
	{tableName: "gateway_request_dispatch_audits", constraintName: "chk_gateway_request_dispatch_price_values", constraintType: "c"},
	{tableName: "gateway_request_dispatch_audits", constraintName: "chk_gateway_request_dispatch_price_fact_shape", constraintType: "c"},
	{tableName: "gateway_request_dispatch_audits", constraintName: "chk_gateway_request_dispatch_usage_schema_shape", constraintType: "c"},
	{tableName: "gateway_request_dispatch_audits", constraintName: "chk_gateway_request_dispatch_unavailable_reason", constraintType: "c"},
	{tableName: "gateway_request_aggregates", constraintName: "chk_gateway_request_aggregates_unavailable_requests", constraintType: "c"},
}

var priceFactConstraintValidationStatement = regexp.MustCompile(
	`(?is)\balter\s+table\s+public\.([a-z0-9_]+)\s+validate\s+constraint\s+([a-z0-9_]+)\s*;`,
)

var (
	priceFactAlterTableStatement = regexp.MustCompile(
		`(?is)\balter\s+table\s+public\.([a-z0-9_]+)\s+([^;]*);`,
	)
	priceFactAddConstraint  = regexp.MustCompile(`(?is)\badd\s+constraint\s+`)
	priceFactConstraintName = regexp.MustCompile(`(?is)^\s*([a-z0-9_]+)\b`)
	priceFactNotValid       = regexp.MustCompile(`(?is)\bnot\s+valid\b`)
)

func TestGatewayRequestPriceFactConstraintsValidationMigration(t *testing.T) {
	sourceBody, err := embeddedMigrations.ReadFile("000045_gateway_request_price_facts.sql")
	if err != nil {
		t.Fatal(err)
	}
	deferred := deferredPriceFactConstraints(t, strings.ToLower(string(sourceBody)))
	expected := priceFactConstraintValidationSet(t)
	assertPriceFactConstraintSet(t, "000045 deferred", deferred, expected)
	if got := len(priceFactNotValid.FindAll(sourceBody, -1)); got != len(deferred) {
		t.Fatalf("000045 NOT VALID occurrences = %d, parsed deferred constraints = %d", got, len(deferred))
	}

	body, err := embeddedMigrations.ReadFile("000052_gateway_request_price_fact_constraints_validation.sql")
	if err != nil {
		t.Fatal(err)
	}
	sqlText := strings.ToLower(string(body))
	matches := priceFactConstraintValidationStatement.FindAllStringSubmatch(sqlText, -1)
	if len(matches) != len(priceFactConstraintValidations) {
		t.Fatalf("validated price fact constraints = %d, want %d", len(matches), len(priceFactConstraintValidations))
	}

	seen := make(map[string]struct{}, len(matches))
	for _, match := range matches {
		key := match[1] + "." + match[2]
		if _, duplicate := seen[key]; duplicate {
			t.Fatalf("validation migration repeats constraint %q", key)
		}
		seen[key] = struct{}{}
	}
	assertPriceFactConstraintSet(t, "000052 validations", seen, deferred)

	if remainder := strings.TrimSpace(priceFactConstraintValidationStatement.ReplaceAllString(sqlText, "")); remainder != "" {
		t.Fatalf("validation migration contains SQL other than the approved validations: %q", remainder)
	}
	forbidden := regexp.MustCompile(`(?i)\b(not\s+valid|add|drop|insert|update|delete|create|truncate)\b`)
	if token := forbidden.FindString(sqlText); token != "" {
		t.Fatalf("validation migration contains forbidden token %q", token)
	}

	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		if migration.version != 52 {
			continue
		}
		if migration.name != "000052_gateway_request_price_fact_constraints_validation.sql" || migration.nonTransactional {
			t.Fatalf("version 52 price fact constraint validation migration = %#v", migration)
		}
		return
	}
	t.Fatal("version 52 price fact constraint validation migration was not loaded")
}

func deferredPriceFactConstraints(t *testing.T, sqlText string) map[string]struct{} {
	t.Helper()
	deferred := make(map[string]struct{})
	for _, tableMatch := range priceFactAlterTableStatement.FindAllStringSubmatch(sqlText, -1) {
		tableName := tableMatch[1]
		constraintSections := priceFactAddConstraint.Split(tableMatch[2], -1)
		for _, section := range constraintSections[1:] {
			if !priceFactNotValid.MatchString(section) {
				continue
			}
			nameMatch := priceFactConstraintName.FindStringSubmatch(section)
			if nameMatch == nil {
				t.Fatalf("000045 has an unnamed NOT VALID constraint on %s", tableName)
			}
			key := tableName + "." + nameMatch[1]
			if _, duplicate := deferred[key]; duplicate {
				t.Fatalf("000045 repeats deferred constraint %q", key)
			}
			deferred[key] = struct{}{}
		}
	}
	return deferred
}

func priceFactConstraintValidationSet(t *testing.T) map[string]struct{} {
	t.Helper()
	expected := make(map[string]struct{}, len(priceFactConstraintValidations))
	for _, validation := range priceFactConstraintValidations {
		key := validation.tableName + "." + validation.constraintName
		if _, duplicate := expected[key]; duplicate {
			t.Fatalf("test inventory repeats constraint %q", key)
		}
		expected[key] = struct{}{}
	}
	return expected
}

func assertPriceFactConstraintSet(t *testing.T, label string, got, want map[string]struct{}) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s constraint count = %d, want %d", label, len(got), len(want))
	}
	for key := range got {
		if _, ok := want[key]; !ok {
			t.Fatalf("%s includes unexpected constraint %q", label, key)
		}
	}
	for key := range want {
		if _, ok := got[key]; !ok {
			t.Fatalf("%s omits constraint %q", label, key)
		}
	}
}

func TestGatewayRequestPriceFactConstraintsValidationDatabase(t *testing.T) {
	if strings.TrimSpace(os.Getenv(testpostgres.EnvironmentDSN)) == "" {
		t.Skip("dedicated PostgreSQL test DSN is not configured")
	}
	db, _ := testpostgres.OpenEmpty(t)
	ctx := context.Background()
	if err := applyMigrations(ctx, db, embeddedMigrationsThrough(t, 45), ".", defaultRunnerConfig()); err != nil {
		t.Fatalf("apply migrations through 45: %v", err)
	}
	assertPriceFactConstraintValidationState(t, ctx, db, false)

	body, err := embeddedMigrations.ReadFile("000052_gateway_request_price_fact_constraints_validation.sql")
	if err != nil {
		t.Fatal(err)
	}
	validationFS := fstest.MapFS{
		"000052_gateway_request_price_fact_constraints_validation.sql": &fstest.MapFile{Data: body},
	}
	if err := applyMigrations(ctx, db, validationFS, ".", defaultRunnerConfig()); err != nil {
		t.Fatalf("apply migration 52: %v", err)
	}
	assertPriceFactConstraintValidationState(t, ctx, db, true)
}

func assertPriceFactConstraintValidationState(t *testing.T, ctx context.Context, db *sql.DB, want bool) {
	t.Helper()
	for _, validation := range priceFactConstraintValidations {
		var constraintType string
		var validated bool
		err := db.QueryRowContext(ctx, `
			SELECT constraint_row.contype::text, constraint_row.convalidated
			FROM pg_catalog.pg_constraint AS constraint_row
			JOIN pg_catalog.pg_class AS relation
			  ON relation.oid = constraint_row.conrelid
			JOIN pg_catalog.pg_namespace AS schema
			  ON schema.oid = relation.relnamespace
			WHERE schema.nspname = 'public'
			  AND relation.relname = $1
			  AND constraint_row.conname = $2
		`, validation.tableName, validation.constraintName).Scan(&constraintType, &validated)
		if err != nil {
			t.Fatalf("read constraint %s.%s: %v", validation.tableName, validation.constraintName, err)
		}
		if constraintType != validation.constraintType {
			t.Fatalf("constraint %s.%s type = %q, want %q", validation.tableName, validation.constraintName, constraintType, validation.constraintType)
		}
		if validated != want {
			t.Fatalf("constraint %s.%s validated = %t, want %t", validation.tableName, validation.constraintName, validated, want)
		}
	}
}
