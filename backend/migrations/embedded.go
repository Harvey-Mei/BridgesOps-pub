// Package migrations applies immutable PostgreSQL schema migrations.
package migrations

import "embed"

// embeddedMigrations contains immutable PostgreSQL migration files in this
// directory. Non-SQL package files are ignored by the migration loader.
//
//go:embed *.sql
var embeddedMigrations embed.FS
