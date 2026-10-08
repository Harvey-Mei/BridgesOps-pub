package migrations

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	embeddedMigrationsRoot = "."

	migrationAdvisoryLockID      int64 = 693504415344510359
	migrationLockTimeout               = 10 * time.Second
	migrationStatementTimeout          = 5 * time.Minute
	migrationAdvisoryWaitTimeout       = 30 * time.Second
	migrationLockRetryInterval         = 250 * time.Millisecond
	migrationReleaseTimeout            = 5 * time.Second
)

const ensurePublicSchemaDDL = `CREATE SCHEMA IF NOT EXISTS public`

const schemaMigrationsDDL = `
	CREATE TABLE IF NOT EXISTS public.schema_migrations (
	    version BIGINT PRIMARY KEY,
	    checksum CHAR(64) NOT NULL,
	    applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
	)
`

const (
	migrationChecksumSQL = "SELECT checksum FROM public.schema_migrations WHERE version = $1"
	recordMigrationSQL   = "INSERT INTO public.schema_migrations (version, checksum) VALUES ($1, $2)"
)

const setMigrationTimeoutsSQL = `
SELECT set_config('lock_timeout', $1, $3),
       set_config('statement_timeout', $2, $3)
`

var migrationFilePattern = regexp.MustCompile(`^([0-9]+)_([A-Za-z][A-Za-z0-9_-]*)\.sql$`)

const embeddedMigrationHeadVersion int64 = 82

// A production release from the mainline route-exclusion work recorded that
// migration under version 000076 before this feature branch was cut. Keep the
// feature branch's original 000076 immutable, accept that exact known checksum
// as a historical alias, and replay the route-exclusion SQL at 000077 so both
// database lineages converge before the 000078 policy migration.
var acceptedLegacyMigrationChecksums = map[int64]map[string]struct{}{
	76: {
		"a48380d56e50429d11dc791cd18b60254bc7e15626ce012b8452f1a6d5e31185": {},
	},
	77: {
		// A pre-release branch opened a policy migration at 000077 but rolled
		// back before production became healthy; accept it if staging completed it.
		"87b8623365c93502db99eaf7d3dd05afc5854ff44f385faa97c3aa56626572b9": {},
	},
}

func acceptedLegacyMigrationChecksum(version int64, checksum string) bool {
	checksums, ok := acceptedLegacyMigrationChecksums[version]
	if !ok {
		return false
	}
	_, ok = checksums[checksum]
	return ok
}

var allowedMigrationGaps = map[int64]struct{}{
	14: {},
	15: {},
	16: {},
	28: {},
	29: {},
	62: {},
	63: {},
	64: {},
}

type migration struct {
	version          int64
	name             string
	checksum         string
	sql              string
	nonTransactional bool
}

type runnerConfig struct {
	advisoryWaitTimeout time.Duration
	lockTimeout         time.Duration
	statementTimeout    time.Duration
	lockRetryInterval   time.Duration
	releaseTimeout      time.Duration
}

func defaultRunnerConfig() runnerConfig {
	return runnerConfig{
		advisoryWaitTimeout: migrationAdvisoryWaitTimeout,
		lockTimeout:         migrationLockTimeout,
		statementTimeout:    migrationStatementTimeout,
		lockRetryInterval:   migrationLockRetryInterval,
		releaseTimeout:      migrationReleaseTimeout,
	}
}

// Apply applies the embedded PostgreSQL migrations.
func Apply(ctx context.Context, db *sql.DB) error {
	migrations, err := loadEmbeddedMigrations()
	if err != nil {
		return err
	}
	return applyLoadedMigrations(ctx, db, migrations, defaultRunnerConfig())
}

func SchemaFingerprint() (string, error) {
	migrations, err := loadEmbeddedMigrations()
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	for _, migration := range migrations {
		_, _ = fmt.Fprintf(hash, "%d:%s\n", migration.version, migration.checksum)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func applyMigrations(ctx context.Context, db *sql.DB, migrationFS fs.FS, root string, config runnerConfig) error {
	if ctx == nil {
		return errors.New("nil context")
	}
	if db == nil {
		return errors.New("nil sql db")
	}

	migrations, err := loadMigrations(migrationFS, root)
	if err != nil {
		return err
	}
	return applyLoadedMigrations(ctx, db, migrations, config)
}

func applyLoadedMigrations(ctx context.Context, db *sql.DB, migrations []migration, config runnerConfig) (resultErr error) {
	if ctx == nil {
		return errors.New("nil context")
	}
	if db == nil {
		return errors.New("nil sql db")
	}

	if err := validateRunnerConfig(config); err != nil {
		return err
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("open migrations connection: %w", err)
	}
	defer func() {
		_ = conn.Close()
	}()

	if err := acquireAdvisoryLock(ctx, conn, config); err != nil {
		return err
	}
	defer func() {
		if err := releaseAdvisoryLock(conn, config); err != nil && resultErr == nil {
			resultErr = err
		}
	}()
	previousTimeouts, err := setSessionTimeouts(ctx, conn, config)
	if err != nil {
		return fmt.Errorf("configure migrations session timeouts: %w", err)
	}
	defer func() {
		if err := restoreSessionTimeouts(conn, previousTimeouts, config); err != nil && resultErr == nil {
			resultErr = fmt.Errorf("restore migrations session timeouts: %w", err)
		}
	}()

	if _, err := conn.ExecContext(ctx, ensurePublicSchemaDDL); err != nil {
		return fmt.Errorf("ensure public migration schema: %w", err)
	}
	if _, err := conn.ExecContext(ctx, schemaMigrationsDDL); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	for _, migration := range migrations {
		existingChecksum, err := migrationChecksum(ctx, conn, migration.version)
		switch {
		case err == nil:
			if existingChecksum != migration.checksum {
				if acceptedLegacyMigrationChecksum(migration.version, existingChecksum) {
					continue
				}
				return fmt.Errorf("migration %s checksum mismatch (database=%s file=%s)", migration.name, existingChecksum, migration.checksum)
			}
			continue
		case !errors.Is(err, sql.ErrNoRows):
			return fmt.Errorf("check migration %s: %w", migration.name, err)
		}

		if migration.nonTransactional {
			err = applyNonTransactionalMigration(ctx, conn, migration, config)
		} else {
			err = applyTransactionalMigration(ctx, conn, migration, config)
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func loadEmbeddedMigrations() ([]migration, error) {
	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		return nil, err
	}
	if err := validateMigrationContinuity(migrations); err != nil {
		return nil, err
	}
	return migrations, nil
}

func validateMigrationContinuity(migrations []migration) error {
	if len(migrations) == 0 || migrations[0].version != 1 {
		return errors.New("migration continuity gap: missing required version 000001")
	}

	previousVersion := int64(0)
	for _, migration := range migrations {
		if migration.version <= previousVersion {
			return fmt.Errorf("migration continuity: version %06d is not strictly increasing", migration.version)
		}
		for version := previousVersion + 1; version < migration.version; version++ {
			if _, allowed := allowedMigrationGaps[version]; !allowed {
				return fmt.Errorf("migration continuity gap: missing required version %06d", version)
			}
		}
		previousVersion = migration.version
	}
	if previousVersion != embeddedMigrationHeadVersion {
		return fmt.Errorf("migration continuity: embedded head is %06d, expected %06d", previousVersion, embeddedMigrationHeadVersion)
	}
	return nil
}

func loadMigrations(migrationFS fs.FS, root string) ([]migration, error) {
	if migrationFS == nil {
		return nil, errors.New("nil migration filesystem")
	}
	root = strings.Trim(strings.TrimSpace(root), "/")
	if root == "" || !fs.ValidPath(root) {
		return nil, fmt.Errorf("invalid migrations root %q", root)
	}

	files, err := fs.Glob(migrationFS, path.Join(root, "*.sql"))
	if err != nil {
		return nil, fmt.Errorf("list migrations: %w", err)
	}

	migrations := make([]migration, 0, len(files))
	versions := make(map[int64]string, len(files))
	for _, file := range files {
		name := path.Base(file)
		matches := migrationFilePattern.FindStringSubmatch(name)
		if matches == nil {
			return nil, fmt.Errorf("invalid migration filename %q: expected NNN_description.sql", name)
		}
		version, err := strconv.ParseInt(matches[1], 10, 64)
		if err != nil || version <= 0 {
			return nil, fmt.Errorf("invalid migration version in %q", name)
		}
		if existing, ok := versions[version]; ok {
			return nil, fmt.Errorf("duplicate migration version %d in %q and %q", version, existing, name)
		}

		content, err := fs.ReadFile(migrationFS, file)
		if err != nil {
			return nil, fmt.Errorf("read migration %s: %w", name, err)
		}
		if len(strings.TrimSpace(string(content))) == 0 {
			return nil, fmt.Errorf("migration %s is empty", name)
		}

		sum := sha256.Sum256(content)
		versions[version] = name
		migrations = append(migrations, migration{
			version:          version,
			name:             name,
			checksum:         hex.EncodeToString(sum[:]),
			sql:              string(content),
			nonTransactional: strings.HasSuffix(strings.ToLower(name), "_notx.sql"),
		})
	}

	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].version < migrations[j].version
	})
	return migrations, nil
}

func validateRunnerConfig(config runnerConfig) error {
	if config.advisoryWaitTimeout <= 0 {
		return errors.New("migration advisory wait timeout must be positive")
	}
	if config.lockTimeout <= 0 {
		return errors.New("migration lock timeout must be positive")
	}
	if config.statementTimeout <= 0 {
		return errors.New("migration statement timeout must be positive")
	}
	if config.lockRetryInterval <= 0 {
		return errors.New("migration lock retry interval must be positive")
	}
	if config.releaseTimeout <= 0 {
		return errors.New("migration release timeout must be positive")
	}
	return nil
}

func acquireAdvisoryLock(ctx context.Context, conn *sql.Conn, config runnerConfig) error {
	lockCtx, cancel := context.WithTimeout(ctx, config.advisoryWaitTimeout)
	defer cancel()

	ticker := time.NewTicker(config.lockRetryInterval)
	defer ticker.Stop()
	for {
		var locked bool
		if err := conn.QueryRowContext(lockCtx, "SELECT pg_try_advisory_lock($1)", migrationAdvisoryLockID).Scan(&locked); err != nil {
			return fmt.Errorf("acquire migrations advisory lock: %w", err)
		}
		if locked {
			return nil
		}

		select {
		case <-lockCtx.Done():
			return fmt.Errorf("acquire migrations advisory lock: %w", lockCtx.Err())
		case <-ticker.C:
		}
	}
}

func releaseAdvisoryLock(conn *sql.Conn, config runnerConfig) error {
	ctx, cancel := context.WithTimeout(context.Background(), config.releaseTimeout)
	defer cancel()

	var released bool
	if err := conn.QueryRowContext(ctx, "SELECT pg_advisory_unlock($1)", migrationAdvisoryLockID).Scan(&released); err != nil {
		return fmt.Errorf("release migrations advisory lock: %w", err)
	}
	if !released {
		return errors.New("release migrations advisory lock: lock was not held")
	}
	return nil
}

func migrationChecksum(ctx context.Context, conn *sql.Conn, version int64) (string, error) {
	var checksum string
	err := conn.QueryRowContext(ctx, migrationChecksumSQL, version).Scan(&checksum)
	return checksum, err
}

func applyTransactionalMigration(ctx context.Context, conn *sql.Conn, migration migration, config runnerConfig) error {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin migration %s: %w", migration.name, err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if err := setTransactionalTimeouts(ctx, tx, config); err != nil {
		return fmt.Errorf("configure migration %s timeouts: %w", migration.name, err)
	}
	if _, err := tx.ExecContext(ctx, migration.sql); err != nil {
		return fmt.Errorf("apply migration %s: %w", migration.name, err)
	}
	if _, err := tx.ExecContext(ctx, recordMigrationSQL, migration.version, migration.checksum); err != nil {
		return fmt.Errorf("record migration %s: %w", migration.name, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit migration %s: %w", migration.name, err)
	}
	committed = true
	return nil
}

func applyNonTransactionalMigration(ctx context.Context, conn *sql.Conn, migration migration, config runnerConfig) (resultErr error) {
	previousTimeouts, err := setSessionTimeouts(ctx, conn, config)
	if err != nil {
		return fmt.Errorf("configure migration %s timeouts: %w", migration.name, err)
	}
	defer func() {
		if err := restoreSessionTimeouts(conn, previousTimeouts, config); err != nil && resultErr == nil {
			resultErr = fmt.Errorf("restore migration %s timeouts: %w", migration.name, err)
		}
	}()

	// Send the file as one SQL string. In particular, do not split on ';': a
	// semicolon may occur inside PostgreSQL strings, comments, or function bodies.
	if _, err := conn.ExecContext(ctx, migration.sql); err != nil {
		return fmt.Errorf("apply non-transactional migration %s: %w", migration.name, err)
	}
	if _, err := conn.ExecContext(ctx, recordMigrationSQL, migration.version, migration.checksum); err != nil {
		return fmt.Errorf("record non-transactional migration %s: %w", migration.name, err)
	}
	return nil
}

func setTransactionalTimeouts(ctx context.Context, tx *sql.Tx, config runnerConfig) error {
	_, err := tx.ExecContext(ctx, setMigrationTimeoutsSQL, postgresTimeout(config.lockTimeout), postgresTimeout(config.statementTimeout), true)
	return err
}

type sessionTimeouts struct {
	lockTimeout      string
	statementTimeout string
}

func setSessionTimeouts(ctx context.Context, conn *sql.Conn, config runnerConfig) (sessionTimeouts, error) {
	var previous sessionTimeouts
	if err := conn.QueryRowContext(ctx, "SELECT current_setting('lock_timeout'), current_setting('statement_timeout')").Scan(&previous.lockTimeout, &previous.statementTimeout); err != nil {
		return sessionTimeouts{}, err
	}
	if _, err := conn.ExecContext(ctx, setMigrationTimeoutsSQL, postgresTimeout(config.lockTimeout), postgresTimeout(config.statementTimeout), false); err != nil {
		return sessionTimeouts{}, err
	}
	return previous, nil
}

func restoreSessionTimeouts(conn *sql.Conn, previous sessionTimeouts, config runnerConfig) error {
	ctx, cancel := context.WithTimeout(context.Background(), config.releaseTimeout)
	defer cancel()
	_, err := conn.ExecContext(ctx, setMigrationTimeoutsSQL, previous.lockTimeout, previous.statementTimeout, false)
	return err
}

func postgresTimeout(value time.Duration) string {
	milliseconds := value.Milliseconds()
	if milliseconds < 1 {
		milliseconds = 1
	}
	return strconv.FormatInt(milliseconds, 10) + "ms"
}
