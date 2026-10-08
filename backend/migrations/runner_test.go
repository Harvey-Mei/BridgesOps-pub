package migrations

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"
)

func TestLoadMigrationsSortsVersionsAndKeepsRawChecksum(t *testing.T) {
	second := "CREATE TABLE second (note TEXT DEFAULT 'a;b');\n"
	migrations, err := loadMigrations(fstest.MapFS{
		"sql/000010_second_notx.sql": &fstest.MapFile{Data: []byte(second)},
		"sql/000002_first.sql":       &fstest.MapFile{Data: []byte("CREATE TABLE first (id BIGINT);\n")},
	}, "sql")
	if err != nil {
		t.Fatalf("load migrations: %v", err)
	}
	if len(migrations) != 2 {
		t.Fatalf("migration count = %d, want 2", len(migrations))
	}
	if migrations[0].version != 2 || migrations[1].version != 10 {
		t.Fatalf("versions = %d, %d, want 2, 10", migrations[0].version, migrations[1].version)
	}
	if !migrations[1].nonTransactional {
		t.Fatal("_notx.sql migration was not marked non-transactional")
	}
	sum := sha256.Sum256([]byte(second))
	if migrations[1].checksum != hex.EncodeToString(sum[:]) {
		t.Fatalf("checksum = %q, want raw file checksum %q", migrations[1].checksum, hex.EncodeToString(sum[:]))
	}
}

func TestEmbeddedMigrationsIncludeInitialSchema(t *testing.T) {
	migrations, err := loadEmbeddedMigrations()
	if err != nil {
		t.Fatalf("load embedded migrations: %v", err)
	}
	if len(migrations) == 0 {
		t.Fatal("embedded migration set is empty")
	}
	if migrations[0].version != 1 || migrations[0].name != "000001_initial_schema.sql" {
		t.Fatalf("first embedded migration = %#v, want 000001_initial_schema.sql", migrations[0])
	}
	if migrations[0].nonTransactional {
		t.Fatal("initial schema migration must run in one transaction")
	}
}

func TestValidateMigrationContinuityAllowsOnlyHistoricalGaps(t *testing.T) {
	if embeddedMigrationHeadVersion != 82 {
		t.Fatalf("embedded migration head = %06d, want 000082", embeddedMigrationHeadVersion)
	}
	wantAllowed := map[int64]struct{}{
		14: {},
		15: {},
		16: {},
		28: {},
		29: {},
		62: {},
		63: {},
		64: {},
	}
	if len(allowedMigrationGaps) != len(wantAllowed) {
		t.Fatalf("allowed migration gaps = %v, want %v", allowedMigrationGaps, wantAllowed)
	}
	for version := range wantAllowed {
		if _, ok := allowedMigrationGaps[version]; !ok {
			t.Fatalf("migration version %06d is not an allowed historical gap", version)
		}
	}
	if _, ok := allowedMigrationGaps[57]; ok {
		t.Fatal("migration version 000057 must be required")
	}

	migrations := migrationSequenceThrough(82, wantAllowed)
	if err := validateMigrationContinuity(migrations); err != nil {
		t.Fatalf("validate migration continuity with historical gaps: %v", err)
	}
}

func TestAcceptedLegacyMigrationChecksumIsExact(t *testing.T) {
	if !acceptedLegacyMigrationChecksum(76, "a48380d56e50429d11dc791cd18b60254bc7e15626ce012b8452f1a6d5e31185") {
		t.Fatal("known production migration checksum was not accepted")
	}
	if acceptedLegacyMigrationChecksum(76, "87b8623365c93502db99eaf7d3dd05afc5854ff44f385faa97c3aa56626572b9") {
		t.Fatal("feature migration checksum must remain canonical for version 000076")
	}
	if acceptedLegacyMigrationChecksum(75, "a48380d56e50429d11dc791cd18b60254bc7e15626ce012b8452f1a6d5e31185") {
		t.Fatal("legacy checksum alias leaked to another migration version")
	}
	if !acceptedLegacyMigrationChecksum(77, "87b8623365c93502db99eaf7d3dd05afc5854ff44f385faa97c3aa56626572b9") {
		t.Fatal("rolled-back policy migration checksum was not accepted for staging compatibility")
	}
}

func TestValidateMigrationContinuityRejectsRequiredGaps(t *testing.T) {
	tests := []struct {
		name       string
		migrations []migration
		want       string
	}{
		{
			name:       "empty sequence",
			migrations: nil,
			want:       "migration continuity gap: missing required version 000001",
		},
		{
			name:       "first version is not one",
			migrations: migrationSequenceThrough(2, map[int64]struct{}{1: {}}),
			want:       "migration continuity gap: missing required version 000001",
		},
		{
			name:       "version 57",
			migrations: migrationSequenceThrough(67, map[int64]struct{}{14: {}, 15: {}, 16: {}, 28: {}, 29: {}, 57: {}, 62: {}, 63: {}, 64: {}}),
			want:       "migration continuity gap: missing required version 000057",
		},
		{
			name:       "another required version",
			migrations: migrationSequenceThrough(67, map[int64]struct{}{14: {}, 15: {}, 16: {}, 28: {}, 29: {}, 42: {}, 62: {}, 63: {}, 64: {}}),
			want:       "migration continuity gap: missing required version 000042",
		},
		{
			name:       "embedded head removed",
			migrations: migrationSequenceThrough(66, map[int64]struct{}{14: {}, 15: {}, 16: {}, 28: {}, 29: {}, 62: {}, 63: {}, 64: {}}),
			want:       "migration continuity: embedded head is 000066, expected 000082",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateMigrationContinuity(test.migrations)
			if err == nil || err.Error() != test.want {
				t.Fatalf("validate migration continuity error = %v, want %q", err, test.want)
			}
		})
	}
}

func migrationSequenceThrough(maxVersion int64, omitted map[int64]struct{}) []migration {
	migrations := make([]migration, 0, maxVersion)
	for version := int64(1); version <= maxVersion; version++ {
		if _, skip := omitted[version]; skip {
			continue
		}
		migrations = append(migrations, migration{version: version})
	}
	return migrations
}

func TestLoadMigrationsRejectsInvalidInputs(t *testing.T) {
	tests := []struct {
		name  string
		files fstest.MapFS
		want  string
	}{
		{
			name: "invalid name",
			files: fstest.MapFS{
				"sql/create_widgets.sql": &fstest.MapFile{Data: []byte("SELECT 1")},
			},
			want: "invalid migration filename",
		},
		{
			name: "duplicate version",
			files: fstest.MapFS{
				"sql/000001_first.sql": &fstest.MapFile{Data: []byte("SELECT 1")},
				"sql/1_second.sql":     &fstest.MapFile{Data: []byte("SELECT 2")},
			},
			want: "duplicate migration version",
		},
		{
			name: "empty migration",
			files: fstest.MapFS{
				"sql/000001_empty.sql": &fstest.MapFile{Data: []byte(" \n\t")},
			},
			want: "is empty",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := loadMigrations(test.files, "sql")
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("load migrations error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestApplyMigrationsUsesOneConnectionAndPreservesNonTransactionalSQL(t *testing.T) {
	transactionalSQL := "CREATE TABLE widgets (id BIGINT PRIMARY KEY);\n"
	nonTransactionalSQL := "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_widgets_id ON widgets (id);\nCOMMENT ON INDEX idx_widgets_id IS 'kept; together';\n"
	files := fstest.MapFS{
		"sql/000001_create_widgets.sql":         &fstest.MapFile{Data: []byte(transactionalSQL)},
		"sql/000002_add_widgets_index_notx.sql": &fstest.MapFile{Data: []byte(nonTransactionalSQL)},
	}
	db, state := newFakeDatabase(t)

	if err := applyMigrations(context.Background(), db, files, "sql", defaultRunnerConfig()); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	if len(state.applied) != 2 {
		t.Fatalf("applied migrations = %d, want 2", len(state.applied))
	}

	lockCall, ok := state.firstCallContaining("pg_try_advisory_lock")
	if !ok {
		t.Fatal("missing advisory lock call")
	}
	unlockCall, ok := state.firstCallContaining("pg_advisory_unlock")
	if !ok {
		t.Fatal("missing advisory unlock call")
	}
	if lockCall.connectionID != unlockCall.connectionID {
		t.Fatalf("lock connection = %d, unlock connection = %d, want same dedicated connection", lockCall.connectionID, unlockCall.connectionID)
	}
	if state.connectionCount() != 1 {
		t.Fatalf("connections = %d, want 1", state.connectionCount())
	}

	transactionalCall, ok := state.firstExactExec(transactionalSQL)
	if !ok {
		t.Fatalf("transactional migration SQL was not executed: %q", transactionalSQL)
	}
	if !transactionalCall.inTransaction {
		t.Fatal("ordinary migration SQL was not executed in a transaction")
	}
	nonTransactionalCall, ok := state.firstExactExec(nonTransactionalSQL)
	if !ok {
		t.Fatalf("non-transactional migration SQL was changed or split: %q", nonTransactionalSQL)
	}
	if nonTransactionalCall.inTransaction {
		t.Fatal("_notx.sql migration SQL was executed in a transaction")
	}
	if state.countExactExec(nonTransactionalSQL) != 1 {
		t.Fatalf("non-transactional migration executions = %d, want 1", state.countExactExec(nonTransactionalSQL))
	}

	for _, call := range state.callsSnapshot() {
		if !strings.Contains(call.query, "set_config('lock_timeout'") {
			continue
		}
		if len(call.args) != 3 {
			t.Fatalf("timeout call arguments = %d, want 3", len(call.args))
		}
	}

	bodyExecutions := state.countExactExec(transactionalSQL) + state.countExactExec(nonTransactionalSQL)
	if err := applyMigrations(context.Background(), db, files, "sql", defaultRunnerConfig()); err != nil {
		t.Fatalf("reapply migrations: %v", err)
	}
	if got := state.countExactExec(transactionalSQL) + state.countExactExec(nonTransactionalSQL); got != bodyExecutions {
		t.Fatalf("already-applied migration bodies executed again: got %d, want %d", got, bodyExecutions)
	}
}

func TestApplyMigrationsHandlesEmptyDirectory(t *testing.T) {
	db, state := newFakeDatabase(t)
	files := fstest.MapFS{
		"sql/README.md": &fstest.MapFile{Data: []byte("no migrations yet")},
	}

	if err := applyMigrations(context.Background(), db, files, "sql", defaultRunnerConfig()); err != nil {
		t.Fatalf("apply empty migrations: %v", err)
	}
	if len(state.applied) != 0 {
		t.Fatalf("applied migrations = %d, want 0", len(state.applied))
	}
	if _, ok := state.firstCallContaining("CREATE TABLE IF NOT EXISTS public.schema_migrations"); !ok {
		t.Fatal("schema_migrations was not initialized for empty migration directory")
	}
}

func TestApplyMigrationsPinsManifestToPublicSchema(t *testing.T) {
	files := fstest.MapFS{
		"sql/000001_create_widgets.sql":     &fstest.MapFile{Data: []byte("SELECT 1\n")},
		"sql/000002_index_widgets_notx.sql": &fstest.MapFile{Data: []byte("SELECT 2\n")},
	}
	db, state := newFakeDatabase(t)

	if err := applyMigrations(context.Background(), db, files, "sql", defaultRunnerConfig()); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	ensureIndex := -1
	createIndex := -1
	for index, call := range state.callsSnapshot() {
		if call.query == ensurePublicSchemaDDL {
			ensureIndex = index
		}
		if call.query == schemaMigrationsDDL {
			createIndex = index
		}
		if strings.Contains(call.query, "schema_migrations") && !strings.Contains(call.query, "public.schema_migrations") {
			t.Fatalf("manifest query is not schema-qualified: %q", call.query)
		}
	}
	if ensureIndex < 0 || createIndex < 0 {
		t.Fatalf("public schema/table initialization calls missing: ensure=%d create=%d", ensureIndex, createIndex)
	}
	if ensureIndex >= createIndex {
		t.Fatalf("public schema was not ensured before manifest creation: ensure=%d create=%d", ensureIndex, createIndex)
	}
}

func TestApplyMigrationsRejectsChecksumMismatchAndReleasesLock(t *testing.T) {
	sqlBody := "CREATE TABLE widgets (id BIGINT PRIMARY KEY);\n"
	db, state := newFakeDatabase(t)
	state.applied[1] = "not-the-file-checksum"
	files := fstest.MapFS{
		"sql/000001_create_widgets.sql": &fstest.MapFile{Data: []byte(sqlBody)},
	}

	err := applyMigrations(context.Background(), db, files, "sql", defaultRunnerConfig())
	if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("apply migrations error = %v, want checksum mismatch", err)
	}
	if _, ok := state.firstExactExec(sqlBody); ok {
		t.Fatal("migration SQL executed despite checksum mismatch")
	}
	lockCall, locked := state.firstCallContaining("pg_try_advisory_lock")
	unlockCall, unlocked := state.firstCallContaining("pg_advisory_unlock")
	if !locked || !unlocked {
		t.Fatal("checksum failure did not make both advisory lock calls")
	}
	if lockCall.connectionID != unlockCall.connectionID {
		t.Fatalf("lock connection = %d, unlock connection = %d, want same connection", lockCall.connectionID, unlockCall.connectionID)
	}
}

func TestApplyMigrationsBoundsAdvisoryLockWait(t *testing.T) {
	db, state := newFakeDatabase(t)
	state.advisoryLockAvailable = false
	config := defaultRunnerConfig()
	config.advisoryWaitTimeout = 20 * time.Millisecond
	config.lockRetryInterval = 2 * time.Millisecond

	err := applyMigrations(context.Background(), db, fstest.MapFS{"sql/README.md": &fstest.MapFile{Data: []byte("empty")}}, "sql", config)
	if err == nil || !strings.Contains(err.Error(), "acquire migrations advisory lock") {
		t.Fatalf("apply migrations error = %v, want advisory lock timeout", err)
	}
	if _, unlocked := state.firstCallContaining("pg_advisory_unlock"); unlocked {
		t.Fatal("runner attempted to unlock an advisory lock it did not acquire")
	}
}

type fakeCall struct {
	connectionID  int
	operation     string
	query         string
	args          []driver.NamedValue
	inTransaction bool
}

type fakeDatabaseState struct {
	mu                    sync.Mutex
	nextConnectionID      int
	connectionIDs         map[int]struct{}
	calls                 []fakeCall
	applied               map[int64]string
	advisoryLockAvailable bool
}

func newFakeDatabase(t *testing.T) (*sql.DB, *fakeDatabaseState) {
	t.Helper()
	state := &fakeDatabaseState{
		connectionIDs:         make(map[int]struct{}),
		applied:               make(map[int64]string),
		advisoryLockAvailable: true,
	}
	db := sql.OpenDB(fakeConnector{state: state})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close fake database: %v", err)
		}
	})
	return db, state
}

func (state *fakeDatabaseState) addCall(call fakeCall) {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.calls = append(state.calls, call)
}

func (state *fakeDatabaseState) callsSnapshot() []fakeCall {
	state.mu.Lock()
	defer state.mu.Unlock()
	return append([]fakeCall(nil), state.calls...)
}

func (state *fakeDatabaseState) connectionCount() int {
	state.mu.Lock()
	defer state.mu.Unlock()
	return len(state.connectionIDs)
}

func (state *fakeDatabaseState) firstCallContaining(fragment string) (fakeCall, bool) {
	for _, call := range state.callsSnapshot() {
		if strings.Contains(call.query, fragment) {
			return call, true
		}
	}
	return fakeCall{}, false
}

func (state *fakeDatabaseState) firstExactExec(query string) (fakeCall, bool) {
	for _, call := range state.callsSnapshot() {
		if call.operation == "exec" && call.query == query {
			return call, true
		}
	}
	return fakeCall{}, false
}

func (state *fakeDatabaseState) countExactExec(query string) int {
	count := 0
	for _, call := range state.callsSnapshot() {
		if call.operation == "exec" && call.query == query {
			count++
		}
	}
	return count
}

type fakeConnector struct {
	state *fakeDatabaseState
}

func (connector fakeConnector) Connect(context.Context) (driver.Conn, error) {
	connector.state.mu.Lock()
	defer connector.state.mu.Unlock()
	connector.state.nextConnectionID++
	id := connector.state.nextConnectionID
	connector.state.connectionIDs[id] = struct{}{}
	return &fakeConn{id: id, state: connector.state}, nil
}

func (connector fakeConnector) Driver() driver.Driver {
	return fakeDriver{}
}

type fakeDriver struct{}

func (fakeDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("fake driver requires a connector")
}

type fakeConn struct {
	id            int
	state         *fakeDatabaseState
	inTransaction bool
}

var (
	_ driver.Conn           = (*fakeConn)(nil)
	_ driver.ConnBeginTx    = (*fakeConn)(nil)
	_ driver.ExecerContext  = (*fakeConn)(nil)
	_ driver.QueryerContext = (*fakeConn)(nil)
)

func (conn *fakeConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepared statements are not expected")
}

func (conn *fakeConn) Close() error {
	conn.state.addCall(fakeCall{connectionID: conn.id, operation: "close"})
	return nil
}

func (conn *fakeConn) Begin() (driver.Tx, error) {
	return conn.begin()
}

func (conn *fakeConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return conn.begin()
}

func (conn *fakeConn) begin() (driver.Tx, error) {
	if conn.inTransaction {
		return nil, errors.New("nested transaction")
	}
	conn.inTransaction = true
	conn.state.addCall(fakeCall{connectionID: conn.id, operation: "begin", inTransaction: true})
	return fakeTx{conn: conn}, nil
}

func (conn *fakeConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	call := fakeCall{
		connectionID:  conn.id,
		operation:     "exec",
		query:         query,
		args:          append([]driver.NamedValue(nil), args...),
		inTransaction: conn.inTransaction,
	}
	conn.state.addCall(call)

	if strings.Contains(query, "INSERT INTO public.schema_migrations") {
		if len(args) != 2 {
			return nil, fmt.Errorf("record migration args = %d, want 2", len(args))
		}
		version, ok := args[0].Value.(int64)
		if !ok {
			return nil, fmt.Errorf("migration version type = %T, want int64", args[0].Value)
		}
		checksum, ok := args[1].Value.(string)
		if !ok {
			return nil, fmt.Errorf("migration checksum type = %T, want string", args[1].Value)
		}
		conn.state.mu.Lock()
		conn.state.applied[version] = checksum
		conn.state.mu.Unlock()
	}
	return driver.RowsAffected(1), nil
}

func (conn *fakeConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	conn.state.addCall(fakeCall{
		connectionID:  conn.id,
		operation:     "query",
		query:         query,
		args:          append([]driver.NamedValue(nil), args...),
		inTransaction: conn.inTransaction,
	})

	switch {
	case strings.Contains(query, "pg_try_advisory_lock"):
		conn.state.mu.Lock()
		available := conn.state.advisoryLockAvailable
		conn.state.mu.Unlock()
		return &fakeRows{columns: []string{"pg_try_advisory_lock"}, values: [][]driver.Value{{available}}}, nil
	case strings.Contains(query, "pg_advisory_unlock"):
		return &fakeRows{columns: []string{"pg_advisory_unlock"}, values: [][]driver.Value{{true}}}, nil
	case strings.Contains(query, "SELECT checksum FROM public.schema_migrations"):
		if len(args) != 1 {
			return nil, fmt.Errorf("migration lookup args = %d, want 1", len(args))
		}
		version, ok := args[0].Value.(int64)
		if !ok {
			return nil, fmt.Errorf("migration lookup version type = %T, want int64", args[0].Value)
		}
		conn.state.mu.Lock()
		checksum, found := conn.state.applied[version]
		conn.state.mu.Unlock()
		if !found {
			return &fakeRows{columns: []string{"checksum"}}, nil
		}
		return &fakeRows{columns: []string{"checksum"}, values: [][]driver.Value{{checksum}}}, nil
	case strings.Contains(query, "current_setting('lock_timeout')"):
		return &fakeRows{columns: []string{"lock_timeout", "statement_timeout"}, values: [][]driver.Value{{"0", "0"}}}, nil
	default:
		return nil, fmt.Errorf("unexpected query %q", query)
	}
}

type fakeTx struct {
	conn *fakeConn
}

func (tx fakeTx) Commit() error {
	if !tx.conn.inTransaction {
		return errors.New("commit without transaction")
	}
	tx.conn.state.addCall(fakeCall{connectionID: tx.conn.id, operation: "commit", inTransaction: true})
	tx.conn.inTransaction = false
	return nil
}

func (tx fakeTx) Rollback() error {
	if !tx.conn.inTransaction {
		return nil
	}
	tx.conn.state.addCall(fakeCall{connectionID: tx.conn.id, operation: "rollback", inTransaction: true})
	tx.conn.inTransaction = false
	return nil
}

type fakeRows struct {
	columns []string
	values  [][]driver.Value
	index   int
}

func (rows *fakeRows) Columns() []string {
	return rows.columns
}

func (rows *fakeRows) Close() error {
	return nil
}

func (rows *fakeRows) Next(destination []driver.Value) error {
	if rows.index >= len(rows.values) {
		return io.EOF
	}
	copy(destination, rows.values[rows.index])
	rows.index++
	return nil
}
