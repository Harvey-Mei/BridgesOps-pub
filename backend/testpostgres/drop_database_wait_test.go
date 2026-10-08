package testpostgres

import (
	"context"
	"database/sql"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestDropDatabaseWaitsForOwnedConnectionsWithoutSignalPrivileges(t *testing.T) {
	database, dsn := OpenEmpty(t)
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	name := strings.TrimPrefix(parsed.Path, "/")
	admin, err := sql.Open("pgx", testAdminURL(t).String())
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	connection, err := database.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		time.Sleep(150 * time.Millisecond)
		_ = connection.Close()
		_ = database.Close()
	}()
	if err := dropDatabase(admin, name); err != nil {
		t.Error(err)
	}
	<-closed
	var exists bool
	if err := admin.QueryRow("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)", name).Scan(&exists); err != nil || exists {
		t.Fatal("temporary database was not removed", err)
	}
	if err := dropDatabase(admin, adminDatabase); err == nil {
		t.Fatal("admin database was accepted as a cleanup target")
	}
}
