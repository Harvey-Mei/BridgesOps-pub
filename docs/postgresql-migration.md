# PostgreSQL Deployment Contract

BridgeOps supports PostgreSQL only. This document records the current database
contract; it is not a cross-database import guide.

## Required Configuration

Configure the same values in YAML and the service environment:

```yaml
database:
  driver: postgres
  host: 127.0.0.1
  port: 5432
  user: bridgeops
  password: replace-me
  name: bridgeops
  timeZone: UTC
  sslMode: disable
```

Use `require`, `verify-ca`, or `verify-full` when PostgreSQL is reached across
hosts. Keep passwords in the root-only systemd environment file in production.
Redis remains optional and disposable.

## Schema Lifecycle

The server applies checksum-verified SQL from `backend/migrations` before it
starts HTTP handling. Released migration files are immutable. Every schema
change requires a new numbered migration and a PostgreSQL integration test.
Runtime schema inference is not used.

To apply migrations without starting the HTTP server:

```bash
bridgeops -config /etc/bridgeops/config.yaml -migrate-schema-only
```

## Deployment Checks

1. Back up PostgreSQL (see [recovery.md](recovery.md)).
2. Run backend tests against a dedicated PostgreSQL test admin database.
3. Apply migrations and verify `public.schema_migrations` checksums.
4. Start BridgeOps and verify `/healthz`.
5. Confirm the root-only runtime status reports `database_driver=postgres` and
   matches the active systemd PID.
6. Create and verify a new PostgreSQL archive after deployment.

Recovery procedures are in [recovery.md](recovery.md).
