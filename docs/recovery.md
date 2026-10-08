# PostgreSQL Backup and Recovery

PostgreSQL is the only persistent database supported by BridgeOps. Redis is a
disposable analytics cache and is never part of a recovery archive.

## Prerequisites

Install Bash, PostgreSQL client tools, PyYAML, `zstd`, `tar`, `curl`, `git`, and
`psmisc`. Keep the BridgeOps config, PostgreSQL environment file, runtime status
file, backup directory, and archives root-only.

The active service must use one explicit config file and one PostgreSQL
`EnvironmentFile`. The YAML and environment values for host, port, user,
database, timezone, and SSL mode must agree. `database.timeZone` must be `UTC`.

## Create a Backup

These scripts target the systemd binary deployment described in
`packaging/server/`. For Docker Compose deployments, see the backup section of
the [user guide](usage.md#backup-and-upgrade). The server-side fixed script is:

```bash
sudo ./scripts/bridgeops-backup-postgres.sh --output /var/backups/bridgeops
```

The gate verifies the active systemd PID, process-start identity, runtime
database driver, PostgreSQL server identity, migration checksums, schema,
metadata timezone, archive table of contents, and external SHA-256 sidecar. A
failed check produces no usable archive.

Every archive is a full, independently restorable snapshot. Keep the archive
and its `.sha256` file together. Do not unpack archives during routine transfer
or place them inside the Git workspace.

## Verify and Restore

Verification is read-only:

```bash
sudo ./scripts/bridgeops-restore-postgres.sh \
  BridgeOps-backup-TIMESTAMP.tar.zst --verify-only
```

Restore only into an empty PostgreSQL target with an explicit target environment
file and matching migration set:

```bash
sudo ./scripts/bridgeops-restore-postgres.sh \
  BridgeOps-backup-TIMESTAMP.tar.zst --yes \
  --postgres-env /etc/bridgeops/postgres.env
```

The restore gate rejects a non-empty target, mismatched service configuration,
unexpected migration checksums, unsafe paths, and archives whose schema or
critical tables are incomplete. The restore script starts `bridgeops.service`
and verifies the loopback `/healthz` endpoint before it returns success. Then
inspect the runtime status and create a fresh backup.

## Security Boundary

Backups contain encrypted credentials and business records. Protect them as
production secrets. These scripts provide fail-closed consistency checks, but
cannot establish trust on a host whose root account or operating system is
already compromised.
