#!/usr/bin/env bash

bridgeops_validate_postgres_service_gate() {
  local load_state exec_start expected_exec_start remainder
  local unit_environment_files expected_environment_file unit_environment pass_environment main_pid runtime_driver

  if ! load_state="$(systemctl show "$SERVICE_NAME" -p LoadState --value 2>/dev/null)"; then
    echo "BridgeOps systemd service is not installed." >&2
    return 1
  fi
  [[ "$load_state" == loaded ]] || {
    echo "BridgeOps systemd service is not loaded." >&2
    return 1
  }

  if ! exec_start="$(systemctl show "$SERVICE_NAME" -p ExecStart --value 2>/dev/null)"; then
    echo "Cannot inspect BridgeOps service ExecStart." >&2
    return 1
  fi
  expected_exec_start="{ path=/usr/local/bin/bridgeops ; argv[]=/usr/local/bin/bridgeops -config ${CONFIG_FILE} ;"
  [[ "$exec_start" == "$expected_exec_start"* ]] || {
    echo "BridgeOps service does not use the reviewed binary and config file." >&2
    return 1
  }
  remainder="${exec_start#"$expected_exec_start"}"
  [[ "$remainder" == *"}" && "$remainder" != *"{"* && "$remainder" != *"argv[]="* ]] || {
    echo "BridgeOps service has an unexpected ExecStart definition." >&2
    return 1
  }

  if ! unit_environment_files="$(systemctl show "$SERVICE_NAME" -p EnvironmentFiles --value 2>/dev/null)"; then
    echo "Cannot inspect BridgeOps service EnvironmentFiles." >&2
    return 1
  fi
  expected_environment_file="${POSTGRES_ENV_FILE} (ignore_errors=no)"
  [[ "$unit_environment_files" == "$expected_environment_file" ]] || {
    echo "BridgeOps service does not use exactly the reviewed PostgreSQL environment file." >&2
    return 1
  }

  if ! unit_environment="$(systemctl show "$SERVICE_NAME" -p Environment --value 2>/dev/null)"; then
    echo "Cannot inspect BridgeOps service Environment." >&2
    return 1
  fi
  [[ "$unit_environment" != *DATABASE_* ]] || {
    echo "BridgeOps service has inline database environment overrides." >&2
    return 1
  }

  if ! pass_environment="$(systemctl show "$SERVICE_NAME" -p PassEnvironment --value 2>/dev/null)"; then
    echo "Cannot inspect BridgeOps service PassEnvironment." >&2
    return 1
  fi
  [[ "$pass_environment" != *DATABASE_* ]] || {
    echo "BridgeOps service passes database environment overrides." >&2
    return 1
  }

  if ! systemctl is-active --quiet "$SERVICE_NAME"; then
    echo "BridgeOps service must be active before backup." >&2
    return 1
  fi
  if ! main_pid="$(systemctl show "$SERVICE_NAME" -p MainPID --value 2>/dev/null)"; then
    echo "Cannot inspect the active BridgeOps service PID." >&2
    return 1
  fi
  [[ "$main_pid" =~ ^[1-9][0-9]*$ ]] || {
    echo "BridgeOps service has an invalid active PID." >&2
    return 1
  }
  if ! runtime_driver="$(python3 "$CONFIG_HELPER" --config "$CONFIG_FILE" --runtime-status "$RUNTIME_STATUS_FILE" --driver-only --expected-pid "$main_pid")"; then
    echo "BridgeOps active database driver did not pass validation." >&2
    return 1
  fi
  [[ "$runtime_driver" == postgres ]] || {
    echo "PostgreSQL backup requires the active database driver to be postgres." >&2
    return 1
  }
}

if [[ "${BASH_SOURCE[0]}" != "$0" ]]; then
  return 0
fi

bridgeops_clean_env_marker="__bridgeops_internal_clean_env_v1__"
if [[ ${BRIDGEOPS_CLEAN_ENV:-} != 1 || ${1:-} != "$bridgeops_clean_env_marker" || -n ${BASH_ENV:-}${ENV:-}${LD_PRELOAD:-}${LD_LIBRARY_PATH:-}${PYTHONHOME:-}${PYTHONPATH:-}${PYTHONSTARTUP:-} ]]; then
  exec /usr/bin/env -i \
    HOME=/root PATH=/usr/sbin:/usr/bin:/sbin:/bin BRIDGEOPS_CLEAN_ENV=1 \
    BRIDGEOPS_BACKUP_DIR="${BRIDGEOPS_BACKUP_DIR:-}" \
    BRIDGEOPS_REPO_DIR="${BRIDGEOPS_REPO_DIR:-}" \
    BRIDGEOPS_CONFIG_FILE="${BRIDGEOPS_CONFIG_FILE:-}" \
    BRIDGEOPS_POSTGRES_ENV_FILE="${BRIDGEOPS_POSTGRES_ENV_FILE:-}" \
    BRIDGEOPS_MIGRATIONS_DIR="${BRIDGEOPS_MIGRATIONS_DIR:-}" \
    BRIDGEOPS_CONFIG_HELPER="${BRIDGEOPS_CONFIG_HELPER:-}" \
    BRIDGEOPS_RUNTIME_STATUS_FILE="${BRIDGEOPS_RUNTIME_STATUS_FILE:-}" \
    BRIDGEOPS_SERVICE_NAME="${BRIDGEOPS_SERVICE_NAME:-}" \
    /bin/bash --noprofile --norc "$(/usr/bin/readlink -f "$0")" "$bridgeops_clean_env_marker" "$@"
fi
shift

set -Eeuo pipefail

umask 077
PATH=/usr/sbin:/usr/bin:/sbin:/bin
export PATH
unset BRIDGEOPS_CLEAN_ENV TAR_OPTIONS GIT_CONFIG GIT_CONFIG_GLOBAL GIT_CONFIG_SYSTEM GIT_CONFIG_NOSYSTEM BASH_ENV ENV CDPATH
unset LD_PRELOAD LD_LIBRARY_PATH PYTHONHOME PYTHONPATH PYTHONSTARTUP PYTHONINSPECT
for environment_name in $(compgen -e); do
  case "$environment_name" in
    HOME|PATH|BRIDGEOPS_BACKUP_DIR|BRIDGEOPS_REPO_DIR|BRIDGEOPS_CONFIG_FILE|BRIDGEOPS_POSTGRES_ENV_FILE|BRIDGEOPS_MIGRATIONS_DIR|BRIDGEOPS_CONFIG_HELPER|BRIDGEOPS_RUNTIME_STATUS_FILE|BRIDGEOPS_SERVICE_NAME) ;;
    *) unset "$environment_name" ;;
  esac
done
while read -r _ _ function_name; do
  [[ "$function_name" == bridgeops_validate_postgres_service_gate ]] || unset -f "$function_name"
done < <(declare -F)

FINAL=false
OUTPUT_DIR="${BRIDGEOPS_BACKUP_DIR:-/var/backups/bridgeops}"
REPO_DIR="${BRIDGEOPS_REPO_DIR:-/opt/bridgeops}"
CONFIG_FILE="${BRIDGEOPS_CONFIG_FILE:-/etc/bridgeops/config.yaml}"
POSTGRES_ENV_FILE="${BRIDGEOPS_POSTGRES_ENV_FILE:-/etc/bridgeops/postgres.env}"
MIGRATIONS_DIR="${BRIDGEOPS_MIGRATIONS_DIR:-${REPO_DIR}/backend/migrations}"
SCRIPT_DIR="$(cd "$(dirname "$(/usr/bin/readlink -f "$0")")" && pwd -P)"
CONFIG_HELPER="${BRIDGEOPS_CONFIG_HELPER:-${SCRIPT_DIR}/bridgeops-postgres-config.py}"
RUNTIME_STATUS_FILE="${BRIDGEOPS_RUNTIME_STATUS_FILE:-/run/bridgeops/runtime-status.json}"
SERVICE_NAME="${BRIDGEOPS_SERVICE_NAME:-bridgeops.service}"

KEY_TABLES=(
  channels
  channel_cost_states
  channel_funding_events
  gateway_downstream_keys
  gateway_upstream_keys
  gateway_request_logs
  gateway_request_aggregates
  system_metadata
)

usage() {
  cat <<'EOF'
Usage: sudo ./bridgeops-backup-postgres.sh [--final] [--output DIR]

  --final       Stop BridgeOps before the snapshot and leave it stopped.
  --output DIR  Backup destination (default: /var/backups/bridgeops).
  -h, --help    Show this help.

The root-owned 0400/0600 PostgreSQL environment file must explicitly match the
database driver, host, port, user, database, UTC timezone and SSL mode in the
BridgeOps YAML config. The script also verifies the connected server identity,
embedded migration checksums and UTC metadata before pg_dump can run.
EOF
}

die() {
  echo "$1" >&2
  exit 1
}

require_private_file() {
  local path="$1" label="$2" mode
  [[ -f "$path" && ! -L "$path" ]] || die "$label must be a regular, non-symlink file."
  [[ -r "$path" ]] || die "$label is not readable."
  mode="$(stat -c '%a' "$path")"
  [[ "$mode" == 600 || "$mode" == 400 ]] || die "$label must have mode 0600 or 0400."
  [[ "$(stat -c '%u' "$path")" == 0 ]] || die "$label must be owned by root."
}

require_trusted_file() {
  local path="$1" label="$2" mode
  [[ -f "$path" && ! -L "$path" && -r "$path" ]] || die "$label must be a readable, non-symlink file."
  mode="$(stat -c '%a' "$path")"
  (( (8#$mode & 0022) == 0 )) || die "$label must not be group- or other-writable."
  [[ "$(stat -c '%u' "$path")" == 0 ]] || die "$label must be owned by root."
}

require_trusted_root_helper() {
  local path="$1" label="$2" current mode
  require_trusted_file "$path" "$label"
  [[ "$path" == /* && "$(/usr/bin/readlink -m "$path")" == "$path" ]] ||
    die "$label path must be canonical and must not traverse symlinks."
  current="$(dirname "$path")"
  while :; do
    [[ -d "$current" && ! -L "$current" && "$(stat -c '%u' "$current")" == 0 ]] ||
      die "$label has an unsafe parent directory."
    mode="$(stat -c '%a' "$current")"
    (( (8#$mode & 0022) == 0 )) || die "$label has a writable parent directory."
    [[ "$current" == / ]] && break
    current="$(dirname "$current")"
  done
}

build_expected_migrations() {
  local output="$1" file name version_text version checksum
  local -a files
  shopt -s nullglob
  files=("$MIGRATIONS_DIR"/*.sql)
  shopt -u nullglob
  ((${#files[@]} > 0)) || die "No trusted PostgreSQL migration files found."
  : > "$output"
  for file in "${files[@]}"; do
    require_trusted_file "$file" "Migration file"
    [[ -s "$file" ]] || die "A trusted PostgreSQL migration file is empty."
    name="${file##*/}"
    [[ "$name" =~ ^([0-9]+)_[A-Za-z][A-Za-z0-9_-]*\.sql$ ]] || die "Invalid PostgreSQL migration filename."
    version_text="${BASH_REMATCH[1]}"
    ((${#version_text} <= 18)) || die "PostgreSQL migration version is too large."
    version="$((10#$version_text))"
    ((version > 0)) || die "PostgreSQL migration versions must be positive."
    checksum="$(sha256sum "$file" | awk '{print $1}')"
    printf '%s\t%s\n' "$version" "$checksum" >> "$output"
  done
  LC_ALL=C sort -t $'\t' -k1,1n -o "$output" "$output"
  awk -F '\t' 'NR > 1 && $1 == previous { exit 1 } { previous = $1 }' "$output" ||
    die "Duplicate PostgreSQL migration version in trusted files."
}

# A production lineage recorded the route-exclusion migration at version 000076
# before this checkout's policy migration. The application migration runner
# accepts these exact historical checksums; the backup gate must use the same
# bounded compatibility set without accepting arbitrary drift.
accepted_legacy_migration_checksum() {
  case "$1:$2" in
    76:a48380d56e50429d11dc791cd18b60254bc7e15626ce012b8452f1a6d5e31185|\
    77:87b8623365c93502db99eaf7d3dd05afc5854ff44f385faa97c3aa56626572b9)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

migration_checksums_match() {
  local expected_file="$1" actual_file="$2" version checksum expected_checksum
  local -A expected_checksums actual_versions
  while IFS=$'\t' read -r version checksum; do
    [[ "$version" =~ ^[0-9]+$ && "$checksum" =~ ^[0-9a-f]{64}$ ]] || return 1
    [[ -z "${expected_checksums[$version]+present}" ]] || return 1
    expected_checksums["$version"]="$checksum"
  done < "$expected_file"
  while IFS=$'\t' read -r version checksum; do
    [[ "$version" =~ ^[0-9]+$ && "$checksum" =~ ^[0-9a-f]{64}$ ]] || return 1
    [[ -z "${actual_versions[$version]+present}" ]] || return 1
    expected_checksum="${expected_checksums[$version]-}"
    [[ -n "$expected_checksum" ]] || return 1
    if [[ "$checksum" != "$expected_checksum" ]] && ! accepted_legacy_migration_checksum "$version" "$checksum"; then
      return 1
    fi
    actual_versions["$version"]=1
  done < "$actual_file"
  [[ "${#actual_versions[@]}" -eq "${#expected_checksums[@]}" ]]
}

write_key_tables() {
  local output="$1" table
  : > "$output"
  for table in "${KEY_TABLES[@]}"; do
    printf '%s\n' "$table" >> "$output"
  done
}

capture_schema() {
  local tables_file="$1" columns_file="$2"
  "${PSQL[@]}" -c "SELECT c.relname, c.relkind FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p') ORDER BY c.relname" > "$tables_file"
  "${PSQL[@]}" -c "SELECT c.relname, a.attnum, a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), a.attnotnull::int, a.attidentity, a.attgenerated FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON c.oid = a.attrelid JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p') AND a.attnum > 0 AND NOT a.attisdropped ORDER BY c.relname, a.attnum" > "$columns_file"
  [[ -s "$tables_file" && -s "$columns_file" ]] || die "PostgreSQL public schema manifest is empty."
}

verify_key_tables() {
  local tables_file="$1" table
  for table in "${KEY_TABLES[@]}"; do
    grep -Fqx "$table"$'\t''r' "$tables_file" || die "Required PostgreSQL table is missing."
  done
}

verify_dump_toc() {
  local toc_file="$1" table
  for table in "${KEY_TABLES[@]}"; do
    grep -Eq " TABLE public ${table} " "$toc_file" || die "PostgreSQL dump is missing a required table definition."
    grep -Eq " TABLE DATA public ${table} " "$toc_file" || die "PostgreSQL dump is missing required table data."
  done
}

while (($#)); do
  case "$1" in
    --final)
      FINAL=true
      shift
      ;;
    --output)
      [[ $# -ge 2 ]] || { echo "--output requires a directory" >&2; exit 2; }
      OUTPUT_DIR="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

[[ ${EUID} -eq 0 ]] || die "Run this script as root."
for command in awk cmp find getent grep install pg_dump pg_isready pg_restore psql python3 sha256sum sort stat systemctl tar zstd; do
  command -v "$command" >/dev/null || die "Missing command: $command"
done
[[ "$SERVICE_NAME" =~ ^[A-Za-z0-9_.@:-]+$ ]] || die "Invalid systemd service name."

require_private_file "$CONFIG_FILE" "BridgeOps config"
require_private_file "$POSTGRES_ENV_FILE" "PostgreSQL environment file"
require_trusted_root_helper "$CONFIG_HELPER" "PostgreSQL config helper"
[[ -d "$MIGRATIONS_DIR" && ! -L "$MIGRATIONS_DIR" ]] || die "Trusted PostgreSQL migrations directory is invalid."
CONFIG_FILE="$(/usr/bin/readlink -f "$CONFIG_FILE")"
POSTGRES_ENV_FILE="$(/usr/bin/readlink -f "$POSTGRES_ENV_FILE")"
[[ "$RUNTIME_STATUS_FILE" == /* ]] || die "BridgeOps runtime status path must be absolute."

bridgeops_validate_postgres_service_gate || exit 1

mkdir -p "$OUTPUT_DIR"
[[ -d "$OUTPUT_DIR" && ! -L "$OUTPUT_DIR" ]] || die "Backup destination must be a real directory."
chmod 700 "$OUTPUT_DIR"
OUTPUT_DIR="$(/usr/bin/readlink -f "$OUTPUT_DIR")"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
stage="$(mktemp -d "${OUTPUT_DIR}/.bridgeops-postgres-backup-${timestamp}.XXXXXX")"
runtime="$(mktemp -d /tmp/bridgeops-postgres-backup.XXXXXX)"
archive="${OUTPUT_DIR}/BridgeOps-backup-${timestamp}.tar.zst"
archive_tmp="${archive}.tmp"
service_was_active=false
success=false

cleanup() {
  local rc=$?
  trap - EXIT
  rm -f "$archive_tmp"
  rm -rf "$stage" "$runtime"
  if [[ "$FINAL" == true && "$service_was_active" == true && "$success" != true ]]; then
    systemctl start "$SERVICE_NAME" || true
  fi
  exit "$rc"
}
trap cleanup EXIT

[[ ! -e "$archive" && ! -e "${archive}.sha256" && ! -e "$archive_tmp" ]] || die "Refusing to overwrite an existing backup timestamp."

connection_file="$runtime/connection"
if ! python3 "$CONFIG_HELPER" --config "$CONFIG_FILE" --env "$POSTGRES_ENV_FILE" --connection > "$connection_file"; then
  die "BridgeOps and PostgreSQL connection settings did not pass validation."
fi
mapfile -d '' -t connection < "$connection_file"
rm -f "$connection_file"
((${#connection[@]} == 9)) || die "PostgreSQL config helper returned an invalid result."
PGHOST="${connection[1]}"
PGPORT="${connection[2]}"
PGUSER="${connection[3]}"
PGDATABASE="${connection[4]}"
PGSSLMODE="${connection[6]}"
PGPASSWORD="${connection[7]}"
PGPASSFILE="${connection[8]}"
export PGHOST PGPORT PGUSER PGDATABASE PGSSLMODE PGCONNECT_TIMEOUT=15 PGAPPNAME=bridgeops-backup-gate
if [[ -n "$PGPASSWORD" ]]; then
  export PGPASSWORD
  unset PGPASSFILE
elif [[ -n "$PGPASSFILE" ]]; then
  require_private_file "$PGPASSFILE" "PostgreSQL password file"
  PGPASSFILE="$(/usr/bin/readlink -f "$PGPASSFILE")"
  export PGPASSFILE
else
  die "PostgreSQL password input is missing."
fi

PSQL=(psql -X -qAt -F $'\t' -v ON_ERROR_STOP=1 --dbname="$PGDATABASE")

if [[ "$FINAL" == true ]]; then
  echo "Stopping ${SERVICE_NAME} for the final PostgreSQL snapshot..."
  service_was_active=true
  systemctl stop "$SERVICE_NAME"
fi
if [[ "$FINAL" == true ]]; then
  state="$(systemctl is-active "$SERVICE_NAME" 2>/dev/null || true)"
  case "$state" in
    inactive|failed) ;;
    *) die "Cannot prove ${SERVICE_NAME} is stopped (state: ${state:-unknown})." ;;
  esac
fi

echo "Checking PostgreSQL backup gates..."
PGCONNECT_TIMEOUT=15 pg_isready -q || die "PostgreSQL is not ready."
if ! "${PSQL[@]}" -c "SELECT current_database(), current_user, COALESCE(inet_server_addr()::text, ''), inet_server_port(), current_setting('TimeZone'), current_setting('server_version_num')" > "$runtime/database-identity-query"; then
  die "Cannot query PostgreSQL server identity."
fi
mapfile -t identity_rows < "$runtime/database-identity-query"
((${#identity_rows[@]} == 1)) || die "PostgreSQL server returned an invalid identity."
IFS=$'\t' read -r actual_database actual_user server_address server_port server_timezone server_version <<< "${identity_rows[0]}"
server_address="${server_address%%/*}"
[[ "$actual_database" == "$PGDATABASE" && "$actual_user" == "$PGUSER" && "$server_port" == "$PGPORT" ]] ||
  die "Connected PostgreSQL database identity does not match the reviewed configuration."
[[ "$server_timezone" == UTC ]] || die "Connected PostgreSQL session timezone is not UTC."
[[ -n "$server_address" ]] || die "BridgeOps PostgreSQL backups require a TCP connection."
host_matches=false
while read -r resolved_address _; do
  if [[ "$resolved_address" == "$server_address" ]]; then
    host_matches=true
    break
  fi
done < <(getent ahosts "$PGHOST")
[[ "$host_matches" == true ]] || die "Connected PostgreSQL server address does not match database.host."

build_expected_migrations "$runtime/expected-migrations.tsv"
if ! "${PSQL[@]}" -c "SELECT version, lower(checksum) FROM public.schema_migrations ORDER BY version" > "$runtime/database-migrations.tsv"; then
  die "Cannot read PostgreSQL migration metadata."
fi
migration_checksums_match "$runtime/expected-migrations.tsv" "$runtime/database-migrations.tsv" ||
  die "PostgreSQL migration checksums do not match the trusted checkout."
if ! "${PSQL[@]}" -c "SELECT value FROM public.system_metadata WHERE key = 'database.timeZone' ORDER BY value" > "$runtime/database-timezone"; then
  die "Cannot read PostgreSQL timezone metadata."
fi
mapfile -t timezone_rows < "$runtime/database-timezone"
((${#timezone_rows[@]} == 1)) && [[ "${timezone_rows[0]}" == UTC ]] ||
  die "PostgreSQL database.timeZone metadata must contain exactly one UTC value."

capture_schema "$runtime/schema-tables-before.tsv" "$runtime/schema-columns-before.tsv"
verify_key_tables "$runtime/schema-tables-before.tsv"

mkdir -p "$stage"/{config,data,system,source,logs}
echo "Creating a consistent PostgreSQL snapshot..."
PGCONNECT_TIMEOUT=15 pg_dump \
  --format=custom \
  --no-owner \
  --no-acl \
  --schema=public \
  --file="$stage/data/bridgeops.pg_dump" \
  --dbname="$PGDATABASE"
pg_restore --list "$stage/data/bridgeops.pg_dump" > "$stage/data/pg-restore.list"
verify_dump_toc "$stage/data/pg-restore.list"

capture_schema "$runtime/schema-tables-after.tsv" "$runtime/schema-columns-after.tsv"
cmp -s "$runtime/schema-tables-before.tsv" "$runtime/schema-tables-after.tsv" ||
  die "PostgreSQL schema changed during the backup."
cmp -s "$runtime/schema-columns-before.tsv" "$runtime/schema-columns-after.tsv" ||
  die "PostgreSQL columns changed during the backup."
if ! "${PSQL[@]}" -c "SELECT version, lower(checksum) FROM public.schema_migrations ORDER BY version" > "$runtime/database-migrations-after.tsv"; then
  die "Cannot recheck PostgreSQL migration metadata."
fi
migration_checksums_match "$runtime/expected-migrations.tsv" "$runtime/database-migrations-after.tsv" ||
  die "PostgreSQL migrations changed during the backup."
if [[ "$FINAL" != true ]]; then
  bridgeops_validate_postgres_service_gate || exit 1
fi

install -m 0600 "$CONFIG_FILE" "$stage/config/config.yaml"
install -m 0600 "$POSTGRES_ENV_FILE" "$stage/config/postgres.env"
if [[ -n "${PGPASSFILE:-}" ]]; then
  install -m 0600 "$PGPASSFILE" "$stage/config/pgpass"
fi
install -m 0600 "$runtime/expected-migrations.tsv" "$stage/data/schema-migrations.tsv"
install -m 0600 "$runtime/schema-tables-before.tsv" "$stage/data/schema-tables.tsv"
install -m 0600 "$runtime/schema-columns-before.tsv" "$stage/data/schema-columns.tsv"
write_key_tables "$stage/data/key-tables"
chmod 0600 "$stage/data/key-tables"
printf '%s\n' postgres > "$stage/data/database-driver"
chmod 0600 "$stage/data/database-driver"
{
  printf 'driver\tpostgres\n'
  printf 'host\t%s\n' "$PGHOST"
  printf 'port\t%s\n' "$PGPORT"
  printf 'user\t%s\n' "$PGUSER"
  printf 'database\t%s\n' "$PGDATABASE"
  printf 'timezone\tUTC\n'
  printf 'sslmode\t%s\n' "$PGSSLMODE"
  printf 'server_address\t%s\n' "$server_address"
  printf 'server_version_num\t%s\n' "$server_version"
} > "$stage/data/database-identity.tsv"
chmod 0600 "$stage/data/database-identity.tsv"

for unit in /etc/systemd/system/bridgeops.service; do
  [[ -f "$unit" ]] && cp -a "$unit" "$stage/system/"
done
if [[ -d /etc/systemd/system/bridgeops.service.d ]]; then
  cp -a /etc/systemd/system/bridgeops.service.d "$stage/system/"
fi
[[ -f /etc/caddy/Caddyfile ]] && cp -a /etc/caddy/Caddyfile "$stage/system/"
[[ -d /var/log/bridgeops ]] && cp -a /var/log/bridgeops/. "$stage/logs/"

if [[ -d "$REPO_DIR/.git" ]]; then
  command -v git >/dev/null || die "Missing command: git"
  echo "Saving Git history and the tracked source snapshot..."
  git -C "$REPO_DIR" bundle create "$stage/source/BridgeOps-all.bundle" --all
  git -C "$REPO_DIR" archive --format=tar HEAD | zstd -6 -T1 -o "$stage/source/BridgeOps-source.tar.zst"
  git -C "$REPO_DIR" rev-parse HEAD > "$stage/source/COMMIT"
  git -C "$REPO_DIR" remote -v > "$stage/source/REMOTES"
fi

cat > "$stage/RESTORE-README.txt" <<EOF
BridgeOps PostgreSQL recovery snapshot created at ${timestamp}.

The archive contains a pg_dump custom-format snapshot, the reviewed database
identity, migration and schema manifests, the BridgeOps configuration, and the
private PostgreSQL environment file. Treat every credential in this archive as
private. Restore only with bridgeops-restore-postgres.sh from a trusted checkout.
EOF

unsafe_entry="$(find "$stage" -mindepth 1 ! -type f ! -type d -print -quit)"
[[ -z "$unsafe_entry" ]] || die "Backup input contains a link or special file."
(
  cd "$stage"
  find . -type f ! -name SHA256SUMS -exec sha256sum '{}' ';' | LC_ALL=C sort > SHA256SUMS
)

echo "Compressing the PostgreSQL backup..."
tar -C "$(dirname "$stage")" -I 'zstd -6 -T1' -cf "$archive_tmp" "$(basename "$stage")"
zstd -t "$archive_tmp"
tar -I zstd -tf "$archive_tmp" >/dev/null
mv "$archive_tmp" "$archive"
chmod 600 "$archive"
(
  cd "$OUTPUT_DIR"
  sha256sum "$(basename "$archive")" > "$(basename "$archive").sha256"
)
chmod 600 "${archive}.sha256"

success=true
echo
echo "PostgreSQL backup complete: $archive"
echo "Checksum:                 ${archive}.sha256"
if [[ "$FINAL" == true && "$service_was_active" == true ]]; then
  echo "${SERVICE_NAME} remains stopped so no post-backup records can be created."
fi
