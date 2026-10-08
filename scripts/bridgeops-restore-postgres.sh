#!/usr/bin/env bash

bridgeops_validate_postgres_restore_unit() {
  local load_state exec_start expected_exec_start remainder
  local unit_environment_files expected_environment_file unit_environment pass_environment

  load_state="$(systemctl show "$SERVICE_NAME" -p LoadState --value 2>/dev/null)" || {
    echo "Target systemd service is not installed." >&2
    return 1
  }
  [[ "$load_state" == loaded ]] || {
    echo "Target systemd service is not loaded." >&2
    return 1
  }
  exec_start="$(systemctl show "$SERVICE_NAME" -p ExecStart --value 2>/dev/null)" || {
    echo "Cannot inspect target service ExecStart." >&2
    return 1
  }
  expected_exec_start="{ path=/usr/local/bin/bridgeops ; argv[]=/usr/local/bin/bridgeops -config ${TARGET_CONFIG} ;"
  [[ "$exec_start" == "$expected_exec_start"* ]] || {
    echo "Target service does not use the reviewed binary and config." >&2
    return 1
  }
  remainder="${exec_start#"$expected_exec_start"}"
  [[ "$remainder" == *"}" && "$remainder" != *"{"* && "$remainder" != *"argv[]="* ]] || {
    echo "Target service has an unexpected ExecStart definition." >&2
    return 1
  }
  unit_environment_files="$(systemctl show "$SERVICE_NAME" -p EnvironmentFiles --value 2>/dev/null)" || {
    echo "Cannot inspect target service EnvironmentFiles." >&2
    return 1
  }
  expected_environment_file="${TARGET_ENV} (ignore_errors=no)"
  [[ "$unit_environment_files" == "$expected_environment_file" ]] || {
    echo "Target service does not use exactly the reviewed PostgreSQL environment file." >&2
    return 1
  }
  unit_environment="$(systemctl show "$SERVICE_NAME" -p Environment --value 2>/dev/null)" || {
    echo "Cannot inspect target service Environment." >&2
    return 1
  }
  [[ "$unit_environment" != *DATABASE_* ]] || {
    echo "Target service has inline database environment overrides." >&2
    return 1
  }
  pass_environment="$(systemctl show "$SERVICE_NAME" -p PassEnvironment --value 2>/dev/null)" || {
    echo "Cannot inspect target service PassEnvironment." >&2
    return 1
  }
  [[ "$pass_environment" != *DATABASE_* ]] || {
    echo "Target service passes database environment overrides." >&2
    return 1
  }
}

bridgeops_validate_postgres_restored_runtime() {
  local main_pid runtime_driver
  systemctl is-active --quiet "$SERVICE_NAME" || {
    echo "Restored BridgeOps service is not active." >&2
    return 1
  }
  main_pid="$(systemctl show "$SERVICE_NAME" -p MainPID --value 2>/dev/null)" || {
    echo "Cannot inspect restored BridgeOps service PID." >&2
    return 1
  }
  [[ "$main_pid" =~ ^[1-9][0-9]*$ ]] || {
    echo "Restored BridgeOps service has an invalid PID." >&2
    return 1
  }
  runtime_driver="$(python3 "$CONFIG_HELPER" --config "$TARGET_CONFIG" --runtime-status "$RUNTIME_STATUS_FILE" --driver-only --expected-pid "$main_pid")" || {
    echo "Restored BridgeOps runtime status did not pass validation." >&2
    return 1
  }
  [[ "$runtime_driver" == postgres ]] || {
    echo "Restored BridgeOps runtime database driver is not postgres." >&2
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
    BRIDGEOPS_REPO_DIR="${BRIDGEOPS_REPO_DIR:-}" \
    BRIDGEOPS_CONFIG_FILE="${BRIDGEOPS_CONFIG_FILE:-}" \
    BRIDGEOPS_POSTGRES_ENV_FILE="${BRIDGEOPS_POSTGRES_ENV_FILE:-}" \
    BRIDGEOPS_MIGRATIONS_DIR="${BRIDGEOPS_MIGRATIONS_DIR:-}" \
    BRIDGEOPS_CONFIG_HELPER="${BRIDGEOPS_CONFIG_HELPER:-}" \
    BRIDGEOPS_RUNTIME_STATUS_FILE="${BRIDGEOPS_RUNTIME_STATUS_FILE:-}" \
    BRIDGEOPS_SERVICE_NAME="${BRIDGEOPS_SERVICE_NAME:-}" \
    BRIDGEOPS_HEALTH_URL="${BRIDGEOPS_HEALTH_URL:-}" \
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
    HOME|PATH|BRIDGEOPS_REPO_DIR|BRIDGEOPS_CONFIG_FILE|BRIDGEOPS_POSTGRES_ENV_FILE|BRIDGEOPS_MIGRATIONS_DIR|BRIDGEOPS_CONFIG_HELPER|BRIDGEOPS_RUNTIME_STATUS_FILE|BRIDGEOPS_SERVICE_NAME|BRIDGEOPS_HEALTH_URL) ;;
    *) unset "$environment_name" ;;
  esac
done
while read -r _ _ function_name; do
  case "$function_name" in
    bridgeops_validate_postgres_restore_unit|bridgeops_validate_postgres_restored_runtime) ;;
    *) unset -f "$function_name" ;;
  esac
done < <(declare -F)

ASSUME_YES=false
VERIFY_ONLY=false
REPO_DIR="${BRIDGEOPS_REPO_DIR:-/opt/bridgeops}"
TARGET_CONFIG="${BRIDGEOPS_CONFIG_FILE:-/etc/bridgeops/config.yaml}"
TARGET_ENV="${BRIDGEOPS_POSTGRES_ENV_FILE:-/etc/bridgeops/postgres.env}"
MIGRATIONS_DIR="${BRIDGEOPS_MIGRATIONS_DIR:-${REPO_DIR}/backend/migrations}"
SCRIPT_DIR="$(cd "$(dirname "$(/usr/bin/readlink -f "$0")")" && pwd -P)"
CONFIG_HELPER="${BRIDGEOPS_CONFIG_HELPER:-${SCRIPT_DIR}/bridgeops-postgres-config.py}"
RUNTIME_STATUS_FILE="${BRIDGEOPS_RUNTIME_STATUS_FILE:-/run/bridgeops/runtime-status.json}"
SERVICE_NAME="${BRIDGEOPS_SERVICE_NAME:-bridgeops.service}"
HEALTH_URL="${BRIDGEOPS_HEALTH_URL:-http://127.0.0.1:8418/healthz}"

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
Usage: sudo ./bridgeops-restore-postgres.sh BACKUP.tar.zst --verify-only
       sudo ./bridgeops-restore-postgres.sh BACKUP.tar.zst --yes

  --verify-only       Verify the archive, dump, migration checksums and schema
                      manifests without connecting to a restore target.
  --yes               Confirm restore into the reviewed empty target database.
  --config FILE       Target BridgeOps YAML config.
  --postgres-env FILE Target PostgreSQL environment file.
  --service NAME      Target BridgeOps systemd service.
  --health-url URL    Loopback BridgeOps health endpoint.
  -h, --help          Show this help.

The restore target must be empty, have no other sessions, and exactly match the
target config and environment file. This script never installs archived config,
environment, executable, systemd, proxy or SSH files. It returns success only
after schema/data validation, service start and the BridgeOps health endpoint.
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

# Keep restore validation aligned with the application migration runner and
# backup gate. Only the two reviewed historical lineage aliases are accepted.
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
}

verify_dump_toc() {
  local toc_file="$1" table
  for table in "${KEY_TABLES[@]}"; do
    grep -Eq " TABLE public ${table} " "$toc_file" || die "PostgreSQL dump is missing a required table definition."
    grep -Eq " TABLE DATA public ${table} " "$toc_file" || die "PostgreSQL dump is missing required table data."
  done
}

identity_value() {
  local key="$1" line
  line="$(awk -F '\t' -v wanted="$key" '$1 == wanted { count++; value = $2 } END { if (count == 1) print value; else exit 1 }' "$root/data/database-identity.tsv")" ||
    die "Backup database identity is incomplete."
  printf '%s' "$line"
}

archive=""
while (($#)); do
  case "$1" in
    --yes)
      ASSUME_YES=true
      shift
      ;;
    --verify-only)
      VERIFY_ONLY=true
      shift
      ;;
    --config)
      [[ $# -ge 2 ]] || { echo "--config requires a file" >&2; exit 2; }
      TARGET_CONFIG="$2"
      shift 2
      ;;
    --postgres-env)
      [[ $# -ge 2 ]] || { echo "--postgres-env requires a file" >&2; exit 2; }
      TARGET_ENV="$2"
      shift 2
      ;;
    --service)
      [[ $# -ge 2 ]] || { echo "--service requires a unit name" >&2; exit 2; }
      SERVICE_NAME="$2"
      shift 2
      ;;
    --health-url)
      [[ $# -ge 2 ]] || { echo "--health-url requires a URL" >&2; exit 2; }
      HEALTH_URL="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    -*)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
    *)
      [[ -z "$archive" ]] || { echo "Only one backup archive may be supplied." >&2; exit 2; }
      archive="$1"
      shift
      ;;
  esac
done

[[ ${EUID} -eq 0 ]] || die "Run this script as root."
[[ -n "$archive" ]] || { usage >&2; exit 2; }
[[ "$VERIFY_ONLY" != true || "$ASSUME_YES" != true ]] || { echo "Choose --verify-only or --yes, not both." >&2; exit 2; }
[[ "$VERIFY_ONLY" == true || "$ASSUME_YES" == true ]] || { echo "Use --verify-only or confirm the restore with --yes." >&2; exit 2; }
[[ -f "$archive" && ! -L "$archive" ]] || die "Backup archive must be a regular, non-symlink file."
archive="$(/usr/bin/readlink -f "$archive")"

for command in awk cmp df find grep pg_restore python3 sha256sum sort stat tar zstd; do
  command -v "$command" >/dev/null || die "Missing command: $command"
done
require_trusted_root_helper "$CONFIG_HELPER" "PostgreSQL config helper"
[[ -d "$MIGRATIONS_DIR" && ! -L "$MIGRATIONS_DIR" ]] || die "Trusted PostgreSQL migrations directory is invalid."

checksum="${archive}.sha256"
[[ -f "$checksum" && ! -L "$checksum" ]] || die "Checksum file is missing or unsafe."
mapfile -t checksum_lines < "$checksum"
((${#checksum_lines[@]} == 1)) || die "Checksum file must contain exactly one entry."
read -r expected_hash expected_name extra <<< "${checksum_lines[0]}"
expected_name="${expected_name#\*}"
[[ -z "${extra:-}" && "$expected_hash" =~ ^[0-9a-fA-F]{64}$ ]] || die "Invalid checksum file."
[[ "$expected_name" == "$(basename "$archive")" ]] || die "Checksum entry does not name the selected archive."
actual_hash="$(sha256sum "$archive" | awk '{print $1}')"
[[ "${actual_hash,,}" == "${expected_hash,,}" ]] || die "Archive checksum mismatch."
zstd -t "$archive" >/dev/null

top=""
while IFS= read -r path; do
  [[ "$path" != /* && "$path" != ".." && "$path" != ../* && "$path" != */../* && "$path" != */.. ]] ||
    die "Archive contains an unsafe path."
  if [[ -z "$top" ]]; then
    top="${path%%/*}"
    [[ "$top" =~ ^\.bridgeops-postgres-backup-[0-9]{8}T[0-9]{6}Z\.[A-Za-z0-9]+$ && "$path" == "$top/" ]] ||
      die "Archive does not have the expected PostgreSQL backup root."
  fi
  [[ "$path" == "$top/" || "$path" == "$top/"* ]] || die "Archive contains an entry outside its root."
done < <(tar -I zstd -tf "$archive")
[[ -n "$top" ]] || die "Archive is empty."

while IFS= read -r verbose; do
  case "${verbose:0:1}" in
    -|d) ;;
    *) die "Archive contains a link or special file." ;;
  esac
done < <(LC_ALL=C tar -I zstd -tvf "$archive")

read -r member_count expanded_bytes < <(
  LC_ALL=C tar -I zstd -tvf "$archive" |
    awk '{ count++; if (substr($1, 1, 1) == "-") bytes += $3 } END { print count + 0, bytes + 0 }'
)
[[ "$member_count" -le 100000 ]] || die "Archive contains too many entries."
available_bytes="$(( $(df -Pk /tmp | awk 'NR == 2 { print $4 }') * 1024 ))"
required_bytes="$(( expanded_bytes + 268435456 ))"
[[ "$required_bytes" -le "$available_bytes" ]] || die "Not enough free space in /tmp to validate the archive."

work="$(mktemp -d /tmp/bridgeops-postgres-restore.XXXXXX)"
service_started=false
cleanup() {
  local rc=$?
  trap - EXIT
  if [[ "$rc" -ne 0 && "$service_started" == true ]]; then
    systemctl stop "$SERVICE_NAME" >/dev/null 2>&1 || true
  fi
  rm -rf "$work"
  exit "$rc"
}
trap cleanup EXIT

tar -C "$work" -I zstd -xf "$archive" --no-same-owner --no-same-permissions
mapfile -t top_entries < <(find "$work" -mindepth 1 -maxdepth 1 -print)
((${#top_entries[@]} == 1)) && [[ -d "${top_entries[0]}" ]] || die "Unexpected backup layout."
root="${top_entries[0]}"
unsafe_entry="$(find "$root" -mindepth 1 ! -type f ! -type d -print -quit)"
[[ -z "$unsafe_entry" ]] || die "Archive contains an unsupported entry."

[[ -f "$root/SHA256SUMS" ]] || die "Internal checksum manifest is missing."
: > "$work/manifest-paths"
while IFS= read -r manifest_line; do
  [[ "$manifest_line" =~ ^([0-9a-fA-F]{64})[[:space:]][\ \*](\./.*)$ ]] || die "Internal checksum manifest has an invalid entry."
  manifest_path="${BASH_REMATCH[2]}"
  [[ "$manifest_path" != "./SHA256SUMS" && "$manifest_path" != */../* && "$manifest_path" != */.. ]] ||
    die "Internal checksum manifest has an unsafe path."
  printf '%s\n' "$manifest_path" >> "$work/manifest-paths"
done < "$root/SHA256SUMS"
(cd "$root" && find . -type f ! -path './SHA256SUMS' -print | LC_ALL=C sort) > "$work/actual-paths"
LC_ALL=C sort -o "$work/manifest-paths" "$work/manifest-paths"
cmp -s "$work/manifest-paths" "$work/actual-paths" || die "Internal checksum manifest does not cover the exact archive files."
(cd "$root" && sha256sum -c SHA256SUMS >/dev/null) || die "Internal archive checksum mismatch."

for required in \
  config/config.yaml \
  config/postgres.env \
  data/bridgeops.pg_dump \
  data/database-driver \
  data/database-identity.tsv \
  data/key-tables \
  data/pg-restore.list \
  data/schema-columns.tsv \
  data/schema-migrations.tsv \
  data/schema-tables.tsv; do
  [[ -f "$root/$required" ]] || die "PostgreSQL backup is missing a required file."
done
[[ "$(<"$root/data/database-driver")" == postgres ]] || die "Selected archive is not a PostgreSQL backup."
pg_restore --list "$root/data/bridgeops.pg_dump" > "$work/pg-restore.list"
cmp -s "$work/pg-restore.list" "$root/data/pg-restore.list" || die "PostgreSQL dump table of contents does not match the archive manifest."
verify_dump_toc "$work/pg-restore.list"

build_expected_migrations "$work/expected-migrations.tsv"
cmp -s "$work/expected-migrations.tsv" "$root/data/schema-migrations.tsv" ||
  die "Backup migration checksums do not match the trusted checkout."
write_key_tables "$work/key-tables"
cmp -s "$work/key-tables" "$root/data/key-tables" || die "Backup key-table gate is incomplete."
[[ -s "$root/data/schema-tables.tsv" && -s "$root/data/schema-columns.tsv" ]] || die "Backup schema manifests are empty."

source_fields_file="$work/source-fields"
if ! python3 "$CONFIG_HELPER" --config "$root/config/config.yaml" --env "$root/config/postgres.env" > "$source_fields_file"; then
  die "Archived BridgeOps and PostgreSQL settings do not match."
fi
mapfile -d '' -t source_fields < "$source_fields_file"
((${#source_fields[@]} == 7)) || die "PostgreSQL config helper returned an invalid archive result."

SOURCE_DRIVER="$(identity_value driver)"
SOURCE_HOST="$(identity_value host)"
SOURCE_PORT="$(identity_value port)"
SOURCE_USER="$(identity_value user)"
SOURCE_DATABASE="$(identity_value database)"
SOURCE_TIMEZONE="$(identity_value timezone)"
SOURCE_SSLMODE="$(identity_value sslmode)"
SOURCE_SERVER_ADDRESS="$(identity_value server_address)"
SOURCE_SERVER_VERSION="$(identity_value server_version_num)"
[[ "$SOURCE_DRIVER" == "${source_fields[0]}" && "$SOURCE_HOST" == "${source_fields[1]}" &&
   "$SOURCE_PORT" == "${source_fields[2]}" && "$SOURCE_USER" == "${source_fields[3]}" &&
   "$SOURCE_DATABASE" == "${source_fields[4]}" && "$SOURCE_TIMEZONE" == "${source_fields[5]}" &&
   "$SOURCE_SSLMODE" == "${source_fields[6]}" && -n "$SOURCE_SERVER_ADDRESS" &&
   "$SOURCE_SERVER_VERSION" =~ ^[0-9]+$ ]] || die "Archived database identity does not match its config and environment."

if [[ "$VERIFY_ONLY" == true ]]; then
  echo "PostgreSQL backup verification passed. No database was restored."
  exit 0
fi

for command in curl getent pg_isready psql sleep systemctl; do
  command -v "$command" >/dev/null || die "Missing command: $command"
done
[[ "$SERVICE_NAME" =~ ^[A-Za-z0-9_.@:-]+$ ]] || die "Invalid systemd service name."
[[ "$HEALTH_URL" =~ ^http://127\.0\.0\.1(:[0-9]+)?/ ]] || die "Health URL must use loopback HTTP on 127.0.0.1."
require_private_file "$TARGET_CONFIG" "Target BridgeOps config"
require_private_file "$TARGET_ENV" "Target PostgreSQL environment file"
TARGET_CONFIG="$(/usr/bin/readlink -f "$TARGET_CONFIG")"
TARGET_ENV="$(/usr/bin/readlink -f "$TARGET_ENV")"
[[ "$RUNTIME_STATUS_FILE" == /* ]] || die "BridgeOps runtime status path must be absolute."

target_fields_file="$work/target-fields"
if ! python3 "$CONFIG_HELPER" --config "$TARGET_CONFIG" --env "$TARGET_ENV" --connection > "$target_fields_file"; then
  die "Target BridgeOps and PostgreSQL settings did not pass validation."
fi
mapfile -d '' -t target_fields < "$target_fields_file"
rm -f "$target_fields_file"
((${#target_fields[@]} == 9)) || die "PostgreSQL config helper returned an invalid target result."
[[ "${target_fields[0]}" == postgres && "${target_fields[3]}" == "$SOURCE_USER" &&
   "${target_fields[4]}" == "$SOURCE_DATABASE" && "${target_fields[5]}" == UTC ]] ||
  die "Restore target logical user or database does not match the backup."

PGHOST="${target_fields[1]}"
PGPORT="${target_fields[2]}"
PGUSER="${target_fields[3]}"
PGDATABASE="${target_fields[4]}"
PGSSLMODE="${target_fields[6]}"
PGPASSWORD="${target_fields[7]}"
PGPASSFILE="${target_fields[8]}"
export PGHOST PGPORT PGUSER PGDATABASE PGSSLMODE PGCONNECT_TIMEOUT=15 PGAPPNAME=bridgeops-restore-gate
if [[ -n "$PGPASSWORD" ]]; then
  export PGPASSWORD
  unset PGPASSFILE
elif [[ -n "$PGPASSFILE" ]]; then
  require_private_file "$PGPASSFILE" "Target PostgreSQL password file"
  PGPASSFILE="$(/usr/bin/readlink -f "$PGPASSFILE")"
  export PGPASSFILE
else
  die "Target PostgreSQL password input is missing."
fi
PSQL=(psql -X -qAt -F $'\t' -v ON_ERROR_STOP=1 --dbname="$PGDATABASE")

PGCONNECT_TIMEOUT=15 pg_isready -q || die "Target PostgreSQL server is not ready."
if ! "${PSQL[@]}" -c "SELECT current_database(), current_user, COALESCE(inet_server_addr()::text, ''), inet_server_port(), current_setting('TimeZone')" > "$work/target-identity"; then
  die "Cannot query target PostgreSQL identity."
fi
mapfile -t target_identity_rows < "$work/target-identity"
((${#target_identity_rows[@]} == 1)) || die "Target PostgreSQL server returned an invalid identity."
IFS=$'\t' read -r actual_database actual_user server_address server_port server_timezone <<< "${target_identity_rows[0]}"
server_address="${server_address%%/*}"
[[ "$actual_database" == "$PGDATABASE" && "$actual_user" == "$PGUSER" && "$server_port" == "$PGPORT" ]] ||
  die "Connected target PostgreSQL identity does not match the reviewed target."
[[ "$server_timezone" == UTC && -n "$server_address" ]] || die "Target PostgreSQL must use TCP and UTC."
host_matches=false
while read -r resolved_address _; do
  if [[ "$resolved_address" == "$server_address" ]]; then
    host_matches=true
    break
  fi
done < <(getent ahosts "$PGHOST")
[[ "$host_matches" == true ]] || die "Connected target PostgreSQL address does not match database.host."

target_object_count="$("${PSQL[@]}" -c "SELECT (SELECT count(*) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname NOT IN ('pg_catalog', 'information_schema') AND n.nspname !~ '^pg_toast' AND c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f')) + (SELECT count(*) FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname NOT IN ('pg_catalog', 'information_schema') AND n.nspname !~ '^pg_toast') + (SELECT count(*) FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace WHERE n.nspname NOT IN ('pg_catalog', 'information_schema') AND n.nspname !~ '^pg_toast' AND t.typtype IN ('d', 'e')) + (SELECT count(*) FROM pg_catalog.pg_namespace WHERE nspname NOT IN ('public', 'pg_catalog', 'information_schema') AND nspname !~ '^pg_toast' AND nspname !~ '^pg_temp_')")"
[[ "$target_object_count" == 0 ]] || die "Restore target is not empty."
other_sessions="$("${PSQL[@]}" -c "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid()")"
[[ "$other_sessions" == 0 ]] || die "Restore target has another active database session."

bridgeops_validate_postgres_restore_unit || exit 1
[[ -x /usr/local/bin/bridgeops ]] || die "Trusted BridgeOps binary is not installed."
service_state="$(systemctl is-active "$SERVICE_NAME" 2>/dev/null || true)"
case "$service_state" in
  inactive|failed) ;;
  *) die "Target BridgeOps service must already be stopped before restore." ;;
esac

echo "Restoring the verified PostgreSQL snapshot into the empty target..."
pg_restore \
  --exit-on-error \
  --single-transaction \
  --no-owner \
  --no-acl \
  --schema=public \
  --dbname="$PGDATABASE" \
  "$root/data/bridgeops.pg_dump"

if ! "${PSQL[@]}" -c "SELECT version, lower(checksum) FROM public.schema_migrations ORDER BY version" > "$work/restored-migrations.tsv"; then
  die "Restored migration metadata is unreadable."
fi
migration_checksums_match "$work/expected-migrations.tsv" "$work/restored-migrations.tsv" || die "Restored migration checksums are invalid."
restored_timezone="$("${PSQL[@]}" -c "SELECT value FROM public.system_metadata WHERE key = 'database.timeZone'")"
[[ "$restored_timezone" == UTC ]] || die "Restored database.timeZone metadata is not UTC."
capture_schema "$work/restored-schema-tables.tsv" "$work/restored-schema-columns.tsv"
cmp -s "$root/data/schema-tables.tsv" "$work/restored-schema-tables.tsv" || die "Restored PostgreSQL table schema does not match the backup."
cmp -s "$root/data/schema-columns.tsv" "$work/restored-schema-columns.tsv" || die "Restored PostgreSQL column schema does not match the backup."
for table in "${KEY_TABLES[@]}"; do
  "${PSQL[@]}" -c "SELECT count(*) FROM public.\"$table\"" >/dev/null || die "A restored key table is unreadable."
done

service_started=true
systemctl start "$SERVICE_NAME"
healthy=false
for _ in {1..30}; do
  if systemctl is-active --quiet "$SERVICE_NAME"; then
    health_body="$(curl -fsS --max-time 3 "$HEALTH_URL" 2>/dev/null || true)"
    if [[ "$health_body" =~ \"status\"[[:space:]]*:[[:space:]]*\"ok\" ]]; then
      healthy=true
      break
    fi
  fi
  sleep 1
done
[[ "$healthy" == true ]] || die "BridgeOps health gate failed; the restored database was left intact for inspection."
bridgeops_validate_postgres_restored_runtime || die "BridgeOps runtime gate failed after PostgreSQL restore."

service_started=false
echo
echo "BridgeOps PostgreSQL restore completed and passed the health gate."
echo "Health check: $HEALTH_URL"
