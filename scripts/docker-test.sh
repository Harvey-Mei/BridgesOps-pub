#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"

COMPOSE="docker compose -f compose.local.yml"
BASE_URL="${BRIDGEOPS_TEST_BASE_URL:-http://${BRIDGEOPS_BIND_HOST:-127.0.0.1}:${BRIDGEOPS_TEST_PORT:-18418}}"
TEST_DATA_DIR="$ROOT_DIR/data/docker-test"

mkdir -p "$TEST_DATA_DIR"
chmod 700 "$TEST_DATA_DIR"
if [ ! -f "$TEST_DATA_DIR/config.yaml" ]; then
  cp "$ROOT_DIR/config.example.yaml" "$TEST_DATA_DIR/config.yaml"
fi

$COMPOSE config >/dev/null
$COMPOSE up -d --build
trap '$COMPOSE logs --tail=80 app >&2' EXIT

attempt=0
while [ "$attempt" -lt 30 ]; do
  if curl -fsS "$BASE_URL/healthz" >/dev/null 2>&1; then
    break
  fi
  attempt=$((attempt + 1))
  sleep 2
done
if [ "$attempt" -ge 30 ]; then
  printf '%s\n' 'Docker service did not become healthy.' >&2
  $COMPOSE ps
  exit 1
fi

$COMPOSE ps
SMOKE_BASE_URL="$BASE_URL" \
SMOKE_AUTH_ENABLED="${BRIDGEOPS_TEST_AUTH_ENABLED:-true}" \
SMOKE_ADMIN_USERNAME="${BRIDGEOPS_TEST_ADMIN_USERNAME:-local-admin}" \
SMOKE_ADMIN_PASSWORD="${BRIDGEOPS_TEST_ADMIN_PASSWORD:-local-admin-password}" \
  "$ROOT_DIR/scripts/local-smoke.sh"
printf '%s\n' 'Restarting the test service to verify persisted config and migrations.'
$COMPOSE restart app >/dev/null
attempt=0
while [ "$attempt" -lt 30 ]; do
  if curl -fsS "$BASE_URL/healthz" >/dev/null 2>&1; then
    break
  fi
  attempt=$((attempt + 1))
  sleep 2
done
if [ "$attempt" -ge 30 ]; then
  printf '%s\n' 'Docker service did not recover after restart.' >&2
  $COMPOSE ps
  exit 1
fi
SMOKE_BASE_URL="$BASE_URL" \
SMOKE_AUTH_ENABLED="${BRIDGEOPS_TEST_AUTH_ENABLED:-true}" \
SMOKE_ADMIN_USERNAME="${BRIDGEOPS_TEST_ADMIN_USERNAME:-local-admin}" \
SMOKE_ADMIN_PASSWORD="${BRIDGEOPS_TEST_ADMIN_PASSWORD:-local-admin-password}" \
  "$ROOT_DIR/scripts/local-smoke.sh"
printf '%s\n' 'Docker checks passed. The test stack is still running; stop it with: docker compose -f compose.local.yml down'
