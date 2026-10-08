#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"

: "${BRIDGEOPS_LOCAL_PORT:=8418}"
: "${BRIDGEOPS_FRONTEND_PORT:=3010}"
: "${BRIDGEOPS_LOCAL_DATA_DIR:=$ROOT_DIR/data/local-dev}"
: "${APP_SECRET:=local-only-bridgeops-dev-secret}"
: "${GO_BIN:=go}"
: "${DATABASE_HOST:=127.0.0.1}"
: "${DATABASE_PORT:=5432}"
: "${DATABASE_USER:=bridgeops}"
: "${DATABASE_NAME:=bridgeops}"
: "${DATABASE_TIMEZONE:=UTC}"
: "${DATABASE_SSLMODE:=disable}"
: "${DATABASE_PASSWORD:?DATABASE_PASSWORD is required for local development}"

mkdir -p "$BRIDGEOPS_LOCAL_DATA_DIR"
chmod 700 "$BRIDGEOPS_LOCAL_DATA_DIR"
CONFIG_PATH="$BRIDGEOPS_LOCAL_DATA_DIR/config.yaml"

if [ ! -f "$CONFIG_PATH" ]; then
  cp "$ROOT_DIR/config.example.yaml" "$CONFIG_PATH"
fi

if [ ! -d frontend/node_modules ]; then
  pnpm --dir frontend install --frozen-lockfile
fi

cleanup() {
  if [ "${BACKEND_PID:-}" ]; then kill "$BACKEND_PID" 2>/dev/null || true; fi
  if [ "${FRONTEND_PID:-}" ]; then kill "$FRONTEND_PID" 2>/dev/null || true; fi
}
trap cleanup INT TERM EXIT

SERVER_PORT="$BRIDGEOPS_LOCAL_PORT" \
SERVER_HOST=127.0.0.1 \
DATABASE_DRIVER=postgres \
DATABASE_HOST="$DATABASE_HOST" \
DATABASE_PORT="$DATABASE_PORT" \
DATABASE_USER="$DATABASE_USER" \
DATABASE_PASSWORD="$DATABASE_PASSWORD" \
DATABASE_NAME="$DATABASE_NAME" \
DATABASE_TIMEZONE="$DATABASE_TIMEZONE" \
DATABASE_SSLMODE="$DATABASE_SSLMODE" \
APP_SECRET="$APP_SECRET" \
AUTH_ENABLED="${AUTH_ENABLED:-false}" \
"$GO_BIN" run ./cmd/server -config "$CONFIG_PATH" &
BACKEND_PID=$!

VITE_BACKEND_URL="http://127.0.0.1:$BRIDGEOPS_LOCAL_PORT" \
  pnpm --dir frontend dev --host 127.0.0.1 --port "$BRIDGEOPS_FRONTEND_PORT" &
FRONTEND_PID=$!

printf '%s\n' "BridgeOps backend: http://127.0.0.1:$BRIDGEOPS_LOCAL_PORT"
printf '%s\n' "BridgeOps frontend: http://127.0.0.1:$BRIDGEOPS_FRONTEND_PORT"
wait "$BACKEND_PID"
