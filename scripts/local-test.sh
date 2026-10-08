#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"

: "${GO_BIN:=go}"
: "${PYTHON_BIN:=python3}"
: "${GOMAXPROCS:=2}"
export GOMAXPROCS

run_pnpm() {
  if command -v pnpm >/dev/null 2>&1; then
    pnpm "$@"
    return
  fi
  if command -v corepack >/dev/null 2>&1; then
    corepack pnpm "$@"
    return
  fi
  echo 'Neither pnpm nor corepack is available.' >&2
  return 127
}

PYTHONDONTWRITEBYTECODE=1 "$PYTHON_BIN" scripts/run-python-tests.py
# Avoid traversing private Docker volumes and local archives under ignored data/.
"$GO_BIN" test -p=2 ./backend/... ./cmd/... ./modelcatalogdata/... ./scripts/internal/... ./web/... -count=1
"$GO_BIN" test -race -p=2 ./backend/storage ./backend/gateway ./backend/api
run_pnpm --dir frontend lint
run_pnpm --dir frontend build
git diff --check
docker compose -f compose.local.yml config >/dev/null
printf '%s\n' 'Local checks passed.'
