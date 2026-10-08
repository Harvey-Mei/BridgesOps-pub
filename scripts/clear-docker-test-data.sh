#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
TARGET_DIR="$ROOT_DIR/data/docker-test"

if [ "${BRIDGEOPS_CONFIRM_DATA_DELETE:-}" != "YES" ]; then
  printf '%s\n' 'Refusing to delete Docker test data.' >&2
  printf '%s\n' 'To remove only data/docker-test, stop the stack and run:' >&2
  printf '%s\n' '  BRIDGEOPS_CONFIRM_DATA_DELETE=YES ./scripts/clear-docker-test-data.sh' >&2
  exit 2
fi

if [ -d "$TARGET_DIR" ]; then
  rm -rf -- "$TARGET_DIR"
  printf '%s\n' "Removed only $TARGET_DIR"
else
  printf '%s\n' "No Docker test data directory: $TARGET_DIR"
fi
