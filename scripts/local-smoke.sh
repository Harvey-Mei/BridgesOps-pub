#!/bin/sh
set -eu

: "${SMOKE_BASE_URL:=http://127.0.0.1:8418}"
: "${SMOKE_AUTH_ENABLED:=true}"
: "${SMOKE_ADMIN_USERNAME:=local-admin}"
: "${SMOKE_ADMIN_PASSWORD:=local-admin-password}"

TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/bridgeops-smoke.XXXXXX")
trap 'rm -rf "$TMP_DIR"' EXIT
COOKIE_JAR="$TMP_DIR/cookies"

request() {
  expected="$1"
  shift
  actual=$(curl -sS -o "$TMP_DIR/response" -w '%{http_code}' "$@")
  if [ "$actual" != "$expected" ]; then
    printf '%s\n' "request failed: expected HTTP $expected, got $actual" >&2
    sed -n '1,20p' "$TMP_DIR/response" >&2
    exit 1
  fi
}

request 200 "$SMOKE_BASE_URL/healthz"

if [ "$SMOKE_AUTH_ENABLED" = "true" ]; then
  request 302 "$SMOKE_BASE_URL/"
  request 200 "$SMOKE_BASE_URL/login"
  request 401 "$SMOKE_BASE_URL/api/version"
  request 401 "$SMOKE_BASE_URL/api/channels"
  request 401 "$SMOKE_BASE_URL/api/settings/config"
  request 200 -H 'Content-Type: application/json' \
    -c "$COOKIE_JAR" \
    --data "{\"username\":\"$SMOKE_ADMIN_USERNAME\",\"password\":\"$SMOKE_ADMIN_PASSWORD\"}" \
    "$SMOKE_BASE_URL/api/auth/login"
  if grep -Eq '"token"[[:space:]]*:' "$TMP_DIR/response"; then
    printf '%s\n' 'smoke test found a bearer token in the login response' >&2
    exit 1
  fi
  if ! grep -Fq 'uh_token' "$COOKIE_JAR"; then
    printf '%s\n' 'smoke test could not persist the login session cookie' >&2
    exit 1
  fi
  request 200 -b "$COOKIE_JAR" "$SMOKE_BASE_URL/"
  request 200 -b "$COOKIE_JAR" "$SMOKE_BASE_URL/api/version"
  request 200 -b "$COOKIE_JAR" "$SMOKE_BASE_URL/api/channels"
  request 200 -b "$COOKIE_JAR" "$SMOKE_BASE_URL/api/gateway/candidate-groups"
  request 200 -b "$COOKIE_JAR" "$SMOKE_BASE_URL/api/settings/config"
  for secret in "$SMOKE_ADMIN_PASSWORD" local-only-bridgeops-test-secret local-only-auth-token-secret; do
    if grep -Fq "$secret" "$TMP_DIR/response"; then
      printf '%s\n' "smoke test found a returned secret: $secret" >&2
      exit 1
    fi
  done
else
  request 200 "$SMOKE_BASE_URL/api/version"
  request 200 "$SMOKE_BASE_URL/"
  request 200 "$SMOKE_BASE_URL/api/channels"
  request 200 "$SMOKE_BASE_URL/api/gateway/candidate-groups"
fi

printf '%s\n' "BridgeOps smoke test passed: $SMOKE_BASE_URL"
