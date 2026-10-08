#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"

usage() {
	printf 'Usage: %s ABSOLUTE_OUTPUT_PATH\n' "$0" >&2
	exit 2
}

fail() {
	printf 'deployment artifact build failed: %s\n' "$*" >&2
	exit 1
}

require_command() {
	command -v "$1" >/dev/null 2>&1 || fail "required command is unavailable: $1"
}

[ "$#" -eq 1 ] || usage
OUTPUT=$1
case "$OUTPUT" in
	/*) ;;
	*) fail "output path must be absolute" ;;
esac

OUTPUT_DIR=$(dirname -- "$OUTPUT")
[ -d "$OUTPUT_DIR" ] || fail "output directory does not exist: $OUTPUT_DIR"
OUTPUT_DIR=$(CDPATH= cd -- "$OUTPUT_DIR" && pwd)
OUTPUT="$OUTPUT_DIR/$(basename -- "$OUTPUT")"
case "$OUTPUT" in
	"$ROOT_DIR" | "$ROOT_DIR"/*) fail "output path must be outside the Git worktree" ;;
esac

: "${GO_BIN:=go}"
: "${NODE_OPTIONS:=--max-old-space-size=1024}"
: "${GOMAXPROCS:=2}"
export NODE_OPTIONS GOMAXPROCS

require_command git
require_command rsync
require_command pnpm
require_command node
require_command strings
require_command mktemp
command -v "$GO_BIN" >/dev/null 2>&1 || fail "Go executable is unavailable: $GO_BIN"

[ "$(git rev-parse --show-toplevel)" = "$ROOT_DIR" ] || fail "script is not running in its Git worktree"
HEAD_REVISION=$(git rev-parse --verify HEAD) || fail "Git HEAD is unavailable"
[ -z "$(git status --porcelain=v1 --untracked-files=all)" ] || fail "Git worktree must be clean and committed"

BACKEND_VERSION=$(sed -n 's/^const VERSION = "\([^"]*\)"$/\1/p' backend/global/version.go)
FRONTEND_VERSION=$(node -p "require('./frontend/package.json').version")
[ -n "$BACKEND_VERSION" ] || fail "backend version is unavailable"
[ "$BACKEND_VERSION" = "$FRONTEND_VERSION" ] || fail "backend and frontend versions do not match"

pnpm --dir frontend build
[ -s frontend/dist/index.html ] || fail "frontend build did not create frontend/dist/index.html"

rsync -a --delete --exclude=.gitkeep frontend/dist/ web/dist/
[ -s web/dist/index.html ] || fail "web/dist/index.html is missing after synchronization"
diff -qr -x .gitkeep frontend/dist web/dist >/dev/null || fail "frontend/dist and web/dist differ"

"$GO_BIN" test -p=1 -tags=release ./web

TEMP_OUTPUT=$(mktemp "$OUTPUT_DIR/.bridgeops-artifact.XXXXXX")
BUILD_DIR=$(mktemp -d)
cleanup() {
	rm -f -- "$TEMP_OUTPUT"
	rm -rf -- "$BUILD_DIR"
}
trap cleanup EXIT HUP INT TERM

git clone --quiet --no-checkout "$ROOT_DIR" "$BUILD_DIR"
git -C "$BUILD_DIR" checkout --quiet --detach "$HEAD_REVISION"
rsync -a --delete --exclude=.gitkeep web/dist/ "$BUILD_DIR/web/dist/"
[ -z "$(git -C "$BUILD_DIR" status --porcelain=v1 --untracked-files=all)" ] || fail "isolated build checkout is dirty"
(
	cd "$BUILD_DIR"
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 "$GO_BIN" build \
		-trimpath -buildvcs=true -o "$TEMP_OUTPUT" ./cmd/server
)

if ! "$GO_BIN" run ./scripts/internal/inspect-deployment-artifact "$TEMP_OUTPUT" "$HEAD_REVISION"; then
	fail "artifact ELF or Go metadata validation failed"
fi

ASSET_PATH=$(node -e '
const fs = require("fs");
const index = fs.readFileSync("web/dist/index.html", "utf8");
const match = index.match(/(?:src|href)=["\x27]\/(assets\/[^"\x27]+)["\x27]/);
if (match) process.stdout.write(match[1]);
')
[ -n "$ASSET_PATH" ] || fail "frontend index does not reference a built asset"
strings -a "$TEMP_OUTPUT" | grep -F -q 'dist/index.html' || fail "artifact does not contain embedded frontend index"
strings -a "$TEMP_OUTPUT" | grep -F -q "dist/$ASSET_PATH" || fail "artifact does not contain referenced frontend asset: $ASSET_PATH"

mv -f -- "$TEMP_OUTPUT" "$OUTPUT"
rm -rf -- "$BUILD_DIR"
trap - EXIT HUP INT TERM
printf 'Built deployment artifact: %s\n' "$OUTPUT"
printf 'Source commit: %s\n' "$HEAD_REVISION"
printf 'Version: %s\n' "$BACKEND_VERSION"
"$GO_BIN" version -m "$OUTPUT"
