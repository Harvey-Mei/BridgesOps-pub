# Local Development and Docker Verification

## Requirements

- Go matching the root `go.mod`.
- Node.js 20 and pnpm 10.4.
- PostgreSQL 16 or newer.
- Python 3 with PyYAML for backup-gate tests.
- Docker Compose v2 for container verification.

PostgreSQL is the only supported database. Use a dedicated development database
and never point local tests at production. Redis is optional and may be deleted
at any time.

## Native Development

Export a local PostgreSQL connection before starting the backend:

```bash
export DATABASE_DRIVER=postgres
export DATABASE_HOST=127.0.0.1
export DATABASE_PORT=5432
export DATABASE_USER=bridgeops
export DATABASE_PASSWORD=replace-me
export DATABASE_NAME=bridgeops_dev
export DATABASE_TIMEZONE=UTC
export DATABASE_SSLMODE=disable
export APP_SECRET=local-only-bridgeops-dev-secret
./scripts/local-dev.sh
```

The backend listens on `127.0.0.1:8418` and Vite on `127.0.0.1:3010` by
default. Stop both with Ctrl-C.

## PostgreSQL Tests

Backend integration tests require `BRIDGEOPS_TEST_POSTGRES_DSN` to point to a
dedicated database named exactly `bridgeops_test_admin`. Tests only create and
drop databases prefixed `bridgeops_test_` and clone a migration-fingerprinted
template, so they cannot use the production database accidentally.

```bash
export BRIDGEOPS_TEST_POSTGRES_DSN='postgres://bridgeops_test:password@127.0.0.1:55432/bridgeops_test_admin?sslmode=disable'
go test -p=2 ./backend/... ./cmd/... ./modelcatalogdata/... ./scripts/internal/... ./web/... -count=1
go test -race -p=2 ./backend/storage ./backend/gateway ./backend/api
```

Run the complete repository checks with:

```bash
GO_BIN=/path/to/go ./scripts/local-test.sh
```

This includes Python backup tests, Go tests, frontend lint and
build, diff checks, and Compose validation.

## Docker Verification

`compose.local.yml` starts an isolated PostgreSQL database, disposable Redis,
and the locally built application. Its persistent test database stays under the
Git-ignored `data/docker-test/postgres/` directory.

```bash
./scripts/docker-test.sh
docker compose -f compose.local.yml ps
docker compose -f compose.local.yml logs --tail=100 app
```

The verification URL is `http://127.0.0.1:18418`. Set `BRIDGEOPS_BIND_HOST`
to expose it on another interface. Default test credentials are
`local-admin` / `local-admin-password`. Remove the test data with
`BRIDGEOPS_CONFIRM_DATA_DELETE=YES ./scripts/clear-docker-test-data.sh` after
stopping the stack.

## Frontend

```bash
pnpm --dir frontend install --frozen-lockfile
pnpm --dir frontend lint
pnpm --dir frontend build
rsync -a --delete --exclude=.gitkeep frontend/dist/ web/dist/
diff -qr --exclude=.gitkeep frontend/dist web/dist
```

When a frontend change affects layout, verify desktop and mobile screenshots.
When a gateway change affects routing, add focused backend regression tests.

## Gateway Routing Regression Matrix

Treat smart and fixed routing as separate execution modes in tests:

- Smart routing owns candidate scoring, coalesced waiting-health inference,
  same-candidate response-level transient confirmation, bounded pre-output
  fallback, routing failure/cooldown, manual snowflake cooldown, and session
  affinity.
- The exact unmarked Responses text `Our servers are currently overloaded. Please
  try again later.` is a narrow pre-acceptance retry exception: buffer its
  standard closing frames, retry only before trusted acceptance and useful
  output, and treat any continuation as ordinary model output. Persist a trusted
  lifecycle immediately, independently from downstream buffering. Capacity,
  malformed frames, EOF and read errors cannot undo that no-replay fence.
  Usage Records retain the physical retry count for administrators.
- Fixed routing sends exactly one request to the configured channel/group. A
  slow first byte must not be stopped by the smart-routing timeout, and a
  transient failure must not retry, fall back, or mutate smart-routing health.
- Legacy scheduler/channel/group recovery-probe flags are compatibility fields and must
  not block current-generation cooldown recovery, candidate-group membership,
  or fixed selection. Healthy groups must never be selected for scheduled active
  probing; real requests contribute latency samples passively.
- Recovery-probe tests must require an exact current presence generation and
  `CoolingDown=true`, and select the request model from that group's synchronized
  model IDs rather than an independently configured probe model.
- Frontend tests must keep manual `route_reason` values out of the snowflake
  action and use an explicit allowlist for known smart-routing outcomes.
- Accepted Responses streams must never be replayed. A transient terminal,
  malformed JSON frame, `stream_read_error`, EOF/read failure, or upstream-side
  cancellation may be converted to a normal completed retry prompt only while
  the published lifecycle is one uniquely reconstructable, still-open assistant
  `output_text` part. Preserve only normally emitted text in done/completed
  snapshots; withheld capacity/error text must never enter the synthetic answer. Persisted acceptance failures, caller cancellation and waiting-probe timeouts remain distinct terminal paths. Also
  keep one sanitized 502 audit record with `transient_after_acceptance`; tool,
  reasoning, ambiguous/closed, owner-fenced, and client-cancelled streams must
  fail closed.
- Price-resolution tests must prefer the exact channel/canonical-group/model
  catalog entry over global prices. Channel catalog components already encode
  model input/output/cache differences, so multiply every present component by
  the same channel-group rate exactly once and never apply the completion rate
  again. When channel pricing is unavailable, verify the 24-hour LiteLLM global
  sync and manual-page fallback; global base prices retain separate input and
  completion-rate semantics. Cover nullable cache-read/write prices: zero usage
  remains calculable, while positive usage without the corresponding price is
  `price_missing`, never a zero-cost component.
- Usage-record cost comparisons are assembled server-side. Prefer a valid
  normalized `reported`/`reconciled` upstream amount; otherwise show the value
  evaluated from the request's frozen USD price fact and explicit usage
  presence bits. If neither is available and the request has no frozen price
  fact, the frontend may visibly label a display-only estimate from the
  request's frozen ratios and token counts using `500000 quota/USD`; it must
  not write that estimate back to authoritative cost
  fields or aggregates. Never apply `funding_factor` again to frozen ratios or
  unit prices. Flag a difference when its ratio to the calculated value is
  strictly over 5%, without an additional absolute-dollar threshold;
  whitelist unavailable-reason output before it reaches the browser.
- Managed remote-key recovery must use the current canonical creation
  fingerprint. A legacy pending fingerprint may rotate only after two complete,
  strict, empty inventories at least ten minutes apart under unchanged account
  identity and upstream-group presence-generation fences. Archive the old
  operation generation atomically, permit at most one create POST after a
  successful rotation, and reject conflicting IDs or identity fields in create
  responses.

## Production Boundary

Local development must not read production config, secrets, PostgreSQL DSNs, or
archives. Production backups follow [recovery.md](recovery.md).

After completing tests, commit all source changes and require a clean worktree.
Build a deployable binary only through the repository gate, using an absolute
output path:

```bash
./scripts/build-deployment-artifact.sh /absolute/path/to/bridgeops-linux-amd64
```

This command serially builds and synchronizes the production frontend, runs the
release-only embedded-asset test, builds a static Linux/amd64 ELF with VCS
metadata, and verifies that the final binary contains the UI. Never substitute
a direct `go build ./cmd/server` or build from a clone whose ignored `web/dist`
has not been synchronized. Such a binary can pass `/healthz` while serving no
Web UI.

Response-model audit regression checks must cover JSON, SSE lifecycle frames, long streams after tail-buffer eviction, bounded large-response projection, credential echoes, persistence and both list projections. Missing model evidence stays unknown. Missing/null billing amounts are not explicit zero; distinct cache-write TTL prices remain unavailable for local estimation until usage can distinguish their components.

Smart text waiting checks start after 30 seconds and never overlap for the same frozen target. The two-minute network deadline starts at dispatch, separately from a five-second local probe preparation limit. Local admission or writer saturation must skip the probe without declaring provider failure. Internal cost evidence writers have four reserved slots and a two-second database context. Waiting checks do not contribute latency samples or recover shared routing health. Keep accepted-no-replay, metadata-only SSE, fixed routing, image dispatch and local response-spool admission regressions.

### Pelican model-check acceptance

Run the `backend/modelcheck`, `backend/api`, `backend/gateway`, and `backend/storage` model-check regressions with the dedicated PostgreSQL test DSN. Verify randomized candy answers against enumerated draws, full SVG preservation, hostile XML/CSS rejection, and a global twelve-result window across multiple targets without resetting the 24-hour request ledger. The UI uses image-context SVG previews; inspect twelve mocked results at 1920×1080 and 2560×1440, pause/resume and detail dialogs, and ensure the initial page sends no paid probe requests. Keep browser checks on the developer device.

For manual gallery selection, verify that a model returned live by the selected group can be selected, its channel/group/model/protocol are frozen in the result, and the browser restores the exact selection without automatically choosing replacements. Test stale account revisions, disabled targets, duplicate clicks, operator/discovery changes during a run, and queued runs using the latest baseline. Manual-only model choices must not expand automatic target discovery.

Also verify live model-list failure and in-place retry, without falling back to another group or cached unsupported names. Exercise streamed progress, folding without cancellation, truncated connections without replay, and the separate 600-second drawing / 60-second candy deadlines. Each attempted inference must produce one visible usage record (including HTTP failures) while model metadata reads produce none. Check token/cache metrics, unknown initial prices, strict reconciliation with account/cost-unit fences, transient identity-query retry before and after billing lookup, and exclusion from latency samples. Browser acceptance uses mocked inference responses; it must not spend real upstream quota.

For the channel funding workbench, verify oldest-first pending pagination (including late detection and timestamp ties), pending/manual mutual exclusion, historical funding after balance consumption, read/write exclusion during reload, stale-version recovery, failed refresh after successful calibration, and uncertain funding submissions without replay. Mock all financial mutations in browser acceptance. Channel actions remain directly visible at 1280/1536/1600/1920/2560 widths; test serialized busy actions and cancel destructive confirmation without sending requests.

Release regression checks also cover request-ID credential echoes, pre-dispatch fence rejection without usage entries, waiting-probe account/unit changes before and during cost lookup, whitelisted detection-kind API labels, and clipboard fallback inside modal focus traps.

When Docker data already exists in the checkout, select the tracked Go package roots explicitly so `go test ./...` does not traverse private PostgreSQL volume files or archived worktrees. Temporary test database cleanup uses ordinary DROP with a bounded wait, preserving the non-superuser test account; it does not force-terminate privileged maintenance workers.
