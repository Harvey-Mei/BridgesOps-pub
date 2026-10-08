# Gateway request memory admission

BridgeOps reserves request-body memory after downstream-key authentication and
before reading the body. The lease remains held until the complete downstream
request ends, including a long streaming response. Limits are process-local;
they are unrelated to provider account concurrency or routing health.

Configure the controller under `gateway.requestAdmission`:

```yaml
gateway:
  requestAdmission:
    maxInFlightRequests: 16
    maxInFlightBodyBytes: 268435456
    maxInFlightRequestsPerKey: 4
    maxInFlightBodyBytesPerKey: 100663296
    maxQueuedRequests: 32
    maxQueuedRequestsPerKey: 8
    maxQueueWaitMilliseconds: 1000
```

The byte fields are weighted peak-memory reservations rather than raw request
sizes. A standard body currently reserves `4 * retained bytes + 64 KiB`; a
request that may rewrite the model payload reserves `8 * retained bytes + 256
KiB`. A known `Content-Length` reserves its complete weighted amount before the
body is read. An unknown or chunked body starts with only the profile's fixed
overhead and calls `GrowTo` before retaining each additional chunk. The final
grown reservation is released exactly once when the request ends.

The raw path limit and `http.MaxBytesReader` remain an independent 64 MiB wire
ceiling. A request can therefore hit either the raw ceiling or a lower weighted
global/per-key ceiling. Dynamic growth lets a small chunked request proceed; it
does not promise that every request up to the raw ceiling fits the configured
memory budget.

## Failure semantics

- A known raw length or weighted target that can never fit its configured
  global/per-key ceiling is a permanent size failure: HTTP `413` without
  `Retry-After`.
- A chunked request that grows beyond a permanent raw or weighted ceiling also
  stops promptly with HTTP `413` and no `Retry-After`.
- Capacity currently occupied by other live requests, or a full/disabled
  admission queue, is transient pressure: HTTP `503` with `Retry-After: 1`.
- A request that remains queued beyond `maxQueueWaitMilliseconds` is also
  transient pressure: HTTP `503` with `Retry-After: 1`. It does not alter the
  caller context or become a `499` cancellation.

The four in-flight limits must be positive. Per-key values cannot exceed their
global counterparts. Queue limits may be zero to reject immediately, cannot be
negative, and the per-key queue cannot exceed the global queue. Omitting queue
fields uses the defaults; explicitly setting both queue limits to zero disables
waiting. `maxQueueWaitMilliseconds` defaults to `1000` only when omitted and
must be an explicit integer from `1` through `60000` when present; `0` is
rejected rather than silently converted to the default. Queue wait begins at
the enqueue boundary. Canceled or expired waiters are removed before FIFO
capacity allocation, while a lease granted before its deadline remains valid
even if its goroutine is scheduled later.

Admission settings are startup-only. Runtime settings apply may continue when
the admission values are unchanged, but a changed
`gateway.requestAdmission` is rejected with HTTP `409`, code
`restart_required`, and the affected section. The running process never
installs a second controller or silently claims that new limits took effect.

## Startup memory validation

Startup computes one effective memory limit from the smallest finite value
available from:

- the current Go soft memory target reported by `debug.SetMemoryLimit(-1)`;
- the current cgroup v2 `memory.max`, including tighter ancestor boundaries;
- host memory (`MemTotal` from `/proc/meminfo` on Linux or `hw.memsize` on
  Darwin).

An absent or unlimited source is ignored. A read/parse error fails startup, as
does the absence of every finite source. Other platforms must supply a finite
Go memory target when they have no platform host-memory detector.

The global weighted request-body budget may use at most one third of the
effective limit. In-flight request slots are conservatively charged 2 MiB each
and queued slots 256 KiB each; their combined reservation may use at most one
sixth. Even when both budgets are full, at least half of the effective limit is
left for response buffering, protocol parsing, database work, goroutine stacks,
and the rest of the service. The 256 MiB default is exactly one third of a 768
MiB effective limit. Structural maxima such as 4 GiB of weighted bodies, 1024
in-flight requests, or 8192 queued requests are not deployment-safe defaults
and are rejected when they exceed the detected budget.

This startup validation does not set an operating-system limit. The versioned
`packaging/server/bridgeops.service` template supplies these deployment
boundaries for a future installed unit:

```ini
Environment=GOMEMLIMIT=768MiB
MemoryHigh=1G
MemoryMax=1280M
```

`GOMEMLIMIT` is a soft Go garbage-collector target, not an out-of-memory guard.
`MemoryHigh` asks cgroup/systemd to reclaim and throttle above the high-water
mark. `MemoryMax` is the hard cgroup boundary. Editing the repository template
does not change an already installed live unit; deployment must update and
verify that unit separately. Because systemd `EnvironmentFile` values override
`Environment`, deployment validation must also reject `GOMEMLIMIT` in the
database-only `/etc/bridgeops/postgres.env` file and verify the active process
environment. Do not infer that the current production process already has
these values.

## Deliberate remaining boundary

Request bodies are still retained in memory and there is no cleanup-safe,
replayable disk spool. Large multimodal payloads therefore remain subject to
the raw and weighted limits instead of being silently under-accounted. Adding
a spool later requires bounded disk admission, cancellation-safe cleanup, and
reserving any decoded or rewritten in-memory copy before allocation.
