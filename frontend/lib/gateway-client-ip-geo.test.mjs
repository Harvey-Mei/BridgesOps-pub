import assert from "node:assert/strict"
import test from "node:test"

import { createServer } from "vite"

async function withGeoModule(run) {
  const server = await createServer({
    appType: "custom",
    logLevel: "silent",
    server: { middlewareMode: true },
  })
  try {
    return await run(await server.ssrLoadModule("/lib/gateway-client-ip-geo.ts"))
  } finally {
    await server.close()
  }
}

function deferred() {
  let resolve
  const promise = new Promise((next) => {
    resolve = next
  })
  return { promise, resolve }
}

function successEntry(label, fetchedAt) {
  return {
    status: "success",
    label,
    detail: {},
    precision: "country",
    networkKind: "ordinary",
    fetchedAt,
  }
}

function createFakeClock(start = 0) {
  let now = start
  let nextTimerID = 1
  const timers = new Map()

  function nextTimerBefore(target) {
    let next
    for (const [id, timer] of timers) {
      if (timer.at > target || (next && timer.at >= next.timer.at)) continue
      next = { id, timer }
    }
    return next
  }

  return {
    now: () => now,
    setTimeout(callback, delayMs) {
      const id = nextTimerID++
      timers.set(id, { at: now + Math.max(0, Number(delayMs) || 0), callback })
      return id
    },
    clearTimeout(id) {
      timers.delete(id)
    },
    advanceBy(durationMs) {
      const target = now + durationMs
      for (let next = nextTimerBefore(target); next; next = nextTimerBefore(target)) {
        now = next.timer.at
        timers.delete(next.id)
        next.timer.callback()
      }
      now = target
    },
    pendingCount() {
      return timers.size
    },
  }
}

test("local MaxMind data uses a compact city, region, and country-code label", async () => {
  await withGeoModule(({ gatewayClientIpGeoPresentation, parseGatewayClientIpGeoResponse }) => {
    const entry = parseGatewayClientIpGeoResponse({
      ip: "8.8.8.8",
	  provider: "local_maxmind",
      country: "United States",
      country_code: "US",
      region: "California",
      city: "Los Angeles",
      latitude: 37.4056,
      longitude: -122.0775,
	  accuracy_radius_km: 20,
	  asn: "AS64500",
	  organization: "Example ISP",
    }, "8.8.8.8")
    assert.equal(entry.status, "success")
    if (entry.status !== "success") return

    const presentation = gatewayClientIpGeoPresentation(entry, "trusted_proxy")
    assert.equal(presentation?.sourceLabel, "可信代理转发")
    assert.equal(presentation?.precisionLabel, "城市级估算")
    assert.equal(presentation?.city, "Los Angeles")
    assert.equal(presentation?.showCoordinates, true)
    assert.equal(presentation?.label, "Los Angeles · California · US")
  })
})

test("known CDN or proxy network never presents its city as the client location", async () => {
  await withGeoModule(({ gatewayClientIpGeoPresentation, parseGatewayClientIpGeoResponse }) => {
    const entry = parseGatewayClientIpGeoResponse({
      ip: "1.1.1.1",
      country: "Australia",
      country_code: "AU",
      region: "Queensland",
      city: "Brisbane",
      latitude: -27.47,
      longitude: 153.02,
	  accuracy_radius_km: 5,
	  asn: "AS13335",
	  organization: "Cloudflare, Inc.",
    }, "1.1.1.1")
    assert.equal(entry.status, "success")
    if (entry.status !== "success") return

    const presentation = gatewayClientIpGeoPresentation(entry, "direct")
    assert.equal(entry.precision, "network_exit")
    assert.equal(presentation?.precisionLabel, "CDN / 代理出口")
    assert.equal(presentation?.city, undefined)
    assert.equal(presentation?.showCoordinates, false)
    assert.equal(presentation?.label, "Queensland · AU")
  })
})

test("a wide city accuracy radius is downgraded to country or region precision", async () => {
  await withGeoModule(({ gatewayClientIpGeoPresentation, parseGatewayClientIpGeoResponse }) => {
    const entry = parseGatewayClientIpGeoResponse({
      ip: "9.9.9.9",
      country: "United States",
      country_code: "US",
      region: "California",
	  city: "Los Angeles",
	  accuracy_radius_km: 200,
	  asn: "AS19281",
	  organization: "Example ISP",
    }, "9.9.9.9")
    assert.equal(entry.status, "success")
    if (entry.status !== "success") return

    const presentation = gatewayClientIpGeoPresentation(entry)
    assert.equal(entry.precision, "region")
    assert.equal(presentation?.precisionLabel, "仅国家 / 地区")
    assert.equal(presentation?.city, undefined)
    assert.equal(presentation?.showPostal, false)
    assert.equal(presentation?.label, "California · US")
  })
})

test("city data without ASN verification is not treated as a client location", async () => {
  await withGeoModule(({ gatewayClientIpGeoPresentation, parseGatewayClientIpGeoResponse }) => {
    const entry = parseGatewayClientIpGeoResponse({
      ip: "9.9.9.9",
      country: "United States",
      region: "California",
      city: "Los Angeles",
      accuracy_radius_km: 10,
    }, "9.9.9.9")
    assert.equal(entry.status, "success")
    if (entry.status !== "success") return

    const presentation = gatewayClientIpGeoPresentation(entry)
    assert.equal(entry.precision, "region")
    assert.equal(presentation?.city, undefined)
    assert.match(presentation?.precisionHint ?? "", /ASN/)
  })
})

test("a missing local database is shown as unavailable instead of falling back to a public lookup", async () => {
  await withGeoModule(({ gatewayClientIpGeoEntryFromError }) => {
    assert.equal(gatewayClientIpGeoEntryFromError({ status: 503 }).status, "unavailable")
  })
})

test("a response for another address is rejected instead of being cached", async () => {
  await withGeoModule(({ parseGatewayClientIpGeoResponse }) => {
	const entry = parseGatewayClientIpGeoResponse({ ip: "1.1.1.1", country: "Australia" }, "8.8.8.8")
    assert.equal(entry.status, "error")
  })
})

test("the same normalized IP in different request rows shares one cache key", async () => {
  await withGeoModule(({ gatewayClientIpGeoCacheKey }) => {
    assert.equal(gatewayClientIpGeoCacheKey(" 8.8.8.8 "), gatewayClientIpGeoCacheKey("8.8.8.8"))
    assert.equal(gatewayClientIpGeoCacheKey("2001:4860:4860:0:0:0:0:8888"), gatewayClientIpGeoCacheKey("2001:4860:4860::8888"))
  })
})

test("two GatewayClientIpCell subscriptions share one in-flight lookup and receive the same result", async () => {
  await withGeoModule(async ({ createGatewayClientIpGeoStore, parseGatewayClientIpGeoResponse }) => {
    const calls = []
    let finishLookup
    const store = createGatewayClientIpGeoStore((requestLogID, normalizedIP, signal) => {
      calls.push({ requestLogID, normalizedIP, signal })
      return new Promise((resolve) => {
        finishLookup = resolve
      })
    })
    const firstCellUpdates = []
    const secondCellUpdates = []
    const unsubscribeFirst = store.subscribe(() => firstCellUpdates.push(store.read(" 8.8.8.8 ").status))
    const unsubscribeSecond = store.subscribe(() => secondCellUpdates.push(store.read("8.8.8.8").status))

    try {
      const firstLookup = store.lookup(101, " 8.8.8.8 ")
      const duplicateLookup = store.lookup(202, "8.8.8.8")

      assert.equal(calls.length, 1)
      assert.equal(calls[0].requestLogID, 101)
      assert.equal(calls[0].normalizedIP, "8.8.8.8")
      assert.equal(calls[0].signal.aborted, false)
      assert.deepEqual(firstCellUpdates, ["loading"])
      assert.deepEqual(secondCellUpdates, ["loading"])

      finishLookup(parseGatewayClientIpGeoResponse({
        ip: "8.8.8.8",
        country_code: "US",
        region: "California",
        city: "Los Angeles",
        accuracy_radius_km: 20,
        asn: "AS64500",
        organization: "Example ISP",
      }, "8.8.8.8"))
      await Promise.all([firstLookup, duplicateLookup])

      assert.equal(store.read("8.8.8.8").status, "success")
      assert.deepEqual(firstCellUpdates, ["loading", "success"])
      assert.deepEqual(secondCellUpdates, ["loading", "success"])
    } finally {
      unsubscribeFirst()
      unsubscribeSecond()
    }
  })
})

test("successful GeoIP cache entries actively notify subscribers when their TTL expires", async () => {
  await withGeoModule(async ({ createGatewayClientIpGeoStore }) => {
    const clock = createFakeClock(1_000)
    const store = createGatewayClientIpGeoStore(
      async () => successEntry("California · US", clock.now()),
      {
        now: clock.now,
        setTimeout: clock.setTimeout,
        clearTimeout: clock.clearTimeout,
        cacheTTLms: 100,
        lookupTimeoutMs: 1_000,
      },
    )
    const updates = []
    const unsubscribe = store.subscribe(() => updates.push(store.read("8.8.8.8").status))

    try {
      await store.lookup(101, "8.8.8.8")
      assert.equal(store.read("8.8.8.8").status, "success")
      assert.deepEqual(updates, ["loading", "success"])
      assert.equal(clock.pendingCount(), 1)

      clock.advanceBy(99)
      assert.deepEqual(updates, ["loading", "success"])
      assert.equal(store.read("8.8.8.8").status, "success")

      clock.advanceBy(1)
      assert.equal(store.read("8.8.8.8").status, "idle")
      assert.deepEqual(updates, ["loading", "success", "idle"])
      assert.equal(clock.pendingCount(), 0)
    } finally {
      unsubscribe()
    }
  })
})

test("GeoIP expiry timers are replaced, evicted, and cleared with the final subscriber", async () => {
  await withGeoModule(async ({ createGatewayClientIpGeoStore }) => {
    const clock = createFakeClock(2_000)
    const store = createGatewayClientIpGeoStore(
      async (_requestLogID, ip) => successEntry(`${ip} · US`, clock.now()),
      {
        now: clock.now,
        setTimeout: clock.setTimeout,
        clearTimeout: clock.clearTimeout,
        cacheTTLms: 100,
        lookupTimeoutMs: 1_000,
        maxEntries: 1,
      },
    )
    const unsubscribe = store.subscribe(() => undefined)

    try {
      await store.lookup(101, "8.8.8.8")
      assert.equal(clock.pendingCount(), 1)

      await store.lookup(102, "8.8.8.8", true)
      assert.equal(clock.pendingCount(), 1)

      await store.lookup(103, "1.1.1.1")
      assert.equal(store.read("8.8.8.8").status, "idle")
      assert.equal(store.read("1.1.1.1").status, "success")
      assert.equal(clock.pendingCount(), 1)
    } finally {
      unsubscribe()
    }

    assert.equal(clock.pendingCount(), 0)
    const resumedUpdates = []
    const unsubscribeResumed = store.subscribe(() => resumedUpdates.push(store.read("1.1.1.1").status))
    try {
      assert.equal(clock.pendingCount(), 1)
      clock.advanceBy(100)
      assert.deepEqual(resumedUpdates, ["idle"])
      assert.equal(store.read("1.1.1.1").status, "idle")
      assert.equal(clock.pendingCount(), 0)
    } finally {
      unsubscribeResumed()
    }
  })
})

test("force GeoIP refresh aborts stale work and late results cannot replace the newest value", async () => {
  await withGeoModule(async ({ createGatewayClientIpGeoStore }) => {
    const clock = createFakeClock(3_000)
    const requests = []
    const store = createGatewayClientIpGeoStore((requestLogID, ip, signal) => {
      const next = deferred()
      requests.push({ requestLogID, ip, signal, ...next })
      return next.promise
    }, {
      now: clock.now,
      setTimeout: clock.setTimeout,
      clearTimeout: clock.clearTimeout,
      cacheTTLms: 100,
      lookupTimeoutMs: 1_000,
    })
    const unsubscribe = store.subscribe(() => undefined)

    try {
      const first = store.lookup(101, "8.8.8.8")
      const second = store.lookup(102, "8.8.8.8", true)
      assert.equal(requests.length, 2)
      assert.equal(requests[0].signal.aborted, true)
      assert.equal(requests[1].signal.aborted, false)

      requests[1].resolve(successEntry("new result", clock.now()))
      await second
      assert.equal(store.read("8.8.8.8").status, "success")
      assert.equal(store.read("8.8.8.8").label, "new result")

      requests[0].resolve(successEntry("stale result", clock.now()))
      await first
      assert.equal(store.read("8.8.8.8").status, "success")
      assert.equal(store.read("8.8.8.8").label, "new result")
    } finally {
      unsubscribe()
    }
  })
})

test("unsubscribed GeoIP cells receive no late callback and release request timers", async () => {
  await withGeoModule(async ({ createGatewayClientIpGeoStore }) => {
    const clock = createFakeClock(4_000)
    const pending = deferred()
    let signal
    const store = createGatewayClientIpGeoStore((_requestLogID, _ip, requestSignal) => {
      signal = requestSignal
      return pending.promise
    }, {
      now: clock.now,
      setTimeout: clock.setTimeout,
      clearTimeout: clock.clearTimeout,
      cacheTTLms: 100,
      lookupTimeoutMs: 1_000,
    })
    const updates = []
    const unsubscribe = store.subscribe(() => updates.push(store.read("8.8.8.8").status))
    const lookup = store.lookup(101, "8.8.8.8")

    assert.deepEqual(updates, ["loading"])
    assert.equal(clock.pendingCount(), 1)
    unsubscribe()
    assert.equal(signal.aborted, true)
    assert.equal(clock.pendingCount(), 0)

    pending.resolve(successEntry("late result", clock.now()))
    await lookup
    assert.deepEqual(updates, ["loading"])
    assert.equal(store.read("8.8.8.8").status, "idle")
  })
})
