import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import test from "node:test"

const queries = readFileSync(new URL("./queries.ts", import.meta.url), "utf8")
const gatewayPage = readFileSync(new URL("../app/gateway-page.tsx", import.meta.url), "utf8")
const usageView = readFileSync(new URL("../components/gateway/gateway-usage-view.tsx", import.meta.url), "utf8")
const refreshContext = readFileSync(new URL("./refresh-context.tsx", import.meta.url), "utf8")

test("group-cache view requests exactly one cursor page and keeps global 30 second refresh", () => {
  assert.match(queries, /useGatewayRequestLogGroupCachePage/)
  assert.match(queries, /\/gateway\/request-log-group-cache-ratios\?\$\{result\.query\}/)
  assert.match(queries, /query\.set\("page_size", String\(pageSize\)\)/)
  assert.match(queries, /if \(cursor\.trim\(\)\) query\.set\("cursor", cursor\.trim\(\)\)/)
  assert.match(refreshContext, /POLL_INTERVAL_MS = 30_000/)
  assert.match(gatewayPage, /usageTab === "group-cache"/)
  assert.match(gatewayPage, /groupCachePage=\{usageDataVisible && groupCacheRequestResolved \? groupCachePage\.data : null\}/)
  assert.match(usageView, /rows=\{groupCachePage\?\.items \?\? \[\]\}/)
  assert.doesNotMatch(usageView, /rows=\{analytics\?\.group_cache_ratios/)
})

test("group-cache cursor navigation locks synchronously and stays keyboard reachable", () => {
  assert.match(gatewayPage, /if \(groupCacheNavigationLockedRef\.current \|\| next === groupCachePagination\) return/)
  assert.match(gatewayPage, /groupCacheNavigationLockedRef\.current = true/)
  assert.match(gatewayPage, /groupCachePage\.resolvedPath !== groupCachePage\.requestPath/)
  assert.match(usageView, /aria-label="缓存比例上一页"/)
  assert.match(usageView, /aria-label="缓存比例下一页"/)
  assert.match(usageView, /max-h-\[calc\(100dvh-22rem\)\] min-h-72 overflow-auto/)
  assert.match(usageView, /Number\.isFinite\(value\) \? `\$\{\(value \* 100\)\.toFixed\(1\)\}%` : "无数据"/)
})
