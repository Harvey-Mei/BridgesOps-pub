import assert from "node:assert/strict"
import test from "node:test"

import {
  createGatewayGroupCachePagination,
  gatewayGroupCacheCanGoBack,
  gatewayGroupCacheCurrentCursor,
  gatewayGroupCacheGoBack,
  gatewayGroupCacheGoNext,
  gatewayGroupCachePageNumber,
  resetGatewayGroupCachePagination,
} from "./gateway-group-cache-pagination"

test("cache-ratio cursors advance and return without loading preceding pages", () => {
  const first = createGatewayGroupCachePagination(50)
  assert.equal(gatewayGroupCacheCurrentCursor(first), "")
  assert.equal(gatewayGroupCachePageNumber(first), 1)
  assert.equal(gatewayGroupCacheCanGoBack(first), false)

  const second = gatewayGroupCacheGoNext(first, "cursor-2")
  const third = gatewayGroupCacheGoNext(second, "cursor-3")
  assert.equal(gatewayGroupCacheCurrentCursor(third), "cursor-3")
  assert.equal(gatewayGroupCachePageNumber(third), 3)

  const back = gatewayGroupCacheGoBack(third)
  assert.equal(gatewayGroupCacheCurrentCursor(back), "cursor-2")
  assert.equal(gatewayGroupCachePageNumber(back), 2)
  assert.equal(gatewayGroupCacheCanGoBack(back), true)
})

test("a refreshed next cursor replaces the stale forward branch", () => {
  const first = createGatewayGroupCachePagination()
  const second = gatewayGroupCacheGoNext(first, "cursor-2")
  const oldThird = gatewayGroupCacheGoNext(second, "old-cursor-3")
  const back = gatewayGroupCacheGoBack(oldThird)
  const refreshedThird = gatewayGroupCacheGoNext(back, "new-cursor-3")

  assert.deepEqual(refreshedThird.cursors, ["", "cursor-2", "new-cursor-3"])
  assert.equal(gatewayGroupCacheCurrentCursor(refreshedThird), "new-cursor-3")
})

test("blank next cursors are inert and page-size changes reset the stack", () => {
  const first = createGatewayGroupCachePagination()
  assert.equal(gatewayGroupCacheGoNext(first, " "), first)
  assert.equal(gatewayGroupCacheGoBack(first), first)

  const second = gatewayGroupCacheGoNext(first, "cursor-2")
  const resized = resetGatewayGroupCachePagination(second, 100)
  assert.deepEqual(resized, { cursors: [""], pageIndex: 0, pageSize: 100 })
})
