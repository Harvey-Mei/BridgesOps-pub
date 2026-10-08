import assert from "node:assert/strict"
import test from "node:test"

import {
  cacheReadRatio,
  confirmDisplayTimeZone,
  formatCacheReadRatio,
  getDisplayTimeZone,
  setDisplayTimeZone,
  stageDisplayTimeZone,
  ZonedDateTimeError,
  zonedDateTimeToISOString,
} from "./format.ts"

test("cache read ratio preserves the observed token ratio", () => {
  assert.equal(cacheReadRatio({ cache_read_tokens: 25, prompt_tokens: 100 }), 0.25)
  assert.equal(formatCacheReadRatio({ cache_read_tokens: 150, prompt_tokens: 100 }), "150.0%")
  assert.equal(formatCacheReadRatio({ cache_read_tokens: 0, prompt_tokens: 0 }), "—")
  assert.equal(formatCacheReadRatio({ cache_read_tokens: -1, prompt_tokens: 100 }), "—")
})

test("saved timezone stays staged until runtime apply succeeds", () => {
  confirmDisplayTimeZone("Asia/Shanghai")
  stageDisplayTimeZone("America/New_York")

  assert.equal(setDisplayTimeZone("America/New_York"), "Asia/Shanghai")
  assert.equal(getDisplayTimeZone(), "Asia/Shanghai")

  assert.equal(confirmDisplayTimeZone("America/New_York"), "America/New_York")
  assert.equal(getDisplayTimeZone(), "America/New_York")
  confirmDisplayTimeZone("Asia/Shanghai")
})

test("unique local datetime converts to its UTC instant", () => {
  assert.equal(
    zonedDateTimeToISOString("2024-07-01T12:30", "America/New_York"),
    "2024-07-01T16:30:00.000Z",
  )
})

test("nonexistent DST local datetime is rejected explicitly", () => {
  assert.throws(
    () => zonedDateTimeToISOString("2024-03-10T02:30", "America/New_York"),
    (error) => error instanceof ZonedDateTimeError && error.code === "nonexistent_local_time",
  )
})

test("ambiguous DST local datetime is rejected explicitly", () => {
  assert.throws(
    () => zonedDateTimeToISOString("2024-11-03T01:30", "America/New_York"),
    (error) => error instanceof ZonedDateTimeError && error.code === "ambiguous_local_time",
  )
})

test("invalid calendar date is not normalized into another day", () => {
  assert.throws(
    () => zonedDateTimeToISOString("2024-02-31T12:00", "Asia/Shanghai"),
    (error) => error instanceof ZonedDateTimeError && error.code === "invalid_date",
  )
})
