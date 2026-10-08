/**
 * 各种小格式化工具：相对时间、金额、倍率箭头等。
 */

import { formatInTimeZone, getTimezoneOffset } from "date-fns-tz"

const RELATIVE_THRESHOLDS: Array<{ limit: number; unit: string; divisor: number }> = [
  { limit: 60, unit: "秒前", divisor: 1 },
  { limit: 60 * 60, unit: "分钟前", divisor: 60 },
  { limit: 24 * 60 * 60, unit: "小时前", divisor: 60 * 60 },
  { limit: 30 * 24 * 60 * 60, unit: "天前", divisor: 24 * 60 * 60 },
]

export const DEFAULT_DISPLAY_TIME_ZONE = "Asia/Shanghai"
// The server-side system setting is the only display-timezone authority.
// Use this stable default only while that setting is still loading.
let displayTimeZone = DEFAULT_DISPLAY_TIME_ZONE
let pendingDisplayTimeZone: string | null = null

export type ZonedDateTimeErrorCode =
  | "invalid_format"
  | "invalid_date"
  | "invalid_time_zone"
  | "nonexistent_local_time"
  | "ambiguous_local_time"

export class ZonedDateTimeError extends Error {
  readonly code: ZonedDateTimeErrorCode
  readonly value: string
  readonly timeZone: string

  constructor(code: ZonedDateTimeErrorCode, value: string, timeZone: string) {
    super(zonedDateTimeErrorMessage(code, value, timeZone))
    this.name = "ZonedDateTimeError"
    this.code = code
    this.value = value
    this.timeZone = timeZone
  }
}

export function normalizeDisplayTimeZone(value?: string | null) {
  const candidate = String(value || "").trim() || DEFAULT_DISPLAY_TIME_ZONE
  try {
    new Intl.DateTimeFormat("en-US", { timeZone: candidate }).format(new Date())
    return candidate
  } catch {
    return DEFAULT_DISPLAY_TIME_ZONE
  }
}

export function setDisplayTimeZone(value?: string | null) {
  const next = normalizeDisplayTimeZone(value)
  if (pendingDisplayTimeZone === next && next !== displayTimeZone) return displayTimeZone
  if (next === displayTimeZone) return next
  displayTimeZone = next
  return next
}

export function stageDisplayTimeZone(value?: string | null) {
  pendingDisplayTimeZone = normalizeDisplayTimeZone(value)
  return pendingDisplayTimeZone
}

export function confirmDisplayTimeZone(value?: string | null) {
  const next = normalizeDisplayTimeZone(value)
  pendingDisplayTimeZone = null
  displayTimeZone = next
  return next
}

export function getDisplayTimeZone() {
  return displayTimeZone
}

export function dateTimeInputValue(date: Date, timeZone: string = displayTimeZone) {
  if (Number.isNaN(date.getTime())) return ""
  return formatInTimeZone(date, normalizeDisplayTimeZone(timeZone), "yyyy-MM-dd'T'HH:mm")
}

export function zonedDateTimeToISOString(value: string, timeZone: string = displayTimeZone) {
  const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2}))?$/.exec(value.trim())
  const input = value.trim()
  const effectiveTimeZone = String(timeZone || "").trim() || displayTimeZone
  if (!match) throw new ZonedDateTimeError("invalid_format", input, effectiveTimeZone)
  if (!isValidTimeZone(effectiveTimeZone)) {
    throw new ZonedDateTimeError("invalid_time_zone", input, effectiveTimeZone)
  }

  const naive = makeNaiveDate(
    Number(match[1]),
    Number(match[2]),
    Number(match[3]),
    Number(match[4]),
    Number(match[5]),
    Number(match[6] || "00"),
  )
  if (!naive) throw new ZonedDateTimeError("invalid_date", input, effectiveTimeZone)

  const normalized = `${match[1]}-${match[2]}-${match[3]}T${match[4]}:${match[5]}:${match[6] || "00"}`
  const candidates = localDateTimeCandidates(naive.getTime(), normalized, effectiveTimeZone)
  if (candidates.length === 0) {
    throw new ZonedDateTimeError("nonexistent_local_time", input, effectiveTimeZone)
  }
  if (candidates.length > 1) {
    throw new ZonedDateTimeError("ambiguous_local_time", input, effectiveTimeZone)
  }
  return candidates[0]
}

const timezoneOffsetCache = new Map<string, number[]>()
const TIMEZONE_OFFSET_CACHE_LIMIT = 64
const OFFSET_SAMPLE_RANGE_MS = 3 * 24 * 60 * 60 * 1000
const OFFSET_SAMPLE_STEP_MS = 6 * 60 * 60 * 1000

function isValidTimeZone(value: string) {
  try {
    new Intl.DateTimeFormat("en-US", { timeZone: value }).format()
    return true
  } catch {
    return false
  }
}

function makeNaiveDate(year: number, month: number, day: number, hour: number, minute: number, second: number) {
  if (month < 1 || month > 12 || hour > 23 || minute > 59 || second > 59) return null
  const date = new Date(0)
  date.setUTCFullYear(year, month - 1, day)
  date.setUTCHours(hour, minute, second, 0)
  if (
    date.getUTCFullYear() !== year
    || date.getUTCMonth() !== month - 1
    || date.getUTCDate() !== day
    || date.getUTCHours() !== hour
    || date.getUTCMinutes() !== minute
    || date.getUTCSeconds() !== second
  ) return null
  return date
}

function localDateTimeCandidates(naiveMillis: number, normalized: string, timeZone: string) {
  const cacheKey = `${timeZone}:${new Date(naiveMillis).toISOString().slice(0, 10)}`
  let offsets = timezoneOffsetCache.get(cacheKey)
  if (!offsets) {
    const found = new Set<number>()
    for (let timestamp = naiveMillis - OFFSET_SAMPLE_RANGE_MS; timestamp <= naiveMillis + OFFSET_SAMPLE_RANGE_MS; timestamp += OFFSET_SAMPLE_STEP_MS) {
      found.add(getTimezoneOffset(timeZone, new Date(timestamp)))
    }
    offsets = Array.from(found)
    timezoneOffsetCache.set(cacheKey, offsets)
    while (timezoneOffsetCache.size > TIMEZONE_OFFSET_CACHE_LIMIT) {
      const oldest = timezoneOffsetCache.keys().next().value
      if (oldest === undefined) break
      timezoneOffsetCache.delete(oldest)
    }
  }

  const candidates = new Set<number>()
  for (const offset of offsets) {
    const instant = new Date(naiveMillis - offset)
    if (formatInTimeZone(instant, timeZone, "yyyy-MM-dd'T'HH:mm:ss") === normalized) {
      candidates.add(instant.getTime())
    }
  }
  return Array.from(candidates).sort((left, right) => left - right).map((timestamp) => new Date(timestamp).toISOString())
}

function zonedDateTimeErrorMessage(code: ZonedDateTimeErrorCode, value: string, timeZone: string) {
  switch (code) {
    case "invalid_format":
      return `时间“${value || "未填写"}”格式无效，应为 YYYY-MM-DDTHH:mm`
    case "invalid_date":
      return `时间“${value}”不是有效的日历日期`
    case "invalid_time_zone":
      return `系统时区“${timeZone}”无效，无法解析时间范围`
    case "nonexistent_local_time":
      return `时间“${value}”在系统时区 ${timeZone} 中不存在（夏令时跳跃）`
    case "ambiguous_local_time":
      return `时间“${value}”在系统时区 ${timeZone} 中有歧义（夏令时回拨），请换一个时间`
  }
}

export function compactDateTime(iso?: string | null, timeZone: string = displayTimeZone) {
  if (!iso) return "—"
  const date = new Date(iso)
  if (Number.isNaN(date.getTime())) return String(iso)
  return formatInTimeZone(date, normalizeDisplayTimeZone(timeZone), "MM-dd HH:mm")
}

/** 把 ISO 时间转成"X 分钟前"等相对描述。 */
export function relativeTime(iso?: string | null, now: Date = new Date()): string {
  if (!iso) return "—"
  const t = new Date(iso).getTime()
  if (!Number.isFinite(t)) return "—"
  const diff = Math.max(0, Math.floor((now.getTime() - t) / 1000))
  if (diff < 5) return "刚刚"
  for (const r of RELATIVE_THRESHOLDS) {
    if (diff < r.limit) {
      return `${Math.floor(diff / r.divisor)} ${r.unit}`
    }
  }
  return new Date(iso).toLocaleDateString("zh-CN", { timeZone: displayTimeZone })
}

/** 把 ISO 时间转成简短的"HH:MM"。 */
export function shortTime(iso?: string | null): string {
  if (!iso) return "—"
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return "—"
  return d.toLocaleTimeString("zh-CN", { timeZone: displayTimeZone, hour: "2-digit", minute: "2-digit", hour12: false })
}

export function dateTime(iso?: string | null, timeZone: string = displayTimeZone): string {
  if (!iso) return "—"
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return "—"
  return d.toLocaleString("zh-CN", {
    timeZone: normalizeDisplayTimeZone(timeZone),
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  })
}

/** 货币格式：$1,234.56。 */
export function money(value: number | null | undefined, opts?: { precise?: boolean }) {
  if (value == null || !Number.isFinite(value)) return "—"
  return (
    "$" +
    value.toLocaleString("en-US", {
      minimumFractionDigits: opts?.precise ? 4 : 2,
      maximumFractionDigits: opts?.precise ? 4 : 2,
    })
  )
}

export function currentDayCost(value: number | null | undefined, sampledAt?: string | null, isToday?: boolean) {
  if (isToday === false) return null
  if (isToday === true) return value ?? null
  if (!sampledAt) return null
  const date = new Date(sampledAt)
  const now = new Date()
  if (Number.isNaN(date.getTime())) return null
  return zonedDayKey(date, displayTimeZone) === zonedDayKey(now, displayTimeZone)
    ? value ?? null
    : null
}

export function isTodayInDisplayTimeZone(iso?: string | null, now: Date = new Date()) {
  if (!iso) return false
  const date = new Date(iso)
  if (Number.isNaN(date.getTime())) return false
  return zonedDayKey(date, displayTimeZone) === zonedDayKey(now, displayTimeZone)
}

function zonedDayKey(date: Date, timeZone: string) {
  return formatInTimeZone(date, timeZone, "yyyy-MM-dd")
}

export function decimal(value: number | null | undefined, digits = 2) {
  if (value == null || !Number.isFinite(value)) return "—"
  return value.toLocaleString("en-US", {
    minimumFractionDigits: 0,
    maximumFractionDigits: digits,
  })
}

function compactFractionDigits(value: number) {
  return value.toFixed(2).replace(/\.00$/, "").replace(/(\.\d)0$/, "$1")
}

export function compactTokenCount(value: number | null | undefined) {
  if (value == null || !Number.isFinite(value)) return "—"
  const absolute = Math.abs(value)
  if (absolute >= 1_000_000_000) return `${compactFractionDigits(value / 1_000_000_000)}B`
  if (absolute >= 1_000_000) return `${compactFractionDigits(value / 1_000_000)}M`
  if (absolute >= 1_000) return `${compactFractionDigits(value / 1_000)}K`
  return decimal(value, 2)
}

export function cacheReadRatio(summary: { cache_read_tokens?: number; prompt_tokens?: number } | null | undefined) {
  const read = Number(summary?.cache_read_tokens ?? 0)
  const prompt = Number(summary?.prompt_tokens ?? 0)
  if (!Number.isFinite(prompt) || prompt <= 0 || !Number.isFinite(read) || read < 0) return null
  return read / prompt
}

export function formatCacheReadRatio(summary: { cache_read_tokens?: number; prompt_tokens?: number } | null | undefined) {
  const ratio = cacheReadRatio(summary)
  return ratio == null ? "—" : `${(ratio * 100).toFixed(1)}%`
}

export function compactCount(value: number | null | undefined) {
  if (value == null || !Number.isFinite(value)) return "—"
  const absolute = Math.abs(value)
  if (absolute >= 1_000_000) {
    const exponent = Math.floor(Math.log10(absolute))
    return `${compactFractionDigits(value / (10 ** exponent))}E${exponent}`
  }
  if (absolute >= 1_000) return `${compactFractionDigits(value / 1_000)}K`
  return decimal(value, 2)
}

export function formatRatio(value: number | null | undefined) {
  if (value == null || !Number.isFinite(value)) return "—"
  return value.toLocaleString("en-US", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 6,
  })
}

/** 把倍率渲染成"1.20 → 1.50"。 */
export function ratioArrow(from: number | null | undefined, to: number) {
  return `${formatRatio(from)} → ${formatRatio(to)}`
}

/** 计算变化方向 / 百分比文案，比如 "+25.0%"。 */
export function ratioDelta(from: number | null | undefined, to: number) {
  if (from == null || from === 0) {
    return { direction: "up" as const, pct: "新增" }
  }
  const pct = ((to - from) / Math.abs(from)) * 100
  const direction = pct >= 0 ? ("up" as const) : ("down" as const)
  return { direction, pct: `${pct >= 0 ? "+" : ""}${pct.toFixed(1)}%` }
}

/** 把 ChannelType "newapi"/"sub2api" 转成显示名 / 角标颜色。 */
export function channelTypeLabel(t: string) {
  switch (t) {
    case "newapi":
      return "NewAPI"
    case "sub2api":
      return "Sub2API"
    default:
      return t
  }
}
