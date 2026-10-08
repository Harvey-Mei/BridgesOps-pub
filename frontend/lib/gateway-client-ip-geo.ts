import { useSyncExternalStore } from "react"
import ipaddr from "ipaddr.js"
import { apiFetch } from "@/lib/api"

export type GatewayClientIpGeoDetail = {
  provider?: string
  country?: string
  countryCode?: string
  region?: string
  city?: string
  postal?: string
  latitude?: number
  longitude?: number
  accuracyRadiusKM?: number
  asn?: string
  organization?: string
  timezone?: string
}

export type GatewayClientIpSource = "direct" | "trusted_proxy" | "unknown"

export type GatewayClientIpGeoPrecision =
  | "city_estimate"
  | "region"
  | "country"
  | "network_exit"

export type GatewayClientIpNetworkKind = "ordinary" | "cdn_proxy" | "cloud_hosting"

export type GatewayClientIpGeoSuccessEntry = {
  status: "success"
  label: string
  detail: GatewayClientIpGeoDetail
  precision: GatewayClientIpGeoPrecision
  networkKind: GatewayClientIpNetworkKind
  networkName?: string
  fetchedAt: number
}

export type GatewayClientIpGeoEntry =
  | { status: "idle" }
  | { status: "loading" }
  | GatewayClientIpGeoSuccessEntry
  | { status: "error" }
  | { status: "unavailable" }
  | { status: "not_found" }
  | { status: "private" }
  | { status: "invalid" }

export type GatewayClientIpGeoPresentation = {
  label: string
  sourceLabel: string
  sourceHint: string
  precisionLabel: string
  precisionHint: string
  city?: string
  showPostal: boolean
  showCoordinates: boolean
}

type RawGeoResponse = {
  ip?: unknown
  provider?: unknown
  country?: unknown
  country_code?: unknown
  region?: unknown
  city?: unknown
  postal?: unknown
  latitude?: unknown
  longitude?: unknown
  accuracy_radius_km?: unknown
  asn?: unknown
  organization?: unknown
  timezone?: unknown
}

const idleEntry: GatewayClientIpGeoEntry = { status: "idle" }
const privateEntry: GatewayClientIpGeoEntry = { status: "private" }
const invalidEntry: GatewayClientIpGeoEntry = { status: "invalid" }
const unavailableEntry: GatewayClientIpGeoEntry = { status: "unavailable" }
const notFoundEntry: GatewayClientIpGeoEntry = { status: "not_found" }
const geoLookupTimeoutMs = 8_000
const geoCacheTTL = 24 * 60 * 60 * 1_000
const maxCachedEntries = 200
const maximumCityAccuracyRadiusKM = 75
const legacyGeoStorageKeys = [
  "bridgeops:gateway-client-ip-geo:v1",
  "bridgeops:gateway-client-ip-geo:v2",
  "bridgeops:gateway-client-ip-geo:v3",
]

export type GatewayClientIpGeoLookup = (
  requestLogID: number,
  normalizedIP: string,
  signal: AbortSignal,
) => Promise<GatewayClientIpGeoEntry>

export type GatewayClientIpGeoStore = {
  read: (ip: string) => GatewayClientIpGeoEntry
  subscribe: (listener: () => void) => () => void
  lookup: (requestLogID: number | undefined, ip: string, force?: boolean) => Promise<void>
}

type GatewayClientIpGeoTimer = ReturnType<typeof globalThis.setTimeout>

export type GatewayClientIpGeoStoreOptions = {
  now?: () => number
  setTimeout?: (callback: () => void, delayMs: number) => GatewayClientIpGeoTimer
  clearTimeout?: (timer: GatewayClientIpGeoTimer) => void
  cacheTTLms?: number
  lookupTimeoutMs?: number
  maxEntries?: number
}

clearLegacyGeoEntries()

const geoStore = createGatewayClientIpGeoStore(fetchGatewayClientIpGeoEntry)

export function useGatewayClientIpGeoEntry(ip: string) {
  return useSyncExternalStore(geoStore.subscribe, () => geoStore.read(ip), () => idleEntry)
}

// lookupGatewayClientIp only submits a request-log ID. The recorded IP remains
// on the BridgeOps server and is looked up against the local database there.
export async function lookupGatewayClientIp(requestLogID: number | undefined, ip: string, force = false) {
  await geoStore.lookup(requestLogID, ip, force)
}

export function createGatewayClientIpGeoStore(
  fetchLookup: GatewayClientIpGeoLookup,
  options: GatewayClientIpGeoStoreOptions = {},
): GatewayClientIpGeoStore {
  const entries = new Map<string, GatewayClientIpGeoEntry>()
  const listeners = new Set<() => void>()
  const expirationTimers = new Map<string, GatewayClientIpGeoTimer>()
  const inFlightLookups = new Map<string, {
    controller: AbortController
    timeout?: GatewayClientIpGeoTimer
  }>()
  const now = options.now ?? Date.now
  const setTimer: (callback: () => void, delayMs: number) => GatewayClientIpGeoTimer = options.setTimeout
    ?? ((callback, delayMs) => globalThis.setTimeout(callback, delayMs))
  const clearTimer: (timer: GatewayClientIpGeoTimer) => void = options.clearTimeout
    ?? ((timer) => globalThis.clearTimeout(timer))
  const cacheTTLms = Math.max(1, options.cacheTTLms ?? geoCacheTTL)
  const lookupTimeoutMs = Math.max(1, options.lookupTimeoutMs ?? geoLookupTimeoutMs)
  const entryLimit = Math.max(1, options.maxEntries ?? maxCachedEntries)

  function subscribe(listener: () => void) {
    const activateTimers = listeners.size === 0
    listeners.add(listener)
    if (activateTimers) scheduleExpirationTimers()
    return () => {
      if (!listeners.delete(listener) || listeners.size > 0) return
      clearExpirationTimers()
      cancelInFlightLookups()
    }
  }

  function emitChange() {
    listeners.forEach((listener) => listener())
  }

  function read(ip: string): GatewayClientIpGeoEntry {
    const classified = classifyIP(ip)
    if (classified.disposition !== "public") {
      return classified.disposition === "invalid" ? invalidEntry : privateEntry
    }
    const entry = entries.get(classified.normalized)
    if (entry?.status === "success" && isExpired(entry)) return idleEntry
    return entry ?? idleEntry
  }

  function setEntry(lookupKey: string, entry: GatewayClientIpGeoEntry) {
    clearExpirationTimer(lookupKey)
    entries.delete(lookupKey)
    entries.set(lookupKey, entry)
    trimEntries()
    if (entry.status === "success" && entries.get(lookupKey) === entry && listeners.size > 0) {
      scheduleExpirationTimer(lookupKey, entry)
    }
    emitChange()
  }

  function isExpired(entry: GatewayClientIpGeoSuccessEntry) {
    return now() >= entry.fetchedAt + cacheTTLms
  }

  function scheduleExpirationTimer(lookupKey: string, entry: GatewayClientIpGeoSuccessEntry) {
    clearExpirationTimer(lookupKey)
    const remainingMs = entry.fetchedAt + cacheTTLms - now()
    if (remainingMs <= 0) {
      if (entries.get(lookupKey) === entry) entries.delete(lookupKey)
      return
    }
    const timer = setTimer(() => {
      expirationTimers.delete(lookupKey)
      if (entries.get(lookupKey) !== entry) return
      const nextRemainingMs = entry.fetchedAt + cacheTTLms - now()
      if (nextRemainingMs > 0) {
        scheduleExpirationTimer(lookupKey, entry)
        return
      }
      entries.delete(lookupKey)
      emitChange()
    }, remainingMs)
    expirationTimers.set(lookupKey, timer)
  }

  function scheduleExpirationTimers() {
    let expired = false
    for (const [lookupKey, entry] of entries) {
      if (entry.status !== "success") continue
      if (isExpired(entry)) {
        entries.delete(lookupKey)
        expired = true
        continue
      }
      scheduleExpirationTimer(lookupKey, entry)
    }
    if (expired) emitChange()
  }

  function clearExpirationTimer(lookupKey: string) {
    const timer = expirationTimers.get(lookupKey)
    if (timer === undefined) return
    expirationTimers.delete(lookupKey)
    clearTimer(timer)
  }

  function clearExpirationTimers() {
    for (const timer of expirationTimers.values()) clearTimer(timer)
    expirationTimers.clear()
  }

  function trimEntries() {
    while (entries.size > entryLimit) {
      const oldestKey = entries.keys().next().value
      if (!oldestKey) return
      entries.delete(oldestKey)
      clearExpirationTimer(oldestKey)
    }
  }

  function cancelInFlightLookup(lookupKey: string, removeLoadingEntry: boolean) {
    const request = inFlightLookups.get(lookupKey)
    if (!request) return
    inFlightLookups.delete(lookupKey)
    if (request.timeout !== undefined) clearTimer(request.timeout)
    request.timeout = undefined
    request.controller.abort()
    if (removeLoadingEntry && entries.get(lookupKey)?.status === "loading") entries.delete(lookupKey)
  }

  function cancelInFlightLookups() {
    for (const lookupKey of Array.from(inFlightLookups.keys())) cancelInFlightLookup(lookupKey, true)
  }

  async function lookup(requestLogID: number | undefined, ip: string, force = false) {
    const classified = classifyIP(ip)
    if (classified.disposition !== "public") return
    const safeRequestLogID = Number(requestLogID)
    if (!Number.isSafeInteger(safeRequestLogID) || safeRequestLogID <= 0) return
    const lookupKey = classified.normalized
    const existing = read(lookupKey)
    if (!force && (existing.status === "success" || existing.status === "loading" || existing.status === "unavailable" || existing.status === "not_found")) return

    cancelInFlightLookup(lookupKey, false)
    const request = { controller: new AbortController(), timeout: undefined as GatewayClientIpGeoTimer | undefined }
    inFlightLookups.set(lookupKey, request)
    request.timeout = setTimer(() => {
      request.timeout = undefined
      request.controller.abort()
    }, lookupTimeoutMs)
    setEntry(lookupKey, { status: "loading" })

    try {
      const entry = await fetchLookup(safeRequestLogID, lookupKey, request.controller.signal)
      if (inFlightLookups.get(lookupKey) !== request) return
      setEntry(lookupKey, entry)
    } catch (error) {
      if (inFlightLookups.get(lookupKey) === request) setEntry(lookupKey, gatewayClientIpGeoEntryFromError(error))
    } finally {
      if (request.timeout !== undefined) clearTimer(request.timeout)
      request.timeout = undefined
      if (inFlightLookups.get(lookupKey) === request) inFlightLookups.delete(lookupKey)
    }
  }

  return { read, subscribe, lookup }
}

async function fetchGatewayClientIpGeoEntry(requestLogID: number, normalizedIP: string, signal: AbortSignal) {
  const response = await apiFetch<RawGeoResponse>(`/gateway/request-logs/${requestLogID}/client-ip-geo`, {
    method: "POST",
    signal,
  })
  return parseGatewayClientIpGeoResponse(response, normalizedIP)
}

export function parseGatewayClientIpGeoResponse(value: unknown, lookupIP: string): GatewayClientIpGeoEntry {
  if (!value || typeof value !== "object") return { status: "error" }
  const raw = value as RawGeoResponse
  if (typeof raw.ip === "string") {
    const responseIP = classifyIP(raw.ip)
    if (responseIP.disposition !== "public" || responseIP.normalized !== lookupIP) return { status: "error" }
  } else return { status: "error" }
  const detail: GatewayClientIpGeoDetail = {
    provider: cleanGeoPart(raw.provider),
    country: cleanGeoPart(raw.country),
    countryCode: cleanGeoPart(raw.country_code),
    region: cleanGeoPart(raw.region),
    city: cleanGeoPart(raw.city),
    postal: cleanGeoPart(raw.postal),
    latitude: cleanGeoCoordinate(raw.latitude, -90, 90),
    longitude: cleanGeoCoordinate(raw.longitude, -180, 180),
    accuracyRadiusKM: cleanGeoAccuracyRadius(raw.accuracy_radius_km),
    asn: cleanGeoASN(raw.asn),
    organization: cleanGeoPart(raw.organization),
    timezone: cleanGeoTimeZone(raw.timezone),
  }
  return newGatewayClientIpGeoSuccessEntry(detail, Date.now()) ?? { status: "error" }
}

export function gatewayClientIpGeoEntryFromError(error: unknown): GatewayClientIpGeoEntry {
  const status = typeof error === "object" && error !== null && "status" in error
    ? Number((error as { status?: unknown }).status)
    : 0
  switch (status) {
    case 503:
      return unavailableEntry
    case 404:
      return notFoundEntry
    case 422:
      return privateEntry
    default:
      return { status: "error" }
  }
}

export function gatewayClientIpGeoPresentation(entry: GatewayClientIpGeoEntry, source?: string): GatewayClientIpGeoPresentation | null {
  if (entry.status !== "success") return null
  const normalizedSource = normalizeGatewayClientIpSource(source)
  const sourcePresentation = gatewayClientIpSourcePresentation(normalizedSource)
  const precisionPresentation = gatewayClientIpPrecisionPresentation(entry)
  const cityVisible = entry.precision === "city_estimate" && entry.networkKind === "ordinary"
  return {
    label: entry.label,
    sourceLabel: sourcePresentation.label,
    sourceHint: sourcePresentation.hint,
    precisionLabel: precisionPresentation.label,
    precisionHint: precisionPresentation.hint,
    city: cityVisible ? entry.detail.city : undefined,
    showPostal: cityVisible,
    showCoordinates: cityVisible,
  }
}

function newGatewayClientIpGeoSuccessEntry(detail: GatewayClientIpGeoDetail, fetchedAt: number): GatewayClientIpGeoSuccessEntry | null {
  const networkName = formatGeoNetwork(detail.organization)
  const networkKind = classifyGatewayClientIpNetwork(networkName)
  const precision = gatewayClientIpGeoPrecision(detail, networkKind, networkName)
  const location = uniqueGeoParts([
    precision === "city_estimate" ? detail.city : undefined,
    detail.region,
    detail.countryCode ?? detail.country,
  ]).join(" · ")
  if (!location) return null
  return {
    status: "success",
    label: location,
    detail,
    precision,
    networkKind,
    networkName: networkName || undefined,
    fetchedAt,
  }
}

function normalizeGatewayClientIpSource(value?: string): GatewayClientIpSource {
  switch (String(value ?? "").trim()) {
    case "direct":
      return "direct"
    case "trusted_proxy":
      return "trusted_proxy"
    default:
      return "unknown"
  }
}

function gatewayClientIpSourcePresentation(source: GatewayClientIpSource) {
  switch (source) {
    case "direct":
      return {
        label: "直连 IP",
        hint: "该地址来自直接连接的网络对端。VPN、企业网关或共享网络会让位置显示为其出口网络。",
      }
    case "trusted_proxy":
      return {
        label: "可信代理转发",
        hint: "服务端仅在连接来自已配置的可信代理时读取转发地址。该地址仍可能是 VPN、企业网关或 CDN 的出口。",
      }
    default:
      return {
        label: "来源未记录",
        hint: "历史记录没有保存 IP 来源，无法确认它是直接客户端地址还是代理出口。",
      }
  }
}

function gatewayClientIpPrecisionPresentation(entry: GatewayClientIpGeoSuccessEntry) {
  switch (entry.precision) {
    case "city_estimate":
      return {
        label: "城市级估算",
        hint: `本地数据库标注的精度半径约 ±${entry.detail.accuracyRadiusKM} km，只表示网络出口的大致区域，不代表精确位置或实际使用者所在地。`,
      }
    case "region":
      return {
        label: "仅国家 / 地区",
        hint: gatewayClientIpRegionPrecisionHint(entry),
      }
    case "country":
      return {
        label: "仅国家",
        hint: "公开 IP 数据没有提供可靠的地区或城市字段，因此不展示更细位置。",
      }
    case "network_exit":
      return {
        label: entry.networkKind === "cdn_proxy" ? "CDN / 代理出口" : "云网络出口",
        hint: "该地址属于已识别的 CDN、代理或云网络。城市通常是服务出口，不代表客户端位置，因此已隐藏。",
      }
  }
}

function gatewayClientIpGeoPrecision(detail: GatewayClientIpGeoDetail, networkKind: GatewayClientIpNetworkKind, networkName: string): GatewayClientIpGeoPrecision {
  if (networkKind !== "ordinary") return "network_exit"
	if (hasReliableCity(detail, networkName)) return "city_estimate"
  if (detail.region) return "region"
  return "country"
}

function hasReliableCity(detail: GatewayClientIpGeoDetail, networkName: string) {
  return Boolean(detail.city) && Boolean(detail.asn) && Boolean(networkName) &&
    typeof detail.accuracyRadiusKM === "number" && detail.accuracyRadiusKM > 0 && detail.accuracyRadiusKM <= maximumCityAccuracyRadiusKM
}

function gatewayClientIpRegionPrecisionHint(entry: GatewayClientIpGeoSuccessEntry) {
  const accuracyRadiusKM = entry.detail.accuracyRadiusKM
  if (entry.detail.city && !entry.detail.asn) return "本地 ASN 数据未能确认网络归属。为避免把 CDN、代理或云出口误作用户位置，城市与坐标已隐藏。"
  if (entry.detail.city && !entry.networkName) return "本地 ASN 数据没有提供网络组织。为避免把网络出口误作用户位置，城市与坐标已隐藏。"
  if (entry.detail.city && !accuracyRadiusKM) return "本地数据库没有提供城市精度半径，因此不展示城市或坐标。"
  if (entry.detail.city && typeof accuracyRadiusKM === "number" && accuracyRadiusKM > maximumCityAccuracyRadiusKM) return `本地数据库标注的城市精度半径约 ±${accuracyRadiusKM} km，超过 ${maximumCityAccuracyRadiusKM} km 阈值，因此不展示城市或坐标。`
  return "本地数据库没有提供可验证的城市级结果，因此不展示城市或坐标。"
}

function classifyGatewayClientIpNetwork(networkName: string): GatewayClientIpNetworkKind {
  const normalized = networkName.toLocaleLowerCase()
  if (!normalized) return "ordinary"
  if (/(?:cloudflare|fastly|akamai|cloudfront|bunny(?:cdn)?|cdn77|gcore|imperva|sucuri|keycdn)/.test(normalized)) {
    return "cdn_proxy"
  }
  if (/(?:amazon(?:\.com)?|\baws\b|google cloud|google llc|microsoft azure|digitalocean|linode|vultr|hetzner|ovh|tencent cloud|alibaba cloud|oracle cloud)/.test(normalized)) {
    return "cloud_hosting"
  }
  return "ordinary"
}

function cleanGeoPart(value: unknown) {
  if (typeof value !== "string") return undefined
  const cleaned = value.trim().replace(/\s+/g, " ").slice(0, 100)
  if (!cleaned || ["-", "n/a", "na", "none", "null", "unknown", "unknown city", "not available"].includes(cleaned.toLocaleLowerCase())) {
    return undefined
  }
  return cleaned
}

function cleanGeoCoordinate(value: unknown, minimum: number, maximum: number) {
  if (typeof value !== "number" && typeof value !== "string") return undefined
  const numberValue = Number(String(value).trim())
  if (!Number.isFinite(numberValue) || numberValue < minimum || numberValue > maximum) return undefined
  return numberValue
}

function cleanGeoAccuracyRadius(value: unknown) {
  if (typeof value !== "number" && typeof value !== "string") return undefined
  const radius = Number(String(value).trim())
  if (!Number.isFinite(radius) || radius < 0 || radius > 10_000) return undefined
  return Math.round(radius)
}

function cleanGeoASN(value: unknown) {
  if (typeof value !== "number" && typeof value !== "string") return undefined
  const normalized = String(value).trim().replace(/^AS/i, "")
  return /^\d{1,10}$/.test(normalized) ? `AS${normalized}` : undefined
}

function cleanGeoTimeZone(value: unknown) {
  return cleanGeoPart(value)
}

function uniqueGeoParts(parts: Array<string | undefined>) {
  const seen = new Set<string>()
  return parts.filter((part): part is string => {
    if (!part) return false
    const normalized = part.toLocaleLowerCase()
    if (seen.has(normalized)) return false
    seen.add(normalized)
    return true
  })
}

function formatGeoNetwork(organization?: string) {
  return uniqueGeoParts([organization]).join(" · ")
}

function clearLegacyGeoEntries() {
  if (typeof window === "undefined") return
  try {
    legacyGeoStorageKeys.forEach((key) => window.localStorage.removeItem(key))
  } catch {
    // Browser storage restrictions must not block the usage page.
  }
}

export function gatewayClientIpGeoCacheKey(ip: string) {
  const classified = classifyIP(ip)
  return classified.disposition === "public" ? classified.normalized : ""
}

type ClassifiedIP =
  | { disposition: "public"; normalized: string }
  | { disposition: "private" | "invalid" }

function classifyIP(ip: string): ClassifiedIP {
  const candidate = ip.trim()
  if (!candidate || candidate.includes("%")) return { disposition: "invalid" }
  if (!candidate.includes(":") && !ipaddr.IPv4.isValidFourPartDecimal(candidate)) {
    return { disposition: "invalid" }
  }

  try {
    const address = ipaddr.process(candidate)
    if (address.range() !== "unicast") return { disposition: "private" }

    // Publicly routable IPv6 unicast is allocated from 2000::/3.
    if (address.kind() === "ipv6" && (address.toByteArray()[0] & 0xe0) !== 0x20) {
      return { disposition: "private" }
    }
    return { disposition: "public", normalized: address.toString() }
  } catch {
    return { disposition: "invalid" }
  }
}
