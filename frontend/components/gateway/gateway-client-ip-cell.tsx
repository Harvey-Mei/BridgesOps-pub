import { memo, useId } from "react"
import { MapPin, RefreshCw } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover"
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import {
  gatewayClientIpGeoPresentation,
  lookupGatewayClientIp,
  useGatewayClientIpGeoEntry,
} from "@/lib/gateway-client-ip-geo"

export const GatewayClientIpCell = memo(function GatewayClientIpCell({ requestLogID, ip, source }: { requestLogID?: number; ip?: string; source?: string }) {
  const displayIP = ip?.trim() || ""
  const entry = useGatewayClientIpGeoEntry(displayIP)
  const presentation = gatewayClientIpGeoPresentation(entry, source)
  const detailTitleID = useId()

  if (!displayIP) {
    return <span className="text-xs text-muted-foreground">{"—"}</span>
  }

  return (
    <div className="min-w-0">
      <div className="truncate text-xs text-foreground" title={displayIP}>
        {displayIP}
      </div>
      <div className="mt-0.5 flex h-5 min-w-0 items-center text-xs text-muted-foreground" aria-live="polite">
        {entry.status === "idle" ? (
          <Tooltip>
            <TooltipTrigger asChild>
              <Button
                type="button"
                variant="ghost"
                size="sm"
                className="h-5 max-w-full gap-1 px-0 text-xs text-muted-foreground hover:bg-transparent hover:text-foreground"
                onClick={() => void lookupGatewayClientIp(requestLogID, displayIP)}
              >
                <MapPin className="size-3 shrink-0" />
                <span className="truncate">{"转换位置"}</span>
              </Button>
            </TooltipTrigger>
            <TooltipContent className="max-w-64">
                {"使用服务器本地定位库；IP 不会发送到第三方。城市仅表示网络出口估算。"}
            </TooltipContent>
          </Tooltip>
        ) : null}
        {entry.status === "loading" ? (
          <span className="inline-flex items-center gap-1">
            <RefreshCw className="size-3 animate-spin" />
            {"转换中"}
          </span>
        ) : null}
        {entry.status === "success" && presentation ? (
          <div className="flex min-w-0 items-center gap-1">
            <Popover>
              <PopoverTrigger asChild>
                <Button
                  type="button"
                  variant="ghost"
                  size="sm"
                  className="h-5 min-w-0 max-w-full gap-1 px-0 text-xs font-normal text-muted-foreground hover:bg-transparent hover:text-foreground"
                  aria-label={`查看 ${displayIP} 的地理位置详情`}
                  title="查看完整位置详情"
                >
                  <MapPin className="size-3 shrink-0" />
                  <span className="truncate">{presentation.label}</span>
                </Button>
              </PopoverTrigger>
              <PopoverContent aria-labelledby={detailTitleID} align="start" side="right" className="max-h-[min(32rem,calc(100vh-2rem))] w-80 max-w-[calc(100vw-2rem)] overflow-x-hidden overflow-y-auto p-0">
                <div className="flex items-center gap-2 border-b border-border bg-muted/20 px-3 py-2.5">
                  <MapPin className="size-4 text-primary" />
                  <div id={detailTitleID} className="min-w-0 text-sm font-semibold">{"IP 网络位置"}</div>
                </div>
                <div className="divide-y divide-border px-3">
                  <GeoDetailLine label="IP 地址" value={displayIP} mono />
                  <GeoDetailLine label="记录来源" value={presentation.sourceLabel} hint={presentation.sourceHint} />
                  <GeoDetailLine label="定位精度" value={presentation.precisionLabel} hint={presentation.precisionHint} />
                  <GeoDetailLine label="定位数据源" value={entry.detail.provider === "local_maxmind" ? "本地 MaxMind 兼容库" : "本地定位库"} />
                  <GeoDetailLine label="国家 / 地区" value={formatCountry(entry.detail.country, entry.detail.countryCode)} />
                  <GeoDetailLine label="省州" value={entry.detail.region} />
                  <GeoDetailLine label="城市（估算）" value={presentation.city} hint={presentation.city ? "城市级结果来自 IP 注册数据，不代表精确位置。" : undefined} />
                  {presentation.showPostal ? <GeoDetailLine label="邮编" value={entry.detail.postal} /> : null}
                  {presentation.showCoordinates ? <GeoDetailLine label="坐标" value={formatCoordinates(entry.detail.latitude, entry.detail.longitude)} mono /> : null}
                  <GeoDetailLine label="精度半径" value={formatAccuracyRadius(entry.detail.accuracyRadiusKM)} />
                  <GeoDetailLine label="ASN" value={entry.detail.asn} mono />
                  <GeoDetailLine label="网络 / 运营商" value={entry.networkName || formatNetwork(entry.detail.organization)} />
                  <GeoDetailLine label="时区" value={entry.detail.timezone} mono />
                </div>
                <div className="border-t border-border bg-muted/10 px-3 py-2 text-[11px] text-muted-foreground">{"本地 IP 注册数据；位置仅表示网络出口。"}</div>
              </PopoverContent>
            </Popover>
            <Tooltip>
              <TooltipTrigger asChild>
                <Button
                  type="button"
                  variant="ghost"
                  size="icon-sm"
                  className="size-5 shrink-0"
                  onClick={() => void lookupGatewayClientIp(requestLogID, displayIP, true)}
                  aria-label="重新转换 IP 地理位置"
                >
                  <RefreshCw className="size-3" />
                </Button>
              </TooltipTrigger>
              <TooltipContent>{"重新转换"}</TooltipContent>
            </Tooltip>
          </div>
        ) : null}
        {entry.status === "error" ? (
          <Tooltip>
            <TooltipTrigger asChild>
              <Button
                type="button"
                variant="ghost"
                size="sm"
                className="h-5 gap-1 px-0 text-xs text-destructive hover:bg-transparent hover:text-destructive"
                onClick={() => void lookupGatewayClientIp(requestLogID, displayIP, true)}
              >
                <MapPin className="size-3" />
                {"转换失败"}
              </Button>
            </TooltipTrigger>
            <TooltipContent>{"重新查询本地定位库"}</TooltipContent>
          </Tooltip>
        ) : null}
        {entry.status === "unavailable" ? (
          <Tooltip>
            <TooltipTrigger asChild>
              <span className="inline-flex items-center gap-1"><MapPin className="size-3" />{"本地定位未配置"}</span>
            </TooltipTrigger>
            <TooltipContent className="max-w-64">{"管理员尚未配置本地 City / ASN 定位数据库；系统不会回退到第三方查询。"}</TooltipContent>
          </Tooltip>
        ) : null}
        {entry.status === "not_found" ? (
          <Tooltip>
            <TooltipTrigger asChild>
              <span className="inline-flex items-center gap-1"><MapPin className="size-3" />{"本地库未收录"}</span>
            </TooltipTrigger>
            <TooltipContent className="max-w-64">{"本地定位库没有该地址的数据，系统不会请求外部服务。"}</TooltipContent>
          </Tooltip>
        ) : null}
        {entry.status === "private" ? <span>{"非公网地址"}</span> : null}
        {entry.status === "invalid" ? <span>{"无法识别 IP"}</span> : null}
      </div>
    </div>
  )
})

function GeoDetailLine({ label, value, mono = false, hint }: { label: string; value?: string; mono?: boolean; hint?: string }) {
  if (!value) return null
  return (
    <div className="grid grid-cols-[5.5rem_minmax(0,1fr)] gap-3 py-2 text-xs">
      <span className="text-muted-foreground" title={hint}>{label}</span>
      <span className={`min-w-0 break-words text-right text-foreground ${mono ? "font-mono" : ""}`} title={value}>{value}</span>
    </div>
  )
}

function formatCountry(country?: string, countryCode?: string) {
  if (!country) return countryCode
  return countryCode ? `${country} (${countryCode})` : country
}

function formatCoordinates(latitude?: number, longitude?: number) {
  return typeof latitude === "number" && typeof longitude === "number" ? `${latitude}, ${longitude}` : undefined
}

function formatAccuracyRadius(radius?: number) {
	return typeof radius === "number" && radius > 0 ? `约 ±${radius} km` : undefined
}

function formatNetwork(organization?: string) {
  const seen = new Set<string>()
	return [organization].filter((part): part is string => {
    if (!part) return false
    const normalized = part.toLowerCase()
    if (seen.has(normalized)) return false
    seen.add(normalized)
    return true
  }).join(" · ")
}
