"use client"

import { useMemo, useState, type FormEvent } from "react"
import {
	CircleDollarSign,
	CloudDownload,
	Pencil,
	Plus,
	RefreshCw,
	RotateCcw,
	Search,
	TriangleAlert,
} from "lucide-react"
import { toast } from "sonner"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
	Dialog,
	DialogContent,
	DialogBody,
  DialogError,
	DialogDescription,
	DialogFooter,
	DialogHeader,
	DialogTitle,
} from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Skeleton } from "@/components/ui/skeleton"
import { Switch } from "@/components/ui/switch"
import {
	Table,
	TableBody,
	TableCell,
	TableHead,
	TableHeader,
	TableRow,
} from "@/components/ui/table"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { apiFetch, type ApiError } from "@/lib/api"
import type {
	GatewayModelPrice,
	GatewayModelPriceInput,
	GatewayModelPriceSyncResult,
	GatewayUsedModel,
} from "@/lib/api-types"
import { compactCount, compactTokenCount, dateTime, relativeTime } from "@/lib/format"
import {
	emptyGatewayModelPriceDraft,
	formatGatewayModelPrice,
	gatewayModelPriceDraft,
	gatewayModelPriceErrorMessage,
	gatewayModelPriceInput,
	gatewayModelPriceLongContextThreshold,
	gatewayModelPricePatch,
	parseGatewayModelPriceDraft,
	sortGatewayModelPrices,
	sortGatewayUsedModels,
	upsertGatewayModelPrice,
	type GatewayModelPriceDraft,
	type GatewayModelPriceField,
} from "@/lib/gateway-model-prices"
import { useGatewayModelPrices, useGatewayUsedModels } from "@/lib/queries"
import { cn } from "@/lib/utils"

type ModelPriceDialogMode =
	| { kind: "create"; modelName?: string }
	| { kind: "edit"; id: number }

type LongContextPriceField =
	| "long_context_input_unit_price_usd"
	| "long_context_output_unit_price_usd"
	| "long_context_cache_read_unit_price_usd"
	| "long_context_cache_write_unit_price_usd"

const priceFields: Array<{ field: GatewayModelPriceField; label: string; required: boolean }> = [
	{ field: "input_unit_price_usd", label: "输入价格 (USD/M)", required: true },
	{ field: "output_unit_price_usd", label: "输出价格 (USD/M)", required: true },
	{ field: "cache_read_unit_price_usd", label: "缓存读取价格 (USD/M)", required: false },
	{ field: "cache_write_unit_price_usd", label: "缓存写入价格 (USD/M)", required: false },
]

const longContextPriceFields: Array<{ field: LongContextPriceField; label: string; required: boolean }> = [
	{ field: "long_context_input_unit_price_usd", label: "长上下文输入价格 (USD/M)", required: true },
	{ field: "long_context_output_unit_price_usd", label: "长上下文输出价格 (USD/M)", required: true },
	{ field: "long_context_cache_read_unit_price_usd", label: "长上下文缓存读取价格 (USD/M，可选)", required: false },
	{ field: "long_context_cache_write_unit_price_usd", label: "长上下文缓存写入价格 (USD/M，可选)", required: false },
]

export function ModelPricesPanel() {
	const prices = useGatewayModelPrices()
	const usedModels = useGatewayUsedModels()
	const [view, setView] = useState<"used" | "configured">("used")
	const [search, setSearch] = useState("")
  const [page, setPage] = useState(1)
	const [dialogMode, setDialogMode] = useState<ModelPriceDialogMode | null>(null)
	const [savingIDs, setSavingIDs] = useState<Set<number>>(() => new Set())
	const [syncing, setSyncing] = useState(false)

	const allItems = useMemo(() => sortGatewayModelPrices(prices.data ?? []), [prices.data])
	const allUsedModels = useMemo(() => sortGatewayUsedModels(usedModels.data ?? []), [usedModels.data])
	const query = search.trim().toLocaleLowerCase()
	const items = useMemo(() => query
		? allItems.filter((item) => item.model_name.toLocaleLowerCase().includes(query))
		: allItems, [allItems, query])
	const visibleUsedModels = useMemo(() => query
		? allUsedModels.filter((item) => item.model_name.toLocaleLowerCase().includes(query))
		: allUsedModels, [allUsedModels, query])
	const editingPrice = dialogMode?.kind === "edit"
		? allItems.find((item) => item.id === dialogMode.id) ?? null
		: null

	function storeSavedPrice(price: GatewayModelPrice) {
		prices.setData((current) => upsertGatewayModelPrice(current ?? [], price))
		usedModels.refetch()
		setDialogMode(null)
	}

	async function updatePriceFlags(
		price: GatewayModelPrice,
		overrides: Partial<Pick<GatewayModelPriceInput, "enabled" | "auto_update">>,
		successMessage: string,
	) {
		if (savingIDs.has(price.id)) return
		setSavingIDs((current) => new Set(current).add(price.id))
		try {
			const saved = await apiFetch<GatewayModelPrice>(`/gateway/model-prices/${price.id}`, {
				method: "PATCH",
				body: JSON.stringify(gatewayModelPricePatch(gatewayModelPriceInput(price, overrides), price.revision)),
			})
			prices.setData((current) => upsertGatewayModelPrice(current ?? [], saved))
			usedModels.refetch()
			toast.success(successMessage)
		} catch (cause) {
			const status = (cause as Partial<ApiError> | null)?.status
			if (status === 409) {
				toast.error(`${price.model_name} 更新冲突，未覆盖其他修改；列表已刷新`)
				prices.refetch()
				usedModels.refetch()
			} else if (status === 404) {
				toast.error(`${price.model_name} 已不存在；列表已刷新`)
				prices.refetch()
				usedModels.refetch()
			} else {
				toast.error(gatewayModelPriceErrorMessage(cause, "更新模型价格失败"))
			}
		} finally {
			setSavingIDs((current) => {
				const next = new Set(current)
				next.delete(price.id)
				return next
			})
		}
	}

	async function syncPrices() {
		if (syncing) return
		setSyncing(true)
		try {
			const result = await apiFetch<GatewayModelPriceSyncResult>("/gateway/model-prices/sync", { method: "POST" })
			prices.refetch()
			usedModels.refetch()
			const unsupported = result.unsupported_models?.length ?? 0
			toast.success(`价格同步完成：新增 ${result.applied.created}，更新 ${result.applied.updated}，未匹配 ${unsupported}`)
		} catch (cause) {
			toast.error(gatewayModelPriceErrorMessage(cause, "自动价格同步失败"))
		} finally {
			setSyncing(false)
		}
	}

	function refreshAll() {
		prices.refetch()
		usedModels.refetch()
	}

	const enabledCount = allItems.filter((item) => item.enabled).length
	const missingCount = allUsedModels.filter((item) => item.price == null && (item.channel_automatic_price_rows ?? 0) <= 0).length
	const refreshing = prices.loading || prices.refreshing || usedModels.loading || usedModels.refreshing

  const total = view === "used" ? visibleUsedModels.length : items.length
  const pageSize = 12
  const pageCount = Math.max(1, Math.ceil(total / pageSize))
  const currentPage = Math.min(page, pageCount)
  const offset = (currentPage - 1) * pageSize

	return (
		<section
			id="model-prices"
			className="scroll-mt-20 overflow-hidden rounded-lg border border-border bg-background"
			title="按精确模型名维护未乘渠道分组倍率的基础价格，单位为 USD/百万 Token"
		>
			<div className="flex min-h-12 flex-wrap items-center justify-between gap-3 border-b border-border bg-muted/20 px-4 py-3">
				<div className="flex min-w-0 flex-wrap items-center gap-2">
					<CircleDollarSign className="size-4 shrink-0 text-brand" aria-hidden="true" />
					<h2 className="truncate text-lg font-semibold">模型基础价格 <span className="text-xs font-normal text-muted-foreground">USD / 百万 Token</span></h2>
					<Badge variant="outline" className="tabular-nums">{`${enabledCount}/${allItems.length} 启用`}</Badge>
					{missingCount > 0 ? <Badge variant="outline" className="border-warning/30 bg-warning/5 text-warning tabular-nums">{`${missingCount} 缺价`}</Badge> : null}
				</div>
				<div className="flex flex-wrap items-center gap-2">
					<Button type="button" size="sm" variant="outline" onClick={() => void syncPrices()} disabled={syncing}>
						<CloudDownload className={cn("size-3.5", syncing && "animate-pulse")} aria-hidden="true" />
						{syncing ? "同步中" : "同步价格"}
					</Button>
					<Button type="button" size="sm" onClick={() => setDialogMode({ kind: "create" })}>
						<Plus className="size-3.5" aria-hidden="true" />
						新建价格
					</Button>
					<Button
						type="button"
						size="icon-sm"
						variant="outline"
						onClick={refreshAll}
						disabled={refreshing}
						aria-label="刷新模型价格"
						title="刷新模型价格"
					>
						<RefreshCw className={cn("size-3.5", refreshing && "animate-spin")} />
					</Button>
				</div>
			</div>

			<Tabs value={view} onValueChange={(value) => { setView(value as "used" | "configured"); setPage(1) }} className="gap-0">
				<div className="flex flex-wrap items-center justify-between gap-3 border-b border-border bg-muted/10 p-3">
					<TabsList>
						<TabsTrigger value="used">{`常用模型 ${allUsedModels.length}`}</TabsTrigger>
						<TabsTrigger value="configured">{`价格配置 ${allItems.length}`}</TabsTrigger>
					</TabsList>
					<div className="relative w-full sm:w-80">
						<Search className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" aria-hidden="true" />
						<Input
							value={search}
							onChange={(event) => { setSearch(event.target.value); setPage(1) }}
							placeholder="搜索精确模型名"
							className="pl-9"
							aria-label="搜索模型价格"
						/>
					</div>
				</div>

				<TabsContent value="used" className="m-0 max-h-[max(18rem,calc(100dvh-22rem))] overflow-auto">
					<QueryAlert error={usedModels.error} retry={usedModels.refetch} refreshing={usedModels.refreshing} fallback="读取常用模型失败" />
					{usedModels.loading && usedModels.data == null ? (
						<ModelPriceTableSkeleton columns={5} minWidth="min-w-[900px]" />
					) : usedModels.data == null ? (
						<ModelPriceEmptyState text="暂时无法读取常用模型" />
					) : visibleUsedModels.length === 0 ? (
						<ModelPriceEmptyState text={allUsedModels.length === 0 ? "近 30 天暂无使用或发现的模型" : "没有匹配的模型"} />
					) : (
						<UsedModelTable
							items={visibleUsedModels.slice(offset, offset + pageSize)}
							onCreate={(modelName) => setDialogMode({ kind: "create", modelName })}
							onEdit={(price) => setDialogMode({ kind: "edit", id: price.id })}
						/>
					)}
				</TabsContent>

				<TabsContent value="configured" className="m-0 max-h-[max(18rem,calc(100dvh-22rem))] overflow-auto">
					<QueryAlert error={prices.error} retry={prices.refetch} refreshing={prices.refreshing} fallback="读取模型价格失败" />
					{prices.loading && prices.data == null ? (
						<ModelPriceTableSkeleton columns={9} minWidth="min-w-[960px]" />
					) : prices.data == null ? (
						<ModelPriceEmptyState text="暂时无法读取模型价格" />
					) : items.length === 0 ? (
						<ModelPriceEmptyState text={allItems.length === 0 ? "暂无模型价格" : "没有匹配的模型价格"} />
					) : (
						<ModelPriceTable
							items={items.slice(offset, offset + pageSize)}
							savingIDs={savingIDs}
							onEdit={(price) => setDialogMode({ kind: "edit", id: price.id })}
							onToggleEnabled={(price, enabled) => void updatePriceFlags(price, { enabled }, `${price.model_name} 已${enabled ? "启用" : "停用"}`)}
							onToggleAutoUpdate={(price, autoUpdate) => void updatePriceFlags(price, { auto_update: autoUpdate }, `${price.model_name} 已${autoUpdate ? "允许自动维护" : "改为人工维护"}`)}
						/>
					)}
				</TabsContent>
			</Tabs>
      <div className="flex items-center justify-between border-t border-border px-4 py-3 text-xs text-muted-foreground">
        <span role="status">{total ? `${offset + 1}–${Math.min(offset + pageSize, total)} / ${total} 个模型` : "0 个模型"}</span>
        <div className="flex items-center gap-2">
          <Button variant="outline" size="sm" onClick={() => setPage(currentPage - 1)} disabled={currentPage <= 1}>上一页</Button>
          <span className="tabular-nums">{currentPage} / {pageCount}</span>
          <Button variant="outline" size="sm" onClick={() => setPage(currentPage + 1)} disabled={currentPage >= pageCount}>下一页</Button>
        </div>
      </div>

			{dialogMode ? (
				<ModelPriceFormDialog
					key={dialogMode.kind === "edit" ? `edit-${dialogMode.id}` : `create-${dialogMode.modelName ?? "new"}`}
					mode={dialogMode.kind}
					initialModelName={dialogMode.kind === "create" ? dialogMode.modelName : undefined}
					price={editingPrice}
					refreshing={prices.refreshing}
					onClose={() => setDialogMode(null)}
					onSaved={storeSavedPrice}
					onConflict={refreshAll}
				/>
			) : null}
		</section>
	)
}

function UsedModelTable({
	items,
	onCreate,
	onEdit,
}: {
	items: GatewayUsedModel[]
	onCreate: (modelName: string) => void
	onEdit: (price: GatewayModelPrice) => void
}) {
	return (
		<Table noScroll className="min-w-[900px] table-fixed">
			<TableHeader className="sticky top-0 z-10 bg-card">
				<TableRow>
					<TableHead className="w-[30%] text-left">精确模型名</TableHead>
					<TableHead className="w-[18%] text-center">近 30 天使用</TableHead>
					<TableHead className="w-[18%] text-center">模型发现</TableHead>
						<TableHead className="w-[22%] text-center">生效价格</TableHead>
					<TableHead className="w-[12%] text-center">操作</TableHead>
				</TableRow>
			</TableHeader>
			<TableBody>
				{items.map((item) => (
					<TableRow key={item.model_name} className="h-16">
						<TableCell>
							<div className="truncate font-mono font-semibold" title={item.model_name}>{item.model_name}</div>
						</TableCell>
						<TableCell className="text-center">
							<div className="font-mono tabular-nums" title={item.request_count.toLocaleString("en-US")}>{compactCount(item.request_count)}</div>
							<div className="mt-1 text-xs text-muted-foreground" title={dateTime(item.last_used_at)}>{item.last_used_at ? relativeTime(item.last_used_at) : "无近期请求"}</div>
						</TableCell>
						<TableCell className="text-center">
							<Badge variant="outline" className={item.upstream_discovered ? "border-brand/25 bg-brand/5 text-brand" : "border-border bg-muted/30 text-muted-foreground"}>
								{item.upstream_discovered ? "上游已发现" : "仅使用记录"}
							</Badge>
							{item.last_discovered_at ? <div className="mt-1 text-xs text-muted-foreground" title={dateTime(item.last_discovered_at)}>{relativeTime(item.last_discovered_at)}</div> : null}
						</TableCell>
						<TableCell className="text-center">
							{(item.channel_automatic_price_rows ?? 0) > 0 ? (
								<div className="space-y-1">
									<div className="flex items-center justify-center gap-1.5">
										<Badge variant="outline" className="border-success/25 bg-success/5 text-success">{`渠道自动 ${item.channel_automatic_price_rows} 组`}</Badge>
										{item.price?.enabled ? <Badge variant="outline">全局兜底</Badge> : null}
									</div>
									<div className="text-xs text-muted-foreground" title={dateTime(item.channel_price_observed_at)}>
										{item.channel_price_observed_at ? `同步 ${relativeTime(item.channel_price_observed_at)}` : "渠道同步"}
									</div>
								</div>
							) : item.price ? (
								<div className="space-y-1">
									<div className="flex items-center justify-center gap-1.5">
										<Badge variant="outline" className={item.price.enabled ? "border-brand/25 bg-brand/5 text-brand" : "border-border bg-muted/30 text-muted-foreground"}>{item.price.enabled ? "全局兜底" : "已停用"}</Badge>
										<Badge variant="outline">{modelPriceSourceLabel(item.price)}</Badge>
									</div>
									<div className="text-xs text-muted-foreground">{item.price.auto_update ? "自动维护" : "人工维护"}</div>
								</div>
							) : (
								<Badge variant="outline" className="border-warning/30 bg-warning/5 text-warning">缺少价格</Badge>
							)}
						</TableCell>
						<TableCell className="text-center">
							{item.price ? (
								<Button type="button" size="icon-sm" variant="outline" onClick={() => onEdit(item.price!)} aria-label={`编辑 ${item.model_name} 模型价格`} title="编辑模型价格">
									<Pencil className="size-3.5" />
								</Button>
							) : (
								<Button type="button" size="sm" variant="outline" onClick={() => onCreate(item.model_name)}>
									<Plus className="size-3.5" aria-hidden="true" />
									填写价格
								</Button>
							)}
						</TableCell>
					</TableRow>
				))}
			</TableBody>
		</Table>
	)
}

function ModelPriceTable({
	items,
	savingIDs,
	onEdit,
	onToggleEnabled,
	onToggleAutoUpdate,
}: {
	items: GatewayModelPrice[]
	savingIDs: Set<number>
	onEdit: (price: GatewayModelPrice) => void
	onToggleEnabled: (price: GatewayModelPrice, enabled: boolean) => void
	onToggleAutoUpdate: (price: GatewayModelPrice, autoUpdate: boolean) => void
}) {
	return (
		<Table noScroll className="min-w-[960px] table-fixed">
			<TableHeader className="sticky top-0 z-10 bg-card">
				<TableRow>
					<TableHead className="w-[20%] text-left">模型与来源</TableHead>
					<TableHead className="w-[10%] text-center">输入</TableHead>
					<TableHead className="w-[10%] text-center">输出</TableHead>
					<TableHead className="w-[10%] text-center">缓存读</TableHead>
					<TableHead className="w-[10%] text-center">缓存写</TableHead>
					<TableHead className="w-[14%] text-center">长上下文阶梯</TableHead>
					<TableHead className="w-[12%] text-center">价格维护</TableHead>
					<TableHead className="w-[8%] text-center">启用</TableHead>
					<TableHead className="w-[6%] text-center">操作</TableHead>
				</TableRow>
			</TableHeader>
			<TableBody>
				{items.map((price) => {
					const saving = savingIDs.has(price.id)
					return (
						<TableRow key={price.id} className="h-[4.5rem]">
							<TableCell>
								<div className="truncate font-mono font-semibold" title={price.model_name}>{price.model_name}</div>
								<div className="mt-1 flex items-center gap-1.5 text-xs text-muted-foreground">
									<Badge variant="outline" className="h-5 px-1.5 text-[10px]">{modelPriceSourceLabel(price)}</Badge>
									<span className="truncate" title={price.source_observed_at ? dateTime(price.source_observed_at) : `版本 ${price.revision}`}>
										{price.source_observed_at ? `同步 ${relativeTime(price.source_observed_at)}` : `版本 ${price.revision}`}
									</span>
								</div>
							</TableCell>
							{priceFields.map(({ field }) => (
								<TableCell key={field} className="text-center font-mono tabular-nums" title={formatGatewayModelPrice(price[field])}>
									<div className="truncate">{formatGatewayModelPrice(price[field])}</div>
								</TableCell>
							))}
							<TableCell className="text-center">
								<LongContextPriceSummary price={price} />
							</TableCell>
							<TableCell className="text-center">
								<div className="inline-flex items-center gap-2">
									<span className="text-xs text-muted-foreground">{price.auto_update ? "自动" : "人工"}</span>
									{saving ? <RefreshCw className="size-3.5 animate-spin text-muted-foreground" /> : (
										<Switch checked={price.auto_update} onCheckedChange={(enabled) => onToggleAutoUpdate(price, enabled)} aria-label={`${price.auto_update ? "关闭" : "开启"} ${price.model_name} 自动价格维护`} />
									)}
								</div>
							</TableCell>
							<TableCell className="text-center">
								{saving ? <RefreshCw className="mx-auto size-3.5 animate-spin text-muted-foreground" /> : (
									<Switch checked={price.enabled} onCheckedChange={(enabled) => onToggleEnabled(price, enabled)} aria-label={`${price.enabled ? "停用" : "启用"} ${price.model_name} 模型价格`} />
								)}
							</TableCell>
							<TableCell className="text-center">
								<Button type="button" size="icon-sm" variant="outline" onClick={() => onEdit(price)} disabled={saving} aria-label={`编辑 ${price.model_name} 模型价格`} title="编辑模型价格">
									<Pencil className="size-3.5" />
								</Button>
							</TableCell>
						</TableRow>
					)
				})}
			</TableBody>
		</Table>
	)
}

function LongContextPriceSummary({ price }: { price: GatewayModelPrice }) {
	if (price.long_context_threshold_tokens == null) {
		return <span className="text-xs text-muted-foreground">未配置</span>
	}
	const detail = [
		`输入 ${formatGatewayModelPrice(price.long_context_input_unit_price_usd ?? Number.NaN)}`,
		`输出 ${formatGatewayModelPrice(price.long_context_output_unit_price_usd ?? Number.NaN)}`,
		`缓存读 ${formatGatewayModelPrice(price.long_context_cache_read_unit_price_usd ?? Number.NaN)}`,
		`缓存写 ${formatGatewayModelPrice(price.long_context_cache_write_unit_price_usd ?? Number.NaN)}`,
	].join(" · ")
	return (
		<div title={`超过 ${price.long_context_threshold_tokens.toLocaleString("en-US")} Token · ${detail}`}>
			<Badge variant="outline" className="tabular-nums">{`> ${compactTokenCount(price.long_context_threshold_tokens)}`}</Badge>
			<div className="mt-1 truncate text-xs text-muted-foreground">阶梯价</div>
		</div>
	)
}

function modelPriceSourceLabel(price: GatewayModelPrice) {
	switch (price.source.trim().toLowerCase()) {
	case "litellm":
		return "LiteLLM"
	case "manual":
		return "人工"
	default:
		return "其他来源"
	}
}

function ModelPriceTableSkeleton({ columns, minWidth }: { columns: number; minWidth: string }) {
	return (
		<Table noScroll className={cn(minWidth, "table-fixed")} aria-label="正在读取模型价格">
			<TableBody>
				{Array.from({ length: 3 }, (_, index) => (
					<TableRow key={index} className="h-16">
						{Array.from({ length: columns }, (__, cellIndex) => (
							<TableCell key={cellIndex}><Skeleton className="mx-auto h-4 w-16" /></TableCell>
						))}
					</TableRow>
				))}
			</TableBody>
		</Table>
	)
}

function QueryAlert({ error, retry, refreshing, fallback }: { error: string | null; retry: () => void; refreshing: boolean; fallback: string }) {
	if (!error) return null
	return (
		<div className="flex min-h-12 flex-wrap items-center justify-between gap-2 border-b border-danger/20 bg-danger/5 px-4 py-2 text-sm text-danger" role="alert">
			<span>{gatewayModelPriceErrorMessage(new Error(error), fallback)}</span>
			<Button type="button" size="sm" variant="outline" onClick={retry} disabled={refreshing}>重试</Button>
		</div>
	)
}

function ModelPriceEmptyState({ text }: { text: string }) {
	return (
		<div className="flex min-h-40 flex-col items-center justify-center gap-2 p-6 text-center text-sm text-muted-foreground">
			<CircleDollarSign className="size-5" aria-hidden="true" />
			<span>{text}</span>
		</div>
	)
}

function ModelPriceFormDialog({
	mode,
	initialModelName,
	price,
	refreshing,
	onClose,
	onSaved,
	onConflict,
}: {
	mode: "create" | "edit"
	initialModelName?: string
	price: GatewayModelPrice | null
	refreshing: boolean
	onClose: () => void
	onSaved: (price: GatewayModelPrice) => void
	onConflict: () => void
}) {
	const isEdit = mode === "edit"
	const [draft, setDraft] = useState<GatewayModelPriceDraft>(() => price ? gatewayModelPriceDraft(price) : emptyGatewayModelPriceDraft(initialModelName))
	const [expectedRevision, setExpectedRevision] = useState(price?.revision ?? 0)
	const [saving, setSaving] = useState(false)
	const [error, setError] = useState<string | null>(null)
	const [errorField, setErrorField] = useState<keyof GatewayModelPriceDraft | null>(null)
	const [conflict, setConflict] = useState(false)
	const latestRevisionAvailable = isEdit && price != null && price.revision !== expectedRevision

	function updateDraft<K extends keyof GatewayModelPriceDraft>(field: K, value: GatewayModelPriceDraft[K]) {
		setDraft((current) => ({ ...current, [field]: value }))
		if (errorField === field) setErrorField(null)
		setError(null)
	}

	function loadLatestVersion() {
		if (!price) return
		setDraft(gatewayModelPriceDraft(price))
		setExpectedRevision(price.revision)
		setConflict(false)
		setError(null)
		setErrorField(null)
	}

	async function save(event: FormEvent<HTMLFormElement>) {
		event.preventDefault()
		const parsed = parseGatewayModelPriceDraft(draft)
		if (!parsed.ok) {
			setError(parsed.message)
			setErrorField(parsed.field)
			return
		}
		if (isEdit && (!price || expectedRevision <= 0)) {
			setError("当前模型价格已不存在，请关闭后刷新列表")
			return
		}

		setSaving(true)
		setError(null)
		setErrorField(null)
		try {
			const saved = await apiFetch<GatewayModelPrice>(isEdit ? `/gateway/model-prices/${price!.id}` : "/gateway/model-prices", {
				method: isEdit ? "PATCH" : "POST",
				body: JSON.stringify(isEdit ? gatewayModelPricePatch(parsed.value, expectedRevision) : parsed.value),
			})
			toast.success(isEdit ? `${saved.model_name} 价格已更新` : `${saved.model_name} 价格已创建`)
			onSaved(saved)
		} catch (cause) {
			const status = (cause as Partial<ApiError> | null)?.status
			if (status === 409) {
				setConflict(true)
				onConflict()
			} else if (status === 404 && isEdit) {
				setError("当前模型价格已不存在，列表已刷新")
				onConflict()
			} else {
				setError(gatewayModelPriceErrorMessage(cause, isEdit ? "更新模型价格失败" : "创建模型价格失败"))
			}
		} finally {
			setSaving(false)
		}
	}

	return (
		<Dialog open busy={saving} onOpenChange={(open) => { if (!open && !saving) onClose() }}>
			<DialogContent formLayout className="sm:max-w-3xl">
				<DialogHeader className="pr-8">
					<DialogTitle>{isEdit ? `${price?.model_name ?? "模型价格"} · 编辑价格` : `${initialModelName ?? "新建模型"} · 填写价格`}</DialogTitle>
					<DialogDescription className="sr-only">维护精确模型名的标准上下文与长上下文基础价格</DialogDescription>
				</DialogHeader>

				{isEdit && !price ? (
					<div className="rounded-md border border-danger/25 bg-danger/5 px-3 py-3 text-sm text-danger" role="alert">当前模型价格已不存在，请关闭后刷新列表。</div>
				) : (
					<form onSubmit={save}>
                        <DialogBody className="space-y-4">
						<div className="space-y-1.5">
							<Label htmlFor="gateway-model-price-name">精确模型名</Label>
							<Input
								id="gateway-model-price-name"
								value={draft.model_name}
								onChange={(event) => updateDraft("model_name", event.target.value)}
								disabled={saving}
								aria-invalid={errorField === "model_name" || undefined}
								autoComplete="off"
								required
							/>
						</div>

						<div className="grid gap-3 sm:grid-cols-2">
								{priceFields.map(({ field, label, required }) => (
									<ModelPriceNumberInput key={field} field={field} label={label} draft={draft} saving={saving} errorField={errorField} required={required} onChange={updateDraft} />
							))}
						</div>

						<div className="flex min-h-11 items-center justify-between gap-3 border-t border-border pt-4">
							<Label htmlFor="gateway-model-price-long-context">配置长上下文阶梯价</Label>
							<Switch id="gateway-model-price-long-context" checked={draft.long_context_enabled} onCheckedChange={(enabled) => updateDraft("long_context_enabled", enabled)} disabled={saving} />
						</div>

						{draft.long_context_enabled ? (
							<div className="grid gap-3 sm:grid-cols-2">
								<div className="space-y-1.5 sm:col-span-2">
									<Label htmlFor="gateway-model-price-long-context-threshold">长上下文阈值 (Token)</Label>
									<Input
										id="gateway-model-price-long-context-threshold"
										type="number"
										inputMode="numeric"
											min={gatewayModelPriceLongContextThreshold}
											max={gatewayModelPriceLongContextThreshold}
											step={gatewayModelPriceLongContextThreshold}
											value={draft.long_context_threshold_tokens}
											readOnly
										disabled={saving}
										aria-invalid={errorField === "long_context_threshold_tokens" || undefined}
										required
									/>
								</div>
									{longContextPriceFields.map(({ field, label, required }) => (
										<ModelPriceNumberInput key={field} field={field} label={label} draft={draft} saving={saving} errorField={errorField} required={required} onChange={updateDraft} />
									))}
							</div>
						) : null}

						<div className="grid gap-3 border-y border-border py-3 sm:grid-cols-2">
							<div className="flex min-h-10 items-center justify-between gap-3 sm:border-r sm:border-border sm:pr-3" title="开启后，定时同步可以更新此模型的自动来源价格">
								<Label htmlFor="gateway-model-price-auto-update">允许自动维护</Label>
								<Switch id="gateway-model-price-auto-update" checked={draft.auto_update} onCheckedChange={(enabled) => updateDraft("auto_update", enabled)} disabled={saving} />
							</div>
							<div className="flex min-h-10 items-center justify-between gap-3">
								<Label htmlFor="gateway-model-price-enabled">启用价格</Label>
								<Switch id="gateway-model-price-enabled" checked={draft.enabled} onCheckedChange={(enabled) => updateDraft("enabled", enabled)} disabled={saving} />
							</div>
						</div>

						{conflict ? (
							<div className="flex flex-wrap items-center justify-between gap-3 rounded-md border border-warning/25 bg-warning/5 px-3 py-2.5 text-sm text-warning" role="alert">
								<div className="flex min-w-0 items-start gap-2">
									<TriangleAlert className="mt-0.5 size-4 shrink-0" aria-hidden="true" />
									<span>{isEdit ? "保存冲突：同名模型已存在或此配置已有新版本。草稿未被覆盖，列表已刷新。" : "该精确模型名可能已存在。草稿仍保留，请从列表编辑或修改模型名后重试。"}</span>
								</div>
								{isEdit && (latestRevisionAvailable || refreshing) ? (
									<Button type="button" size="sm" variant="outline" onClick={loadLatestVersion} disabled={!latestRevisionAvailable || refreshing}>
										<RotateCcw className={cn("size-3.5", refreshing && "animate-spin")} />
										{latestRevisionAvailable ? "载入最新版本" : "正在刷新"}
									</Button>
								) : null}
							</div>
						) : null}


						</DialogBody>
          {error ? <DialogError>{error}</DialogError> : null}
                        <DialogFooter>
							<Button type="button" variant="outline" onClick={onClose} disabled={saving}>取消</Button>
							<Button type="submit" disabled={saving}>{saving ? "保存中" : isEdit ? "保存价格" : "创建价格"}</Button>
						</DialogFooter>
					</form>
				)}
			</DialogContent>
		</Dialog>
	)
}

function ModelPriceNumberInput<Field extends GatewayModelPriceField | LongContextPriceField>({
	field,
	label,
	draft,
	saving,
	errorField,
	required,
	onChange,
}: {
	field: Field
	label: string
	draft: GatewayModelPriceDraft
	saving: boolean
	errorField: keyof GatewayModelPriceDraft | null
	required: boolean
	onChange: <Key extends keyof GatewayModelPriceDraft>(field: Key, value: GatewayModelPriceDraft[Key]) => void
}) {
	const id = `gateway-model-price-${field}`
	return (
		<div className="space-y-1.5">
			<Label htmlFor={id}>{label}</Label>
			<Input
				id={id}
				type="number"
				inputMode="decimal"
				min="0"
				step="any"
				value={draft[field]}
				onChange={(event) => onChange(field, event.target.value)}
				disabled={saving}
				aria-invalid={errorField === field || undefined}
				placeholder={required ? undefined : "未知时留空"}
				required={required}
			/>
		</div>
	)
}
