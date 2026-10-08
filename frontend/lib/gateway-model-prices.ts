import type {
	GatewayModelPrice,
	GatewayModelPriceInput,
	GatewayModelPricePatch,
	GatewayUsedModel,
} from "@/lib/api-types"

export const gatewayModelPriceFields = [
	"input_unit_price_usd",
	"output_unit_price_usd",
	"cache_read_unit_price_usd",
	"cache_write_unit_price_usd",
] as const

export type GatewayModelPriceField = (typeof gatewayModelPriceFields)[number]

const requiredGatewayModelPriceFields = [
	"input_unit_price_usd",
	"output_unit_price_usd",
] as const satisfies ReadonlyArray<GatewayModelPriceField>

const optionalGatewayModelPriceFields = [
	"cache_read_unit_price_usd",
	"cache_write_unit_price_usd",
] as const satisfies ReadonlyArray<GatewayModelPriceField>

export type GatewayModelPriceDraft = {
	model_name: string
	input_unit_price_usd: string
	output_unit_price_usd: string
	cache_read_unit_price_usd: string
	cache_write_unit_price_usd: string
	long_context_enabled: boolean
	long_context_threshold_tokens: string
	long_context_input_unit_price_usd: string
	long_context_output_unit_price_usd: string
	long_context_cache_read_unit_price_usd: string
	long_context_cache_write_unit_price_usd: string
	auto_update: boolean
	enabled: boolean
}

export type GatewayModelPriceDraftResult =
	| { ok: true; value: GatewayModelPriceInput }
	| { ok: false; field: keyof GatewayModelPriceDraft; message: string }

export const gatewayModelPriceLongContextThreshold = 272_000

const priceFieldLabels: Record<GatewayModelPriceField, string> = {
	input_unit_price_usd: "输入价格",
	output_unit_price_usd: "输出价格",
	cache_read_unit_price_usd: "缓存读取价格",
	cache_write_unit_price_usd: "缓存写入价格",
}

const longContextPriceFields = [
	"long_context_input_unit_price_usd",
	"long_context_output_unit_price_usd",
	"long_context_cache_read_unit_price_usd",
	"long_context_cache_write_unit_price_usd",
] as const satisfies ReadonlyArray<keyof GatewayModelPriceDraft>

const longContextPriceLabels: Record<(typeof longContextPriceFields)[number], string> = {
	long_context_input_unit_price_usd: "长上下文输入价格",
	long_context_output_unit_price_usd: "长上下文输出价格",
	long_context_cache_read_unit_price_usd: "长上下文缓存读取价格",
	long_context_cache_write_unit_price_usd: "长上下文缓存写入价格",
}

export function emptyGatewayModelPriceDraft(modelName = ""): GatewayModelPriceDraft {
	return {
		model_name: modelName,
		input_unit_price_usd: "",
		output_unit_price_usd: "",
		cache_read_unit_price_usd: "",
		cache_write_unit_price_usd: "",
		long_context_enabled: false,
		long_context_threshold_tokens: String(gatewayModelPriceLongContextThreshold),
		long_context_input_unit_price_usd: "",
		long_context_output_unit_price_usd: "",
		long_context_cache_read_unit_price_usd: "",
		long_context_cache_write_unit_price_usd: "",
		auto_update: false,
		enabled: true,
	}
}

export function gatewayModelPriceDraft(price: GatewayModelPrice): GatewayModelPriceDraft {
	return {
		model_name: price.model_name,
		input_unit_price_usd: String(price.input_unit_price_usd),
		output_unit_price_usd: String(price.output_unit_price_usd),
		cache_read_unit_price_usd: price.cache_read_unit_price_usd == null ? "" : String(price.cache_read_unit_price_usd),
		cache_write_unit_price_usd: price.cache_write_unit_price_usd == null ? "" : String(price.cache_write_unit_price_usd),
		long_context_enabled: price.long_context_threshold_tokens != null,
		long_context_threshold_tokens: price.long_context_threshold_tokens == null ? "" : String(price.long_context_threshold_tokens),
		long_context_input_unit_price_usd: price.long_context_input_unit_price_usd == null ? "" : String(price.long_context_input_unit_price_usd),
		long_context_output_unit_price_usd: price.long_context_output_unit_price_usd == null ? "" : String(price.long_context_output_unit_price_usd),
		long_context_cache_read_unit_price_usd: price.long_context_cache_read_unit_price_usd == null ? "" : String(price.long_context_cache_read_unit_price_usd),
		long_context_cache_write_unit_price_usd: price.long_context_cache_write_unit_price_usd == null ? "" : String(price.long_context_cache_write_unit_price_usd),
		auto_update: price.auto_update,
		enabled: price.enabled,
	}
}

export function parseGatewayModelPriceDraft(draft: GatewayModelPriceDraft): GatewayModelPriceDraftResult {
	const modelName = draft.model_name.trim()
	if (!modelName) return { ok: false, field: "model_name", message: "请输入精确模型名" }
	if ([...modelName].length > 256) return { ok: false, field: "model_name", message: "精确模型名不能超过 256 个字符" }

	const prices: Pick<
		GatewayModelPriceInput,
		"input_unit_price_usd" | "output_unit_price_usd" | "cache_read_unit_price_usd" | "cache_write_unit_price_usd"
	> = {
		input_unit_price_usd: 0,
		output_unit_price_usd: 0,
		cache_read_unit_price_usd: null,
		cache_write_unit_price_usd: null,
	}
	for (const field of requiredGatewayModelPriceFields) {
		if (draft[field].trim() === "") {
			return { ok: false, field, message: `请输入${priceFieldLabels[field]}` }
		}
		const value = Number(draft[field])
		if (!Number.isFinite(value) || value < 0) {
			return { ok: false, field, message: `${priceFieldLabels[field]}必须是有限的非负数` }
		}
		prices[field] = value
	}
	for (const field of optionalGatewayModelPriceFields) {
		const raw = draft[field].trim()
		if (raw === "") {
			prices[field] = null
			continue
		}
		const value = Number(raw)
		if (!Number.isFinite(value) || value < 0) {
			return { ok: false, field, message: `${priceFieldLabels[field]}必须是有限的非负数` }
		}
		prices[field] = value
	}

	const longContext: Partial<GatewayModelPriceInput> = {}
	if (draft.long_context_enabled) {
		const threshold = Number(draft.long_context_threshold_tokens)
		if (!Number.isSafeInteger(threshold) || threshold !== gatewayModelPriceLongContextThreshold) {
			return { ok: false, field: "long_context_threshold_tokens", message: "当前仅支持 272000 Token 长上下文阈值" }
		}
		longContext.long_context_threshold_tokens = threshold
		for (const field of longContextPriceFields) {
			const optionalCache = field === "long_context_cache_read_unit_price_usd" || field === "long_context_cache_write_unit_price_usd"
			if (draft[field].trim() === "") {
				if (optionalCache) {
					longContext[field] = null
					continue
				}
				return { ok: false, field, message: `请输入${longContextPriceLabels[field]}` }
			}
			const value = Number(draft[field])
			if (!Number.isFinite(value) || value < 0) {
				return { ok: false, field, message: `${longContextPriceLabels[field]}必须是有限的非负数` }
			}
			longContext[field] = value
		}
	}

	return {
		ok: true,
		value: {
			model_name: modelName,
			...prices,
			...longContext,
			auto_update: draft.auto_update,
			enabled: draft.enabled,
		},
	}
}

export function gatewayModelPricePatch(
	input: GatewayModelPriceInput,
	expectedRevision: number,
): GatewayModelPricePatch {
	return { ...input, expected_revision: expectedRevision }
}

export function gatewayModelPriceInput(
	price: GatewayModelPrice,
	overrides: Partial<Pick<GatewayModelPriceInput, "enabled" | "auto_update">> = {},
): GatewayModelPriceInput {
	const input: GatewayModelPriceInput = {
		model_name: price.model_name,
		input_unit_price_usd: price.input_unit_price_usd,
		output_unit_price_usd: price.output_unit_price_usd,
		cache_read_unit_price_usd: price.cache_read_unit_price_usd,
		cache_write_unit_price_usd: price.cache_write_unit_price_usd,
		auto_update: overrides.auto_update ?? price.auto_update,
		enabled: overrides.enabled ?? price.enabled,
	}
	if (price.long_context_threshold_tokens != null) {
		input.long_context_threshold_tokens = price.long_context_threshold_tokens
		input.long_context_input_unit_price_usd = price.long_context_input_unit_price_usd
		input.long_context_output_unit_price_usd = price.long_context_output_unit_price_usd
		input.long_context_cache_read_unit_price_usd = price.long_context_cache_read_unit_price_usd
		input.long_context_cache_write_unit_price_usd = price.long_context_cache_write_unit_price_usd
	}
	return input
}

export function upsertGatewayModelPrice(
	items: GatewayModelPrice[],
	next: GatewayModelPrice,
): GatewayModelPrice[] {
	const index = items.findIndex((item) => item.id === next.id)
	const updated = index < 0
		? [...items, next]
		: items.map((item, itemIndex) => itemIndex === index ? next : item)
	return sortGatewayModelPrices(updated)
}

export function sortGatewayModelPrices(items: GatewayModelPrice[]): GatewayModelPrice[] {
	return [...items].sort((left, right) => (
		Number(right.enabled) - Number(left.enabled)
		|| left.model_name.localeCompare(right.model_name, undefined, { numeric: true })
		|| left.id - right.id
	))
}

export function sortGatewayUsedModels(items: GatewayUsedModel[]): GatewayUsedModel[] {
	return [...items].sort((left, right) => (
		Number(left.price != null || (left.channel_automatic_price_rows ?? 0) > 0)
			- Number(right.price != null || (right.channel_automatic_price_rows ?? 0) > 0)
		|| right.request_count - left.request_count
		|| left.model_name.localeCompare(right.model_name, undefined, { numeric: true })
	))
}

export function formatGatewayModelPrice(value: number | null | undefined): string {
	if (typeof value !== "number" || !Number.isFinite(value)) return "-"
	return new Intl.NumberFormat("en-US", {
		maximumSignificantDigits: 15,
		useGrouping: true,
	}).format(value)
}

export function gatewayModelPriceErrorMessage(error: unknown, fallback: string): string {
	const message = error instanceof Error ? error.message.trim() : ""
	if (!message || message.length > 160 || /:\/\//.test(message)) return fallback
	return message
}
