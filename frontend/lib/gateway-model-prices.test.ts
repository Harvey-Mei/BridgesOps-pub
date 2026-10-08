import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import test from "node:test"
import type { GatewayModelPrice } from "@/lib/api-types"
import {
	emptyGatewayModelPriceDraft,
	formatGatewayModelPrice,
	gatewayModelPriceDraft,
	gatewayModelPriceInput,
	gatewayModelPriceLongContextThreshold,
	gatewayModelPricePatch,
	parseGatewayModelPriceDraft,
	sortGatewayUsedModels,
	upsertGatewayModelPrice,
} from "@/lib/gateway-model-prices"

function modelPrice(overrides: Partial<GatewayModelPrice> = {}): GatewayModelPrice {
	return {
		id: 1,
		model_name: "gpt-5.4",
		input_unit_price_usd: 1.25,
		output_unit_price_usd: 10,
		cache_read_unit_price_usd: 0.125,
		cache_write_unit_price_usd: 2.5,
		source: "manual",
		auto_update: false,
		enabled: true,
		revision: 1,
		created_at: "2026-08-30T00:00:00Z",
		updated_at: "2026-08-30T00:00:00Z",
		...overrides,
	}
}

test("模型价格草稿保留模型名大小写并接受零价格", () => {
	const result = parseGatewayModelPriceDraft({
		...emptyGatewayModelPriceDraft(),
		model_name: "  GPT-5.4-mini  ",
		input_unit_price_usd: "0",
		output_unit_price_usd: "2.5",
		cache_read_unit_price_usd: "0.125",
		cache_write_unit_price_usd: "1e-3",
	})

	assert.equal(result.ok, true)
	if (!result.ok) return
		assert.deepEqual(result.value, {
			model_name: "GPT-5.4-mini",
			input_unit_price_usd: 0,
			output_unit_price_usd: 2.5,
			cache_read_unit_price_usd: 0.125,
			cache_write_unit_price_usd: 0.001,
			auto_update: false,
			enabled: true,
		})
})

test("模型价格草稿保留未知缓存价格为 null", () => {
	const result = parseGatewayModelPriceDraft({
		...emptyGatewayModelPriceDraft("gpt-5"),
		input_unit_price_usd: "1.25",
		output_unit_price_usd: "10",
	})
	assert.equal(result.ok, true)
	if (!result.ok) return
	assert.equal(result.value.cache_read_unit_price_usd, null)
	assert.equal(result.value.cache_write_unit_price_usd, null)
	assert.deepEqual(gatewayModelPriceDraft(modelPrice({
		cache_read_unit_price_usd: null,
		cache_write_unit_price_usd: null,
	})), {
		...emptyGatewayModelPriceDraft("gpt-5.4"),
		input_unit_price_usd: "1.25",
		output_unit_price_usd: "10",
		long_context_threshold_tokens: "",
	})
	assert.equal(formatGatewayModelPrice(null), "-")
	assert.equal(formatGatewayModelPrice(undefined), "-")
})

test("模型价格草稿只要求输入输出并拒绝无效数值", () => {
	const base = {
		...emptyGatewayModelPriceDraft(),
		model_name: "gpt-5.4",
		input_unit_price_usd: "1",
		output_unit_price_usd: "2",
		cache_read_unit_price_usd: "0",
		cache_write_unit_price_usd: "3",
	}

	assert.deepEqual(parseGatewayModelPriceDraft({ ...base, model_name: "  " }), {
		ok: false,
		field: "model_name",
		message: "请输入精确模型名",
	})
	for (const field of ["input_unit_price_usd", "output_unit_price_usd"] as const) {
		const result = parseGatewayModelPriceDraft({ ...base, [field]: "" })
		assert.equal(result.ok, false)
		if (!result.ok) assert.equal(result.field, field)
	}
	for (const value of ["-1", "Infinity", "NaN"]) {
		const result = parseGatewayModelPriceDraft({ ...base, cache_write_unit_price_usd: value })
		assert.equal(result.ok, false)
		if (!result.ok) assert.equal(result.field, "cache_write_unit_price_usd")
	}
	const unknownCache = parseGatewayModelPriceDraft({ ...base, cache_read_unit_price_usd: "", cache_write_unit_price_usd: "" })
	assert.equal(unknownCache.ok, true)
	if (unknownCache.ok) {
		assert.equal(unknownCache.value.cache_read_unit_price_usd, null)
		assert.equal(unknownCache.value.cache_write_unit_price_usd, null)
	}
})

test("编辑请求携带当前 revision，成功项按状态和精确模型名更新", () => {
	const input = {
		model_name: "gpt-5.4",
		input_unit_price_usd: 1,
		output_unit_price_usd: 2,
		cache_read_unit_price_usd: 0.5,
		cache_write_unit_price_usd: 1.5,
		auto_update: false,
		enabled: true,
	}
	assert.deepEqual(gatewayModelPricePatch(input, 7), { ...input, expected_revision: 7 })
	assert.deepEqual(
		gatewayModelPricePatch(gatewayModelPriceInput(modelPrice(), { enabled: false, auto_update: true }), 1),
		{
			model_name: "gpt-5.4",
			input_unit_price_usd: 1.25,
			output_unit_price_usd: 10,
			cache_read_unit_price_usd: 0.125,
			cache_write_unit_price_usd: 2.5,
			auto_update: true,
			enabled: false,
			expected_revision: 1,
		},
	)

	const result = upsertGatewayModelPrice([
		modelPrice({ id: 1, model_name: "gpt-4", enabled: false }),
		modelPrice({ id: 2, model_name: "gpt-5", enabled: true }),
	], modelPrice({ id: 1, model_name: "gpt-4.1", enabled: true, revision: 2 }))
	assert.deepEqual(result.map((item) => [item.id, item.revision]), [[1, 2], [2, 1]])
})

test("长上下文阶梯价要求完整阈值和输入输出价格，缓存价格可未知", () => {
	const draft = {
		...emptyGatewayModelPriceDraft("gpt-5.6-sol"),
		input_unit_price_usd: "5",
		output_unit_price_usd: "30",
		cache_read_unit_price_usd: "0.5",
		cache_write_unit_price_usd: "6.25",
		long_context_enabled: true,
		long_context_threshold_tokens: "272000",
		long_context_input_unit_price_usd: "10",
		long_context_output_unit_price_usd: "45",
		long_context_cache_read_unit_price_usd: "1",
		long_context_cache_write_unit_price_usd: "12.5",
		auto_update: true,
	}
	const parsed = parseGatewayModelPriceDraft(draft)
	assert.equal(parsed.ok, true)
	if (!parsed.ok) return
	assert.deepEqual(parsed.value, {
		model_name: "gpt-5.6-sol",
		input_unit_price_usd: 5,
		output_unit_price_usd: 30,
		cache_read_unit_price_usd: 0.5,
		cache_write_unit_price_usd: 6.25,
		long_context_threshold_tokens: 272000,
		long_context_input_unit_price_usd: 10,
		long_context_output_unit_price_usd: 45,
		long_context_cache_read_unit_price_usd: 1,
		long_context_cache_write_unit_price_usd: 12.5,
		auto_update: true,
		enabled: true,
	})

	const unknownCache = parseGatewayModelPriceDraft({
		...draft,
		long_context_cache_read_unit_price_usd: "",
		long_context_cache_write_unit_price_usd: "",
	})
	assert.equal(unknownCache.ok, true)
	if (unknownCache.ok) {
		assert.equal(unknownCache.value.long_context_cache_read_unit_price_usd, null)
		assert.equal(unknownCache.value.long_context_cache_write_unit_price_usd, null)
	}

	const incomplete = parseGatewayModelPriceDraft({ ...draft, long_context_output_unit_price_usd: "" })
	assert.equal(incomplete.ok, false)
	if (!incomplete.ok) assert.equal(incomplete.field, "long_context_output_unit_price_usd")

	const invalidThreshold = parseGatewayModelPriceDraft({ ...draft, long_context_threshold_tokens: "1.5" })
	assert.equal(invalidThreshold.ok, false)
	if (!invalidThreshold.ok) assert.equal(invalidThreshold.field, "long_context_threshold_tokens")

	const unsupportedThreshold = parseGatewayModelPriceDraft({ ...draft, long_context_threshold_tokens: "200000" })
	assert.equal(unsupportedThreshold.ok, false)
	if (!unsupportedThreshold.ok) assert.equal(unsupportedThreshold.field, "long_context_threshold_tokens")
	assert.equal(gatewayModelPriceLongContextThreshold, 272000)
})

test("行级开关请求保留自动来源的长上下文价格", () => {
	const input = gatewayModelPriceInput(modelPrice({
		source: "litellm",
		auto_update: true,
		long_context_threshold_tokens: 272000,
		long_context_input_unit_price_usd: 2,
		long_context_output_unit_price_usd: 20,
		long_context_cache_read_unit_price_usd: 0.2,
		long_context_cache_write_unit_price_usd: 4,
	}), { enabled: false })
	assert.equal(input.long_context_threshold_tokens, 272000)
	assert.equal(input.long_context_cache_write_unit_price_usd, 4)
	assert.equal(input.auto_update, true)
	assert.equal(input.enabled, false)
})

test("常用模型把渠道自动价和全局价都视为有价，再按请求量排序", () => {
	const sorted = sortGatewayUsedModels([
		{ model_name: "configured", request_count: 100, upstream_discovered: true, price: modelPrice() },
		{ model_name: "channel-auto", request_count: 50, upstream_discovered: true, channel_automatic_price_rows: 2 },
		{ model_name: "missing-low", request_count: 3, upstream_discovered: true },
		{ model_name: "missing-high", request_count: 30, upstream_discovered: false },
	])
	assert.deepEqual(sorted.map((item) => item.model_name), ["missing-high", "missing-low", "configured", "channel-auto"])
})

test("配置页接入模型价格面板、分区导航和 CAS 冲突处理", () => {
	const gatewayPage = readFileSync(new URL("../app/gateway-page.tsx", import.meta.url), "utf8")
	const panel = readFileSync(new URL("../components/gateway/model-prices-panel.tsx", import.meta.url), "utf8")
	const queries = readFileSync(new URL("./queries.ts", import.meta.url), "utf8")

	assert.match(gatewayPage, /\["model-prices", "模型价格"\]/)
	assert.match(gatewayPage, /<ModelPricesPanel \/>/)
	assert.match(queries, /useGatewayModelPrices[\s\S]*"\/gateway\/model-prices"/)
	assert.match(queries, /useGatewayUsedModels[\s\S]*\/gateway\/model-prices\/used-models\?days=/)
	assert.match(panel, /"\/gateway\/model-prices\/sync"/)
	assert.match(panel, /gatewayModelPricePatch\(gatewayModelPriceInput\(price, overrides\), price\.revision\)/)
	assert.match(panel, /gatewayModelPricePatch\(parsed\.value, expectedRevision\)/)
	assert.match(panel, /status === 409/)
	assert.match(panel, /草稿未被覆盖/)
	assert.match(panel, /min-w-\[960px\] table-fixed/)
	assert.match(panel, /配置长上下文阶梯价/)
	assert.match(panel, /允许自动维护/)
	assert.match(panel, /kind: "create", modelName/)
	assert.match(panel, /<Dialog open/)
})
