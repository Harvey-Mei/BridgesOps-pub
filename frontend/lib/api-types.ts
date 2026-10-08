/**
 * API response shapes for BridgeOps backend.
 * Keep in sync with backend/storage/*.go and backend/api/*.go.
 */

export type ChannelType = "newapi" | "sub2api"

export type GatewayApp = "codex" | "claude" | "gemini"

export type GatewayModelFamily =
  | "codex"
  | "grok"
  | "deepseek"
  | "qwen"
  | "mistral"
  | "llama"
  | "glm"
  | "minimax"
  | "moonshot"
  | "kimi"
  | "yi"
  | "ernie"
  | "claude"
  | "gemini"

export type CredentialMode = "password" | "token" | "direct"

export type RechargeMultiplierMode = "divide" | "multiply"

export type ChannelAccountingStatus = "ready" | "needs_review" | "error" | string

export type ChannelAccountingReviewReason =
  | "provider_factor_changed"
  | "funding_basis_missing"
  | "accounting_review"
  | string

export type ChannelDeletionState =
  | "pending_remote_cleanup"
  | "manual_review_required"
  | "ready_for_local_finalize"
  | "finalized"

export interface ChannelDeletionAccepted {
  channel_id: number
  intent_id?: string
  deletion_epoch?: string
  channel_identity_revision?: string
  status: ChannelDeletionState
  message: string
}

export type ChannelDeletionKeyDisposition =
  | "verify_managed_delete"
  | "retain_adopted"
  | "manual_review_required"

export type ChannelDeletionKeyClaimStatus =
  | "pending_verification"
  | "processing"
  | "processing_unknown"
  | "delete_confirmed"
  | "initially_absent_confirmed"
  | "retained_adopted"
  | "retained_by_review"
  | "manual_review_required"

export interface ChannelDeletionReviewClaim {
  id: string
  claim_epoch: string
  local_key_id: string
  key_generation: string
  upstream_group_id?: string
  presence_generation?: string
  canonical_group_name?: string
  upstream_key_id?: string
  observed_remote_id?: string
  upstream_key_name?: string
  ownership_provenance: string
  provisioning: boolean
  provisioning_started_at?: string
  disposition: ChannelDeletionKeyDisposition
  status: ChannelDeletionKeyClaimStatus
  outcome_reason?: string
  outcome_actor?: string
  outcome_at?: string
  last_error?: string
  lease_expires_at?: string
  updated_at: string
}

export interface ChannelDeletionReview {
  intent_id: string
  channel_id: string
  deletion_epoch: string
  channel_identity_revision: string
  channel_name: string
  channel_type: ChannelType
  status: ChannelDeletionState
  last_error?: string
  attempt_count: string
  last_attempt_at?: string
  next_attempt_at?: string
  finalized_at?: string
  requested_at: string
  updated_at: string
  claims: ChannelDeletionReviewClaim[]
}

export interface ChannelDeletionReviewPage {
  items: ChannelDeletionReview[]
  total: number
  page: number
  page_size: number
  pages: number
}

export type NotificationChannelType =
  | "telegram"
  | "webhook"
  | "email"
  | "wecom"
  | "dingtalk"
  | "feishu"
  | "serverchan3"

export type CaptchaProviderType =
  | "capsolver"
  | "2captcha"
  | "anticaptcha"
  | "yescaptcha"

export type MonitorJob = "login" | "balance" | "cost" | "rates"

export type ModelCheckTargetStatus =
  | "pending"
  | "healthy"
  | "suspect"
  | "unavailable"

export type ModelCheckRunStatus = "passed" | "anomaly" | "error" | "deferred"

export type NotificationEvent =
  | "balance_low"
  | "rate_changed"
  | "rate_structure_changed"
  | "rate_added"
  | "rate_removed"
  | "announcement"
  | "login_failed"
  | "captcha_failed"
  | "monitor_failed"
  | "subscription_daily_remaining_low"
  | "subscription_weekly_remaining_low"
  | "subscription_monthly_remaining_low"
  | "subscription_expiring"
  | "model_check_alert"
  | "model_check_recovered"

export interface Channel {
  id: number
  name: string
  type: ChannelType
  gateway_app: GatewayApp
  site_url: string
  api_url?: string
  username: string
  sort_order: number
  user_id?: string
  credential_mode: CredentialMode
  direct_models?: string
  direct_credentials?: DirectCredential[]
  login_extra_params: string
  turnstile_enabled: boolean
  ignore_announcements: boolean
  subscription_enabled: boolean
  proxy_enabled: boolean
  network_proxy_id?: number | null
  captcha_config_id?: number | null
  balance_threshold: number
  recharge_multiplier?: number | null
  recharge_multiplier_mode: RechargeMultiplierMode
  monitor_enabled: boolean
  latency_probe_enabled: boolean
  admin_revision: number
  deletion_pending?: boolean
  deletion_epoch?: number
  deletion_state?: ChannelDeletionState
  deletion_requested_at?: string | null
  last_balance?: number | null
  raw_last_balance?: number | null
  funding_factor: number
  funding_factor_source: string
  last_balance_at?: string | null
  today_cost?: number | null
  raw_today_cost?: number | null
  today_cost_at?: string | null
  today_cost_is_today?: boolean
  total_cost?: number | null
  raw_total_cost?: number | null
  pending_funding_count: number
  accounting_status?: ChannelAccountingStatus
  accounting_review_reason?: ChannelAccountingReviewReason
  model_check_status?: ModelCheckTargetStatus
  model_check_alert_count?: number
  model_check_unavailable_count?: number
  model_check_last_checked_at?: string | null
  model_check_lowest_score?: number | null
  request_count?: number
  last_error?: string
  balance_error?: string
  balance_error_at?: string | null
  upstream_account_balance_insufficient?: boolean
  upstream_account_balance_insufficient_at?: string | null
  created_at: string
  updated_at: string
  upstream_account_concurrency?: UpstreamAccountConcurrencyFact | null
}

export interface DirectCredential {
  id: number
  group_name: string
  base_url: string
  api_key_prefix?: string
  key_configured: boolean
  models: string
  ratio: number
  completion_ratio: number
  enabled: boolean
  revision: number
  created_at: string
  updated_at: string
}

export interface DirectCredentialInput {
  id?: number
  group_name: string
  base_url: string
  api_key?: string
  models: string
  ratio: number
  completion_ratio: number
  enabled: boolean
}

export interface UpstreamAccountConcurrencyFact {
  concurrency_limit: number
  concurrency_scope: "account" | string
  concurrency_source?: string
  concurrency_observed_at?: string | null
}

export interface ChannelCostComparisonRow {
  channel_id: number
  channel_name: string
  gateway_cost_24h: number
  gateway_cost_today: number
  account_cost_today?: number | null
  difference_24h?: number | null
  difference_today?: number | null
  account_cost_sampled_at?: string | null
}

export interface ChannelCostComparison {
  generated_at: string
  business_day_start: string
  rows: ChannelCostComparisonRow[]
}

export type NetworkProxyProtocol = "http" | "https" | "socks5"

export type NetworkProxyImportFormat = "auto" | "uri" | "clash" | "3x-ui"

export interface NetworkProxy {
  id: number
  name: string
  protocol: NetworkProxyProtocol
  host: string
  port: number
  source: string
  clean_ip: boolean
  enabled: boolean
  auth_configured: boolean
  bound_channel_count: number
  last_tested_at?: string | null
  last_latency_ms?: number | null
  last_egress_ip?: string | null
  last_error?: string | null
  created_at: string
  updated_at: string
}

export interface NetworkProxyInput {
  name: string
  protocol: NetworkProxyProtocol
  host: string
  port: number
  username: string
  password: string
  clear_credentials?: boolean
  source?: string
  clean_ip: boolean
  enabled: boolean
}

export interface NetworkProxyImportInput {
  format: NetworkProxyImportFormat
  content?: string
  subscription_url?: string
  clean_ip: boolean
  enabled: boolean
}

export interface NetworkProxyImportSkipped {
  entry: string
  reason: string
}

export interface NetworkProxyImportResult {
  created: NetworkProxy[]
  skipped: NetworkProxyImportSkipped[]
}

export interface NetworkProxyTestResult {
  ok: boolean
  latency_ms: number
  ip: string
  provider: string
  error?: string
}

export interface ModelCheckConfig {
  enabled: boolean
  runsPerDay: number
  concurrency: number
  maxTargetsPerRun: number
  probesPerTarget: number
  requestBudget24h: number
  timeoutSeconds: number
  pelicanTimeoutSeconds: number
  alertFailures: number
  historyDays: number
}

export interface ModelCheckSummary {
  total: number
  enabled: number
  healthy: number
  suspect: number
  unavailable: number
  pending: number
  requests_24h: number
}

export interface ModelCheckTarget {
  id: number
  channel_id: number
  channel_name: string
  group_name: string
  model_name: string
  app: GatewayApp
  enabled: boolean
  active: boolean
  last_discovered_at: string
  last_checked_at?: string | null
  status: ModelCheckTargetStatus
  last_score?: number | null
  baseline_score?: number | null
  baseline_samples: number
  recent_anomalies: number
  consecutive_errors: number
  alert_active: boolean
  alert_reason?: string
  alerted_at?: string | null
  recovered_at?: string | null
  last_actual_model?: string
  last_error?: string
  created_at: string
  updated_at: string
}

export interface ModelCheckManualOption {
  channel_id: number
  identity_revision: number
  channel_name: string
  group_name: string
  model_name: string
  app: GatewayApp
}

export interface ModelCheckRun {
  id: number
  target_id: number
  channel_id: number
  channel_name: string
  group_name: string
  model_name: string
  actual_model?: string
  app: GatewayApp
  probe_version: string
  status: ModelCheckRunStatus
  score: number
  baseline_before?: number | null
  hard_mismatch: boolean
  reasons: string[]
  evidence: Array<Record<string, unknown>>
  request_count: number
  prompt_tokens: number
  completion_tokens: number
  cache_read_tokens: number
  total_tokens: number
  first_byte_ms: number
  latency_ms: number
  error?: string
  started_at: string
  finished_at: string
  created_at: string
}

export interface ModelCheckRunsPage {
  items: ModelCheckRun[]
  total: number
  page: number
  page_size: number
}

export interface ModelCheckOverview {
  config: ModelCheckConfig
  summary: ModelCheckSummary
  targets: ModelCheckTarget[]
}

export interface ChannelCostState {
  channel_id: number
  raw_balance: number
  actual_inventory: number
  funding_factor: number
  raw_today_cost: number
  actual_today_cost: number
  raw_total_cost: number
  actual_total_cost: number
  cost_sampled_at?: string | null
  balance_sampled_at?: string | null
  observed_provider_factor?: number | null
  observed_provider_factor_source?: string
  observed_provider_factor_sampled_at?: string | null
  cost_unit_epoch: number
  version: number
  source: string
  status: string
  last_error?: string
  updated_at: string
}

export interface ChannelFundingEvent {
  id: number
  channel_id: number
  kind: string
  paid_amount: number
  credited_amount: number
  raw_balance_before: number
  raw_balance_after: number
  actual_inventory_before: number
  actual_inventory_after: number
  funding_factor_before: number
  raw_today_cost_before: number
  raw_total_cost_before: number
  funding_factor_after: number
  note?: string
  effective_at: string
}

export interface ChannelFundingCandidate {
  id: number
  channel_id: number
  source: string
  external_id: string
  status: string
  paid_amount?: number | null
  credited_amount: number
  raw_balance_before: number
  raw_balance_detected: number
  funding_factor_before: number
  raw_today_cost_before: number
  raw_total_cost_before: number
  occurred_at?: string | null
  detected_at: string
  note?: string
}

export interface ChannelCostRate {
  group_name: string
  raw_ratio: number
  raw_completion_ratio: number
  effective_ratio: number
  effective_completion_ratio: number
}

export interface ChannelCostUnitRebaseReadiness {
  supported: boolean
  eligible: boolean
  blockers: string[]
  expected_version: number
  expected_cost_unit_epoch: number
  multiplier_mode: RechargeMultiplierMode
  previous_multiplier?: number | null
  previous_provider_factor?: number | null
}

export interface ChannelCostUnitRebaseAudit {
  id: number
  channel_id: number
  expected_state_version: number
  applied_state_version: number
  expected_cost_unit_epoch: number
  applied_cost_unit_epoch: number
  previous_multiplier: number
  current_multiplier: number
  previous_provider_factor: number
  current_provider_factor: number
  funding_factor_before: number
  funding_factor_after: number
  applied_at: string
}

export interface ChannelCostUnitRebaseResult {
  audit: ChannelCostUnitRebaseAudit
  state?: ChannelCostState | null
  idempotent_replay: boolean
}

export interface ChannelCostBasisOverrideReadiness {
  eligible: boolean
  blockers: string[]
  expected_version: number
  expected_identity_revision: number
  expected_cost_unit_epoch: number
  current_funding_factor: number
  pending_funding_candidates: number
}

export interface ChannelCostBasisOverrideAudit {
  id: number
  channel_id: number
  channel_identity_revision: number
  expected_state_version: number
  applied_state_version: number
  expected_cost_unit_epoch: number
  applied_cost_unit_epoch: number
  funding_factor_before: number
  funding_factor_after: number
  superseded_candidate_count: number
  superseded_candidate_digest: string
  actor: string
  note: string
  applied_at: string
}

export interface ChannelCostBasisOverrideResult {
  audit: ChannelCostBasisOverrideAudit
  state?: ChannelCostState | null
  idempotent_replay: boolean
}

export interface ChannelCostProfile {
  state: ChannelCostState
  events: ChannelFundingEvent[]
  total: number
  pending: ChannelFundingCandidate[]
  pending_total: number
  rates: ChannelCostRate[]
  cost_unit_rebase: ChannelCostUnitRebaseReadiness
  cost_basis_override: ChannelCostBasisOverrideReadiness
  candidate_resolution?: "superseded"
}

export interface ChannelPage {
  items: Channel[]
  total: number
  page: number
  page_size: number
  pages: number
}

export interface CaptchaConfig {
  id: number
  name: string
  type: CaptchaProviderType
  endpoint?: string
  extra?: string
  enabled: boolean
  proxy_enabled: boolean
  last_balance?: number | null
  balance_unit?: string
  balance_at?: string | null
  balance_error?: string
  created_at: string
  updated_at: string
}

export interface RateSnapshot {
  id: number
  channel_id: number
  model_name: string
  description?: string
  ratio: number
  completion_ratio: number
  first_seen_at: string
  last_seen_at: string
}

export interface RateChangeLog {
  id: number
  channel_id: number
  model_name: string
  old_ratio: number | null
  new_ratio: number
  old_completion_ratio?: number | null
  new_completion_ratio?: number
  funding_factor_before: number
  funding_factor_after: number
  old_effective_ratio?: number | null
  new_effective_ratio: number
  old_effective_completion_ratio?: number | null
  new_effective_completion_ratio: number
  change_reason: "upstream_rate" | "funding_factor" | string
  changed_at: string
}

export interface RateChangeLogPage {
  items: RateChangeLog[]
  total: number
  page: number
  page_size: number
  pages: number
}

export interface RateChangeAnalytics {
  total: number
  up: number
  down: number
  other: number
  daily: Array<{ day: string; up: number; down: number; other: number }>
	models: Array<{ channel_id: number; channel_name: string; model_name: string; changes: number; up: number; down: number }>
  recent: RateChangeLog[]
}

export interface GatewayDownstreamKey {
  id: number
  name: string
  app: GatewayApp
  model_family?: GatewayModelFamily | string
  key_prefix: string
  api_key?: string
  smart_routing: boolean
  manual_channel_id?: number
  manual_channel_name?: string
  manual_group_name?: string
  manual_route_invalid?: boolean
  route_exclusions?: GatewayRouteExclusion[]
  candidate_group_id?: number
  candidate_group_name?: string
	candidate_target_mode?: "candidate_group" | "all_candidates" | "unset"
	candidate_target_source?: "manual" | "key_group_policy" | "legacy"
	route_policy_id?: number
	route_policy_version?: number
  key_group_id?: number
  key_group_name?: string
  model_mapping_strategy?: GatewayModelMappingStrategy
  model_mapping_model?: string
  model_mapping_reasoning_effort?: string
  model_mapping_preview?: {
    strategy: GatewayModelMappingStrategy | string
    config_key?: string
    label?: string
    model?: string
    reasoning_effort?: string
    source?: string
    error?: string
  }
  candidate_preview_model?: string
  candidate_preview_model_source?: "recent_request" | string
  recent_route?: {
    request_id: number
    channel_id: number
    channel_name: string
    group_name: string
    model_name: string
    created_at: string
  }
  usage?: GatewayDownstreamKeyUsage
  current_concurrency: number
  enabled: boolean
  last_used_at?: string | null
  created_at: string
  updated_at: string
}

export interface GatewayModelPrice {
	id: number
	model_name: string
	input_unit_price_usd: number
	output_unit_price_usd: number
	cache_read_unit_price_usd: number | null
	cache_write_unit_price_usd: number | null
	long_context_threshold_tokens?: number | null
	long_context_input_unit_price_usd?: number | null
	long_context_output_unit_price_usd?: number | null
	long_context_cache_read_unit_price_usd?: number | null
	long_context_cache_write_unit_price_usd?: number | null
	source: "manual" | "litellm" | string
	source_url?: string
	source_checksum?: string
	source_observed_at?: string | null
	auto_update: boolean
	enabled: boolean
	revision: number
	created_at: string
	updated_at: string
}

export interface GatewayModelPriceInput {
	model_name: string
	input_unit_price_usd: number
	output_unit_price_usd: number
	cache_read_unit_price_usd: number | null
	cache_write_unit_price_usd: number | null
	long_context_threshold_tokens?: number | null
	long_context_input_unit_price_usd?: number | null
	long_context_output_unit_price_usd?: number | null
	long_context_cache_read_unit_price_usd?: number | null
	long_context_cache_write_unit_price_usd?: number | null
	auto_update: boolean
	enabled: boolean
}

export interface GatewayModelPricePatch extends GatewayModelPriceInput {
	expected_revision: number
}

export interface GatewayUsedModel {
	model_name: string
	request_count: number
	last_used_at?: string | null
	upstream_discovered: boolean
	last_discovered_at?: string | null
	channel_automatic_price_rows?: number
	channel_price_observed_at?: string | null
	price?: GatewayModelPrice | null
}

export interface GatewayModelPriceSyncResult {
	source_url: string
	source_checksum?: string
	source_observed_at?: string | null
	discovered_models: number
	matched_models: number
	unsupported_models: string[]
	applied: {
		created: number
		updated: number
		unchanged: number
		manual_locked: number
		created_models: string[]
		updated_models: string[]
	}
}

export interface GatewayKeyGroup {
  id: number
  name: string
  created_at: string
  updated_at: string
}

export type GatewayModelMappingStrategy = "off" | "iq" | "cost" | "time" | "balanced" | "manual"

export interface GatewayUpstreamGroupChange {
  id: number
  channel_id: number
  channel_name?: string
  group_name: string
  ratio?: number | null
  completion_ratio?: number | null
  kind: "added" | "removed"
  status: "pending" | "assigned" | "ignored" | "acknowledged" | "superseded"
  upstream_key_id?: number
  upstream_key_name?: string
  upstream_group_id?: number
  presence_generation?: number
  assignment_state?: "awaiting_facts" | "unmatched" | "conflict" | "ready" | "assigned" | "ignored" | "superseded"
  assignment_reason_code?: string
  assignment_actionable?: boolean
  assignment_blocked_reason?: string | null
  removal_manual_review_reason?: string
  removal_manual_review_actor?: string
  removal_manual_review_resolution?: string
  removal_manual_review_resolved_by?: string
  removal_manual_reviewed_at?: string | null
  removal_manual_review_resolved_at?: string | null
  suggested_candidate_group_ids?: number[]
  evaluation_token?: string
  facts_digest?: string
  detected_at: string
  resolved_at?: string | null
  assignment_audit_action?: string
  assignment_audit_trigger?: string
  assignment_audit_actor?: string
  assignment_audit_target_candidate_group_ids?: number[]
  assignment_audit_presence_generation?: number
  assignment_audit_created_at?: string | null
}

export interface GatewayUpstreamGroupChangePage {
	items: GatewayUpstreamGroupChange[]
	total: number
	page: number
  page_size: number
  pages: number
  filter_options?: {
    channels: Array<{ value: string; label: string }>
    groups: Array<{ value: string; label: string }>
    sources: Array<{ value: string; label: string }>
    truncated?: boolean
	}
}

export interface GatewayUpstreamGroupChangeHistoryCursorPage {
	items: GatewayUpstreamGroupChange[]
	next_cursor?: string | null
	filter_options?: GatewayUpstreamGroupChangePage["filter_options"]
}

export interface GatewayUpstreamGroupChangeReviewSummary {
  count: number
}

export interface GatewayCandidateGroupMember {
  id: number
  candidate_group_id: number
  upstream_group_id: number
  presence_generation?: number
  channel_id: number
  group_name: string
}

export interface GatewayCandidateGroup {
  id: number
  name: string
  model_mapping_family?: "codex" | "claude" | string
  membership_version: number
  members: GatewayCandidateGroupMember[]
  created_at: string
  updated_at: string
}

export interface GatewayUpstreamGroup {
  id: number
  channel_id: number
  canonical_group_name: string
  raw_group_names: string[]
  present: boolean
  presence_generation: number
  model_family_codes: string[]
  model_ids: string[]
  unclassified_model_ids: string[]
  ambiguous_model_ids: string[]
  gateway_apps: string[]
  gateway_app_sources: Record<string, string>
  channel_type: "newapi" | "sub2api"
  service_tier?: string
  service_tier_source: "manual" | "provider"
  facts_complete: boolean
  incomplete_reasons: string[]
  facts_version: number
  models_synced_at?: string | null
}

export interface GatewayAutomaticAssignmentRule {
	id: number
	candidate_group_id: number
	enabled: boolean
	priority: number
	model_families: string[]
	family_match_mode: "single_any" | "exact_set" | "contains_all"
	gateway_apps: string[]
	app_match_mode: "exact_set" | "contains_all"
	accepted_app_sources: string[]
	channel_types: string[]
	service_tiers: string[]
	model_id_conditions: GatewayModelIDCondition[]
	model_condition_quantifier: "any" | "all"
	model_fact_coverage: "matched" | "all"
	capability_conditions: GatewayCapabilityCondition[]
	rule_version: number
}

export interface GatewayModelIDCondition {
	match: "exact" | "prefix"
	value: string
}

export interface GatewayCapabilityCondition {
	selector_type: "family" | "exact" | "prefix" | "matched_model_ids"
	selector_values: string[]
	quantifier: "all_models" | "any_model"
	evidence_source: "probed"
	max_age_hours: number
	min_context_window_tokens?: number
	min_safe_input_tokens?: number
}

export interface GatewayModelFamilyCatalog {
	version: string
	families: Array<{
		code: string
		display_name: string
		aliases: string[]
		compatible_apps: string[]
		icon: string
		brand_color: string
		auto_assignable: boolean
	}>
}

export interface GatewayAutomaticAssignmentState {
	id: number
	ruleset_version: number
	backfill_complete: boolean
	auto_execute_enabled: boolean
}

export interface GatewayAutomaticAssignmentConfig {
	state: GatewayAutomaticAssignmentState
	rules: GatewayAutomaticAssignmentRule[]
	upstream_groups: GatewayUpstreamGroup[]
	audits: GatewayAutomaticAssignmentAudit[]
	model_catalog: GatewayModelFamilyCatalog
}

export interface GatewayAutomaticAssignmentAudit {
	id: number
	change_id?: number
	upstream_group_id: number
	raw_group_name: string
	canonical_group_name: string
	presence_generation: number
	action: string
	trigger_source: string
	classification_version: string
	facts_version: number
	ruleset_version: number
	override_version: number
	source_ref?: number
	matched_rule_ids: number[]
	target_candidate_group_ids: number[]
	facts_snapshot: Record<string, unknown>
	explanation: Record<string, unknown>
	actor: string
	created_at: string
}

export interface GatewayAutomaticRuleMatch {
	rule_id: number
	candidate_group_id: number
	priority: number
	matched: boolean
	facts_incomplete: boolean
	reasons: string[]
	missing_facts: string[]
}

export interface GatewayAutomaticAssignmentPreview {
	change_id: number
	upstream_group_id: number
	channel_id: number
	group_name: string
	presence_generation: number
	model_family_codes: string[]
	model_ids: string[]
	gateway_apps: string[]
	channel_type: string
	service_tier?: string
	service_tier_source?: string
	state: "awaiting_facts" | "unmatched" | "conflict" | "ready" | "superseded"
	reason_code: string
	missing_facts: string[]
	suggested_candidate_group_ids: number[]
	matched_rule_ids: number[]
	rule_matches: GatewayAutomaticRuleMatch[]
	evaluation_token?: string
	evaluated_at: string
}

export interface GatewayKeyGroupRoutePolicy {
	id: number
	key_group_id: number
	gateway_app: GatewayApp
	model_family: string
	default_candidate_group_id?: number
	target_mode: "candidate_group" | "all_candidates"
	enabled: boolean
	policy_version: number
}

export interface GatewayDownstreamKeyUsage {
  total_cost: number
  today_cost: number
  total_requests: number
  today_requests: number
  total_tokens: number
  today_tokens: number
  actual_cost: number
  estimated_cost: number
  reported_cost: number
  reconciled_cost: number
}

export interface GatewayRouteExclusion {
  id: number
  downstream_key_id: number
  channel_id: number
  group_name: string
  created_at: string
  updated_at: string
}

export interface GatewayDownstreamKeyCreateResult {
  key: GatewayDownstreamKey
  plain: string
  notice: string
}

export interface GatewayRouteCandidate {
  upstream_group_id?: number
  presence_generation?: number
  channel_identity_revision?: number
  channel_id: number
  channel_name: string
  channel_type: ChannelType
  gateway_app: GatewayApp
  credential_mode: CredentialMode
  group_name: string
  ratio: number
  completion_ratio: number
  raw_ratio: number
  raw_completion_ratio: number
  funding_factor: number
  cost_basis_version: number
  cost_basis_source?: string
  effective_price: number
  effective_price_basis?: string
  price_input_share?: number
  cache_ratio: number
  cache_ratio_known: boolean
  model_families: string[]
  model_mapping_families: string[]
  model_ids: string[]
  latency_probe_model: string
  models_synced_at?: string | null
  models_error?: string
  avg_latency_ms: number
  score: number
  price_score: number
  latency_score: number
  latency_samples: number
  latency_raw_samples?: number[]
  probe_enabled?: boolean
  probe_disabled?: boolean
  probe_estimated_cost_usd: number
  disabled_for_key: boolean
  cooling_down: boolean
  failure_count: number
  cooldown_until?: string | null
  cooldown_remaining: number
  routing_available?: boolean
  availability_reasons?: string[]
  probe_eligible?: boolean
  probe_blockers?: string[]
  latency_qualified?: boolean
  latency_source?: "group" | "channel" | "none"
  upstream_key_configured?: boolean
  upstream_key_enabled?: boolean
  upstream_key_provisioning?: boolean
  upstream_key_available?: boolean
  upstream_key_last_error?: string
  last_routing_failure_error?: string
  last_routing_failure_kind?: string
  last_routing_failure_status?: number
  last_routing_failure_at?: string | null
}

export type GatewayManualMappedTargetMappingState = "not_applicable" | "resolved" | "unresolved"

export interface GatewayManualMappedTargetGate {
  candidate_member_id: number
  channel_id: number
  group_name: string
  structural_blocked: boolean
  manual_mapped_model_gate: boolean
  gate_provenance: "manual_mapped_model_gate" | "mapping_unresolved" | string
  reason?: string
}

export interface GatewayManualMappedTargetPreview {
  mapping_state: GatewayManualMappedTargetMappingState
  gate_provenance: "manual_mapped_model_gate" | "mapping_unresolved" | string
  targets: GatewayManualMappedTargetGate[]
}

export type GatewayImageTaskMode = "unknown" | "sync" | "async" | "sync_async"
export type GatewayImageCapabilityProtocol = "openai_images" | "openai_responses"

export interface GatewayImageOperationTarget {
  channel_id: number
  channel_name: string
  upstream_group_id: number
  group_name: string
  presence_generation: number
  model_name: string
  models_synced_at?: string
  group_facts_version: number
}

export interface GatewayImageOperationTargetInventoryPage {
  items: GatewayImageOperationTarget[]
  next_cursor?: string
  snapshot: string
  captured_at: string
}

export interface GatewayImageOperationCapability {
  id: number
  channel_id: number
  channel_name?: string
  upstream_group_id: number
  group_name: string
  presence_generation: number
  model_name: string
  operation: "image_generation"
  protocol: GatewayImageCapabilityProtocol
  endpoint_path: string
  task_mode: GatewayImageTaskMode
  idempotency_key_supported: boolean
  idempotency_header?: string
  task_id_returned: boolean
  task_id_json_path?: string
  task_query_supported: boolean
  task_query_method?: string
  task_query_path_template?: string
  task_status_json_path?: string
  task_success_values: string[]
  task_failure_values: string[]
  evidence_source: "observed_success" | "provider_metadata" | "admin_config"
  evidence_detail: string
  evidence_observed_at: string
  configured_by?: string
  capability_version: number
  recovery_complete: boolean
  automatic_replay_allowed: boolean
  recovery_blockers: string[]
  created_at: string
  updated_at: string
}

export interface GatewayImageOperationCapabilityAudit {
  id: number
  capability_id?: number
  channel_id: number
  upstream_group_id: number
  group_name: string
  presence_generation: number
  model_name: string
  operation: "image_generation"
  protocol: GatewayImageCapabilityProtocol
  action: "configured" | "observed" | "provider_metadata" | "evidence_ignored"
  evidence_source: GatewayImageOperationCapability["evidence_source"]
  evidence_observed_at: string
  evidence_reference_kind?: string
  evidence_reference_id?: number
  actor: string
  capability_version: number
  snapshot: Record<string, unknown>
  created_at: string
}

export interface GatewayProbeConfigGroup {
  upstream_group_id: number
  presence_generation: number
  channel_identity_revision: number
  group_name: string
  probe_enabled: boolean
  description?: string
  ratio: number
  completion_ratio: number
  raw_ratio: number
  raw_completion_ratio: number
  funding_factor: number
  cost_basis_version: number
  cost_basis_source?: string
  effective_price: number
  model_families: string[]
  model_mapping_families: string[]
  model_ids: string[]
  gateway_apps: GatewayApp[]
  latency_probe_model: string
  models_synced_at?: string | null
  models_error?: string
  upstream_key_configured: boolean
  upstream_key_enabled: boolean
  upstream_key_provisioning: boolean
  upstream_key_available: boolean
  last_error?: string
  last_routing_failure_error?: string
  last_routing_failure_kind?: string
  last_routing_failure_status?: number
  last_routing_failure_at?: string | null
  cooling_down: boolean
  failure_count: number
  cooldown_until?: string | null
  cooldown_remaining: number
  avg_latency_ms: number
  latency_samples: number
  latency_raw_samples?: number[]
  latency_qualified: boolean
  latency_source: "group" | "channel" | "none"
  probe_eligible: boolean
  probe_blockers: string[]
  probe_estimated_cost_usd: number
  status?: string
  at?: string | null
}

export interface GatewayProbeConfigChannel {
  channel_id: number
  channel_name: string
  channel_type: ChannelType
  gateway_app: GatewayApp
  credential_mode: CredentialMode
  monitor_enabled: boolean
  probe_enabled: boolean
  routing_available: boolean
  availability_reasons: string[]
  last_error?: string
  balance_error?: string
  balance_error_at?: string | null
  upstream_account_balance_insufficient?: boolean
  upstream_account_balance_insufficient_at?: string | null
  last_balance?: number | null
  balance_threshold: number
  last_balance_at?: string | null
  groups: GatewayProbeConfigGroup[]
}

export type GatewayCostStatus = "none" | "estimated" | "unavailable" | "reported" | "reconciled"

export interface GatewayRouteAttempt {
  channel_id?: number
  channel_name: string
  group_name?: string
  reason: string
  summary?: string
  status_code?: number
  eligible_rank?: number
  physically_dispatched: boolean
  first_byte_budget_ms: number
}

export interface GatewaySessionAffinityCandidate {
  channel_id: number
  channel_name: string
  group_name: string
  eligible_rank: number
}

export interface GatewayRouteExclusionSnapshotEntry {
  channel_id: number
  group_name: string
  reason: string
  upstream_group_id?: number
  presence_generation?: number
  failure_count?: number
  cooldown_until?: string
}

export interface GatewayRouteExclusionSnapshot {
  version: number
  captured_at: string
  total_entries: number
  truncated: boolean
  entries: GatewayRouteExclusionSnapshotEntry[]
}

export interface GatewayRequestLog {
  id: number
  downstream_key_id: number
  downstream_key_name: string
  downstream_key_group_name?: string
  upstream_request_id?: string
  upstream_key_id?: number
  upstream_key_name?: string
  upstream_accepted?: boolean
  billing_committed?: boolean
  client_ip?: string
  client_ip_source?: string
  channel_id: number
  channel_name: string
  group_name: string
  group_ratio: number
  completion_ratio: number
  raw_group_ratio?: number | null
  raw_completion_ratio?: number | null
  funding_factor?: number | null
  cost_basis_version: number
  cost_basis_source?: string
  model_name: string
  upstream_model_name?: string
  upstream_response_model?: string
  upstream_model_mismatch?: boolean
  reasoning_effort: string
  requested_model_name?: string
  requested_reasoning_effort?: string
  model_mapping_strategy?: GatewayModelMappingStrategy | string
  model_mapping_details?: string
  method: string
  path: string
  status_code: number
  first_byte_ms: number
  downstream_first_visible_ms?: number | null
  latency_ms: number
  prompt_tokens: number
  completion_tokens: number
  cache_creation_tokens: number
  cache_read_tokens: number
  total_tokens: number
  cost: number
  raw_cost?: number | null
  cost_estimated: boolean
  cost_source: string
  cost_status: GatewayCostStatus
  cost_unavailable_reason?: string
  /** Frozen price-fact calculation, independent of any upstream reported amount. */
  calculated_cost?: number | null
  calculated_cost_status?: "estimated" | "unavailable" | string
  calculated_cost_unavailable_reason?: string | null
  /** Upstream amount after normalization to the local USD billing unit. */
  upstream_cost?: number | null
  upstream_cost_status?: "none" | "reported" | "reconciled" | string
  /** Amount selected for list display and comparison facts from the backend. */
  display_cost?: number | null
  cost_difference?: number | null
  cost_difference_ratio?: number | null
  cost_difference_material?: boolean
  price_fact_id?: number | null
  price_fact_version?: number
  price_fact_schema_version?: number
  price_cost_unit_epoch?: number
  price_model_name?: string
  price_operation?: string
  price_protocol?: string
  price_currency?: string
  price_unit?: string
  price_source?: string
  price_observed_at?: string | null
  usage_schema?: string
  usage_schema_version?: number
  cost_billable?: boolean
  cost_reconcile_error?: string
  cost_reconcile_at?: string | null
  route_reason: string
  model_check_kind?: "pelican_svg" | "candy_shapes"
  route_selection_rank?: number
  session_affinity_origin_request_id?: number
  session_affinity_expires_at?: string | null
  session_affinity_overridden_candidates?: GatewaySessionAffinityCandidate[]
  route_details: string
  route_attempts?: GatewayRouteAttempt[]
  logical_request_trace_id?: string
  telemetry_version: number
  routing_mode?: "smart" | "fixed" | string
  gateway_app?: GatewayApp | string
  terminal_failure_code?: string
  connection_event?: "client_disconnect" | "upstream_interrupted" | "dispatch_recovery" | string
  prior_physically_dispatched_failure_count: number
  real_fallback: boolean
  route_exclusion_snapshot_version: number
  route_exclusion_snapshot?: GatewayRouteExclusionSnapshot
  thinking_leak_warning?: boolean
  error?: string
  created_at: string
}

export interface GatewayManualRoutingCooldownResult {
  request_log_id: number
  channel_id: number
  channel_name: string
  group_name: string
  duration_minutes: number
  cooldown_until: string
  idempotent_replay: boolean
}

export interface GatewayRoutingEnableResult {
  upstream_group_id: number
  channel_id: number
  channel_identity_revision: number
  group_name: string
  presence_generation: number
  changed: boolean
  previous_failure_kind?: string
  previous_failure_count: number
  previous_cooldown_until?: string | null
  idempotent_replay: boolean
}

export interface GatewayRequestLogSummary {
  total_requests: number
  success: number
  failed: number
  canceled: number
  total_tokens: number
  prompt_tokens: number
  completion_tokens: number
  cache_creation_tokens: number
  cache_read_tokens: number
  total_cost: number
  estimated_cost: number
  reported_cost: number
  reconciled_cost: number
  actual_cost: number
  unavailable_requests: number
  avg_latency_ms: number
  window_seconds: number
  performance_window_seconds: number
  rpm: number
  tpm: number
  performance_requests: number
  performance_tokens: number
  performance_avg_latency_ms: number
  estimated_concurrency: number
  current_concurrency: number
  current_concurrency_scope: string
  first_request_at?: string | null
  last_request_at?: string | null
}

export interface GatewayRequestLogPage {
  items: GatewayRequestLog[]
  total: number
  page: number
  page_size: number
  pages: number
  summary?: GatewayRequestLogSummary
}

export interface GatewayRequestLogFilters {
  models: string[]
  groups: string[]
}

export interface GatewayRequestLogTrendPoint {
  bucket: string
  requests: number
  success: number
  failed: number
  canceled: number
  total_tokens: number
  prompt_tokens: number
  completion_tokens: number
  cache_creation_tokens: number
  cache_read_tokens: number
  total_cost: number
  estimated_cost: number
  reported_cost: number
  reconciled_cost: number
  actual_cost: number
  unavailable_requests: number
  avg_latency_ms: number
}

export interface GatewayRequestLogModelUnitPriceTrendPoint {
  bucket: string
  model_family: "gpt" | "claude" | string
  model_name: string
  token_type: "input" | "output" | "cache_creation" | "cache_read" | string
  total_tokens: number
  total_cost: number | null
  cost_per_million_tokens: number | null
}

export interface GatewayRequestLogBreakdownItem {
  name: string
  channel_id?: number
  downstream_key_id?: number
  requests: number
  success: number
  failed: number
  canceled: number
  total_tokens: number
  prompt_tokens: number
  completion_tokens: number
  cache_creation_tokens: number
  cache_read_tokens: number
  total_cost: number
  estimated_cost: number
  reported_cost: number
  reconciled_cost: number
  actual_cost: number
  unavailable_requests: number
  avg_latency_ms: number
}

export interface GatewayRequestLogGroupCacheRatio {
  channel_id: number
  channel_name: string
  group_name: string
  cache_read_tokens: number
  cache_creation_tokens: number
  prompt_tokens: number
  valid_requests: number
  ratio: number | null
}

export interface GatewayRequestLogGroupCacheRatioPage {
  items: GatewayRequestLogGroupCacheRatio[]
  next_cursor?: string | null
  page_size: number
}

export interface GatewayRequestLogAnalytics {
  trend: GatewayRequestLogTrendPoint[]
  model_unit_prices: GatewayRequestLogModelUnitPriceTrendPoint[]
  channels: GatewayRequestLogBreakdownItem[]
  models: GatewayRequestLogBreakdownItem[]
  models_by_tokens: GatewayRequestLogBreakdownItem[]
  keys: GatewayRequestLogBreakdownItem[]
  cost_sources: GatewayRequestLogBreakdownItem[]
  route_reasons: GatewayRequestLogBreakdownItem[]
  group_cache_ratios: GatewayRequestLogGroupCacheRatio[]
}

export interface BalanceSnapshot {
  id: number
  channel_id: number
  balance: number
  sampled_at: string
}

export interface NotificationSubscription {
  channel_ids: number[]
  mode: "all" | "groups"
  groups?: string[]
  events?: NotificationEvent[]
}

// The backend returns only the non-sensitive part of a notification config.
// Tokens, passwords, webhook URLs, secrets, and custom headers remain
// encrypted server-side and are intentionally absent here.
export interface NotificationChannelConfig {
  chat_id?: string
  method?: string
  host?: string
  port?: number
  username?: string
  from?: string
  from_name?: string
  to?: string[]
  use_tls?: boolean
  uid?: string
}

export interface NotificationChannel {
  id: number
  name: string
  type: NotificationChannelType
  enabled: boolean
  proxy_enabled: boolean
  config?: NotificationChannelConfig
  subscriptions?: string
  created_at: string
  updated_at: string
}

export interface NotificationLog {
  id: number
  channel_id: number
  upstream_channel_id?: number
  channel_name?: string
  channel_type?: string
  event: NotificationEvent
  subject: string
  body: string
  success: boolean
  error_message?: string
  sent_at: string
}

export interface UpstreamAnnouncement {
  id: number
  channel_id: number
  source_key: string
  title?: string
  content: string
  type?: string
  link?: string
  published_at?: string | null
  source_updated_at?: string | null
  first_seen_at: string
}

export interface MonitorLog {
  id: number
  channel_id: number
  job: MonitorJob
  success: boolean
  error_message?: string
  duration_ms: number
  started_at: string
  finished_at: string
}

export interface DashboardLowest {
  channel_id: number
  name: string
  balance: number | null
}

export interface DashboardChannelStat {
  id: number
  name: string
  type: string
  monitor_enabled: boolean
  balance_threshold: number
  last_balance?: number | null
  today_cost?: number | null
  today_cost_at?: string | null
  today_cost_is_today: boolean
  total_cost?: number | null
  last_error?: string
  balance_error?: string
  balance_error_at?: string | null
  upstream_account_balance_insufficient?: boolean
  upstream_account_balance_insufficient_at?: string | null
}

export interface DashboardSummary {
  total_channels: number
  active_channels: number
  failed_channels: number
  total_balance: number
  today_total_cost: number
  total_cost: number
  lowest_balance: DashboardLowest | null
  channels: DashboardChannelStat[]
  recent_rate_changes: RateChangeLog[]
}

export interface BalanceTrendPoint {
  day: string
  balance: number | null
}

export interface CostTrendPoint {
  day: string
  cost: number | null
}

export interface DashboardMultiplierTrendPoint {
  bucket: string
  successful_requests: number
  input_tokens: number
  output_tokens: number
  input_covered_tokens: number
  output_covered_tokens: number
  input_sample_requests: number
  output_sample_requests: number
  input_multiplier: number | null
  output_multiplier: number | null
  smoothed_input_multiplier: number | null
  smoothed_output_multiplier: number | null
  smoothed_input_covered_tokens: number
  smoothed_output_covered_tokens: number
  smoothed_input_sample_requests: number
  smoothed_output_sample_requests: number
}

export interface DashboardMultiplierTrendApp {
  app: string
  points: DashboardMultiplierTrendPoint[]
}

export interface DashboardMultiplierTrend {
  bucket_seconds: number
  smoothing_buckets: number
  apps: DashboardMultiplierTrendApp[]
}

export interface SystemAuthConfig {
  enabled: boolean
  username: string
  password: string
  passwordConfigured: boolean
  tokenSecret: string
  tokenSecretConfigured: boolean
  clearTokenSecret?: boolean
  sessionTTLHours: number
}

export interface AppConfig {
  title: string
  notificationPrefix: string
  timeZone: string
}

export interface SystemSchedulerRetentionConfig {
  cron: string
  monitorLogsDays: number
  balanceSnapshotsDays: number
  notificationLogsDays: number
  announcementsDays: number
}

export interface GatewayRoutingFailureConfig {
  responseTimeoutSeconds: number
  manualCooldownMinutes: number
}

export interface SystemSchedulerConfig {
  balanceCron: string
  rateCron: string
  fundingDetectionThreshold: number
  rateRefreshIntervalMinutes: number
  concurrency: number
  sessionKeepAliveEnabled: boolean
  sessionKeepAliveIntervalMinutes: number
  latencyProbeEnabled: boolean
  latencyProbeIntervalSeconds: number
  latencyProbeMaxCandidates: number
  latencyProbeTimeoutSeconds: number
  latencyProbeSampleSize: number
  latencyStatsWindowHours: number
  latencyProbeModelSyncMinutes: number
  latencyFullScoreMs: number
  latencyZeroScoreMs: number
  unknownLatencyScoreFactor: number
  routingPriceScoreMax: number
  routingLatencyScoreMax: number
  cacheInputPriceMultiplier: number
  cachePriceMinInputTokens: number
  cachePriceWindowInputTokens: number
  cacheStatsMaxAgeDays: number
  routingFailures: GatewayRoutingFailureConfig
  sessionAffinityMaxAgeMinutes: number
  sessionAffinityStableSamples: number
  codexRadarRefreshIntervalMinutes: number
  retention: SystemSchedulerRetentionConfig
}

export interface CodexRadarSnapshot {
  payload?: Record<string, unknown>
  history?: CodexRadarHistoryPoint[]
  source_url: string
  fetched_at?: string | null
  last_attempt_at?: string | null
  last_error?: string
  stale: boolean
}

export interface CodexRadarHistoryPoint {
  observed_at: string
  config_key: string
  label?: string
  model?: string
  reasoning_effort?: string
  score?: number
  status?: string
  passed?: number
  tasks?: number
  valid_tasks?: number
  invalid?: number
  wall_seconds?: number
  cost_usd?: number
  total_tokens?: number
}

export interface ClaudeRadarHistoryPoint {
  observed_at: string
  config_key: string
  label?: string
  model?: string
  reasoning_effort?: string
  score?: number
  status?: string
  passed?: number
  tasks?: number
  valid_tasks?: number
  invalid?: number
  wall_seconds?: number
  cost_usd?: number
  total_tokens?: number
}

export interface ClaudeRadarSnapshot {
  payload?: Record<string, unknown>
  history?: ClaudeRadarHistoryPoint[]
  source_url: string
  fetched_at?: string | null
  last_attempt_at?: string | null
  last_error?: string
  stale: boolean
}

export interface SystemNotificationsConfig {
  batchRateChanges: boolean
  minChangePct: number
  balanceLowCooldownMinutes: number
  subscriptionDailyRemainingThresholdPct: number
  subscriptionWeeklyRemainingThresholdPct: number
  subscriptionMonthlyRemainingThresholdPct: number
  subscriptionExpiryThresholdHours: number
  subscriptionAlertCooldownMinutes: number
  sendMaxAttempts: number
}

export interface SystemProxyConfig {
  enabled: boolean
  versionCheckEnabled: boolean
  protocol: "http" | "https" | "socks5"
  host: string
  port: number
  username: string
  password: string
  passwordConfigured: boolean
  clearPassword?: boolean
}

export interface SystemUpstreamConfig {
  timeoutSeconds: number
  userAgent: string
}

export interface SystemConfig {
  app: AppConfig
  auth: SystemAuthConfig
  scheduler: SystemSchedulerConfig
  notifications: SystemNotificationsConfig
  proxy: SystemProxyConfig
  upstream: SystemUpstreamConfig
}

export interface SystemConfigResponse {
  config_path: string
  effective_time_zone: string
  config: SystemConfig
}

export interface AppVersion {
  name: string
  title: string
  version: string
  latest_version?: string
  update_available?: boolean
  repo_url?: string
  release_url?: string
  update_error?: string
}

export interface ApplyConfigResult {
  applied_sections: string[]
  message: string
}

export interface ChannelRedeemResult {
  message: string
  type: string
  value: number
  new_balance?: number
  new_concurrency?: number
  group_name?: string
  validity_days?: number
}

export type RechargePaymentMethod = "alipay" | "wxpay"
export type SubscriptionPaymentMethod =
  | "balance"
  | "alipay"
  | "wxpay"
  | "stripe"
  | "creem"
  | "waffo_pancake"
  | string

export interface ChannelRechargeMethod {
  type: RechargePaymentMethod
  name: string
  min_amount: number
  max_amount: number
}

export interface ChannelRechargeInfo {
  amount_label: string
  amount_step: number
  min_amount: number
  max_amount: number
  preset_amounts: number[]
  help_text?: string
  help_image_url?: string
  alipay_force_qrcode: boolean
  methods: ChannelRechargeMethod[]
}

export interface ChannelRechargeLaunch {
  mode: "qrcode" | "redirect" | "form" | "success"
  qr_code?: string
  pay_url?: string
  form_action?: string
  form_fields?: Record<string, string>
  expires_at?: string
}

export interface ChannelSubscriptionMethod {
  type: SubscriptionPaymentMethod
  name: string
}

export interface ChannelSubscriptionPlan {
  id: string
  name: string
  description?: string
  price: number
  currency?: string
  validity?: string
  group_name?: string
  quota?: number
  daily_limit_usd?: number | null
  weekly_limit_usd?: number | null
  monthly_limit_usd?: number | null
  features?: string[]
  payment_methods?: string[]
}

export interface ChannelSubscriptionInfo {
  plans: ChannelSubscriptionPlan[]
  methods: ChannelSubscriptionMethod[]
}

export type ChannelSubscriptionLaunch = ChannelRechargeLaunch

export interface ChannelSubscriptionUsageWindow {
  limit_usd: number
  used_usd: number
  remaining_usd: number
  remaining_percent: number
  used_percent: number
  window_start?: string | null
  resets_at?: string | null
  resets_in_seconds: number
}

export interface ChannelSubscriptionUsage {
  id: number
  group_id: number
  group_name: string
  status: string
  starts_at?: string | null
  expires_at?: string | null
  expires_in_days: number
  daily?: ChannelSubscriptionUsageWindow | null
  weekly?: ChannelSubscriptionUsageWindow | null
  monthly?: ChannelSubscriptionUsageWindow | null
  cycle?: ChannelSubscriptionUsageWindow | null
  total?: ChannelSubscriptionUsageWindow | null
}

export interface ChannelSubscriptionUsageInfo {
  items: ChannelSubscriptionUsage[]
}

export type ChannelAPIKeyStatus = "active" | "disabled" | "expired" | "quota_exhausted" | "unknown"

export interface ChannelAPIKey {
  id: number
  key: string
  name: string
  status: ChannelAPIKeyStatus | string
  group?: string
  group_name?: string
  group_description?: string
  group_ratio: number
  group_id?: number | null
  quota: number
  quota_used: number
  unlimited_quota: boolean
  expired_time: number
  expires_at?: string | null
  created_at?: string | null
  updated_at?: string | null
  last_used_at?: string | null
  allow_ips?: string
  ip_whitelist?: string[]
  ip_blacklist?: string[]
  model_limits_enabled: boolean
  model_limits?: string
  cross_group_retry: boolean
  rate_limit_5h: number
  rate_limit_1d: number
  rate_limit_7d: number
  usage_5h: number
  usage_1d: number
  usage_7d: number
}

export interface ChannelAPIKeyPage {
  items: ChannelAPIKey[]
  total: number
  page: number
  page_size: number
  pages: number
}

export interface NotificationLogPage {
  items: NotificationLog[]
  total: number
  page: number
  page_size: number
  pages: number
}

export interface UpstreamAnnouncementPage {
  items: UpstreamAnnouncement[]
  total: number
  page: number
  page_size: number
  pages: number
}

export interface ChannelAPIKeyGroup {
  id?: number | null
  name: string
  description?: string
  ratio: number
}

export interface ChannelAPIKeyReveal {
  key: string
}
