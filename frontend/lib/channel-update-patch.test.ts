import assert from "node:assert/strict"
import test from "node:test"

import {
  buildChannelUpdatePatch,
  buildDirectCredentialPayload,
  channelEditableValues,
  directCredentialsChanged,
  directCredentialValidationError,
  directCredentialValidationIssue,
  directModelsChanged,
  directModelsForForm,
  directModelsList,
  directModelsPayload,
  initializeDirectCredentialDrafts,
  type ChannelEditableValues,
} from "./channel-update-patch"
import type { Channel } from "./api-types"

function channelFixture(): Channel {
  return {
    id: 7,
    name: "before",
    type: "sub2api",
    gateway_app: "codex",
    site_url: "https://example.com",
    username: "user",
    sort_order: 1,
    credential_mode: "password",
    login_extra_params: "",
    turnstile_enabled: false,
    ignore_announcements: false,
    subscription_enabled: true,
    proxy_enabled: true,
    network_proxy_id: 11,
    captcha_config_id: null,
    balance_threshold: 0.1,
    recharge_multiplier_mode: "multiply",
    monitor_enabled: true,
    latency_probe_enabled: true,
    admin_revision: 9,
    funding_factor: 1,
    funding_factor_source: "default",
    pending_funding_count: 0,
    created_at: "2026-07-23T12:00:00Z",
    updated_at: "2026-07-23T12:00:00Z",
  }
}

test("channel update patch does not resubmit stale untouched form fields", () => {
  const channel = channelFixture()
  const initial = channelEditableValues(channel)
  const next: ChannelEditableValues = { ...initial, name: "renamed" }

  assert.deepEqual(buildChannelUpdatePatch(initial, next, channel.admin_revision), {
    expected_admin_revision: 9,
    name: "renamed",
  })
})

test("channel update patch keeps nullable selectors and booleans in the explicit diff", () => {
  const channel = channelFixture()
  const initial = channelEditableValues(channel)
  const next: ChannelEditableValues = {
    ...initial,
    monitor_enabled: false,
    network_proxy_id: 0,
    captcha_config_id: 3,
  }

  assert.deepEqual(buildChannelUpdatePatch(initial, next, channel.admin_revision), {
    expected_admin_revision: 9,
    monitor_enabled: false,
    network_proxy_id: 0,
    captcha_config_id: 3,
  })
})

test("direct model form helpers split only on lines", () => {
  assert.equal(directModelsForForm('["vendor-alias","plain"]'), "vendor-alias\nplain")
  assert.deepEqual(directModelsList("vendor-alias\nplain"), ["vendor-alias", "plain"])
  assert.equal(directModelsPayload("vendor-alias\nplain"), '["vendor-alias","plain"]')
})

test("direct model change detection ignores unchanged persisted selections", () => {
  assert.equal(directModelsChanged('["vendor-alias","plain"]', "vendor-alias\nplain"), false)
  assert.equal(directModelsChanged('["vendor-alias","plain"]', "vendor-alias\nother"), true)
})

const persistedDirectCredential = {
  id: 21,
  group_name: "primary",
  base_url: "https://api.example.com",
  api_key_prefix: "sk-saved",
  key_configured: true,
  models: '["model-a","model-b"]',
  ratio: 1.5,
  completion_ratio: 2,
  enabled: true,
  revision: 4,
  created_at: "2026-08-11T00:00:00Z",
  updated_at: "2026-08-11T00:00:00Z",
}

const persistedDirectDraft = {
  id: 21,
  group_name: "primary",
  base_url: "https://api.example.com",
  api_key: "",
  key_configured: true,
  models: "model-a\nmodel-b",
  ratio: "1.5",
  completion_ratio: "2",
  enabled: true,
}

test("entering direct mode initializes the first untouched credential", () => {
  const blankDraft = {
    ...persistedDirectDraft,
    id: undefined,
    group_name: "",
    base_url: "",
    api_key: "",
    key_configured: false,
    models: "",
  }
  assert.deepEqual(initializeDirectCredentialDrafts([blankDraft], " https://api.example.com/v1 "), [{
    ...blankDraft,
    group_name: "direct",
    base_url: "https://api.example.com/v1",
  }])

  const configuredBaseURL = { ...blankDraft, base_url: "https://saved.example/v1" }
  assert.deepEqual(initializeDirectCredentialDrafts([configuredBaseURL], "https://site.example"), [{
    ...configuredBaseURL,
    group_name: "direct",
  }])
  assert.deepEqual(initializeDirectCredentialDrafts([
    { ...blankDraft, api_key: "sk-entered" },
  ], "https://site.example"), [
    { ...blankDraft, api_key: "sk-entered" },
  ])
})

test("direct credential payload omits blank saved keys and keeps independent ratios", () => {
  assert.deepEqual(buildDirectCredentialPayload([
    persistedDirectDraft,
    {
      id: undefined,
      group_name: " backup ",
      base_url: " https://backup.example.com/v1 ",
      api_key: " sk-new ",
      key_configured: false,
      models: "model-c\nmodel-c",
      ratio: "0.75",
      completion_ratio: "1.25",
      enabled: false,
    },
  ]), [
    {
      id: 21,
      group_name: "primary",
      base_url: "https://api.example.com",
      models: '["model-a","model-b"]',
      ratio: 1.5,
      completion_ratio: 2,
      enabled: true,
    },
    {
      group_name: "backup",
      base_url: "https://backup.example.com/v1",
      api_key: "sk-new",
      models: '["model-c"]',
      ratio: 0.75,
      completion_ratio: 1.25,
      enabled: false,
    },
  ])
})

test("direct credential validation requires keys only when no saved key exists", () => {
  assert.equal(directCredentialValidationError([persistedDirectDraft]), null)
  assert.equal(
    directCredentialValidationError([{ ...persistedDirectDraft, key_configured: false }]),
    "直连分组 1 必须填写 API Key",
  )
  assert.equal(
    directCredentialValidationError([
      persistedDirectDraft,
      { ...persistedDirectDraft, id: 22, group_name: " PRIMARY " },
    ]),
    "直连分组 2 的名称不能重复",
  )
})

test("direct credential change detection ignores blank retained keys", () => {
  assert.equal(directCredentialsChanged([persistedDirectCredential], [persistedDirectDraft]), false)
  assert.equal(
    directCredentialsChanged([persistedDirectCredential], [{ ...persistedDirectDraft, api_key: "sk-replaced" }]),
    true,
  )
  assert.equal(
    directCredentialsChanged([persistedDirectCredential], [{ ...persistedDirectDraft, completion_ratio: "2.5" }]),
    true,
  )
})


test("direct validation identifies the actual group and field for focus", () => {
  const drafts = [persistedDirectDraft, { ...persistedDirectDraft, id: 22, group_name: "second" }]
  const missingKey = { ...drafts[1], id: undefined, api_key: "", key_configured: false }
  assert.equal(directCredentialValidationIssue([drafts[0], missingKey])?.index, 1)
  assert.equal(directCredentialValidationIssue([drafts[0], missingKey])?.field, "direct-api-key")
  assert.equal(directCredentialValidationIssue([drafts[0], { ...drafts[1], completion_ratio: "bad" }])?.field, "direct-output-ratio")
  assert.equal(directCredentialValidationIssue([drafts[0], { ...drafts[1], group_name: drafts[0].group_name.toUpperCase() }])?.index, 1)
})
