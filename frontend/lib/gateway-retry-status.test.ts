import assert from "node:assert/strict"
import test from "node:test"
import { gatewayRetryStatus } from "./gateway-retry-status"

test("labels intercepted terminal overloads as retry prompts", () => {
  assert.deepEqual(gatewayRetryStatus({
    terminal_failure_code: "transient_after_acceptance",
    prior_physically_dispatched_failure_count: 0,
    status_code: 502,
    error: "upstream overloaded",
  }), {
    label: "提示重试",
    detail: "已拦截上游故障，并向客户端发送安全重试提示",
    attempts: 0,
    prompted: true,
    recovered: false,
  })
})

test("labels recovered physical retries without hiding success", () => {
  assert.deepEqual(gatewayRetryStatus({
    terminal_failure_code: "",
    prior_physically_dispatched_failure_count: 2,
    status_code: 200,
    error: "",
  }), {
    label: "自动重试 2 次",
    detail: "上游恢复前已自动重试 2 次",
    attempts: 2,
    prompted: false,
    recovered: true,
  })
})

test("keeps final failures distinct from recovered retries", () => {
  assert.deepEqual(gatewayRetryStatus({
    terminal_failure_code: "",
    prior_physically_dispatched_failure_count: 2,
    status_code: 502,
    error: "upstream failed",
  }), {
    label: "已重试 2 次",
    detail: "已自动重试 2 次，最终请求仍失败",
    attempts: 2,
    prompted: false,
    recovered: false,
  })
})

test("prompted failures do not call physical dispatches automatic retries", () => {
  assert.deepEqual(gatewayRetryStatus({
    terminal_failure_code: "retry_prompt_after_exhaustion",
    prior_physically_dispatched_failure_count: 1,
    status_code: 502,
    error: "overloaded",
  }), {
    label: "提示重试",
    detail: "已拦截上游故障，并向客户端发送安全重试提示",
    attempts: 1,
    prompted: true,
    recovered: false,
  })
})

test("ignores ordinary requests", () => {
  assert.equal(gatewayRetryStatus({
    terminal_failure_code: "",
    prior_physically_dispatched_failure_count: 0,
    status_code: 200,
    error: "",
  }), null)
})
