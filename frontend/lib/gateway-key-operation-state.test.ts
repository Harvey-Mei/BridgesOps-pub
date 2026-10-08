import assert from "node:assert/strict"
import test from "node:test"
import { beginKeyOperation, finishKeyEditing, finishKeyOperation } from "./gateway-key-operation-state"

test("key operations allow different rows to save concurrently", () => {
  const first = beginKeyOperation(new Set(), 11)
  const second = beginKeyOperation(first.activeKeyIDs, 22)

  assert.equal(first.started, true)
  assert.equal(second.started, true)
  assert.deepEqual([...second.activeKeyIDs].sort((left, right) => left - right), [11, 22])

  const afterFirstFinishes = finishKeyOperation(second.activeKeyIDs, 11)
  assert.deepEqual([...afterFirstFinishes], [22])
})

test("key operations reject reentry without unlocking another row", () => {
  const first = beginKeyOperation(new Set([11, 22]), 11)
  assert.equal(first.started, false)
  assert.deepEqual([...first.activeKeyIDs].sort((left, right) => left - right), [11, 22])

  const afterSecondFinishes = finishKeyOperation(first.activeKeyIDs, 22)
  assert.deepEqual([...afterSecondFinishes], [11])
})

test("a completed save only closes editing for its own row", () => {
  assert.equal(finishKeyEditing(22, 11), 22)
  assert.equal(finishKeyEditing(11, 11), null)
  assert.equal(finishKeyEditing(null, 11), null)
})
