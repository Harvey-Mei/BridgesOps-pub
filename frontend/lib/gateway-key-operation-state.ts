export type KeyOperationStart = {
  activeKeyIDs: Set<number>
  started: boolean
}

export function beginKeyOperation(activeKeyIDs: ReadonlySet<number>, keyID: number): KeyOperationStart {
  if (activeKeyIDs.has(keyID)) {
    return { activeKeyIDs: new Set(activeKeyIDs), started: false }
  }
  const next = new Set(activeKeyIDs)
  next.add(keyID)
  return { activeKeyIDs: next, started: true }
}

export function finishKeyOperation(activeKeyIDs: ReadonlySet<number>, keyID: number): Set<number> {
  const next = new Set(activeKeyIDs)
  next.delete(keyID)
  return next
}

export function finishKeyEditing(currentEditingID: number | null, completedKeyID: number): number | null {
  return currentEditingID === completedKeyID ? null : currentEditingID
}
