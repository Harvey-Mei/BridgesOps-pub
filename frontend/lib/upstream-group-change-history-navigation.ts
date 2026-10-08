export type UpstreamGroupChangeHistoryCursorState = {
  page: number
  cursor: string
  previousCursors: string[]
}

export type UpstreamGroupChangeHistoryRequestToken = {
  key: string
  generation: number
}

export function initialUpstreamGroupChangeHistoryCursorState(): UpstreamGroupChangeHistoryCursorState {
  return { page: 1, cursor: "", previousCursors: [] }
}

export function nextUpstreamGroupChangeHistoryCursorState(state: UpstreamGroupChangeHistoryCursorState, nextCursor: string | null | undefined): UpstreamGroupChangeHistoryCursorState {
  const cursor = String(nextCursor ?? "").trim()
  if (!cursor || cursor === state.cursor) return state
  return {
    page: Math.max(1, state.page) + 1,
    cursor,
    previousCursors: [...state.previousCursors, state.cursor],
  }
}

export function previousUpstreamGroupChangeHistoryCursorState(state: UpstreamGroupChangeHistoryCursorState): UpstreamGroupChangeHistoryCursorState {
  if (state.previousCursors.length === 0) return state
  const previousCursors = state.previousCursors.slice(0, -1)
  return {
    page: Math.max(1, state.page - 1),
    cursor: state.previousCursors[state.previousCursors.length - 1],
    previousCursors,
  }
}

export type UpstreamGroupChangeHistoryRequestGate = {
  startNavigation: (targetRequestKey: string) => boolean
  navigationTargetKey: () => string | null
  invalidateNavigation: () => void
  beginRequest: (requestKey: string) => UpstreamGroupChangeHistoryRequestToken
  matchesCurrentRequest: (token: UpstreamGroupChangeHistoryRequestToken, requestKey: string) => boolean
  cancelRequest: (token: UpstreamGroupChangeHistoryRequestToken) => void
  completeRequest: (token: UpstreamGroupChangeHistoryRequestToken, requestKey: string) => boolean
}

/**
 * Keeps a cursor transition synchronous even before React has rendered the
 * disabled button state. Request tokens make an old completion harmless when
 * a cursor/filter/view change starts another request with the same URL.
 */
export function createUpstreamGroupChangeHistoryRequestGate(): UpstreamGroupChangeHistoryRequestGate {
  let navigationTargetKey: string | null = null
  let requestGeneration = 0
  let activeRequest: UpstreamGroupChangeHistoryRequestToken | null = null

  const matchesCurrentRequest = (token: UpstreamGroupChangeHistoryRequestToken, requestKey: string) => (
    Boolean(requestKey)
    && token.key === requestKey
    && activeRequest?.key === token.key
    && activeRequest.generation === token.generation
  )

  return {
    startNavigation(targetRequestKey) {
      const target = targetRequestKey.trim()
      if (!target || navigationTargetKey !== null) return false
      navigationTargetKey = target
      return true
    },
    navigationTargetKey: () => navigationTargetKey,
    invalidateNavigation() {
      navigationTargetKey = null
    },
    beginRequest(requestKey) {
      const token = { key: requestKey, generation: requestGeneration + 1 }
      requestGeneration = token.generation
      activeRequest = token
      return token
    },
    matchesCurrentRequest,
    cancelRequest(token) {
      if (activeRequest?.key === token.key && activeRequest.generation === token.generation) activeRequest = null
    },
    completeRequest(token, requestKey) {
      if (!matchesCurrentRequest(token, requestKey)) return false
      activeRequest = null
      if (navigationTargetKey !== token.key) return false
      navigationTargetKey = null
      return true
    },
  }
}
