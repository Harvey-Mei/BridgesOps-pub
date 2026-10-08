export interface GatewayGroupCachePaginationState {
  cursors: string[]
  pageIndex: number
  pageSize: number
}

export function createGatewayGroupCachePagination(pageSize = 50): GatewayGroupCachePaginationState {
  return { cursors: [""], pageIndex: 0, pageSize }
}

export function gatewayGroupCacheCurrentCursor(state: GatewayGroupCachePaginationState) {
  return state.cursors[state.pageIndex] ?? ""
}

export function gatewayGroupCachePageNumber(state: GatewayGroupCachePaginationState) {
  return state.pageIndex + 1
}

export function gatewayGroupCacheCanGoBack(state: GatewayGroupCachePaginationState) {
  return state.pageIndex > 0
}

export function gatewayGroupCacheGoNext(state: GatewayGroupCachePaginationState, nextCursor: string | null | undefined): GatewayGroupCachePaginationState {
  const cursor = nextCursor?.trim() ?? ""
  if (!cursor) return state
  const nextIndex = state.pageIndex + 1
  if (state.cursors[nextIndex] === cursor) {
    return { ...state, pageIndex: nextIndex }
  }
  return {
    ...state,
    cursors: [...state.cursors.slice(0, nextIndex), cursor],
    pageIndex: nextIndex,
  }
}

export function gatewayGroupCacheGoBack(state: GatewayGroupCachePaginationState): GatewayGroupCachePaginationState {
  if (!gatewayGroupCacheCanGoBack(state)) return state
  return { ...state, pageIndex: state.pageIndex - 1 }
}

export function resetGatewayGroupCachePagination(state: GatewayGroupCachePaginationState, pageSize = state.pageSize): GatewayGroupCachePaginationState {
  if (state.pageIndex === 0 && state.cursors.length === 1 && state.cursors[0] === "" && state.pageSize === pageSize) return state
  return createGatewayGroupCachePagination(pageSize)
}
