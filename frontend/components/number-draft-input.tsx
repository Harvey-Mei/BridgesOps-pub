import { useEffect, useRef, useState } from "react"
import { Input } from "@/components/ui/input"

type NumberDraftInputProps = {
  draftKey: string
  value: number
  min?: number
  max?: number
  integer?: boolean
  onDraftDirtyChange?: (key: string, dirty: boolean) => void
  onValueChange: (value: number) => void
}

export function NumberDraftInput({
  draftKey,
  value,
  min,
  max,
  integer = true,
  onDraftDirtyChange,
  onValueChange,
}: NumberDraftInputProps) {
  const [draft, setDraft] = useState(String(value))
  const skipBlurCommit = useRef(false)

  useEffect(() => {
    setDraft(String(value))
  }, [value])

  function normalize(parsed: number) {
    let next = integer ? Math.round(parsed) : parsed
    if (min != null) next = Math.max(min, next)
    if (max != null) next = Math.min(max, next)
    return next
  }

  function setDirty(dirty: boolean) {
    onDraftDirtyChange?.(draftKey, dirty)
  }

  function reset() {
    setDraft(String(value))
    setDirty(false)
  }

  function commit() {
    if (skipBlurCommit.current) {
      skipBlurCommit.current = false
      return
    }
    if (draft.trim() === "") {
      reset()
      return
    }
    const parsed = Number(draft)
    if (!Number.isFinite(parsed)) {
      reset()
      return
    }
    const next = normalize(parsed)
    setDraft(String(next))
    if (next !== value) onValueChange(next)
    setDirty(false)
  }

  function draftIsDirty(nextDraft: string) {
    if (nextDraft.trim() === "") return true
    const parsed = Number(nextDraft)
    if (!Number.isFinite(parsed)) return true
    return normalize(parsed) !== value
  }

  return (
    <Input
      type="number"
      min={min}
      max={max}
      step={integer ? 1 : "any"}
      value={draft}
      onChange={(event) => {
        const nextDraft = event.target.value
        setDraft(nextDraft)
        setDirty(draftIsDirty(nextDraft))
      }}
      onBlur={commit}
      onKeyDown={(event) => {
        if (event.key === "Enter") event.currentTarget.blur()
        if (event.key === "Escape") {
          skipBlurCommit.current = true
          reset()
          event.currentTarget.blur()
        }
      }}
    />
  )
}
