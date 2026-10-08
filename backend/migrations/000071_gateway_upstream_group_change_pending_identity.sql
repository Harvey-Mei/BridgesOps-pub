WITH duplicate_pending AS (
    SELECT id
    FROM (
        SELECT id,
               ROW_NUMBER() OVER (
                   PARTITION BY upstream_group_id, presence_generation
                   ORDER BY id DESC
               ) AS identity_rank
        FROM public.gateway_upstream_group_changes
        WHERE status = 'pending'
          AND kind = 'added'
          AND upstream_group_id IS NOT NULL
          AND presence_generation IS NOT NULL
    ) AS ranked_pending
    WHERE identity_rank > 1
)
UPDATE public.gateway_upstream_group_changes AS pending_change
SET status = 'superseded',
    assignment_state = 'superseded',
    assignment_reason_code = 'duplicate_pending_identity',
    evaluated_at = CURRENT_TIMESTAMP,
    resolved_at = CURRENT_TIMESTAMP,
    updated_at = CURRENT_TIMESTAMP
FROM duplicate_pending
WHERE pending_change.id = duplicate_pending.id;

CREATE UNIQUE INDEX gateway_upstream_group_changes_pending_added_identity_idx
    ON public.gateway_upstream_group_changes (upstream_group_id, presence_generation)
    WHERE status = 'pending'
      AND kind = 'added'
      AND upstream_group_id IS NOT NULL
      AND presence_generation IS NOT NULL;
