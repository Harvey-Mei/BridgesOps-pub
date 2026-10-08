ALTER TABLE public.gateway_request_logs
    ADD COLUMN IF NOT EXISTS route_selection_rank bigint NOT NULL DEFAULT 0;

ALTER TABLE public.gateway_request_logs
    ADD COLUMN IF NOT EXISTS session_affinity_origin_request_id bigint;

CREATE INDEX IF NOT EXISTS idx_gateway_logs_affinity_origin
    ON public.gateway_request_logs USING btree (session_affinity_origin_request_id);
