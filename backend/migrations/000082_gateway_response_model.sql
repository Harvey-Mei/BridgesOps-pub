-- Keep the response's own model separate from the requested, dispatched and
-- reconciled billing models. Historical requests have no response evidence.
ALTER TABLE public.gateway_request_logs
    ADD COLUMN upstream_response_model character varying(256) NOT NULL DEFAULT '';
