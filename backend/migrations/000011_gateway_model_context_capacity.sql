ALTER TABLE public.gateway_model_capabilities
    ADD COLUMN context_window_tokens BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN safe_input_tokens BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN context_safety_tokens BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN context_limit_observed_at TIMESTAMPTZ;
