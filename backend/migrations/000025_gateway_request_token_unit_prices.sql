-- Token unit prices must keep uncached input, output, cache creation, and
-- cache reads separate. These additive fields preserve the component usage
-- and the request-ratio-attributed input/output costs in the 15-minute ledger;
-- the v5 background rebuild fills historical rows without scanning raw logs
-- from dashboard reads.
ALTER TABLE public.gateway_request_aggregates
    ADD COLUMN IF NOT EXISTS performance_input_tokens bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS performance_output_tokens bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS performance_input_priced_tokens bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS performance_output_priced_tokens bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS performance_cache_creation_tokens bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS performance_cache_read_tokens bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS performance_input_cost double precision NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS performance_output_cost double precision NOT NULL DEFAULT 0;
