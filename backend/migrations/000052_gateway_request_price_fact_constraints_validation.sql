ALTER TABLE public.gateway_request_logs
    VALIDATE CONSTRAINT fk_gateway_request_logs_price_fact;

ALTER TABLE public.gateway_request_logs
    VALIDATE CONSTRAINT chk_gateway_request_logs_price_identity_nonnegative;

ALTER TABLE public.gateway_request_logs
    VALIDATE CONSTRAINT chk_gateway_request_logs_price_values;

ALTER TABLE public.gateway_request_logs
    VALIDATE CONSTRAINT chk_gateway_request_logs_price_fact_shape;

ALTER TABLE public.gateway_request_logs
    VALIDATE CONSTRAINT chk_gateway_request_logs_usage_schema_shape;

ALTER TABLE public.gateway_request_logs
    VALIDATE CONSTRAINT chk_gateway_request_logs_unavailable_cost_shape;

ALTER TABLE public.gateway_request_dispatch_audits
    VALIDATE CONSTRAINT fk_gateway_request_dispatch_price_fact;

ALTER TABLE public.gateway_request_dispatch_audits
    VALIDATE CONSTRAINT chk_gateway_request_dispatch_price_identity_nonnegative;

ALTER TABLE public.gateway_request_dispatch_audits
    VALIDATE CONSTRAINT chk_gateway_request_dispatch_price_values;

ALTER TABLE public.gateway_request_dispatch_audits
    VALIDATE CONSTRAINT chk_gateway_request_dispatch_price_fact_shape;

ALTER TABLE public.gateway_request_dispatch_audits
    VALIDATE CONSTRAINT chk_gateway_request_dispatch_usage_schema_shape;

ALTER TABLE public.gateway_request_dispatch_audits
    VALIDATE CONSTRAINT chk_gateway_request_dispatch_unavailable_reason;

ALTER TABLE public.gateway_request_aggregates
    VALIDATE CONSTRAINT chk_gateway_request_aggregates_unavailable_requests;
