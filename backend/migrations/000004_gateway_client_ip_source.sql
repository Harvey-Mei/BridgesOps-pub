ALTER TABLE public.gateway_request_logs
    ADD COLUMN IF NOT EXISTS client_ip_source character varying(32);
