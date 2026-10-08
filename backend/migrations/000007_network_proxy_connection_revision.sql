-- Keep proxy client caches stable across health-test telemetry updates while
-- still invalidating them whenever an administrator changes an endpoint.
ALTER TABLE public.network_proxies
    ADD COLUMN IF NOT EXISTS connection_revision bigint NOT NULL DEFAULT 1;

UPDATE public.network_proxies
SET connection_revision = 1
WHERE connection_revision < 1;
