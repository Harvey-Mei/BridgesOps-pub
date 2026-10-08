-- Convert any legacy dangling binding into an explicit failed-closed channel
-- before enforcing the parent-row reference. proxy_enabled remains true so a
-- channel cannot fall back to direct traffic because of this repair.
UPDATE public.channels AS channel
SET network_proxy_id = NULL,
    proxy_enabled = TRUE
WHERE channel.network_proxy_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM public.network_proxies AS proxy
      WHERE proxy.id = channel.network_proxy_id
  );

ALTER TABLE public.channels
    ADD CONSTRAINT fk_channels_network_proxy
    FOREIGN KEY (network_proxy_id)
    REFERENCES public.network_proxies (id)
    ON UPDATE RESTRICT
    ON DELETE RESTRICT
    NOT VALID;

ALTER TABLE public.channels
    VALIDATE CONSTRAINT fk_channels_network_proxy;
