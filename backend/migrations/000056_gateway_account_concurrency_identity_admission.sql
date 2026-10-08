-- Project account concurrency only through a binding that still belongs to
-- the channel's current provider identity revision. The view deliberately
-- omits both account identity hashes; callers receive only the internal record
-- key and the bounded capability fact needed for request admission.
CREATE VIEW public.gateway_current_upstream_account_concurrency AS
SELECT bindings.channel_id,
       bindings.channel_identity_revision,
       bindings.upstream_account_id AS account_record_id,
       accounts.concurrency_limit,
       accounts.concurrency_scope,
       accounts.concurrency_source,
       accounts.concurrency_observed_at
FROM public.gateway_channel_upstream_account_bindings AS bindings
JOIN public.channels AS channels
  ON channels.id = bindings.channel_id
 AND channels.type = bindings.provider
 AND channels.upstream_account_identity_revision = bindings.channel_identity_revision
JOIN public.gateway_upstream_accounts AS accounts
  ON accounts.id = bindings.upstream_account_id
 AND accounts.provider = bindings.provider;
