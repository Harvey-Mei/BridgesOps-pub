CREATE TABLE public.gateway_image_operation_capabilities (
    id BIGSERIAL PRIMARY KEY,
    channel_id BIGINT NOT NULL REFERENCES public.channels(id) ON DELETE CASCADE,
    upstream_group_id BIGINT NOT NULL REFERENCES public.gateway_upstream_groups(id) ON DELETE CASCADE,
    canonical_group_name VARCHAR(128) NOT NULL,
    presence_generation BIGINT NOT NULL,
    model_name VARCHAR(256) NOT NULL,
    operation VARCHAR(32) NOT NULL,
    protocol VARCHAR(32) NOT NULL,
    endpoint_path VARCHAR(128) NOT NULL,
    task_mode VARCHAR(16) NOT NULL DEFAULT 'unknown',
    idempotency_key_supported BOOLEAN NOT NULL DEFAULT FALSE,
    idempotency_header VARCHAR(128) NOT NULL DEFAULT '',
    task_id_returned BOOLEAN NOT NULL DEFAULT FALSE,
    task_id_json_path VARCHAR(256) NOT NULL DEFAULT '',
    task_query_supported BOOLEAN NOT NULL DEFAULT FALSE,
    task_query_method VARCHAR(16) NOT NULL DEFAULT '',
    task_query_path_template VARCHAR(512) NOT NULL DEFAULT '',
    task_status_json_path VARCHAR(256) NOT NULL DEFAULT '',
    task_success_values JSONB NOT NULL DEFAULT '[]'::jsonb,
    task_failure_values JSONB NOT NULL DEFAULT '[]'::jsonb,
    evidence_source VARCHAR(32) NOT NULL,
    evidence_detail VARCHAR(64) NOT NULL DEFAULT '',
    evidence_observed_at TIMESTAMPTZ NOT NULL,
    configured_by VARCHAR(128) NOT NULL DEFAULT '',
    capability_version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT gateway_image_capability_generation_positive CHECK (presence_generation > 0),
    CONSTRAINT gateway_image_capability_version_positive CHECK (capability_version > 0),
    CONSTRAINT gateway_image_capability_group_canonical CHECK (
        canonical_group_name <> '' AND canonical_group_name = lower(btrim(canonical_group_name))
    ),
    CONSTRAINT gateway_image_capability_operation CHECK (operation = 'image_generation'),
    CONSTRAINT gateway_image_capability_protocol CHECK (protocol IN ('openai_images', 'openai_responses')),
    CONSTRAINT gateway_image_capability_endpoint CHECK (
        (protocol = 'openai_images' AND endpoint_path = '/v1/images/generations') OR
        (protocol = 'openai_responses' AND endpoint_path = '/v1/responses')
    ),
    CONSTRAINT gateway_image_capability_task_mode CHECK (task_mode IN ('unknown', 'sync', 'async', 'sync_async')),
    CONSTRAINT gateway_image_capability_idempotency_shape CHECK (
        (idempotency_key_supported AND idempotency_header <> '') OR
        (NOT idempotency_key_supported AND idempotency_header = '')
    ),
    CONSTRAINT gateway_image_capability_task_id_shape CHECK (
        (task_id_returned AND task_id_json_path <> '') OR
        (NOT task_id_returned AND task_id_json_path = '')
    ),
    CONSTRAINT gateway_image_capability_task_query_shape CHECK (
        (task_query_supported AND task_query_method = 'GET' AND task_query_path_template <> '' AND task_status_json_path <> '') OR
        (NOT task_query_supported AND task_query_method = '' AND task_query_path_template = '' AND
         task_status_json_path = '' AND task_success_values = '[]'::jsonb AND task_failure_values = '[]'::jsonb)
    ),
    CONSTRAINT gateway_image_capability_evidence_source CHECK (
        evidence_source IN ('observed_success', 'provider_metadata', 'admin_config')
    ),
    CONSTRAINT gateway_image_capability_evidence_shape CHECK (
        (evidence_source = 'observed_success' AND evidence_detail IN ('observed_sync_response', 'observed_async_task') AND configured_by = '') OR
        (evidence_source = 'provider_metadata' AND evidence_detail = 'provider_metadata' AND configured_by = '') OR
        (evidence_source = 'admin_config' AND evidence_detail = 'administrator_configuration' AND configured_by <> '')
    ),
    CONSTRAINT gateway_image_capability_json_arrays CHECK (
        jsonb_typeof(task_success_values) = 'array' AND
        jsonb_typeof(task_failure_values) = 'array'
    ),
    UNIQUE (upstream_group_id, presence_generation, model_name, operation, protocol)
);

CREATE INDEX gateway_image_operation_capabilities_route_idx
    ON public.gateway_image_operation_capabilities (channel_id, canonical_group_name, model_name);

CREATE INDEX gateway_image_operation_capabilities_evidence_idx
    ON public.gateway_image_operation_capabilities (evidence_source, evidence_observed_at DESC);

CREATE TABLE public.gateway_image_operation_capability_audits (
    id BIGSERIAL PRIMARY KEY,
    capability_id BIGINT REFERENCES public.gateway_image_operation_capabilities(id) ON DELETE SET NULL,
    channel_id BIGINT NOT NULL,
    upstream_group_id BIGINT NOT NULL,
    canonical_group_name VARCHAR(128) NOT NULL,
    presence_generation BIGINT NOT NULL,
    model_name VARCHAR(256) NOT NULL,
    operation VARCHAR(32) NOT NULL,
    protocol VARCHAR(32) NOT NULL,
    action VARCHAR(32) NOT NULL,
    evidence_source VARCHAR(32) NOT NULL,
    evidence_observed_at TIMESTAMPTZ NOT NULL,
    evidence_reference_kind VARCHAR(32) NOT NULL DEFAULT '',
    evidence_reference_id BIGINT NOT NULL DEFAULT 0,
    actor VARCHAR(128) NOT NULL DEFAULT 'system',
    capability_version BIGINT NOT NULL,
    snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT gateway_image_capability_audits_generation_positive CHECK (presence_generation > 0),
    CONSTRAINT gateway_image_capability_audits_version_positive CHECK (capability_version > 0),
    CONSTRAINT gateway_image_capability_audits_action CHECK (
        action IN ('configured', 'observed', 'provider_metadata', 'evidence_ignored')
    ),
    CONSTRAINT gateway_image_capability_audits_evidence_source CHECK (
        evidence_source IN ('observed_success', 'provider_metadata', 'admin_config')
    ),
    CONSTRAINT gateway_image_capability_audits_snapshot_object CHECK (jsonb_typeof(snapshot) = 'object')
);

CREATE INDEX gateway_image_operation_capability_audits_route_idx
    ON public.gateway_image_operation_capability_audits (
        channel_id, canonical_group_name, model_name, created_at DESC
    );

CREATE INDEX gateway_image_operation_capability_audits_capability_idx
    ON public.gateway_image_operation_capability_audits (capability_id, created_at DESC);
