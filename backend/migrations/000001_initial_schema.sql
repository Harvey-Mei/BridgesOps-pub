-- PostgreSQL initial schema generated from the current GORM model set.
-- This file is immutable after release; schema changes require a new migration.
-- codex_radar_states.payload_json is TEXT here; the model's LONGTEXT tag is
-- retained only for MySQL compatibility and is not used by PostgreSQL runtime.
CREATE TABLE public.auth_sessions (
    channel_id bigint NOT NULL,
    user_id character varying(64),
    access_token_cipher text,
    refresh_token_cipher text,
    cookie_cipher text,
    csrf_token_cipher character varying(1024),
    expires_at timestamp with time zone,
    last_login_at timestamp with time zone,
    updated_at timestamp with time zone
);
CREATE SEQUENCE public.auth_sessions_channel_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.auth_sessions_channel_id_seq OWNED BY public.auth_sessions.channel_id;
CREATE TABLE public.balance_snapshots (
    id bigint NOT NULL,
    channel_id bigint NOT NULL,
    balance numeric NOT NULL,
    raw_balance numeric,
    funding_factor numeric,
    cost_basis_version bigint DEFAULT 0 NOT NULL,
    sampled_at timestamp with time zone NOT NULL
);
CREATE SEQUENCE public.balance_snapshots_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.balance_snapshots_id_seq OWNED BY public.balance_snapshots.id;
CREATE TABLE public.captcha_configs (
    id bigint NOT NULL,
    name character varying(128) NOT NULL,
    type character varying(32) NOT NULL,
    api_key_cipher character varying(1024),
    endpoint character varying(512),
    extra text,
    enabled boolean DEFAULT true,
    proxy_enabled boolean DEFAULT false,
    last_balance numeric,
    balance_unit character varying(32),
    balance_at timestamp with time zone,
    balance_error text,
    created_at timestamp with time zone,
    updated_at timestamp with time zone
);
CREATE SEQUENCE public.captcha_configs_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.captcha_configs_id_seq OWNED BY public.captcha_configs.id;
CREATE TABLE public.channel_cost_states (
    channel_id bigint NOT NULL,
    raw_balance numeric DEFAULT 0 NOT NULL,
    actual_inventory numeric DEFAULT 0 NOT NULL,
    funding_factor numeric DEFAULT 1 NOT NULL,
    raw_today_cost numeric DEFAULT 0 NOT NULL,
    actual_today_cost numeric DEFAULT 0 NOT NULL,
    raw_total_cost numeric DEFAULT 0 NOT NULL,
    actual_total_cost numeric DEFAULT 0 NOT NULL,
    cost_sampled_at timestamp with time zone,
    balance_sampled_at timestamp with time zone,
    version bigint DEFAULT 1 NOT NULL,
    source character varying(32) DEFAULT 'default'::character varying NOT NULL,
    status character varying(32) DEFAULT 'ready'::character varying NOT NULL,
    last_error text,
    created_at timestamp with time zone,
    updated_at timestamp with time zone
);
CREATE SEQUENCE public.channel_cost_states_channel_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.channel_cost_states_channel_id_seq OWNED BY public.channel_cost_states.channel_id;
CREATE TABLE public.channel_funding_candidates (
    id bigint NOT NULL,
    channel_id bigint NOT NULL,
    source character varying(32) NOT NULL,
    external_id character varying(191) NOT NULL,
    status character varying(24) DEFAULT 'pending'::character varying NOT NULL,
    paid_amount numeric,
    credited_amount numeric NOT NULL,
    raw_balance_before numeric NOT NULL,
    raw_balance_detected numeric NOT NULL,
    funding_factor_before numeric DEFAULT 1 NOT NULL,
    raw_today_cost_before numeric DEFAULT 0 NOT NULL,
    raw_total_cost_before numeric DEFAULT 0 NOT NULL,
    occurred_at timestamp with time zone,
    detected_at timestamp with time zone NOT NULL,
    resolved_event_id bigint,
    note character varying(1024),
    created_at timestamp with time zone,
    updated_at timestamp with time zone
);
CREATE SEQUENCE public.channel_funding_candidates_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.channel_funding_candidates_id_seq OWNED BY public.channel_funding_candidates.id;
CREATE TABLE public.channel_funding_events (
    id bigint NOT NULL,
    channel_id bigint NOT NULL,
    kind character varying(32) NOT NULL,
    paid_amount numeric NOT NULL,
    credited_amount numeric NOT NULL,
    raw_balance_before numeric NOT NULL,
    raw_balance_after numeric NOT NULL,
    actual_inventory_before numeric NOT NULL,
    actual_inventory_after numeric NOT NULL,
    funding_factor_before numeric NOT NULL,
    funding_factor_after numeric NOT NULL,
    raw_today_cost_before numeric DEFAULT 0 NOT NULL,
    raw_total_cost_before numeric DEFAULT 0 NOT NULL,
    note character varying(1024),
    effective_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone NOT NULL
);
CREATE SEQUENCE public.channel_funding_events_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.channel_funding_events_id_seq OWNED BY public.channel_funding_events.id;
CREATE TABLE public.channels (
    id bigint NOT NULL,
    name character varying(128) NOT NULL,
    type character varying(32) NOT NULL,
    gateway_app character varying(32) DEFAULT 'codex'::character varying NOT NULL,
    site_url character varying(512) NOT NULL,
    username character varying(256) NOT NULL,
    sort_order bigint DEFAULT 1 NOT NULL,
    password_cipher character varying(4096) NOT NULL,
    credential_mode character varying(16) DEFAULT 'password'::character varying NOT NULL,
    login_extra_params text,
    turnstile_enabled boolean DEFAULT false,
    ignore_announcements boolean DEFAULT false,
    subscription_enabled boolean DEFAULT false,
    proxy_enabled boolean DEFAULT false,
    captcha_config_id bigint,
    balance_threshold numeric DEFAULT 0,
    recharge_multiplier numeric,
    recharge_multiplier_mode character varying(16) DEFAULT 'divide'::character varying NOT NULL,
    monitor_enabled boolean DEFAULT true,
    latency_probe_enabled boolean DEFAULT true,
    last_balance numeric,
    raw_last_balance numeric,
    funding_factor numeric DEFAULT 1 NOT NULL,
    funding_factor_source character varying(32) DEFAULT 'default'::character varying NOT NULL,
    last_balance_at timestamp with time zone,
    today_cost numeric,
    raw_today_cost numeric,
    today_cost_at timestamp with time zone,
    total_cost numeric,
    raw_total_cost numeric,
    last_error text,
    sync_failure_count bigint DEFAULT 0 NOT NULL,
    next_sync_retry_at timestamp with time zone,
    created_at timestamp with time zone,
    updated_at timestamp with time zone
);
CREATE SEQUENCE public.channels_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.channels_id_seq OWNED BY public.channels.id;
CREATE TABLE public.codex_radar_model_points (
    id bigint NOT NULL,
    source_url character varying(512) NOT NULL,
    snapshot_hash character varying(64) NOT NULL,
    config_key character varying(128) NOT NULL,
    label character varying(256),
    model character varying(256),
    reasoning_effort character varying(64),
    observed_at timestamp with time zone NOT NULL,
    score numeric,
    status character varying(32),
    passed bigint,
    tasks bigint,
    valid_tasks bigint,
    invalid bigint,
    wall_seconds numeric,
    cost_usd numeric,
    total_tokens bigint,
    created_at timestamp with time zone
);
CREATE SEQUENCE public.codex_radar_model_points_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.codex_radar_model_points_id_seq OWNED BY public.codex_radar_model_points.id;
CREATE TABLE public.codex_radar_states (
    id bigint NOT NULL,
    source_url character varying(512) NOT NULL,
    payload_json text NOT NULL,
    payload_hash character varying(64) NOT NULL,
    model_hash character varying(64) NOT NULL,
    fetched_at timestamp with time zone,
    last_attempt_at timestamp with time zone,
    last_error text,
    created_at timestamp with time zone,
    updated_at timestamp with time zone
);
CREATE SEQUENCE public.codex_radar_states_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.codex_radar_states_id_seq OWNED BY public.codex_radar_states.id;
CREATE TABLE public.cost_snapshots (
    id bigint NOT NULL,
    channel_id bigint NOT NULL,
    today_cost numeric NOT NULL,
    raw_today_cost numeric,
    total_cost numeric DEFAULT 0 NOT NULL,
    raw_total_cost numeric,
    funding_factor numeric,
    cost_basis_version bigint DEFAULT 0 NOT NULL,
    sampled_at timestamp with time zone NOT NULL
);
CREATE SEQUENCE public.cost_snapshots_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.cost_snapshots_id_seq OWNED BY public.cost_snapshots.id;
CREATE TABLE public.gateway_candidate_group_members (
    id bigint NOT NULL,
    candidate_group_id bigint NOT NULL,
    channel_id bigint NOT NULL,
    group_name character varying(128) NOT NULL,
    created_at timestamp with time zone
);
CREATE SEQUENCE public.gateway_candidate_group_members_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.gateway_candidate_group_members_id_seq OWNED BY public.gateway_candidate_group_members.id;
CREATE TABLE public.gateway_candidate_groups (
    id bigint NOT NULL,
    name character varying(128) NOT NULL,
    model_mapping_family character varying(32),
    created_at timestamp with time zone,
    updated_at timestamp with time zone
);
CREATE SEQUENCE public.gateway_candidate_groups_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.gateway_candidate_groups_id_seq OWNED BY public.gateway_candidate_groups.id;
CREATE TABLE public.gateway_downstream_keys (
    id bigint NOT NULL,
    name character varying(128) NOT NULL,
    app character varying(32) DEFAULT 'codex'::character varying NOT NULL,
    model_family character varying(32) DEFAULT 'codex'::character varying NOT NULL,
    candidate_group_id bigint DEFAULT 0 NOT NULL,
    key_group_id bigint DEFAULT 0 NOT NULL,
    model_mapping_strategy character varying(32) DEFAULT 'off'::character varying NOT NULL,
    model_mapping_model character varying(256),
    model_mapping_reasoning_effort character varying(64),
    key_hash character varying(64) NOT NULL,
    api_key_cipher text,
    key_prefix character varying(24) NOT NULL,
    smart_routing boolean DEFAULT true NOT NULL,
    manual_channel_id bigint DEFAULT 0,
    manual_group_name character varying(128),
    enabled boolean DEFAULT true,
    last_used_at timestamp with time zone,
    created_at timestamp with time zone,
    updated_at timestamp with time zone
);
CREATE SEQUENCE public.gateway_downstream_keys_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.gateway_downstream_keys_id_seq OWNED BY public.gateway_downstream_keys.id;
CREATE TABLE public.gateway_key_groups (
    id bigint NOT NULL,
    name character varying(128) NOT NULL,
    created_at timestamp with time zone,
    updated_at timestamp with time zone
);
CREATE SEQUENCE public.gateway_key_groups_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.gateway_key_groups_id_seq OWNED BY public.gateway_key_groups.id;
CREATE TABLE public.gateway_model_capabilities (
    id bigint NOT NULL,
    channel_id bigint NOT NULL,
    group_name character varying(128) NOT NULL,
    model_name character varying(256) NOT NULL,
    supported boolean NOT NULL,
    last_checked_at timestamp with time zone NOT NULL,
    last_error text,
    created_at timestamp with time zone,
    updated_at timestamp with time zone
);
CREATE SEQUENCE public.gateway_model_capabilities_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.gateway_model_capabilities_id_seq OWNED BY public.gateway_model_capabilities.id;
CREATE TABLE public.gateway_request_aggregates (
    id bigint NOT NULL,
    bucket_start timestamp with time zone NOT NULL,
    downstream_key_id bigint NOT NULL,
    channel_id bigint NOT NULL,
    channel_name_snapshot character varying(256) DEFAULT ''::character varying NOT NULL,
    model_name character varying(256) NOT NULL,
    cost_source character varying(32) NOT NULL,
    route_reason character varying(32) NOT NULL,
    result character varying(16) NOT NULL,
    requests bigint DEFAULT 0 NOT NULL,
    prompt_tokens bigint DEFAULT 0 NOT NULL,
    completion_tokens bigint DEFAULT 0 NOT NULL,
    cache_creation_tokens bigint DEFAULT 0 NOT NULL,
    cache_read_tokens bigint DEFAULT 0 NOT NULL,
    total_tokens bigint DEFAULT 0 NOT NULL,
    total_cost numeric DEFAULT 0 NOT NULL,
    estimated_cost numeric DEFAULT 0 NOT NULL,
    actual_cost numeric DEFAULT 0 NOT NULL,
    reported_cost numeric DEFAULT 0 NOT NULL,
    reconciled_cost numeric DEFAULT 0 NOT NULL,
    latency_total_ms bigint DEFAULT 0 NOT NULL,
    latency_samples bigint DEFAULT 0 NOT NULL,
    performance_requests bigint DEFAULT 0 NOT NULL,
    performance_tokens bigint DEFAULT 0 NOT NULL,
    performance_cost numeric DEFAULT 0 NOT NULL,
    first_request_at timestamp with time zone NOT NULL,
    last_request_at timestamp with time zone NOT NULL,
    version bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone,
    updated_at timestamp with time zone
);
CREATE SEQUENCE public.gateway_request_aggregates_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.gateway_request_aggregates_id_seq OWNED BY public.gateway_request_aggregates.id;
CREATE TABLE public.gateway_request_logs (
    id bigint NOT NULL,
    downstream_key_id bigint NOT NULL,
    channel_id bigint,
    channel_name_snapshot character varying(256),
    group_name character varying(128),
    group_ratio numeric,
    completion_ratio numeric,
    raw_group_ratio numeric,
    raw_completion_ratio numeric,
    funding_factor numeric,
    cost_basis_version bigint DEFAULT 0 NOT NULL,
    cost_basis_source character varying(32),
    model_name character varying(256),
    upstream_model_name character varying(256),
    reasoning_effort character varying(64),
    requested_model_name character varying(256),
    requested_reasoning_effort character varying(64),
    model_mapping_strategy character varying(32),
    model_mapping_details text,
    method character varying(16) NOT NULL,
    path character varying(512) NOT NULL,
    status_code bigint NOT NULL,
    first_byte_ms bigint DEFAULT 0 NOT NULL,
    latency_ms bigint NOT NULL,
    prompt_tokens bigint,
    completion_tokens bigint,
    cache_creation_tokens bigint,
    cache_read_tokens bigint,
    total_tokens bigint,
    cost numeric,
    raw_cost numeric,
    cost_estimated boolean,
    cost_source character varying(32),
    cost_status character varying(16) DEFAULT 'none'::character varying NOT NULL,
    upstream_key_id bigint,
    upstream_key_name character varying(128),
    upstream_request_id character varying(128),
    upstream_usage_id character varying(128),
    client_ip character varying(64),
    session_hash character varying(64),
    route_reason character varying(32),
    route_details text,
    route_attempted_channels text,
    cost_reconcile_at timestamp with time zone,
    cost_reconcile_tries bigint DEFAULT 0 NOT NULL,
    cost_reconcile_conflicts bigint DEFAULT 0 NOT NULL,
    cost_reconcile_error text,
    aggregate_version bigint DEFAULT 0 NOT NULL,
    error text,
    created_at timestamp with time zone NOT NULL
);
CREATE SEQUENCE public.gateway_request_logs_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.gateway_request_logs_id_seq OWNED BY public.gateway_request_logs.id;
CREATE TABLE public.gateway_route_exclusions (
    id bigint NOT NULL,
    downstream_key_id bigint NOT NULL,
    channel_id bigint NOT NULL,
    group_name character varying(128) NOT NULL,
    created_at timestamp with time zone,
    updated_at timestamp with time zone
);
CREATE SEQUENCE public.gateway_route_exclusions_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.gateway_route_exclusions_id_seq OWNED BY public.gateway_route_exclusions.id;
CREATE TABLE public.gateway_routing_failures (
    id bigint NOT NULL,
    channel_id bigint NOT NULL,
    group_name character varying(128) NOT NULL,
    occurred_at timestamp with time zone NOT NULL
);
CREATE SEQUENCE public.gateway_routing_failures_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.gateway_routing_failures_id_seq OWNED BY public.gateway_routing_failures.id;
CREATE TABLE public.gateway_upstream_group_changes (
    id bigint NOT NULL,
    channel_id bigint NOT NULL,
    group_name character varying(128) NOT NULL,
    kind character varying(16) NOT NULL,
    status character varying(16) DEFAULT 'pending'::character varying NOT NULL,
    upstream_key_id bigint,
    upstream_key_name character varying(128),
    detected_at timestamp with time zone NOT NULL,
    resolved_at timestamp with time zone,
    created_at timestamp with time zone,
    updated_at timestamp with time zone
);
CREATE SEQUENCE public.gateway_upstream_group_changes_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.gateway_upstream_group_changes_id_seq OWNED BY public.gateway_upstream_group_changes.id;
CREATE TABLE public.gateway_upstream_keys (
    id bigint NOT NULL,
    channel_id bigint NOT NULL,
    upstream_key_id bigint,
    upstream_key_name character varying(128),
    provisioning boolean DEFAULT false NOT NULL,
    provisioning_started_at timestamp with time zone,
    api_key_cipher text NOT NULL,
    api_key_prefix character varying(24) NOT NULL,
    group_name character varying(128),
    group_ratio numeric,
    model_families character varying(256),
    model_ids text,
    latency_probe_model character varying(256),
    latency_probe_enabled boolean DEFAULT true NOT NULL,
    models_synced_at timestamp with time zone,
    models_error text,
    enabled boolean DEFAULT true,
    last_used_at timestamp with time zone,
    last_error text,
    last_routing_failure_kind character varying(32),
    last_routing_failure_status bigint,
    last_routing_failure_at timestamp with time zone,
    routing_failure_count bigint DEFAULT 0 NOT NULL,
    routing_cooldown_until timestamp with time zone,
    created_at timestamp with time zone,
    updated_at timestamp with time zone
);
CREATE SEQUENCE public.gateway_upstream_keys_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.gateway_upstream_keys_id_seq OWNED BY public.gateway_upstream_keys.id;
CREATE TABLE public.monitor_logs (
    id bigint NOT NULL,
    channel_id bigint NOT NULL,
    job character varying(32) NOT NULL,
    success boolean NOT NULL,
    error_message text,
    duration_ms bigint,
    started_at timestamp with time zone NOT NULL,
    finished_at timestamp with time zone
);
CREATE SEQUENCE public.monitor_logs_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.monitor_logs_id_seq OWNED BY public.monitor_logs.id;
CREATE TABLE public.notification_channels (
    id bigint NOT NULL,
    name character varying(128) NOT NULL,
    type character varying(32) NOT NULL,
    config_cipher text NOT NULL,
    subscriptions character varying(4096) DEFAULT '[]'::character varying NOT NULL,
    enabled boolean DEFAULT true,
    proxy_enabled boolean DEFAULT false,
    created_at timestamp with time zone,
    updated_at timestamp with time zone
);
CREATE SEQUENCE public.notification_channels_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.notification_channels_id_seq OWNED BY public.notification_channels.id;
CREATE TABLE public.notification_cooldowns (
    channel_id bigint NOT NULL,
    event character varying(64) NOT NULL,
    last_sent_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone
);
CREATE TABLE public.notification_logs (
    id bigint NOT NULL,
    channel_id bigint NOT NULL,
    upstream_channel_id bigint DEFAULT 0 NOT NULL,
    event character varying(64) NOT NULL,
    subject character varying(512) NOT NULL,
    body text,
    success boolean NOT NULL,
    error_message text,
    sent_at timestamp with time zone NOT NULL
);
CREATE SEQUENCE public.notification_logs_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.notification_logs_id_seq OWNED BY public.notification_logs.id;
CREATE TABLE public.rate_change_logs (
    id bigint NOT NULL,
    channel_id bigint NOT NULL,
    model_name character varying(256) NOT NULL,
    old_ratio numeric,
    new_ratio numeric NOT NULL,
    old_completion_ratio numeric,
    new_completion_ratio numeric,
    funding_factor_before numeric DEFAULT 1 NOT NULL,
    funding_factor_after numeric DEFAULT 1 NOT NULL,
    old_effective_ratio numeric,
    new_effective_ratio numeric,
    old_effective_completion_ratio numeric,
    new_effective_completion_ratio numeric,
    change_reason character varying(32) DEFAULT 'upstream_rate'::character varying NOT NULL,
    changed_at timestamp with time zone NOT NULL
);
CREATE SEQUENCE public.rate_change_logs_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.rate_change_logs_id_seq OWNED BY public.rate_change_logs.id;
CREATE TABLE public.rate_snapshots (
    id bigint NOT NULL,
    channel_id bigint NOT NULL,
    model_name character varying(256) NOT NULL,
    description character varying(512),
    ratio numeric NOT NULL,
    completion_ratio numeric,
    first_seen_at timestamp with time zone,
    last_seen_at timestamp with time zone
);
CREATE SEQUENCE public.rate_snapshots_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.rate_snapshots_id_seq OWNED BY public.rate_snapshots.id;
CREATE TABLE public.system_metadata (
    key character varying(128) NOT NULL,
    value text NOT NULL,
    created_at timestamp with time zone,
    updated_at timestamp with time zone
);
CREATE TABLE public.upstream_announcements (
    id bigint NOT NULL,
    channel_id bigint NOT NULL,
    source_key character varying(512) NOT NULL,
    title character varying(512),
    content text NOT NULL,
    type character varying(64),
    link character varying(512),
    published_at timestamp with time zone,
    source_updated_at timestamp with time zone,
    first_seen_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone,
    updated_at timestamp with time zone
);
CREATE SEQUENCE public.upstream_announcements_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.upstream_announcements_id_seq OWNED BY public.upstream_announcements.id;
ALTER TABLE ONLY public.auth_sessions ALTER COLUMN channel_id SET DEFAULT nextval('public.auth_sessions_channel_id_seq'::regclass);
ALTER TABLE ONLY public.balance_snapshots ALTER COLUMN id SET DEFAULT nextval('public.balance_snapshots_id_seq'::regclass);
ALTER TABLE ONLY public.captcha_configs ALTER COLUMN id SET DEFAULT nextval('public.captcha_configs_id_seq'::regclass);
ALTER TABLE ONLY public.channel_cost_states ALTER COLUMN channel_id SET DEFAULT nextval('public.channel_cost_states_channel_id_seq'::regclass);
ALTER TABLE ONLY public.channel_funding_candidates ALTER COLUMN id SET DEFAULT nextval('public.channel_funding_candidates_id_seq'::regclass);
ALTER TABLE ONLY public.channel_funding_events ALTER COLUMN id SET DEFAULT nextval('public.channel_funding_events_id_seq'::regclass);
ALTER TABLE ONLY public.channels ALTER COLUMN id SET DEFAULT nextval('public.channels_id_seq'::regclass);
ALTER TABLE ONLY public.codex_radar_model_points ALTER COLUMN id SET DEFAULT nextval('public.codex_radar_model_points_id_seq'::regclass);
ALTER TABLE ONLY public.codex_radar_states ALTER COLUMN id SET DEFAULT nextval('public.codex_radar_states_id_seq'::regclass);
ALTER TABLE ONLY public.cost_snapshots ALTER COLUMN id SET DEFAULT nextval('public.cost_snapshots_id_seq'::regclass);
ALTER TABLE ONLY public.gateway_candidate_group_members ALTER COLUMN id SET DEFAULT nextval('public.gateway_candidate_group_members_id_seq'::regclass);
ALTER TABLE ONLY public.gateway_candidate_groups ALTER COLUMN id SET DEFAULT nextval('public.gateway_candidate_groups_id_seq'::regclass);
ALTER TABLE ONLY public.gateway_downstream_keys ALTER COLUMN id SET DEFAULT nextval('public.gateway_downstream_keys_id_seq'::regclass);
ALTER TABLE ONLY public.gateway_key_groups ALTER COLUMN id SET DEFAULT nextval('public.gateway_key_groups_id_seq'::regclass);
ALTER TABLE ONLY public.gateway_model_capabilities ALTER COLUMN id SET DEFAULT nextval('public.gateway_model_capabilities_id_seq'::regclass);
ALTER TABLE ONLY public.gateway_request_aggregates ALTER COLUMN id SET DEFAULT nextval('public.gateway_request_aggregates_id_seq'::regclass);
ALTER TABLE ONLY public.gateway_request_logs ALTER COLUMN id SET DEFAULT nextval('public.gateway_request_logs_id_seq'::regclass);
ALTER TABLE ONLY public.gateway_route_exclusions ALTER COLUMN id SET DEFAULT nextval('public.gateway_route_exclusions_id_seq'::regclass);
ALTER TABLE ONLY public.gateway_routing_failures ALTER COLUMN id SET DEFAULT nextval('public.gateway_routing_failures_id_seq'::regclass);
ALTER TABLE ONLY public.gateway_upstream_group_changes ALTER COLUMN id SET DEFAULT nextval('public.gateway_upstream_group_changes_id_seq'::regclass);
ALTER TABLE ONLY public.gateway_upstream_keys ALTER COLUMN id SET DEFAULT nextval('public.gateway_upstream_keys_id_seq'::regclass);
ALTER TABLE ONLY public.monitor_logs ALTER COLUMN id SET DEFAULT nextval('public.monitor_logs_id_seq'::regclass);
ALTER TABLE ONLY public.notification_channels ALTER COLUMN id SET DEFAULT nextval('public.notification_channels_id_seq'::regclass);
ALTER TABLE ONLY public.notification_logs ALTER COLUMN id SET DEFAULT nextval('public.notification_logs_id_seq'::regclass);
ALTER TABLE ONLY public.rate_change_logs ALTER COLUMN id SET DEFAULT nextval('public.rate_change_logs_id_seq'::regclass);
ALTER TABLE ONLY public.rate_snapshots ALTER COLUMN id SET DEFAULT nextval('public.rate_snapshots_id_seq'::regclass);
ALTER TABLE ONLY public.upstream_announcements ALTER COLUMN id SET DEFAULT nextval('public.upstream_announcements_id_seq'::regclass);
ALTER TABLE ONLY public.auth_sessions
    ADD CONSTRAINT auth_sessions_pkey PRIMARY KEY (channel_id);
ALTER TABLE ONLY public.balance_snapshots
    ADD CONSTRAINT balance_snapshots_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.captcha_configs
    ADD CONSTRAINT captcha_configs_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.channel_cost_states
    ADD CONSTRAINT channel_cost_states_pkey PRIMARY KEY (channel_id);
ALTER TABLE ONLY public.channel_funding_candidates
    ADD CONSTRAINT channel_funding_candidates_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.channel_funding_events
    ADD CONSTRAINT channel_funding_events_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.channels
    ADD CONSTRAINT channels_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.codex_radar_model_points
    ADD CONSTRAINT codex_radar_model_points_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.codex_radar_states
    ADD CONSTRAINT codex_radar_states_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.cost_snapshots
    ADD CONSTRAINT cost_snapshots_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.gateway_candidate_group_members
    ADD CONSTRAINT gateway_candidate_group_members_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.gateway_candidate_groups
    ADD CONSTRAINT gateway_candidate_groups_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.gateway_downstream_keys
    ADD CONSTRAINT gateway_downstream_keys_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.gateway_key_groups
    ADD CONSTRAINT gateway_key_groups_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.gateway_model_capabilities
    ADD CONSTRAINT gateway_model_capabilities_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.gateway_request_aggregates
    ADD CONSTRAINT gateway_request_aggregates_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.gateway_request_logs
    ADD CONSTRAINT gateway_request_logs_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.gateway_route_exclusions
    ADD CONSTRAINT gateway_route_exclusions_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.gateway_routing_failures
    ADD CONSTRAINT gateway_routing_failures_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.gateway_upstream_group_changes
    ADD CONSTRAINT gateway_upstream_group_changes_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.gateway_upstream_keys
    ADD CONSTRAINT gateway_upstream_keys_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.monitor_logs
    ADD CONSTRAINT monitor_logs_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.notification_channels
    ADD CONSTRAINT notification_channels_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.notification_cooldowns
    ADD CONSTRAINT notification_cooldowns_pkey PRIMARY KEY (channel_id, event);
ALTER TABLE ONLY public.notification_logs
    ADD CONSTRAINT notification_logs_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.rate_change_logs
    ADD CONSTRAINT rate_change_logs_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.rate_snapshots
    ADD CONSTRAINT rate_snapshots_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.system_metadata
    ADD CONSTRAINT system_metadata_pkey PRIMARY KEY (key);
ALTER TABLE ONLY public.upstream_announcements
    ADD CONSTRAINT upstream_announcements_pkey PRIMARY KEY (id);
CREATE UNIQUE INDEX idx_announcement_chan_source ON public.upstream_announcements USING btree (channel_id, source_key);
CREATE INDEX idx_balance_snapshots_channel_id ON public.balance_snapshots USING btree (channel_id);
CREATE INDEX idx_balance_snapshots_sampled_at ON public.balance_snapshots USING btree (sampled_at);
CREATE UNIQUE INDEX idx_captcha_configs_name ON public.captcha_configs USING btree (name);
CREATE INDEX idx_captcha_configs_type ON public.captcha_configs USING btree (type);
CREATE INDEX idx_channel_cost_states_status ON public.channel_cost_states USING btree (status);
CREATE INDEX idx_channel_funding_candidates_channel_id ON public.channel_funding_candidates USING btree (channel_id);
CREATE INDEX idx_channel_funding_candidates_detected_at ON public.channel_funding_candidates USING btree (detected_at);
CREATE INDEX idx_channel_funding_candidates_status ON public.channel_funding_candidates USING btree (status);
CREATE INDEX idx_channel_funding_events_channel_effective ON public.channel_funding_events USING btree (channel_id, effective_at);
CREATE INDEX idx_channel_funding_events_kind ON public.channel_funding_events USING btree (kind);
CREATE INDEX idx_channels_gateway_app ON public.channels USING btree (gateway_app);
CREATE INDEX idx_channels_monitor_retry ON public.channels USING btree (monitor_enabled, next_sync_retry_at);
CREATE UNIQUE INDEX idx_channels_name ON public.channels USING btree (name);
CREATE INDEX idx_channels_type ON public.channels USING btree (type);
CREATE INDEX idx_codex_radar_model_points_observed_at ON public.codex_radar_model_points USING btree (observed_at);
CREATE UNIQUE INDEX idx_codex_radar_point_snapshot_config ON public.codex_radar_model_points USING btree (source_url, snapshot_hash, config_key);
CREATE INDEX idx_codex_radar_point_source_config ON public.codex_radar_model_points USING btree (config_key);
CREATE INDEX idx_codex_radar_point_source_observed ON public.codex_radar_model_points USING btree (source_url, observed_at);
CREATE INDEX idx_codex_radar_states_model_hash ON public.codex_radar_states USING btree (model_hash);
CREATE UNIQUE INDEX idx_codex_radar_states_source_url ON public.codex_radar_states USING btree (source_url);
CREATE INDEX idx_cost_snapshots_channel_id ON public.cost_snapshots USING btree (channel_id);
CREATE INDEX idx_cost_snapshots_sampled_at ON public.cost_snapshots USING btree (sampled_at);
CREATE UNIQUE INDEX idx_funding_candidate_external ON public.channel_funding_candidates USING btree (channel_id, source, external_id);
CREATE UNIQUE INDEX idx_gateway_candidate_group_member ON public.gateway_candidate_group_members USING btree (candidate_group_id, channel_id, group_name);
CREATE INDEX idx_gateway_candidate_group_members_candidate_group_id ON public.gateway_candidate_group_members USING btree (candidate_group_id);
CREATE INDEX idx_gateway_candidate_group_members_channel_id ON public.gateway_candidate_group_members USING btree (channel_id);
CREATE UNIQUE INDEX idx_gateway_candidate_groups_name ON public.gateway_candidate_groups USING btree (name);
CREATE UNIQUE INDEX idx_gateway_channel_usage ON public.gateway_request_logs USING btree (channel_id, upstream_usage_id);
CREATE INDEX idx_gateway_downstream_keys_app ON public.gateway_downstream_keys USING btree (app);
CREATE INDEX idx_gateway_downstream_keys_candidate_group_id ON public.gateway_downstream_keys USING btree (candidate_group_id);
CREATE INDEX idx_gateway_downstream_keys_key_group_id ON public.gateway_downstream_keys USING btree (key_group_id);
CREATE UNIQUE INDEX idx_gateway_downstream_keys_key_hash ON public.gateway_downstream_keys USING btree (key_hash);
CREATE INDEX idx_gateway_downstream_keys_key_prefix ON public.gateway_downstream_keys USING btree (key_prefix);
CREATE INDEX idx_gateway_downstream_keys_model_family ON public.gateway_downstream_keys USING btree (model_family);
CREATE UNIQUE INDEX idx_gateway_key_groups_name ON public.gateway_key_groups USING btree (name);
CREATE INDEX idx_gateway_logs_aggregate_backfill ON public.gateway_request_logs USING btree (aggregate_version, id);
CREATE INDEX idx_gateway_logs_cache_route ON public.gateway_request_logs USING btree (downstream_key_id, model_name, status_code, created_at);
CREATE INDEX idx_gateway_logs_channel_time ON public.gateway_request_logs USING btree (channel_id, created_at);
CREATE INDEX idx_gateway_logs_key_time ON public.gateway_request_logs USING btree (downstream_key_id, created_at);
CREATE INDEX idx_gateway_logs_model_time ON public.gateway_request_logs USING btree (model_name, created_at);
CREATE INDEX idx_gateway_logs_route_time ON public.gateway_request_logs USING btree (route_reason, created_at);
CREATE INDEX idx_gateway_logs_session_route ON public.gateway_request_logs USING btree (downstream_key_id, session_hash, model_name, status_code, created_at);
CREATE INDEX idx_gateway_model_capabilities_last_checked_at ON public.gateway_model_capabilities USING btree (last_checked_at);
CREATE UNIQUE INDEX idx_gateway_model_capability ON public.gateway_model_capabilities USING btree (channel_id, group_name, model_name);
CREATE UNIQUE INDEX idx_gateway_request_aggregate_key ON public.gateway_request_aggregates USING btree (bucket_start, downstream_key_id, channel_id, channel_name_snapshot, model_name, cost_source, route_reason, result);
CREATE INDEX idx_gateway_request_aggregates_bucket_start ON public.gateway_request_aggregates USING btree (bucket_start);
CREATE INDEX idx_gateway_request_aggregates_channel_id ON public.gateway_request_aggregates USING btree (channel_id);
CREATE INDEX idx_gateway_request_aggregates_downstream_key_id ON public.gateway_request_aggregates USING btree (downstream_key_id);
CREATE INDEX idx_gateway_request_aggregates_version ON public.gateway_request_aggregates USING btree (version);
CREATE INDEX idx_gateway_request_logs_channel_id ON public.gateway_request_logs USING btree (channel_id);
CREATE INDEX idx_gateway_request_logs_cost_reconcile_at ON public.gateway_request_logs USING btree (cost_reconcile_at);
CREATE INDEX idx_gateway_request_logs_created_at ON public.gateway_request_logs USING btree (created_at);
CREATE INDEX idx_gateway_request_logs_downstream_key_id ON public.gateway_request_logs USING btree (downstream_key_id);
CREATE INDEX idx_gateway_request_logs_group_name ON public.gateway_request_logs USING btree (group_name);
CREATE INDEX idx_gateway_request_logs_model_name ON public.gateway_request_logs USING btree (model_name);
CREATE INDEX idx_gateway_request_logs_upstream_key_id ON public.gateway_request_logs USING btree (upstream_key_id);
CREATE INDEX idx_gateway_request_logs_upstream_request_id ON public.gateway_request_logs USING btree (upstream_request_id);
CREATE UNIQUE INDEX idx_gateway_route_exclusion ON public.gateway_route_exclusions USING btree (downstream_key_id, channel_id, group_name);
CREATE INDEX idx_gateway_route_exclusions_channel_id ON public.gateway_route_exclusions USING btree (channel_id);
CREATE INDEX idx_gateway_route_exclusions_downstream_key_id ON public.gateway_route_exclusions USING btree (downstream_key_id);
CREATE INDEX idx_gateway_routing_failures_route_time ON public.gateway_routing_failures USING btree (channel_id, group_name, occurred_at);
CREATE INDEX idx_gateway_upstream_group_change_lookup ON public.gateway_upstream_group_changes USING btree (channel_id, group_name);
CREATE INDEX idx_gateway_upstream_group_changes_status ON public.gateway_upstream_group_changes USING btree (status);
CREATE UNIQUE INDEX idx_gateway_upstream_key_group ON public.gateway_upstream_keys USING btree (channel_id, group_name);
CREATE INDEX idx_gateway_upstream_keys_upstream_key_id ON public.gateway_upstream_keys USING btree (upstream_key_id);
CREATE INDEX idx_monitor_logs_channel_id ON public.monitor_logs USING btree (channel_id);
CREATE INDEX idx_monitor_logs_job ON public.monitor_logs USING btree (job);
CREATE INDEX idx_monitor_logs_started_at ON public.monitor_logs USING btree (started_at);
CREATE UNIQUE INDEX idx_notification_channels_name ON public.notification_channels USING btree (name);
CREATE INDEX idx_notification_channels_type ON public.notification_channels USING btree (type);
CREATE INDEX idx_notification_logs_channel_id ON public.notification_logs USING btree (channel_id);
CREATE INDEX idx_notification_logs_event ON public.notification_logs USING btree (event);
CREATE INDEX idx_notification_logs_sent_at ON public.notification_logs USING btree (sent_at);
CREATE INDEX idx_notification_logs_upstream_channel_id ON public.notification_logs USING btree (upstream_channel_id);
CREATE UNIQUE INDEX idx_rate_chan_model ON public.rate_snapshots USING btree (channel_id, model_name);
CREATE INDEX idx_rate_change_logs_changed_at ON public.rate_change_logs USING btree (changed_at);
CREATE INDEX idx_rate_change_logs_channel_id ON public.rate_change_logs USING btree (channel_id);
CREATE INDEX idx_rate_change_logs_model_name ON public.rate_change_logs USING btree (model_name);
CREATE INDEX idx_upstream_announcements_channel_id ON public.upstream_announcements USING btree (channel_id);
CREATE INDEX idx_upstream_announcements_first_seen_at ON public.upstream_announcements USING btree (first_seen_at);

INSERT INTO public.system_metadata (key, value, created_at, updated_at)
VALUES ('database.timeZone', 'UTC', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT (key) DO NOTHING;
