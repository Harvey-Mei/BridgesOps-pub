ALTER TABLE public.channels
    ADD COLUMN IF NOT EXISTS balance_error text NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS balance_error_at timestamp with time zone,
    ADD COLUMN IF NOT EXISTS upstream_account_balance_insufficient boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS upstream_account_balance_insufficient_at timestamp with time zone,
    ADD COLUMN IF NOT EXISTS upstream_account_balance_insufficient_revision bigint NOT NULL DEFAULT 0,
    ADD CONSTRAINT chk_channels_upstream_account_balance_insufficient
        CHECK (
            (
                NOT upstream_account_balance_insufficient
                AND upstream_account_balance_insufficient_at IS NULL
                AND upstream_account_balance_insufficient_revision = 0
            )
            OR
            (
                upstream_account_balance_insufficient
                AND upstream_account_balance_insufficient_at IS NOT NULL
                AND upstream_account_balance_insufficient_revision > 0
				AND upstream_account_balance_insufficient_revision = upstream_account_identity_revision
            )
        );
