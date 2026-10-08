CREATE TABLE public.gateway_request_price_import_commands (
    id bigserial PRIMARY KEY,
    actor character varying(128) NOT NULL,
    idempotency_key character varying(128) NOT NULL,
    payload_digest character(64) NOT NULL,
    payload_digest_version smallint NOT NULL,
    item_count integer NOT NULL,
    imported_at timestamp(6) with time zone NOT NULL,
    created_at timestamp(6) with time zone NOT NULL,
    CONSTRAINT uq_gateway_request_price_import_command_key
        UNIQUE (idempotency_key),
    CONSTRAINT chk_gateway_request_price_import_command_actor
        CHECK (
            actor = btrim(actor)
            AND length(actor) BETWEEN 1 AND 128
        ),
    CONSTRAINT chk_gateway_request_price_import_command_key
        CHECK (idempotency_key ~ '^[A-Za-z0-9._:-]{1,128}$'),
    CONSTRAINT chk_gateway_request_price_import_command_digest
        CHECK (
            payload_digest ~ '^[0-9a-f]{64}$'
            AND payload_digest_version = 1
        ),
    CONSTRAINT chk_gateway_request_price_import_command_count
        CHECK (item_count BETWEEN 1 AND 500),
    CONSTRAINT chk_gateway_request_price_import_command_time
        CHECK (
            isfinite(imported_at)
            AND isfinite(created_at)
            AND created_at = imported_at
        )
);

CREATE INDEX idx_gateway_request_price_import_commands_created
    ON public.gateway_request_price_import_commands (created_at, id);

CREATE TABLE public.gateway_request_price_import_items (
    command_id bigint NOT NULL,
    item_index integer NOT NULL,
    price_fact_id bigint NOT NULL,
    created_at timestamp(6) with time zone NOT NULL,
    CONSTRAINT pk_gateway_request_price_import_items
        PRIMARY KEY (command_id, item_index),
    CONSTRAINT uq_gateway_request_price_import_item_fact
        UNIQUE (price_fact_id),
    CONSTRAINT fk_gateway_request_price_import_item_command
        FOREIGN KEY (command_id)
        REFERENCES public.gateway_request_price_import_commands(id)
        ON DELETE RESTRICT,
    CONSTRAINT fk_gateway_request_price_import_item_fact
        FOREIGN KEY (price_fact_id)
        REFERENCES public.gateway_request_price_facts(id)
        ON DELETE RESTRICT,
    CONSTRAINT chk_gateway_request_price_import_item_index
        CHECK (item_index BETWEEN 0 AND 499),
    CONSTRAINT chk_gateway_request_price_import_item_time
        CHECK (isfinite(created_at))
);

-- A receipt ordinal and timestamp belong to the immutable command, while its
-- fact must be the administrator fact created at that command's import time.
-- Missing parent rows are rejected here with the corresponding FK identity.
-- Waiting for an uncommitted parent in the FK check would otherwise skip these
-- consistency checks and allow a concurrent item to reopen a completed set.
CREATE FUNCTION public.enforce_gateway_request_price_import_item_consistency()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM public.gateway_request_price_import_commands AS command_row
        WHERE command_row.id = NEW.command_id
    ) THEN
        RAISE EXCEPTION 'gateway request price import command does not exist'
            USING ERRCODE = '23503',
                  CONSTRAINT = 'fk_gateway_request_price_import_item_command';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM public.gateway_request_price_import_commands AS command_row
        WHERE command_row.id = NEW.command_id
          AND NEW.item_index >= 0
          AND NEW.item_index < command_row.item_count
          AND NEW.created_at IS NOT DISTINCT FROM command_row.imported_at
    ) THEN
        RAISE EXCEPTION 'gateway request price import item does not match its command'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'gateway_request_price_import_item_consistency';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM public.gateway_request_price_facts AS fact
        WHERE fact.id = NEW.price_fact_id
    ) THEN
        RAISE EXCEPTION 'gateway request price import fact does not exist'
            USING ERRCODE = '23503',
                  CONSTRAINT = 'fk_gateway_request_price_import_item_fact';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM public.gateway_request_price_facts AS fact
        WHERE fact.id = NEW.price_fact_id
          AND fact.source = 'administrator'
          AND fact.observed_at <= NEW.created_at
          AND fact.valid_from IS NOT DISTINCT FROM NEW.created_at
          AND fact.created_at IS NOT DISTINCT FROM NEW.created_at
    ) THEN
        RAISE EXCEPTION 'gateway request price import item does not reference its command fact'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'gateway_request_price_import_item_consistency';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_gateway_request_price_import_items_consistency
BEFORE INSERT ON public.gateway_request_price_import_items
FOR EACH ROW
EXECUTE FUNCTION public.enforce_gateway_request_price_import_item_consistency();

-- A staged deployment of 000045 before the authenticated importer existed
-- must not silently grandfather administrator-labelled facts without receipts.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM public.gateway_request_price_facts AS fact
        WHERE fact.source = 'administrator'
    ) THEN
        RAISE EXCEPTION 'existing administrator request price facts have no import receipts'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'gateway_request_price_administrator_receipt';
    END IF;
END;
$$;

-- Every administrator-sourced fact must have exactly one immutable import
-- receipt by transaction end. This closes direct SQL and generic append paths
-- that could otherwise forge the trusted source label.
CREATE FUNCTION public.enforce_gateway_request_price_administrator_receipt()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.source = 'administrator' AND NOT EXISTS (
        SELECT 1
        FROM public.gateway_request_price_import_items AS receipt
        WHERE receipt.price_fact_id = NEW.id
    ) THEN
        RAISE EXCEPTION 'administrator request price fact has no import receipt'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'gateway_request_price_administrator_receipt';
    END IF;

    RETURN NEW;
END;
$$;

CREATE CONSTRAINT TRIGGER gateway_request_price_administrator_receipt
AFTER INSERT ON public.gateway_request_price_facts
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION public.enforce_gateway_request_price_administrator_receipt();

-- The command is inserted before its items, so completeness must be checked
-- at transaction end. Once this deferred constraint succeeds, the item INSERT
-- guard and append-only triggers make the exact 0..item_count-1 set permanent.
CREATE FUNCTION public.enforce_gateway_request_price_import_command_complete()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    receipt_count bigint;
    first_index integer;
    last_index integer;
BEGIN
    SELECT count(*), min(item_index), max(item_index)
    INTO receipt_count, first_index, last_index
    FROM public.gateway_request_price_import_items
    WHERE command_id = NEW.id;

    IF receipt_count IS DISTINCT FROM NEW.item_count::bigint
       OR first_index IS DISTINCT FROM 0
       OR last_index IS DISTINCT FROM NEW.item_count - 1 THEN
        RAISE EXCEPTION 'gateway request price import command receipt set is incomplete'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'gateway_request_price_import_command_complete';
    END IF;

    RETURN NEW;
END;
$$;

CREATE CONSTRAINT TRIGGER gateway_request_price_import_command_complete
AFTER INSERT ON public.gateway_request_price_import_commands
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION public.enforce_gateway_request_price_import_command_complete();

CREATE FUNCTION public.reject_gateway_request_price_import_receipt_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'gateway request price import receipts are append-only'
        USING ERRCODE = '23514';
    RETURN NULL;
END;
$$;

CREATE TRIGGER trg_gateway_request_price_import_commands_append_only
BEFORE UPDATE OR DELETE ON public.gateway_request_price_import_commands
FOR EACH ROW
EXECUTE FUNCTION public.reject_gateway_request_price_import_receipt_mutation();

CREATE TRIGGER trg_gateway_request_price_import_commands_reject_truncate
BEFORE TRUNCATE ON public.gateway_request_price_import_commands
FOR EACH STATEMENT
EXECUTE FUNCTION public.reject_gateway_request_price_import_receipt_mutation();

CREATE TRIGGER trg_gateway_request_price_import_items_append_only
BEFORE UPDATE OR DELETE ON public.gateway_request_price_import_items
FOR EACH ROW
EXECUTE FUNCTION public.reject_gateway_request_price_import_receipt_mutation();

CREATE TRIGGER trg_gateway_request_price_import_items_reject_truncate
BEFORE TRUNCATE ON public.gateway_request_price_import_items
FOR EACH STATEMENT
EXECUTE FUNCTION public.reject_gateway_request_price_import_receipt_mutation();
