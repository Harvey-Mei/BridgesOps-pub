ALTER TABLE public.gateway_channel_deletion_key_audits
    DROP CONSTRAINT chk_gateway_channel_deletion_key_audit_disposition,
    ADD CONSTRAINT chk_gateway_channel_deletion_key_audit_disposition
        CHECK (
            (ownership_provenance IN ('created', 'provisioning_claim') AND disposition = 'verify_managed_delete')
            OR (ownership_provenance = 'adopted' AND disposition = 'retain_adopted')
            OR (
                ownership_provenance = 'unknown'
                AND (
                    disposition = 'manual_review_required'
                    OR (
                        disposition = 'verify_managed_delete'
                        AND upstream_key_name ~ '^BOG-p-[0-9a-f]{16}$'
                    )
                )
            )
        );

ALTER TABLE public.gateway_channel_deletion_key_claims
    DROP CONSTRAINT chk_gateway_channel_deletion_key_claim_disposition,
    ADD CONSTRAINT chk_gateway_channel_deletion_key_claim_disposition
        CHECK (
            (ownership_provenance IN ('created', 'provisioning_claim') AND disposition = 'verify_managed_delete')
            OR (ownership_provenance = 'adopted' AND disposition = 'retain_adopted')
            OR (
                ownership_provenance = 'unknown'
                AND (
                    disposition = 'manual_review_required'
                    OR (
                        disposition = 'verify_managed_delete'
                        AND upstream_key_name ~ '^BOG-p-[0-9a-f]{16}$'
                    )
                )
            )
        );
