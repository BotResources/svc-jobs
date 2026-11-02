ALTER TABLE inbox_items
    DROP CONSTRAINT inbox_items_kind_check,
    DROP CONSTRAINT inbox_items_mail_shape;

ALTER TABLE inbox_items
    ADD COLUMN source_key uuid,
    ADD CONSTRAINT inbox_items_kind_check
        CHECK (kind IN ('MAIL', 'CC_NOTICE', 'NOTICE', 'MAIL_NOTICE')),
    ADD CONSTRAINT inbox_items_mail_shape CHECK (
        (kind IN ('MAIL', 'CC_NOTICE') AND address IS NOT NULL AND mail_id IS NOT NULL
            AND notice IS NULL)
        OR (kind = 'NOTICE' AND run_id IS NOT NULL AND mail_id IS NULL
            AND notice IS NOT NULL AND jsonb_typeof(notice) = 'object')
        OR (kind = 'MAIL_NOTICE' AND address IS NOT NULL AND run_id IS NOT NULL
            AND mail_id IS NULL AND notice IS NOT NULL AND jsonb_typeof(notice) = 'object')
    );

CREATE UNIQUE INDEX inbox_items_source_key
    ON inbox_items (address, source_key)
    WHERE source_key IS NOT NULL;
