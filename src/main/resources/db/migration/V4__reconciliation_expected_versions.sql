ALTER TABLE reconciliation_missing
    ADD COLUMN expected_source_version BIGINT NOT NULL DEFAULT 0;

ALTER TABLE reconciliation_changed
    ADD COLUMN expected_source_version BIGINT NOT NULL DEFAULT 0;
