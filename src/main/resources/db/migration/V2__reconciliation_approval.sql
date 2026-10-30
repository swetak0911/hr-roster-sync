CREATE TABLE reconciliation_runs (
 id UUID PRIMARY KEY,
 tenant_id VARCHAR(80) NOT NULL,
 status VARCHAR(32) NOT NULL,
 active_before BIGINT NOT NULL,
 snapshot_rows BIGINT NOT NULL DEFAULT 0,
 estimated_missing BIGINT NOT NULL DEFAULT 0,
 missing_ratio DOUBLE PRECISION NOT NULL DEFAULT 0,
 safety_brake BOOLEAN NOT NULL DEFAULT FALSE,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 approved_by VARCHAR(120),
 approved_at TIMESTAMPTZ,
 applied_at TIMESTAMPTZ
);
CREATE INDEX idx_reconciliation_tenant_created ON reconciliation_runs(tenant_id, created_at DESC);
CREATE TABLE reconciliation_missing (
 run_id UUID NOT NULL REFERENCES reconciliation_runs(id) ON DELETE CASCADE,
 tenant_id VARCHAR(80) NOT NULL,
 employee_id VARCHAR(80) NOT NULL,
 PRIMARY KEY(run_id, employee_id)
);
CREATE INDEX idx_reconciliation_missing_tenant ON reconciliation_missing(run_id, tenant_id, employee_id);
