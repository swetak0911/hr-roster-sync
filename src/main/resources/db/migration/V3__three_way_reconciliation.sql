ALTER TABLE reconciliation_runs
  ADD COLUMN IF NOT EXISTS estimated_new BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS estimated_changed BIGINT NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS reconciliation_new (
  run_id UUID NOT NULL REFERENCES reconciliation_runs(id) ON DELETE CASCADE,
  tenant_id VARCHAR(80) NOT NULL,
  employee_id VARCHAR(80) NOT NULL,
  name VARCHAR(160) NOT NULL,
  email VARCHAR(254) NOT NULL,
  manager_id VARCHAR(80),
  office_code VARCHAR(40) NOT NULL,
  phone VARCHAR(20),
  PRIMARY KEY (run_id, employee_id)
);
CREATE INDEX IF NOT EXISTS idx_reconciliation_new_tenant ON reconciliation_new(run_id, tenant_id, employee_id);

CREATE TABLE IF NOT EXISTS reconciliation_changed (
  run_id UUID NOT NULL REFERENCES reconciliation_runs(id) ON DELETE CASCADE,
  tenant_id VARCHAR(80) NOT NULL,
  employee_id VARCHAR(80) NOT NULL,
  name VARCHAR(160) NOT NULL,
  email VARCHAR(254) NOT NULL,
  manager_id VARCHAR(80),
  office_code VARCHAR(40) NOT NULL,
  phone VARCHAR(20),
  old_name VARCHAR(160),
  old_email VARCHAR(254),
  old_manager_id VARCHAR(80),
  old_office_code VARCHAR(40),
  PRIMARY KEY (run_id, employee_id)
);
CREATE INDEX IF NOT EXISTS idx_reconciliation_changed_tenant ON reconciliation_changed(run_id, tenant_id, employee_id);

