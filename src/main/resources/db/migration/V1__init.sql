CREATE TABLE employees (
 id BIGSERIAL PRIMARY KEY, tenant_id VARCHAR(80) NOT NULL, employee_id VARCHAR(80) NOT NULL,
 name VARCHAR(160) NOT NULL, email VARCHAR(254) NOT NULL, manager_id VARCHAR(80), office_code VARCHAR(40) NOT NULL, phone VARCHAR(20),
 pickup_point VARCHAR(200), special_assistance BOOLEAN NOT NULL DEFAULT FALSE, source_version BIGINT NOT NULL,
 active BOOLEAN NOT NULL DEFAULT TRUE, updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 CONSTRAINT uk_employee_tenant UNIQUE(tenant_id,employee_id)
);
CREATE INDEX idx_employees_active_tenant ON employees(tenant_id,active);
CREATE TABLE outbox_events (
 id UUID PRIMARY KEY, tenant_id VARCHAR(80) NOT NULL, employee_id VARCHAR(80) NOT NULL, event_type VARCHAR(80) NOT NULL,
 payload TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL, published_at TIMESTAMPTZ, attempts INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX idx_outbox_pending ON outbox_events(published_at,created_at);
CREATE TABLE cab_bookings (
 id UUID PRIMARY KEY, tenant_id VARCHAR(80) NOT NULL, employee_id VARCHAR(80) NOT NULL,
 travel_at TIMESTAMPTZ NOT NULL, status VARCHAR(24) NOT NULL, seat_reserved BOOLEAN NOT NULL DEFAULT TRUE
);
CREATE INDEX idx_future_employee_bookings ON cab_bookings(tenant_id,employee_id,travel_at,status);
