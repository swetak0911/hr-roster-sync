# 4–6 minute reviewer demo script

## 1. Start the stack

```bash
cp .env.example .env
# Change passwords in .env
docker compose up --build
```

Show `/actuator/health` returning `UP` and briefly point out the PostgreSQL, Redis and Kafka containers.

## 2. Idempotency and ordering

Create employee `E-1001` with version 7, then replay the same request. Show `APPLIED` followed by `IGNORED / DUPLICATE_VERSION`.

Send version 6 after version 7. Show `IGNORED / STALE_VERSION`, then query `/api/v1/employees/E-1001` and show that version 7 remains.

## 3. Field ownership

As `transport-admin`, set a pickup point and special-assistance flag. Then send an HR update with a higher version and changed HR-owned fields. Query the employee and show the transport-owned values are unchanged.

## 4. Per-record validation

Call `/api/v1/events/batch` with one valid record and one record with an invalid phone or unknown office code. Show that the valid record is applied and the invalid record is rejected independently. Show that the error response does not echo name, phone or email.

## 5. Safety brake and operator approval

Create a CSV snapshot that omits at least 30% of the currently active employee IDs. Call `/api/v1/reconciliation/preview` and show `SAFETY_BRAKE_APPROVAL_REQUIRED` plus the estimated missing count. Attempt `/apply` before approval and show HTTP 409. Then call `/approve`, call `/apply`, and show that the missing employees are deactivated and outbox rows are created.

## 6. Deactivation side effect

Insert a future confirmed booking for the employee into the demo database:

```bash
docker compose exec postgres psql -U roster -d roster -c \
  "INSERT INTO cab_bookings(id,tenant_id,employee_id,travel_at,status,seat_reserved) VALUES (gen_random_uuid(),'acme','E-1001',now()+interval '1 day','CONFIRMED',true);"
```

Then deactivate the employee through the event API. Show that the booking consumer changes the booking to `CANCELLED` and sets `seat_reserved=false`:

```bash
docker compose exec postgres psql -U roster -d roster -c \
  "SELECT employee_id,status,seat_reserved FROM cab_bookings WHERE employee_id='E-1001';"
```

## 7. Outbox recovery demonstration

Stop Kafka, create an employee event, and show that the database transaction succeeds while the outbox pending gauge grows. Restart Kafka and show that the publisher drains the pending outbox. Explain that a crash after Kafka acceptance but before the `published_at` commit can produce a duplicate, so downstream consumers must deduplicate by `eventId`.

## Closing points

- Per-employee ordering is enforced by a PostgreSQL advisory transaction lock plus row lock.
- Employee and outbox writes are atomic.
- Snapshot comparison uses disk hash buckets and bounded invalid-row reporting.
- The benchmark test measures 200,000 rows; report only the actual elapsed time and machine details from your run.
