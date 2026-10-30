# HR Roster Sync â€“ MoveInSync Case Study 5

## About this project

This project is my Java backend solution for **MoveInSync's Case Study 5: HR Roster Sync â€“ Idempotent, Out-of-Order, Self-Healing**.

MoveInSync receives employee information from corporate clients. That information can arrive more than once, arrive in the wrong order, contain invalid data, or come through an incomplete nightly file. Some details are managed by HR, while others are managed by the transport team.

The goal of this project is to keep employee records correct even when these problems happen.

## What the case study asks for

| Case-study requirement | What this project does |
|---|---|
| Receive employee create, update and deactivate events | Provides APIs to receive employee changes |
| Reject invalid records without failing a whole batch | Validates records and reports errors per record |
| Ignore duplicate and older updates | Uses an employee's source version to decide whether an update is new, repeated or old |
| Keep HR and transport fields separate | HR updates do not overwrite pickup point or special-assistance details |
| Compare a nightly file of up to 200,000 employees without loading everything into memory | Reads the CSV and database records in streams and compares them using temporary disk buckets |
| Stop unsafe mass deactivation | Pauses a run when 30% or more of active employees are missing and waits for operator approval |
| Publish reliable change events | Saves employee changes and outgoing event records in one database transaction, then publishes through Kafka |
| Cancel future cab bookings when an employee leaves | A Kafka consumer cancels future confirmed bookings and releases the reserved seat |
| Protect client data | Checks the authenticated user's tenant and role |
| Provide monitoring, caching and safe error handling | Includes health/metrics endpoints, Redis profile caching and non-sensitive error responses |

## How it works

There are two ways employee information enters the application:

1. **Employee event API:** HR sends an event when an employee joins, changes details or leaves.
2. **Nightly CSV file:** HR sends a full employee list. The application compares it with the employee records already stored.

For an event, the application checks the employee's version. If the same version was already processed, it ignores the duplicate. If an older version arrives after a newer one, it ignores the old update. A newer version can be applied.

For the nightly file, the application prepares three lists:

- **New:** present in the file but not in the database.
- **Changed:** present in both, but an HR-managed detail has changed.
- **Missing:** active in the database but absent from the file.

The preview does not change live employee records. If 30% or more of active employees appear to be missing, the application pauses the run until an operator approves it.

## Field ownership

| HR manages | Transport admin manages |
|---|---|
| Name | Pickup point |
| Email | Special-assistance needs |
| Manager | |
| Office | |
| Phone | |
| Active/inactive status | |

An HR update changes only HR-managed fields. Transport details are preserved.

## Technology

| Technology | Purpose |
|---|---|
| Java 21 | Backend programming language |
| Spring Boot | REST APIs and application structure |
| PostgreSQL | Employee records, reconciliation results and outgoing events |
| Flyway | Database schema changes |
| Redis | Employee profile cache |
| Apache Kafka | Sends employee-change events to other services |
| Docker Compose | Starts the application and required services |
| Maven | Build and test commands |

## Run the project

### Requirements

- Docker Desktop with Docker Compose
- JDK 21 and Maven 3.9+ if you want to run the application directly with Maven

### Start everything with Docker

From the project folder:

```bash
cp .env.example .env
```

Open `.env` and change the demo passwords. Then run:

```bash
docker compose up --build
```

The API runs at `http://localhost:8080`.

Check that it is running:

```text
http://localhost:8080/actuator/health
```

A healthy application returns:

```json
{"status":"UP"}
```

### Run the application with Maven

Start the supporting services first:

```bash
docker compose up -d postgres redis kafka
```

On Windows PowerShell:

```powershell
$env:KAFKA_BOOTSTRAP_SERVERS="localhost:9094"
mvn spring-boot:run
```

## Demo accounts

These accounts are for local assessment/demo use. Change the passwords before sharing the running application.

| Username | Role | Tenant | Default password |
|---|---|---|---|
| `operator` | OPERATOR | `acme` | `change-me` |
| `transport-admin` | TRANSPORT_ADMIN | `acme` | `change-me` |
| `admin` | ADMIN | `acme` | `change-me` |
| `globex-operator` | OPERATOR | `globex` | `globex-change-me` |
| `employee-e1001` | EMPLOYEE (own profile only) | `acme` | `employee-change-me` |

The assessment version uses in-memory demo accounts and HTTP Basic authentication. A production deployment should use an enterprise identity provider and persistent role/tenant management.

## Main API examples

The examples below use the `acme` tenant and the local demo account.

### Add an employee

```bash
curl -u operator:change-me -X POST http://localhost:8080/api/v1/events \
  -H 'Content-Type: application/json' -H 'X-Tenant-Id: acme' \
  -d '{"tenantId":"acme","employeeId":"E-1001","type":"CREATE","version":1,"name":"Asha Rao","email":"asha@example.com","managerId":"M-10","officeCode":"BLR","phone":"+919876543210"}'
```

Send the same request again to see the duplicate being ignored.

### Deactivate an employee

```bash
curl -u operator:change-me -X POST http://localhost:8080/api/v1/events \
  -H 'Content-Type: application/json' -H 'X-Tenant-Id: acme' \
  -d '{"tenantId":"acme","employeeId":"E-1001","type":"DEACTIVATE","version":2}'
```

### Update transport details

```bash
curl -u transport-admin:change-me -X PUT http://localhost:8080/api/v1/transport-profiles/E-1001 \
  -H 'Content-Type: application/json' -H 'X-Tenant-Id: acme' \
  -d '{"pickupPoint":"Indiranagar Metro","specialAssistance":false}'
```

### Preview the nightly employee file

```bash
curl -u operator:change-me -X POST http://localhost:8080/api/v1/reconciliation/preview \
  -H 'X-Tenant-Id: acme' -F 'file=@sample-data/employees.csv'
```

The CSV needs these headers: `employeeId`, `name`, `email`, `managerId`, `officeCode`, and `phone`.

The preview response includes the run ID and counts of new, changed and missing employees. Previewing does not modify live employee records.

### Approve a run if the safety check is triggered

Replace `<RUN_ID>` with the ID returned by the preview:

```bash
curl -u operator:change-me -X POST http://localhost:8080/api/v1/reconciliation/<RUN_ID>/approve \
  -H 'X-Tenant-Id: acme'
```

### Apply the preview

```bash
curl -u operator:change-me -X POST http://localhost:8080/api/v1/reconciliation/<RUN_ID>/apply \
  -H 'X-Tenant-Id: acme'
```

The response reports how many employees were created, updated and deactivated.

## Tests and performance

Run the automated tests:

```bash
mvn clean test
```

The normal test suite includes **62 automated tests** across 14 test classes covering stale/duplicate event handling, out-of-order versions, transport field preservation, deactivation semantics, RFC-4180 streaming CSV parsing across buffer boundaries, safe record-level handling of malformed rows, safety-brake threshold enforcement, concurrent apply and approval row-locking, outbox publishing with `SKIP LOCKED`, real process-crash recovery between database commit and Kafka publication, and end-to-end reconciliation diffing. The latest local `mvn clean test` run completed with 62 tests passing, 0 failures, 0 errors, and 0 skipped in 2 minutes 2 seconds. The dedicated process-crash recovery test also passed independently (1 test, 0 failures, 0 errors, 0 skipped). The application was also started with Docker Compose, and its health endpoint returned `UP`.

### Automated Test Suite Overview

| Test Class | Tests | Focus Area | Technology |
|---|---|---|---|
| `EmployeeEventApiTest` | 12 | Event ingestion API, validation, idempotency, auth | MockMvc + SpringBootTest |
| `EmployeeSyncServiceTest` | 5 | Domain sync rules, field ownership, versioning | Unit / Mockito |
| `EmployeeSyncConcurrencyTest` | 1 | Real concurrent database event updates | Testcontainers PostgreSQL |
| `ThreeWayReconciliationTest` | 20 | 3-way diffs, RFC-4180, malformed deactivation prevention, DB failure handling | Unit / In-Memory Mock |
| `CsvStreamParserTest` | 9 | Quoted fields, escaped quotes, multiline, CRLF, buffer boundaries | Unit |
| `ReconciliationApplyConcurrencyTest` | 2 | Concurrent apply requests, approval race safety, row locking | Testcontainers PostgreSQL |
| `ReconciliationApprovalTest` | 2 | Safety-brake manual approval and validation | Unit |
| `ReconciliationServiceTest` | 2 | Reconciliation service unit flow and status checks | Unit / Mockito |
| `DeactivationBookingListenerTest` | 2 | Kafka deactivation listener, duplicate delivery safety | Unit / Mockito |
| `OutboxRecoveryTest` | 2 | Polling outbox recovery and retry after Kafka failure | Unit / Mockito |
| `OutboxProcessCrashRecoveryTest` | 1 | Kill a real child JVM after PostgreSQL commit, then publish the persisted outbox event to embedded Kafka after restart | Testcontainers PostgreSQL + Embedded Kafka |
| `OutboxSkipLockedConcurrencyTest` | 1 | Multi-instance outbox polling with `SKIP LOCKED` | Testcontainers PostgreSQL |
| `Reconciliation200kBenchmarkTest` | 1 | Mocked repo streaming disk CSV diff (200k rows) | Benchmark |
| `ReconciliationPostgresIntegrationBenchmarkTest` | 1 | Real PostgreSQL 16 streaming diff & staging writes (5k DB + 5k CSV) | Testcontainers PostgreSQL |

### Recorded performance checks

- **200,000-row mock benchmark:** approximately 1.98-5.70 seconds. This checks the file-processing algorithm with mocked database access.
- **5,000-row PostgreSQL integration benchmark:** approximately 2.20-3.72 seconds in an earlier recorded run.
- **200,000-row PostgreSQL integration benchmark (latest local run):** 16,455 ms preview and 3,286 ms apply. The test seeded 200,000 real PostgreSQL employee rows, processed a 200,000-row CSV snapshot, detected 20,000 missing and 20,000 new employees, deactivated the 20,000 missing employees, inserted the 20,000 new employees, and verified 40,000 outbox events. The benchmark test completed in 33.84 seconds; Maven completed in 43.504 seconds. Result: 1 test passed, 0 failures, 0 errors, 0 skipped.

The 200,000-row database-backed preview completed under the case-study target of 60 seconds in this test environment. These are measurements from one machine/run, not a guarantee for every deployment. Disk buckets reduce memory use by partitioning the comparison, but a heavily skewed bucket distribution can still increase peak memory; this benchmark does not by itself prove a strict worst-case memory bound.
## Design choices and limitations

- **Employee ordering:** Updates for one employee are protected using database locks and source versions. This favours correct ordering over allowing simultaneous writes to the same employee.
- **Outgoing events:** Employee changes and their outgoing event records are committed together. Kafka delivery is at least once, so a receiving service must safely handle a repeated event ID.
- **CSV memory use:** Disk buckets reduce the amount of data held in memory at one time. Expected memory use is lower when records are spread across buckets; an unusually uneven distribution can make one bucket large.
- **Authentication:** Demo users are stored in memory and use HTTP Basic authentication. Production needs enterprise login and stronger access management.
- **Outbox history:** Published event records are retained. A production system should add an archive or cleanup process.
- **Failure testing:** `OutboxProcessCrashRecoveryTest` starts a separate JVM that commits an employee and outbox row, forcibly terminates that process before publication, then verifies that a fresh publisher can deliver the persisted event to Kafka and mark it published. This validates recovery from the tested crash window; it does not simulate every possible broker, network, or host failure.

## Repository

[GitHub – HR Roster Sync](https://github.com/swetak0911/hr-roster-sync)

Do not commit the real .env file, passwords or real employee information.
