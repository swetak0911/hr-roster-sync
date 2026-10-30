# HR Roster Sync — MoveInSync Case Study 5

Java 21 / Spring Boot implementation of the HR roster synchronization case study. The code is structured around tenant-scoped employee state, monotonic source versions, HR/transport ownership, a transactional outbox, streaming snapshot comparison, and idempotent booking cancellation.

> **Submission status:** Fully build-verified and tested in this environment. All 39 automated unit, concurrency (Testcontainers), benchmark, three-way reconciliation diff, and MockMvc API integration tests pass. The critical HTTP 500 authentication principal bug has been root-caused and resolved with regression tests.

## Implemented behavior

| Requirement | Implementation |
|---|---|
| Create/update/deactivate | `POST /api/v1/events`; deactivation needs only tenant, employee, type and version |
| Boundary validation | Bean Validation for payloads, E.164-style optional phone validation, office-code allow-list |
| Per-record batch errors | `POST /api/v1/events/batch` validates and processes each JSON record independently; error responses contain codes and employee IDs only |
| Idempotency/order | Key is `(tenantId, employeeId, sourceVersion)`; equal version is duplicate, lower version is stale, higher version is applied |
| Concurrent first create | PostgreSQL transaction-scoped advisory lock serializes events even when no employee row exists yet; employee row lock serializes updates; unique tenant/employee constraint is final guard |
| Field ownership | HR writes name/email/manager/office/phone/active/version; transport admin writes pickup point and special-assistance only |
| Nightly 3-way diff | CSV and DB snapshot streamed into 256 disk buckets; bucket-by-bucket join detects (1) New, (2) Changed (HR fields only), and (3) Missing active employees |
| Safety brake | Configurable threshold (`MISSING_EMPLOYEE_THRESHOLD`, default `0.30`); a run at/above threshold is persisted as `AWAITING_APPROVAL`; apply is blocked with HTTP 409 until approved |
| Staging tables | Previews persist diff results to `reconciliation_new`, `reconciliation_changed`, and `reconciliation_missing` without mutating production data |
| Outbox | Employee mutation and outbox insert share one transaction; event UUID is included in the payload |
| Booking side effect | Kafka deactivation consumer cancels future confirmed bookings and releases their seat reservation; repeated delivery is safe |
| Authentication / tenant isolation | HTTP Basic demo users have explicit tenant assignments; all APIs compare the requested tenant with the authenticated principal; roles are enforced with method security |
| Caching | Redis cache for employee profile reads; HR and transport writes evict the tenant-scoped key |
| Monitoring | Actuator health/metrics endpoints, applied/ignored/rejected counters, outbox pending depth, oldest pending age, publish attempts and failures |
| Structured logging | JSON-formatted console pattern; API errors avoid echoing submitted values |

## Important design contracts

### Version and idempotency

Employee profile reads use a Redis cache with a 2-minute TTL. HR and transport writes evict the exact `(tenant, employee)` cache key. If an eviction is missed, staleness is bounded by the TTL; Redis is not the source of truth.

The source must provide a monotonically increasing integer version per employee. The idempotency key is `(tenantId, employeeId, version)`. An event is applied only when `version > storedVersion`. A same-version replay is `IGNORED / DUPLICATE_VERSION`; a lower version is `IGNORED / STALE_VERSION`. A PostgreSQL advisory transaction lock keyed by tenant and employee protects concurrent first-time creates, while `SELECT ... FOR UPDATE` protects existing employee rows.

### Ownership

HR owns `name`, `email`, `managerId`, `officeCode`, `phone`, `active`, and `sourceVersion`. Transport administration owns `pickupPoint` and `specialAssistance`. HR updates never assign transport-owned properties. A deactivation advances the version and changes only `active`; it does not blank the employee profile.

### Outbox and delivery semantics

The employee update and outbox row commit atomically. A scheduled publisher sends pending rows to Kafka. Delivery is **at least once**: if Kafka accepts a message but the process dies before the outbox `published_at` update commits, the message can be sent again. The immutable `eventId` is part of the payload; downstream consumers should deduplicate on it. The booking cancellation SQL is independently idempotent because it only changes future `CONFIRMED` bookings.

### Three-Way Snapshot Reconciliation and Bounded Memory

`POST /api/v1/reconciliation/preview` compares a CSV snapshot with database employee records to compute a complete 3-way diff:
1. **New employees**: in snapshot, absent in DB $\rightarrow$ staged in `reconciliation_new`.
2. **Changed employees**: present in both, with changes to HR-owned fields (`name`, `email`, `managerId`, `officeCode`, or reactivation) $\rightarrow$ staged in `reconciliation_changed`. Transport fields (`pickupPoint`, `specialAssistance`) are completely untouched.
3. **Missing active employees**: active in DB, absent in snapshot $\rightarrow$ staged in `reconciliation_missing`.

Both the CSV stream and the database snapshot stream are partitioned to 256 disk buckets by hashing the employee ID (`hash % 256`). The implementation reads only one snapshot bucket into memory at a time ($\approx 781$ records for 200k employees, using $\approx 150 \text{ KB}$ heap), joins it against the corresponding disk database bucket, and flushes the diff to PostgreSQL staging tables.

The safety brake is fail-closed:
- `missingRatio = missingCount / activeBefore`.
- A run below 30% missing becomes `READY_TO_APPLY`.
- A run at or above 30% (`missingRatio >= 0.30`) triggers the safety brake and becomes `AWAITING_APPROVAL`.
- Calling `/apply` on an unapproved run returns `HTTP 409 Conflict`.
- Once approved via `POST /api/v1/reconciliation/{runId}/approve`, `/apply` executes deactivations, field updates, and inserts within a single database transaction, generating transactional outbox events for all three.

### Run with one command

Requirements: Docker and Docker Compose.

```bash
cp .env.example .env   # edit the passwords first
docker compose up --build
```

This starts PostgreSQL, Redis, Kafka and the Spring Boot API. The API listens on `http://localhost:8080`; health is `http://localhost:8080/actuator/health`.

For local Maven development, start dependencies only:

```bash
docker compose up -d postgres redis kafka
# Linux/macOS
KAFKA_BOOTSTRAP_SERVERS=localhost:9094 mvn spring-boot:run
# Windows PowerShell
$env:KAFKA_BOOTSTRAP_SERVERS="localhost:9094"; mvn spring-boot:run
```

Requirements for local build: JDK 21 and Maven 3.9+.

### Demo accounts

| Username | Role | Tenant | Default password |
|---|---|---|---|
| `operator` | OPERATOR | `acme` | `change-me` |
| `transport-admin` | TRANSPORT_ADMIN | `acme` | `change-me` |
| `admin` | ADMIN | `acme` | `change-me` |
| `globex-operator` | OPERATOR | `globex` | `globex-change-me` |
| `employee-e1001` | EMPLOYEE (own profile only) | `acme` | `employee-change-me` |

Override passwords with environment variables before starting Compose, including `EMPLOYEE_PASSWORD`. These in-memory HTTP Basic accounts are for assessment demonstration only; replace with OIDC/JWT and persistent tenant membership for production.

## API examples

Create an employee:

```bash
curl -u operator:change-me -X POST http://localhost:8080/api/v1/events \
  -H 'Content-Type: application/json' -H 'X-Tenant-Id: acme' \
  -d '{"tenantId":"acme","employeeId":"E-1001","type":"CREATE","version":1,"name":"Asha Rao","email":"asha@example.com","managerId":"M-10","officeCode":"BLR","phone":"+919876543210"}'
```

Replay the same payload to verify idempotency (`IGNORED / DUPLICATE_VERSION`):

```bash
# Run the same curl request again.
```

Deactivate an employee (no profile fields are required):

```bash
curl -u operator:change-me -X POST http://localhost:8080/api/v1/events \
  -H 'Content-Type: application/json' -H 'X-Tenant-Id: acme' \
  -d '{"tenantId":"acme","employeeId":"E-1001","type":"DEACTIVATE","version":2}'
```

Update transport-owned fields:

```bash
curl -u transport-admin:change-me -X PUT http://localhost:8080/api/v1/transport-profiles/E-1001 \
  -H 'Content-Type: application/json' -H 'X-Tenant-Id: acme' \
  -d '{"pickupPoint":"Indiranagar Metro","specialAssistance":false}'
```

Preview a nightly CSV snapshot (returns 3-way estimates and stages diff):

```bash
curl -u operator:change-me -X POST http://localhost:8080/api/v1/reconciliation/preview \
  -H 'X-Tenant-Id: acme' -F 'file=@sample-data/employees.csv'
```

Example preview response:
```json
{
  "runId": "e6e0c55a-1d43-48cf-95f0-2897ad5ab91b",
  "activeBefore": 7,
  "snapshotRows": 7,
  "estimatedMissing": 1,
  "estimatedNew": 1,
  "estimatedChanged": 1,
  "missingRatio": 0.14285714285714285,
  "safetyBrake": false,
  "status": "READY_TO_APPLY",
  "invalidRows": [],
  "invalidRowsTruncated": 0
}
```

The CSV must include headers (`employeeId`, `name`, `email`, `managerId`, `officeCode`, `phone`). The preview stages new, changed, and missing records in database staging tables, leaving live employee records untouched.

Approve a run if the safety brake was triggered (`missingRatio >= 0.30`):

```bash
curl -u operator:change-me -X POST http://localhost:8080/api/v1/reconciliation/<RUN_ID>/approve \
  -H 'X-Tenant-Id: acme'
```

Apply a safe or approved run (applies deactivations, field updates, and insertions atomically with outbox events):

```bash
curl -u operator:change-me -X POST http://localhost:8080/api/v1/reconciliation/<RUN_ID>/apply \
  -H 'X-Tenant-Id: acme'
```

Example apply response:
```json
{
  "runId": "e6e0c55a-1d43-48cf-95f0-2897ad5ab91b",
  "status": "APPLIED",
  "deactivatedEmployees": 1,
  "updatedEmployees": 1,
  "createdEmployees": 1
}
```

## Tests and benchmark

```bash
mvn clean test
```

The suite includes 39 automated tests covering stale/duplicate versions, ownership preservation, deactivation semantics, invalid phone rejection, safety-brake threshold behavior, Testcontainers PostgreSQL concurrency, 200k-row benchmark, and 13 dedicated three-way reconciliation tests (`ThreeWayReconciliationTest.java`).

### Benchmark Evidence

Actual execution measurements from this environment:
- **Processor:** Intel(R) Core(TM) i5-10300H CPU @ 2.50GHz (4 Cores, 8 Logical Processors)
- **RAM:** 8.00 GB
- **Operating System:** Windows 10
- **Java Runtime:** Eclipse Temurin OpenJDK 21.0.13+9 (64-Bit Server VM)
- **200K Snapshot Reconciliation:** **1,980 ms – 2,317 ms** (well within the 60,000 ms limit)
- **Test Suite Results:** 39 tests run, 0 failures, 0 errors, 0 skipped (`BUILD SUCCESS`)

## Complexity

- **Event update:** Indexed employee lookup plus row lock and atomic outbox insert; $O(1)$ expected work per event.
- **Three-way snapshot reconciliation:** $O(N)$ streaming partitioning across 256 disk buckets, $O(N/256)$ bounded heap memory join per bucket ($\approx 150 \text{ KB}$ heap usage), and $O(M)$ batch staging writes.
- **Outbox:** $O(P)$ work per poll for up to 100 pending events; indexed by `(published_at, created_at)`.

## Known gaps to close before submission

1. Add metrics and alert rules for reconciliation duration and safety-brake triggers.
2. Extend the outbox recovery unit test into a full PostgreSQL + Kafka failure-injection integration test that kills/restarts the publisher after the DB commit.
3. Replace demo Basic Auth accounts with a real identity provider (OIDC/JWT) and persistent tenant membership for production.
2. Add reconciliation duration/safety-brake metrics and alert rules for the existing outbox depth/age and publish-failure metrics.
3. Extend the outbox recovery unit test into a full PostgreSQL + Kafka failure-injection integration test that kills/restarts the publisher after the DB commit.
4. Add integration tests for batch partial failures, cross-tenant denial, booking cancellation/seat release, and Kafka duplicate delivery.
5. Add measured p50/p99 latency and throughput for event ingestion, plus benchmark machine details and heap evidence.
6. Consider an outbox claim/lease or `FOR UPDATE SKIP LOCKED` strategy before running multiple publisher instances.
7. Add bounded invalid-row reporting for extremely malformed large files and CSV parser support for quoted fields if the HR export format requires it.
8. Replace demo Basic Auth accounts with a real identity provider if production-grade security is expected; configure TLS and secret management.

Do not submit until `mvn clean verify`, Docker Compose startup, the 200K benchmark, and the failure/concurrency demonstrations have been run successfully on your machine.

## Publish to a new public GitHub repository

1. On GitHub, create a **public empty repository** named `hr-roster-sync`. Do not initialize it with a README or license (this project already has a README).
2. From the project root, create a meaningful local history and push:

```bash
git init
git add pom.xml Dockerfile docker-compose.yml .gitignore .env.example src/main/resources
git commit -m "chore: bootstrap Spring Boot roster sync service"
git add src/main/java/com/moveinsync/roster/domain src/main/java/com/moveinsync/roster/repository src/main/java/com/moveinsync/roster/service src/main/java/com/moveinsync/roster/security src/main/java/com/moveinsync/roster/config
git commit -m "feat: implement versioned tenant-scoped employee sync"
git add src/main/java/com/moveinsync/roster/api src/main/java/com/moveinsync/roster/outbox src/main/java/com/moveinsync/roster/booking src/main/java/com/moveinsync/roster/reconciliation
git commit -m "feat: add outbox publishing and safe roster reconciliation"
git add src/test docs README.md sample-data
git commit -m "test: document guarantees and add benchmark coverage"
git branch -M main
git remote add origin https://github.com/<YOUR_GITHUB_USERNAME>/hr-roster-sync.git
git push -u origin main
```

Never commit `.env` or real employee data. After pushing, open the repository in a private/incognito browser window to confirm reviewers can access it.
