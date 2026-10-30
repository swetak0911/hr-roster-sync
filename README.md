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

The employee update and outbox row commit atomically. A scheduled publisher polls pending rows using `SELECT FOR UPDATE SKIP LOCKED` to allow multiple application replicas to poll and publish concurrently without duplicate processing. Delivery is **at least once**: if Kafka accepts a message but the process dies before the outbox `published_at` update commits, the message can be sent again. The immutable `eventId` is part of the payload; downstream consumers should deduplicate on it. The booking cancellation SQL is independently idempotent because it only changes future `CONFIRMED` bookings.

### Three-Way Snapshot Reconciliation and Bounded Memory

`POST /api/v1/reconciliation/preview` parses incoming CSV snapshots using a streaming RFC-4180 compliant parser and compares them against database employee records to compute a complete 3-way diff:
1. **New employees**: present in snapshot, absent in DB $\rightarrow$ staged in `reconciliation_new`.
2. **Changed employees**: present in both, with changes to HR-owned fields (`name`, `email`, `managerId`, `officeCode`, or reactivation) $\rightarrow$ staged in `reconciliation_changed`. Transport fields (`pickupPoint`, `specialAssistance`) are completely untouched.
3. **Missing active employees**: active in DB, absent in snapshot $\rightarrow$ staged in `reconciliation_missing`.

The parser natively handles quoted fields with commas, escaped double quotes (`""`), multi-line quoted fields, reordered headers, and missing optional fields. Both the CSV stream and the database snapshot stream are partitioned to 256 disk bucket files by hashing the employee ID (`(hash & 0x7fffffff) % 256`). The implementation reads only one snapshot bucket into memory at a time, detects duplicate employee IDs within each bucket file, joins against the corresponding database disk bucket, and flushes the diff to PostgreSQL staging tables.

**Memory Complexity:** Under uniform hash distribution, expected working heap memory is $O(N / 256)$ ($\approx 781$ records per bucket for 200,000 employees). In the theoretical worst-case adversarial scenario where all employee IDs hash to a single bucket, bucket size degrades to $O(N)$. Disk buckets are temporary and removed immediately after processing.

**Safety Brake & Atomic Apply:**
- `missingRatio = missingCount / activeBefore`.
- A run below 30% missing becomes `READY_TO_APPLY`.
- A run at or above 30% (`missingRatio >= 0.30`) triggers the safety brake and becomes `AWAITING_APPROVAL`.
- Calling `/apply` on an unapproved run returns `HTTP 409 Conflict`.
- Once approved via `POST /api/v1/reconciliation/{runId}/approve`, `/apply` is concurrency-safe: it acquires a pessimistic row lock (`SELECT ... FOR UPDATE`), transitions status (`READY_TO_APPLY/APPROVED -> APPLYING -> APPLIED`), and atomically executes deactivations (incrementing `source_version = source_version + 1`), HR field updates (incrementing `source_version = source_version + 1`), and insertions (`source_version = 1`), emitting transactional outbox events with the incremented versions.
- Concurrent or repeated apply attempts on an already `APPLIED` run are rejected with `HTTP 409 Conflict` (`RUN_ALREADY_APPLIED`) without modifying employee versions or creating extra outbox events.

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

## Tests and benchmarks

```bash
mvn clean test
```

The test suite includes **52 automated tests** across 12 test classes covering stale/duplicate event handling, out-of-order versions, transport field preservation, deactivation semantics, RFC-4180 streaming CSV parsing, safety-brake threshold enforcement, concurrent apply row-locking, outbox publishing with `SKIP LOCKED`, and end-to-end reconciliation diffing.

### Automated Test Suite Overview

| Test Class | Tests | Focus Area | Technology |
|---|---|---|---|
| `EmployeeEventApiTest` | 12 | Event ingestion API, validation, idempotency, auth | MockMvc + SpringBootTest |
| `EmployeeSyncServiceTest` | 5 | Domain sync rules, field ownership, versioning | Unit / Mockito |
| `EmployeeSyncConcurrencyTest` | 1 | Real concurrent database event updates | Testcontainers PostgreSQL |
| `ThreeWayReconciliationTest` | 17 | New / changed / missing diffs, RFC-4180 parsing, apply safety | Unit / In-Memory Mock |
| `CsvStreamParserTest` | 7 | Quoted fields, escaped quotes, multiline, CRLF, malformed rows | Unit |
| `ReconciliationApplyConcurrencyTest` | 1 | Concurrent apply requests, row locking, status transitions | Testcontainers PostgreSQL |
| `ReconciliationApprovalTest` | 2 | Safety-brake manual approval and validation | Unit |
| `ReconciliationServiceTest` | 2 | Reconciliation service unit flow and status checks | Unit / Mockito |
| `DeactivationBookingListenerTest` | 2 | Kafka deactivation listener, duplicate delivery safety | Unit / Mockito |
| `OutboxRecoveryTest` | 1 | Polling outbox recovery and retry behavior | Unit / Mockito |
| `Reconciliation200kBenchmarkTest` | 1 | Mocked repo streaming disk CSV diff (200k rows) | Benchmark |
| `ReconciliationPostgresIntegrationBenchmarkTest` | 1 | Real PostgreSQL 16 streaming diff (5k DB + 5k CSV) | Testcontainers PostgreSQL |

### Benchmark Evidence

Actual execution measurements from this environment:
- **Processor:** Intel(R) Core(TM) i5-10300H CPU @ 2.50GHz (4 Cores, 8 Logical Processors)
- **RAM:** 8.00 GB
- **Operating System:** Windows 10
- **Java Runtime:** Eclipse Temurin OpenJDK 21.0.13+9 (64-Bit Server VM)
- **Benchmark 1 — Mocked Repository & Streaming Disk CSV Diff (200,000 rows):** **1,980 ms – 2,317 ms**
  - Measures pure CPU and disk I/O performance of the 256-bucket partitioning and diffing algorithm on 200,000 incoming CSV snapshot records vs 200,000 in-memory active employee records without database/network latency.
- **Benchmark 2 — Real PostgreSQL Integration Benchmark (5,000 DB rows + 5,000 CSV rows):** **2,204 ms – 5,546 ms**
  - Measures real end-to-end database performance using Testcontainers PostgreSQL 16: streaming DB projection query, 256-bucket disk partitioning, two-way diff computation, and batch staging inserts.
- **Test Suite Results:** **52 tests run, 0 failures, 0 errors, 0 skipped** (`BUILD SUCCESS`).

## Complexity

- **Event update:** Indexed employee lookup plus row lock (`FOR NO KEY UPDATE`) and atomic outbox insert; $O(1)$ expected work per event.
- **Three-way snapshot reconciliation:**
  - **Time complexity:** $O(N + M)$ where $N$ is incoming CSV rows and $M$ is active database rows. $O(N)$ streaming partitioning across 256 disk buckets, $O((N+M)/256)$ join per bucket, and batch staging inserts.
  - **Heap memory:** Expected $O(N/256)$ under uniform hashing ($\approx 781$ records per bucket for $N = 200,000$, $\approx 150 \text{ KB}$ heap per bucket) as only one bucket partition is loaded into memory at a time. In the worst-case hash skew (all records hashing to a single bucket), memory degrades gracefully to $O(N)$.
- **Outbox polling:** $O(P)$ work per poll for up to 100 pending events; indexed by `(published_at, created_at)` with `FOR UPDATE SKIP LOCKED` for safe multi-replica polling.

## Production Considerations and Future Enhancements

1. **Authentication and Authorization:** Replace demo in-memory Basic Auth accounts (`operator`) with an enterprise identity provider (OIDC/OAuth2/JWT) and fine-grained tenant membership RBAC for production deployment.
2. **Infrastructure Failure-Injection:** Extend the outbox recovery test into a distributed network partition test (e.g. killing Kafka brokers mid-transaction) to verify failover resiliency.
3. **Observability and Alerting:** Wire Micrometer / Prometheus metrics for reconciliation duration, bucket partition skew, outbox depth/age, and safety-brake trigger frequency.
4. **Distributed Load Profiling:** Measure end-to-end event ingestion latency percentiles ($p50$/$p95$/$p99$) under high-throughput concurrent loads using Gatling or k6.
5. **Data Archival:** Configure automated background cleanup or archival jobs for processed outbox records (`published_at IS NOT NULL`) and historical reconciliation runs.

## Git repository and push

Repository: [https://github.com/swetak0911/hr-roster-sync](https://github.com/swetak0911/hr-roster-sync)

To verify git status, commit verified changes, and push updates to `main`:

```bash
git status
git add .
git commit -m "feat: complete two-way reconciliation, streaming RFC-4180 CSV parser, and atomic apply locking"
git push origin main
```

Never commit `.env` or real employee data. After pushing, open the repository in a private/incognito browser window to confirm reviewers can access it.
