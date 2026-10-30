# HR Roster Sync - MoveInSync Case Study 5

**Advanced Track | Java 21 | Spring Boot | PostgreSQL | Kafka | Redis**

A backend implementation of MoveInSync's Case Study 5: **HR Roster Sync - Idempotent, Out-of-Order, Self-Healing**.

The service keeps employee roster data consistent when HR events are duplicated, delayed, delivered out of order, partially invalid, or inconsistent with a nightly full snapshot. It also protects transport-owned employee details and publishes reliable downstream events.

---

## 1. Case-study requirements and implementation

| Requirement | Implementation |
|---|---|
| Ingest employee create, update and deactivate events | REST event-ingestion API |
| Validate records independently | Per-record validation and error reporting; one invalid record does not invalidate the entire batch |
| Handle duplicate and out-of-order events | Per-employee source-version checks; duplicate and stale events are ignored |
| Respect field ownership | HR updates change HR-owned fields only; transport-owned pickup and assistance fields are preserved |
| Reconcile up to 200,000 employees | Streaming CSV parsing and database reads, with disk-backed hash buckets for snapshot comparison |
| Prevent unsafe mass deactivation | Safety brake pauses reconciliation when the missing-active-employee threshold is reached; operator approval is required before apply |
| Publish every applied change reliably | Employee update and outbox row are committed in the same database transaction; a publisher delivers outbox events to Kafka |
| Handle employee deactivation downstream | Kafka consumer cancels future confirmed cab bookings and releases reserved seats |
| Protect tenant data | Authenticated tenant and role checks on API operations |
| Operate and troubleshoot the service | Actuator health/metrics, Redis profile cache, structured server-side logging and safe API errors |

## 2. Architecture

The application is a Spring Boot service backed by PostgreSQL, Redis and Kafka.

- **PostgreSQL** stores employee records, reconciliation runs/staging rows and outbox events.
- **Flyway** manages database schema migrations.
- **Redis** caches employee profile reads. Employee writes evict the corresponding tenant-scoped cache entry.
- **Kafka** carries committed employee-change events to downstream consumers.
- **Transactional outbox** closes the database-write/message-publish gap: the business change and its outbox row are committed atomically. The publisher retries unpublished rows. Delivery is at least once, so consumers must deduplicate by event ID.
- **Docker Compose** starts the application and its local dependencies.

### Event ingestion flow

1. The API authenticates the caller and checks tenant/role access.
2. The request is validated.
3. The employee's source version is compared with the stored version.
4. A duplicate version is a no-op; an older version is treated as stale; a newer version can be applied.
5. The employee update and outbox event are committed in one database transaction.
6. The outbox publisher sends the event to Kafka.

### Nightly reconciliation flow

1. Upload a full employee CSV and create a **preview**.
2. The parser reads records incrementally and validates them.
3. The service streams the current tenant snapshot and compares it with the file using disk-backed hash buckets.
4. It stages three groups:
   - **New:** in the file, not in the database.
   - **Changed:** in both, with a changed HR-owned field.
   - **Missing:** active in the database, absent from the file.
5. If the missing-active count reaches the configured safety threshold, the run requires operator approval. No employee is deactivated during preview.
6. Apply runs transactionally. The staged expected source versions are checked against current employee versions. If a live event changed an employee after preview, apply fails rather than applying a stale snapshot.
7. Applied employee changes and their outbox events are committed atomically.

## 3. Field ownership

| HR-owned fields | Transport-admin-owned fields |
|---|---|
| Name | Pickup point |
| Email | Special-assistance needs |
| Manager | |
| Office | |
| Phone | |
| Active/inactive status | |

An HR event or reconciliation does not overwrite transport-admin-owned details.

## 4. Technology stack

| Technology | Purpose |
|---|---|
| Java 21 | Backend language |
| Spring Boot 3.5.6 | REST API and application framework |
| Spring Data JPA / JDBC | Persistence and streaming database operations |
| PostgreSQL 16 | Primary database |
| Flyway | Schema migrations |
| Redis 7 | Employee profile cache |
| Apache Kafka | Downstream event delivery |
| Docker Compose | Local multi-service environment |
| Maven | Build and test lifecycle |
| JUnit, Mockito, Testcontainers, Embedded Kafka | Automated and integration testing |

---

## 5. Run locally

### Prerequisites

- Docker Desktop with the Docker engine running
- Docker Compose v2
- JDK 21
- Maven 3.9 or newer (for Maven commands)

### Option A: Run the complete stack with Docker Compose

From the repository root:

**Windows PowerShell**
```powershell
Copy-Item .env.example .env
```

**macOS / Linux**
```bash
cp .env.example .env
```

Open `.env` and set your own demo passwords. The values in `.env` are used by Docker Compose; the API examples below use the example values from `.env.example`.

Start the services:

```bash
docker compose up --build -d
```

Check service status and application logs:

```bash
docker compose ps
docker compose logs -f app
```

The API is available at:

- Application: `http://localhost:8080`
- Health: `http://localhost:8080/actuator/health`

A healthy application returns:

```json
{"status":"UP"}
```

Stop the stack:

```bash
docker compose down
```

To also remove the local PostgreSQL volume and its data (destructive):

```bash
docker compose down -v
```

### Option B: Run the application with Maven

Start PostgreSQL, Redis and Kafka first:

```bash
docker compose up -d postgres redis kafka
```

Set the Kafka bootstrap address and the demo passwords in your terminal environment, using the values you configured in `.env`. Then run:

**Windows PowerShell**
```powershell
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9094"
mvn spring-boot:run
```

**macOS / Linux**
```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9094
mvn spring-boot:run
```

The application also needs the database and Redis connection settings configured for your local services. Docker Compose supplies these automatically when you use Option A.

### Environment variables

See [`.env.example`](.env.example). It contains demo-only values; do not use them for a shared or production deployment.

| Variable | Purpose |
|---|---|
| `OPERATOR_PASSWORD` | Password for the `acme` operator |
| `TRANSPORT_ADMIN_PASSWORD` | Password for the `acme` transport admin |
| `ADMIN_PASSWORD` | Password for the `acme` admin |
| `GLOBEX_OPERATOR_PASSWORD` | Password for the `globex` operator |
| `EMPLOYEE_PASSWORD` | Password for the demo employee account |
| `MISSING_EMPLOYEE_THRESHOLD` | Missing-active-employee safety-brake threshold; default example is `0.30` |

---

## 6. Demo accounts

The assessment build uses in-memory demo users and HTTP Basic authentication. These are for local evaluation only. Their passwords are read from environment variables.

| Username | Role | Tenant | Password environment variable |
|---|---|---|---|
| `operator` | OPERATOR | `acme` | `OPERATOR_PASSWORD` |
| `transport-admin` | TRANSPORT_ADMIN | `acme` | `TRANSPORT_ADMIN_PASSWORD` |
| `admin` | ADMIN | `acme` | `ADMIN_PASSWORD` |
| `globex-operator` | OPERATOR | `globex` | `GLOBEX_OPERATOR_PASSWORD` |
| `employee-e1001` | EMPLOYEE (own profile only) | `acme` | `EMPLOYEE_PASSWORD` |

For a local run using the unmodified example file, the example passwords are:

- `operator`: `change-this-operator-password`
- `transport-admin`: `change-this-transport-password`
- `admin`: `change-this-admin-password`
- `globex-operator`: `change-this-globex-password`
- `employee-e1001`: `change-this-employee-password`

If you edit `.env`, use the passwords you set there instead. Never commit `.env` or share real credentials.

**Production security note:** replace in-memory HTTP Basic demo authentication with an enterprise identity provider (for example, OAuth2/OIDC/JWT) and persistent role/tenant administration. Use TLS and managed secrets in deployed environments.

---

## 7. API examples

The examples use `acme` and the default values in the checked-in `.env.example`. If you changed the passwords, substitute your configured values.

### Create an employee event

```bash
curl -u operator:change-this-operator-password -X POST http://localhost:8080/api/v1/events \
  -H 'Content-Type: application/json' -H 'X-Tenant-Id: acme' \
  -d '{"tenantId":"acme","employeeId":"E-1001","type":"CREATE","version":1,"name":"Asha Rao","email":"asha@example.com","managerId":"M-10","officeCode":"BLR","phone":"+919876543210"}'
```

Send the same event again to verify duplicate handling.

### Deactivate an employee

```bash
curl -u operator:change-this-operator-password -X POST http://localhost:8080/api/v1/events \
  -H 'Content-Type: application/json' -H 'X-Tenant-Id: acme' \
  -d '{"tenantId":"acme","employeeId":"E-1001","type":"DEACTIVATE","version":2}'
```

### Update transport-owned details

```bash
curl -u transport-admin:change-this-transport-password -X PUT http://localhost:8080/api/v1/transport-profiles/E-1001 \
  -H 'Content-Type: application/json' -H 'X-Tenant-Id: acme' \
  -d '{"pickupPoint":"Indiranagar Metro","specialAssistance":false}'
```

### Preview a nightly CSV

```bash
curl -u operator:change-this-operator-password -X POST http://localhost:8080/api/v1/reconciliation/preview \
  -H 'X-Tenant-Id: acme' -F 'file=@sample-data/employees.csv'
```

The CSV header must include:

```text
employeeId,name,email,managerId,officeCode,phone
```

The preview response contains a reconciliation run ID and counts for new, changed and missing employees. Preview does not modify live employee records.

### Approve a run when the safety brake is triggered

Replace `<RUN_ID>` with the ID returned by preview:

```bash
curl -u operator:change-this-operator-password -X POST http://localhost:8080/api/v1/reconciliation/<RUN_ID>/approve \
  -H 'X-Tenant-Id: acme'
```

### Apply the preview

```bash
curl -u operator:change-this-operator-password -X POST http://localhost:8080/api/v1/reconciliation/<RUN_ID>/apply \
  -H 'X-Tenant-Id: acme'
```

Apply reports the number of employees created, updated and deactivated. A run cannot be applied repeatedly. If a live employee event changes a staged employee after preview, the expected-version check rejects the stale apply; create a fresh preview instead.

---

## 8. Automated tests

Run the complete test suite from the repository root:

```bash
mvn clean test
```

**Docker Desktop must be running** for Testcontainers-based PostgreSQL integration tests.

### Latest full-suite result

The latest local run reported:

| Metric | Result |
|---|---:|
| Tests | **63** |
| Failures | **0** |
| Errors | **0** |
| Skipped | **0** |
| Maven build | **BUILD SUCCESS** |
| Total time | **2 min 4 sec** |
| Run finished | **2026-10-30 23:05:30 IST** |

The test suite covers:

- Event API validation, authorization and idempotency.
- Duplicate and stale source versions, out-of-order updates and employee concurrency.
- HR/transport field ownership.
- Three-way reconciliation, CSV parsing (quoted fields, escaped quotes, multiline values, CRLF and buffer boundaries) and malformed-row handling.
- Safety-brake approval and prevention of unsafe deactivation.
- Concurrent apply/approval protection and stale-preview detection using expected source versions.
- Outbox retry, multi-publisher `SKIP LOCKED` behavior and deactivation listener idempotency.
- A child-process crash-recovery scenario: a separate JVM commits the employee and outbox row, is forcibly terminated before publishing, and a fresh publisher subsequently delivers the persisted event to embedded Kafka.

There are 14 test classes in the current test-suite report. Test counts can change as tests are added; rerun `mvn clean test` to reproduce the result on your machine.

---

## 9. Performance checks

### Reproduce the 200,000-row PostgreSQL benchmark

With Docker Desktop running, execute:

```bash
mvn "-Dtest=ReconciliationPostgresIntegrationBenchmarkTest" "-Dbenchmark.rows=200000" test
```

The benchmark uses Testcontainers PostgreSQL and a generated CSV snapshot. The row count is configurable with `benchmark.rows`.

### Recorded measurements

| Scenario | Recorded result | Notes |
|---|---:|---|
| 200,000-row mocked repository benchmark | ~1.98-5.70 s | Algorithm/file-processing test with mocked database access |
| Earlier 5,000-row PostgreSQL integration run | ~2.20-3.72 s | Earlier recorded run |
| 200,000-row PostgreSQL integration run | 16.455 s preview; 3.286 s apply | One recorded local run |

In the recorded 200,000-row PostgreSQL run:

- 200,000 database employee rows and a 200,000-row CSV snapshot were processed.
- The diff identified 20,000 missing and 20,000 new employees.
- Apply deactivated 20,000 missing employees and inserted 20,000 new employees.
- 40,000 outbox events were verified.
- The benchmark test took 33.84 seconds; Maven completed in 43.504 seconds.
- The measured preview time was below the case-study target of 60 seconds in that environment.

**Measurement limitations:** these are recorded results from one local machine/run, not a guarantee for other hardware or deployment configurations. The benchmark does not include a published p50/p99 latency distribution or a throughput-under-load study. Record CPU, RAM, OS, JVM and repeated-run results when preparing the final assessment evidence.

**Memory limitation:** CSV parsing and database reads are streamed, and disk-backed hash buckets reduce the amount of data held in memory at once. However, if employee IDs are distributed very unevenly, one bucket can become large. The current benchmark does not prove a strict worst-case bounded-memory guarantee for every possible input distribution.

---

## 10. Design decisions and known limitations

| Area | Current decision / limitation |
|---|---|
| Event ordering | Source versions and database concurrency controls protect per-employee ordering. |
| Reconciliation race safety | Apply checks staged expected source versions; a stale preview is rejected rather than overwriting a newer live event. |
| Outbox delivery | Transactional outbox with at-least-once Kafka delivery. Consumers must deduplicate using the event ID. |
| Snapshot memory | Streaming plus disk-backed buckets; worst-case bucket skew still needs stronger bounded-memory proof. |
| Authentication | In-memory HTTP Basic users are assessment/demo authentication, not production identity management. |
| Outbox retention | Published rows are retained; production should add a safe archive/retention policy. |
| Kafka local image | Compose currently uses the Bitnami legacy Kafka image for the local assessment environment; choose and maintain a supported Kafka distribution for production. |
| Performance evidence | One recorded 200k integration run is available; p50/p99 and load-throughput measurements remain to be collected. |

---

## 11. Assessment evidence checklist

- [x] Employee event ingestion and per-record validation
- [x] Duplicate/stale/out-of-order event handling
- [x] HR and transport field ownership
- [x] Nightly CSV preview and three-way reconciliation
- [x] 30% safety brake and operator approval
- [x] Transactional outbox and Kafka publisher
- [x] Deactivation booking-cancellation consumer
- [x] Tenant/role checks and Redis cache
- [x] Automated test suite: 63 tests passed in the latest local run
- [x] Recorded 200,000-row PostgreSQL benchmark below 60 seconds for preview
- [x] Process-crash recovery test
- [ ] Capture benchmark machine details and repeated measurements
- [ ] Add p50/p99 and throughput-under-load results
- [ ] Record the short end-to-end demo video
- [x] Add design note, architecture/sequence diagrams, trade-offs and failure-mode table ([design note](docs/DESIGN.md))

---

## 12. Repository and safety

Repository: [swetak0911/hr-roster-sync](https://github.com/swetak0911/hr-roster-sync)

- Never commit `.env`, credentials, tokens or real employee data.
- The checked-in `.env.example` contains demo credentials only; replace them before sharing a running instance.
- Review the design and limitations above before treating this assessment implementation as production-ready.
