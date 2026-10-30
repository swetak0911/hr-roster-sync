# Design Note - HR Roster Sync

**MoveInSync Case Study 5 | Advanced Track | Java 21 / Spring Boot**

## 1. Problem and goals

HR roster updates can be duplicated, delayed, delivered out of order, partially invalid, or represented by an incomplete nightly export. The service must preserve correct employee state, separate HR-owned and transport-owned fields, prevent unsafe mass deactivation, and reliably notify downstream systems.

The implementation uses source versions, tenant-aware authorization, PostgreSQL transactions, a transactional outbox, Kafka, Redis caching and staged nightly reconciliation.

## 2. Architecture

~~~mermaid
flowchart LR
  HR[HR source] -->|Versioned events| API[Spring Boot API]
  HR -->|Nightly CSV| PARSER[Streaming CSV parser]
  API --> EMP[(PostgreSQL)]
  PARSER --> BUCKETS[Disk-backed hash buckets]
  EMP --> BUCKETS
  BUCKETS --> STAGE[New / changed / missing staging]
  STAGE --> BRAKE[30% safety brake + approval]
  BRAKE -->|Apply with version checks| EMP
  API --> OUTBOX[(Transactional outbox)]
  EMP --> OUTBOX
  OUTBOX --> PUBLISHER[Outbox publisher]
  PUBLISHER --> KAFKA[Kafka]
  KAFKA --> LISTENER[Deactivation listener]
  LISTENER --> BOOKINGS[Cancel future bookings / free seats]
  API <--> REDIS[(Redis profile cache)]
  TRANSPORT[Transport admin] --> API

~~~

- **Spring Boot API:** event ingestion, transport-profile updates, employee reads and reconciliation.
- **PostgreSQL 16:** employees, reconciliation runs/staging rows and outbox events; Flyway manages schema migrations.
- **Redis 7:** tenant-scoped employee profile cache with a two-minute TTL; successful writes evict the affected cache entry.
- **Kafka:** downstream employee-change events; deactivation listener cancels future confirmed bookings and releases reserved seats.
- **Docker Compose:** local application dependencies and one-command startup.

## 3. Event ingestion, ordering and ownership

Each event contains tenant, employee ID, event type and source version. Records are validated independently. For one employee:

| Incoming version vs stored version | Behavior |
|---|---|
| Equal | Duplicate; no-op |
| Lower | Stale/out of order; ignored |
| Higher | Valid newer update can apply |

Database concurrency controls protect simultaneous updates to the same employee. Tenant identity is checked against the authenticated principal.

HR owns name, email, manager, office, phone and active status. Transport admins own pickup point and special-assistance details. HR updates modify only HR-owned fields.

## 4. Nightly reconciliation

1. Stream and validate CSV records, including quoted commas, escaped quotes and multiline values.
2. Partition file records and streamed database snapshot records into 256 disk-backed hash buckets.
3. Compare matching buckets and stage **new**, **changed** and **missing active** records in PostgreSQL.
4. Calculate missing-active / active-before-comparison.
5. If the ratio reaches the configured threshold (default 30%), hold the run for operator approval. Preview never changes live employee rows.
6. Apply staged changes transactionally.

### Stale-preview protection

Preview records each existing employee's expected source version. At apply time, updates are conditional on that expected version and affected-row counts are checked. If a live event changes an employee after preview, reconciliation aborts and rolls back instead of overwriting the newer event. The reconciliation run is row-locked and can be applied only once.

### Memory trade-off

CSV parsing and database reads are streamed; temporary disk buckets reduce the working set. A skewed hash distribution can still make an individual bucket large. The current approach therefore does **not** prove a strict worst-case fixed memory ceiling for every input distribution.

## 5. Transactional outbox and deactivation

Employee state and its outbox event are written in the same PostgreSQL transaction. A background publisher claims pending rows with SELECT FOR UPDATE SKIP LOCKED, sends them to Kafka, then marks them published. If Kafka is unavailable, the row remains retryable. Delivery is at least once, so consumers must deduplicate by immutable event ID.

The deactivation consumer cancels future confirmed cab bookings and releases reserved seats. Its operation is idempotent.

### Event sequence

~~~mermaid
sequenceDiagram
  participant HR as HR system
  participant API as Spring Boot API
  participant DB as PostgreSQL
  participant P as Outbox publisher
  participant K as Kafka
  participant B as Booking consumer
  HR->>API: Employee event (source version)
  API->>DB: Lock/check employee version
  alt Duplicate or stale
    API-->>HR: No-op result
  else Newer valid event
    API->>DB: Update employee + insert outbox row
    DB-->>API: Commit transaction
    API-->>HR: Accepted
    P->>DB: Claim pending row (SKIP LOCKED)
    P->>K: Publish event ID + payload
    K-->>B: Deactivation event
    B->>B: Cancel future confirmed rides; release seats
    P->>DB: Mark event published
  end

~~~

## 6. Failure-mode analysis

| Failure / race | Protection | Expected recovery |
|---|---|---|
| Duplicate event | Equal source version is a no-op | No duplicate employee mutation |
| Older event arrives late | Lower source version is ignored | Newer state remains |
| Invalid row in a batch | Per-record validation | Other valid records continue |
| Cross-tenant request | Principal tenant and role checks | Reject request without disclosing other tenant data |
| Incomplete nightly file | 30% safety brake | Hold for human review; no deactivation before approval |
| Live event arrives after preview | Expected source-version check at apply | Abort stale apply; create a fresh preview |
| Two apply requests race | Reconciliation row lock and state transition | At most one apply succeeds |
| Database transaction fails | Employee and outbox writes are atomic | Both roll back |
| Kafka is unavailable | Pending outbox rows remain retryable | Publisher retries after broker recovery |
| Process stops after DB commit, before publish | Outbox row is durable | Publisher sends it after restart |
| Kafka accepted event but marker was not committed | At-least-once delivery | Consumer deduplicates by event ID |
| Duplicate deactivation delivery | Idempotent booking cancellation | Already-cancelled bookings remain unchanged |

## 7. Consistency, availability and latency trade-offs

- **Per-employee consistency over write availability:** writes for one employee serialize through database controls. This can increase latency for hot employee IDs but prevents stale overwrites.
- **Durable change over immediate downstream visibility:** employee state and outbox can commit while Kafka is unavailable; downstream consumers observe the change after publisher recovery.
- **Cache latency vs freshness:** Redis reduces repeated profile reads; a two-minute TTL bounds staleness if eviction is missed. PostgreSQL remains the source of truth.
- **Disk I/O vs memory:** disk buckets reduce typical in-memory working set but add I/O and do not establish a worst-case memory bound.

## 8. Verification and measured evidence

The latest local full-suite run (2026-10-30 23:05:30 IST) reported **63 tests, 0 failures, 0 errors and 0 skipped** in 2 minutes 4 seconds.

A real PostgreSQL 16 integration benchmark with a 200,000-row database snapshot and 200,000-row CSV recorded:

| Measurement | Recorded result |
|---|---:|
| Preview/diff | 16.455 s |
| Apply | 3.286 s |
| Missing employees | 20,000 |
| New employees | 20,000 |
| Outbox events verified | 40,000 |
| Test elapsed time | 33.84 s |
| Maven elapsed time | 43.504 s |

This run met the case-study preview target of less than 60 seconds on the machine used. It is one recorded run, not a hardware-independent guarantee. CPU/RAM/OS details and repeated-run p50/p99/throughput measurements still need to be captured on the assessment machine.

The automated suite includes a process-crash recovery test: a child JVM commits employee and outbox rows, is forcibly terminated before publication, and a fresh publisher subsequently delivers the persisted event to embedded Kafka. This validates the tested crash window, not every possible infrastructure failure.

## 9. Known limitations and production hardening

- Disk-bucket skew means strict worst-case bounded memory is not yet proven.
- p50/p99 latency and throughput-under-load results have not yet been collected.
- Assessment authentication uses in-memory HTTP Basic demo accounts; production should use OIDC/OAuth2/JWT and persistent role/tenant administration.
- Published outbox rows are retained; production needs archive/retention management.
- The local Compose file uses the Bitnami legacy Kafka image; production should use a currently supported Kafka distribution and hardened broker configuration.
- Production deployment also needs TLS, managed secrets, operational alert thresholds, backup/restore drills and load testing.

## 10. Key invariants

1. A tenant cannot read or mutate another tenant's employee data.
2. Duplicate or older source versions cannot overwrite newer state.
3. HR updates preserve transport-owned fields.
4. Employee changes and outbox records commit or roll back together.
5. Unsafe reconciliation cannot deactivate employees before required approval.
6. A stale reconciliation preview cannot overwrite an employee changed since preview.
7. A reconciliation run is applied at most once.
8. Consumers handle repeated event delivery idempotently.
