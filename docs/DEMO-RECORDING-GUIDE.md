# HR Roster Sync - Assessment Demo Recording Guide

**Target length:** 4-5 minutes  
**Format:** 1080p screen recording. Use readable terminal/editor text, fictional employee data and no visible secrets.

This is a recording plan, not a pre-recorded video. Record the actual application on the assessment machine and retain the original clips.

## Before recording

1. Start Docker Desktop.
2. From the repository root, run: docker compose up --build -d
3. Confirm docker compose ps shows the required services running.
4. Confirm http://localhost:8080/actuator/health returns {"status":"UP"}.
5. Use demo credentials from your local .env; do not show the password file.
6. Prepare Postman or PowerShell requests and a sample CSV.
7. Close unrelated windows and notifications.

## Scene-by-scene plan

### 0:00-0:25 - Introduction

**Show:** README title, project folder and architecture diagram.

**Say:**  
"Hello, this is my implementation of MoveInSync Case Study 5, HR Roster Sync. The challenge is keeping employee data correct when events are duplicated, arrive out of order, or a nightly roster export is incomplete. The solution uses Java 21, Spring Boot, PostgreSQL, Redis, Kafka and a transactional outbox."

### 0:25-0:55 - Running services

**Show:** terminal output.

    docker compose ps
    Invoke-RestMethod http://localhost:8080/actuator/health

**Say:**  
"The application and dependencies run through Docker Compose. The health endpoint confirms that the application is ready."

### 0:55-1:40 - Event ordering and field ownership

**Show:** Send a CREATE event with version 1, resend it, then send an older version. Show API responses. Update transport details with the transport-admin account and show that an HR update does not overwrite those fields.

**Say:**  
"Each employee update has a source version. A repeated version is ignored, and an older version cannot replace newer data. HR and transport fields have separate ownership, so HR updates preserve pickup and special-assistance settings."

### 1:40-2:35 - Nightly reconciliation and safety brake

**Show:** Upload the sample CSV and display preview counts. If practical, show an intentionally incomplete test snapshot that crosses the configured threshold; do not apply it until the approval step is visible.

**Say:**  
"The nightly file is first processed as a preview. The service stages new, changed and missing employees. If the missing-active ratio reaches the safety threshold, the run requires operator approval. Preview itself does not deactivate anyone."

### 2:35-3:20 - Transactional outbox and crash recovery

**Show:** Run the test:

    mvn "-Dtest=OutboxProcessCrashRecoveryTest" test

**Say:**  
"The employee change and outbox row are committed in one database transaction. This test starts a separate process, commits the employee and outbox records, then forcibly stops that process before publication. A new publisher recovers the pending event and sends it to Kafka."

Only narrate what the actual test output demonstrates.

### 3:20-4:05 - Automated tests and 200k benchmark

**Show:** actual test output from your machine.

    mvn clean test
    mvn "-Dtest=ReconciliationPostgresIntegrationBenchmarkTest" "-Dbenchmark.rows=200000" test

**Say:**  
"The full suite currently has 63 passing tests. The recorded PostgreSQL 200,000-row run completed preview in 16.455 seconds and apply in 3.286 seconds. These are measurements from the recorded test environment, not a universal performance guarantee."

If you rerun the benchmark and get different numbers, narrate the new measured output instead.

### 4:05-4:30 - Wrap-up

**Show:** README assessment checklist and design note.

**Say:**  
"This implementation covers version-aware event ingestion, field ownership, safe nightly reconciliation, tenant-aware access, transactional event publishing, booking cancellation and automated failure testing. The design note also documents trade-offs and remaining production-hardening work."

## Recording checklist

- [ ] 1080p recording with readable terminal text
- [ ] Health check and Docker services shown
- [ ] Duplicate/stale event behavior shown
- [ ] HR vs transport field ownership shown
- [ ] Reconciliation preview and safety approval shown
- [ ] Crash recovery test output shown
- [ ] Full test and benchmark output shown
- [ ] No passwords, tokens or real employee data visible
- [ ] Export final video as MP4 and add its link to README/assessment submission
