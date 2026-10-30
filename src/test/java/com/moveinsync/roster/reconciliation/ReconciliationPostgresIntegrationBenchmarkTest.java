package com.moveinsync.roster.reconciliation;

import com.moveinsync.roster.RosterSyncApplication;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Configurable real PostgreSQL-backed reconciliation benchmark.
 *
 * Default: 5,000 DB rows and 5,000 CSV rows.
 * Larger run: mvn -Dtest=ReconciliationPostgresIntegrationBenchmarkTest
 *                 -Dbenchmark.rows=200000 test
 *
 * The test is intentionally kept in the normal test suite at its small default
 * size. The same code path is used for the full 200k assessment benchmark.
 */
@DataJpaTest
@ContextConfiguration(classes = RosterSyncApplication.class)
@Testcontainers(disabledWithoutDocker = true)
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
@Import(ReconciliationService.class)
class ReconciliationPostgresIntegrationBenchmarkTest {

    private static final String TENANT = "bench-tenant";

    @Container
    static final PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16-alpine");

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired
    ReconciliationService service;

    @Autowired
    JdbcTemplate jdbc;

    @TempDir
    Path temp;

    @Test
    void realPostgresReconciliationBenchmark() throws Exception {
        final int totalDbRows = Integer.getInteger("benchmark.rows", 5_000);
        assertTrue(totalDbRows >= 10, "benchmark.rows must be at least 10");
        final int unchangedRows = (int) (totalDbRows * 0.90);
        final int missingRows = totalDbRows - unchangedRows;
        final int newRows = missingRows;
        final int snapshotRows = unchangedRows + newRows;

        // Seed directly in PostgreSQL. This avoids allocating a Java object per employee.
        jdbc.execute("""
                INSERT INTO employees
                    (tenant_id, employee_id, name, email, manager_id, office_code,
                     phone, source_version, active, special_assistance, updated_at)
                SELECT
                    'bench-tenant',
                    'EMP-' || n,
                    'Employee ' || n,
                    'emp' || n || '@example.com',
                    'MGR-1',
                    'BLR',
                    '+919876543210',
                    1,
                    TRUE,
                    FALSE,
                    now()
                FROM generate_series(0, %d) AS n
                """.formatted(totalDbRows - 1));

        // Keep CSV generation streaming too: 90%% existing and 10%% new records.
        Path csvFile = temp.resolve("snapshot-bench-" + totalDbRows + ".csv");
        try (BufferedWriter writer = Files.newBufferedWriter(csvFile, StandardCharsets.UTF_8)) {
            writer.write("employeeId,name,email,managerId,officeCode,phone\n");
            for (int i = 0; i < unchangedRows; i++) {
                writer.write("EMP-" + i + ",Employee " + i + ",emp" + i
                        + "@example.com,MGR-1,BLR,+919876543210\n");
            }
            for (int i = totalDbRows; i < totalDbRows + newRows; i++) {
                writer.write("EMP-" + i + ",New Employee " + i + ",new" + i
                        + "@example.com,MGR-1,BLR,+919876543210\n");
            }
        }

        long previewStart = System.nanoTime();
        ReconciliationService.ReconciliationReport report;
        try (var input = Files.newInputStream(csvFile)) {
            report = service.preview(TENANT, input);
        }
        long previewMs = (System.nanoTime() - previewStart) / 1_000_000;

        System.out.printf(
                "[Real PostgreSQL Benchmark] DB rows=%d, CSV rows=%d, preview=%d ms, missing=%d, new=%d, changed=%d%n",
                totalDbRows, snapshotRows, previewMs,
                report.estimatedMissing(), report.estimatedNew(), report.estimatedChanged());

        assertEquals(totalDbRows, report.activeBefore());
        assertEquals(snapshotRows, report.snapshotRows());
        assertEquals(missingRows, report.estimatedMissing());
        assertEquals(newRows, report.estimatedNew());
        assertEquals(0, report.estimatedChanged());
        assertEquals((double) missingRows / totalDbRows, report.missingRatio(), 0.001);
        assertFalse(report.safetyBrake(), "10% missing must remain below the 30% safety-brake threshold");

        if (totalDbRows >= 200_000) {
            assertTrue(previewMs < 60_000,
                    "The real PostgreSQL-backed 200k preview must finish under 60 seconds");
        }

        assertEquals((long) missingRows, jdbc.queryForObject(
                "SELECT count(*) FROM reconciliation_missing WHERE run_id=? AND tenant_id=?",
                Long.class, report.runId(), TENANT));
        assertEquals((long) newRows, jdbc.queryForObject(
                "SELECT count(*) FROM reconciliation_new WHERE run_id=? AND tenant_id=?",
                Long.class, report.runId(), TENANT));
        assertEquals("READY_TO_APPLY", jdbc.queryForObject(
                "SELECT status FROM reconciliation_runs WHERE id=? AND tenant_id=?",
                String.class, report.runId(), TENANT));

        long applyStart = System.nanoTime();
        service.apply(TENANT, report.runId());
        long applyMs = (System.nanoTime() - applyStart) / 1_000_000;

        Long deactivated = jdbc.queryForObject(
                "SELECT count(*) FROM employees WHERE tenant_id=? AND active=false",
                Long.class, TENANT);
        Long totalEmployees = jdbc.queryForObject(
                "SELECT count(*) FROM employees WHERE tenant_id=?",
                Long.class, TENANT);
        Long outboxEvents = jdbc.queryForObject(
                "SELECT count(*) FROM outbox_events WHERE tenant_id=?",
                Long.class, TENANT);

        System.out.printf(
                "[Real PostgreSQL Benchmark] apply=%d ms, deactivated=%d, total employees=%d, outbox events=%d%n",
                applyMs, deactivated, totalEmployees, outboxEvents);

        assertEquals((long) missingRows, deactivated);
        assertEquals((long) (totalDbRows + newRows), totalEmployees);
        assertEquals((long) (missingRows + newRows), outboxEvents);
        assertEquals("APPLIED", jdbc.queryForObject(
                "SELECT status FROM reconciliation_runs WHERE id=? AND tenant_id=?",
                String.class, report.runId(), TENANT));
    }

    @Test
    void liveEmployeeUpdateAfterPreviewMustAbortStaleReconciliationApply() throws Exception {
        jdbc.update("""
                INSERT INTO employees
                    (tenant_id, employee_id, name, email, manager_id, office_code,
                     phone, source_version, active, special_assistance, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, TRUE, FALSE, now())
                """, TENANT, "RACE-1", "Original Name", "original@example.com",
                "MGR-1", "BLR", "+919876543210", 1L);

        Path csv = temp.resolve("race-snapshot.csv");
        Files.writeString(csv, """
                employeeId,name,email,managerId,officeCode,phone
                RACE-1,Snapshot Name,snapshot@example.com,MGR-1,HYD,+919876543210
                """, StandardCharsets.UTF_8);

        ReconciliationService.ReconciliationReport report;
        try (var input = Files.newInputStream(csv)) {
            report = service.preview(TENANT, input);
        }
        assertEquals(1, report.estimatedChanged());

        // Simulate a newer live HR event after preview but before apply.
        jdbc.update("""
                UPDATE employees
                SET name='Live Event Name', email='live@example.com',
                    source_version=2, updated_at=now()
                WHERE tenant_id=? AND employee_id=?
                """, TENANT, "RACE-1");

        IllegalStateException conflict = assertThrows(IllegalStateException.class,
                () -> service.apply(TENANT, report.runId()));
        assertTrue(conflict.getMessage().contains("RECONCILIATION_VERSION_CONFLICT"),
                "Expected optimistic version conflict but got: " + conflict.getMessage());

        assertEquals("Live Event Name", jdbc.queryForObject(
                "SELECT name FROM employees WHERE tenant_id=? AND employee_id=?",
                String.class, TENANT, "RACE-1"));
        assertEquals(2L, jdbc.queryForObject(
                "SELECT source_version FROM employees WHERE tenant_id=? AND employee_id=?",
                Long.class, TENANT, "RACE-1"));
        assertEquals(0L, jdbc.queryForObject(
                "SELECT count(*) FROM outbox_events WHERE tenant_id=? AND employee_id=?",
                Long.class, TENANT, "RACE-1"));
        assertEquals("READY_TO_APPLY", jdbc.queryForObject(
                "SELECT status FROM reconciliation_runs WHERE id=? AND tenant_id=?",
                String.class, report.runId(), TENANT));
    }
}
