package com.moveinsync.roster.reconciliation;

import com.moveinsync.roster.RosterSyncApplication;
import com.moveinsync.roster.repository.EmployeeRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
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
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Real PostgreSQL-backed integration benchmark using Testcontainers.
 * Measures real database cursor streaming (fetchSize=1000), 256 disk-bucket partitioning,
 * paired hash join, and real JDBC batch inserts into PostgreSQL staging tables.
 */
@DataJpaTest
@org.springframework.test.context.ContextConfiguration(classes = RosterSyncApplication.class)
@Testcontainers(disabledWithoutDocker = true)
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
@Import(ReconciliationService.class)
class ReconciliationPostgresIntegrationBenchmarkTest {

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired
    ReconciliationService service;

    @Autowired
    EmployeeRepository employees;

    @Autowired
    JdbcTemplate jdbc;

    @TempDir
    Path temp;

    @Test
    void realPostgresReconciliationBenchmark() throws Exception {
        final int totalDbRows = 5_000;
        final String tenant = "bench-tenant";

        // Seed 5,000 real employee records into PostgreSQL via batch insert
        List<Object[]> batch = new ArrayList<>(500);
        for (int i = 0; i < totalDbRows; i++) {
            batch.add(new Object[]{tenant, "EMP-" + i, "Employee " + i, "emp" + i + "@example.com", "MGR-1", "BLR", "+919876543210", 1L, true, false});
            if (batch.size() >= 500) {
                jdbc.batchUpdate("INSERT INTO employees(tenant_id, employee_id, name, email, manager_id, office_code, phone, source_version, active, special_assistance, updated_at) " +
                                 "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())", batch);
                batch.clear();
            }
        }
        if (!batch.isEmpty()) {
            jdbc.batchUpdate("INSERT INTO employees(tenant_id, employee_id, name, email, manager_id, office_code, phone, source_version, active, special_assistance, updated_at) " +
                             "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())", batch);
            batch.clear();
        }

        // Create CSV snapshot with:
        // - 4,500 unchanged
        // - 500 omitted (missing -> 10% missing ratio)
        // - 500 new employees
        Path csvFile = temp.resolve("snapshot-bench.csv");
        try (BufferedWriter writer = Files.newBufferedWriter(csvFile, StandardCharsets.UTF_8)) {
            writer.write("employeeId,name,email,managerId,officeCode,phone\n");
            // 4,500 existing
            for (int i = 0; i < 4_500; i++) {
                writer.write("EMP-" + i + ",Employee " + i + ",emp" + i + "@example.com,MGR-1,BLR,+919876543210\n");
            }
            // 500 new
            for (int i = 5_000; i < 5_500; i++) {
                writer.write("EMP-" + i + ",New Employee " + i + ",new" + i + "@example.com,MGR-1,BLR,+919876543210\n");
            }
        }

        long startTime = System.nanoTime();
        ReconciliationService.ReconciliationReport report;
        try (var input = Files.newInputStream(csvFile)) {
            report = service.preview(tenant, input);
        }
        long elapsedMs = (System.nanoTime() - startTime) / 1_000_000;

        System.out.printf("[Real PostgreSQL Integration Benchmark] 5,000 DB rows + 5,000 CSV rows: %d ms (missing=%d, new=%d, changed=%d)%n",
                elapsedMs, report.estimatedMissing(), report.estimatedNew(), report.estimatedChanged());

        assertEquals(5_000, report.activeBefore());
        assertEquals(5_000, report.snapshotRows());
        assertEquals(500, report.estimatedMissing()); // EMP-4500 to EMP-4999
        assertEquals(500, report.estimatedNew());     // EMP-5000 to EMP-5499
        assertEquals(0, report.estimatedChanged());
        assertEquals(0.10, report.missingRatio(), 0.001);
        assertFalse(report.safetyBrake(), "10% is below 30% brake threshold");
        assertTrue(elapsedMs < 30_000, "Benchmark should finish comfortably under 30 seconds");

        // Verify real staging writes directly in PostgreSQL database
        Long stagedMissingCount = jdbc.queryForObject(
                "SELECT count(*) FROM reconciliation_missing WHERE run_id=? AND tenant_id=?", Long.class, report.runId(), tenant);
        assertEquals(500L, stagedMissingCount, "PostgreSQL reconciliation_missing must contain 500 staged records");

        Long stagedNewCount = jdbc.queryForObject(
                "SELECT count(*) FROM reconciliation_new WHERE run_id=? AND tenant_id=?", Long.class, report.runId(), tenant);
        assertEquals(500L, stagedNewCount, "PostgreSQL reconciliation_new must contain 500 staged records");

        Long stagedChangedCount = jdbc.queryForObject(
                "SELECT count(*) FROM reconciliation_changed WHERE run_id=? AND tenant_id=?", Long.class, report.runId(), tenant);
        assertEquals(0L, stagedChangedCount, "PostgreSQL reconciliation_changed must contain 0 staged records");

        String runStatus = jdbc.queryForObject(
                "SELECT status FROM reconciliation_runs WHERE id=? AND tenant_id=?", String.class, report.runId(), tenant);
        assertEquals("READY_TO_APPLY", runStatus, "PostgreSQL reconciliation_runs status must be READY_TO_APPLY");

        // Apply the benchmark run to verify real database mutations and outbox records
        service.apply(tenant, report.runId());

        String appliedStatus = jdbc.queryForObject(
                "SELECT status FROM reconciliation_runs WHERE id=? AND tenant_id=?", String.class, report.runId(), tenant);
        assertEquals("APPLIED", appliedStatus, "PostgreSQL reconciliation_runs status must be APPLIED");

        Long postDeactivatedCount = jdbc.queryForObject(
                "SELECT count(*) FROM employees WHERE tenant_id=? AND active=false", Long.class, tenant);
        assertEquals(500L, postDeactivatedCount, "PostgreSQL must have 500 deactivated employees");

        Long postTotalEmployees = jdbc.queryForObject(
                "SELECT count(*) FROM employees WHERE tenant_id=?", Long.class, tenant);
        assertEquals(5_500L, postTotalEmployees, "PostgreSQL total employees must be 5,500 (5,000 initial + 500 new)");

        Long outboxCount = jdbc.queryForObject(
                "SELECT count(*) FROM outbox_events WHERE tenant_id=?", Long.class, tenant);
        assertEquals(1_000L, outboxCount, "PostgreSQL outbox_events must contain 1,000 events (500 deactivations + 500 creations)");
    }
}

