package com.moveinsync.roster.reconciliation;

import com.moveinsync.roster.RosterSyncApplication;
import com.moveinsync.roster.domain.Employee;
import com.moveinsync.roster.repository.EmployeeRepository;
import com.moveinsync.roster.repository.OutboxRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.annotation.Propagation;

import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@DataJpaTest
@org.springframework.test.context.ContextConfiguration(classes = RosterSyncApplication.class)
@Testcontainers(disabledWithoutDocker = true)
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
@Import(ReconciliationService.class)
class ReconciliationApplyConcurrencyTest {

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
    OutboxRepository outbox;

    @Autowired
    JdbcTemplate jdbc;

    @Test
    void concurrentApplyRequestsNeverApplyTwiceAndIncrementVersionExactlyOnce() throws Exception {
        String tenant = "acme";
        UUID runId = UUID.randomUUID();

        // 1. Insert existing active employee
        Employee emp = new Employee(tenant, "E-100", "Asha Rao", "asha@example.com", "M-1", "BLR", "+919876543210", 1L);
        employees.saveAndFlush(emp);

        // 2. Insert reconciliation run in READY_TO_APPLY
        jdbc.update("INSERT INTO reconciliation_runs(id, tenant_id, status, active_before, snapshot_rows, estimated_missing, missing_ratio, safety_brake, created_at) " +
                    "VALUES (?, ?, 'READY_TO_APPLY', 1, 1, 1, 0.20, false, now())", runId, tenant);

        // 3. Stage missing employee (E-100) and new employee (E-200)
        jdbc.update("INSERT INTO reconciliation_missing(run_id, tenant_id, employee_id) VALUES (?, ?, ?)", runId, tenant, "E-100");
        jdbc.update("INSERT INTO reconciliation_new(run_id, tenant_id, employee_id, name, email, manager_id, office_code, phone) " +
                    "VALUES (?, ?, ?, 'New Employee', 'new@example.com', 'M-1', 'BLR', '+919876543299')", runId, tenant, "E-200");

        // 4. Launch two simultaneous apply requests
        int threads = 2;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger conflictCount = new AtomicInteger(0);

        try {
            Future<?> f1 = pool.submit(() -> {
                try {
                    startLatch.await();
                    service.apply(tenant, runId);
                    successCount.incrementAndGet();
                } catch (IllegalStateException e) {
                    conflictCount.incrementAndGet();
                } catch (Exception e) {
                    throw new CompletionException(e);
                }
            });

            Future<?> f2 = pool.submit(() -> {
                try {
                    startLatch.await();
                    service.apply(tenant, runId);
                    successCount.incrementAndGet();
                } catch (IllegalStateException e) {
                    conflictCount.incrementAndGet();
                } catch (Exception e) {
                    throw new CompletionException(e);
                }
            });

            startLatch.countDown();
            f1.get(20, TimeUnit.SECONDS);
            f2.get(20, TimeUnit.SECONDS);

            // Assert exactly one thread succeeded and one received conflict
            assertEquals(1, successCount.get(), "Exactly one apply request must succeed");
            assertEquals(1, conflictCount.get(), "The concurrent apply request must be rejected with conflict");

            // 5. Verify final status is APPLIED
            String finalStatus = jdbc.queryForObject("SELECT status FROM reconciliation_runs WHERE id=?", String.class, runId);
            assertEquals("APPLIED", finalStatus);

            // 6. Verify employee E-100 was deactivated and version incremented from 1 to 2 exactly once
            Employee deactivatedEmp = employees.findByTenantIdAndEmployeeId(tenant, "E-100").orElseThrow();
            assertFalse(deactivatedEmp.isActive(), "E-100 must be deactivated");
            assertEquals(2L, deactivatedEmp.getSourceVersion(), "E-100 version must increment exactly once from 1 to 2");

            // 7. Verify new employee E-200 created with version 1
            Employee newEmp = employees.findByTenantIdAndEmployeeId(tenant, "E-200").orElseThrow();
            assertTrue(newEmp.isActive(), "E-200 must be active");
            assertEquals(1L, newEmp.getSourceVersion(), "E-200 initial version must be 1");

            // 8. Verify exactly 2 outbox events created (1 deactivation, 1 creation)
            assertEquals(2L, outbox.count(), "Outbox must have exactly 2 events created (no duplicates)");

            // 9. Repeated apply on already APPLIED run must fail with RUN_ALREADY_APPLIED and change nothing
            IllegalStateException ex = assertThrows(IllegalStateException.class, () -> service.apply(tenant, runId));
            assertEquals("RUN_ALREADY_APPLIED", ex.getMessage());

            // 10. Verify versions and outbox remain unchanged after repeated apply
            Employee postRepeated = employees.findByTenantIdAndEmployeeId(tenant, "E-100").orElseThrow();
            assertEquals(2L, postRepeated.getSourceVersion());
            assertEquals(2L, outbox.count());

            // 11. Cross-tenant apply is denied
            assertThrows(IllegalArgumentException.class, () -> service.apply("other-tenant", runId));

        } finally {
            pool.shutdownNow();
        }
    }
}
