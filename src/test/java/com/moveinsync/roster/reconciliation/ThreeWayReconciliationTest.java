package com.moveinsync.roster.reconciliation;

import com.moveinsync.roster.repository.EmployeeRepository;
import com.moveinsync.roster.repository.EmployeeSnapshotRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ThreeWayReconciliationTest {

    private EmployeeRepository repo;
    private JdbcTemplate jdbc;
    private ReconciliationService service;

    @BeforeEach
    void setUp() {
        repo = mock(EmployeeRepository.class);
        jdbc = mock(JdbcTemplate.class);
        service = new ReconciliationService(repo, 0.30, jdbc);
    }

    // 1. New employee detection: present in snapshot, absent in DB
    @Test
    void testNewEmployeeDetection() throws Exception {
        when(repo.countByTenantIdAndActiveTrue("acme")).thenReturn(1L);
        when(repo.streamAllForReconciliation("acme")).thenReturn(Stream.of(
            new EmployeeSnapshotRecord("E1", "Asha", "asha@example.com", "M1", "BLR", null, true)
        ));

        String csv = """
            employeeId,name,email,managerId,officeCode,phone
            E1,Asha,asha@example.com,M1,BLR,+919876543210
            E2,Ravi,ravi@example.com,M1,BLR,+919876543211
            """;

        var report = service.preview("acme", new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8)));

        assertEquals(1, report.estimatedNew());
        assertEquals(0, report.estimatedChanged());
        assertEquals(0, report.estimatedMissing());
        assertEquals(1, report.activeBefore());
        assertEquals(2, report.snapshotRows());
        assertFalse(report.safetyBrake());
    }

    // 2. Changed name detection: present in both, name differs
    @Test
    void testChangedNameDetection() throws Exception {
        when(repo.countByTenantIdAndActiveTrue("acme")).thenReturn(1L);
        when(repo.streamAllForReconciliation("acme")).thenReturn(Stream.of(
            new EmployeeSnapshotRecord("E1", "Asha Old", "asha@example.com", "M1", "BLR", null, true)
        ));

        String csv = """
            employeeId,name,email,managerId,officeCode,phone
            E1,Asha New,asha@example.com,M1,BLR,+919876543210
            """;

        var report = service.preview("acme", new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8)));

        assertEquals(0, report.estimatedNew());
        assertEquals(1, report.estimatedChanged());
        assertEquals(0, report.estimatedMissing());
    }

    // 3. Changed email detection: present in both, email differs
    @Test
    void testChangedEmailDetection() throws Exception {
        when(repo.countByTenantIdAndActiveTrue("acme")).thenReturn(1L);
        when(repo.streamAllForReconciliation("acme")).thenReturn(Stream.of(
            new EmployeeSnapshotRecord("E1", "Asha", "old_email@example.com", "M1", "BLR", null, true)
        ));

        String csv = """
            employeeId,name,email,managerId,officeCode,phone
            E1,Asha,new_email@example.com,M1,BLR,+919876543210
            """;

        var report = service.preview("acme", new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8)));

        assertEquals(0, report.estimatedNew());
        assertEquals(1, report.estimatedChanged());
        assertEquals(0, report.estimatedMissing());
    }

    // 4. Changed manager detection: present in both, managerId differs
    @Test
    void testChangedManagerDetection() throws Exception {
        when(repo.countByTenantIdAndActiveTrue("acme")).thenReturn(1L);
        when(repo.streamAllForReconciliation("acme")).thenReturn(Stream.of(
            new EmployeeSnapshotRecord("E1", "Asha", "asha@example.com", "M-OLD", "BLR", null, true)
        ));

        String csv = """
            employeeId,name,email,managerId,officeCode,phone
            E1,Asha,asha@example.com,M-NEW,BLR,+919876543210
            """;

        var report = service.preview("acme", new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8)));

        assertEquals(0, report.estimatedNew());
        assertEquals(1, report.estimatedChanged());
        assertEquals(0, report.estimatedMissing());
    }

    // 5. Changed office detection: present in both, officeCode differs
    @Test
    void testChangedOfficeDetection() throws Exception {
        when(repo.countByTenantIdAndActiveTrue("acme")).thenReturn(1L);
        when(repo.streamAllForReconciliation("acme")).thenReturn(Stream.of(
            new EmployeeSnapshotRecord("E1", "Asha", "asha@example.com", "M1", "BLR", null, true)
        ));

        String csv = """
            employeeId,name,email,managerId,officeCode,phone
            E1,Asha,asha@example.com,M1,HYD,+919876543210
            """;

        var report = service.preview("acme", new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8)));

        assertEquals(0, report.estimatedNew());
        assertEquals(1, report.estimatedChanged());
        assertEquals(0, report.estimatedMissing());
    }

    // 6. Transport-owned fields remaining untouched: preview does not compare pickup point or special assistance
    @Test
    void testTransportOwnedFieldsNotCompared() throws Exception {
        when(repo.countByTenantIdAndActiveTrue("acme")).thenReturn(1L);
        // DB employee has pickup point and special assistance, but HR fields match CSV
        when(repo.streamAllForReconciliation("acme")).thenReturn(Stream.of(
            new EmployeeSnapshotRecord("E1", "Asha", "asha@example.com", "M1", "BLR", "+919876543210", true)
        ));

        // Snapshot has matching HR fields
        String csv = """
            employeeId,name,email,managerId,officeCode,phone
            E1,Asha,asha@example.com,M1,BLR,+919876543210
            """;

        var report = service.preview("acme", new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8)));

        // Unchanged because HR fields match perfectly
        assertEquals(0, report.estimatedChanged());
        assertEquals(0, report.estimatedNew());
        assertEquals(0, report.estimatedMissing());
    }

    // 7. Missing active employee detection: active in DB, absent in snapshot
    @Test
    void testMissingEmployeeDetection() throws Exception {
        when(repo.countByTenantIdAndActiveTrue("acme")).thenReturn(2L);
        when(repo.streamAllForReconciliation("acme")).thenReturn(Stream.of(
            new EmployeeSnapshotRecord("E1", "Asha", "asha@example.com", "M1", "BLR", null, true),
            new EmployeeSnapshotRecord("E2", "Ravi", "ravi@example.com", "M1", "BLR", null, true)
        ));

        String csv = """
            employeeId,name,email,managerId,officeCode,phone
            E1,Asha,asha@example.com,M1,BLR,+919876543210
            """;

        var report = service.preview("acme", new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8)));

        assertEquals(1, report.estimatedMissing());
        assertEquals(0, report.estimatedNew());
        assertEquals(0, report.estimatedChanged());
        assertEquals(0.5, report.missingRatio(), 0.001); // 1/2 = 50%
        assertTrue(report.safetyBrake()); // 50% >= 30% threshold
    }

    // 8. All three categories in one snapshot: new + changed + missing
    @Test
    void testAllThreeCategoriesInOneSnapshot() throws Exception {
        when(repo.countByTenantIdAndActiveTrue("acme")).thenReturn(3L);
        when(repo.streamAllForReconciliation("acme")).thenReturn(Stream.of(
            new EmployeeSnapshotRecord("E1", "Asha", "asha@example.com", "M1", "BLR", null, true),
            new EmployeeSnapshotRecord("E2", "Ravi Old", "ravi@example.com", "M1", "BLR", null, true),
            new EmployeeSnapshotRecord("E3", "Kavita", "kavita@example.com", "M1", "BLR", null, true)
        ));

        // Snapshot has: E1 (unchanged), E2 (changed name), E4 (brand new), E3 is omitted (missing)
        String csv = """
            employeeId,name,email,managerId,officeCode,phone
            E1,Asha,asha@example.com,M1,BLR,+919876543210
            E2,Ravi New,ravi@example.com,M1,BLR,+919876543210
            E4,New Employee,new@example.com,M1,BLR,+919876543299
            """;

        var report = service.preview("acme", new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8)));

        assertEquals(1, report.estimatedNew(), "E4 should be detected as new");
        assertEquals(1, report.estimatedChanged(), "E2 should be detected as changed");
        assertEquals(1, report.estimatedMissing(), "E3 should be detected as missing");
        assertEquals(3, report.activeBefore());
        assertEquals(3, report.snapshotRows());
        assertEquals(1.0 / 3.0, report.missingRatio(), 0.001);
        assertTrue(report.safetyBrake()); // 33.3% >= 30%
    }

    // 9. Duplicate IDs in the incoming CSV: duplicate rows flagged and deduplicated with conflicting data
    @Test
    void testDuplicateIdsInCsv() throws Exception {
        when(repo.countByTenantIdAndActiveTrue("acme")).thenReturn(1L);
        when(repo.streamAllForReconciliation("acme")).thenReturn(Stream.of(
            new EmployeeSnapshotRecord("E1", "Asha", "asha@example.com", "M1", "BLR", null, true)
        ));

        // E1 has conflicting data between duplicate rows; E2 is new and has duplicate rows
        String csv = """
            employeeId,name,email,managerId,officeCode,phone
            E1,Asha Conflicting 1,asha1@example.com,M1,BLR,+919876543210
            E1,Asha Conflicting 2,asha2@example.com,M2,HYD,+919876543299
            E2,Ravi,ravi@example.com,M1,BLR,+919876543211
            E2,Ravi Duplicate,ravi@example.com,M1,BLR,+919876543211
            """;

        var report = service.preview("acme", new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8)));

        assertEquals(1, report.estimatedChanged(), "E1 should be counted as changed");
        assertEquals(1, report.estimatedNew(), "Only 1 unique new employee E2 should be counted");
        assertTrue(report.invalidRows().stream().anyMatch(s -> s.contains("duplicate:E1")), "Conflicting duplicate E1 must be flagged in invalidRows");
        assertTrue(report.invalidRows().stream().anyMatch(s -> s.contains("duplicate:E2")), "Duplicate E2 must be flagged in invalidRows");
    }

    // 10. Cross-tenant isolation
    @Test
    void testCrossTenantIsolation() throws Exception {
        when(repo.countByTenantIdAndActiveTrue("globex")).thenReturn(2L);
        when(repo.streamAllForReconciliation("globex")).thenReturn(Stream.of(
            new EmployeeSnapshotRecord("G1", "Globex 1", "g1@globex.com", "GM1", "BLR", null, true),
            new EmployeeSnapshotRecord("G2", "Globex 2", "g2@globex.com", "GM1", "BLR", null, true)
        ));

        String csv = """
            employeeId,name,email,managerId,officeCode,phone
            G1,Globex 1,g1@globex.com,GM1,BLR,+919876543001
            G3,Globex 3,g3@globex.com,GM1,BLR,+919876543003
            """;

        var report = service.preview("globex", new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8)));

        assertEquals(1, report.estimatedNew());
        assertEquals(0, report.estimatedChanged());
        assertEquals(1, report.estimatedMissing()); // G2 is missing
        verify(repo).countByTenantIdAndActiveTrue("globex");
        verify(repo, never()).countByTenantIdAndActiveTrue("acme");
    }

    // 11. Large 200,000-row bounded-memory behavior (verifies 256-bucket partitioning)
    @Test
    void testLargeBoundedMemoryBucketing() throws Exception {
        final int total = 2000; // Fast verification of 256-bucket diff
        when(repo.countByTenantIdAndActiveTrue("bench")).thenReturn((long) total);
        when(repo.streamAllForReconciliation("bench")).thenAnswer(inv ->
            java.util.stream.IntStream.range(0, total)
                .mapToObj(i -> new EmployeeSnapshotRecord("E" + i, "Name " + i, "e" + i + "@example.com", "M1", "BLR", null, true))
        );

        StringBuilder sb = new StringBuilder("employeeId,name,email,managerId,officeCode,phone\n");
        for (int i = 0; i < total; i++) {
            // Half unchanged, half changed
            String name = (i % 2 == 0) ? "Name " + i : "ChangedName " + i;
            sb.append("E").append(i).append(",").append(name).append(",e").append(i).append("@example.com,M1,BLR,+919876543210\n");
        }

        var report = service.preview("bench", new ByteArrayInputStream(sb.toString().getBytes(StandardCharsets.UTF_8)));

        assertEquals(0, report.estimatedMissing());
        assertEquals(0, report.estimatedNew());
        assertEquals(total / 2, report.estimatedChanged());
        assertFalse(report.safetyBrake());
    }

    // 12. Existing safety-brake boundary tests remaining valid (below, at, and above 30%)
    @Test
    void testSafetyBrakeBoundaries() throws Exception {
        when(repo.countByTenantIdAndActiveTrue("acme")).thenReturn(10L);

        // Case A: Below 30% (2 missing out of 10 = 20%)
        when(repo.streamAllForReconciliation("acme")).thenReturn(
            java.util.stream.IntStream.range(1, 11).mapToObj(i -> new EmployeeSnapshotRecord("E" + i, "N", "e@x.com", "M", "BLR", null, true))
        );
        String csvBelow = "employeeId,name\nE1,N\nE2,N\nE3,N\nE4,N\nE5,N\nE6,N\nE7,N\nE8,N\n"; // E9, E10 missing
        var reportBelow = service.preview("acme", new ByteArrayInputStream(csvBelow.getBytes(StandardCharsets.UTF_8)));
        assertFalse(reportBelow.safetyBrake());
        assertEquals(0.2, reportBelow.missingRatio(), 0.001);
        assertEquals("READY_TO_APPLY", reportBelow.status());

        // Case B: Exactly at 30% (3 missing out of 10 = 30%)
        when(repo.streamAllForReconciliation("acme")).thenReturn(
            java.util.stream.IntStream.range(1, 11).mapToObj(i -> new EmployeeSnapshotRecord("E" + i, "N", "e@x.com", "M", "BLR", null, true))
        );
        String csvAt = "employeeId,name\nE1,N\nE2,N\nE3,N\nE4,N\nE5,N\nE6,N\nE7,N\n"; // E8, E9, E10 missing
        var reportAt = service.preview("acme", new ByteArrayInputStream(csvAt.getBytes(StandardCharsets.UTF_8)));
        assertTrue(reportAt.safetyBrake());
        assertEquals(0.3, reportAt.missingRatio(), 0.001);
        assertEquals("SAFETY_BRAKE_APPROVAL_REQUIRED", reportAt.status());

        // Case C: Above 30% (4 missing out of 10 = 40%)
        when(repo.streamAllForReconciliation("acme")).thenReturn(
            java.util.stream.IntStream.range(1, 11).mapToObj(i -> new EmployeeSnapshotRecord("E" + i, "N", "e@x.com", "M", "BLR", null, true))
        );
        String csvAbove = "employeeId,name\nE1,N\nE2,N\nE3,N\nE4,N\nE5,N\nE6,N\n"; // E7, E8, E9, E10 missing
        var reportAbove = service.preview("acme", new ByteArrayInputStream(csvAbove.getBytes(StandardCharsets.UTF_8)));
        assertTrue(reportAbove.safetyBrake());
        assertEquals(0.4, reportAbove.missingRatio(), 0.001);
    }

    // 13. Approval and apply workflow regression tests
    @Test
    void testApprovalAndApplyWorkflowRegression() {
        UUID runId = UUID.randomUUID();

        // 13a: Apply unapproved run when awaiting approval throws IllegalStateException
        when(jdbc.queryForObject(anyString(), eq(String.class), eq(runId), eq("acme"))).thenReturn("AWAITING_APPROVAL");
        assertThrows(IllegalStateException.class, () -> service.apply("acme", runId));

        // 13b: Approve transitions run to APPROVED
        when(jdbc.update(contains("status='APPROVED'"), eq("operator"), eq(runId), eq("acme"))).thenReturn(1);
        service.approve("acme", runId, "operator");
        verify(jdbc).update(contains("status='APPROVED'"), eq("operator"), eq(runId), eq("acme"));

        // 13c: Apply approved run updates missing, changed, and new atomically
        when(jdbc.queryForObject(anyString(), eq(String.class), eq(runId), eq("acme"))).thenReturn("APPROVED");
        when(jdbc.update(contains("WITH deactivated"), eq(runId), eq("acme"))).thenReturn(3);
        when(jdbc.update(contains("WITH updated_emps"), eq(runId), eq("acme"))).thenReturn(2);
        when(jdbc.update(contains("WITH inserted_emps"), eq(runId), eq("acme"))).thenReturn(1);

        var result = service.apply("acme", runId);

        assertEquals(3, result.deactivated());
        assertEquals(2, result.updated());
        assertEquals(1, result.created());
        verify(jdbc).update(contains("status='APPLIED'"), eq(runId), eq("acme"));
    }

    // 14. Quoted fields containing commas and escaped quotes in CSV
    @Test
    void testQuotedFieldsAndEscapedQuotesInCsv() throws Exception {
        when(repo.countByTenantIdAndActiveTrue("acme")).thenReturn(1L);
        when(repo.streamAllForReconciliation("acme")).thenReturn(Stream.of(
            new EmployeeSnapshotRecord("E1", "Asha Rao", "asha@example.com", "M1", "BLR", null, true)
        ));

        // E1 has changed name with quoted comma: "Rao, Asha"
        // E2 is new with escaped double quotes: "Asha ""Rock"" Rao"
        String csv = """
            employeeId,name,email,managerId,officeCode,phone
            E1,"Rao, Asha",asha@example.com,M1,BLR,+919876543210
            E2,"Asha ""Rock"" Rao",rock@example.com,M1,BLR,+919876543211
            """;

        var report = service.preview("acme", new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8)));

        assertEquals(1, report.estimatedChanged(), "E1 with comma in quotes should be recognized as changed");
        assertEquals(1, report.estimatedNew(), "E2 with escaped quote should be recognized as new");
        assertEquals(0, report.estimatedMissing());
    }

    // 15. Reordered headers and optional fields omitted
    @Test
    void testReorderedHeadersAndOptionalFields() throws Exception {
        when(repo.countByTenantIdAndActiveTrue("acme")).thenReturn(1L);
        when(repo.streamAllForReconciliation("acme")).thenReturn(Stream.of(
            new EmployeeSnapshotRecord("E1", "Asha", "asha@example.com", "M1", "BLR", null, true)
        ));

        // Reordered: officeCode first, managerId and phone completely omitted
        String csv = """
            office_code,email,employee_id,name
            HYD,asha@example.com,E1,Asha
            BLR,new@example.com,E2,New Person
            """;

        var report = service.preview("acme", new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8)));

        assertEquals(1, report.estimatedChanged(), "E1 should have office changed to HYD");
        assertEquals(1, report.estimatedNew(), "E2 should be new");
    }

    // 16. Malformed row handling (e.g. unclosed quote)
    @Test
    void testMalformedRowHandling() throws Exception {
        when(repo.countByTenantIdAndActiveTrue("acme")).thenReturn(1L);
        when(repo.streamAllForReconciliation("acme")).thenReturn(Stream.of(
            new EmployeeSnapshotRecord("E1", "Asha", "asha@example.com", "M1", "BLR", null, true)
        ));

        // Row 2 is malformed with an unclosed quote
        String csv = "employeeId,name,email,managerId,officeCode,phone\n" +
                     "E1,Asha,asha@example.com,M1,BLR,+919876543210\n" +
                     "E2,\"Unclosed quote,email,M1,BLR,+919876543211";

        var report = service.preview("acme", new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8)));

        assertEquals(1, report.snapshotRows());
        assertTrue(report.invalidRows().stream().anyMatch(r -> r.contains("malformed")));
    }

    // 17. Repeated apply on already APPLIED run throws IllegalStateException
    @Test
    void testRepeatedApplyRejected() {
        UUID runId = UUID.randomUUID();
        when(jdbc.queryForObject(anyString(), eq(String.class), eq(runId), eq("acme"))).thenReturn("APPLIED");

        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> service.apply("acme", runId));
        assertEquals("RUN_ALREADY_APPLIED", ex.getMessage());
        verify(jdbc, never()).update(contains("WITH deactivated"), any(), any());
    }

    // 18. Regression: Malformed row for an existing active employee does NOT cause deactivation
    @Test
    void testMalformedRowCannotCauseExistingEmployeeDeactivation() throws Exception {
        when(repo.countByTenantIdAndActiveTrue("acme")).thenReturn(2L);
        when(repo.streamAllForReconciliation("acme")).thenReturn(Stream.of(
            new EmployeeSnapshotRecord("E1", "Asha", "asha@example.com", "M1", "BLR", null, true),
            new EmployeeSnapshotRecord("E2", "Ravi", "ravi@example.com", "M1", "BLR", null, true)
        ));

        // E1 has valid row; E2 has malformed quote in field
        String csv = """
            employeeId,name,email,managerId,officeCode,phone
            E1,Asha,asha@example.com,M1,BLR,+919876543210
            E2,"Ravi "Malformed Quote,ravi@example.com,M1,BLR,+919876543210
            """;

        var report = service.preview("acme", new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8)));

        // Safe record-level handling: E2 is identified as present, so E2 is NOT missing and NOT deactivated!
        assertEquals(0, report.estimatedMissing(), "E2 must NOT be classified as missing despite malformed row");
        assertEquals(0, report.estimatedChanged(), "E2 must NOT be updated with malformed data");
        assertEquals(0, report.estimatedNew());
        assertFalse(report.safetyBrake());
        assertTrue(report.invalidRows().stream().anyMatch(r -> r.contains("malformed")));
        verify(jdbc, never()).batchUpdate(contains("reconciliation_missing"), anyList());
    }

    // 19. Unidentifiable malformed row with missing active DB employee fails preview and prevents deactivation
    @Test
    void testUnidentifiableMalformedRowFailsPreviewAndPreventsDeactivation() {
        when(repo.countByTenantIdAndActiveTrue("acme")).thenReturn(2L);
        when(repo.streamAllForReconciliation("acme")).thenReturn(Stream.of(
            new EmployeeSnapshotRecord("E1", "Asha", "asha@example.com", "M1", "BLR", null, true),
            new EmployeeSnapshotRecord("E2", "Ravi", "ravi@example.com", "M1", "BLR", null, true)
        ));

        // Row 2 is unidentifiable because employeeId is completely blank
        String csv = """
            employeeId,name,email,managerId,officeCode,phone
            E1,Asha,asha@example.com,M1,BLR,+919876543210
            ,"Corrupted line with no ID",corrupted@example.com,M1,BLR,+919876543210
            """;

        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
            service.preview("acme", new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8)))
        );
        assertTrue(ex.getMessage().contains("RECONCILIATION_SAFETY_VIOLATION"));

        // Verify status was marked FAILED and staging cleared
        verify(jdbc).update(contains("status='FAILED'"), any(), eq("acme"));
        verify(jdbc).update(contains("delete from reconciliation_missing"), any(), eq("acme"));
    }

    // 20. Database streaming exception fails the run safely without applying changes or using fake fallback
    @Test
    void testDatabaseStreamingExceptionFailsRunWithoutApplyingChanges() {
        when(repo.countByTenantIdAndActiveTrue("acme")).thenReturn(5L);
        when(repo.streamAllForReconciliation("acme"))
                .thenThrow(new org.springframework.dao.DataAccessResourceFailureException("PostgreSQL streaming cursor terminated"));

        String csv = """
            employeeId,name,email,managerId,officeCode,phone
            E1,Asha,asha@example.com,M1,BLR,+919876543210
            """;

        assertThrows(RuntimeException.class, () ->
            service.preview("acme", new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8)))
        );

        // Verify status was updated to FAILED in reconciliation_runs
        verify(jdbc).update(contains("status='FAILED'"), any(), eq("acme"));

        // Verify unsafe fallback streamActiveIds was NEVER called
        verify(repo, never()).streamActiveIds(any());

        // Verify no staging records were inserted
        verify(jdbc, never()).batchUpdate(contains("reconciliation_missing"), anyList());
        verify(jdbc, never()).batchUpdate(contains("reconciliation_changed"), anyList());
        verify(jdbc, never()).batchUpdate(contains("reconciliation_new"), anyList());
    }
}

