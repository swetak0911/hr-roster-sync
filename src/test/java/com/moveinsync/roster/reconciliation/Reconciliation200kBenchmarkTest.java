package com.moveinsync.roster.reconciliation;

import com.moveinsync.roster.repository.EmployeeRepository;
import com.moveinsync.roster.repository.EmployeeSnapshotRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.stream.LongStream;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class Reconciliation200kBenchmarkTest {
    @TempDir Path temp;
    @Test void streamsTwoHundredThousandRowsWithinSixtySeconds() throws Exception {
        final int total = 200_000;
        Path csv = temp.resolve("employees-200k.csv");
        try (BufferedWriter writer = Files.newBufferedWriter(csv, StandardCharsets.UTF_8)) {
            writer.write("employeeId,name,email,managerId,officeCode\n");
            for (int i = 0; i < total; i++) writer.write("E" + i + ",Name,hidden@example.com,M1,BLR\n");
        }
        EmployeeRepository repo = mock(EmployeeRepository.class);
        when(repo.countByTenantIdAndActiveTrue("bench")).thenReturn((long) total);
        when(repo.streamActiveIds("bench")).thenAnswer(invocation -> LongStream.range(0, total).mapToObj(i -> "E" + i));
        when(repo.streamAllForReconciliation("bench")).thenAnswer(invocation ->
            LongStream.range(0, total).mapToObj(i -> new EmployeeSnapshotRecord("E" + i, "Name", "hidden@example.com", "M1", "BLR", null, true))
        );
        ReconciliationService service = new ReconciliationService(repo, 0.30, mock(org.springframework.jdbc.core.JdbcTemplate.class));
        long started = System.nanoTime();
        ReconciliationService.ReconciliationReport report;
        try (var input = Files.newInputStream(csv)) { report = service.preview("bench", input); }
        long elapsedMs = (System.nanoTime() - started) / 1_000_000;
        System.out.printf("200k reconciliation: %d ms; missing=%d; buckets=%d%n", elapsedMs, report.estimatedMissing(), 256);
        assertEquals(0, report.estimatedMissing());
        assertEquals(0, report.estimatedNew());
        assertEquals(0, report.estimatedChanged());
        assertFalse(report.safetyBrake());
        assertTrue(elapsedMs < 60_000, "200K reconciliation exceeded 60 seconds: " + elapsedMs + "ms");
    }
}
