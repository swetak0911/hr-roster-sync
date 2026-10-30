package com.moveinsync.roster.reconciliation;

import com.moveinsync.roster.repository.EmployeeRepository;
import com.moveinsync.roster.repository.EmployeeSnapshotRecord;
import org.junit.jupiter.api.Test;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;
import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

class ReconciliationServiceTest {
    @Test
    void safetyBrakeTripsWhenMissingRatioReachesThreshold() throws Exception {
        EmployeeRepository repo = mock(EmployeeRepository.class);
        when(repo.countByTenantIdAndActiveTrue("acme")).thenReturn(10L);
        when(repo.streamActiveIds("acme")).thenReturn(Stream.of("E1","E2","E3","E4","E5","E6","E7","E8","E9","E10"));
        when(repo.streamAllForReconciliation("acme")).thenReturn(
            Stream.of("E1","E2","E3","E4","E5","E6","E7","E8","E9","E10")
                .map(id -> new EmployeeSnapshotRecord(id, "Name", "email@example.com", "M1", "BLR", null, true))
        );
        var service = new ReconciliationService(repo, 0.30, mock(org.springframework.jdbc.core.JdbcTemplate.class));
        var csv = "employeeId,name\nE1,A\nE2,B\nE3,C\nE4,D\nE5,E\nE6,F\nE7,G\n";
        var report = service.preview("acme", new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8)));
        assertTrue(report.safetyBrake());
        assertEquals(0.3, report.missingRatio(), 0.0001);
    }

    @Test
    void healthySnapshotDoesNotTripBrake() throws Exception {
        EmployeeRepository repo = mock(EmployeeRepository.class);
        when(repo.countByTenantIdAndActiveTrue("acme")).thenReturn(2L);
        when(repo.streamActiveIds("acme")).thenReturn(Stream.of("E1","E2"));
        when(repo.streamAllForReconciliation("acme")).thenReturn(
            Stream.of("E1","E2")
                .map(id -> new EmployeeSnapshotRecord(id, "Name", "email@example.com", "M1", "BLR", null, true))
        );
        var service = new ReconciliationService(repo, 0.30, mock(org.springframework.jdbc.core.JdbcTemplate.class));
        var csv = "employeeId,name\nE1,A\nE2,B\n";
        var report = service.preview("acme", new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8)));
        assertFalse(report.safetyBrake());
    }
}
