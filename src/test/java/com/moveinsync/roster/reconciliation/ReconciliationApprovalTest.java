package com.moveinsync.roster.reconciliation;

import com.moveinsync.roster.repository.EmployeeRepository;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ReconciliationApprovalTest {
    @Test void brakeRunMustBeApprovedBeforeApply() {
        EmployeeRepository employees = mock(EmployeeRepository.class);
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        ReconciliationService service = new ReconciliationService(employees, 0.30, jdbc);
        UUID runId = UUID.randomUUID();
        when(jdbc.queryForObject(anyString(), eq(String.class), eq(runId), eq("acme"))).thenReturn("AWAITING_APPROVAL");
        assertThrows(IllegalStateException.class, () -> service.apply("acme", runId));
        verify(jdbc, never()).update(contains("WITH deactivated"), any(), any());
    }

    @Test void approvedRunCanDeactivateAndWriteOutboxAtomically() {
        EmployeeRepository employees = mock(EmployeeRepository.class);
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        ReconciliationService service = new ReconciliationService(employees, 0.30, jdbc);
        UUID runId = UUID.randomUUID();
        when(jdbc.queryForObject(anyString(), eq(String.class), eq(runId), eq("acme"))).thenReturn("APPROVED");
        when(jdbc.queryForObject(contains("count(*) from reconciliation_missing"), eq(Long.class), eq(runId), eq("acme"))).thenReturn(42L);
        when(jdbc.queryForObject(contains("count(*) from reconciliation_changed"), eq(Long.class), eq(runId), eq("acme"))).thenReturn(0L);
        when(jdbc.queryForObject(contains("count(*) from reconciliation_new"), eq(Long.class), eq(runId), eq("acme"))).thenReturn(0L);
        when(jdbc.update(contains("WITH deactivated"), eq(runId), eq("acme"))).thenReturn(42);
        assertEquals(42, service.apply("acme", runId).deactivated());
        verify(jdbc).update(contains("status='APPLIED'"), eq(runId), eq("acme"));
    }
}
