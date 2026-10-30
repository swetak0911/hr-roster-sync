package com.moveinsync.roster.api;

import com.moveinsync.roster.reconciliation.ReconciliationService;
import com.moveinsync.roster.security.TenantPrincipal;
import org.springframework.http.MediaType;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import java.util.Map;
import java.util.UUID;

@RestController @RequestMapping("/api/v1/reconciliation")
public class ReconciliationController {
    private final ReconciliationService service;
    public ReconciliationController(ReconciliationService service) { this.service = service; }

    @PostMapping(value="/preview", consumes=MediaType.MULTIPART_FORM_DATA_VALUE)
    @PreAuthorize("hasAnyRole('OPERATOR','ADMIN')")
    public ReconciliationService.ReconciliationReport preview(@AuthenticationPrincipal TenantPrincipal principal,
            @RequestHeader("X-Tenant-Id") String tenant, @RequestPart("file") MultipartFile file) throws java.io.IOException {
        assertTenant(principal, tenant);
        return service.preview(principal.tenantId(), file.getInputStream());
    }

    @PostMapping("/{runId}/approve") @PreAuthorize("hasAnyRole('OPERATOR','ADMIN')")
    public Map<String,String> approve(@PathVariable UUID runId, @AuthenticationPrincipal TenantPrincipal principal,
            @RequestHeader("X-Tenant-Id") String tenant) {
        assertTenant(principal, tenant);
        service.approve(principal.tenantId(), runId, principal.getUsername());
        return Map.of("runId", runId.toString(), "status", "APPROVED");
    }

    @PostMapping("/{runId}/apply") @PreAuthorize("hasAnyRole('OPERATOR','ADMIN')")
    public Map<String,Object> apply(@PathVariable UUID runId, @AuthenticationPrincipal TenantPrincipal principal,
            @RequestHeader("X-Tenant-Id") String tenant) {
        assertTenant(principal, tenant);
        int deactivated = service.apply(principal.tenantId(), runId);
        return Map.of("runId", runId.toString(), "status", "APPLIED", "deactivatedEmployees", deactivated);
    }

    private void assertTenant(TenantPrincipal principal, String tenant) {
        if (!tenant.equals(principal.tenantId())) throw new org.springframework.web.server.ResponseStatusException(org.springframework.http.HttpStatus.FORBIDDEN);
    }
}
