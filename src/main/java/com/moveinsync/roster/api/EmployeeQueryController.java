package com.moveinsync.roster.api;

import com.moveinsync.roster.security.TenantPrincipal;
import com.moveinsync.roster.service.EmployeeQueryService;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.*;

@RestController @RequestMapping("/api/v1/employees")
public class EmployeeQueryController {
    private final EmployeeQueryService queries;
    public EmployeeQueryController(EmployeeQueryService queries) { this.queries = queries; }
    @GetMapping("/{employeeId}") @PreAuthorize("isAuthenticated()")
    public EmployeeQueryService.EmployeeView get(@PathVariable String employeeId,
            @AuthenticationPrincipal TenantPrincipal principal, @RequestHeader("X-Tenant-Id") String tenant) {
        if (!tenant.equals(principal.tenantId())) throw new org.springframework.web.server.ResponseStatusException(org.springframework.http.HttpStatus.FORBIDDEN);
        if (principal.role().equals("EMPLOYEE") && !employeeId.equals(principal.employeeId())) throw new org.springframework.web.server.ResponseStatusException(org.springframework.http.HttpStatus.FORBIDDEN);
        return queries.find(principal.tenantId(), employeeId);
    }
}
