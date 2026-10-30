package com.moveinsync.roster.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.roster.service.EmployeeEvent;
import com.moveinsync.roster.service.EmployeeSyncService;
import com.moveinsync.roster.service.SyncResult;
import com.moveinsync.roster.security.TenantPrincipal;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import jakarta.validation.Valid;
import jakarta.validation.Validator;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;
import java.util.*;

@RestController
@RequestMapping("/api/v1/events")
public class EmployeeController {
    private final EmployeeSyncService sync;
    private final ObjectMapper mapper;
    private final Validator validator;

    public EmployeeController(EmployeeSyncService sync, ObjectMapper mapper, Validator validator) {
        this.sync = sync; this.mapper = mapper; this.validator = validator;
    }

    @PostMapping
    @PreAuthorize("hasAnyRole('OPERATOR','ADMIN')")
    public ResponseEntity<SyncResult> ingest(@Valid @RequestBody EmployeeEvent event,
            @AuthenticationPrincipal TenantPrincipal principal, @RequestHeader("X-Tenant-Id") String tenant) {
        if (!tenant.equals(principal.tenantId()) || !tenant.equals(event.tenantId())) return ResponseEntity.status(403).build();
        SyncResult result = sync.apply(event);
        return ResponseEntity.status(result.status().equals("REJECTED") ? 422 : 200).body(result);
    }

    /** Each item is validated and processed independently; one bad item never aborts its siblings. */
    @PostMapping("/batch")
    @PreAuthorize("hasAnyRole('OPERATOR','ADMIN')")
    public List<SyncResult> batch(@RequestBody List<JsonNode> events,
            @AuthenticationPrincipal TenantPrincipal principal, @RequestHeader("X-Tenant-Id") String tenant) {
        if (!tenant.equals(principal.tenantId())) return events.stream().map(n -> new SyncResult(n.path("employeeId").asText("UNKNOWN"), "REJECTED", "TENANT_MISMATCH", -1)).toList();
        List<SyncResult> results = new ArrayList<>(events.size());
        for (JsonNode node : events) {
            String id = node == null ? "UNKNOWN" : node.path("employeeId").asText("UNKNOWN");
            try {
                if (node == null || !node.isObject()) throw new IllegalArgumentException("MALFORMED_RECORD");
                EmployeeEvent event = mapper.treeToValue(node, EmployeeEvent.class);
                var violations = validator.validate(event);
                if (!violations.isEmpty()) {
                    results.add(new SyncResult(id, "REJECTED", "VALIDATION_FAILED", -1));
                } else if (!tenant.equals(event.tenantId())) {
                    results.add(new SyncResult(id, "REJECTED", "TENANT_MISMATCH", -1));
                } else {
                    results.add(sync.apply(event));
                }
            } catch (Exception ignored) {
                // Do not return raw parser/validation messages: they may contain submitted PII.
                results.add(new SyncResult(id, "REJECTED", "MALFORMED_RECORD", -1));
            }
        }
        return results;
    }
}
