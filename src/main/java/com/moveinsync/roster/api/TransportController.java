package com.moveinsync.roster.api;
import com.moveinsync.roster.service.TransportProfileService; import com.moveinsync.roster.security.TenantPrincipal; import org.springframework.security.core.annotation.AuthenticationPrincipal; import org.springframework.security.access.prepost.PreAuthorize; import org.springframework.web.bind.annotation.*;
@RestController @RequestMapping("/api/v1/transport-profiles") public class TransportController {private final TransportProfileService service;public TransportController(TransportProfileService s){service=s;}
 public record UpdateRequest(String pickupPoint,boolean specialAssistance){}
 @PutMapping("/{employeeId}") @PreAuthorize("hasAnyRole('ADMIN','TRANSPORT_ADMIN')") public void update(@PathVariable String employeeId,@AuthenticationPrincipal TenantPrincipal principal,@RequestHeader("X-Tenant-Id") String tenant,@RequestBody UpdateRequest request){if(!tenant.equals(principal.tenantId())) throw new org.springframework.web.server.ResponseStatusException(org.springframework.http.HttpStatus.FORBIDDEN);service.update(principal.tenantId(),employeeId,request.pickupPoint(),request.specialAssistance());}
}
