package com.moveinsync.roster.service;

import com.moveinsync.roster.repository.EmployeeRepository;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

@Service
public class EmployeeQueryService {
    private final EmployeeRepository employees;
    public EmployeeQueryService(EmployeeRepository employees) { this.employees = employees; }
    @Cacheable(cacheNames="employee-profile", key="#tenantId + ':' + #employeeId")
    public EmployeeView find(String tenantId, String employeeId) {
        var employee = employees.findByTenantIdAndEmployeeId(tenantId, employeeId)
            .orElseThrow(() -> new IllegalArgumentException("EMPLOYEE_NOT_FOUND"));
        return new EmployeeView(employee.getEmployeeId(), employee.getName(), employee.getEmail(),
            employee.getManagerId(), employee.getOfficeCode(), employee.getPhone(), employee.getPickupPoint(),
            employee.isSpecialAssistance(), employee.isActive(), employee.getSourceVersion());
    }
    public record EmployeeView(String employeeId, String name, String email, String managerId, String officeCode,
            String phone, String pickupPoint, boolean specialAssistance, boolean active, long sourceVersion) { }
}
