package com.moveinsync.roster.repository;

public record EmployeeSnapshotRecord(
    String employeeId,
    String name,
    String email,
    String managerId,
    String officeCode,
    String phone,
    boolean active
) {
    public EmployeeSnapshotRecord(String employeeId, String name, String email, String managerId, String officeCode, boolean active) {
        this(employeeId, name, email, managerId, officeCode, null, active);
    }
}

