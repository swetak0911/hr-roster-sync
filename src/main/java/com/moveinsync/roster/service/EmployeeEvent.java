package com.moveinsync.roster.service;
import jakarta.validation.constraints.*;
public record EmployeeEvent(
 @NotBlank String tenantId,
 @NotBlank String employeeId,
 @NotNull EventType type,
 @PositiveOrZero long version,
 @Size(max=160) String name,
 @Email @Size(max=254) String email,
 @Size(max=80) String managerId,
 @Size(max=40) String officeCode,
 @Pattern(regexp="^$|^\\+?[1-9][0-9]{7,14}$") @Size(max=20) String phone) {
 public enum EventType { CREATE, UPDATE, DEACTIVATE }
}
