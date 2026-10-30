package com.moveinsync.roster.domain;

import jakarta.persistence.*;
import java.time.Instant;

@Entity @Table(name="employees", uniqueConstraints=@UniqueConstraint(name="uk_employee_tenant", columnNames={"tenant_id","employee_id"}))
public class Employee {
    @Id @GeneratedValue(strategy=GenerationType.IDENTITY) private Long id;
    @Column(name="tenant_id", nullable=false, length=80) private String tenantId;
    @Column(name="employee_id", nullable=false, length=80) private String employeeId;
    @Column(nullable=false, length=160) private String name;
    @Column(nullable=false, length=254) private String email;
    @Column(name="manager_id", length=80) private String managerId;
    @Column(name="office_code", nullable=false, length=40) private String officeCode;
    @Column(name="phone", length=20) private String phone;
    @Column(name="pickup_point", length=200) private String pickupPoint;
    @Column(name="special_assistance", nullable=false) private boolean specialAssistance;
    @Column(name="source_version", nullable=false) private long sourceVersion;
    @Column(nullable=false) private boolean active=true;
    @Column(name="updated_at", nullable=false) private Instant updatedAt=Instant.now();
    protected Employee() {}
    public Employee(String tenantId,String employeeId,String name,String email,String managerId,String officeCode,String phone,long version) { this.tenantId=tenantId;this.employeeId=employeeId;this.name=name;this.email=email;this.managerId=managerId;this.officeCode=officeCode;this.phone=phone;this.sourceVersion=version; }
    public void applyHr(String name,String email,String managerId,String officeCode,String phone,long version,boolean active) { this.name=name;this.email=email;this.managerId=managerId;this.officeCode=officeCode;this.phone=phone;this.sourceVersion=version;this.active=active;this.updatedAt=Instant.now(); }
    public void deactivate(long version) { this.active=false; this.sourceVersion=version; this.updatedAt=Instant.now(); }
    public void updateTransport(String pickupPoint,boolean specialAssistance) { this.pickupPoint=pickupPoint;this.specialAssistance=specialAssistance;this.updatedAt=Instant.now(); }
    public Long getId(){return id;} public String getTenantId(){return tenantId;} public String getEmployeeId(){return employeeId;} public String getName(){return name;} public String getEmail(){return email;} public String getManagerId(){return managerId;} public String getOfficeCode(){return officeCode;} public String getPhone(){return phone;} public String getPickupPoint(){return pickupPoint;} public boolean isSpecialAssistance(){return specialAssistance;} public long getSourceVersion(){return sourceVersion;} public boolean isActive(){return active;} public Instant getUpdatedAt(){return updatedAt;}
}
