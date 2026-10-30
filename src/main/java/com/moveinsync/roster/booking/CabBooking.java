package com.moveinsync.roster.booking;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity @Table(name="cab_bookings", indexes=@Index(name="idx_future_employee_bookings", columnList="tenant_id,employee_id,travel_at,status"))
public class CabBooking {
    @Id private UUID id;
    @Column(name="tenant_id", nullable=false) private String tenantId;
    @Column(name="employee_id", nullable=false) private String employeeId;
    @Column(name="travel_at", nullable=false) private Instant travelAt;
    @Column(nullable=false, length=24) private String status;
    @Column(name="seat_reserved", nullable=false) private boolean seatReserved;
    protected CabBooking() { }
    public UUID getId(){return id;} public String getTenantId(){return tenantId;} public String getEmployeeId(){return employeeId;}
    public Instant getTravelAt(){return travelAt;} public String getStatus(){return status;} public boolean isSeatReserved(){return seatReserved;}
}
