package com.moveinsync.roster.booking;
import org.springframework.data.jpa.repository.*; import org.springframework.data.repository.query.Param; import java.time.Instant;
public interface CabBookingRepository extends JpaRepository<CabBooking, java.util.UUID> {
 @Modifying @Query("update CabBooking b set b.status='CANCELLED', b.seatReserved=false where b.tenantId=:tenant and b.employeeId=:employee and b.travelAt>:now and b.status='CONFIRMED'")
 int cancelFutureAndReleaseSeats(@Param("tenant") String tenant,@Param("employee") String employee,@Param("now") Instant now);
}
