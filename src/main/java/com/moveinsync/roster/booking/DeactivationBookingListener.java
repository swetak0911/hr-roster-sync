package com.moveinsync.roster.booking;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import java.time.Instant;

@Component
public class DeactivationBookingListener {
    private final CabBookingRepository bookings;
    private final ObjectMapper mapper;
    public DeactivationBookingListener(CabBookingRepository bookings, ObjectMapper mapper) { this.bookings=bookings; this.mapper=mapper; }

    /** Idempotent: after the first update there are no CONFIRMED future rows left to cancel. */
    @KafkaListener(topics="employee-changes", groupId="booking-deactivation")
    @Transactional
    public void onEmployeeChange(String payload) throws Exception {
        JsonNode event = mapper.readTree(payload);
        if (event.path("active").asBoolean(true)) return;
        String tenant = event.path("tenantId").asText();
        String employee = event.path("employeeId").asText();
        if (tenant.isBlank() || employee.isBlank()) return;
        bookings.cancelFutureAndReleaseSeats(tenant, employee, Instant.now());
    }
}
