package com.moveinsync.roster.booking;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class DeactivationBookingListenerTest {
    @Test void deactivationCancelsFutureBookingsAndDuplicateDeliveryIsSafe() throws Exception {
        CabBookingRepository repository = mock(CabBookingRepository.class);
        DeactivationBookingListener listener = new DeactivationBookingListener(repository, new ObjectMapper());
        String payload = "{\"eventId\":\"evt-1\",\"tenantId\":\"acme\",\"employeeId\":\"E-1\",\"active\":false}";
        listener.onEmployeeChange(payload);
        listener.onEmployeeChange(payload);
        verify(repository, times(2)).cancelFutureAndReleaseSeats(eq("acme"), eq("E-1"), any(Instant.class));
    }

    @Test void activeEmployeeChangeDoesNotTouchBookings() throws Exception {
        CabBookingRepository repository = mock(CabBookingRepository.class);
        DeactivationBookingListener listener = new DeactivationBookingListener(repository, new ObjectMapper());
        listener.onEmployeeChange("{\"tenantId\":\"acme\",\"employeeId\":\"E-1\",\"active\":true}");
        verifyNoInteractions(repository);
    }
}
