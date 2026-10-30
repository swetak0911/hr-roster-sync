package com.moveinsync.roster.outbox;

import com.moveinsync.roster.domain.OutboxEvent;
import com.moveinsync.roster.repository.OutboxRepository;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class OutboxRecoveryTest {
    @Test void pendingEventIsPublishedAfterPublisherRestart() {
        OutboxRepository repository = mock(OutboxRepository.class);
        @SuppressWarnings("unchecked") KafkaTemplate<String,String> kafka = mock(KafkaTemplate.class);
        UUID id = UUID.randomUUID();
        OutboxEvent pending = new OutboxEvent(id, "acme", "E-1", "EMPLOYEE_CHANGED", "{\"eventId\":\"" + id + "\"}");
        // Simulate process death after the employee/outbox transaction committed but before publish.
        assertNull(pending.getPublishedAt());
        when(repository.findById(id)).thenReturn(Optional.of(pending));
        when(kafka.send(eq("employee-changes"), anyString(), anyString())).thenReturn(CompletableFuture.completedFuture(null));
        OutboxPublisher restartedPublisher = new OutboxPublisher(repository, kafka, new OutboxMetrics(repository, new io.micrometer.core.instrument.simple.SimpleMeterRegistry()));
        restartedPublisher.publishOne(id);
        assertNotNull(pending.getPublishedAt());
        verify(kafka, times(1)).send(eq("employee-changes"), eq("acme:E-1"), anyString());
    }

    @Test
    void failedKafkaPublicationLeavesEventRetryableAndIncrementsAttempts() {
        OutboxRepository repository = mock(OutboxRepository.class);
        @SuppressWarnings("unchecked") KafkaTemplate<String,String> kafka = mock(KafkaTemplate.class);
        UUID id = UUID.randomUUID();
        OutboxEvent event = new OutboxEvent(id, "acme", "E-1", "EMPLOYEE_CHANGED", "{\"eventId\":\"" + id + "\"}");

        when(repository.findById(id)).thenReturn(Optional.of(event));
        // Kafka throws failure
        CompletableFuture<org.springframework.kafka.support.SendResult<String,String>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Kafka broker down"));
        when(kafka.send(eq("employee-changes"), anyString(), anyString())).thenReturn(failedFuture);

        OutboxPublisher publisher = new OutboxPublisher(repository, kafka,
                new OutboxMetrics(repository, new io.micrometer.core.instrument.simple.SimpleMeterRegistry()));

        publisher.publishOne(id);

        // Verification: event is NOT marked published, attempts incremented
        assertNull(event.getPublishedAt(), "Failed event must not be marked published");
        assertEquals(1, event.getAttempts(), "Attempts counter must be incremented on failure");

        // Simulate subsequent retry when Kafka recovers
        when(kafka.send(eq("employee-changes"), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));

        publisher.publishOne(id);

        assertNotNull(event.getPublishedAt(), "Event must be marked published after successful retry");
        assertEquals(1, event.getAttempts());
    }
}

