package com.moveinsync.roster.outbox;

import com.moveinsync.roster.repository.OutboxRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;
import java.time.Duration;
import java.time.Instant;

@Component
public class OutboxMetrics {
    private final Counter attempts;
    private final Counter failures;
    private final Counter published;
    public OutboxMetrics(OutboxRepository repository, MeterRegistry registry) {
        Gauge.builder("roster.outbox.pending", repository, OutboxRepository::countByPublishedAtIsNull)
            .description("Number of unpublished outbox events").register(registry);
        Gauge.builder("roster.outbox.oldest_pending_age_seconds", repository, repo -> repo.findFirstByPublishedAtIsNullOrderByCreatedAtAsc()
                .map(event -> (double) Duration.between(event.getCreatedAt(), Instant.now()).toSeconds()).orElse(0.0))
            .description("Age of oldest unpublished outbox event").register(registry);
        attempts = Counter.builder("roster.outbox.publish_attempts").register(registry);
        failures = Counter.builder("roster.outbox.publish_failures").register(registry);
        published = Counter.builder("roster.outbox.published").register(registry);
    }
    public void attempted() { attempts.increment(); }
    public void failed() { failures.increment(); }
    public void published() { published.increment(); }
}
