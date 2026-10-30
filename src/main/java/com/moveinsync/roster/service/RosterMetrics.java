package com.moveinsync.roster.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class RosterMetrics {
    private final Counter applied;
    private final Counter ignored;
    private final Counter rejected;
    public RosterMetrics(MeterRegistry registry) {
        applied = Counter.builder("roster.events.applied").description("Accepted HR events").register(registry);
        ignored = Counter.builder("roster.events.ignored").description("Duplicate or stale HR events").register(registry);
        rejected = Counter.builder("roster.events.rejected").description("Rejected HR events").register(registry);
    }
    public void applied() { applied.increment(); }
    public void ignored() { ignored.increment(); }
    public void rejected() { rejected.increment(); }
}
