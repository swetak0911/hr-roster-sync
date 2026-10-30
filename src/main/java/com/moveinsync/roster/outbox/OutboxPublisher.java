package com.moveinsync.roster.outbox;

import com.moveinsync.roster.repository.OutboxRepository;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class OutboxPublisher {
    private final OutboxRepository repo;
    private final KafkaTemplate<String,String> kafka;
    private final OutboxMetrics metrics;
    public OutboxPublisher(OutboxRepository repo, KafkaTemplate<String,String> kafka, OutboxMetrics metrics) {
        this.repo=repo; this.kafka=kafka; this.metrics=metrics;
    }
    @Scheduled(fixedDelayString="${app.outbox.poll-ms:1000}")
    @Transactional
    public void publishPending() {
        for (var event : repo.findTop100ByPublishedAtIsNullOrderByCreatedAtAsc()) publishOne(event.getId());
    }
    @Transactional
    public void publishOne(java.util.UUID id) {
        var event=repo.findById(id).orElse(null);
        if (event==null || event.getPublishedAt()!=null) return;
        metrics.attempted();
        try {
            kafka.send("employee-changes", event.getTenantId()+":"+event.getEmployeeId(), event.getPayload()).get();
            event.markPublished();
            metrics.published();
        } catch (Exception ex) {
            event.incrementAttempts();
            metrics.failed();
        }
    }
}
