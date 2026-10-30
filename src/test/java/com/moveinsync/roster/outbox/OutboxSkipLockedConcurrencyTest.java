package com.moveinsync.roster.outbox;

import com.moveinsync.roster.RosterSyncApplication;
import com.moveinsync.roster.domain.OutboxEvent;
import com.moveinsync.roster.repository.OutboxRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

@DataJpaTest
@org.springframework.test.context.ContextConfiguration(classes = RosterSyncApplication.class)
@Testcontainers(disabledWithoutDocker = true)
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class OutboxSkipLockedConcurrencyTest {

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired
    OutboxRepository outboxRepository;

    @Autowired
    PlatformTransactionManager txManager;

    @Test
    void multiInstancePollingWithSkipLockedNeverSelectsSameRows() throws Exception {
        // 1. Insert 2 pending outbox events
        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();
        outboxRepository.saveAndFlush(new OutboxEvent(id1, "acme", "E-1", "EMPLOYEE_CHANGED", "{\"id\":1}"));
        outboxRepository.saveAndFlush(new OutboxEvent(id2, "acme", "E-2", "EMPLOYEE_CHANGED", "{\"id\":2}"));

        TransactionTemplate txTemplate = new TransactionTemplate(txManager,
                new DefaultTransactionDefinition(TransactionDefinition.PROPAGATION_REQUIRES_NEW));

        CountDownLatch thread1LockedLatch = new CountDownLatch(1);
        CountDownLatch thread2DoneLatch = new CountDownLatch(1);

        ExecutorService pool = Executors.newFixedThreadPool(2);

        try {
            // Worker 1: Locks top 100 rows within an open transaction
            Future<?> worker1 = pool.submit(() -> {
                txTemplate.execute(status -> {
                    List<OutboxEvent> lockedBatch = outboxRepository.findTop100ByPublishedAtIsNullOrderByCreatedAtAsc();
                    assertEquals(2, lockedBatch.size(), "Worker 1 must acquire both pending rows");
                    thread1LockedLatch.countDown(); // Signal Worker 2 to query

                    try {
                        thread2DoneLatch.await(10, TimeUnit.SECONDS); // Hold lock until Worker 2 has queried
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }

                    // Mark as published and complete transaction
                    lockedBatch.forEach(OutboxEvent::markPublished);
                    outboxRepository.saveAllAndFlush(lockedBatch);
                    return null;
                });
            });

            // Worker 2: Concurrently queries while Worker 1 holds the lock
            Future<?> worker2 = pool.submit(() -> {
                try {
                    thread1LockedLatch.await(10, TimeUnit.SECONDS);
                    txTemplate.execute(status -> {
                        // Because Worker 1 locked the 2 rows with FOR UPDATE SKIP LOCKED, Worker 2 must skip them and return empty without blocking
                        List<OutboxEvent> secondBatch = outboxRepository.findTop100ByPublishedAtIsNullOrderByCreatedAtAsc();
                        assertTrue(secondBatch.isEmpty(), "Worker 2 must skip locked rows and return empty without blocking");
                        return null;
                    });
                } catch (Exception e) {
                    throw new CompletionException(e);
                } finally {
                    thread2DoneLatch.countDown();
                }
            });

            worker1.get(15, TimeUnit.SECONDS);
            worker2.get(15, TimeUnit.SECONDS);

            // 3. Confirm all events are now published
            assertEquals(0L, outboxRepository.countByPublishedAtIsNull());

        } finally {
            pool.shutdownNow();
        }
    }
}

