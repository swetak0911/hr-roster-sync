package com.moveinsync.roster.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.roster.repository.EmployeeRepository;
import com.moveinsync.roster.repository.OutboxRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;
import com.moveinsync.roster.RosterSyncApplication;

@DataJpaTest
@org.springframework.test.context.ContextConfiguration(
    classes = RosterSyncApplication.class
)
@Testcontainers(disabledWithoutDocker = true)
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({EmployeeSyncService.class, RosterMetrics.class, EmployeeSyncConcurrencyTest.TestBeans.class})
class EmployeeSyncConcurrencyTest {
    @Container static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");
    @DynamicPropertySource static void database(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }
    @Configuration static class TestBeans { @Bean ObjectMapper objectMapper() { return new ObjectMapper(); }
        @Bean SimpleMeterRegistry simpleMeterRegistry() { return new SimpleMeterRegistry(); } }

    @Autowired EmployeeSyncService service;
    @Autowired EmployeeRepository employees;
    @Autowired OutboxRepository outbox;

    @Test void concurrentFirstEventsForSameEmployeeCannotCreateTwoRowsOrRegressVersion() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<?> v1 = pool.submit(() -> service.apply(event(1)));
            Future<?> v2 = pool.submit(() -> service.apply(event(2)));
            v1.get(15, TimeUnit.SECONDS); v2.get(15, TimeUnit.SECONDS);
            var stored = employees.findByTenantIdAndEmployeeId("acme", "E-77").orElseThrow();
            assertEquals(2, stored.getSourceVersion());
            assertEquals(1, employees.count());
            assertTrue(outbox.count() >= 1 && outbox.count() <= 2);
        } finally { pool.shutdownNow(); }
    }
    private EmployeeEvent event(long version) {
        return new EmployeeEvent("acme", "E-77", EmployeeEvent.EventType.CREATE, version,
                "Test Employee", "test@example.com", "M-1", "BLR", "+919876543210");
    }
}
