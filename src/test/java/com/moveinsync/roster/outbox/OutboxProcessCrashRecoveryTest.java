package com.moveinsync.roster.outbox;

import com.moveinsync.roster.RosterSyncApplication;
import com.moveinsync.roster.repository.OutboxRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@DataJpaTest
@org.springframework.test.context.ContextConfiguration(classes = RosterSyncApplication.class)
@Testcontainers(disabledWithoutDocker = true)
@EmbeddedKafka(partitions = 1, topics = "employee-changes")
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class OutboxProcessCrashRecoveryTest {

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    OutboxRepository outboxRepository;

    @Autowired
    PlatformTransactionManager transactionManager;

    @Autowired
    EmbeddedKafkaBroker embeddedKafka;

    @Test
    void committedOutboxEventSurvivesRealProcessCrashAndPublishesAfterRestart() throws Exception {
        UUID eventId = UUID.randomUUID();
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        String java = Path.of(System.getProperty("java.home"), "bin",
                System.getProperty("os.name").toLowerCase().contains("win") ? "java.exe" : "java").toString();

        Process child = new ProcessBuilder(
                java, "-cp", classpath,
                OutboxCrashWriterProcess.class.getName(),
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword(), eventId.toString())
                .redirectErrorStream(true)
                .start();

        String marker;
        try (BufferedReader output = new BufferedReader(
                new InputStreamReader(child.getInputStream(), StandardCharsets.UTF_8))) {
            marker = output.readLine();
        } catch (Exception failure) {
            child.destroyForcibly();
            throw failure;
        }

        assertEquals("COMMITTED:" + eventId, marker,
                "Child process must confirm the employee and outbox transaction committed");
        assertTrue(child.isAlive(), "Child process must still be alive before simulated crash");

        // This is an actual OS process termination after the DB commit and before publishing.
        child.destroyForcibly();
        assertTrue(child.waitFor(10, TimeUnit.SECONDS), "Child process should terminate after forced crash");

        assertEquals(1, jdbc.queryForObject(
                "SELECT count(*) FROM employees WHERE tenant_id='crash-tenant' AND employee_id='CRASH-EMP-1'",
                Integer.class));
        assertEquals(1, jdbc.queryForObject(
                "SELECT count(*) FROM outbox_events WHERE id=? AND published_at IS NULL",
                Integer.class, eventId));

        // A fresh publisher instance represents the restarted application process.
        Map<String, Object> producerProperties = KafkaTestUtils.producerProps(embeddedKafka);
        var producerFactory = new DefaultKafkaProducerFactory<String, String>(producerProperties);
        KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(producerFactory);
        OutboxMetrics metrics = new OutboxMetrics(outboxRepository, new SimpleMeterRegistry());
        OutboxPublisher restartedPublisher = new OutboxPublisher(outboxRepository, kafkaTemplate, metrics);

        Map<String, Object> consumerProperties = KafkaTestUtils.consumerProps(
                "crash-recovery-" + eventId, "false", embeddedKafka);
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (Consumer<String, String> consumer = new KafkaConsumer<>(
                consumerProperties, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(java.util.List.of("employee-changes"));

            TransactionTemplate transaction = new TransactionTemplate(transactionManager);
            transaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
            transaction.executeWithoutResult(status -> restartedPublisher.publishOne(eventId));

            ConsumerRecord<String, String> delivered = null;
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (delivered == null && System.nanoTime() < deadline) {
                var records = consumer.poll(Duration.ofMillis(250));
                for (var record : records) {
                    if (record.value().contains(eventId.toString())) {
                        delivered = record;
                        break;
                    }
                }
            }

            assertNotNull(delivered, "Restarted publisher must deliver the committed event to Kafka");
            assertEquals("crash-tenant:CRASH-EMP-1", delivered.key());
        } finally {
            kafkaTemplate.destroy();
            producerFactory.destroy();
        }

        assertEquals(1, jdbc.queryForObject(
                "SELECT count(*) FROM outbox_events WHERE id=? AND published_at IS NOT NULL",
                Integer.class, eventId));
        assertEquals(0, outboxRepository.countByPublishedAtIsNull());
    }
}
