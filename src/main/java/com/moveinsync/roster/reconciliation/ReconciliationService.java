package com.moveinsync.roster.reconciliation;

import com.moveinsync.roster.repository.EmployeeRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.annotation.Isolation;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Stream;

@Service
public class ReconciliationService {
    private static final int BUCKETS = 256;
    private static final int DB_BATCH_SIZE = 500;
    private final EmployeeRepository employees;
    private final double missingThreshold;
    private final JdbcTemplate jdbc;

    public ReconciliationService(EmployeeRepository employees,
            @Value("${app.reconciliation.missing-threshold:0.30}") double threshold,
            JdbcTemplate jdbc) {
        if (threshold <= 0 || threshold >= 1) throw new IllegalArgumentException("threshold must be between 0 and 1");
        this.employees = employees;
        this.missingThreshold = threshold;
        this.jdbc = jdbc;
    }

    /**
     * Streams snapshot and active employee IDs into hash buckets. Only one bucket is held in memory.
     * Missing IDs are persisted so a later operator approval/apply does not need the original file.
     */
    @Transactional(isolation = Isolation.REPEATABLE_READ)
    public ReconciliationReport preview(String tenant, InputStream csv) throws IOException {
        long activeCount = employees.countByTenantIdAndActiveTrue(tenant);
        UUID runId = UUID.randomUUID();
        jdbc.update("insert into reconciliation_runs(id,tenant_id,status,active_before,created_at) values (?,?,?, ?, now())",
                runId, tenant, "PROCESSING", activeCount);
        Path work = Files.createTempDirectory("roster-reconcile-");
        List<String> invalidRows = new ArrayList<>(100);
        long invalidRowsTruncated = 0;
        long snapshotRows = 0;
        long missing = 0;
        try {
            Path[] snapshotBuckets = createBuckets(work, "snapshot-");
            Path[] activeBuckets = createBuckets(work, "active-");
            try (WriterGroup snapshotWriters = new WriterGroup(snapshotBuckets)) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(csv, StandardCharsets.UTF_8), 64 * 1024)) {
                    String line; long row = 0; boolean header = true;
                    while ((line = reader.readLine()) != null) {
                        row++;
                        if (header) { header = false; continue; }
                        if (line.isBlank()) continue;
                        String id = line.split(",", 2)[0].trim();
                        if (id.isBlank()) {
                            if (invalidRows.size() < 100) invalidRows.add("row:" + row); else invalidRowsTruncated++;
                            continue;
                        }
                        snapshotWriters.writer(bucket(id)).write(id + "\n");
                        snapshotRows++;
                    }
                }
            }
            try (WriterGroup activeWriters = new WriterGroup(activeBuckets); Stream<String> ids = employees.streamActiveIds(tenant)) {
                ids.forEach(id -> {
                    try { activeWriters.writer(bucket(id)).write(id + "\n"); }
                    catch (IOException e) { throw new UncheckedIOException(e); }
                });
            } catch (UncheckedIOException e) { throw e.getCause(); }

            List<Object[]> missingBatch = new ArrayList<>(DB_BATCH_SIZE);
            for (int i = 0; i < BUCKETS; i++) {
                Set<String> incoming = new HashSet<>();
                try (BufferedReader snapshotReader = Files.newBufferedReader(snapshotBuckets[i], StandardCharsets.UTF_8)) {
                    String id; while ((id = snapshotReader.readLine()) != null) incoming.add(id);
                }
                try (BufferedReader activeReader = Files.newBufferedReader(activeBuckets[i], StandardCharsets.UTF_8)) {
                    String id;
                    while ((id = activeReader.readLine()) != null) {
                        if (!incoming.contains(id)) {
                            missing++;
                            missingBatch.add(new Object[]{runId, tenant, id});
                            if (missingBatch.size() == DB_BATCH_SIZE) flushMissing(missingBatch);
                        }
                    }
                }
            }
            flushMissing(missingBatch);
            double ratio = activeCount == 0 ? 0.0 : (double) missing / activeCount;
            boolean brake = ratio >= missingThreshold;
            String status = brake ? "AWAITING_APPROVAL" : "READY_TO_APPLY";
            jdbc.update("update reconciliation_runs set status=?,snapshot_rows=?,estimated_missing=?,missing_ratio=?,safety_brake=? where id=? and tenant_id=?",
                    status, snapshotRows, missing, ratio, brake, runId, tenant);
            return new ReconciliationReport(runId, activeCount, snapshotRows, missing, ratio, brake,
                    brake ? "SAFETY_BRAKE_APPROVAL_REQUIRED" : "READY_TO_APPLY", invalidRows, invalidRowsTruncated);
        } catch (IOException | RuntimeException ex) {
            jdbc.update("update reconciliation_runs set status='FAILED' where id=? and tenant_id=?", runId, tenant);
            throw ex;
        } finally {
            try (var paths = Files.walk(work)) {
                paths.sorted(Comparator.reverseOrder()).forEach(path -> { try { Files.deleteIfExists(path); } catch (IOException ignored) { } });
            }
        }
    }

    @Transactional
    public void approve(String tenant, UUID runId, String approvedBy) {
        String status = getRunStatus(tenant, runId);
        if (!"AWAITING_APPROVAL".equals(status)) throw new IllegalStateException("RUN_NOT_AWAITING_APPROVAL");
        int updated = jdbc.update("update reconciliation_runs set status='APPROVED',approved_by=?,approved_at=now() where id=? and tenant_id=? and status='AWAITING_APPROVAL'",
                approvedBy, runId, tenant);
        if (updated != 1) throw new IllegalStateException("RUN_APPROVAL_CONFLICT");
    }

    /** Applies only READY_TO_APPLY or explicitly APPROVED runs. Employee changes and outbox rows are atomic. */
    @Transactional
    @CacheEvict(cacheNames="employee-profile", allEntries=true)
    public int apply(String tenant, UUID runId) {
        String status = getRunStatus(tenant, runId);
        if (!"READY_TO_APPLY".equals(status) && !"APPROVED".equals(status))
            throw new IllegalStateException("RUN_REQUIRES_APPROVAL_OR_IS_NOT_APPLICABLE");
        String sql = """
            WITH deactivated AS (
              UPDATE employees e SET active=false, updated_at=now()
              FROM reconciliation_missing m
              WHERE m.run_id=? AND m.tenant_id=? AND e.tenant_id=m.tenant_id
                AND e.employee_id=m.employee_id AND e.active=true
              RETURNING e.tenant_id,e.employee_id,e.source_version
            ), event_rows AS (
              SELECT gen_random_uuid() AS id,tenant_id,employee_id,source_version FROM deactivated
            )
            INSERT INTO outbox_events(id,tenant_id,employee_id,event_type,payload,created_at)
            SELECT id,tenant_id,employee_id,'EMPLOYEE_DEACTIVATED',
              json_build_object('eventId',id::text,'tenantId',tenant_id,'employeeId',employee_id,
                'version',source_version,'operation','SNAPSHOT_DEACTIVATED','active',false)::text,now()
            FROM event_rows
            """;
        int deactivated = jdbc.update(sql, runId, tenant);
        jdbc.update("update reconciliation_runs set status='APPLIED',applied_at=now() where id=? and tenant_id=?", runId, tenant);
        return deactivated;
    }

    private String getRunStatus(String tenant, UUID runId) {
        try { return jdbc.queryForObject("select status from reconciliation_runs where id=? and tenant_id=?", String.class, runId, tenant); }
        catch (org.springframework.dao.EmptyResultDataAccessException ex) { throw new IllegalArgumentException("RECONCILIATION_RUN_NOT_FOUND"); }
    }
    private void flushMissing(List<Object[]> batch) {
        if (batch.isEmpty()) return;
        jdbc.batchUpdate("insert into reconciliation_missing(run_id,tenant_id,employee_id) values (?,?,?) on conflict do nothing", batch);
        batch.clear();
    }
    private Path[] createBuckets(Path dir, String prefix) throws IOException {
        Path[] paths = new Path[BUCKETS];
        for (int i = 0; i < BUCKETS; i++) paths[i] = Files.createFile(dir.resolve(prefix + i + ".txt"));
        return paths;
    }
    private int bucket(String id) { return (id.hashCode() & 0x7fffffff) % BUCKETS; }
    private static final class WriterGroup implements AutoCloseable {
        private final Writer[] writers;
        WriterGroup(Path[] paths) throws IOException {
            writers = new Writer[paths.length];
            try { for (int i = 0; i < paths.length; i++) writers[i] = Files.newBufferedWriter(paths[i], StandardCharsets.UTF_8, StandardOpenOption.APPEND); }
            catch (IOException failure) { close(); throw failure; }
        }
        Writer writer(int bucket) { return writers[bucket]; }
        @Override public void close() throws IOException {
            IOException failure = null;
            for (Writer writer : writers) if (writer != null) try { writer.close(); } catch (IOException e) { if (failure == null) failure = e; }
            if (failure != null) throw failure;
        }
    }
    public record ReconciliationReport(UUID runId, long activeBefore, long snapshotRows, long estimatedMissing,
            double missingRatio, boolean safetyBrake, String status, List<String> invalidRows, long invalidRowsTruncated) { }
}
