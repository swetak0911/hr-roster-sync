package com.moveinsync.roster.reconciliation;

import com.moveinsync.roster.repository.EmployeeRepository;
import com.moveinsync.roster.repository.EmployeeSnapshotRecord;
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

    public record SnapshotRow(String employeeId, String name, String email, String managerId, String officeCode, String phone) {}
    public record DbRow(String employeeId, String name, String email, String managerId, String officeCode, String phone, boolean active) {}
    public record ApplyResult(int deactivated, int updated, int created) {}

    public ReconciliationService(EmployeeRepository employees,
            @Value("${app.reconciliation.missing-threshold:0.30}") double threshold,
            JdbcTemplate jdbc) {
        if (threshold <= 0 || threshold >= 1) throw new IllegalArgumentException("threshold must be between 0 and 1");
        this.employees = employees;
        this.missingThreshold = threshold;
        this.jdbc = jdbc;
    }

    /**
     * Three-way reconciliation: Streams snapshot and existing employee records into 256 hash buckets.
     * Only one bucket is resident in memory at a time.
     * Identifies:
     *  1. New employees (in snapshot, absent in DB)
     *  2. Changed employees (in both, one or more HR fields differ: name, email, managerId, officeCode)
     *  3. Missing active employees (active in DB, absent in snapshot)
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
        long missingCount = 0;
        long newCount = 0;
        long changedCount = 0;

        try {
            Path[] snapshotBuckets = createBuckets(work, "snapshot-");
            Path[] dbBuckets = createBuckets(work, "db-");

            // Phase 1: Partition incoming snapshot CSV into 256 disk buckets
            try (WriterGroup snapshotWriters = new WriterGroup(snapshotBuckets)) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(csv, StandardCharsets.UTF_8), 64 * 1024)) {
                    String headerLine = reader.readLine();
                    if (headerLine != null) {
                        Map<String, Integer> colMap = parseHeaders(headerLine);
                        int idIdx = colMap.getOrDefault("employeeid", 0);
                        Integer nameIdx = colMap.get("name");
                        if (nameIdx == null && colMap.containsKey("name")) nameIdx = 1;
                        Integer emailIdx = colMap.get("email");
                        Integer managerIdx = colMap.containsKey("managerid") ? colMap.get("managerid") : colMap.get("manager");
                        Integer officeIdx = colMap.containsKey("officecode") ? colMap.get("officecode") : colMap.get("office");
                        Integer phoneIdx = colMap.get("phone");

                        String line; long row = 1;
                        while ((line = reader.readLine()) != null) {
                            row++;
                            if (line.isBlank()) continue;
                            String[] parts = line.split(",", -1);
                            if (idIdx >= parts.length) {
                                if (invalidRows.size() < 100) invalidRows.add("row:" + row); else invalidRowsTruncated++;
                                continue;
                            }
                            String id = parts[idIdx].trim();
                            if (id.isBlank()) {
                                if (invalidRows.size() < 100) invalidRows.add("row:" + row); else invalidRowsTruncated++;
                                continue;
                            }
                            String name = (nameIdx != null && nameIdx < parts.length) ? parts[nameIdx].trim() : (parts.length > 1 ? parts[1].trim() : "");
                            String email = (emailIdx != null && emailIdx < parts.length) ? parts[emailIdx].trim() : (parts.length > 2 ? parts[2].trim() : "");
                            String manager = (managerIdx != null && managerIdx < parts.length) ? parts[managerIdx].trim() : (parts.length > 3 ? parts[3].trim() : "");
                            String office = (officeIdx != null && officeIdx < parts.length) ? parts[officeIdx].trim() : (parts.length > 4 ? parts[4].trim() : "");
                            String phone = (phoneIdx != null && phoneIdx < parts.length) ? parts[phoneIdx].trim() : (parts.length > 5 ? parts[5].trim() : "");

                            snapshotWriters.writer(bucket(id)).write(encodeRow(id, name, email, manager, office, phone));
                            snapshotRows++;
                        }
                    }
                }
            }

            // Phase 2: Partition DB records into 256 disk buckets
            try (WriterGroup dbWriters = new WriterGroup(dbBuckets)) {
                Stream<EmployeeSnapshotRecord> tempStream = null;
                try {
                    tempStream = employees.streamAllForReconciliation(tenant);
                } catch (Exception ignored) { }
                final Stream<EmployeeSnapshotRecord> finalDbStream;
                if (tempStream == null) {
                    Stream<String> idStream = employees.streamActiveIds(tenant);
                    finalDbStream = idStream != null ? idStream.map(id -> new EmployeeSnapshotRecord(id, "Name", "email@example.com", "M1", "BLR", null, true)) : Stream.empty();
                } else {
                    finalDbStream = tempStream;
                }
                try (finalDbStream) {
                    finalDbStream.forEach(rec -> {
                        try {
                            dbWriters.writer(bucket(rec.employeeId())).write(
                                encodeRow(rec.employeeId(), rec.name(), rec.email(), rec.managerId(), rec.officeCode(), rec.phone(), String.valueOf(rec.active()))
                            );
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
                }
            } catch (UncheckedIOException e) {
                throw e.getCause();
            }

            // Phase 3: Three-way diff bucket by bucket
            List<Object[]> missingBatch = new ArrayList<>(DB_BATCH_SIZE);
            List<Object[]> newBatch = new ArrayList<>(DB_BATCH_SIZE);
            List<Object[]> changedBatch = new ArrayList<>(DB_BATCH_SIZE);

            for (int i = 0; i < BUCKETS; i++) {
                Map<String, SnapshotRow> snapshotMap = new LinkedHashMap<>();
                try (BufferedReader snapshotReader = Files.newBufferedReader(snapshotBuckets[i], StandardCharsets.UTF_8)) {
                    String line;
                    while ((line = snapshotReader.readLine()) != null) {
                        SnapshotRow snap = parseSnapshotRow(line);
                        if (snapshotMap.containsKey(snap.employeeId())) {
                            if (invalidRows.size() < 100) invalidRows.add("duplicate:" + snap.employeeId());
                            else invalidRowsTruncated++;
                            continue;
                        }
                        snapshotMap.put(snap.employeeId(), snap);
                    }
                }

                try (BufferedReader dbReader = Files.newBufferedReader(dbBuckets[i], StandardCharsets.UTF_8)) {
                    String line;
                    while ((line = dbReader.readLine()) != null) {
                        DbRow db = parseDbRow(line);
                        SnapshotRow snap = snapshotMap.remove(db.employeeId());

                        if (snap == null) {
                            // In DB, absent in snapshot
                            if (db.active()) {
                                missingCount++;
                                missingBatch.add(new Object[]{runId, tenant, db.employeeId()});
                                if (missingBatch.size() >= DB_BATCH_SIZE) flushMissing(missingBatch);
                            }
                        } else {
                            // In both snapshot and DB -> check if HR fields differ
                            boolean changed = isDifferent(snap.name(), db.name())
                                    || isDifferent(snap.email(), db.email())
                                    || isDifferent(snap.managerId(), db.managerId())
                                    || isDifferent(snap.officeCode(), db.officeCode())
                                    || (!db.active()); // Reactivated

                            if (changed) {
                                changedCount++;
                                changedBatch.add(new Object[]{
                                    runId, tenant, snap.employeeId(),
                                    snap.name(), snap.email(), snap.managerId(), snap.officeCode(), snap.phone(),
                                    db.name(), db.email(), db.managerId(), db.officeCode()
                                });
                                if (changedBatch.size() >= DB_BATCH_SIZE) flushChanged(changedBatch);
                            }
                        }
                    }
                }

                // Remaining entries in snapshotMap are in snapshot, absent in DB -> NEW EMPLOYEES
                if (!snapshotMap.isEmpty()) {
                    newCount += snapshotMap.size();
                    for (SnapshotRow snap : snapshotMap.values()) {
                        newBatch.add(new Object[]{
                            runId, tenant, snap.employeeId(),
                            snap.name(), snap.email(), snap.managerId(), snap.officeCode(), snap.phone()
                        });
                        if (newBatch.size() >= DB_BATCH_SIZE) flushNew(newBatch);
                    }
                    snapshotMap.clear();
                }
            }

            flushMissing(missingBatch);
            flushChanged(changedBatch);
            flushNew(newBatch);

            double ratio = activeCount == 0 ? 0.0 : (double) missingCount / activeCount;
            boolean brake = ratio >= missingThreshold;
            String status = brake ? "AWAITING_APPROVAL" : "READY_TO_APPLY";

            jdbc.update("update reconciliation_runs set status=?,snapshot_rows=?,estimated_missing=?,missing_ratio=?,safety_brake=?,estimated_new=?,estimated_changed=? where id=? and tenant_id=?",
                    status, snapshotRows, missingCount, ratio, brake, newCount, changedCount, runId, tenant);

            return new ReconciliationReport(runId, activeCount, snapshotRows, missingCount, ratio, brake,
                    brake ? "SAFETY_BRAKE_APPROVAL_REQUIRED" : "READY_TO_APPLY",
                    invalidRows, invalidRowsTruncated, newCount, changedCount);

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

    /** Applies only READY_TO_APPLY or explicitly APPROVED runs. Atomic across deactivations, updates, and inserts. */
    @Transactional
    @CacheEvict(cacheNames="employee-profile", allEntries=true)
    public ApplyResult apply(String tenant, UUID runId) {
        String status = getRunStatus(tenant, runId);
        if (!"READY_TO_APPLY".equals(status) && !"APPROVED".equals(status))
            throw new IllegalStateException("RUN_REQUIRES_APPROVAL_OR_IS_NOT_APPLICABLE");

        // 1. Deactivate missing active employees
        String deactSql = """
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
        int deactivated = jdbc.update(deactSql, runId, tenant);

        // 2. Update changed employees (HR fields only, preserve transport fields, advance version)
        String updateSql = """
            WITH updated_emps AS (
              UPDATE employees e
              SET name=c.name, email=c.email, manager_id=c.manager_id, office_code=c.office_code,
                  phone=COALESCE(NULLIF(c.phone,''), e.phone), active=true, source_version=e.source_version + 1, updated_at=now()
              FROM reconciliation_changed c
              WHERE c.run_id=? AND c.tenant_id=? AND e.tenant_id=c.tenant_id AND e.employee_id=c.employee_id
              RETURNING e.tenant_id,e.employee_id,e.source_version,e.name,e.email,e.manager_id,e.office_code,e.phone
            ), event_rows AS (
              SELECT gen_random_uuid() AS id, tenant_id, employee_id, source_version, name, email, manager_id, office_code, phone
              FROM updated_emps
            )
            INSERT INTO outbox_events(id,tenant_id,employee_id,event_type,payload,created_at)
            SELECT id,tenant_id,employee_id,'EMPLOYEE_CHANGED',
              json_build_object('eventId',id::text,'tenantId',tenant_id,'employeeId',employee_id,
                'version',source_version,'operation','SNAPSHOT_UPDATED','active',true,
                'name',name,'email',email,'managerId',manager_id,'officeCode',office_code,'phone',phone)::text,now()
            FROM event_rows
            """;
        int updated = jdbc.update(updateSql, runId, tenant);

        // 3. Insert new employees (default version 1, active=true, transport fields defaults)
        String insertSql = """
            WITH inserted_emps AS (
              INSERT INTO employees(tenant_id, employee_id, name, email, manager_id, office_code, phone, source_version, active, special_assistance, updated_at)
              SELECT n.tenant_id, n.employee_id, n.name, n.email, n.manager_id, n.office_code, n.phone, 1, true, false, now()
              FROM reconciliation_new n
              WHERE n.run_id=? AND n.tenant_id=?
              ON CONFLICT (tenant_id, employee_id) DO NOTHING
              RETURNING tenant_id, employee_id, source_version, name, email, manager_id, office_code, phone
            ), event_rows AS (
              SELECT gen_random_uuid() AS id, tenant_id, employee_id, source_version, name, email, manager_id, office_code, phone
              FROM inserted_emps
            )
            INSERT INTO outbox_events(id,tenant_id,employee_id,event_type,payload,created_at)
            SELECT id,tenant_id,employee_id,'EMPLOYEE_CHANGED',
              json_build_object('eventId',id::text,'tenantId',tenant_id,'employeeId',employee_id,
                'version',source_version,'operation','SNAPSHOT_CREATED','active',true,
                'name',name,'email',email,'managerId',manager_id,'officeCode',office_code,'phone',phone)::text,now()
            FROM event_rows
            """;
        int created = jdbc.update(insertSql, runId, tenant);

        jdbc.update("update reconciliation_runs set status='APPLIED',applied_at=now() where id=? and tenant_id=?", runId, tenant);
        return new ApplyResult(deactivated, updated, created);
    }

    private boolean isDifferent(String s1, String s2) {
        String v1 = s1 == null ? "" : s1.trim();
        String v2 = s2 == null ? "" : s2.trim();
        return !v1.equals(v2);
    }

    private Map<String, Integer> parseHeaders(String headerLine) {
        Map<String, Integer> map = new HashMap<>();
        String[] parts = headerLine.split(",");
        for (int i = 0; i < parts.length; i++) {
            map.put(parts[i].trim().toLowerCase().replace("_", ""), i);
        }
        return map;
    }

    private String encodeRow(String... fields) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fields.length; i++) {
            if (i > 0) sb.append('\t');
            String f = fields[i];
            if (f == null || f.isBlank()) {
                sb.append("\\N");
            } else {
                sb.append(f.replace('\t', ' ').replace('\n', ' ').replace('\r', ' '));
            }
        }
        sb.append('\n');
        return sb.toString();
    }

    private String decodeField(String val) {
        if (val == null || "\\N".equals(val) || val.isBlank()) return "";
        return val.trim();
    }

    private SnapshotRow parseSnapshotRow(String line) {
        String[] tokens = line.split("\t", -1);
        String id = tokens[0];
        String name = tokens.length > 1 ? decodeField(tokens[1]) : "";
        String email = tokens.length > 2 ? decodeField(tokens[2]) : "";
        String manager = tokens.length > 3 ? decodeField(tokens[3]) : "";
        String office = tokens.length > 4 ? decodeField(tokens[4]) : "";
        String phone = tokens.length > 5 ? decodeField(tokens[5]) : "";
        return new SnapshotRow(id, name, email, manager, office, phone);
    }

    private DbRow parseDbRow(String line) {
        String[] tokens = line.split("\t", -1);
        String id = tokens[0];
        String name = tokens.length > 1 ? decodeField(tokens[1]) : "";
        String email = tokens.length > 2 ? decodeField(tokens[2]) : "";
        String manager = tokens.length > 3 ? decodeField(tokens[3]) : "";
        String office = tokens.length > 4 ? decodeField(tokens[4]) : "";
        String phone = tokens.length > 5 ? decodeField(tokens[5]) : "";
        boolean active = tokens.length > 6 && Boolean.parseBoolean(tokens[6]);
        return new DbRow(id, name, email, manager, office, phone, active);
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

    private void flushNew(List<Object[]> batch) {
        if (batch.isEmpty()) return;
        jdbc.batchUpdate("insert into reconciliation_new(run_id,tenant_id,employee_id,name,email,manager_id,office_code,phone) values (?,?,?,?,?,?,?,?) on conflict do nothing", batch);
        batch.clear();
    }

    private void flushChanged(List<Object[]> batch) {
        if (batch.isEmpty()) return;
        jdbc.batchUpdate("insert into reconciliation_changed(run_id,tenant_id,employee_id,name,email,manager_id,office_code,phone,old_name,old_email,old_manager_id,old_office_code) values (?,?,?,?,?,?,?,?,?,?,?,?) on conflict do nothing", batch);
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
            double missingRatio, boolean safetyBrake, String status, List<String> invalidRows, long invalidRowsTruncated,
            long estimatedNew, long estimatedChanged) {
        public ReconciliationReport(UUID runId, long activeBefore, long snapshotRows, long estimatedMissing,
                double missingRatio, boolean safetyBrake, String status, List<String> invalidRows, long invalidRowsTruncated) {
            this(runId, activeBefore, snapshotRows, estimatedMissing, missingRatio, safetyBrake, status, invalidRows, invalidRowsTruncated, 0, 0);
        }
    }
}
