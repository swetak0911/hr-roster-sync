package com.moveinsync.roster.outbox;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.UUID;

/**
 * Child JVM used by OutboxProcessCrashRecoveryTest.
 * It commits an employee row and its outbox event atomically, signals the parent,
 * then stays alive until the parent forcibly terminates this process.
 */
public final class OutboxCrashWriterProcess {
    private OutboxCrashWriterProcess() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            throw new IllegalArgumentException("Expected JDBC URL, username, password and event UUID");
        }
        String jdbcUrl = args[0];
        String username = args[1];
        String password = args[2];
        UUID eventId = UUID.fromString(args[3]);

        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password)) {
            connection.setAutoCommit(false);
            try {
                try (PreparedStatement employee = connection.prepareStatement(
                        "INSERT INTO employees (tenant_id, employee_id, name, email, manager_id, office_code, phone, source_version, active, special_assistance, updated_at) " +
                        "VALUES ('crash-tenant', 'CRASH-EMP-1', 'Crash Test Employee', 'crash@example.com', 'M-1', 'BLR', '+919876543210', 1, TRUE, FALSE, now())")) {
                    employee.executeUpdate();
                }
                String payload = "{\"eventId\":\"" + eventId + "\",\"tenantId\":\"crash-tenant\",\"employeeId\":\"CRASH-EMP-1\"}";
                try (PreparedStatement outbox = connection.prepareStatement(
                        "INSERT INTO outbox_events (id, tenant_id, employee_id, event_type, payload, created_at, attempts) " +
                        "VALUES (?, 'crash-tenant', 'CRASH-EMP-1', 'EMPLOYEE_CREATED', ?, now(), 0)")) {
                    outbox.setObject(1, eventId);
                    outbox.setString(2, payload);
                    outbox.executeUpdate();
                }
                connection.commit();
                System.out.println("COMMITTED:" + eventId);
                System.out.flush();

                // Keep the process alive after commit. The parent test kills it here,
                // before any publisher process is started.
                while (true) {
                    Thread.sleep(1_000);
                }
            } catch (Exception failure) {
                connection.rollback();
                throw failure;
            }
        }
    }
}
