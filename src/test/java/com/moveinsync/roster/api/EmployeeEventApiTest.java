package com.moveinsync.roster.api;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Base64;
import java.util.UUID;

import static org.hamcrest.Matchers.hasSize;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@TestPropertySource(properties = {
    "spring.kafka.bootstrap-servers=localhost:9094",
    "spring.datasource.url=jdbc:postgresql://localhost:5432/roster",
    "spring.datasource.username=roster",
    "spring.datasource.password=roster",
    "spring.data.redis.host=localhost",
    "spring.data.redis.port=6379"
})
class EmployeeEventApiTest {

    @Autowired
    private MockMvc mockMvc;

    private final String operatorAuth = "Basic " + Base64.getEncoder().encodeToString("operator:change-me".getBytes());

    @Test
    void testCreateEmployeeSuccess() throws Exception {
        String empId = "E-" + UUID.randomUUID().toString().substring(0, 8);
        String payload = """
        {
          "tenantId": "acme",
          "employeeId": "%s",
          "type": "CREATE",
          "version": 1,
          "name": "Dev Sharma",
          "email": "dev@example.com",
          "managerId": "M-1",
          "officeCode": "BLR",
          "phone": "+919876543210"
        }
        """.formatted(empId);

        mockMvc.perform(post("/api/v1/events")
                .header(HttpHeaders.AUTHORIZATION, operatorAuth)
                .header("X-Tenant-Id", "acme")
                .contentType(MediaType.APPLICATION_JSON)
                .content(payload))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.employeeId").value(empId))
                .andExpect(jsonPath("$.status").value("APPLIED"))
                .andExpect(jsonPath("$.reason").value("VERSION_ACCEPTED"))
                .andExpect(jsonPath("$.appliedVersion").value(1));
    }

    @Test
    void testDuplicateVersionIsIgnored() throws Exception {
        String empId = "E-" + UUID.randomUUID().toString().substring(0, 8);
        String payload = """
        {
          "tenantId": "acme",
          "employeeId": "%s",
          "type": "CREATE",
          "version": 1,
          "name": "Dev Sharma",
          "email": "dev@example.com",
          "managerId": "M-1",
          "officeCode": "BLR",
          "phone": "+919876543210"
        }
        """.formatted(empId);

        // First apply
        mockMvc.perform(post("/api/v1/events")
                .header(HttpHeaders.AUTHORIZATION, operatorAuth)
                .header("X-Tenant-Id", "acme")
                .contentType(MediaType.APPLICATION_JSON)
                .content(payload))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("APPLIED"));

        // Replay duplicate
        mockMvc.perform(post("/api/v1/events")
                .header(HttpHeaders.AUTHORIZATION, operatorAuth)
                .header("X-Tenant-Id", "acme")
                .contentType(MediaType.APPLICATION_JSON)
                .content(payload))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.employeeId").value(empId))
                .andExpect(jsonPath("$.status").value("IGNORED"))
                .andExpect(jsonPath("$.reason").value("DUPLICATE_VERSION"))
                .andExpect(jsonPath("$.appliedVersion").value(1));
    }

    @Test
    void testStaleVersionIsIgnored() throws Exception {
        String empId = "E-" + UUID.randomUUID().toString().substring(0, 8);
        String v2Payload = """
        {
          "tenantId": "acme",
          "employeeId": "%s",
          "type": "CREATE",
          "version": 2,
          "name": "Dev Sharma",
          "email": "dev@example.com",
          "managerId": "M-1",
          "officeCode": "BLR",
          "phone": "+919876543210"
        }
        """.formatted(empId);

        mockMvc.perform(post("/api/v1/events")
                .header(HttpHeaders.AUTHORIZATION, operatorAuth)
                .header("X-Tenant-Id", "acme")
                .contentType(MediaType.APPLICATION_JSON)
                .content(v2Payload))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("APPLIED"));

        // Stale v1
        String v1Payload = """
        {
          "tenantId": "acme",
          "employeeId": "%s",
          "type": "UPDATE",
          "version": 1,
          "name": "Dev Old",
          "email": "dev@example.com",
          "managerId": "M-1",
          "officeCode": "BLR",
          "phone": "+919876543210"
        }
        """.formatted(empId);

        mockMvc.perform(post("/api/v1/events")
                .header(HttpHeaders.AUTHORIZATION, operatorAuth)
                .header("X-Tenant-Id", "acme")
                .contentType(MediaType.APPLICATION_JSON)
                .content(v1Payload))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.employeeId").value(empId))
                .andExpect(jsonPath("$.status").value("IGNORED"))
                .andExpect(jsonPath("$.reason").value("STALE_VERSION"))
                .andExpect(jsonPath("$.appliedVersion").value(2));
    }

    @Test
    void testHigherVersionIsApplied() throws Exception {
        String empId = "E-" + UUID.randomUUID().toString().substring(0, 8);
        String v1Payload = """
        {
          "tenantId": "acme",
          "employeeId": "%s",
          "type": "CREATE",
          "version": 1,
          "name": "Dev Sharma",
          "email": "dev@example.com",
          "managerId": "M-1",
          "officeCode": "BLR",
          "phone": "+919876543210"
        }
        """.formatted(empId);

        mockMvc.perform(post("/api/v1/events")
                .header(HttpHeaders.AUTHORIZATION, operatorAuth)
                .header("X-Tenant-Id", "acme")
                .contentType(MediaType.APPLICATION_JSON)
                .content(v1Payload))
                .andExpect(status().isOk());

        String v2Payload = """
        {
          "tenantId": "acme",
          "employeeId": "%s",
          "type": "UPDATE",
          "version": 2,
          "name": "Dev Updated",
          "email": "dev.updated@example.com",
          "managerId": "M-1",
          "officeCode": "HYD",
          "phone": "+919876543210"
        }
        """.formatted(empId);

        mockMvc.perform(post("/api/v1/events")
                .header(HttpHeaders.AUTHORIZATION, operatorAuth)
                .header("X-Tenant-Id", "acme")
                .contentType(MediaType.APPLICATION_JSON)
                .content(v2Payload))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.employeeId").value(empId))
                .andExpect(jsonPath("$.status").value("APPLIED"))
                .andExpect(jsonPath("$.reason").value("VERSION_ACCEPTED"))
                .andExpect(jsonPath("$.appliedVersion").value(2));
    }

    @Test
    void testTenantMismatchInHeaderForbidden() throws Exception {
        String payload = """
        {
          "tenantId": "globex",
          "employeeId": "E-9001",
          "type": "UPDATE",
          "version": 3,
          "name": "Dev",
          "email": "dev@example.com",
          "managerId": "M-1",
          "officeCode": "HYD",
          "phone": "+919876543210"
        }
        """;

        mockMvc.perform(post("/api/v1/events")
                .header(HttpHeaders.AUTHORIZATION, operatorAuth) // operator is in tenant "acme"
                .header("X-Tenant-Id", "globex")
                .contentType(MediaType.APPLICATION_JSON)
                .content(payload))
                .andExpect(status().isForbidden());
    }

    @Test
    void testTenantMismatchInBodyForbidden() throws Exception {
        String payload = """
        {
          "tenantId": "globex",
          "employeeId": "E-9002",
          "type": "UPDATE",
          "version": 3,
          "name": "Dev",
          "email": "dev@example.com",
          "managerId": "M-1",
          "officeCode": "HYD",
          "phone": "+919876543210"
        }
        """;

        mockMvc.perform(post("/api/v1/events")
                .header(HttpHeaders.AUTHORIZATION, operatorAuth) // operator is in tenant "acme"
                .header("X-Tenant-Id", "acme")
                .contentType(MediaType.APPLICATION_JSON)
                .content(payload))
                .andExpect(status().isForbidden());
    }

    @Test
    void testInvalidOfficeCodeRejected() throws Exception {
        String payload = """
        {
          "tenantId": "acme",
          "employeeId": "E-3001",
          "type": "CREATE",
          "version": 1,
          "name": "Priya",
          "email": "priya@example.com",
          "managerId": "M-1",
          "officeCode": "INVALID_OFFICE",
          "phone": "+919876543210"
        }
        """;

        mockMvc.perform(post("/api/v1/events")
                .header(HttpHeaders.AUTHORIZATION, operatorAuth)
                .header("X-Tenant-Id", "acme")
                .contentType(MediaType.APPLICATION_JSON)
                .content(payload))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.status").value("REJECTED"))
                .andExpect(jsonPath("$.reason").value("UNKNOWN_OFFICE_CODE"));
    }

    @Test
    void testInvalidPhoneRejected() throws Exception {
        String payload = """
        {
          "tenantId": "acme",
          "employeeId": "E-3002",
          "type": "CREATE",
          "version": 1,
          "name": "Priya",
          "email": "priya@example.com",
          "managerId": "M-1",
          "officeCode": "BLR",
          "phone": "invalid-phone"
        }
        """;

        mockMvc.perform(post("/api/v1/events")
                .header(HttpHeaders.AUTHORIZATION, operatorAuth)
                .header("X-Tenant-Id", "acme")
                .contentType(MediaType.APPLICATION_JSON)
                .content(payload))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("VALIDATION_FAILED"));
    }

    @Test
    void testDeactivateBeforeCreateIgnored() throws Exception {
        String empId = "E-nonexistent-" + UUID.randomUUID().toString().substring(0, 8);
        String payload = """
        {
          "tenantId": "acme",
          "employeeId": "%s",
          "type": "DEACTIVATE",
          "version": 1
        }
        """.formatted(empId);

        mockMvc.perform(post("/api/v1/events")
                .header(HttpHeaders.AUTHORIZATION, operatorAuth)
                .header("X-Tenant-Id", "acme")
                .contentType(MediaType.APPLICATION_JSON)
                .content(payload))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.employeeId").value(empId))
                .andExpect(jsonPath("$.status").value("IGNORED"))
                .andExpect(jsonPath("$.reason").value("DEACTIVATE_BEFORE_CREATE"));
    }

    @Test
    void testDeactivateExistingEmployee() throws Exception {
        String empId = "E-" + UUID.randomUUID().toString().substring(0, 8);
        String v1Payload = """
        {
          "tenantId": "acme",
          "employeeId": "%s",
          "type": "CREATE",
          "version": 1,
          "name": "Dev Sharma",
          "email": "dev@example.com",
          "managerId": "M-1",
          "officeCode": "BLR",
          "phone": "+919876543210"
        }
        """.formatted(empId);

        mockMvc.perform(post("/api/v1/events")
                .header(HttpHeaders.AUTHORIZATION, operatorAuth)
                .header("X-Tenant-Id", "acme")
                .contentType(MediaType.APPLICATION_JSON)
                .content(v1Payload))
                .andExpect(status().isOk());

        String deactPayload = """
        {
          "tenantId": "acme",
          "employeeId": "%s",
          "type": "DEACTIVATE",
          "version": 2
        }
        """.formatted(empId);

        mockMvc.perform(post("/api/v1/events")
                .header(HttpHeaders.AUTHORIZATION, operatorAuth)
                .header("X-Tenant-Id", "acme")
                .contentType(MediaType.APPLICATION_JSON)
                .content(deactPayload))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.employeeId").value(empId))
                .andExpect(jsonPath("$.status").value("APPLIED"))
                .andExpect(jsonPath("$.reason").value("VERSION_ACCEPTED"))
                .andExpect(jsonPath("$.appliedVersion").value(2));
    }

    @Test
    void testGetEmployeeProfile() throws Exception {
        String empId = "E-" + UUID.randomUUID().toString().substring(0, 8);
        String v1Payload = """
        {
          "tenantId": "acme",
          "employeeId": "%s",
          "type": "CREATE",
          "version": 1,
          "name": "Dev Queryable",
          "email": "devq@example.com",
          "managerId": "M-1",
          "officeCode": "DEL",
          "phone": "+919876543210"
        }
        """.formatted(empId);

        mockMvc.perform(post("/api/v1/events")
                .header(HttpHeaders.AUTHORIZATION, operatorAuth)
                .header("X-Tenant-Id", "acme")
                .contentType(MediaType.APPLICATION_JSON)
                .content(v1Payload))
                .andExpect(status().isOk());

        mockMvc.perform(get("/api/v1/employees/" + empId)
                .header(HttpHeaders.AUTHORIZATION, operatorAuth)
                .header("X-Tenant-Id", "acme"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.employeeId").value(empId))
                .andExpect(jsonPath("$.name").value("Dev Queryable"))
                .andExpect(jsonPath("$.officeCode").value("DEL"))
                .andExpect(jsonPath("$.active").value(true))
                .andExpect(jsonPath("$.sourceVersion").value(1));
    }

    @Test
    void testBatchEndpointWithPartialSuccess() throws Exception {
        String empValid = "E-batch-" + UUID.randomUUID().toString().substring(0, 8);
        String empInvalid = "E-batch-" + UUID.randomUUID().toString().substring(0, 8);

        String batchPayload = """
        [
          {
            "tenantId": "acme",
            "employeeId": "%s",
            "type": "CREATE",
            "version": 1,
            "name": "Batch One",
            "email": "batch1@example.com",
            "managerId": "M-1",
            "officeCode": "BLR",
            "phone": "+919876543210"
          },
          {
            "tenantId": "acme",
            "employeeId": "%s",
            "type": "CREATE",
            "version": 1,
            "name": "Batch Two",
            "email": "invalid-email",
            "managerId": "M-1",
            "officeCode": "BLR",
            "phone": "+919876543210"
          }
        ]
        """.formatted(empValid, empInvalid);

        mockMvc.perform(post("/api/v1/events/batch")
                .header(HttpHeaders.AUTHORIZATION, operatorAuth)
                .header("X-Tenant-Id", "acme")
                .contentType(MediaType.APPLICATION_JSON)
                .content(batchPayload))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(2)))
                .andExpect(jsonPath("$[0].employeeId").value(empValid))
                .andExpect(jsonPath("$[0].status").value("APPLIED"))
                .andExpect(jsonPath("$[1].employeeId").value(empInvalid))
                .andExpect(jsonPath("$[1].status").value("REJECTED"))
                .andExpect(jsonPath("$[1].reason").value("VALIDATION_FAILED"));
    }
}
