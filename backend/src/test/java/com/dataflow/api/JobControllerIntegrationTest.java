package com.dataflow.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class JobControllerIntegrationTest {
    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void acceptsCsvAndReturnsGroupedNumericAggregation() throws Exception {
        String csv = "city,amount\nPune,10.25\nPune,9.75\nDelhi,5.00\n";
        String jobId = submit(csv, "amount", "SUM", "city");

        JsonNode result = awaitResult(jobId);
        assertEquals("COMPLETED", result.get("status").asText());
        assertEquals("SUM", result.get("operation").asText());
        assertEquals(0, new java.math.BigDecimal("20.00")
                .compareTo(result.get("aggregatesByGroup").get("Pune").decimalValue()));
        assertEquals(0, new java.math.BigDecimal("5.00")
                .compareTo(result.get("aggregatesByGroup").get("Delhi").decimalValue()));
    }

    @Test
    void keepsTheOriginalCountByValueApiAsTheDefaultOperation() throws Exception {
        String csv = "city\nKolkata\nDelhi\nKolkata\n";
        String jobId = submit(csv, "city", null, null);

        JsonNode result = awaitResult(jobId);
        assertEquals("COUNT", result.get("operation").asText());
        assertEquals(2L, result.get("countsByValue").get("Kolkata").asLong());
        assertEquals(1L, result.get("countsByValue").get("Delhi").asLong());
    }

    @Test
    void marksInvalidNumericInputAsAFailedJob() throws Exception {
        String csv = "city,amount\nPune,not-a-number\n";
        String jobId = submit(csv, "amount", "SUM", "city");

        JsonNode snapshot = awaitTerminalSnapshot(jobId);
        assertEquals("FAILED", snapshot.get("status").asText());
        assertTrue(snapshot.get("error").asText().contains("non-numeric"));
    }

    private String submit(String csv, String column, String operation, String groupBy) throws Exception {
        MockMultipartFile file = new MockMultipartFile(
                "file", "integration.csv", "text/csv",
                csv.getBytes(java.nio.charset.StandardCharsets.UTF_8));

        var request = multipart("/api/jobs")
                .file(file)
                .param("column", column)
                .param("partitions", "3");
        if (operation != null) {
            request.param("operation", operation);
        }
        if (groupBy != null) {
            request.param("groupBy", groupBy);
        }

        MvcResult response = mockMvc.perform(request)
                .andExpect(status().isAccepted())
                .andReturn();
        return objectMapper.readTree(response.getResponse().getContentAsString())
                .get("jobId").asText();
    }

    private JsonNode awaitResult(String jobId) throws Exception {
        JsonNode snapshot = awaitTerminalSnapshot(jobId);
        assertEquals("COMPLETED", snapshot.get("status").asText(), snapshot.toString());
        MvcResult response = mockMvc.perform(get("/api/jobs/{id}/result", jobId))
                .andExpect(status().isOk())
                .andReturn();
        return objectMapper.readTree(response.getResponse().getContentAsString());
    }

    private JsonNode awaitTerminalSnapshot(String jobId) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
        JsonNode snapshot;
        do {
            MvcResult response = mockMvc.perform(get("/api/jobs/{id}", jobId))
                    .andExpect(status().isOk())
                    .andReturn();
            snapshot = objectMapper.readTree(response.getResponse().getContentAsString());
            String status = snapshot.get("status").asText();
            if ("COMPLETED".equals(status) || "FAILED".equals(status)) {
                return snapshot;
            }
            Thread.sleep(25);
        } while (System.nanoTime() < deadline);

        fail("Job did not reach a terminal state before the test timeout");
        return snapshot;
    }
}
