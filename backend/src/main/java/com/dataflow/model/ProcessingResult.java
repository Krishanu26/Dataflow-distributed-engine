package com.dataflow.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;

public record ProcessingResult(
        String jobId,
        String status,
        long totalRecords,
        int partitions,
        long durationMillis,
        Map<String, Long> countsByValue,
        Map<String, BigDecimal> aggregatesByGroup,
        String operation,
        String column,
        String groupBy,
        Instant completedAt
) {
    public ProcessingResult {
        countsByValue = Map.copyOf(countsByValue);
        aggregatesByGroup = Map.copyOf(aggregatesByGroup);
    }

    /** Preserves the original constructor for existing COUNT callers. */
    public ProcessingResult(String jobId, String status, long totalRecords, int partitions,
                            long durationMillis, Map<String, Long> countsByValue, Instant completedAt) {
        this(jobId, status, totalRecords, partitions, durationMillis, countsByValue, Map.of(),
                "COUNT", null, null, completedAt);
    }
}
