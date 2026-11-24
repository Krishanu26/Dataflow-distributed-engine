package com.dataflow.engine;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/** Immutable output of a partitioned aggregation. */
public record AggregationOutput(
        Map<String, Long> countsByGroup,
        Map<String, BigDecimal> valuesByGroup
) {
    public AggregationOutput {
        countsByGroup = Collections.unmodifiableMap(new TreeMap<>(countsByGroup));
        valuesByGroup = Collections.unmodifiableMap(new TreeMap<>(valuesByGroup));
    }
}
