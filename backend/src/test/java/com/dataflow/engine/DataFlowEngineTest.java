package com.dataflow.engine;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.*;

class DataFlowEngineTest {
    @Test
    void aggregatesCountsAcrossParallelPartitions() {
        var engine = new DataFlowEngine(new Partitioner());
        try {
            var rows = List.of(Map.of("city", "Pune"), Map.of("city", "Delhi"),
                    Map.of("city", "Pune"), Map.of("city", "Kolkata"), Map.of("city", "Delhi"));
            assertEquals(Map.of("Pune", 2L, "Delhi", 2L, "Kolkata", 1L),
                    engine.countByColumn(rows, "city", 3));
        } finally {
            engine.close();
        }
    }

    @Test
    void performsGroupedNumericMapReduceWithExactDecimalArithmetic() {
        var engine = new DataFlowEngine(new Partitioner());
        try {
            var rows = List.of(
                    Map.of("city", "Pune", "amount", "10.25"),
                    Map.of("city", "Pune", "amount", "9.75"),
                    Map.of("city", "Delhi", "amount", "5.00"));

            var sum = engine.aggregate(rows, "amount", "city", AggregationOperation.SUM, 2);
            assertDecimalEquals("20.00", sum.valuesByGroup().get("Pune"));
            assertDecimalEquals("5.00", sum.valuesByGroup().get("Delhi"));

            var average = engine.aggregate(rows, "amount", "city", AggregationOperation.AVG, 3);
            assertDecimalEquals("10.00", average.valuesByGroup().get("Pune"));
            assertDecimalEquals("5.00", average.valuesByGroup().get("Delhi"));

            var minimum = engine.aggregate(rows, "amount", "city", AggregationOperation.MIN, 2);
            assertDecimalEquals("9.75", minimum.valuesByGroup().get("Pune"));

            var maximum = engine.aggregate(rows, "amount", "city", AggregationOperation.MAX, 2);
            assertDecimalEquals("10.25", maximum.valuesByGroup().get("Pune"));
        } finally {
            engine.close();
        }
    }

    @Test
    void supportsGlobalAggregationWhenNoGroupingColumnIsProvided() {
        var engine = new DataFlowEngine(new Partitioner());
        try {
            var rows = List.of(Map.of("amount", "2.50"), Map.of("amount", "7.50"));
            var result = engine.aggregate(rows, "amount", null, AggregationOperation.SUM, 2);
            assertDecimalEquals("10.00", result.valuesByGroup().get("ALL"));
        } finally {
            engine.close();
        }
    }

    @Test
    void rejectsInvalidNumericValuesInsteadOfSilentlyDroppingRows() {
        var engine = new DataFlowEngine(new Partitioner());
        try {
            var rows = List.of(Map.of("amount", "12.00"), Map.of("amount", "not-a-number"));
            assertThrows(CompletionException.class,
                    () -> engine.aggregate(rows, "amount", null, AggregationOperation.SUM, 2));
        } finally {
            engine.close();
        }
    }

    @Test
    void rejectsWorkAfterShutdownWithoutLeavingAnIncompleteFuture() {
        var engine = new DataFlowEngine(new Partitioner());
        engine.close();

        assertThrows(CompletionException.class, () -> engine.countByColumn(
                List.of(Map.of("city", "Pune")), "city", 1));
    }

    private static void assertDecimalEquals(String expected, BigDecimal actual) {
        assertNotNull(actual);
        assertEquals(0, new BigDecimal(expected).compareTo(actual));
    }
}
