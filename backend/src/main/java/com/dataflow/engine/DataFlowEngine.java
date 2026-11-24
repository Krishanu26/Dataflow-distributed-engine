package com.dataflow.engine;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

@Component
public class DataFlowEngine implements AutoCloseable {
    private static final String ALL_GROUPS = "ALL";

    private final Partitioner partitioner;
    private final ThreadPoolExecutor workers;

    @Autowired
    public DataFlowEngine(Partitioner partitioner,
                          @Value("$" + "{dataflow.engine.worker-count:8}") int configuredWorkers,
                          @Value("$" + "{dataflow.engine.queue-capacity:512}") int queueCapacity) {
        if (configuredWorkers < 1 || queueCapacity < 1) {
            throw new IllegalArgumentException("Engine worker count and queue capacity must be positive");
        }
        this.partitioner = partitioner;
        int workerCount = Math.min(8, configuredWorkers);
        AtomicInteger threadNumber = new AtomicInteger();
        this.workers = new ThreadPoolExecutor(
                workerCount,
                workerCount,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueCapacity),
                runnable -> new Thread(runnable, "dataflow-partition-" + threadNumber.incrementAndGet()),
                (task, executor) -> {
                    while (!executor.isShutdown()) {
                        try {
                            if (executor.getQueue().offer(task, 100, TimeUnit.MILLISECONDS)) {
                                if (!executor.isShutdown() || !executor.remove(task)) {
                                    return;
                                }
                            }
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new RejectedExecutionException(
                                    "Interrupted while waiting for engine capacity", interrupted);
                        }
                    }
                    throw new RejectedExecutionException("DataFlow engine is shutting down");
                }
        );
    }

    public DataFlowEngine(Partitioner partitioner) {
        this(partitioner, Math.max(2, Runtime.getRuntime().availableProcessors()), 512);
    }

    /** Backwards-compatible count-by-value operation. */
    public Map<String, Long> countByColumn(List<Map<String, String>> rows,
                                           String column, int partitionCount) {
        return aggregate(rows, column, column, AggregationOperation.COUNT, partitionCount)
                .countsByGroup();
    }

    /**
     * Executes a map-reduce aggregation. Each partition builds local accumulators;
     * the calling thread merges those partial results after all partition tasks finish.
     *
     * @param groupBy null or blank to aggregate all records into one ALL group
     */
    public AggregationOutput aggregate(List<Map<String, String>> rows,
                                       String column,
                                       String groupBy,
                                       AggregationOperation operation,
                                       int partitionCount) {
        if (rows == null || column == null || column.isBlank() || operation == null) {
            throw new IllegalArgumentException("Rows, column, and aggregation operation are required");
        }

        String groupingColumn = groupBy == null || groupBy.isBlank() ? null : groupBy.trim();
        List<List<Map<String, String>>> partitions = partitioner.partition(rows, partitionCount);
        List<CompletableFuture<Map<String, Accumulator>>> tasks = partitions.stream()
                .map(partition -> submitPartition(
                        () -> aggregatePartition(partition, column, groupingColumn, operation)))
                .toList();

        // Wait for all submitted partitions, even when one partition fails.
        CompletableFuture.allOf(tasks.toArray(new CompletableFuture<?>[0])).join();

        Map<String, Accumulator> combined = new HashMap<>();
        for (CompletableFuture<Map<String, Accumulator>> task : tasks) {
            task.join().forEach((key, partial) ->
                    combined.computeIfAbsent(key, ignored -> new Accumulator()).merge(partial));
        }

        if (operation == AggregationOperation.COUNT) {
            Map<String, Long> counts = new TreeMap<>();
            combined.forEach((key, accumulator) -> counts.put(key, accumulator.count));
            return new AggregationOutput(counts, Map.of());
        }

        Map<String, BigDecimal> values = new TreeMap<>();
        combined.forEach((key, accumulator) -> values.put(key, accumulator.value(operation)));
        return new AggregationOutput(Map.of(), values);
    }

    private Map<String, Accumulator> aggregatePartition(
            List<Map<String, String>> rows,
            String column,
            String groupBy,
            AggregationOperation operation) {
        Map<String, Accumulator> partials = new HashMap<>();
        for (Map<String, String> row : rows) {
            String groupKey = groupBy == null ? ALL_GROUPS : row.getOrDefault(groupBy, "");
            Accumulator accumulator = partials.computeIfAbsent(groupKey, ignored -> new Accumulator());

            if (operation == AggregationOperation.COUNT) {
                accumulator.increment();
            } else {
                String rawValue = row.get(column);
                if (rawValue == null || rawValue.isBlank()) {
                    throw new IllegalArgumentException(
                            "Column '" + column + "' contains a blank numeric value");
                }
                try {
                    accumulator.add(new BigDecimal(rawValue.trim()));
                } catch (NumberFormatException invalidNumber) {
                    throw new IllegalArgumentException(
                            "Column '" + column + "' contains a non-numeric value: '" + rawValue + "'",
                            invalidNumber);
                }
            }
        }
        return partials;
    }

    private <T> CompletableFuture<T> submitPartition(Supplier<T> action) {
        CompletableFuture<T> result = new CompletableFuture<>();
        PartitionTask<T> task = new PartitionTask<>(action, result);
        try {
            workers.execute(task);
        } catch (RejectedExecutionException rejected) {
            result.completeExceptionally(rejected);
        }
        return result;
    }

    @Override
    public void close() {
        workers.shutdown();
        try {
            if (!workers.awaitTermination(30, TimeUnit.SECONDS)) {
                completeAbandonedTasks(workers.shutdownNow());
            }
        } catch (InterruptedException interrupted) {
            completeAbandonedTasks(workers.shutdownNow());
            Thread.currentThread().interrupt();
        }
    }

    private void completeAbandonedTasks(List<Runnable> tasks) {
        for (Runnable task : tasks) {
            if (task instanceof PartitionTask<?> partitionTask) {
                partitionTask.failOnShutdown();
            }
        }
    }

    private static final class Accumulator {
        private long count;
        private BigDecimal sum = BigDecimal.ZERO;
        private BigDecimal min;
        private BigDecimal max;

        private void increment() {
            count = Math.addExact(count, 1);
        }

        private void add(BigDecimal value) {
            count = Math.addExact(count, 1);
            sum = sum.add(value);
            min = min == null || value.compareTo(min) < 0 ? value : min;
            max = max == null || value.compareTo(max) > 0 ? value : max;
        }

        private void merge(Accumulator other) {
            count = Math.addExact(count, other.count);
            sum = sum.add(other.sum);
            if (other.min != null && (min == null || other.min.compareTo(min) < 0)) {
                min = other.min;
            }
            if (other.max != null && (max == null || other.max.compareTo(max) > 0)) {
                max = other.max;
            }
        }

        private BigDecimal value(AggregationOperation operation) {
            return switch (operation) {
                case SUM -> sum;
                case AVG -> sum.divide(BigDecimal.valueOf(count), MathContext.DECIMAL128);
                case MIN -> min;
                case MAX -> max;
                case COUNT -> throw new IllegalStateException("COUNT is represented as a long");
            };
        }
    }

    private static final class PartitionTask<T> implements Runnable {
        private final Supplier<T> action;
        private final CompletableFuture<T> result;

        private PartitionTask(Supplier<T> action, CompletableFuture<T> result) {
            this.action = action;
            this.result = result;
        }

        @Override
        public void run() {
            try {
                result.complete(action.get());
            } catch (RuntimeException exception) {
                result.completeExceptionally(exception);
            }
        }

        private void failOnShutdown() {
            result.completeExceptionally(new RejectedExecutionException(
                    "DataFlow engine shut down before the partition could run"));
        }
    }
}
