package com.dataflow.engine;

import org.springframework.stereotype.Component;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class DataFlowEngine implements AutoCloseable {
    private final Partitioner partitioner;
    private final ExecutorService workers;

    public DataFlowEngine(Partitioner partitioner) {
        this.partitioner = partitioner;
        int workerCount = Math.max(2, Runtime.getRuntime().availableProcessors());
        this.workers = Executors.newFixedThreadPool(workerCount);
    }

    public Map<String, Long> countByColumn(List<Map<String, String>> rows, String column, int partitionCount) {
        List<List<Map<String, String>>> partitions = partitioner.partition(rows, partitionCount);
        List<CompletableFuture<Map<String, Long>>> tasks = partitions.stream()
                .map(partition -> CompletableFuture.supplyAsync(() -> countPartition(partition, column), workers))
                .toList();
        Map<String, Long> combined = new HashMap<>();
        for (CompletableFuture<Map<String, Long>> task : tasks) {
            task.join().forEach((value, count) -> combined.merge(value, count, Long::sum));
        }
        return Map.copyOf(combined);
    }

    private Map<String, Long> countPartition(List<Map<String, String>> rows, String column) {
        Map<String, Long> counts = new HashMap<>();
        for (Map<String, String> row : rows) {
            String value = row.getOrDefault(column, "");
            counts.merge(value, 1L, Long::sum);
        }
        return counts;
    }

    @Override public void close() { workers.shutdown(); }
}
