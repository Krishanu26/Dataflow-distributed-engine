package com.dataflow.job;

import com.dataflow.engine.CsvDatasetReader;
import com.dataflow.engine.DataFlowEngine;
import com.dataflow.model.JobStatus;
import com.dataflow.model.ProcessingResult;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
public class JobService {
    private final JobStore store;
    private final CsvDatasetReader reader;
    private final DataFlowEngine engine;

    public JobService(JobStore store, CsvDatasetReader reader, DataFlowEngine engine) {
        this.store = store; this.reader = reader; this.engine = engine;
    }

    public ProcessingJob submit(MultipartFile file, String column, int partitions) throws IOException {
        if (file == null || file.isEmpty()) throw new IllegalArgumentException("Please upload a non-empty CSV file");
        if (column == null || column.isBlank()) throw new IllegalArgumentException("Column is required");
        if (partitions < 1 || partitions > 128) throw new IllegalArgumentException("Partitions must be between 1 and 128");
        String original = file.getOriginalFilename();
        if (original == null || !original.toLowerCase().endsWith(".csv")) throw new IllegalArgumentException("Only CSV files are supported");

        Path temp = Files.createTempFile("dataflow-", ".csv");
        file.transferTo(temp);
        ProcessingJob job = new ProcessingJob(UUID.randomUUID().toString(), partitions);
        store.save(job);
        CompletableFuture.runAsync(() -> execute(job, temp, column, partitions));
        return job;
    }

    private void execute(ProcessingJob job, Path file, String column, int partitions) {
        long started = System.nanoTime();
        job.setStatus(JobStatus.RUNNING);
        try {
            CsvDatasetReader.Dataset dataset = reader.read(file);
            if (!dataset.headers().contains(column)) throw new IllegalArgumentException("Column not found. Available columns: " + dataset.headers());
            job.addProcessed(dataset.rows().size());
            var counts = engine.countByColumn(dataset.rows(), column, partitions);
            long elapsed = (System.nanoTime() - started) / 1_000_000;
            job.complete(new ProcessingResult(job.getId(), "COMPLETED", dataset.rows().size(),
                    Math.min(partitions, Math.max(1, dataset.rows().size())), elapsed, counts, Instant.now()), elapsed);
        } catch (Exception exception) {
            long elapsed = (System.nanoTime() - started) / 1_000_000;
            job.fail(exception.getMessage() == null ? "Processing failed" : exception.getMessage(), elapsed);
        } finally {
            try { Files.deleteIfExists(file); } catch (IOException ignored) { }
        }
    }
}
