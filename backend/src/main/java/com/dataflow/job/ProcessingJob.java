package com.dataflow.job;

import com.dataflow.model.JobStatus;
import com.dataflow.model.ProcessingResult;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

public class ProcessingJob {
    private final String id;
    private final Instant submittedAt = Instant.now();
    private final int partitions;
    private final AtomicLong recordsProcessed = new AtomicLong();
    private volatile JobStatus status = JobStatus.QUEUED;
    private volatile Instant completedAt;
    private volatile long durationMillis;
    private volatile String error;
    private volatile ProcessingResult result;

    public ProcessingJob(String id, int partitions) { this.id = id; this.partitions = partitions; }
    public String getId() { return id; }
    public int getPartitions() { return partitions; }
    public Instant getSubmittedAt() { return submittedAt; }
    public long getRecordsProcessed() { return recordsProcessed.get(); }
    public void addProcessed(long count) { recordsProcessed.addAndGet(count); }
    public JobStatus getStatus() { return status; }
    public void setStatus(JobStatus status) { this.status = status; }
    public Instant getCompletedAt() { return completedAt; }
    public long getDurationMillis() { return durationMillis; }
    public String getError() { return error; }
    public ProcessingResult getResult() { return result; }
    public void complete(ProcessingResult result, long durationMillis) {
        this.result = result; this.durationMillis = durationMillis;
        this.completedAt = Instant.now(); this.status = JobStatus.COMPLETED;
    }
    public void fail(String message, long durationMillis) {
        this.error = message; this.durationMillis = durationMillis;
        this.completedAt = Instant.now(); this.status = JobStatus.FAILED;
    }
}
