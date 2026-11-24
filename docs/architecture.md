# DataFlow architecture

## 1. Scope

DataFlow is a single-process parallel data processing engine. A single Spring Boot application accepts CSV jobs and uses local worker pools to process partitions concurrently. The project demonstrates MapReduce-style computation and resource-aware job execution; it is not currently a distributed cluster.

## 2. Request and execution flow

1. JobController accepts a multipart CSV upload and aggregation parameters.
2. JobService validates the request, copies the upload to a temporary file, creates a job record, and submits the job.
3. JobExecutionPool admits the job to a fixed-size executor with a bounded waiting queue.
4. CsvDatasetReader parses the file and exposes its headers and records.
5. Partitioner divides the records into balanced immutable partitions.
6. DataFlowEngine submits partition tasks to a second bounded executor.
7. Each partition builds local aggregation accumulators; the calling job thread reduces partial accumulators into a final result.
8. ProcessingJob transitions to a terminal state and JobStore applies terminal retention.
9. Temporary upload files are deleted in a finally block.

## 3. Components

### API layer

JobController exposes:
- POST /api/jobs: accept a CSV job and return 202 Accepted.
- GET /api/jobs: list jobs known to this process.
- GET /api/jobs/{id}: retrieve job state.
- GET /api/jobs/{id}/result: retrieve a successful result or return 409 while no successful result exists.

ApiExceptionHandler maps invalid input to HTTP 400, queue saturation to HTTP 503 with Retry-After: 5, and unexpected server failures to a generic response without exposing stack traces.

### Job orchestration

JobService owns validation, upload staging, job submission, CSV schema checks, result construction, and temporary-file cleanup.

ProcessingJob is a thread-safe in-memory state holder. Synchronized transition methods enforce:

    QUEUED -> RUNNING -> COMPLETED
                       -> FAILED
       \--------------> FAILED (shutdown before start)

Only queued jobs can start. Running jobs can complete or fail. A queued job can fail when forced application shutdown prevents it from starting.

JobStore uses a ConcurrentHashMap and retains up to the configured number of terminal jobs. Oldest terminal jobs are removed first; queued and running jobs are never evicted. The store is not durable.

### Application job executor

JobExecutionPool uses a fixed-size ThreadPoolExecutor, an ArrayBlockingQueue, and an abort policy.

- Defaults: four job workers and 32 queued jobs.
- Active workers and the waiting queue together bound accepted in-process work.
- When saturated, new submissions are rejected. The service removes the unaccepted job record, deletes its temporary file, and returns HTTP 503 with a retry hint.
- Shutdown first drains accepted work. If the configured timeout expires, queued tasks are abandoned through an explicit callback.

### Partitioning and aggregation

Partitioner splits an input list into at most the requested number of partitions. Partition sizes differ by no more than one record, and partitions preserve input order. The returned partition lists are immutable.

DataFlowEngine runs partition tasks on its own fixed-size ThreadPoolExecutor, with a bounded task queue. When the queue is full, a producer waits for capacity; this applies backpressure instead of allowing unbounded task accumulation.

Supported operations:
- COUNT: count records by grouping key.
- SUM: add numeric values per group.
- AVG: calculate the arithmetic mean per group.
- MIN and MAX: calculate the minimum or maximum numeric value per group.

For COUNT, the selected column is the default grouping key to preserve the original API behavior. For numeric operations, groupBy is optional; without it, all rows belong to the ALL group.

Numeric values use BigDecimal, and partition-local accumulators are merged using exact addition and comparison. AVG uses MathContext.DECIMAL128 for a bounded, high-precision decimal result. Blank or malformed numeric values fail the job.

## 4. Concurrency and resource boundaries

The application has two independently bounded queues:

| Layer | Default worker count | Default queue capacity | Admission behavior |
|---|---:|---:|---|
| Submitted jobs | 4 | 32 | Reject new jobs with HTTP 503 |
| Partition tasks | 8 (maximum) | 512 | Block producer until capacity is available |

The partition worker count is capped at eight by the current implementation. Both levels are configurable and require workload-specific tuning. These bounds apply to queued tasks, not to total process memory: the complete CSV dataset is currently materialized before partitioning.

## 5. Failure and shutdown semantics

- Invalid operation names and invalid request parameters are rejected.
- Missing columns and malformed numeric values fail the job with an actionable error.
- Queue saturation rejects admission; the rejected job is not retained.
- Temporary uploads are deleted after processing and when admission fails.
- Job state transitions are guarded to prevent duplicate completion or failure.
- Graceful shutdown drains accepted work; queued tasks that are discarded after timeout are marked failed and cleaned up.
- Unexpected API errors are logged internally while clients receive a generic message.

This is a reliability-focused learning project, not a production durability guarantee. A process crash can lose all job metadata and results held in memory.

## 6. Complexity

For N records, P partitions, and G distinct groups:
- Partitioning: O(N + P) time.
- Aggregation: O(N) expected work with hash-based group lookup.
- Partial aggregation state: up to O(G) keys per active partition.
- Final reduction: proportional to the number of partial group accumulators.
- Input storage: O(N), because the CSV reader currently materializes the dataset.

## 7. Test strategy

- Algorithm tests: partition balance, partition order, empty input, invalid partition count, count reduction, grouped numeric operations, and exact decimal behavior.
- Concurrency tests: queue saturation, rejected admission, bounded configuration, and forced-shutdown cleanup.
- Lifecycle tests: allowed and disallowed state transitions and terminal retention.
- HTTP integration tests: multipart upload, asynchronous execution, result retrieval, legacy COUNT compatibility, and invalid numeric input.

## 8. Planned evolution

1. Bounded streaming CSV ingestion and record-level memory budgets.
2. Durable job metadata and results behind a repository interface.
3. Cancellation, retry policies, and process-restart recovery.
4. Operational metrics for queue depth, active workers, duration, failures, and records processed.
5. Reproducible performance benchmarks and load tests.
6. Remote workers and multi-node coordination only when supported by a concrete deployment requirement.
