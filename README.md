# DataFlow — Parallel Data Processing Engine

DataFlow is a Java 17 / Spring Boot learning project for CSV ingestion, deterministic partitioning, parallel aggregation, asynchronous job execution, and job monitoring.

> **Scope:** DataFlow currently runs as a single application process with a local worker pool. It demonstrates parallel processing and bounded job admission; it is not a multi-node distributed cluster and does not yet persist job state across restarts.

## Features

- Upload CSV files and select a column to aggregate.
- Split records into balanced, immutable partitions.
- Count values concurrently and merge partition-local results.
- Submit jobs asynchronously and poll their lifecycle.
- Use a dedicated, bounded application job executor rather than the common ForkJoinPool.
- Reject overload with HTTP `503 Service Unavailable` and a `Retry-After` response header.
- Enforce controlled job lifecycle transitions (`QUEUED → RUNNING → COMPLETED|FAILED`); queued work abandoned during forced shutdown is marked failed.
- Retain up to 1,000 most recent completed or failed jobs; active jobs are never evicted.
- Clean up temporary upload files after processing and when admission fails.
- Drain submitted jobs during graceful shutdown and mark queued jobs that cannot start as failed.
- Run automated unit tests and a GitHub Actions Java CI workflow.

## Architecture

```text
HTTP client
    |
    v
JobController --> JobService --> JobExecutionPool (bounded queue)
                                      |
                                      v
                               ProcessingJob
                                      |
                                      v
                              CsvDatasetReader
                                      |
                                      v
                                Partitioner
                                      |
                                      v
                         DataFlowEngine / worker pool
                                      |
                                      v
                          Aggregation result + JobStore
```

See [`docs/architecture.md`](docs/architecture.md) for component responsibilities, overload behavior, lifecycle rules, and current limitations.

## Requirements

- JDK 17 or later
- Maven 3.9+

## Run locally

```bash
cd backend
mvn clean verify
mvn spring-boot:run
```

The API starts at `http://localhost:8080`.

## Configuration

| Environment variable | Default | Purpose |
|---|---:|---|
| `DATAFLOW_JOB_WORKERS` | `4` | Number of concurrently executing submitted jobs |
| `DATAFLOW_JOB_QUEUE_CAPACITY` | `32` | Maximum number of waiting jobs |
| `DATAFLOW_JOB_TERMINAL_RETENTION_COUNT` | `1000` | Maximum number of completed or failed jobs retained in memory |
| `DATAFLOW_ENGINE_WORKERS` | `8` (capped at 8) | Worker threads for partition-level aggregation |
| `DATAFLOW_ENGINE_QUEUE_CAPACITY` | `512` | Maximum queued partition tasks; submitters experience backpressure when full |
| `DATAFLOW_MAX_FILE_SIZE` | `20MB` | Maximum uploaded file size |
| `DATAFLOW_MAX_REQUEST_SIZE` | `21MB` | Maximum multipart request size |

The job queue capacity applies to waiting jobs; active jobs are in addition to the waiting queue. When all job workers and waiting slots are occupied, new submissions are rejected with `503` rather than accepted without a bound. The separate partition-task queue is also bounded; when it fills, job threads wait for capacity.

## Try it

From the repository root, in another terminal:

```bash
curl -X POST http://localhost:8080/api/jobs \
  -F 'file=@sample-data/sales.csv' \
  -F 'column=city' \
  -F 'partitions=3'
```

The response contains a `jobId`. Poll it:

```bash
curl http://localhost:8080/api/jobs/JOB_ID_HERE
```

When the status is `COMPLETED`, retrieve the result:

```bash
curl http://localhost:8080/api/jobs/JOB_ID_HERE/result
```

Expected counts for the sample file: `Kolkata=3`, `Delhi=2`, `Mumbai=2`.

List in-memory jobs:

```bash
curl http://localhost:8080/api/jobs
```

## API

| Method | Endpoint | Purpose |
|---|---|---|
| `POST` | `/api/jobs` | Submit multipart CSV job (`file`, `column`, optional `partitions`) |
| `GET` | `/api/jobs` | List jobs known to this process |
| `GET` | `/api/jobs/{id}` | Read job status |
| `GET` | `/api/jobs/{id}/result` | Read successful result; returns `409` before completion |

A full job queue returns `503 Service Unavailable` with `Retry-After: 5`. Clients should retry with backoff rather than immediately resubmitting in a tight loop.
When the terminal-job retention limit is reached, the oldest completed or failed jobs are removed first; queued and running jobs are not evicted. Job state remains process-local and is cleared on restart.

## Testing and CI

Run locally:

```bash
cd backend
mvn clean verify
```

GitHub Actions runs the Maven verification build on pushes and pull requests targeting `main` or `master`.

Current tests cover partition behavior, parallel aggregation, bounded executor saturation, forced-shutdown cleanup, terminal-job retention, and job lifecycle transitions. Integration tests for the full HTTP/database path and performance benchmarks are future work.

## Known limitations and next steps

1. Job metadata and results are stored in memory and disappear when the process restarts.
2. The CSV reader currently loads the complete dataset into memory; it is not a streaming ingestion implementation.
3. Authentication, authorization, persistent storage, job cancellation, and durable retry scheduling are not implemented.
4. Both the application-level job queue and per-partition task queue are bounded. Their defaults are conservative starting points and must be load-tested and tuned before production use.
5. There is no multi-node coordination, message broker, or distributed scheduler.
6. Add integration tests, benchmark workloads, and operational metrics before making performance or production-readiness claims.

## Engineering principles

- Prefer explicit resource limits to unbounded background work.
- Make job state transitions predictable and thread-safe.
- Keep failures observable without returning internal stack traces to API clients.
- Test overload and failure paths, not only successful aggregation.
- Document measured results; do not invent throughput or latency claims.
