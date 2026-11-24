<div align="center">

# DataFlow
### Bounded Parallel Data Processing Engine

**A Java 17 / Spring Boot MapReduce-style engine for asynchronous, partitioned CSV analytics.**

[![Java CI](https://github.com/Krishanu26/Dataflow-distributed-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/Krishanu26/Dataflow-distributed-engine/actions/workflows/ci.yml)
![Java](https://img.shields.io/badge/Java-17-orange?logo=openjdk)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.4.2-6DB33F?logo=springboot)
![Maven](https://img.shields.io/badge/build-Maven-C71A36?logo=apachemaven)
![License](https://img.shields.io/badge/license-MIT-blue)

</div>

DataFlow accepts CSV processing jobs over a REST API, partitions datasets, runs partition-level aggregations concurrently, and combines partial results into deterministic output. It is designed around explicit resource bounds, backpressure, thread-safe job state transitions, and predictable overload behavior.

> **Architecture scope:** DataFlow is currently a single-process parallel engine. It uses multiple local worker threads; it does not yet coordinate execution across machines or provide durable job storage.

## What it demonstrates

- **Parallel MapReduce-style execution:** balanced data partitioning, partition-local aggregation, and deterministic reduction.
- **Five aggregation operations:** **COUNT**, **SUM**, **AVG**, **MIN**, and **MAX**.
- **Grouped analytics:** aggregate numeric columns by a categorical column, or across the entire dataset.
- **Exact decimal arithmetic:** BigDecimal avoids binary floating-point rounding in financial-style CSV values.
- **Two-level bounded concurrency:** a bounded application job executor and a separate bounded partition worker pool.
- **Backpressure and overload control:** reject excess job submissions with HTTP 503 and Retry-After; partition producers wait for available queue capacity.
- **Asynchronous job lifecycle:** QUEUED → RUNNING → COMPLETED | FAILED, with guarded transitions and status polling.
- **Graceful shutdown:** drain accepted work, fail queued work that cannot start, and clean up temporary uploads.
- **Bounded in-memory retention:** evict old terminal jobs without evicting queued or running jobs.
- **Automated quality checks:** unit tests for algorithms and concurrency plus Spring MVC integration tests for the end-to-end job flow.

## System architecture

    REST clients
         |
         v
    +---------------------+
    |     JobController   |
    +----------+----------+
               |
               v
    +---------------------+
    |      JobService     |
    | validate / lifecycle|
    +----------+----------+
               |
               v
    +---------------------+
    |   JobExecutionPool  |
    | fixed workers       |
    | bounded job queue   |
    +----------+----------+
               |
               v
    +---------------------+
    |   CsvDatasetReader  |
    +----------+----------+
               |
               v
    +---------------------+
    |     Partitioner     |
    | balanced immutable  |
    | partitions          |
    +----------+----------+
               |
      submit partition tasks
               |
               v
    +---------------------+
    |    DataFlowEngine   |
    | bounded worker pool |
    +----------+----------+
               |
        +------+------+
        |             |
        v             v
    Partition 1   Partition N
    local reduce  local reduce
        |             |
        +------+------+
               |
               v
       Merge partial results
               |
               v
    +---------------------+
    |  ProcessingResult   |
    |     JobStore        |
    +---------------------+

See [Architecture](docs/architecture.md) and [Design Decisions](docs/design-decisions.md) for implementation details, failure semantics, and trade-offs.

## Technology stack

| Area | Technology |
|---|---|
| Language | Java 17 |
| API and dependency injection | Spring Boot 3.4.2, Spring Web |
| CSV parsing | Apache Commons CSV 1.13.0 |
| Concurrency | ThreadPoolExecutor, ArrayBlockingQueue, CompletableFuture |
| Numeric correctness | BigDecimal, MathContext.DECIMAL128 |
| Build and tests | Maven, JUnit 5, Spring Boot Test, MockMvc |
| CI | GitHub Actions |

## Run locally

**Requirements:** JDK 17+ and Maven 3.9+.

    git clone https://github.com/Krishanu26/Dataflow-distributed-engine.git
    cd Dataflow-distributed-engine/backend
    mvn clean verify
    mvn spring-boot:run

The API listens on http://localhost:8080.

## API

All processing jobs use multipart CSV uploads. Submission is asynchronous and returns HTTP 202 Accepted with a jobId.

### 1. Count values

This preserves the original behavior: count occurrences of each value in the selected column.

    curl -X POST http://localhost:8080/api/jobs \
      -F 'file=@../sample-data/sales.csv' \
      -F 'column=city' \
      -F 'partitions=3'

### 2. Grouped numeric aggregation

Calculate the sum of amount for each city:

    curl -X POST http://localhost:8080/api/jobs \
      -F 'file=@../sample-data/sales.csv' \
      -F 'column=amount' \
      -F 'operation=SUM' \
      -F 'groupBy=city' \
      -F 'partitions=4'

Supported operations:

| Operation | Behavior | Numeric column required |
|---|---|---|
| COUNT | Count records per value; defaults to grouping by column | No |
| SUM | Sum values per group | Yes |
| AVG | Calculate arithmetic mean per group | Yes |
| MIN | Find the minimum value per group | Yes |
| MAX | Find the maximum value per group | Yes |

For numeric operations, omit groupBy to aggregate the entire dataset into the ALL group. Numeric input is parsed as BigDecimal; blank or malformed values fail the job instead of being silently skipped.

### 3. Poll job status

    curl http://localhost:8080/api/jobs/JOB_ID

Possible states: QUEUED, RUNNING, COMPLETED, FAILED.

### 4. Retrieve the result

    curl http://localhost:8080/api/jobs/JOB_ID/result

The result includes the operation and selected columns, record count, partition count, execution duration, and either countsByValue or aggregatesByGroup.

### 5. List known jobs

    curl http://localhost:8080/api/jobs

### API reference

| Method | Endpoint | Description |
|---|---|---|
| POST | /api/jobs | Submit a CSV aggregation job |
| GET | /api/jobs | List jobs retained by this process |
| GET | /api/jobs/{id} | Retrieve job state and progress |
| GET | /api/jobs/{id}/result | Retrieve the completed result; 409 until successful completion |

When the application job queue is full, submission returns 503 Service Unavailable and Retry-After: 5. Clients should retry with exponential backoff.

## Configuration

All settings can be supplied as environment variables.

| Environment variable | Default | Purpose |
|---|---:|---|
| DATAFLOW_JOB_WORKERS | 4 | Concurrent submitted jobs |
| DATAFLOW_JOB_QUEUE_CAPACITY | 32 | Waiting job capacity |
| DATAFLOW_JOB_TERMINAL_RETENTION_COUNT | 1000 | Maximum retained completed or failed jobs |
| DATAFLOW_ENGINE_WORKERS | 8 | Partition worker threads; implementation caps this at 8 |
| DATAFLOW_ENGINE_QUEUE_CAPACITY | 512 | Bounded partition task queue |
| DATAFLOW_MAX_FILE_SIZE | 20MB | Maximum CSV upload size |
| DATAFLOW_MAX_REQUEST_SIZE | 21MB | Maximum multipart request size |

The job queue capacity is in addition to active job workers. The partition queue is independently bounded. When it is full, a submitting job thread waits for capacity rather than creating unbounded queued work.

## Correctness and testing

Run the full verification suite:

    cd backend
    mvn --batch-mode clean verify

Coverage includes:
- Balanced partitioning, empty input, and invalid partition counts.
- Parallel count reduction and grouped SUM, AVG, MIN, and MAX.
- Exact decimal aggregation and rejection of malformed numeric values.
- Bounded executor saturation, forced-shutdown cleanup, and job lifecycle transitions.
- End-to-end multipart submission, asynchronous polling, numeric result retrieval, and backward-compatible default COUNT behavior.

GitHub Actions runs the Maven verification build on pushes and pull requests targeting the configured branches.

## Complexity and resource model

Let N be the number of CSV records, P the number of partitions, and G the number of distinct group keys.

- Partitioning performs one pass over the record list: O(N + P) time.
- Each aggregation scans every record once: O(N) total work, excluding map operations.
- Partial aggregation state is proportional to the number of distinct groups: O(G) per active partition.
- Reduction merges partition-local accumulators; the final output contains at most G entries.
- Job admission and partition task queues are bounded to make queued work explicit.

The CSV reader currently materializes all parsed records in memory. Therefore total memory usage is also O(N) for the input dataset, in addition to aggregation state. Streaming ingestion and explicit record-level memory budgets remain future work.

## Current limitations

- Single-process execution; no remote workers, distributed scheduler, or cross-node coordination.
- Job metadata and results are in memory and are lost on process restart.
- CSV parsing loads the complete file into memory.
- No authentication, authorization, durable retries, or job cancellation.
- No published throughput or latency claims; representative benchmarks and load tests are still needed before production sizing.

## Roadmap

1. Stream CSV records through bounded partition batches.
2. Add durable job and result storage behind a repository abstraction.
3. Add cancellation, retry policies, and recovery after process restart.
4. Expose operational metrics for queue depth, active workers, job duration, failures, and records processed.
5. Add reproducible benchmarks and load tests.
6. Explore remote worker coordination only when a multi-node deployment requirement exists.

## Engineering goals

DataFlow prioritizes explicit resource limits, deterministic reduction, thread-safe state transitions, actionable failures, and testable concurrency behavior. Performance and production-readiness claims should be supported by reproducible measurements rather than assumptions.
