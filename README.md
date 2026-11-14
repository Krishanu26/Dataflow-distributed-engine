# DataFlow — Distributed Data Processing & Analytics Engine

A Java 17 / Spring Boot learning project that demonstrates CSV ingestion, deterministic data partitioning, parallel aggregation, asynchronous job execution, REST APIs, and job monitoring. It is an educational single-process engine: partitions execute concurrently in a local worker pool; this is **not** a multi-node distributed cluster.

## Current capabilities
- Upload a CSV file and choose a column.
- Split rows into balanced partitions.
- Count occurrences of each value in the selected column concurrently.
- Submit jobs asynchronously and poll job status.
- Retrieve completed aggregation results.
- Unit tests for partition correctness and parallel aggregation.

## Requirements
- JDK 17 or later
- Maven 3.9+

## Run
```bash
cd backend
mvn test
mvn spring-boot:run
```
The API starts at `http://localhost:8080`.

## Try it
In another terminal, from the repository root:
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
When status is `COMPLETED`, fetch results:
```bash
curl http://localhost:8080/api/jobs/JOB_ID_HERE/result
```
Expected counts for the sample file: Kolkata=3, Delhi=2, Mumbai=2.

List jobs:
```bash
curl http://localhost:8080/api/jobs
```

## API
| Method | Endpoint | Purpose |
|---|---|---|
| POST | `/api/jobs` | Submit multipart CSV job (`file`, `column`, optional `partitions`) |
| GET | `/api/jobs` | List jobs |
| GET | `/api/jobs/{id}` | Read job status |
| GET | `/api/jobs/{id}/result` | Read completed result |

## Design notes
- `Partitioner` balances input records across the requested number of partitions, with at most a one-record size difference.
- `DataFlowEngine` runs each partition on a bounded fixed thread pool and merges local `HashMap` counts.
- `JobStore` uses `ConcurrentHashMap` because jobs are read and updated by different request/worker threads.
- Job execution is asynchronous; job state is held in memory and is lost on restart in this first version.
- Uploaded files are copied to temporary storage and deleted after processing.

## Known limitations / next milestones
1. Add durable job metadata and results in PostgreSQL.
2. Add upload size limits, authentication, cancellation, and bounded job submission/back-pressure.
3. Add configurable operations (filter, map, sort, group-by, sum) and a typed query plan.
4. Add retry policy and per-partition failure reporting.
5. Add Docker Compose, integration tests, frontend dashboard, and benchmark suite.

Do not claim multi-node distribution, production readiness, or benchmark improvements until those features have actually been implemented and measured.
