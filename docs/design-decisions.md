# Design decisions and engineering trade-offs

This document explains the core computer-science and systems decisions in DataFlow.

## 1. MapReduce-style execution

A job is decomposed into independent partition tasks:

1. **Partition:** divide the input records into balanced chunks.
2. **Map:** compute a partition-local accumulator for each group key.
3. **Reduce:** merge partial accumulators into the final result.

This keeps the hot record scan independent across partitions and confines shared-state mutation to the final reduction stage. Workers do not concurrently mutate one global result map.

For numeric operations, each accumulator contains a record count, decimal sum, minimum, and maximum. The reducer combines these statistics without re-reading source records. AVG is calculated from the merged sum and count.

## 2. Balanced immutable partitions

For N records and P requested partitions, the partitioner creates at most min(N, P) non-empty partitions for non-empty input. The first N mod P partitions receive one additional record, so partition sizes differ by at most one.

The partitioner preserves source order and returns immutable partition lists. This makes ownership explicit and prevents worker tasks from changing the structure of the input while another task is processing it.

## 3. Two levels of bounded concurrency

Job-level concurrency and partition-level concurrency solve different problems:

- The job executor limits how many independent uploads can be processed at once.
- The partition executor limits how many partition computations can run or wait at once.

Both use bounded queues. The job executor rejects excess work so the HTTP layer can communicate overload. The partition executor applies producer backpressure so one accepted job cannot enqueue an unbounded number of partition tasks.

A submitted job is not equivalent to a worker thread: a job may be queued, running, or waiting while its partition tasks are scheduled.

## 4. Why decimal arithmetic uses BigDecimal

CSV fields often contain decimal quantities such as prices. Binary floating-point cannot represent many decimal fractions exactly. BigDecimal keeps decimal addition and comparison exact for the parsed input values.

The average operation uses MathContext.DECIMAL128, which defines a finite precision for division. Consumers that require a fixed currency scale should apply their domain-specific rounding policy at the API boundary.

## 5. Explicit job state transitions

Job lifecycle transitions are synchronized and intentionally narrow:

- Only QUEUED can transition to RUNNING.
- Only RUNNING can transition to COMPLETED or FAILED.
- Only QUEUED can be marked failed when shutdown prevents execution.

This prevents a shutdown callback and a worker from independently completing the same job. Volatile fields and atomic record counters make job snapshots visible to status-polling requests.

## 6. Deterministic results and error behavior

Group results are sorted by key before they are returned. Numeric input is validated during partition processing. A malformed or blank numeric field fails the aggregation instead of silently changing the meaning of the dataset.

All partition futures are awaited before the aggregation call returns, including when a partition fails. This prevents the caller from abandoning still-running partition work after observing the first failed future.

## 7. Complexity and current memory trade-off

For N rows, P partitions, and G distinct keys:
- Partitioning is O(N + P).
- Each operation scans each row once: O(N) expected work.
- Each partition holds at most O(G) local accumulators.
- Reduction is proportional to the number of partial accumulators.
- The input dataset itself uses O(N) memory because the current CSV reader loads all rows before partitioning.

Bounded queues constrain queued work, but they do not make input storage streaming or provide a hard process-wide memory limit.

## 8. Why the current system is not described as distributed

The current engine uses local Java threads and in-memory job storage. It has no remote worker protocol, distributed scheduler, durable queue, shared state store, or cross-node failure recovery.

The architecture is intentionally a parallel processing baseline. A future distributed version would need to define worker registration, task ownership, idempotent execution, result commit semantics, retry behavior, and coordination under worker failure before claiming multi-node execution.
