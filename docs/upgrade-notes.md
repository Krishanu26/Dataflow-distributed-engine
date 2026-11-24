# DataFlow upgrade notes

## Parallel analytics upgrade

This change extends the original count-by-value engine into a grouped numeric analytics engine while preserving the default COUNT API.

### Implemented in this change

- Added COUNT, SUM, AVG, MIN, and MAX aggregation operations.
- Added optional group-by support and a global ALL group for numeric aggregation.
- Added partition-local accumulators and a reduction stage.
- Added BigDecimal arithmetic for exact decimal sums and comparisons; AVG uses DECIMAL128.
- Added explicit failures for blank and malformed numeric values.
- Extended processing results with operation, source column, grouping column, and numeric aggregates.
- Preserved the original JobService submission overload and default COUNT behavior.
- Added unit coverage for grouped aggregations, global aggregation, malformed input, and compatibility.
- Added Spring MVC integration tests for multipart submission, asynchronous status polling, and result retrieval.
- Reworked the README and architecture documentation and added design-decision notes.

### Still not implemented

- Multi-node execution, remote workers, or distributed coordination.
- Durable job metadata, persistent result storage, or restart recovery.
- Streaming CSV ingestion or record-level memory budgets.
- Job cancellation, durable retries, or dead-letter handling.
- Authentication and authorization.
- Published benchmarks or production capacity claims.

### Verification

Run from the backend directory:

    mvn --batch-mode clean verify
