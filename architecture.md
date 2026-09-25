# Architecture and Design Decisions

## 1. Medallion-style layers

### Raw
Immutable source copy. No business transformations.

### Staging
Normalize names, country/tier values and dates. Add data-quality indicators and derived age/stale-member fields.

### Curated/current
One current member record per `member_id`.

### Country targets
Country-specific views or physical tables.

### Fact redemption
One row per redemption transaction.

## 2. Latest-record-wins

A member can move from USA to India, for example. The current-state table uses the latest valid record by:

1. enrollment date
2. flight date
3. ingestion timestamp

This makes the target deterministic when records have the same business date.

## 3. Why not physically duplicate every country record?

A single current member table plus country views avoids unnecessary duplication. If the assessment's downstream contract requires physical country tables, replace the views with `CREATE TABLE AS`/incremental MERGE targets. The logical routing remains the same.

## 4. Billion-record strategy

For very large daily volumes:

- ingest files in parallel
- use distributed Spark transformations
- partition by batch/feed date
- use columnar Parquet/Delta storage
- deduplicate on distributed keys
- avoid driver-side state
- use incremental MERGE instead of rebuilding all countries
- maintain checkpoint/audit metadata
- quarantine bad records instead of failing an entire batch
- monitor skew on `member_id`
- compact small files
- tune Spark AQE and shuffle partitions based on actual cluster metrics

## 5. Reliability

The pipeline should be idempotent:

```text
batch_id = source/feed date
```

A rerun of the same batch should replace/reconcile that batch's output rather than create duplicate records.

## 6. Observability

Recommended batch metrics:

- input rows
- valid rows
- rejected rows
- duplicate member IDs
- duplicate transaction IDs
- country counts
- processing duration
- records/sec
- null mandatory fields
- orphan redemptions

## 7. Security

- secrets stored in a secret manager
- least-privilege storage/database roles
- encryption at rest/in transit
- no production data in Git
- audit access to member/transaction data
- environment-specific configuration
