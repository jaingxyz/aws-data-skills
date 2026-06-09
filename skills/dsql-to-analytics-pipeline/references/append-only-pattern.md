# Append-Only Event Log + Reconstruction Pattern

This is the single most important design decision in the pipeline. Use
the append-only pattern. The rest of this page is the rationale and the
DDL.

## Why append-only

CDC delivery from Kinesis is **unordered and may duplicate**. Two facts
together kill naive `INSERT ... ON CONFLICT ... UPDATE` upserts at the
sink:

1. Records can arrive out of commit order. Within a Kinesis shard,
   ordering is preserved per partition key, but with multiple shards
   (or under retry) two events for the same row can land in the wrong
   order.
2. The same record can be delivered more than once. Lambda batch retry
   on transient errors, `BisectBatchOnFunctionError`, and producer
   retries all cause duplicates.

Append-only writes are idempotent under both conditions. Every CDC
event becomes a new row in `cdc_events`; current state is reconstructed
at read time by picking the latest `commit_timestamp` per primary key.
Late-arriving older events are discarded by the window function rather
than corrupting state via a re-applied UPDATE.

The DSQL preview adds one more reason: both INSERT and UPDATE arrive
as `op='c'`. With no `'u'` op to distinguish UPDATE from INSERT, an
in-place upsert cannot tell whether a row exists; the append-only
pattern sidesteps this entirely.

## `cdc_events` table DDL (Redshift Serverless)

```sql
CREATE TABLE IF NOT EXISTS cdc_events (
    event_id          BIGINT IDENTITY(1,1) PRIMARY KEY,
    source_table      VARCHAR(100) NOT NULL,
    operation         VARCHAR(10)  NOT NULL,    -- 'c' or 'd' (DSQL preview); 'u' on other sources
    record_id         VARCHAR(50)  NOT NULL,    -- source row primary key, stringified
    event_data        SUPER,                     -- full row payload (JSON)
    commit_timestamp  TIMESTAMP    NOT NULL,    -- source-side commit time
    ingested_at       TIMESTAMP    NOT NULL DEFAULT GETDATE()
)
DISTSTYLE KEY
DISTKEY (record_id)
SORTKEY (source_table, commit_timestamp);

-- Lambda's IAM-mapped DB user is auto-created on first GetCredentials.
-- Granting to PUBLIC keeps the demo simple. In production, grant to a
-- specific role created to match the IAM identity (typically
-- 'IAMR:<role-name>') and drop the PUBLIC grant.
GRANT INSERT, SELECT ON cdc_events TO PUBLIC;
```

Why each choice:

- **`BIGINT IDENTITY` event_id**: gives a stable secondary ordering for
  debugging. It does NOT participate in correctness. Correctness comes
  entirely from `commit_timestamp`.
- **`record_id VARCHAR(50)`**: the consumer stringifies all PKs (UUID,
  bigint, composite) so this single column absorbs any source-table PK
  shape.
- **`event_data SUPER`**: lets a single sink table absorb any source
  schema. New source tables and new columns require no DDL changes
  here. `SUPER` is queryable with `event_data."col"::TYPE` subscript +
  cast at projection time.
- **`DISTKEY(record_id)`**: colocates all events for a given row on
  the same compute slice, so the `ROW_NUMBER` window in current-state
  views runs locally rather than redistributing across slices.
- **`SORTKEY(source_table, commit_timestamp)`**: lets zone maps prune
  by table and by time on every per-table view query.

## Current-state view DDL

One view per source table. The pattern: pick the latest event per
`record_id`, treat `'d'` as a tombstone, project SUPER fields with
`::TYPE` casts.

```sql
CREATE OR REPLACE VIEW orders_current AS
SELECT
    record_id                            AS order_id,
    event_data."customer_id"::VARCHAR    AS customer_id,
    event_data."total_cents"::BIGINT     AS total_cents,
    event_data."status"::VARCHAR         AS status,
    event_data."payment_method"::VARCHAR AS payment_method,
    commit_timestamp                     AS last_change_at
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY record_id
               ORDER BY commit_timestamp DESC
           ) AS rn
    FROM cdc_events
    WHERE source_table = 'orders'
)
WHERE rn = 1
  AND operation <> 'd';

GRANT SELECT ON orders_current TO PUBLIC;
```

Key points:

- **Filter `source_table` BEFORE the window function.** The planner
  pushes the predicate down, but the explicit form keeps zone-map
  pruning effective and is easier to read.
- **`WHERE operation <> 'd'` is the tombstone.** Deletes still produce
  the latest event for that PK; the view simply hides them. If a
  downstream reader needs to know about deletes (e.g. an audit
  consumer), it can read `cdc_events` directly.
- **Cast at projection time.** Without `::VARCHAR`, `::BIGINT`, etc.,
  callers get raw `SUPER` values, which most BI tools cannot consume.
- **Quote field names.** `event_data."customer_id"`. Without quotes,
  Redshift lowercases the identifier before subscripting; if the JSON
  key is mixed-case, the lookup will fail.

## Querying current state

```sql
-- Per-row current state.
SELECT * FROM orders_current WHERE order_id = 'abc-123';

-- Aggregations work normally.
SELECT status, COUNT(*) FROM orders_current GROUP BY 1;

-- Time-travel: state as of an arbitrary timestamp.
SELECT
    record_id AS order_id,
    event_data."status"::VARCHAR AS status
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY record_id
               ORDER BY commit_timestamp DESC
           ) AS rn
    FROM cdc_events
    WHERE source_table = 'orders'
      AND commit_timestamp <= TIMESTAMP '2026-01-01 00:00:00'
)
WHERE rn = 1
  AND operation <> 'd';
```

Time-travel queries fall out of the append-only pattern for free; they
are not possible with an in-place upsert table.

## When NOT to use SUPER

If the source schema is fixed and small and you control both ends, plain
typed columns are simpler and faster to query. `SUPER`'s value comes
entirely from schema-drift absorption. If there is no drift, plain
columns avoid the small per-row SUPER overhead.

You can mix the patterns: keep `event_data SUPER` for the rare-evolving
fields and project the stable fields into typed columns at insert time.
This is rarely worth the extra Lambda complexity for a CDC pipeline of
this scale.

## Storage growth

The append-only table grows linearly with change volume. For most
operational workloads this is fine for years; Redshift Serverless
storage is cheap and the `SORTKEY(source_table, commit_timestamp)`
keeps queries against recent state fast even with billions of rows.

If volume justifies it, you can periodically:

- **Archive old events to S3 Tables (Iceberg).** See
  [sink-s3-iceberg.md](sink-s3-iceberg.md). The cold-path archive can
  use the same append-only shape and serve historical/audit queries
  while the hot path serves recent state.
- **Materialize `*_current` into a real table.** Redshift's
  `CREATE MATERIALIZED VIEW ... AUTO REFRESH YES` over the current-state
  view trades a few minutes of staleness for query speed at very large
  scale. Refresh cost is proportional to the new events since last
  refresh, not to the full history.

## What's next

- Write the consumer: [lambda-consumer.md](lambda-consumer.md).
- Wire IAM and finish the Redshift sink: [sink-redshift.md](sink-redshift.md).
