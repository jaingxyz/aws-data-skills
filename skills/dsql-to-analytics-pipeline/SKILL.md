---
name: dsql-to-analytics-pipeline
description: "Build a real-time analytics pipeline downstream of Aurora DSQL using the DSQL CDC stream. Covers consumer Lambda correctness, the append-only event log + ROW_NUMBER reconstruction pattern that absorbs unordered/duplicate delivery, Redshift Data API parameter-cap chunking, async statement polling, BisectBatchOnFunctionError, MaximumRecordAgeInSeconds, DLQ wiring, and the DSQL-preview gotcha that both INSERT and UPDATE arrive as op='c'. Triggers on phrases like: DSQL to analytics, DSQL to Redshift, analyze DSQL data, real-time DSQL analytics, DSQL CDC pipeline, Aurora DSQL change data capture, stream DSQL changes, replicate DSQL, DSQL to S3, DSQL to Iceberg, DSQL Kinesis Lambda, DSQL data warehouse."
license: Apache-2.0
metadata:
  tags: aws, aurora, dsql, aurora-dsql, cdc, change-data-capture, kinesis, lambda, redshift, redshift-serverless, redshift-data-api, analytics, streaming, append-only, super, json-parse, iceberg, s3-tables, firehose, pipeline
---

# Aurora DSQL to Analytics Pipeline Skill

This skill covers everything DOWNSTREAM of the DSQL CDC stream: how to wire
the stream into Kinesis, write a correct consumer Lambda, model events at
the analytics sink, and avoid the data-loss footguns that come with async
statement APIs and unordered delivery. Source-side DSQL concerns (DDL,
IAM auth, OCC, schema design) belong to the companion `dsql` skill.

## Why this skill exists

In the Aurora DSQL CDC public preview, both INSERT and UPDATE arrive as
`op='c'`. Code that expects a separate `'u'` op (the typical Debezium /
DMS shape) will silently treat every UPDATE as if no row existed, then
miss the latest state when downstream readers query a naive `MAX(event_id)`
view. The append-only + ROW_NUMBER-by-commit-timestamp pattern this skill
prescribes handles both cases correctly. There are several sibling
footguns (the Redshift Data API's 200-parameter cap, the async
`execute_statement` poll requirement, poison-record wedging) that cause
silent data loss when missed. Each one is called out below.

## When to use

- You have an Aurora DSQL cluster and want the change events available
  for analytics in Redshift Serverless, S3 Tables (Iceberg), or both.
- You are writing the consumer Lambda that reads from the DSQL CDC
  Kinesis stream and lands rows in an analytics sink.
- You are choosing between an in-place upsert and an append-only event
  log for the sink table shape.
- You need the operational gotchas (batch sizes, retries, DLQ, async
  poll) before you ship.

## When NOT to use

- You are working on the DSQL source cluster itself: DDL, IAM auth,
  OCC retries, multi-tenant isolation, query plans. Use the companion
  [`dsql`](../dsql/SKILL.md) skill.
- You are doing a one-shot bulk export from DSQL (not ongoing CDC).
  Use a `COPY ... TO` or pg_dump style flow; CDC is for change capture.
- You need EMR / Glue / Spark jobs inside the stream. This skill scopes
  to Lambda + Redshift Data API + Firehose.
- The source is not Aurora DSQL. The reconstruction pattern still
  applies, but several callouts (preview status, op='c'-only,
  `aws dsql create-stream`, the `dsql.amazonaws.com` trust principal)
  are DSQL-specific.

## Architecture in one paragraph

DSQL cluster -> DSQL CDC stream (preview) -> Kinesis Data Stream ->
Lambda event source mapping -> parameterized INSERTs into a Redshift
Serverless append-only `cdc_events` table (or Firehose with an Iceberg
destination + transform Lambda) -> per-source-table `*_current` views
that reconstruct current state via
`ROW_NUMBER() OVER (PARTITION BY record_id ORDER BY commit_timestamp DESC)`.

---

## Reference Files

Load these as needed. Each file leads with the non-obvious lessons; AWS
documentation paraphrasing is intentionally avoided.

### [references/cdc-stream-setup.md](references/cdc-stream-setup.md)

**When:** MUST load before creating the DSQL CDC stream or wiring the
DSQL-to-Kinesis IAM trust.
**Contains:** Public-preview status callout, why there is no
CloudFormation resource type yet, idempotent `aws dsql create-stream`
+ `get-stream` polling, the `dsql.amazonaws.com` trust principal with
`aws:SourceArn` matching `<cluster-arn>/stream/*` (NOT the bare cluster
ARN), the `c`-and-`d`-only-in-preview gotcha, the CDC record envelope
shape your consumer must parse.

### [references/append-only-pattern.md](references/append-only-pattern.md)

**When:** MUST load before designing the sink schema. This is the
single most important design decision in the pipeline.
**Contains:** Why append-only beats in-place upsert under unordered
duplicate delivery, the `cdc_events` DDL with DISTKEY/SORTKEY rationale,
the `*_current` view template with ROW_NUMBER, the tombstone pattern
(`operation <> 'd'`), schema-drift absorption via `SUPER`.

### [references/lambda-consumer.md](references/lambda-consumer.md)

**When:** MUST load before writing or reviewing the consumer Lambda.
Each pattern in here was earned by debugging a silent data-loss class.
**Contains:** Parameterized SQL only (named params via the Data API
`Parameters=` argument), the 200-parameter cap and ROWS_PER_CHUNK math,
async `describe_statement` polling with exponential backoff, re-raise on
failure to drive Kinesis retry, poison-record skipping vs raising,
Lambda timeout sizing, full handler skeleton.

### [references/event-source-mapping.yaml](references/event-source-mapping.yaml)

**When:** Load when authoring the EventSourceMapping CFN resource.
**Contains:** Complete CFN snippet with `BatchSize`,
`MaximumBatchingWindowInSeconds`, `MaximumRetryAttempts`,
`MaximumRecordAgeInSeconds`, `BisectBatchOnFunctionError`,
`DestinationConfig.OnFailure` -> SQS DLQ, plus the DLQ resource and
the placeholder Lambda body that fails loudly if real code is not
deployed.

### [references/sink-redshift.md](references/sink-redshift.md)

**When:** Load when the sink is Redshift Serverless.
**Contains:** IAM policy with the `redshift-data:*` resource-must-be-`*`
quirk, `redshift-serverless:GetCredentials` for IAM-auth into the
workgroup, `JSON_PARSE(:d)` to land SUPER, `event_data."col"::TYPE`
projection rules, `enable_case_sensitive_identifier` for mixed-case
JSON keys, PartiQL unnest pattern for SUPER arrays.

### [references/sink-s3-iceberg.md](references/sink-s3-iceberg.md)

**When:** Load when the sink (or additional sink) is S3 Tables /
Iceberg via Firehose.
**Contains:** Why a transform Lambda is required (Firehose maps
top-level JSON keys to Iceberg columns by name, so the CDC envelope
must be flattened), pointer to the sibling streaming-into-data-lake
skill that covers the Firehose-specific footguns end-to-end.

---

## Cross-references to complementary skills

Each row tells you which skill owns a topic so you do not duplicate the
guidance here.

| If you also need...                                    | Load this skill                                                                              |
| ------------------------------------------------------ | -------------------------------------------------------------------------------------------- |
| DSQL DDL, schema design, IAM auth, OCC, query plans    | [`dsql`](../dsql/SKILL.md) (same plugin)                                                     |
| Validate SQL is DSQL-compatible before running it      | [`dsql`](../dsql/SKILL.md) -> `dsql_lint` MCP tool                                           |
| Land CDC into S3 Tables (Iceberg) via Firehose         | `streaming-into-data-lake` (data-analytics plugin) for Firehose / Iceberg-specific footguns  |
| Query a Redshift Serverless external schema over S3    | The Redshift external-schema skill (lakehouse-style) in your data-analytics plugin          |
| Generic Kinesis / Lambda / SQS reference               | The serverless skill in `aws-core`                                                           |

---

## Quick start

1. **Stand up the source.** Use the [`dsql`](../dsql/SKILL.md) skill to
   create the cluster and validate the schema with `dsql_lint`.
2. **Create the CDC stream** ([cdc-stream-setup.md](references/cdc-stream-setup.md)).
   Kinesis stream + IAM role first, then `aws dsql create-stream`, then
   poll until `ACTIVE`. There is no CFN type yet; script it.
3. **Create the analytics sink schema**
   ([append-only-pattern.md](references/append-only-pattern.md)).
   `cdc_events` table + one `*_current` view per source table. Resist
   the urge to model in-place upserts.
4. **Deploy the consumer Lambda + EventSourceMapping**
   ([lambda-consumer.md](references/lambda-consumer.md),
   [event-source-mapping.yaml](references/event-source-mapping.yaml)).
   Set `BisectBatchOnFunctionError`, `MaximumRetryAttempts`,
   `MaximumRecordAgeInSeconds`, and a DLQ.
5. **Wire IAM** for the sink ([sink-redshift.md](references/sink-redshift.md)
   for Redshift; [sink-s3-iceberg.md](references/sink-s3-iceberg.md) for
   Iceberg). Remember: `redshift-data:*` only accepts `Resource: "*"`.
6. **Drive load and verify.** A trickle of inserts at the source should
   land as new rows in `cdc_events`; the `*_current` view should reflect
   the latest state per primary key within seconds.

---

## Common workflows

### Workflow 1: Add a new source table

The append-only sink absorbs new tables with NO DDL. The Lambda routes
all events to the same `cdc_events` table; only a new `*_current` view
is needed if downstream consumers want a typed projection.

1. Add the table to DSQL via `transact` (see the [`dsql`](../dsql/SKILL.md)
   skill).
2. Confirm new events appear in `cdc_events` with the new
   `source_table` value (`SELECT DISTINCT source_table FROM cdc_events`).
3. Author a `*_current` view per the template in
   [append-only-pattern.md](references/append-only-pattern.md).

### Workflow 2: Add a column to an existing source table

Same answer: zero sink-side DDL required for ingestion. The new column
appears in `event_data` automatically because `SUPER` is schema-flexible.
The matching `*_current` view needs a new
`event_data."new_col"::TYPE AS new_col` line if the column should be
projected.

### Workflow 3: Diagnose missing rows in `*_current`

The append-only pattern fails closed: if a row is missing from
`*_current`, check (in order):

1. Is the row in `cdc_events`? `SELECT * FROM cdc_events WHERE record_id
   = '...' ORDER BY commit_timestamp DESC LIMIT 5`. If absent, the
   event never reached Redshift -> check Lambda CloudWatch logs and the
   DLQ.
2. Is the latest event a `'d'` (delete tombstone)? The view filters
   those out.
3. Is the `commit_timestamp` parsing correct? See the
   `CAST(:ts AS BIGINT) / 1000.0` note in
   [lambda-consumer.md](references/lambda-consumer.md). A wrong divisor
   moves the timestamp by 3 orders of magnitude and the
   ROW_NUMBER ordering picks the wrong event.

### Workflow 4: Reset / replay

Truncate `cdc_events`, set the EventSourceMapping
`StartingPosition: TRIM_HORIZON`, redeploy. CDC retention on the Kinesis
stream defines the replay window (default 24 hours, max 365 days).

---

## Error scenarios

- **`execute_statement` succeeded but rows are missing.** You skipped
  the async poll. See [lambda-consumer.md](references/lambda-consumer.md)
  section 3.
- **`Number of parameters in statement exceeds maximum allowed (200)`.**
  `ROWS_PER_CHUNK` is set above `200 / params_per_row`. Lower it.
- **DSQL CDC stream stuck in `CREATING`.** The IAM trust policy almost
  certainly mismatches. Check `aws:SourceArn` is `ArnLike` against
  `<cluster-arn>/stream/*`, not the bare cluster ARN. See
  [cdc-stream-setup.md](references/cdc-stream-setup.md).
- **Lambda hits its timeout under load.** Either lower `BatchSize` or
  raise the function timeout. The math: `ceil(BatchSize / ROWS_PER_CHUNK)
  * STATEMENT_POLL_TIMEOUT_S` plus boto3 RTT.
- **DLQ is filling with batches.** Pull a sample, decode the
  `kinesis.data` Base64, inspect the payload. Common causes: source
  schema change the Lambda chokes on, malformed `ts_ms`, missing
  primary key.

---

## Additional resources

- [Aurora DSQL CDC (preview) docs](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/cdc.html)
- [Redshift Data API docs](https://docs.aws.amazon.com/redshift/latest/mgmt/data-api.html)
- [Lambda EventSourceMapping for Kinesis](https://docs.aws.amazon.com/lambda/latest/dg/with-kinesis.html)
- [SUPER type in Redshift](https://docs.aws.amazon.com/redshift/latest/dg/r_SUPER_type.html)
- [Aurora DSQL Documentation](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/)
