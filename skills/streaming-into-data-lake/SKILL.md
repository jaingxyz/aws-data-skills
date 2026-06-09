---
name: streaming-into-data-lake
description: >
  Stream rows continuously into Apache Iceberg tables on S3 Tables (or
  standard Iceberg on a general purpose bucket) using Amazon Data Firehose
  with IcebergDestinationConfiguration. Covers the Firehose stream, the
  optional record-shaping Lambda (ProcessingConfiguration), the Lake
  Formation grants required for Firehose to write through Glue, and the
  three-phase CloudFormation deploy that gates the stream on grants
  existing. Triggers on: stream into data lake, Firehose to S3 Tables,
  Firehose to Iceberg, real-time ingestion to Iceberg, Firehose
  IcebergDestinationConfiguration, stream Kinesis to Iceberg, near
  real-time CDC to data lake, continuous append to Iceberg. Do NOT use
  for: batch file imports (use ingesting-into-data-lake), creating the
  destination Iceberg table itself (use creating-data-lake-table), the
  one-time Lake Formation onboarding (use setting-up-lake-formation),
  finding tables by fuzzy name (use finding-data-lake-assets), running
  queries (use querying-data-lake), Glue connections (Firehose does not
  use them; do not invoke connecting-to-data-source), Kinesis to Lambda
  to Redshift Data API direct writes (not Iceberg; recommend a different
  workflow), or producer-side Kafka or MSK setup.
argument-hint: "[stream-name|source-arn] [--target s3-tables|iceberg]"
license: Apache-2.0
metadata:
  service: [firehose, s3tables, glue, lakeformation, lambda, kinesis]
  task: [deploy, debug]
  persona: [developer, data-engineer]
  workload: [data-analytics]
  tags: aws, firehose, iceberg, s3-tables, glue, lake-formation, streaming, data-lake, real-time, lambda, transform, cloudformation
---

<!--
A submission-flavored copy of this skill (with the extra deployment
metadata the AWS MCP server registry requires) lives in a separate
fork. The version here is the public learning copy.
-->

# Stream into Data Lake (Firehose to Iceberg)

Continuously land rows into an Iceberg table on S3 Tables or a general purpose bucket using Amazon Data Firehose. This skill assumes the destination Iceberg table already exists. Creating the table is owned by `creating-data-lake-table`. The first-time Lake Formation onboarding is owned by `setting-up-lake-formation`.

## Why this skill exists

Wiring Firehose to Iceberg through Lake Formation hits four failure modes that no AWS doc surfaces in one place: (1) Firehose silently routes records to the error bucket when the Lambda transformer's output column names do not exactly match the Iceberg schema; (2) Firehose drops Iceberg writes with `AccessDenied` until the Firehose role holds `DESCRIBE` and `SELECT` on the catalog and database plus `SELECT, INSERT, DELETE` on the table; (3) defining the Firehose stream and the Lake Formation grants in one CloudFormation template creates a race because CloudFormation evaluates IAM at stack-create time, before the grants apply; (4) re-deploying the stack with Firehose conditionals enabled but other condition flags omitted resets the unspecified flags to their template default of `false`, silently disabling resources that were already live.

This skill encodes the working sequence and the precise error signatures so the agent does not have to re-discover them.

## When to use

- The user wants Kinesis Data Streams, Direct PUT, or MSK to feed an existing Iceberg table on S3 Tables or a general purpose bucket.
- The user names "Firehose" or "Data Firehose" and the destination is Iceberg.
- The user reports records appearing in the Firehose error bucket prefix `errors/<stream>/iceberg-failed/`.
- The user reports a CloudFormation deploy stuck on a Firehose stream resource with an IAM or Lake Formation message.

## When NOT to use

- The destination table does not exist yet: hand off to `creating-data-lake-table`, then return here.
- Lake Formation is not yet enabled on the account or region: hand off to `setting-up-lake-formation`, then return here.
- The user wants Kinesis to Lambda to Redshift Data API direct writes (no Iceberg): this skill does not cover that. Recommend a Lambda or workflow-specific skill.
- The user wants to set up the producer side (Kafka, MSK cluster, application code that calls `PutRecord`): not in scope.
- The user wants batch file ingest from S3 or JDBC: use `ingesting-into-data-lake`.

## Phases

### Phase 1: Confirm preconditions

You MUST verify all four before touching Firehose. Stop and delegate if any fails.

1. Destination Iceberg table exists. Run `aws glue get-table --catalog-id <catalog> --database-name <db> --name <table> --region <region>`. If missing, delegate to `creating-data-lake-table`.
2. Lake Formation is enabled and the executing identity is a Data Lake Admin. Run `aws lakeformation get-data-lake-settings --region <region>` and confirm the caller ARN appears in `DataLakeAdmins`. If not, delegate to `setting-up-lake-formation`.
3. Source exists. For Kinesis Data Streams, run `aws kinesis describe-stream-summary --stream-name <name>`. For Direct PUT, no source check needed. For MSK, confirm the cluster ARN.
4. Region supports Firehose to Iceberg. CLI `--help` text is not regional and is unreliable as a capability signal. Confirm against the [Amazon Data Firehose regional services availability](https://docs.aws.amazon.com/general/latest/gr/fh.html) page or attempt a dry-run create-delivery-stream and inspect the error. The supported-region set has changed across releases; do not hard-code an allow-list here.

### Phase 2: Decide whether a transform Lambda is needed

Firehose writes records to Iceberg by parsing each record as JSON and matching top-level keys to Iceberg column names. A transform Lambda is required when ANY of the following is true:

- Source records are not JSON (Kinesis Data Streams payloads are often base64-encoded JSON or another format).
- Source field names do not match the Iceberg column names exactly (case-sensitive).
- The destination table has Iceberg-required metadata fields the source does not provide. Two are mandatory for `IcebergDestinationConfiguration`: a per-record `recordId` (string) and an `operation` (`insert`, `update`, or `delete`).
- Timestamps need to be normalized. Iceberg `timestamp` columns require microseconds since epoch (int64). Firehose does not coerce ISO-8601 strings.

If a transform Lambda is needed, read [`references/transform-lambda.md`](references/transform-lambda.md). It contains the generalized handler template and the column-name footgun that loses every record on a mismatch.

### Phase 3: Plan the deploy in two CloudFormation phases

Defining the Firehose stream, its IAM role, and the Lake Formation grants in a single stack creates a race: CloudFormation creates the stream while the grants are still propagating, and stream creation fails with `AccessDeniedException`.

You MUST split the deploy into two phases gated by stack parameters, with Lake Formation grants applied out-of-CFN between them. The pattern that works (matches the companion repo):

| Phase | What it creates | Gate parameters |
|---|---|---|
| A | Bucket, namespace, Firehose IAM role, transform Lambda (if any), error bucket. Stream held back. | `EnableFirehose=true`, `EnableFirehoseStream=false` |
|  | Apply Lake Formation grants out-of-CFN (`lf_grant` helper). Reapply transform Lambda code if updated. |  |
| B | Firehose delivery stream | `EnableFirehose=true`, `EnableFirehoseStream=true` |

Read [`references/three-phase-deploy.md`](references/three-phase-deploy.md) for the parameter-inheritance footgun and the historical reason this collapsed from three phases to two. Each `aws cloudformation deploy` call MUST pass every condition flag explicitly, including ones that did not change. Omitted parameters revert to the template default of `false` and silently disable resources the previous phase created.

### Phase 4: Author the Firehose stream resource

Read [`references/firehose-iceberg-config.md`](references/firehose-iceberg-config.md) for the full `IcebergDestinationConfiguration` block. Required pieces:

- `CatalogConfiguration.CatalogARN` pointing at the federated `s3tablescatalog/<bucket>` catalog (S3 Tables) or the account's default Glue catalog (standard Iceberg).
- `DestinationTableConfigurationList` entries naming `DestinationDatabaseName`, `DestinationTableName`. The `UniqueKeys` field is REQUIRED only when records carry per-record `operation` of `update` or `delete`. For append-only ingest (the typical CDC archive case), set `AppendOnly: true` on the destination configuration and omit `UniqueKeys` entirely; Iceberg's MERGE codepath is bypassed and INSERTs go through the faster append-write path.
- `S3Configuration` pointing at an error bucket. This is mandatory. Failed records land at `errors/<stream-name>/iceberg-failed/`.
- `RoleARN` for the Firehose role created in Phase 1.
- `ProcessingConfiguration` with `Type: Lambda` if Phase 2 produced a transform Lambda. The Firehose role MUST hold `lambda:InvokeFunction` on the Lambda ARN. Without this grant Firehose fails the entire batch with `Lambda.InvokeAccessDenied`.

#### Append-only vs MERGE-mode (which to choose)

| Mode | Use when | Trade-off |
|---|---|---|
| `AppendOnly: true` (recommended for CDC archives) | Each event is independently appended; downstream readers reconstruct current state via `ROW_NUMBER() OVER (PARTITION BY pk ORDER BY commit_ts DESC)`. The transform Lambda emits ONE output record per input record, with `recordId`, `result`, `data`. No `metadata.otfMetadata` is required because there is one static destination table. | Cold archive grows monotonically; downstream tiering (separate skill) prunes by time window. |
| `AppendOnly: false` with per-record `operation` and `UniqueKeys` | The destination table must reflect "current state" via Iceberg MERGE. Each record carries `operation` of `insert`, `update`, or `delete`. Transform Lambda output includes `metadata.otfMetadata.destinationDatabaseName/destinationTableName` and a per-record `operation` field. | MERGE is a heavier write; compaction matters more; not recommended for sustained high-throughput CDC. |

The transform-lambda template in [`references/transform-lambda.md`](references/transform-lambda.md) defaults to the append-only shape because that matches the typical CDC archive pattern. Switch to the MERGE shape only when downstream readers need an in-place authoritative table without window-function reconstruction.

### Phase 5: Apply Lake Formation grants

The Firehose role needs three grants. You MUST apply all three. Missing any one produces an opaque `AccessDenied` from Firehose. The exact permissions depend on whether you are in `AppendOnly: true` or MERGE mode (per Phase 4).

For `AppendOnly: true` (the recommended default):

| Resource | Permissions |
|---|---|
| Catalog | `DESCRIBE` |
| Database | `DESCRIBE`, `CREATE_TABLE`, `ALTER` |
| Table | `DESCRIBE`, `SELECT`, `INSERT`, `ALTER` |

For MERGE mode (`AppendOnly: false` with per-record updates and deletes), add `DELETE` to the Table permissions.

The companion deploy script's working set ([`07-deploy-iceberg.sh`](https://github.com/jaingxyz/dsql-redshift-cdc-pipeline/blob/main/infra/scripts/07-deploy-iceberg.sh)) uses the AppendOnly grants; if you are following the companion repo, do not add `DELETE` until you switch modes.

Read [`references/lake-formation-grants.md`](references/lake-formation-grants.md) for the `lf_grant` shell helper that aborts the deploy on real errors and treats "already granted" as success. The deploy script MUST run as a Data Lake Admin; otherwise grant-bestowing calls return `AccessDeniedException: Insufficient Lake Formation permission(s)` even when the IAM identity has admin privileges.

### Phase 6: Validate end to end

Run all four. Do not skip:

1. Push a test record to the source. For Direct PUT, the `Data` field MUST be base64-encoded:

   ```bash
   aws firehose put-record \
       --delivery-stream-name <name> \
       --record "Data=$(printf '%s' '{"recordId":"r1","commit_ts_ms":1717286400000,"row":{"id":"u-1"}}' | base64)"
   ```

   The CLI rejects literal `<base64-json>` placeholder strings as not-base64; encode the payload before invoking.
2. Wait at least 60 seconds (Firehose buffers).
3. Query the destination Iceberg table for the test row. Use `querying-data-lake`.
4. Inspect both CloudWatch metrics and the error bucket.

CloudWatch metrics that matter:

- `DeliveryToIceberg.SuccessfulRowCount` (must be > 0)
- `DeliveryToIceberg.FailedRowCount` (must be 0)
- `ExecuteProcessingFailure.Records` (must be 0; non-zero means the transform Lambda is failing)

Error bucket layout: `s3://<error-bucket>/errors/<stream-name>/iceberg-failed/<yyyy>/<mm>/<dd>/<hh>/<error-file>`. Each line is JSON with `errorCode`, `errorMessage`, and a base64-encoded `rawData`. Read [`references/error-bucket-decoding.md`](references/error-bucket-decoding.md) to decode and to map each `errorCode` value to a fix.

## Gotchas

- Firehose Iceberg destination column matching is case-sensitive and exact. A record with `record_id` will be discarded when the Iceberg column is `recordid`. The error bucket records this as `errorCode: Iceberg.MissingColumnWithinRecord`.
- Iceberg `timestamp` columns require microseconds since epoch. The transform Lambda MUST emit `int(epoch_seconds * 1_000_000)`, not `int(epoch_ms)`. Wrong scale silently writes timestamps in year 1970 or year 50000+.
- Re-deploying the CloudFormation stack with only one condition flag set on the CLI resets all other flags to `false` and tears down resources. Always pass every `--parameter-overrides` flag explicitly.
- `BufferingHints` minimum is 60 seconds for Iceberg destinations. Records do not appear in the table until the buffer flushes.
- The transform Lambda's response per record MUST set `result` to one of `Ok`, `Dropped`, or `ProcessingFailed`. Unknown values cause Firehose to mark every record in the batch as failed.
- The Firehose role MUST be assumable by `firehose.amazonaws.com`. A copy-paste from a Lambda role trust policy will pass IAM validation but fail at delivery time.
- Lake Formation grants applied via `aws lakeformation grant-permissions` do not appear in `aws iam simulate-principal-policy`. Verify with `aws lakeformation list-permissions --principal DataLakePrincipalIdentifier=<role-arn>` instead.

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `Iceberg.MissingColumnWithinRecord` | Transform Lambda emits a column name not in the Iceberg schema | Compare keys in transform output to `aws glue get-table` schema; rename in the Lambda |
| `Iceberg.GlueTableNotFound` | `DestinationDatabaseName` or `DestinationTableName` wrong, or wrong catalog ARN | Re-confirm via `aws glue get-table`; use the federated `s3tablescatalog/<bucket>` ARN for S3 Tables |
| `Iceberg.AccessDenied` | Lake Formation grants missing or Firehose role wrong | Re-run `lf_grant` for catalog, database, and table; confirm role trust policy |
| `Lambda.JsonProcessingException` | Transform Lambda returned non-JSON or wrong schema | Check Lambda CloudWatch logs; ensure response shape matches Firehose contract |
| `Lambda.MissingRecordId` | Transform Lambda did not echo back the `recordId` Firehose passed in | Each output record MUST include the input `recordId` unchanged |
| `Lambda.DuplicatedRecordId` | Transform Lambda emitted two output records with the same `recordId` | One input record produces exactly one output record with the original `recordId` |
| `Lambda.InvokeAccessDenied` | Firehose role lacks `lambda:InvokeFunction` on the transform Lambda | Add the grant to the Firehose role inline policy |
| `ExecuteProcessingFailure.Records > 0` and no error bucket entries | Transform Lambda is throwing before producing per-record results | Check Lambda logs; the entire batch failed before being assembled |

For the full decoding workflow including the base64 decode of `rawData`, read [`references/error-bucket-decoding.md`](references/error-bucket-decoding.md).

## Cross-references

| If you also need to... | Use |
|---|---|
| Create the destination Iceberg table | `creating-data-lake-table` |
| Onboard Lake Formation for the first time | `setting-up-lake-formation` |
| Resolve a fuzzy table name to `database.table` | `finding-data-lake-assets` |
| Run validation queries against the table | `querying-data-lake` |
| Batch file ingest from S3, JDBC, etc. | `ingesting-into-data-lake` |
| Set up Glue connections | `connecting-to-data-source` (NOT applicable to Firehose; Firehose does not use Glue connections) |

## References

- [`references/firehose-iceberg-config.md`](references/firehose-iceberg-config.md) - `IcebergDestinationConfiguration` deep dive, `ProcessingConfiguration`, `lambda:InvokeFunction` grant
- [`references/transform-lambda.md`](references/transform-lambda.md) - Generalized transform Lambda template, column-mapping footgun, microsecond timestamps
- [`references/three-phase-deploy.md`](references/three-phase-deploy.md) - `EnableFirehose` / `EnableFirehoseStream` pattern, parameter-inheritance footgun
- [`references/lake-formation-grants.md`](references/lake-formation-grants.md) - Catalog, database, table grants, `lf_grant` helper, Data Lake Admin requirement
- [`references/error-bucket-decoding.md`](references/error-bucket-decoding.md) - Error bucket layout, base64 decoding `rawData`, `errorCode` to fix mapping, CloudWatch metrics
