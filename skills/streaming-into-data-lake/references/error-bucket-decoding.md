# Decoding the Firehose Error Bucket

When Firehose fails to write a record to Iceberg, the record lands in the error bucket configured under `S3Configuration.ErrorOutputPrefix`. This file is the operational guide for finding, decoding, and acting on those failures.

## Bucket layout

```
s3://<error-bucket>/errors/<stream-name>/iceberg-failed/<yyyy>/<mm>/<dd>/<hh>/<file>
```

- One file per Firehose flush window. Files are gzipped JSON Lines.
- Each line is one failed record.
- Time partitions use UTC.

For Lambda-side failures (the transform Lambda threw), records land at:

```
s3://<error-bucket>/errors/<stream-name>/processing-failed/<yyyy>/<mm>/<dd>/<hh>/<file>
```

These have a different `errorCode` set, decoded below.

## File line shape

```json
{
  "attemptsMade": 1,
  "arrivalTimestamp": 1717286400000,
  "errorCode": "Iceberg.MissingColumnWithinRecord",
  "errorMessage": "...",
  "attemptEndingTimestamp": 1717286460000,
  "rawData": "<base64-encoded-record-bytes>",
  "EventId": "...",
  "SubsequenceNumber": null,
  "ApproximateArrivalTimestamp": 1717286400000
}
```

`rawData` is the post-transform record (what Firehose tried to write to Iceberg) for `iceberg-failed`, or the pre-transform record for `processing-failed`. Always base64-decode before reading.

## Decoding rawData

```bash
aws s3 cp s3://<error-bucket>/errors/<stream>/iceberg-failed/2026/06/05/14/sample.gz - | \
  gunzip | \
  jq -r '.rawData' | \
  base64 -d
```

For programmatic processing:

```python
import base64, gzip, json
with gzip.open("sample.gz", "rt") as f:
    for line in f:
        rec = json.loads(line)
        decoded = base64.b64decode(rec["rawData"]).decode("utf-8")
        print(rec["errorCode"], decoded)
```

## errorCode catalog

### Iceberg-side failures (in `iceberg-failed/`)

| `errorCode` | Meaning | Fix |
|---|---|---|
| `Iceberg.MissingColumnWithinRecord` | The decoded record JSON has a top-level key that does not exist in the Iceberg schema, OR a required Iceberg column has no matching key | Compare keys in the decoded `rawData` to `aws glue get-table` schema. Update the transform Lambda to rename or add keys. |
| `Iceberg.GlueTableNotFound` | The `DestinationDatabaseName` or `DestinationTableName` in the stream config or per-record `metadata.otfMetadata` does not resolve in the catalog | Re-run `aws glue get-table` with the exact names. For S3 Tables, confirm `CatalogARN` is `arn:aws:glue:<region>:<account>:catalog/s3tablescatalog/<bucket>`. |
| `Iceberg.AccessDenied` | Lake Formation grants missing or wrong | Re-run the three `lf_grant` calls. Verify with `aws lakeformation list-permissions --principal DataLakePrincipalIdentifier=<role>`. |
| `Iceberg.SchemaTypeMismatch` | A record value does not match the column's Iceberg type (e.g., string for an int column, ISO-8601 string for a `timestamp` column) | Convert types in the transform Lambda. For `timestamp`, emit microseconds since epoch as int64. |
| `Iceberg.UniqueKeysNotFound` | `operation` is `update` or `delete` but the record does not include all columns named in the stream's `UniqueKeys` list | Update the transform Lambda to always emit every unique-key column for non-insert operations. |

### Lambda-side failures (in `processing-failed/`)

| `errorCode` | Meaning | Fix |
|---|---|---|
| `Lambda.JsonProcessingException` | Transform Lambda response was not valid JSON or did not match the Firehose response schema | Check Lambda CloudWatch logs. The output MUST be `{"records": [...]}` with each entry having `recordId`, `result`, and `data`. |
| `Lambda.MissingRecordId` | Lambda emitted an output record without echoing the input `recordId` | Update the Lambda to copy `record["recordId"]` from input to output verbatim. |
| `Lambda.DuplicatedRecordId` | Two output records share the same `recordId` | One input record must produce exactly one output record. Do not fan out in the Lambda. |
| `Lambda.InvokeAccessDenied` | Firehose role lacks `lambda:InvokeFunction` on the transform Lambda ARN | Add the grant; see `firehose-iceberg-config.md` under "lambda:InvokeFunction grant". |
| `Lambda.FunctionTimeout` | Lambda did not return within 60 seconds | Reduce work per invocation or increase Lambda memory. The Firehose timeout is fixed. |
| `Lambda.FunctionError` | Lambda raised an unhandled exception | Check Lambda CloudWatch logs for the stack trace. |

## CloudWatch metrics

Firehose publishes three metrics that signal Iceberg health. Alarm on these:

| Metric | Healthy value | Alarm |
|---|---|---|
| `DeliveryToIceberg.SuccessfulRowCount` | > 0 during traffic | < expected during traffic |
| `DeliveryToIceberg.FailedRowCount` | 0 | > 0 for 5 minutes |
| `ExecuteProcessingFailure.Records` | 0 | > 0 for 5 minutes |

`ExecuteProcessingFailure.Records` increments when the entire transform Lambda batch fails (e.g., `Lambda.InvokeAccessDenied`). When this is non-zero AND the error bucket has no `processing-failed/` entries, the Lambda is failing before it can produce per-record results. The Lambda execution role or the Firehose role's `lambda:InvokeFunction` grant is the most common cause.

`DeliveryToIceberg.FailedRowCount` increments when records reach Iceberg but the write fails. The error bucket `iceberg-failed/` prefix has the corresponding records.

## Operational triage flow

1. CloudWatch metric `DeliveryToIceberg.FailedRowCount` > 0 fires.
2. List the most recent error file: `aws s3 ls s3://<error-bucket>/errors/<stream>/iceberg-failed/ --recursive | tail -1`.
3. Download and decode one line.
4. Map `errorCode` to the table above.
5. Apply the fix in the transform Lambda or grants.
6. Re-deploy. Records that landed in the error bucket are not auto-replayed; resend from source if needed.

## Replay strategy

Firehose does not retry records from the error bucket. To replay:

- For Direct PUT sources: re-emit from the upstream system if available.
- For Kinesis Data Streams: rewind the consumer's checkpoint to before the failed batch. The Firehose-side checkpoint is not directly settable; the practical option is to create a new Firehose stream consuming the same Kinesis stream from `TRIM_HORIZON`.
- For MSK: same pattern as Kinesis Data Streams.

For idempotent Iceberg writes, ensure each record carries a stable `record_id` and the destination table has a `UniqueKeys` config so re-delivery does not duplicate rows.
