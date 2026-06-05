---
name: firehose-iceberg-pipeline
description: Build an Amazon Data Firehose delivery stream that writes to Apache Iceberg tables on S3 Tables via IcebergDestinationConfiguration. Covers the column-shape footgun (records land in the error bucket with Iceberg.MissingColumnWithinRecord unless a transform Lambda reshapes them), the microsecond timestamp requirement, the three-phase deploy pattern that works around Firehose's synchronous glue:GetTable validation at create time, parameter-inheritance traps in `aws cloudformation deploy`, Lake Formation grant ordering, and how to decode the error bucket. Use when you see "Firehose to Iceberg", "Firehose to S3 Tables", "IcebergDestinationConfiguration", "Firehose data transformation Lambda", "Firehose error bucket", "Iceberg.MissingColumnWithinRecord", "MissingColumnWithinRecord", or any time someone is wiring a Kinesis Data Stream / Direct PUT source to Iceberg tables and producer record shape does not match the Iceberg column layout.
---

# firehose-iceberg-pipeline

Specialty skill for one architecture choice: **Amazon Data Firehose with
`IcebergDestinationConfiguration` writing to Apache Iceberg tables on
S3 Tables**, with optional reshape via a transform Lambda.

## When to use

Load this skill when:

- You are setting up `AWS::KinesisFirehose::DeliveryStream` with
  `IcebergDestinationConfiguration` (CFN), or the equivalent
  `IcebergDestinationConfiguration` block in the
  `firehose:CreateDeliveryStream` API.
- The Firehose source is either a Kinesis Data Stream
  (`KinesisStreamAsSource`) or `DirectPut`.
- The destination is an Iceberg table managed by Amazon S3 Tables
  (`AWS::S3Tables::TableBucket` + namespace + table), exposed via the
  bucket-nested `s3tablescatalog/<bucket>` Glue catalog.
- You see `Iceberg.MissingColumnWithinRecord` in the error bucket and
  every record is failing.
- You hit `Role ... is not authorized to perform: glue:GetTable for the
  given table or the table does not exist` during stack create.

## When NOT to use

- The Iceberg table lives in a regular S3 bucket with the standard Glue
  catalog (not S3 Tables). The IAM, ARN, and Lake Formation patterns
  here assume the bucket-nested `s3tablescatalog` federation; a generic
  Iceberg-in-S3 setup uses the default catalog and different grants.
- You are reading the Iceberg table from Amazon Redshift Serverless via
  `CREATE EXTERNAL SCHEMA`. That belongs in `lakehouse-redshift`. This
  skill stops at "Firehose is delivering rows to the Iceberg table".
- You are streaming Aurora DSQL CDC into Kinesis. That is upstream of
  this skill; see `cdc-streaming-pipeline` for the producer side.
- You want EMR/Glue/Athena to write to Iceberg. Different code paths;
  this skill is Firehose only.

## THE BIGGEST FOOTGUN: column-name mapping

`IcebergDestinationConfiguration` takes the **top-level JSON keys** of
each source record and maps them to Iceberg columns **by name**. Any
key that does not exist as a column gets dropped. Any required column
that has no matching key causes the entire record to fail.

If your producer record shape does not exactly match the Iceberg column
names, **100% of records land in the error bucket** with:

```
errorCode: "Iceberg.MissingColumnWithinRecord"
errorMessage: "One or more columns are missing in the record"
```

Firehose does not surface this as a stack-create error. The stream
deploys clean, the metrics show records flowing in, and the destination
table stays empty. You only notice when you run
`SELECT COUNT(*) FROM the_table` and get zero.

### Concrete example

A Kinesis stream carries records like:

```json
{"op": "c", "after": {"id": 42, "amount": 19.99}, "ts_ms": 1780615647656}
```

The Iceberg table has columns
`source_table, operation, record_id, event_data, commit_timestamp,
ingested_at`.

None of the top-level keys (`op`, `after`, `ts_ms`) match a column
name. Firehose rejects every record. The fix is a transform Lambda
wired in via `ProcessingConfiguration` that emits the column shape:

```json
{"source_table": "orders", "operation": "c", "record_id": "42",
 "event_data": "{\"id\":42,\"amount\":19.99}",
 "commit_timestamp": 1780615647656000, "ingested_at": 1780615647700000}
```

### Transform Lambda template

```python
"""Firehose transform: reshape source records to Iceberg column shape."""
import base64
import binascii
import json
import logging
import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def reshape(payload: dict, ingested_us: int):
    """Map a raw producer payload into the Iceberg column layout.

    Return None to drop the record (Firehose will mark it Dropped, not
    error-bucket it). Return a dict matching the Iceberg columns
    exactly. Replace this body to fit your schema.
    """
    # Example: a CDC envelope with op + after/before sub-objects.
    op = payload.get("op")
    if op == "c":
        row = payload.get("after")
    elif op == "d":
        row = payload.get("before")
    else:
        return None
    if not row:
        return None

    record_id = row.get("id")
    ts_ms = payload.get("ts_ms")
    if record_id is None or ts_ms is None:
        return None

    return {
        "source_table": payload.get("source", {}).get("table", "unknown"),
        "operation": op,
        "record_id": str(record_id),
        "event_data": json.dumps(row),
        # Iceberg timestamp columns expect MICROSECONDS. ts_ms is ms,
        # so multiply by 1000. See "Iceberg timestamp gotcha" below.
        "commit_timestamp": int(ts_ms) * 1000,
        "ingested_at": ingested_us,
    }


def lambda_handler(event, context):
    ingested_us = time.time_ns() // 1000
    out = []
    for record in event.get("records", []):
        rid = record["recordId"]
        try:
            raw = base64.b64decode(record["data"])
            payload = json.loads(raw)
        except (KeyError, TypeError, ValueError, binascii.Error) as e:
            logger.warning("Dropping undecodable record %s: %s", rid, e)
            out.append({"recordId": rid, "result": "Dropped"})
            continue

        reshaped = reshape(payload, ingested_us)
        if reshaped is None:
            out.append({"recordId": rid, "result": "Dropped"})
            continue

        # Firehose expects base64-encoded UTF-8 JSON. The trailing
        # newline is harmless and matches the Firehose SDK convention.
        data = base64.b64encode(
            (json.dumps(reshaped) + "\n").encode("utf-8")
        ).decode("utf-8")
        out.append({"recordId": rid, "result": "Ok", "data": data})
    return {"records": out}
```

Output contract reminders:

- `result="Ok"` plus `data`: delivered to Iceberg.
- `result="Dropped"`: Firehose neither delivers it nor sends it to the
  error bucket. Use for poison or unparseable records you do not want
  to flood the error bucket with.
- `result="ProcessingFailed"`: Firehose routes to the error bucket
  under a `processing-failed/` prefix and also retries per the stream's
  retry policy. Use for transient issues you actually want to inspect.

### ProcessingConfiguration CFN snippet

```yaml
FirehoseStream:
  Type: AWS::KinesisFirehose::DeliveryStream
  Properties:
    DeliveryStreamName: my-iceberg-stream
    DeliveryStreamType: KinesisStreamAsSource
    KinesisStreamSourceConfiguration:
      KinesisStreamARN: !ImportValue my-kinesis-stream-arn
      RoleARN: !GetAtt FirehoseRole.Arn
    IcebergDestinationConfiguration:
      RoleARN: !GetAtt FirehoseRole.Arn
      CatalogConfiguration:
        # Bucket-nested catalog: arn:aws:glue:<region>:<account>:catalog/s3tablescatalog/<bucket>
        CatalogArn: !Sub
          - "arn:aws:glue:${AWS::Region}:${AWS::AccountId}:catalog/s3tablescatalog/${BucketName}"
          - BucketName: !Select [1, !Split ["/", !GetAtt TableBucket.TableBucketARN]]
      AppendOnly: true
      DestinationTableConfigurationList:
        - DestinationDatabaseName: cdc           # = S3 Tables namespace
          DestinationTableName: cdc_events_archive
          S3ErrorOutputPrefix: "errors/cdc_events_archive/"
      ProcessingConfiguration:
        Enabled: true
        Processors:
          - Type: Lambda
            Parameters:
              - ParameterName: LambdaArn
                ParameterValue: !GetAtt TransformLambda.Arn
              # Keep payload well under Lambda's 6 MB synchronous limit.
              - ParameterName: BufferSizeInMBs
                ParameterValue: "3"
              - ParameterName: BufferIntervalInSeconds
                ParameterValue: "60"
      BufferingHints:
        IntervalInSeconds: 60
        SizeInMBs: 64
      RetryOptions:
        DurationInSeconds: 300
      S3Configuration:
        BucketARN: !GetAtt FirehoseErrorBucket.Arn
        RoleARN: !GetAtt FirehoseRole.Arn
        Prefix: "errors/"
        ErrorOutputPrefix: "errors/!{firehose:error-output-type}/"
      s3BackupMode: FailedDataOnly
      CloudWatchLoggingOptions:
        Enabled: true
        LogGroupName: !Ref FirehoseLogGroup
        LogStreamName: iceberg-delivery
```

### Firehose role: lambda:InvokeFunction grant

`ProcessingConfiguration` requires the FIREHOSE role (not the Lambda
role) to be allowed to invoke the function. Easy to forget. Add this
inline policy on the Firehose role:

```yaml
- PolicyName: InvokeTransform
  PolicyDocument:
    Version: "2012-10-17"
    Statement:
      - Effect: Allow
        Action:
          - lambda:InvokeFunction
          - lambda:GetFunctionConfiguration
        Resource: !GetAtt TransformLambda.Arn
```

If you forget this, Firehose health metrics show
`DeliveryToLambdaFailedRecords > 0` and the CloudWatch log stream for
the delivery shows `is not authorized to perform: lambda:InvokeFunction`.

## Iceberg timestamp gotcha: MICROSECONDS, not milliseconds

Iceberg `timestamp` and `timestamptz` columns expect **microseconds
since epoch** in the JSON Firehose receives. The Firehose docs mention
this in one line under "supported data types" and it is easy to miss.

If your source carries milliseconds, multiply by 1000:

```python
"commit_timestamp": int(ts_ms) * 1000,   # ms -> us
```

If your source carries seconds, multiply by 1,000,000.

If your source carries an ISO-8601 string, parse and convert:

```python
from datetime import datetime, timezone
dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
us = int(dt.replace(tzinfo=timezone.utc).timestamp() * 1_000_000)
```

Symptom of getting this wrong: rows land successfully but
`commit_timestamp` reads as the year 1970 or some absurd far-future
date. Filter `WHERE commit_timestamp > NOW() - INTERVAL '1 day'`
returns nothing even though rows are present.

## Three-phase deploy pattern

`AWS::KinesisFirehose::DeliveryStream` with
`IcebergDestinationConfiguration` validates the destination synchronously
at stack create time. It calls `glue:GetTable` against the
bucket-nested catalog and fails the resource if either:

- the destination Iceberg table does not yet exist, OR
- the Firehose role does not have Lake Formation permission to read it.

So you cannot deploy bucket + namespace + table + Firehose in one CFN
shot. Use a **three-phase** pattern with two boolean parameters:

```yaml
Parameters:
  EnableFirehose:
    Type: String
    Default: "false"
    AllowedValues: ["true", "false"]
    Description: Provision Firehose role, log group, error bucket, transform Lambda.
  EnableFirehoseStream:
    Type: String
    Default: "false"
    AllowedValues: ["true", "false"]
    Description: Provision the actual delivery stream. Requires EnableFirehose=true and LF grants.

Conditions:
  WantFirehose:       !Equals [!Ref EnableFirehose, "true"]
  WantFirehoseStream: !Equals [!Ref EnableFirehoseStream, "true"]
```

Apply `Condition: WantFirehose` to the Firehose role / log group / error
bucket / transform Lambda. Apply `Condition: WantFirehoseStream` to the
delivery stream itself.

### Phase A: bucket + namespace + supporting resources

```bash
aws cloudformation deploy \
    --stack-name "${ICEBERG_STACK_NAME}" \
    --template-file cloudformation-iceberg.yaml \
    --parameter-overrides \
        "ProjectName=${PROJECT_NAME}" \
        "EnableFirehose=true" \
        "EnableFirehoseStream=false" \
    --capabilities CAPABILITY_NAMED_IAM \
    --no-fail-on-empty-changeset
```

This creates the table bucket, the namespace, the Firehose role, the
transform Lambda, the error bucket, and log groups. **No stream yet.**

### Out-of-band: create the Iceberg table

`AWS::S3Tables::Table` is unreliable in CFN due to namespace propagation
lag (5+ minutes). Create with `aws s3tables create-table` from a script
with bash retries. See `reference/iceberg-table-create.md` for a full
working snippet.

### Out-of-band: grant Lake Formation perms to the Firehose role

The Firehose role needs LF DESCRIBE on the bucket-nested catalog,
DESCRIBE / CREATE_TABLE / ALTER on the database (namespace), and ALL on
the table. Phase B's stream-create fails immediately if any are
missing, with a misleading `Role ... is not authorized to perform:
glue:GetTable` error.

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
CATALOG_ID_NESTED="${ACCOUNT_ID}:s3tablescatalog/${BUCKET_NAME}"
FIREHOSE_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${PROJECT_NAME}-iceberg-firehose-role"

aws lakeformation grant-permissions \
    --principal "DataLakePrincipalIdentifier=${FIREHOSE_ROLE_ARN}" \
    --resource "{\"Catalog\":{\"Id\":\"${CATALOG_ID_NESTED}\"}}" \
    --permissions DESCRIBE
aws lakeformation grant-permissions \
    --principal "DataLakePrincipalIdentifier=${FIREHOSE_ROLE_ARN}" \
    --resource "{\"Database\":{\"CatalogId\":\"${CATALOG_ID_NESTED}\",\"Name\":\"cdc\"}}" \
    --permissions DESCRIBE CREATE_TABLE ALTER
aws lakeformation grant-permissions \
    --principal "DataLakePrincipalIdentifier=${FIREHOSE_ROLE_ARN}" \
    --resource "{\"Table\":{\"CatalogId\":\"${CATALOG_ID_NESTED}\",\"DatabaseName\":\"cdc\",\"TableWildcard\":{}}}" \
    --permissions ALL
```

### Out-of-band: deploy the real transform Lambda code

Phase A typically ships a placeholder `lambda_handler` (real code does
not fit in CFN's 4096-byte inline `ZipFile`). Push the real code with
`aws lambda update-function-code` and then `aws lambda wait
function-updated` so Phase B's stream validation invokes the real
handler, not the stub.

### Phase B: create the delivery stream

```bash
aws cloudformation deploy \
    --stack-name "${ICEBERG_STACK_NAME}" \
    --template-file cloudformation-iceberg.yaml \
    --parameter-overrides \
        "ProjectName=${PROJECT_NAME}" \
        "EnableFirehose=true" \
        "EnableFirehoseStream=true" \
    --capabilities CAPABILITY_NAMED_IAM \
    --no-fail-on-empty-changeset
```

### CRITICAL: parameter inheritance bites three-phase deploys

`aws cloudformation deploy` reuses the stack's previous values for any
parameter not in `--parameter-overrides`. On a re-run that omits
`EnableFirehoseStream`, the prior "true" value sticks, and you can end
up with a `WantFirehoseStream` resource depending on a `WantFirehose`
resource that you accidentally toggled off elsewhere. CFN reports this
as "unresolved resource dependencies" with no hint at the cause.

**Always pin every flag explicitly on every deploy call**, even when
the value is "obviously the default":

```bash
# WRONG: relies on inherited values
aws cloudformation deploy ... --parameter-overrides "ProjectName=${PROJECT_NAME}"

# RIGHT: pin everything that gates a Condition
aws cloudformation deploy ... --parameter-overrides \
    "ProjectName=${PROJECT_NAME}" \
    "EnableFirehose=true" \
    "EnableFirehoseStream=false"
```

A subtle related trap: do NOT use Phase A to toggle `EnableFirehose=false`
on a re-run. CFN will tear down the error bucket (and Firehose role,
log group). Re-deploys then race S3's bucket-name reservation cooldown
and may fail to recreate the bucket while it still holds failed-delivery
objects. Keep `EnableFirehose=true` from the first deploy onward; only
toggle `EnableFirehoseStream` between phases.

## Lake Formation grants for the Firehose role

The bucket-nested `s3tablescatalog/<bucket>` catalog is in **Lake
Formation access control mode** (this is what S3 Tables registers it
as). The Firehose role's IAM `glue:GetTable` permission alone is not
enough; LF must also grant DESCRIBE on the catalog, DESCRIBE +
CREATE_TABLE + ALTER on the database, and ALL on the table.

The `grant-permissions` API itself requires the **caller** to be a Lake
Formation Data Lake Administrator. If you are running as an admin IAM
user but not a Data Lake Admin, every `grant-permissions` call returns
`AccessDeniedException`. To add yourself as an admin (preserving any
existing admins):

```bash
# Read existing admins first; LF settings are full-replace.
EXISTING=$(aws lakeformation get-data-lake-settings \
    --query 'DataLakeSettings.DataLakeAdmins[*].DataLakePrincipalIdentifier' \
    --output text)
ME=$(aws sts get-caller-identity --query Arn --output text)

# Build a JSON admin list that includes you and any prior admins.
PRINCIPALS=$(python3 -c "
import json, os
existing = os.environ['EXISTING'].split()
me = os.environ['ME']
admins = [{'DataLakePrincipalIdentifier': p} for p in existing if p]
if not any(a['DataLakePrincipalIdentifier'] == me for a in admins):
    admins.append({'DataLakePrincipalIdentifier': me})
print(json.dumps({'DataLakeAdmins': admins}))
")
aws lakeformation put-data-lake-settings --data-lake-settings "${PRINCIPALS}"
```

### Do NOT swallow grant errors with `|| true`

A common bash idiom is to append `|| true` to LF grants on the theory
that they are idempotent. They ARE idempotent for "already exists",
but `AccessDenied` (caller is not a Data Lake Admin) hits the same
`|| true` and the missing grant surfaces ten minutes later as an
opaque `Firehose: Role ... is not authorized to perform: glue:GetTable`
during stream creation.

Use this `lf_grant` helper instead - it tolerates "already exists" but
aborts on any other error:

```bash
# Run a Lake Formation grant-permissions call that is idempotent but
# fails LOUDLY on real errors. Re-running an existing grant is fine;
# AccessDenied or any other failure must abort.
lf_grant() {
    local out rc
    out=$("$@" 2>&1)
    rc=$?
    if [ "${rc}" -eq 0 ]; then
        return 0
    fi
    if printf '%s' "${out}" | grep -Eqi 'already exists'; then
        return 0
    fi
    echo "Lake Formation grant failed: ${out}" >&2
    exit 1
}

# Usage:
lf_grant aws lakeformation grant-permissions \
    --principal "DataLakePrincipalIdentifier=${FIREHOSE_ROLE_ARN}" \
    --resource "{\"Catalog\":{\"Id\":\"${CATALOG_ID_NESTED}\"}}" \
    --permissions DESCRIBE
```

## Error bucket: how to decode failures

When Firehose cannot deliver a record to Iceberg, it writes the record
to the S3 error bucket under
`errors/<stream-name>/iceberg-failed/YYYY/MM/DD/HH/<file>`.

The bucket layout (with the snippet above) is:

```
s3://my-iceberg-fh-errors-<account-id>/
  errors/<project>/
    iceberg-failed/             # records that reached Iceberg writer but failed
      YYYY/MM/DD/HH/<files>
    processing-failed/          # transform Lambda returned ProcessingFailed
      YYYY/MM/DD/HH/<files>
```

Each line in an error-bucket file is a JSON object:

```json
{
  "errorCode": "Iceberg.MissingColumnWithinRecord",
  "errorMessage": "One or more columns are missing in the record",
  "rawData": "<base64-encoded original record>"
}
```

To inspect:

```bash
# List recent failures.
aws s3 ls "s3://my-iceberg-fh-errors-<account-id>/errors/${PROJECT_NAME}/iceberg-failed/" \
    --recursive --human-readable | tail -10

# Download and decode one.
aws s3 cp "s3://.../iceberg-failed/.../sample.gz" - | gunzip | head -1 | python3 -c '
import json, base64, sys
rec = json.loads(sys.stdin.read())
print("errorCode:", rec["errorCode"])
print("errorMessage:", rec["errorMessage"])
print("rawData:", base64.b64decode(rec["rawData"]).decode("utf-8"))
'
```

Common `errorCode` values and what they mean:

| errorCode                              | Cause                                                                                |
| -------------------------------------- | ------------------------------------------------------------------------------------ |
| `Iceberg.MissingColumnWithinRecord`    | Producer record has no key matching a required column. Add a transform Lambda.       |
| `Iceberg.InvalidColumnValue`           | Type mismatch. Check timestamp microseconds and JSON-encoded nested values.          |
| `Iceberg.GlueTableNotFound`            | Table does not exist or LF DESCRIBE missing. Phase A LF grants likely incomplete.    |
| `Iceberg.AccessDenied`                 | Firehose role missing `s3tables:PutTableData` or LF table ALL.                       |
| `Lambda.InvokeAccessDenied`            | Firehose role missing `lambda:InvokeFunction` on the transform.                      |
| `Lambda.FunctionInvocationTimeout`     | Transform exceeded its timeout. Default Firehose cap on transform invocation is 5min.|
| `Lambda.UserBadResponse` / `BadRequest`| Transform returned the wrong shape. Each output record needs `recordId` + `result`.  |

Set a CloudWatch alarm on
`AWS/Firehose / DeliveryToIcebergRecords` (zero) plus
`DeliveryToIcebergFailedRecords` (>0) so a regression in record shape
fires an alarm rather than silently filling the error bucket.

## Cross-references

- Producer side. If your records originate from Aurora DSQL CDC on a
  Kinesis Data Stream, also load `cdc-streaming-pipeline`. It covers
  the DSQL CDC envelope shape (`{op, after, before, source, ts_ms}`)
  that this skill's transform Lambda example reshapes into the Iceberg
  column layout.

- Query side. To query the Iceberg table from Amazon Redshift Serverless
  via `CREATE EXTERNAL SCHEMA` (Glue resource link, federated catalog,
  the `WITH NO SCHEMA BINDING` view requirement), load
  `lakehouse-redshift`.

- Reference material in this skill:
  - `reference/iceberg-table-create.md`: full S3 Tables `create-table`
    snippet with the bash retry loop for namespace propagation.
  - `reference/firehose-role-policies.md`: the complete IAM policy
    document for the Firehose role (Kinesis read, Glue write, S3 Tables
    I/O, error bucket, logs, lambda invoke).
