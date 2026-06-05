# Creating the S3 Tables Iceberg table out-of-band

`AWS::S3Tables::Table` (CFN) and Lambda-backed custom resources both
race the S3 Tables namespace propagation lag. After
`AWS::S3Tables::Namespace` reaches `CREATE_COMPLETE`, the namespace is
not visible to `s3tables:CreateTable` for **up to 5 minutes**. CFN's
resource handler gives up long before that.

The workaround is to create the table from a script with a generous
retry loop. This is idempotent: a second run finds the table and
exits.

## Bash snippet

```bash
#!/usr/bin/env bash
set -euo pipefail

# Inputs (from environment or stack outputs):
#   BUCKET_ARN      arn:aws:s3tables:<region>:<account>:bucket/<name>
#   AWS_REGION      e.g. us-east-1
NAMESPACE=cdc
TABLE_NAME=cdc_events_archive

# Skip if the table already exists.
if aws s3tables get-table \
        --table-bucket-arn "${BUCKET_ARN}" \
        --namespace "${NAMESPACE}" \
        --name "${TABLE_NAME}" \
        --region "${AWS_REGION}" >/dev/null 2>&1; then
    echo "Table ${NAMESPACE}.${TABLE_NAME} already exists"
    exit 0
fi

# Iceberg schema. Required columns must be present in every record;
# optional columns can be absent. Timestamp columns expect microseconds.
metadata_file=$(mktemp)
cat > "${metadata_file}" <<'JSON'
{
  "iceberg": {
    "schema": {
      "fields": [
        {"name": "source_table",     "type": "string",    "required": true},
        {"name": "operation",        "type": "string",    "required": true},
        {"name": "record_id",        "type": "string",    "required": true},
        {"name": "event_data",       "type": "string",    "required": false},
        {"name": "commit_timestamp", "type": "timestamp", "required": true},
        {"name": "ingested_at",      "type": "timestamp", "required": true}
      ]
    }
  }
}
JSON

# Retry up to 5 minutes (60 attempts at 5s intervals). In practice the
# table usually creates within ~30s, but the cushion prevents flakes.
success=0
for _ in $(seq 1 60); do
    if aws s3tables create-table \
            --table-bucket-arn "${BUCKET_ARN}" \
            --namespace "${NAMESPACE}" \
            --name "${TABLE_NAME}" \
            --format ICEBERG \
            --metadata "file://${metadata_file}" \
            --region "${AWS_REGION}" >/dev/null 2>&1; then
        success=1
        break
    fi
    printf '.' >&2
    sleep 5
done
rm -f "${metadata_file}"
[ "${success}" = "1" ] || { echo "create-table failed after 5 minutes" >&2; exit 1; }
echo "Created ${NAMESPACE}.${TABLE_NAME}"
```

## Why not CFN

Three concrete failure modes seen with `AWS::S3Tables::Table`:

1. `Resource handler returned message: "The specified namespace does
   not exist." (Status Code: 404)` even though the namespace resource
   reports `CREATE_COMPLETE`.
2. Lambda-backed custom resources with 30-second retry budgets fail
   identically. A 5-minute custom resource would work but adds Lambda
   code, a role, and a log group for what is fundamentally a one-shot
   bash retry.
3. After the table eventually exists, downgrading or deleting the
   stack and immediately re-creating the same bucket name races S3
   Tables' name reservation (~minutes after delete). The script
   pattern handles this with a `BucketSuffix` parameter.

## `!Ref TableBucket` returns the ARN, not the name

This is non-obvious. For nearly every CFN resource type, `!Ref` returns
the bare name. `AWS::S3Tables::TableBucket` is an exception: `!Ref`
returns the full ARN
(`arn:aws:s3tables:<region>:<account>:bucket/<name>`).

If you need the bare name (Firehose's `DestinationDatabaseName`, the
Glue catalog ID, log paths), extract it:

```yaml
!Select [1, !Split ["/", !GetAtt TableBucket.TableBucketARN]]
```

You will need this in 4+ places: the Glue catalog ARN, the Glue catalog
ID, the bucket name in stack outputs, and any prefix that embeds the
bucket name. Define it once at the top of the template via a Mapping
or a Sub local if the indirection bothers you, but inline `!Select` is
the more common pattern.

## Bucket-name reservation cooldown

S3 Tables reserves bucket names for several minutes after delete. If
you tear down a stack and immediately re-deploy with the same template,
you get:

```
The bucket is in a transitional state because of a previous deletion attempt.
```

Add a `BucketSuffix` parameter so iterators can pass `v2`, `v3`, etc.
without waiting:

```yaml
Parameters:
  BucketSuffix:
    Type: String
    Default: ""
    AllowedPattern: "^[a-z0-9-]{0,16}$"

Conditions:
  HasBucketSuffix: !Not [!Equals [!Ref BucketSuffix, ""]]

Resources:
  TableBucket:
    Type: AWS::S3Tables::TableBucket
    Properties:
      TableBucketName: !If
        - HasBucketSuffix
        - !Sub "${ProjectName}-iceberg-${BucketSuffix}"
        - !Sub "${ProjectName}-iceberg"
```
