# CloudFormation gotchas specific to S3 Tables

These are the things that have eaten the most time when authoring CFN for an S3 Tables + Glue + Lake Formation + Redshift Serverless stack. Keep them in your head while writing the template.

## 1. `!Ref AWS::S3Tables::TableBucket` returns the ARN, not the name

This is the single highest-time-wasted issue in this stack. `!Ref` on an `AWS::S3Tables::TableBucket` returns the full ARN:

```
arn:aws:s3tables:<region>:<account-id>:bucket/<name>
```

If you write `${TableBucket}` in a `!Sub` expecting the name, every downstream interpolation breaks. The bucket-nested catalog ARN ends up looking like:

```
arn:aws:glue:...:catalog/s3tablescatalog/arn:aws:s3tables:...
```

which fails CloudFormation validation in opaque ways because the regex for catalog ARNs allows up to two path components and this has many.

### The fix

Extract the bare name from the ARN:

```yaml
!Select [1, !Split ["/", !GetAtt TableBucket.TableBucketARN]]
```

This pattern is needed in 4+ places in a non-trivial stack. To avoid repetition, declare it once as a `!Sub` substitution variable:

```yaml
GlueCatalogArn:
  Description: Bucket-nested Glue catalog ARN
  Value: !Sub
    - "arn:aws:glue:${AWS::Region}:${AWS::AccountId}:catalog/s3tablescatalog/${BucketName}"
    - BucketName: !Select [1, !Split ["/", !GetAtt TableBucket.TableBucketARN]]
  Export:
    Name: !Sub "${ProjectName}-iceberg-catalog-arn"
```

If you find yourself writing the `!Select`/`!Split` more than twice, consider exposing the bucket name as an Output and consuming it via `!ImportValue` in a downstream stack, or compute it once in a script and pass it as a parameter to the next phase.

## 2. `AWS::S3Tables::Table` resource handler races namespace propagation

The S3 Tables namespace API returns success on `CreateNamespace` immediately, but the namespace is not readable to subsequent `CreateTable` calls for **5+ minutes**. CloudFormation's resource handler considers `AWS::S3Tables::Namespace` `CREATE_COMPLETE` within a few hundred milliseconds. If `AWS::S3Tables::Table` follows it in the same stack, the table create call returns `404 NotFound: The specified namespace does not exist.`

Adding `DependsOn: TableNamespace` does not help. It is already implicit via `!Ref`, and the lag is not in the dependency graph - it is in eventual consistency on the S3 Tables side.

A Lambda-backed custom resource with retries also fails unless you raise the Lambda timeout above 5 minutes and the retry budget covers the worst case. Doable, but adds a lot of complexity for a one-time create.

### The recommendation

Move table creation OUT of CFN. Create the bucket and namespace in CFN; create the table in a deploy script with bash-level retries:

```bash
metadata_file=$(mktemp)
cat > "${metadata_file}" <<'JSON'
{
  "iceberg": {
    "schema": {
      "fields": [
        {"name": "col_a", "type": "string",    "required": true},
        {"name": "col_b", "type": "timestamp", "required": true}
      ]
    }
  }
}
JSON

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
[ "${success}" = "1" ] || { echo "create-table failed after 5 minutes"; exit 1; }
```

60 iterations of 5 seconds = 5 minutes of cushion. In practice the call succeeds within ~30s once the namespace propagates, but the cushion prevents flakes on a cold bucket.

Run the create idempotently by checking with `get-table` first:

```bash
if aws s3tables get-table \
        --table-bucket-arn "${BUCKET_ARN}" \
        --namespace "${NAMESPACE}" \
        --name "${TABLE_NAME}" \
        --region "${AWS_REGION}" >/dev/null 2>&1; then
    echo "Table already exists"
else
    # create with retry loop above
fi
```

## 3. S3 Tables bucket name reservation cooldown

After deleting an `AWS::S3Tables::TableBucket`, the bucket name is reserved within the account/region for several minutes. Re-deploying the same stack within that window fails with:

```
The bucket is in a transitional state because of a previous deletion attempt.
```

This is similar to S3 bucket name cooldown but the window is longer.

### The fix

Add a `BucketSuffix` parameter so iterators can pass `v2`, `v3`, etc. without waiting:

```yaml
Parameters:
  BucketSuffix:
    Type: String
    Default: ""
    Description: >
      Optional suffix appended to the S3 Tables bucket name. S3 Tables
      bucket names are globally reserved within an account/region for
      ~minutes after deletion; pass a fresh suffix to work around the
      cooldown when iterating.
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

## 4. `aws cloudformation deploy` parameter inheritance

`aws cloudformation deploy` reuses the stack's previous parameter value for any flag NOT in `--parameter-overrides`. This breaks multi-phase deploys when later phases assume a specific value for a flag that is left unspecified.

Two safe patterns:
- Pin every flag explicitly in every phase, even when its value is "the same as before."
- Design the phases so that re-running them does not toggle destructive flags.

The second is more robust. For an S3 Tables + Firehose stack, it means: do not have a Phase 1 that sets `EnableFirehose=false` and a Phase 2 that sets `EnableFirehose=true` if the resources gated on `EnableFirehose=true` include a stateful bucket (the error bucket). Re-running Phase 1 tears down the bucket, fails on bucket name cooldown, and bricks the stack. Instead collapse to a single phase that always has `EnableFirehose=true` and only toggles the actual delivery-stream flag.

## 5. `--no-fail-on-empty-changeset`

`aws cloudformation deploy` exits non-zero if the change set is empty (i.e., re-running the same template with no changes). Idempotent deploy scripts should pass `--no-fail-on-empty-changeset` so re-runs are safe:

```bash
aws cloudformation deploy \
    --stack-name "${STACK_NAME}" \
    --template-file "${TEMPLATE}" \
    --parameter-overrides ... \
    --capabilities CAPABILITY_NAMED_IAM \
    --region "${AWS_REGION}" \
    --no-fail-on-empty-changeset
```

## 6. No `DeletionPolicy` on demo-grade Redshift resources

For demo stacks meant to be torn down with a script, omit `DeletionPolicy` on the Redshift Serverless namespace and workgroup so the teardown is clean. For production, add `DeletionPolicy: Snapshot` on the namespace so accidental stack delete preserves the data. Same logic applies to the S3 Tables bucket: a demo bucket can drop, a production lakehouse cannot.
