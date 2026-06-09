# DSQL CDC Stream Setup

This page covers everything from "I have a DSQL cluster" to "events are
landing in my Kinesis stream". Source-side DDL belongs to the
[`dsql`](../../dsql/SKILL.md) skill.

## Public preview status (read first)

Aurora DSQL CDC is in public preview. Two consequences shape the rest of
this page:

1. **There is no CloudFormation resource type for the CDC stream yet.**
   You cannot put `AWS::DSQL::Stream` (or any equivalent) into a CFN
   template. The stream is created via the AWS CLI or SDK after the
   cluster + Kinesis stream + trust role exist. Make the script
   idempotent so re-runs are safe.
2. **Only `c` and `d` ops are emitted.** Both source-side INSERT and
   source-side UPDATE arrive as `op='c'` (create). There is no `'u'`
   in preview. Code that special-cases `op == 'u'` will never fire on
   DSQL today. The append-only + ROW_NUMBER reconstruction pattern in
   [append-only-pattern.md](append-only-pattern.md) handles this
   correctly because the latest `c` per `record_id` wins regardless of
   whether the source operation was an insert or an update.

## CDC record envelope

A single Kinesis record (after Base64-decoding `kinesis.data` and
parsing JSON) looks roughly like this for a row create:

```json
{
  "op": "c",
  "ts_ms": 1717508412345,
  "source": {
    "db": "postgres",
    "schema": "public",
    "table": "orders"
  },
  "before": null,
  "after": {
    "id": "9f3d...",
    "customer_id": "c-1234",
    "total_cents": 4999,
    "status": "PENDING"
  }
}
```

For deletes, `op` is `d`, `after` is `null`, and `before` contains the
primary key of the removed row.

The consumer Lambda must:

1. Base64-decode `record["kinesis"]["data"]`.
2. JSON-parse the result.
3. Pick `after` for `c`, `before` for `d`.
4. Read `record_id` from the row payload (your source PK column name).
5. Read `commit_ts_ms` from `ts_ms` at the envelope level.

See [lambda-consumer.md](lambda-consumer.md) for the full handler.

## Step 1: Pre-requisites

These must exist before `aws dsql create-stream` will succeed.

- The DSQL cluster, in `ACTIVE` state.
- A Kinesis Data Stream in the same Region. On-demand mode is the
  simplest choice; provisioned mode works too.
- An IAM role that DSQL can assume to put records onto your Kinesis
  stream. The trust policy is the gotcha; see step 2.

## Step 2: IAM trust for the DSQL-to-Kinesis role

The CDC stream assumes an IAM role to write to your Kinesis stream.
The trust policy must:

- Trust the `dsql.amazonaws.com` service principal.
- Constrain `aws:SourceAccount` to your account ID (defense in depth).
- Constrain `aws:SourceArn` to match the cluster's stream ARN
  namespace, NOT the bare cluster ARN.

The non-obvious bit: DSQL passes the stream ARN
(`<cluster-arn>/stream/<stream-id>`) as `aws:SourceArn`, not the cluster
ARN itself. Use `ArnLike` with the `/stream/*` suffix:

```yaml
DsqlCdcKinesisRole:
  Type: AWS::IAM::Role
  Properties:
    AssumeRolePolicyDocument:
      Version: "2012-10-17"
      Statement:
        - Effect: Allow
          Principal:
            Service: dsql.amazonaws.com
          Action: sts:AssumeRole
          Condition:
            StringEquals:
              "aws:SourceAccount": !Ref AWS::AccountId
            ArnLike:
              "aws:SourceArn": !Sub "${DsqlCluster.ResourceArn}/stream/*"
    Policies:
      - PolicyName: PutCdcToKinesis
        PolicyDocument:
          Version: "2012-10-17"
          Statement:
            - Effect: Allow
              Action:
                - kinesis:PutRecord
                - kinesis:PutRecords
                - kinesis:DescribeStreamSummary
                - kinesis:ListShards
              Resource: !GetAtt CdcStream.Arn
```

If the trust mismatches, the stream stays in `CREATING` or transitions
to `FAILED`. Inspect with
`aws dsql get-stream --cluster-identifier ... --stream-identifier ...`.

## Step 3: Create the stream (idempotent script)

```bash
set -euo pipefail

# Reuse an existing stream rather than failing on re-run.
existing=$(aws dsql list-streams \
    --cluster-identifier "${DSQL_CLUSTER_ID}" \
    --region "${AWS_REGION}" \
    --query 'streams[0].streamIdentifier' \
    --output text 2>/dev/null || true)

if [ -n "${existing}" ] && [ "${existing}" != "None" ]; then
    DSQL_STREAM_ID="${existing}"
else
    DSQL_STREAM_ID=$(aws dsql create-stream \
        --cluster-identifier "${DSQL_CLUSTER_ID}" \
        --target-definition "$(printf '{"kinesis":{"streamArn":"%s","roleArn":"%s"}}' \
            "${KINESIS_STREAM_ARN}" "${DSQL_CDC_ROLE_ARN}")" \
        --ordering UNORDERED \
        --format JSON \
        --region "${AWS_REGION}" \
        --query 'streamIdentifier' \
        --output text)
fi

# Wait for ACTIVE before continuing. Fail fast on FAILED / DELETING.
for _ in $(seq 1 60); do
    status=$(aws dsql get-stream \
        --cluster-identifier "${DSQL_CLUSTER_ID}" \
        --stream-identifier "${DSQL_STREAM_ID}" \
        --region "${AWS_REGION}" \
        --query 'status' --output text)
    case "${status}" in
        ACTIVE) break ;;
        FAILED|DELETING|DELETED|IMPAIRED) echo "stream entered ${status}" >&2; exit 1 ;;
        *) sleep 5 ;;
    esac
done

if [ "${status}" != "ACTIVE" ]; then
    echo "stream did not become ACTIVE within timeout (last=${status})" >&2
    exit 1
fi
```

Notes:

- `--ordering UNORDERED` is the documented default and is correct for
  the append-only pattern. It also gives DSQL the most freedom to
  parallelize across Kinesis shards.
- `--format JSON` gives you the envelope shown above. Other formats
  exist; JSON is the simplest for a Lambda consumer.
- The IAM role ARN passed in `--target-definition` must already exist
  and have the trust + put-record permissions described in step 2.

## Step 4: Confirm events are flowing

Before wiring the Lambda, sanity-check end-to-end with the Kinesis CLI:

```bash
shard_iter=$(aws kinesis get-shard-iterator \
    --stream-name "${KINESIS_STREAM_NAME}" \
    --shard-id "shardId-000000000000" \
    --shard-iterator-type LATEST \
    --query 'ShardIterator' --output text)

aws kinesis get-records --shard-iterator "${shard_iter}" \
    | jq -r '.Records[].Data' \
    | base64 -d \
    | jq .
```

Run a small INSERT or UPDATE against the source. Within seconds you
should see one or more `op: "c"` envelopes in the output.

## Common failure modes

- **Stream stuck in `CREATING`.** Trust policy mismatch on the IAM
  role. Re-check `aws:SourceArn` (must be `ArnLike` against
  `<cluster>/stream/*`).
- **Stream goes to `FAILED`.** Often a permission error: the role
  cannot `kinesis:PutRecord` on the target stream. Check the role's
  inline policy.
- **No records appear in Kinesis after source writes.** Confirm the
  stream is `ACTIVE`, then confirm there is actual change activity on
  tables visible to the CDC stream (CDC publishes for the whole
  cluster; verify your source schema is included).

## What's next

After the stream is `ACTIVE` and you can see envelopes:

- Design the sink shape: [append-only-pattern.md](append-only-pattern.md).
- Write the consumer: [lambda-consumer.md](lambda-consumer.md) and
  [event-source-mapping.yaml](event-source-mapping.yaml).
