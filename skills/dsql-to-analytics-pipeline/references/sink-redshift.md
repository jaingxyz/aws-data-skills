# Sink: Redshift Serverless

This page covers the IAM, SUPER projection, and unnesting patterns
specific to the Redshift Serverless sink. The append-only DDL itself
is in [append-only-pattern.md](append-only-pattern.md).

## IAM for the consumer Lambda

The Lambda execution role needs three things: read from Kinesis, log
to CloudWatch, and call the Redshift Data API with IAM auth into the
workgroup.

```yaml
LambdaExecRole:
  Type: AWS::IAM::Role
  Properties:
    AssumeRolePolicyDocument:
      Statement:
        - Effect: Allow
          Principal: { Service: lambda.amazonaws.com }
          Action: sts:AssumeRole
    ManagedPolicyArns:
      # Reads from Kinesis + writes CloudWatch logs.
      - arn:aws:iam::aws:policy/service-role/AWSLambdaKinesisExecutionRole
      - arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
    Policies:
      - PolicyName: RedshiftDataAPI
        PolicyDocument:
          Statement:
            # Redshift Data API actions do NOT support resource-level
            # permissions. Resource: "*" is the only valid value.
            # Trying to scope this with an ARN will fail policy
            # validation with a confusing error.
            - Effect: Allow
              Action:
                - redshift-data:ExecuteStatement
                - redshift-data:DescribeStatement
                - redshift-data:GetStatementResult
              Resource: "*"
            # IAM auth into the Serverless workgroup, scoped to the
            # workgroup ARN. This is what gives the Lambda
            # workgroup-level credentials without a Secrets Manager
            # secret to rotate.
            - Effect: Allow
              Action:
                - redshift-serverless:GetCredentials
              Resource: !GetAtt RedshiftWorkgroup.Workgroup.WorkgroupArn
```

Two specifics worth memorizing:

- **Redshift Data API actions don't support resource-level
  permissions.** Trying
  `Resource: !Sub "arn:aws:redshift-data:${AWS::Region}:${AWS::AccountId}:..."`
  is rejected at policy creation. `Resource: "*"` is the only valid
  value. Workgroup-level access control happens via
  `redshift-serverless:GetCredentials`.
- **`redshift-serverless:GetCredentials`** is what gives the Lambda
  workgroup-level auth. The Data API requires either this for IAM
  auth or a Secrets Manager secret ARN. Prefer IAM auth; less to
  rotate, no secret to leak.

## How the Lambda authenticates at call time

The Lambda does not call `GetCredentials` directly. It passes
`WorkgroupName=` and `Database=` to `execute_statement`, and the
Data API performs the credential exchange behind the scenes using
the Lambda's IAM identity. The IAM-mapped Redshift user is
auto-created on first call and named `IAMR:<role-name>`.

```python
response = redshift.execute_statement(
    WorkgroupName=os.environ["REDSHIFT_WORKGROUP"],
    Database=os.environ["REDSHIFT_DATABASE"],
    Sql=sql,
    Parameters=parameters,
)
```

GRANTs in the schema can target either `PUBLIC` (simple, demo-grade) or
`IAMR:<role-name>` (production: explicit, least-privilege).

## Landing JSON into a `SUPER` column

The Lambda inserts a JSON-encoded string parameter; the SQL wraps it in
`JSON_PARSE` to convert to `SUPER` at insert time:

```sql
INSERT INTO cdc_events (source_table, operation, record_id, event_data, commit_timestamp)
VALUES (:t0, :op0, :id0, JSON_PARSE(:d0),
        TIMESTAMP 'epoch' + CAST(:ts0 AS BIGINT) / 1000.0 * INTERVAL '1 second');
```

Without `JSON_PARSE`, the value lands as plain text and
`event_data."col"::TYPE` subscripting fails at view time.

## Reading `SUPER` at view time

```sql
SELECT
    event_data."customer_id"::VARCHAR AS customer_id,
    event_data."total_cents"::BIGINT  AS total_cents,
    event_data."status"::VARCHAR      AS status
FROM cdc_events
WHERE source_table = 'orders';
```

Rules of thumb:

- **Quote field names**: `event_data."customer_id"`. Without quotes,
  Redshift lowercases the identifier. Source columns are usually
  case-sensitive in JSON, so quoting is the defensive default.
- **Cast at projection time**: `::VARCHAR`, `::BIGINT`, `::INT`,
  `::BOOLEAN`, `::TIMESTAMP`. Without casts you get raw `SUPER`
  values, which most BI tools cannot consume directly.
- **Nested fields**: `event_data."address"."city"::VARCHAR`. Deeper
  subscripting works.

## Mixed-case JSON keys

If your source produces mixed-case keys (e.g. `customerId` rather
than `customer_id`), enable case-sensitive identifiers per session:

```sql
SET enable_case_sensitive_identifier TO TRUE;
SELECT event_data."customerId"::VARCHAR FROM cdc_events;
```

This is a session GUC, not a cluster-wide setting. Set it at the
top of any script that needs case-sensitive subscripting. Most BI
tools have a "session init" hook for exactly this.

## Unnesting `SUPER` arrays with PartiQL

Redshift's PartiQL extension lets you treat a `SUPER` array as a
virtual table on the right-hand side of `FROM`, joining it
row-by-element with its parent. Comma-cross-join the source row
with its array field and bind a per-element alias:

```sql
-- Suppose event_data has a "tags" array, e.g.
--   event_data = {"id": "...", "tags": ["vip", "new"]}
SELECT
    e.record_id,
    e.commit_timestamp,
    t::VARCHAR AS tag
FROM cdc_events AS e, e.event_data."tags" AS t
WHERE e.source_table = 'orders';
```

Output: one row per (event, tag) pair. The `t::VARCHAR` cast
produces a scalar; without it `t` stays `SUPER`.

For arrays of objects:

```sql
-- event_data = {"id": "...", "items": [{"sku": "A", "qty": 2}, ...]}
SELECT
    e.record_id,
    item."sku"::VARCHAR AS sku,
    item."qty"::INT     AS qty
FROM cdc_events AS e, e.event_data."items" AS item
WHERE e.source_table = 'orders';
```

If you only need a single element by index, no unnest required:
`event_data."tags"[0]::VARCHAR`.

## Putting it all together: production checklist

- [ ] `cdc_events` table exists with `DISTKEY(record_id)` and
      `SORTKEY(source_table, commit_timestamp)`.
- [ ] One `*_current` view per source table, with `WHERE rn = 1
      AND operation <> 'd'`.
- [ ] Lambda execution role has `redshift-data:*` on `Resource: "*"`
      and `redshift-serverless:GetCredentials` on the workgroup ARN.
- [ ] `WorkgroupName` and `Database` env vars are set on the Lambda.
- [ ] GRANTs target either `PUBLIC` (demo) or the
      `IAMR:<role-name>` user (production).
- [ ] If the source emits mixed-case keys, downstream queries set
      `enable_case_sensitive_identifier`.

## Related

- [append-only-pattern.md](append-only-pattern.md) for the table
  + view DDL.
- [lambda-consumer.md](lambda-consumer.md) for the writer.
- [sink-s3-iceberg.md](sink-s3-iceberg.md) for an Iceberg cold-path
  archive that complements (not replaces) this hot-path Redshift sink.
