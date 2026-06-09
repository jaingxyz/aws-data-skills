# Firehose IcebergDestinationConfiguration

The `IcebergDestinationConfiguration` block on `AWS::KinesisFirehose::DeliveryStream` is the contract between Firehose and the Iceberg table. Every field below is load-bearing.

## Minimum viable block

```yaml
IcebergDestinationConfiguration:
  RoleARN: !GetAtt FirehoseRole.Arn
  CatalogConfiguration:
    CatalogARN: !Sub "arn:${AWS::Partition}:glue:${AWS::Region}:${AWS::AccountId}:catalog/s3tablescatalog/${TableBucketName}"
  DestinationTableConfigurationList:
    - DestinationDatabaseName: !Ref Namespace
      DestinationTableName: !Ref TableName
      UniqueKeys: [record_id]
  BufferingHints:
    SizeInMBs: 64
    IntervalInSeconds: 60
  S3Configuration:
    BucketARN: !GetAtt ErrorBucket.Arn
    RoleARN: !GetAtt FirehoseRole.Arn
    Prefix: !Sub "errors/${StreamName}/"
    ErrorOutputPrefix: !Sub "errors/${StreamName}/iceberg-failed/"
  ProcessingConfiguration:
    Enabled: true
    Processors:
      - Type: Lambda
        Parameters:
          - ParameterName: LambdaArn
            ParameterValue: !GetAtt TransformLambda.Arn
          - ParameterName: BufferSizeInMBs
            ParameterValue: "1"
          - ParameterName: BufferIntervalInSeconds
            ParameterValue: "60"
```

## CatalogConfiguration

| Target | `CatalogARN` value |
|---|---|
| S3 Tables (federated catalog) | `arn:aws:glue:<region>:<account>:catalog/s3tablescatalog/<table-bucket-name>` |
| Standard Iceberg on a general purpose bucket | `arn:aws:glue:<region>:<account>:catalog` (the default Glue catalog) |

The federated catalog ARN MUST exist as a registered catalog before Firehose creates the stream. Verify by listing databases inside it: `aws glue get-databases --catalog-id "<account-id>:s3tablescatalog/<bucket>"` (use `aws sts get-caller-identity --query Account --output text` to source the account-id prefix). The call returns `EntityNotFoundException` if the catalog is unregistered. Otherwise the stream creation fails with `Iceberg.GlueTableNotFound` even though the underlying table exists. (`aws glue get-catalog` is not a subcommand on most installed AWS CLI versions; `get-databases` is the reliable existence probe.)

## DestinationTableConfigurationList

- `DestinationDatabaseName` is the Glue database (the S3 Tables namespace, for S3 Tables targets).
- `DestinationTableName` is the table name. Lowercase only.
- `UniqueKeys` is required when records carry `operation = update` or `operation = delete`. List the column or columns that uniquely identify a row. For pure `insert` workloads, `UniqueKeys` may be omitted.

You can list multiple `DestinationTableConfigurationList` entries to fan one stream into multiple tables. Firehose routes each record to the entry whose `DestinationDatabaseName` and `DestinationTableName` match the per-record metadata fields the transform Lambda emits. Cover this in [`transform-lambda.md`](transform-lambda.md) under per-record routing.

## S3Configuration (error bucket)

Mandatory. Even with no Iceberg failures expected, Firehose requires an S3 fallback. `ErrorOutputPrefix` controls where failed records land: `errors/<stream-name>/iceberg-failed/`. Decoding the contents is covered in [`error-bucket-decoding.md`](error-bucket-decoding.md).

The Firehose role MUST hold `s3:PutObject`, `s3:GetBucketLocation`, and `s3:ListBucket` on the error bucket.

## ProcessingConfiguration

Set `Enabled: true` only when a transform Lambda is needed (see SKILL.md Phase 2). The Lambda ARN goes in the `LambdaArn` parameter. `BufferSizeInMBs` and `BufferIntervalInSeconds` apply to the Lambda invocation buffer, not the Iceberg buffer. Keep both small (1 MB, 60 s) to limit blast radius when the Lambda is misbehaving.

## lambda:InvokeFunction grant

The Firehose role MUST hold `lambda:InvokeFunction` on the transform Lambda ARN. Add this to the role inline policy alongside the Iceberg, S3, and Lake Formation grants:

```yaml
- Effect: Allow
  Action: lambda:InvokeFunction
  Resource: !GetAtt TransformLambda.Arn
```

Without this, Firehose fails the entire batch with `Lambda.InvokeAccessDenied` and CloudWatch reports `ExecuteProcessingFailure.Records` matching the batch size.

## BufferingHints

- `SizeInMBs`: 1 to 128. Default 64 is fine for most workloads.
- `IntervalInSeconds`: 60 to 900. Minimum is 60 for Iceberg destinations. The stream will accept smaller values at create time but enforce 60 at runtime, leading to records that never appear during a 30-second test.

## Source-side configuration

`IcebergDestinationConfiguration` works with all three Firehose source types:

- Direct PUT: no `KinesisStreamSourceConfiguration` block.
- Kinesis Data Streams: add `KinesisStreamSourceConfiguration` with `KinesisStreamARN` and a role that holds `kinesis:DescribeStream`, `kinesis:GetShardIterator`, `kinesis:GetRecords`, `kinesis:ListShards`.
- MSK: add `MSKSourceConfiguration` with the cluster ARN and topic. The Firehose role needs `kafka-cluster:Connect`, `kafka-cluster:DescribeCluster`, `kafka-cluster:DescribeClusterDynamicConfiguration`, `kafka-cluster:DescribeTopic`, `kafka-cluster:DescribeGroup`, `kafka-cluster:ReadData` on the topic, plus `kafka:GetBootstrapBrokers` on the cluster.
