# Firehose role: complete IAM policy document

The Firehose role for `IcebergDestinationConfiguration` writing to S3
Tables needs five distinct permission groups. Missing any one of them
results in a synchronous stream-create failure or, worse, a successful
stream that silently drops records.

This file is the full inline-policies template. Replace `<account-id>`,
`<region>`, and the resource names to fit your stack.

## Trust policy

```yaml
AssumeRolePolicyDocument:
  Version: "2012-10-17"
  Statement:
    - Effect: Allow
      Principal:
        Service: firehose.amazonaws.com
      Action: sts:AssumeRole
```

## Inline policy 1: Kinesis read (skip if source is Direct PUT)

```yaml
- PolicyName: KinesisRead
  PolicyDocument:
    Version: "2012-10-17"
    Statement:
      - Effect: Allow
        Action:
          - kinesis:DescribeStream
          - kinesis:GetShardIterator
          - kinesis:GetRecords
          - kinesis:ListShards
        Resource: !ImportValue my-kinesis-stream-arn
```

## Inline policy 2: Glue access through the federated catalog

The bucket-nested `s3tablescatalog/<bucket>` catalog has THREE ARN
layers that Firehose walks: the parent catalog, the bucket-nested
child, and the database/table under each. List all of them:

```yaml
- PolicyName: GlueIceberg
  PolicyDocument:
    Version: "2012-10-17"
    Statement:
      - Effect: Allow
        Action:
          - glue:GetTable
          - glue:GetTables
          - glue:GetDatabase
          - glue:GetDatabases
          - glue:CreateTable
          - glue:UpdateTable
        Resource:
          - !Sub "arn:aws:glue:${AWS::Region}:${AWS::AccountId}:catalog"
          - !Sub "arn:aws:glue:${AWS::Region}:${AWS::AccountId}:catalog/s3tablescatalog"
          - !Sub "arn:aws:glue:${AWS::Region}:${AWS::AccountId}:catalog/s3tablescatalog/*"
          - !Sub "arn:aws:glue:${AWS::Region}:${AWS::AccountId}:database/s3tablescatalog/*"
          - !Sub "arn:aws:glue:${AWS::Region}:${AWS::AccountId}:database/*/s3tablescatalog/*"
          - !Sub "arn:aws:glue:${AWS::Region}:${AWS::AccountId}:table/s3tablescatalog/*/*"
          - !Sub "arn:aws:glue:${AWS::Region}:${AWS::AccountId}:table/*/s3tablescatalog/*/*"
      # Lake Formation is the gatekeeper for the federated catalog.
      # Grant the API-level call here; the actual data perms are
      # granted via lakeformation grant-permissions, not IAM.
      - Effect: Allow
        Action:
          - lakeformation:GetDataAccess
        Resource: "*"
```

## Inline policy 3: S3 Tables data I/O

Firehose talks to S3 Tables directly for the actual writes (Glue is
only the metadata path):

```yaml
- PolicyName: S3TablesIO
  PolicyDocument:
    Version: "2012-10-17"
    Statement:
      - Effect: Allow
        Action:
          - s3tables:GetTableBucket
          - s3tables:GetNamespace
          - s3tables:GetTable
          - s3tables:GetTableMetadataLocation
          - s3tables:UpdateTableMetadataLocation
          - s3tables:PutTableData
          - s3tables:GetTableData
        Resource:
          - !GetAtt TableBucket.TableBucketARN
          - !Sub "${TableBucket.TableBucketARN}/*"
```

## Inline policy 4: error bucket I/O

```yaml
- PolicyName: ErrorBucketIO
  PolicyDocument:
    Version: "2012-10-17"
    Statement:
      - Effect: Allow
        Action:
          - s3:PutObject
          - s3:GetBucketLocation
          - s3:ListBucket
        Resource:
          - !GetAtt FirehoseErrorBucket.Arn
          - !Sub "${FirehoseErrorBucket.Arn}/*"
```

## Inline policy 5: CloudWatch Logs

```yaml
- PolicyName: Logs
  PolicyDocument:
    Version: "2012-10-17"
    Statement:
      - Effect: Allow
        Action:
          - logs:CreateLogStream
          - logs:PutLogEvents
        Resource: !GetAtt FirehoseLogGroup.Arn
```

## Inline policy 6: invoke transform Lambda

Easy to forget. The FIREHOSE role (not the Lambda execution role) needs
this for `ProcessingConfiguration` to work:

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

## What is NOT in IAM: Lake Formation grants

The IAM `glue:GetTable` permission alone is not enough. Lake Formation
also needs to grant DESCRIBE on the catalog, DESCRIBE / CREATE_TABLE /
ALTER on the database, and ALL on the table to the role. These are
applied via `aws lakeformation grant-permissions` (NOT in CFN; see
SKILL.md "Lake Formation grants for the Firehose role").

If the IAM is correct but LF grants are missing, the symptom is the
same as missing IAM:

```
Role <arn> is not authorized to perform: glue:GetTable for the given
table or the table does not exist
```

This error is misleading; both IAM and LF have to be present.
