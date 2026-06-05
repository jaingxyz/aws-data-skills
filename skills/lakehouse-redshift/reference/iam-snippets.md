# IAM and Lake Formation snippets for the Redshift Serverless query path

Copy-pasteable fragments. Substitute `<region>`, `<account-id>`, `<bucket-name>`, `<namespace>`, `<role-arn>` as appropriate. These are sized for a single-bucket lakehouse; tighten resource patterns when you have multiple buckets sharing a workgroup.

## Redshift Serverless query role (CloudFormation)

This is the role attached to the Redshift Serverless namespace via `IamRoles` + `DefaultIamRoleArn`. It needs read access to:
- Glue Data Catalog (default + bucket-nested)
- S3 Tables data
- Lake Formation (which gates the federated catalog reads)

```yaml
RedshiftQueryRole:
  Type: AWS::IAM::Role
  Properties:
    RoleName: !Sub "${ProjectName}-redshift-query-role"
    AssumeRolePolicyDocument:
      Version: "2012-10-17"
      Statement:
        - Effect: Allow
          Principal:
            Service:
              - redshift.amazonaws.com
              - redshift-serverless.amazonaws.com
          Action: sts:AssumeRole
    Policies:
      - PolicyName: GlueRead
        PolicyDocument:
          Version: "2012-10-17"
          Statement:
            - Effect: Allow
              Action:
                - glue:GetCatalog
                - glue:GetCatalogs
                - glue:GetDatabase
                - glue:GetDatabases
                - glue:GetTable
                - glue:GetTables
                - glue:GetPartition
                - glue:GetPartitions
              Resource:
                - !Sub "arn:aws:glue:${AWS::Region}:${AWS::AccountId}:catalog"
                - !Sub "arn:aws:glue:${AWS::Region}:${AWS::AccountId}:catalog/s3tablescatalog"
                - !Sub "arn:aws:glue:${AWS::Region}:${AWS::AccountId}:catalog/s3tablescatalog/*"
                - !Sub "arn:aws:glue:${AWS::Region}:${AWS::AccountId}:database/s3tablescatalog/*"
                - !Sub "arn:aws:glue:${AWS::Region}:${AWS::AccountId}:database/*/s3tablescatalog/*"
                - !Sub "arn:aws:glue:${AWS::Region}:${AWS::AccountId}:table/s3tablescatalog/*/*"
                - !Sub "arn:aws:glue:${AWS::Region}:${AWS::AccountId}:table/*/s3tablescatalog/*/*"
      - PolicyName: S3TablesRead
        PolicyDocument:
          Version: "2012-10-17"
          Statement:
            - Effect: Allow
              Action:
                - s3tables:GetTableBucket
                - s3tables:GetNamespace
                - s3tables:GetTable
                - s3tables:GetTableMetadataLocation
                - s3tables:GetTableData
                - s3tables:ListTables
                - s3tables:ListNamespaces
                - s3tables:ListTableBuckets
              Resource:
                - !GetAtt TableBucket.TableBucketARN
                - !Sub "${TableBucket.TableBucketARN}/*"
      - PolicyName: LakeFormation
        PolicyDocument:
          Version: "2012-10-17"
          Statement:
            - Effect: Allow
              Action:
                - lakeformation:GetDataAccess
                - lakeformation:GetResourceLFTags
                - lakeformation:ListLFTags
                - lakeformation:GetLFTag
                - lakeformation:SearchTablesByLFTags
                - lakeformation:SearchDatabasesByLFTags
              Resource: "*"
```

If you are using **Path A** (Glue resource link in the default catalog), the role also needs Glue read on the default-catalog database/table wildcards so it can traverse the resource link:

```yaml
- PolicyName: DefaultCatalogTraversal
  PolicyDocument:
    Version: "2012-10-17"
    Statement:
      - Effect: Allow
        Action:
          - glue:GetDatabase
          - glue:GetDatabases
          - glue:GetTable
          - glue:GetTables
          - glue:GetPartition
          - glue:GetPartitions
        Resource:
          - !Sub "arn:aws:glue:${AWS::Region}:${AWS::AccountId}:catalog"
          - !Sub "arn:aws:glue:${AWS::Region}:${AWS::AccountId}:database/*"
          - !Sub "arn:aws:glue:${AWS::Region}:${AWS::AccountId}:table/*/*"
```

## Lake Formation grants for the query role

After the role exists, grant LF permissions on the catalog hierarchy. These are idempotent ("already exists" is OK).

### Path A grants (Glue resource link)

```bash
ROLE_ARN="arn:aws:iam::<account-id>:role/<role-name>"
BUCKET_NAME="<bucket-name>"
NAMESPACE="<namespace>"
ACCOUNT_ID="<account-id>"
LINK_NAME="<resource-link-name>"
REGION="<region>"

# Bucket-nested catalog (the actual data location)
aws lakeformation grant-permissions \
    --principal "DataLakePrincipalIdentifier=${ROLE_ARN}" \
    --resource "{\"Catalog\":{\"Id\":\"${ACCOUNT_ID}:s3tablescatalog/${BUCKET_NAME}\"}}" \
    --permissions DESCRIBE \
    --region "${REGION}"

aws lakeformation grant-permissions \
    --principal "DataLakePrincipalIdentifier=${ROLE_ARN}" \
    --resource "{\"Database\":{\"CatalogId\":\"${ACCOUNT_ID}:s3tablescatalog/${BUCKET_NAME}\",\"Name\":\"${NAMESPACE}\"}}" \
    --permissions DESCRIBE \
    --region "${REGION}"

aws lakeformation grant-permissions \
    --principal "DataLakePrincipalIdentifier=${ROLE_ARN}" \
    --resource "{\"Table\":{\"CatalogId\":\"${ACCOUNT_ID}:s3tablescatalog/${BUCKET_NAME}\",\"DatabaseName\":\"${NAMESPACE}\",\"TableWildcard\":{}}}" \
    --permissions SELECT DESCRIBE \
    --region "${REGION}"

# Resource link in the default catalog
aws lakeformation grant-permissions \
    --principal "DataLakePrincipalIdentifier=${ROLE_ARN}" \
    --resource "{\"Database\":{\"CatalogId\":\"${ACCOUNT_ID}\",\"Name\":\"${LINK_NAME}\"}}" \
    --permissions DESCRIBE \
    --region "${REGION}"
```

### Path B grants (auto-mount, LF mode required)

Same as Path A's bucket-nested-catalog grants. Skip the resource-link grant.

## `lf_grant` helper (don't swallow AccessDenied)

```bash
# Tolerates "already exists" but aborts on any other error.
# Pass the full aws CLI invocation as arguments.
lf_grant() {
  local out
  if out=$("$@" 2>&1); then
    return 0
  fi
  if echo "$out" | grep -q "already exists"; then
    return 0
  fi
  echo "lf_grant: failed:" >&2
  echo "$out" >&2
  return 1
}

# Usage
lf_grant aws lakeformation grant-permissions \
    --principal "DataLakePrincipalIdentifier=${ROLE_ARN}" \
    --resource "{\"Catalog\":{\"Id\":\"${ACCOUNT_ID}:s3tablescatalog/${BUCKET_NAME}\"}}" \
    --permissions DESCRIBE \
    --region "${REGION}"
```

The crucial difference from `... || true`: this aborts on `AccessDeniedException` (which means the caller is not a Data Lake Admin), making the failure visible at the LF step instead of much later as a downstream `glue:GetTable` error from a service that depended on the missing grant.

## Add the deploying identity as a Data Lake Admin

Read existing admins, append, put back. `put-data-lake-settings` is full-replace, so reading first is mandatory.

```bash
EXISTING=$(aws lakeformation get-data-lake-settings \
  --region "<region>" \
  --query 'DataLakeSettings.DataLakeAdmins[].DataLakePrincipalIdentifier' \
  --output text)

NEW_IDENTITY=$(aws sts get-caller-identity --query Arn --output text)

ADMINS_JSON=$(python3 - <<PY
import json, os
existing = os.environ['EXISTING'].split()
new = os.environ['NEW']
identities = [a for a in existing if a]
if new not in identities:
    identities.append(new)
print(json.dumps([{'DataLakePrincipalIdentifier': a} for a in identities]))
PY
)

EXISTING="$EXISTING" NEW="$NEW_IDENTITY" \
aws lakeformation put-data-lake-settings \
  --data-lake-settings "{\"DataLakeAdmins\": $ADMINS_JSON}" \
  --region "<region>"
```

Side note: assumed-role ARNs (`arn:aws:sts::<account-id>:assumed-role/<role>/<session>`) are not what LF expects in `DataLakePrincipalIdentifier`; use the underlying role ARN (`arn:aws:iam::<account-id>:role/<role>`) instead. Convert with a sed/awk transform if `get-caller-identity` returns the assumed-role form.
