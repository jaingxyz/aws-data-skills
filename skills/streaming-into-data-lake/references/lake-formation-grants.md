# Lake Formation Grants for Firehose

Firehose writes to Iceberg tables through the Glue catalog. Lake Formation gates every catalog access. The Firehose role MUST hold three grants. Missing any one produces an opaque `AccessDenied` from Firehose with no indication of which grant is missing.

## The grants (depends on stream mode)

The exact permissions depend on whether the stream is `AppendOnly: true` (the default for CDC archive use cases) or in MERGE mode (`AppendOnly: false` with per-record `update`/`delete`).

For `AppendOnly: true`:

| Resource | Permissions | What it lets Firehose do |
|---|---|---|
| Catalog | `DESCRIBE` | List databases under the catalog |
| Database | `DESCRIBE`, `CREATE_TABLE`, `ALTER` | Read metadata, evolve schema. `CREATE_TABLE` is needed for some Glue-side compaction operations Iceberg performs. |
| Table | `DESCRIBE`, `SELECT`, `INSERT`, `ALTER` | Read manifest, append rows, evolve schema. `DESCRIBE` is required even for insert-only streams (Lake Formation gates manifest reads under `DESCRIBE` + `SELECT`). |

For MERGE mode add `DELETE` to the Table grant.

The companion repo's working set (`07-deploy-iceberg.sh`) uses the AppendOnly grants and is the validated reference. Do not add `DELETE` until you switch the stream's `AppendOnly` flag to `false`; under append-only, granting unused `DELETE` is harmless but inconsistent with the principle of least privilege.

## CloudFormation form

```yaml
LFGrantOnCatalog:
  Type: AWS::LakeFormation::PrincipalPermissions
  Condition: CreateFirehoseRole
  Properties:
    Principal:
      DataLakePrincipalIdentifier: !GetAtt FirehoseRole.Arn
    Resource:
      Catalog: {}
    Permissions: [DESCRIBE]
    PermissionsWithGrantOption: []

LFGrantOnDatabase:
  Type: AWS::LakeFormation::PrincipalPermissions
  Condition: CreateFirehoseRole
  Properties:
    Principal:
      DataLakePrincipalIdentifier: !GetAtt FirehoseRole.Arn
    Resource:
      Database:
        CatalogId: !Ref AWS::AccountId
        Name: !Ref Namespace
    Permissions: [DESCRIBE, CREATE_TABLE, ALTER]

LFGrantOnTable:
  Type: AWS::LakeFormation::PrincipalPermissions
  Condition: CreateFirehoseRole
  Properties:
    Principal:
      DataLakePrincipalIdentifier: !GetAtt FirehoseRole.Arn
    Resource:
      Table:
        CatalogId: !Ref AWS::AccountId
        DatabaseName: !Ref Namespace
        Name: !Ref TableName
    Permissions: [DESCRIBE, SELECT, INSERT, ALTER]
    # For MERGE mode (AppendOnly: false), append DELETE to Permissions.
```

For S3 Tables targets, the catalog ID for the database and table grants is the federated catalog identifier `<account-id>:s3tablescatalog/<bucket>`, not the account id alone.

## Shell helper for incremental grants

CloudFormation's `AWS::LakeFormation::PrincipalPermissions` does not handle the case where the grant already exists. A re-run produces `ResourceAlreadyExistsException` and fails the deploy. For deploys that may be re-run (CI, idempotent scripts), use a shell helper that aborts on real errors and treats already-granted as success:

```bash
lf_grant() {
  local principal="$1"
  local resource_json="$2"
  local permissions="$3"
  local out

  out=$(aws lakeformation grant-permissions \
    --principal "DataLakePrincipalIdentifier=${principal}" \
    --resource "${resource_json}" \
    --permissions ${permissions} 2>&1) || {
    if echo "${out}" | grep -q "AlreadyExists\|already granted"; then
      echo "  already granted, skipping"
      return 0
    fi
    echo "ERROR: lf_grant failed:"
    echo "${out}"
    return 1
  }
  echo "  granted"
}

# Catalog grant
lf_grant "${FIREHOSE_ROLE_ARN}" \
  '{"Catalog":{}}' \
  "DESCRIBE"

# Database grant
lf_grant "${FIREHOSE_ROLE_ARN}" \
  "{\"Database\":{\"CatalogId\":\"${CATALOG_ID}\",\"Name\":\"${DB_NAME}\"}}" \
  "DESCRIBE"

# Table grant
lf_grant "${FIREHOSE_ROLE_ARN}" \
  "{\"Table\":{\"CatalogId\":\"${CATALOG_ID}\",\"DatabaseName\":\"${DB_NAME}\",\"Name\":\"${TABLE_NAME}\"}}" \
  "SELECT INSERT DELETE"
```

The function aborts on any error other than already-granted. The deploy script that calls it MUST run with `set -euo pipefail` so a failed grant stops the rest of the deploy.

## Data Lake Admin requirement

The identity running the Lake Formation grant calls MUST be a Data Lake Admin in the region. Without admin status, every grant call returns:

```
AccessDeniedException: Insufficient Lake Formation permission(s) on <resource>
```

This happens even when the IAM identity has `lakeformation:GrantPermissions` on `*`. Lake Formation enforces admin status separately from IAM. Verify with:

```bash
aws lakeformation get-data-lake-settings --region <region> \
  --query "DataLakeSettings.DataLakeAdmins[].DataLakePrincipalIdentifier" \
  --output text
```

If the calling ARN is not in the list, either run the deploy as a different identity or have an existing admin add the deploy identity. Adding a Data Lake Admin requires existing admin status and is not in scope for this skill; see `setting-up-lake-formation`.

## Verifying grants applied

```bash
aws lakeformation list-permissions \
  --principal DataLakePrincipalIdentifier=<firehose-role-arn> \
  --region <region>
```

Output should show three entries: one with `Catalog`, one with the database, one with the table. If any is missing, re-run the corresponding `lf_grant` call. Lake Formation grant propagation is usually under 5 seconds but can take up to 30 seconds.

`aws iam simulate-principal-policy` does NOT see Lake Formation grants. Do not use it to verify Firehose Iceberg permissions.
