---
name: lakehouse-redshift
description: Build a lakehouse on AWS that Redshift Serverless can query - S3 Tables (managed Apache Iceberg) for storage, the Glue federated catalog (s3tablescatalog) for metadata, Lake Formation for access control, and a Redshift Serverless external schema for SQL. Use when the user wants to "build a lakehouse on AWS", "set up a data lake on AWS", "query Iceberg from Redshift Serverless", "wire S3 Tables to Redshift", "set up the Glue federated catalog for S3 Tables", or "let Redshift Serverless read S3-resident Iceberg tables". Covers the highest time-wasted gotchas in this stack: !Ref AWS::S3Tables::TableBucket returning the ARN (not the name), the bucket-nested Glue catalog ARN format, the "<bucket>@s3tablescatalog" 3-part naming, IAM access control mode versus Lake Formation access control mode, and which Redshift external-schema path to pick. Does NOT cover Firehose-specific authoring (use firehose-iceberg-pipeline) or hot-path Kinesis/Lambda CDC ingestion (use cdc-streaming-pipeline).
---

# lakehouse-redshift

A high-level entry point for "I want a lakehouse on AWS that Redshift Serverless can query." This skill picks the architecture, names every ARN format you will hit, and steers you past the gotchas that have eaten hours of debugging time.

## When to use

Load this skill when the user wants:

- Redshift Serverless to query S3-resident Iceberg tables that are written by some other producer (Firehose, Spark, Athena, Glue jobs, Flink, an external pipeline).
- A "lakehouse on AWS" with managed Iceberg storage and Redshift as the query engine.
- To wire up the bucket-nested Glue federated catalog (`s3tablescatalog`) so a Redshift Serverless workgroup can run `SELECT` against tables in an S3 Tables bucket.
- To decide between IAM access control mode and Lake Formation access control mode for an S3 Tables bucket.

## When NOT to use

- The customer is running pure Redshift OLAP with all data inside Redshift-managed storage and there is no external lake. They do not need this stack.
- The customer wants the actual real-time CDC ingestion path (DSQL or RDS to Kinesis to Redshift). Use the `cdc-streaming-pipeline` skill.
- The customer is authoring a Firehose to Iceberg delivery stream (transform Lambda, `ProcessingConfiguration`, three-phase deploy, microsecond timestamps). Use the `firehose-iceberg-pipeline` skill. This skill stops at "you have an Iceberg table and Redshift can query it"; firehose-iceberg-pipeline picks up at "now write to it from Firehose."
- The customer is on provisioned Redshift (not Serverless). The DDL and IAM advice here assumes a Serverless workgroup + namespace. Some of it transfers, but you should validate against provisioned-Redshift docs first.

If the user is doing the full pipeline (CDC source -> Kinesis -> Firehose -> Iceberg -> Redshift), load all three skills. This one owns the "Iceberg + Redshift" half.

## Architecture

```
   Producers (Firehose, Spark, Athena, Glue, Flink)
                       |
                       v
           +---------------------------+
           |    S3 Tables bucket       |    managed Iceberg storage,
           |    (Apache Iceberg)       |    auto-compaction, snapshots
           +---------------------------+
                       |
                       | account-level federation (set up once)
                       v
           +---------------------------+
           |  Glue Data Catalog        |
           |    s3tablescatalog/       |    bucket-nested catalog
           |      <bucket-name>/       |    one per S3 Tables bucket
           |        <namespace>/       |    = "database"
           |          <table>          |
           +---------------------------+
                       |
                       | (LF mode required for some paths)
                       v
           +---------------------------+
           |    Lake Formation         |    grants gate Glue + S3 Tables
           |    grants                 |    reads
           +---------------------------+
                       |
                       v
           +---------------------------+
           |  Redshift Serverless      |    CREATE EXTERNAL SCHEMA ...
           |  workgroup + namespace    |    via Glue resource link
           |  (IAM role attached)      |    OR awsdatacatalog auto-mount
           +---------------------------+
```

The S3 Tables to Glue federation is configured once per AWS account/region; once it exists you get an `s3tablescatalog` parent catalog under your account's default Glue catalog, with one child catalog per S3 Tables bucket.

## ARN and naming gotchas (read this first)

These are the highest-time-wasted errors in this stack. Get them right up front.

### `!Ref AWS::S3Tables::TableBucket` returns the ARN, not the name

Unlike most CloudFormation resources, `!Ref` on an `AWS::S3Tables::TableBucket` returns the full ARN (`arn:aws:s3tables:<region>:<account-id>:bucket/<name>`), not the bare name. Every place you would naively write `${TableBucket}` in a `!Sub` will produce double-nested garbage like `arn:...:catalog/s3tablescatalog/arn:aws:s3tables:...`.

To get the bare bucket name:

```yaml
!Select [1, !Split ["/", !GetAtt TableBucket.TableBucketARN]]
```

Use this everywhere you need the name (catalog ARN, output exports, IAM policies that reference table-data ARNs).

A clean pattern is to compute the name once with `!Sub` and a substitution variable, so you do not repeat the `!Select`:

```yaml
CatalogArn: !Sub
  - "arn:aws:glue:${AWS::Region}:${AWS::AccountId}:catalog/s3tablescatalog/${BucketName}"
  - BucketName: !Select [1, !Split ["/", !GetAtt TableBucket.TableBucketARN]]
```

### Bucket-nested Glue catalog ARN format

The federated S3 Tables catalog has three layers in Glue:

```
arn:aws:glue:<region>:<account-id>:catalog                          # default catalog
arn:aws:glue:<region>:<account-id>:catalog/s3tablescatalog          # parent federated catalog
arn:aws:glue:<region>:<account-id>:catalog/s3tablescatalog/<bucket> # bucket-nested child catalog
```

Databases (= S3 Tables namespaces) and tables live under the bucket-nested child:

```
arn:aws:glue:<region>:<account-id>:database/s3tablescatalog/<bucket>/<namespace>
arn:aws:glue:<region>:<account-id>:table/s3tablescatalog/<bucket>/<namespace>/<table>
```

When some IAM policies request `glue:Get*` on databases/tables, the resource pattern with two slash segments before the bucket also matches, so `database/*/s3tablescatalog/*` and `table/*/s3tablescatalog/*/*` are good defensive patterns to include alongside the direct ones.

### Three-part Redshift naming for auto-mount

If you use Redshift's `awsdatacatalog` auto-mount path (Lake Formation mode required, see below), the SQL identifier for an S3 Tables Iceberg table is:

```sql
SELECT * FROM "<bucket>@s3tablescatalog".<namespace>.<table>;
```

Specifically:
- The catalog identifier is `"<bucket>@s3tablescatalog"` with double quotes and an `@` separator. Not a slash.
- It is a 3-part name (`catalog.schema.relation`), not 4-part. Common wrong attempt: `awsdatacatalog."s3tablescatalog/<bucket>".<ns>.<table>`. That is 4 parts and Redshift will reject it.

### `destinationDatabaseName` / namespace name regex

Names referenced through the federated catalog must match `[a-zA-Z0-9._]+`. No slashes, no hyphens. This bites you if you accidentally pass an ARN where a name is expected (see the `!Ref` gotcha above), or if you choose namespace/table names with hyphens.

Valid: `cdc`, `events.v1`, `customer_events`.
Invalid: `events-v1`, `cdc/events`.

## IAM access control mode vs Lake Formation mode

Every bucket-nested S3 Tables catalog runs in one of two modes. Pick one up front, because switching has consequences.

### IAM access control mode (default for new federations)

- The catalog's `CreateDatabaseDefaultPermissions` includes `IAM_ALLOWED_PRINCIPALS / ALL`.
- Access is gated entirely by IAM policies (`glue:*` + `s3tables:*`).
- Lake Formation grants are not consulted.
- Compatible with: most direct Glue/S3 Tables API callers (Firehose with explicit IAM, Spark with IAM, Athena workgroups configured for IAM-only).
- NOT compatible with: Redshift `awsdatacatalog` auto-mount (the 3-part `"<bucket>@s3tablescatalog"` syntax), and most "federated catalog SQL access" engines that resolve through Lake Formation by default.

### Lake Formation access control mode (required for Redshift auto-mount)

- The bucket is registered as a Lake Formation resource via `aws lakeformation register-resource`.
- The catalog's `CreateDatabaseDefaultPermissions` is empty (`[]`) - `IAM_ALLOWED_PRINCIPALS` removed.
- All access goes through LF grants. IAM policies must still permit `glue:*` and `s3tables:*` actions, but LF is the gate that says yes/no on individual databases and tables.
- Required for: Redshift Serverless `awsdatacatalog` auto-mount, Redshift external-schema reads via a Glue resource link, federated Athena queries that resolve through LF.

### How to register a bucket in LF mode

```bash
aws lakeformation register-resource \
  --resource-arn "<S3 Tables bucket ARN>" \
  --use-service-linked-role \
  --region "<region>"
```

If you already have a bucket-nested catalog set up in IAM mode, you switch by removing `IAM_ALLOWED_PRINCIPALS` from `CreateDatabaseDefaultPermissions` (via `aws glue update-catalog` on the bucket-nested catalog) and registering the bucket. WARNING: any engine that was relying on IAM-only access stops working at the moment of the switch unless you add explicit LF grants for that principal first. Do the grants first, then flip the mode.

### Recommendation

If the customer's only readers are Redshift Serverless (this skill's scope) and they have not already invested in IAM-mode tooling against this bucket, **start in LF mode**. Both Redshift external-schema paths work cleanly in LF mode, and you avoid a mode switch later. Only stay in IAM mode if there is a hard reason: a non-LF-aware engine already in production against the bucket, or an organizational LF-readiness gap.

## Two Redshift Serverless external-schema paths

You have two ways to make S3 Tables Iceberg tables queryable from a Redshift Serverless workgroup. Pick one based on your access-control mode.

### Path A: Glue resource link in the default catalog (works in either mode, recommended)

This is the path that worked in the source repo. You create a Glue **resource link** in the default catalog (account-id only, no `s3tablescatalog`) that points at the namespace inside the bucket-nested catalog. Then Redshift's `CREATE EXTERNAL SCHEMA` resolves through the default catalog.

Resource link create:

```bash
aws glue create-database \
  --region "<region>" \
  --cli-input-json '{
    "CatalogId": "<account-id>",
    "DatabaseInput": {
      "Name": "<link-name>",
      "TargetDatabase": {
        "CatalogId": "<account-id>:s3tablescatalog/<bucket-name>",
        "DatabaseName": "<namespace>"
      }
    }
  }'
```

LF grants on the link itself:

```bash
aws lakeformation grant-permissions \
  --principal "DataLakePrincipalIdentifier=<redshift-role-arn>" \
  --resource '{"Database":{"CatalogId":"<account-id>","Name":"<link-name>"}}' \
  --permissions DESCRIBE \
  --region "<region>"
```

LF grants on the target (bucket-nested) catalog, database, and tables.
The catalog-level DESCRIBE grant is required; without it the database
and table grants alone can fail in confusing ways downstream:

```bash
# Grant DESCRIBE on the bucket-nested catalog itself.
aws lakeformation grant-permissions \
  --principal "DataLakePrincipalIdentifier=<redshift-role-arn>" \
  --resource '{"Catalog":{"Id":"<account-id>:s3tablescatalog/<bucket-name>"}}' \
  --permissions DESCRIBE \
  --region "<region>"

aws lakeformation grant-permissions \
  --principal "DataLakePrincipalIdentifier=<redshift-role-arn>" \
  --resource '{"Database":{"CatalogId":"<account-id>:s3tablescatalog/<bucket-name>","Name":"<namespace>"}}' \
  --permissions DESCRIBE \
  --region "<region>"

aws lakeformation grant-permissions \
  --principal "DataLakePrincipalIdentifier=<redshift-role-arn>" \
  --resource '{"Table":{"CatalogId":"<account-id>:s3tablescatalog/<bucket-name>","DatabaseName":"<namespace>","TableWildcard":{}}}' \
  --permissions SELECT DESCRIBE \
  --region "<region>"
```

Redshift DDL (run as admin via the Data API or query editor):

```sql
DROP SCHEMA IF EXISTS cold CASCADE;
CREATE EXTERNAL SCHEMA cold
FROM DATA CATALOG
DATABASE '<link-name>'
IAM_ROLE '<redshift-role-arn>'
CATALOG_ID '<account-id>';
```

> **CASCADE drops dependent views.** If you have already built any
> views on top of `cold.*` (for example a `cdc_events_all` UNION view
> spanning hot + cold), `DROP SCHEMA ... CASCADE` will silently drop
> them. Reapply your view layer immediately after, or skip the DROP
> and use `CREATE EXTERNAL SCHEMA IF NOT EXISTS` for idempotency.

The IAM role attached to the Redshift Serverless namespace needs `glue:Get*` on the **default catalog** so it can traverse the resource link. Bucket-nested-only patterns are not enough; add the default-catalog wildcards too:

```yaml
- Effect: Allow
  Action:
    - glue:GetDatabase
    - glue:GetDatabases
    - glue:GetTable
    - glue:GetTables
    - glue:GetPartition
    - glue:GetPartitions
  Resource:
    - "arn:aws:glue:<region>:<account-id>:catalog"
    - "arn:aws:glue:<region>:<account-id>:database/*"
    - "arn:aws:glue:<region>:<account-id>:table/*/*"
```

After deploying, query as:

```sql
SELECT COUNT(*) FROM cold.<table-name>;
```

### Path B: `awsdatacatalog` auto-mount via 3-part naming (LF mode + federated identity only)

If the bucket is in LF mode AND your Redshift Serverless workgroup has the in-database `data_catalog_auto_mount` system parameter set, AND your callers connect with a **federated IAM identity** (Query Editor v2 "Federated user" sign-in, or JDBC/ODBC with `redshift-serverless:GetCredentials`), you can skip the resource link and `CREATE EXTERNAL SCHEMA` entirely. Just query:

```sql
SELECT COUNT(*) FROM "<bucket>@s3tablescatalog".<namespace>.<table>;
```

#### Enabling auto-mount

Auto-mount is **not** a `redshift-serverless update-workgroup` flag. It is an in-database system parameter that you set via SQL on the workgroup, then bounce compute:

```sql
-- Connect as admin (Data API or Query Editor v2 admin connection),
-- run on the dev database (or any database in the namespace).
ALTER SYSTEM SET data_catalog_auto_mount = on;
SHOW data_catalog_auto_mount;   -- expect: on
```

The change takes effect on the **next compute cycle** (auto-pause and resume of the workgroup). Force a cycle by waiting past the idle timeout, or by toggling `--max-capacity` to a new value and back via `aws redshift-serverless update-workgroup`. After the cycle, `awsdatacatalog` becomes visible to federated-IAM connections.

#### Per-caller grants (in addition to the namespace IAM role's permissions)

Auto-mount alone is not enough; the **federated identity making the query** also needs:

1. **In-database grant** on the auto-mounted database (one-time per principal):

   ```sql
   GRANT USAGE ON DATABASE awsdatacatalog TO "IAMR:<caller-role-name>";
   ```

2. **Lake Formation grants** on the namespace and table for the caller's IAM role (mirrors Path A's grants, but the principal is the caller's role, not the namespace's role):

   ```bash
   aws lakeformation grant-permissions \
     --principal DataLakePrincipalIdentifier=<caller-role-arn> \
     --resource '{"Database":{"CatalogId":"<account-id>:s3tablescatalog/<bucket-name>","Name":"<namespace>"}}' \
     --permissions DESCRIBE
   aws lakeformation grant-permissions \
     --principal DataLakePrincipalIdentifier=<caller-role-arn> \
     --resource '{"Table":{"CatalogId":"<account-id>:s3tablescatalog/<bucket-name>","DatabaseName":"<namespace>","Name":"<table>"}}' \
     --permissions SELECT DESCRIBE
   ```

#### Path B caveats

- The `<bucket>@s3tablescatalog` identifier must be **double-quoted** in psql, JDBC, and most clients because of the `@`. Query Editor v2 quotes it for you.
- **DB-user / admin-password connections cannot use Path B.** They cannot resolve `awsdatacatalog` at all; the identity must be IAM-federated (Query Editor v2 "Federated user", or `GetCredentials` flow). If your pipeline writes via the Redshift Data API as the admin user, the deploy script itself cannot use Path B; reserve Path B for human users hitting Query Editor v2.
- Auto-mount honors only the **default Glue Data Catalog of the workgroup's account and Region**. Cross-account S3 Tables still require a Path A resource link.

### Which to recommend

- **If your only consumer is a federated human user via Query Editor v2** and LF mode is acceptable account-wide: Path B is one fewer moving part (no resource link, no `CREATE EXTERNAL SCHEMA`).
- **If your pipeline does any DDL or queries from Lambda / Data API / DB-user sessions**: Path A. Path B will not resolve `awsdatacatalog` for non-federated identities, so the same data has to be exposed via Path A anyway. Maintaining only Path A is simpler.
- **If the user already has IAM-mode buckets they cannot switch**: Path A. Path B requires LF mode.
- **In doubt, default to Path A.** It works for every caller type, matches the pattern most existing AWS samples document, and is what the source repo for this skill ships.

## Lake Formation operational gotchas

### `GrantPermissions` requires Data Lake Admin

A plain IAM admin (even with `*` on `lakeformation:*`) cannot run `lakeformation grant-permissions` unless they are a registered Data Lake Administrator on this account. Without admin status the call returns `AccessDeniedException` with no useful message.

Add the calling identity:

```bash
# Read existing admins first - this call is FULL REPLACE.
EXISTING=$(aws lakeformation get-data-lake-settings --region <region> \
  --query 'DataLakeSettings.DataLakeAdmins[].DataLakePrincipalIdentifier' \
  --output text)

# Append your identity, then put the merged list back.
NEW_IDENTITY="arn:aws:iam::<account-id>:role/<your-role>"
ADMINS_JSON=$(python3 -c "
import json, os
# --output text returns the literal string 'None' when the field is empty;
# strip it out so we don't write a bogus admin entry.
existing = [a for a in os.environ['EXISTING'].split() if a and a != 'None']
new = os.environ['NEW']
if new not in existing: existing.append(new)
print(json.dumps([{'DataLakePrincipalIdentifier': a} for a in existing]))
")

EXISTING="$EXISTING" NEW="$NEW_IDENTITY" \
aws lakeformation put-data-lake-settings \
  --data-lake-settings "{\"DataLakeAdmins\": $ADMINS_JSON}" \
  --region "<region>"
```

### `put-data-lake-settings` is full-replace

The whole `DataLakeSettings` object is replaced on every call. ALWAYS read first, append, then put. Forgetting this clobbers existing admins and existing default permissions, breaking other workloads on the account. There is no diff/patch mode.

### Do not swallow grant errors with `|| true`

LF grants are idempotent for the "already exists" case but `AccessDenied` (caller not a Data Lake Admin) hits the same error path. Wrapping the grant in `|| true` to make it idempotent silently swallows missing-admin errors, and the missing grants then surface much later as opaque downstream failures (`Role ... is not authorized to perform: glue:GetTable`). Either match on the error message or use a wrapper that allows only the expected idempotency cases:

```bash
lf_grant() {
  local out
  if out=$("$@" 2>&1); then return 0; fi
  if echo "$out" | grep -q "already exists"; then return 0; fi
  echo "$out" >&2
  return 1
}
```

## Redshift Serverless workgroup needs an IAM role

`CREATE EXTERNAL SCHEMA ... IAM_ROLE default` requires that the workgroup's namespace has a default IAM role associated. Redshift Serverless namespaces do NOT have a default role unless you attach one. If the customer's existing workgroup was provisioned without one (e.g., they were only using the Data API with IAM auth), you must attach the role for external schemas to work.

Two options:

**Option 1: Attach via CloudFormation if you own the workgroup definition.**

```yaml
WorkGroup:
  Type: AWS::RedshiftServerless::Workgroup
  Properties:
    # ...
Namespace:
  Type: AWS::RedshiftServerless::Namespace
  Properties:
    NamespaceName: !Ref NamespaceName
    IamRoles:
      - !GetAtt RedshiftQueryRole.Arn
    DefaultIamRoleArn: !GetAtt RedshiftQueryRole.Arn
```

**Option 2: Attach out-of-band via the API.** Useful when the workgroup is in another stack and you cannot do a circular import. Read existing roles, append, update:

```bash
EXISTING=$(aws redshift-serverless get-namespace \
  --namespace-name "<namespace>" --region "<region>" \
  --query 'namespace.iamRoles' --output text)

# build a JSON list of existing + new, dedup, then:
aws redshift-serverless update-namespace \
  --namespace-name "<namespace>" \
  --iam-roles "$ROLES_JSON" \
  --default-iam-role-arn "<role-arn>" \
  --region "<region>"
```

Either way, when you reference this in prose call it the **Redshift Serverless external-schema role** or **Redshift query role**. (Some older AWS samples and the source repo for this skill name it with a legacy term that no longer reflects the Serverless architecture; do not propagate that name into new content.)

## Putting it together (recommended sequence)

For a new lakehouse from scratch:

0. **(One-time per account/region) Enable the S3 Tables to Lake Formation / SageMaker Lakehouse integration.** Until this is on, the `s3tablescatalog` parent catalog will not appear under your default Glue catalog and every later step that references `s3tablescatalog/<bucket>` will fail with NotFound. Enable via the S3 Tables console "Enable integration" button or the documented CLI/API equivalent. Skip this if integration is already on.
1. **Create the S3 Tables bucket and namespace** in CloudFormation. Use the `!Select` pattern for any place you reference the bucket name. Keep tables OUT of the CFN template (see "S3 Tables CFN gotchas" in `reference/cfn-gotchas.md`).
2. **Register the bucket in Lake Formation.** `aws lakeformation register-resource ...`.
3. **Make sure the deploying identity is a Data Lake Admin.** `put-data-lake-settings` with the existing admins preserved + the deploying identity appended.
4. **Create the Iceberg table from a script with retries** (5+ minutes). The S3 Tables namespace propagation lag means CFN-side `AWS::S3Tables::Table` and short-lived custom resources both lose the race.
5. **Create the Redshift Serverless query IAM role.** `glue:Get*` on the default catalog, `glue:Get*` on the bucket-nested catalog, `s3tables:Get*`/`s3tables:ListTables`, `lakeformation:GetDataAccess`.
6. **Attach the role to the Redshift Serverless namespace** with `default-iam-role-arn`.
7. **Grant Lake Formation permissions** to that role on the bucket-nested catalog, the namespace database, and the table wildcard.
8. **Create the Glue resource link** in the default catalog pointing at the namespace.
9. **Grant LF DESCRIBE on the link** to the Redshift query role.
10. **Run `CREATE EXTERNAL SCHEMA`** in Redshift via the Data API.
11. **Verify**: `SELECT COUNT(*) FROM <schema>.<table>;`.

If the customer is also setting up a Firehose writer, hand off to `firehose-iceberg-pipeline` after step 4 (the table must exist before Firehose validates its destination at create time). If they are doing the upstream CDC ingestion, hand off to `cdc-streaming-pipeline` for the producer side.

## Redshift views over external tables: `WITH NO SCHEMA BINDING`

If you want to define Redshift views that reference an external schema (e.g., a UNION view across hot Redshift-managed tables and cold S3 Tables Iceberg tables), you must use `WITH NO SCHEMA BINDING`:

```sql
CREATE OR REPLACE VIEW public.events_all AS
SELECT ... FROM public.events_hot
UNION ALL
SELECT ... FROM cold.events_archive
WITH NO SCHEMA BINDING;
```

Two consequences:
- All relation names inside the view must be **fully qualified** (`public.events_hot`, not `events_hot`). Otherwise Redshift errors with "All the relation names inside should be qualified."
- Any view that references this view must also be `WITH NO SCHEMA BINDING`. The annotation is transitive.
- Redshift no longer tracks dependencies on the underlying tables. Schema changes on the Iceberg side will not block view changes; they will surface as runtime errors. Acceptable when the Iceberg schema is fixed by your deploy script; risky when an unrelated team owns the writer.

## Cross-references

- **`firehose-iceberg-pipeline`** - if the customer is writing to the Iceberg table from a Kinesis Data Firehose delivery stream. That skill covers the synchronous-validation-at-create-time gotcha, the `ProcessingConfiguration` transform Lambda needed to reshape records, the microsecond timestamp encoding, the three-phase deploy pattern, and the `EnableFirehose`/`EnableFirehoseStream` parameter inheritance trap. Load it ALONGSIDE this skill if the lakehouse is fed by Firehose.
- **`cdc-streaming-pipeline`** - if the customer is also building the upstream CDC ingestion path (DSQL or RDS to Kinesis to Redshift hot path). That skill covers the Kinesis Data API parameter limit (200 per statement, 40 rows per chunk), Lambda chunked execution with poll-to-FINISHED, and the append-only `ROW_NUMBER OVER` reconstruction pattern. Pair it with this skill when the user wants both a hot path and a cold lakehouse path on the same data.

## Reference

- `reference/cfn-gotchas.md` - the full list of CloudFormation footguns specific to S3 Tables (TableBucket `!Ref` pitfall, namespace propagation lag, bucket name reservation cooldown, `BucketSuffix` parameter pattern).
- `reference/iam-snippets.md` - copy-pasteable IAM policy fragments for the Redshift Serverless query role and a Lake Formation grant helper.
