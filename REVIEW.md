# Skill Pre-Publish Review

Three skills, nine reviews (open-source-security + code-review + doc-vs-code fidelity per skill).

## Hard-constraint pass

`grep -rnE 'Spectrum|spectrum|-' skills/` returned **zero hits** in skill prose, both before and after fixes applied here. All three skills:

- Use Redshift Serverless terminology only (no provisioned-cluster assumptions).
- Do not use the term "Spectrum" anywhere; the legacy IAM role name from the source repo is renamed in prose to "Redshift Serverless external-schema role" / "Redshift query role".
- Use ASCII hyphens only; no em dashes.
- Contain no Amazon-internal references, internal URLs, employee emails, internal account IDs, or hardcoded secrets.

## Per-skill summary

### `lakehouse-redshift`

| Review | Verdict | Notes |
|---|---|---|
| open-source-security | OK | No blockers. Service principals are public AWS (`redshift-serverless.amazonaws.com`). |
| code-review | needs-fixes | High: missing prerequisite for S3 Tables to LF integration enable; missing Path B auto-mount enable instructions. Medium: inline Path A LF grants omit catalog DESCRIBE; `DROP SCHEMA CASCADE` footgun; Python `None` admin literal bug. |
| doc-vs-code fidelity | 0.95 | All 9 in-scope items covered; minor gaps non-material. |

### `cdc-streaming-pipeline`

| Review | Verdict | Notes |
|---|---|---|
| open-source-security | OK | No blockers. |
| code-review | needs-fixes | **High: trust policy used `ArnEquals` with cluster ARN; DSQL passes the stream ARN via `aws:SourceArn`, so the condition would never match.** Medium: wrong Kinesis IAM action (`DescribeStream` vs `DescribeStreamSummary` + `ListShards`); missing `Version` field on PolicyDocument; SUPER unnesting example is muddled. |
| doc-vs-code fidelity | 1.0 | All 8 in-scope items covered. |

### `firehose-iceberg-pipeline`

| Review | Verdict | Notes |
|---|---|---|
| open-source-security | OK | No blockers. |
| code-review | ship | All findings low/info. Minor: `Lambda.UserBadResponse` errorCode may be misnamed; CloudWatch metric names worth verifying; Logs IAM resource pattern strict-mode requires `:log-stream:*` suffix. |
| doc-vs-code fidelity | 1.0 | All in-scope items covered. |

## Top blockers

1. **(cdc-streaming-pipeline) Trust policy `ArnEquals` cluster-ARN bug.** Would prevent DSQL from assuming the role at runtime, silently breaking the producer. **Fixed in this commit.**
2. **(cdc-streaming-pipeline) Kinesis IAM action mismatch.** `DescribeStream` is legacy; production uses `DescribeStreamSummary` + `ListShards`. **Fixed in this commit.**
3. **(lakehouse-redshift) Path A LF grants omit bucket-nested catalog DESCRIBE.** Inconsistent with deploy script and reference snippet. Customers may hit confusing downstream errors. **Fixed in this commit.**
4. **(lakehouse-redshift) S3 Tables to Lake Formation integration prerequisite undocumented.** New-account customers will see no `s3tablescatalog` parent catalog and every later step fails. **Fixed in this commit (Step 0).**

## Polish fixes (applied in follow-up commit)

All four deferred items addressed; doc-cited corrections from current AWS sources.

- **lakehouse-redshift Path B**: rewrote with the actual enable mechanism - `ALTER SYSTEM SET data_catalog_auto_mount = on` (an in-database system parameter, not a `redshift-serverless update-workgroup` flag) plus a workgroup pause/resume cycle. Added the per-caller `GRANT USAGE ON DATABASE awsdatacatalog` plus LF grants on the caller's IAM role. Added a hard caveat: Path B works only for federated-IAM connections (Query Editor v2, JDBC with `GetCredentials`); DB-user/admin-password sessions cannot resolve `awsdatacatalog`. Default recommendation is now Path A; Path B reserved for human Query Editor v2 use.
- **cdc-streaming-pipeline §SUPER unnesting**: replaced the broken `WHERE element IN (SELECT ...)` example with the canonical PartiQL cross-join form (`FROM cdc_events AS e, e.event_data."tags" AS t`), plus an arrays-of-objects variant, plus the `enable_case_sensitive_identifier` session prerequisite.
- **cdc-streaming-pipeline EventSourceMapping reference**: added `MaximumRecordAgeInSeconds: 21600` (6h cap), real `DestinationConfig.OnFailure` wired to a new `CdcDlq` SQS resource (14d retention) with the note that DLQ messages are pointers, not records, so replay requires re-reading the stream.
- **firehose-iceberg-pipeline**: replaced `Lambda.UserBadResponse`/`BadRequest` (not real codes) with the documented set: `Lambda.JsonProcessingException`, `Lambda.MissingRecordId`, `Lambda.DuplicatedRecordId`. Replaced `DeliveryToLambdaFailedRecords`/`DeliveryToIcebergRecords`/`DeliveryToIcebergFailedRecords` (not real metrics) with the canonical `DeliveryToIceberg.SuccessfulRowCount`, `DeliveryToIceberg.FailedRowCount`, and `ExecuteProcessingFailure.Records`. Tightened the Logs IAM resource ARN to `${FirehoseLogGroup.Arn}:log-stream:*` (the canonical stream-level ARN form).

## Initial-commit fixes (parent commit)

- `cdc-streaming-pipeline/SKILL.md`: trust policy now uses `ArnLike` with `${ClusterArn}/stream/*`; Kinesis IAM action set to `DescribeStreamSummary` + `ListShards`; added `Version: "2012-10-17"` to inline PolicyDocument.
- `lakehouse-redshift/SKILL.md`: added bucket-nested catalog DESCRIBE grant to inline Path A example; added warning callout that `DROP SCHEMA ... CASCADE` drops dependent views; fixed the Python admin-merge snippet to filter the literal `'None'` returned by `--output text` on empty fields; added Step 0 to "Putting it together" calling out the one-time S3 Tables to Lake Formation integration enable prerequisite.

## Publish-readiness call

**GREEN** - all open-source-security reviews pass, all hard constraints clean, all reviewer-flagged correctness blockers fixed, all deferred quality-polish items addressed with doc-cited corrections.
