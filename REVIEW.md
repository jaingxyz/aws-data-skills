# Skill Pre-Publish Review

Three skills, nine reviews (open-source-security + code-review + AutoCR fidelity per skill).

## Hard-constraint pass

`grep -rnE 'Spectrum|spectrum|—' skills/` returned **zero hits** in skill prose, both before and after fixes applied here. All three skills:

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
| AutoCR fidelity | 0.95 | All 9 in-scope items covered; minor gaps non-material. |

### `cdc-streaming-pipeline`

| Review | Verdict | Notes |
|---|---|---|
| open-source-security | OK | No blockers. |
| code-review | needs-fixes | **High: trust policy used `ArnEquals` with cluster ARN; DSQL passes the stream ARN via `aws:SourceArn`, so the condition would never match.** Medium: wrong Kinesis IAM action (`DescribeStream` vs `DescribeStreamSummary` + `ListShards`); missing `Version` field on PolicyDocument; SUPER unnesting example is muddled. |
| AutoCR fidelity | 1.0 | All 8 in-scope items covered. |

### `firehose-iceberg-pipeline`

| Review | Verdict | Notes |
|---|---|---|
| open-source-security | OK | No blockers. |
| code-review | ship | All findings low/info. Minor: `Lambda.UserBadResponse` errorCode may be misnamed; CloudWatch metric names worth verifying; Logs IAM resource pattern strict-mode requires `:log-stream:*` suffix. |
| AutoCR fidelity | 1.0 | All in-scope items covered. |

## Top blockers

1. **(cdc-streaming-pipeline) Trust policy `ArnEquals` cluster-ARN bug.** Would prevent DSQL from assuming the role at runtime, silently breaking the producer. **Fixed in this commit.**
2. **(cdc-streaming-pipeline) Kinesis IAM action mismatch.** `DescribeStream` is legacy; production uses `DescribeStreamSummary` + `ListShards`. **Fixed in this commit.**
3. **(lakehouse-redshift) Path A LF grants omit bucket-nested catalog DESCRIBE.** Inconsistent with deploy script and reference snippet. Customers may hit confusing downstream errors. **Fixed in this commit.**
4. **(lakehouse-redshift) S3 Tables to Lake Formation integration prerequisite undocumented.** New-account customers will see no `s3tablescatalog` parent catalog and every later step fails. **Fixed in this commit (Step 0).**

## Recommended fixes (deferred, beyond trivial)

- **lakehouse-redshift Path B**: document how to enable Redshift Serverless workgroup auto-mount, or remove Path B and recommend Path A only.
- **cdc-streaming-pipeline §SUPER unnesting**: rewrite the array-unnest example with a runnable PartiQL `FROM cdc_events e, e.event_data."tags" t` form.
- **cdc-streaming-pipeline §EventSourceMapping**: add `MaximumRecordAgeInSeconds` and DLQ `DestinationConfig.OnFailure` example.
- **firehose-iceberg-pipeline**: verify `Lambda.UserBadResponse` errorCode name and CloudWatch metric names against current AWS docs; tighten Logs IAM resource pattern.

## Fixes applied (this commit)

- `cdc-streaming-pipeline/SKILL.md`: trust policy now uses `ArnLike` with `${ClusterArn}/stream/*`; Kinesis IAM action set to `DescribeStreamSummary` + `ListShards`; added `Version: "2012-10-17"` to inline PolicyDocument.
- `lakehouse-redshift/SKILL.md`: added bucket-nested catalog DESCRIBE grant to inline Path A example; added warning callout that `DROP SCHEMA ... CASCADE` drops dependent views; fixed the Python admin-merge snippet to filter the literal `'None'` returned by `--output text` on empty fields; added Step 0 to "Putting it together" calling out the one-time S3 Tables to Lake Formation integration enable prerequisite.

## Publish-readiness call

**YELLOW** — all open-source-security reviews pass and all hard constraints are clean. The three high/medium correctness blockers (cdc trust policy, kinesis action, lakehouse catalog grant, lakehouse Step 0) are fixed in this commit. Remaining items are quality polish (Path B auto-mount docs, SUPER unnesting example, Firehose metric/errorCode verification) and can land in a follow-up before broad publish.
