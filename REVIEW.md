# Skill Pre-Publish Review

The repository now hosts **five** skills across two flavors. The
original three (`lakehouse-redshift`, `cdc-streaming-pipeline`,
`firehose-iceberg-pipeline`) went through a full nine-review pass and
shipped GREEN. The two AWS-MCP-shaped skills (`dsql-to-analytics-pipeline`,
`streaming-into-data-lake`) are documented in their own section below;
they will go through the AWS MCP team's evaluation framework as the
next gate, not another local nine-review pass.

## Original three skills, nine reviews (open-source-security + code-review + doc-vs-code fidelity per skill)

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

## Publish-readiness call (original three)

**GREEN** - all open-source-security reviews pass, all hard constraints clean, all reviewer-flagged correctness blockers fixed, all deferred quality-polish items addressed with doc-cited corrections.

---

## AWS-MCP-shaped skills (added 2026-06-08)

Two additional skills consolidated into this repo from their original
authoring locations:

| Skill | Origin | Sanitization | Local review |
|---|---|---|---|
| `dsql-to-analytics-pipeline` | Personal fork of `awslabs/agent-plugins`, branch `add-dsql-to-analytics-pipeline-skill`. Already public-licensed (Apache-2.0); no Amazon-internal references in tree. | Not needed - clean as copied. | 3-reviewer panel pending (next step). |
| `streaming-into-data-lake` | Authored in a separate workspace and copied in for consolidation. | Submission-target frontmatter (`owner_team` / `owner_cti` / `stages`) and an internal eval-build reference removed; replaced with a comment about the submission-flavored copy and the canonical multi-model eval gate language (3 models x 3 runs). | 3-reviewer panel pending (next step). |

### Why these are tracked separately from the original three

These skills are scoped for AWS MCP server submission, not standalone
personal-repo distribution. The AWS MCP team runs its own evaluation
framework (>= 80 % task completion across 3 models x 3 runs, plus
selection tests in a selection-test suite and E2E tests in
an end-to-end skill eval suite) as the official quality gate. Adding a
redundant nine-review pass here would not change the publishing
decision.

The local 3-reviewer panel still runs on these (because every commit
goes through it per the project's standing rule), but the gates are
different: it's pre-check, not pre-publish. The pre-publish gate is
the AWS MCP team's review.

### Deferred items from the original authoring sessions

Carried over from the handoff document; tracked here so reviewers can
decide which to address before submission.

**`dsql-to-analytics-pipeline`** (handoff "Deferred quality-of-exposition items"):

- PK column parameterization (currently assumes `id` field).
- Transform Lambda PK guard.
- Partition transform on TIMESTAMP column.
- Missing imports / handler clients block in skeleton.
- IAM `Version: "2012-10-17"` boilerplate consistency.
- BatchSize comment math, IDENTITY guarantees wording, GRANT-to-PUBLIC framing, BisectBatchOnFunctionError walkthrough recovery story.

**`streaming-into-data-lake`** (handoff):

- Soften "microseconds since epoch MUST" claim with citation.
- Restructure SKILL.md vs `three-phase-deploy.md` phase tables.
- Rework LF grant CFN to use federated catalog ID.
- Add `UPDATE` to LF table grant for streams handling op=update.
- Replace `--help`-as-region-check; correct "files are gzipped" default.
- Date-column emit-shape; `recordId`/`operation` framing.
- CloudWatch Logs `:log-stream:*` ARN scoping in IAM.
- Error-bucket S3 lifecycle.

The submission-flavored copies of both skills (which will live in a
fork of `aws/agent-toolkit-for-aws` per the AWS MCP Skill Publishing
Process) need `owner_team` / `owner_cti` resolved + `stages: [preprod]`
re-added before they can move through intake. That work is downstream
of this consolidation commit.

## Publish-readiness call (AWS-MCP-shaped two)

**YELLOW** - copy-and-sanitization complete; local 3-reviewer panel
pending; AWS MCP submission gate (intake -> evals -> security review ->
paired contributions) is the binding pre-publish gate, not local review.
