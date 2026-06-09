# aws-data-skills

[![License: AGPL v3+](https://img.shields.io/badge/license-AGPL--3.0--or--later-blue)](./LICENSE)

Composable skills for building real-time analytics pipelines on AWS.
Two flavors:

- **Comprehensive learning skills** (~500-700 lines each, narrative
  format) - for someone reading the repo and wanting the full
  pattern with rationale, gotchas, and worked examples.
- **AWS-MCP-shaped skills** (~200 lines, references on demand) -
  staged for submission to the public
  [aws/agent-toolkit-for-aws][toolkit] repo (and from there into the
  AWS MCP server registry).

The two flavors overlap topically. They are kept side-by-side
deliberately: the comprehensive ones teach the patterns; the
AWS-MCP-shaped ones are the artifact that, after the AWS MCP
publishing process (intake, evals, security review, paired
contributions), will be vended through the AWS MCP server.

[toolkit]: https://github.com/aws/agent-toolkit-for-aws

## Skills

### Comprehensive learning skills

| Skill | Topic | When to load |
|---|---|---|
| [`lakehouse-redshift`](skills/lakehouse-redshift/SKILL.md) | Wire a Redshift Serverless workgroup to query S3 Tables (Iceberg) via the federated `s3tablescatalog`. External schema, IAM-role attachment, 3-part naming, `WITH NO SCHEMA BINDING` views. | Querying Iceberg from Redshift Serverless. |
| [`cdc-streaming-pipeline`](skills/cdc-streaming-pipeline/SKILL.md) | Real-time CDC pipeline: transactional source -> Kinesis -> Lambda -> Redshift Serverless. Append-only event log + ROW_NUMBER reconstruction, SUPER + JSON_PARSE, Redshift Data API parameter cap, async statement polling, poison-record handling. | Source-agnostic CDC pipeline. Aurora DSQL preview specifics flagged. |
| [`firehose-iceberg-pipeline`](skills/firehose-iceberg-pipeline/SKILL.md) | Amazon Data Firehose -> S3 Tables Iceberg via `IcebergDestinationConfiguration`. Column-shape footgun, microsecond timestamps, three-phase deploy around Firehose's synchronous `glue:GetTable` validation, error-bucket decoding. | Streaming ingest to Iceberg with the column reshape pattern. |

### AWS-MCP-shaped skills (staged for submission)

| Skill | Submission target | Topic |
|---|---|---|
| [`dsql-to-analytics-pipeline`](skills/dsql-to-analytics-pipeline/SKILL.md) | [`aws/agent-toolkit-for-aws`][toolkit] (databases-on-aws plugin) | Aurora DSQL -> analytics: subset of `cdc-streaming-pipeline` scoped to DSQL specifically, with references for on-demand loading. |
| [`streaming-into-data-lake`](skills/streaming-into-data-lake/SKILL.md) | [`aws/agent-toolkit-for-aws`][toolkit] (aws-data-analytics plugin) | Firehose -> Iceberg: subset of `firehose-iceberg-pipeline`, structured to fit a multi-model evaluation gate (3 models x 3 runs >= 80% task completion). |

The submission flow for the AWS-MCP-shaped skills is documented in
the AWS MCP Skill Publishing Process (intake -> evals -> paired
contributions to the public toolkit and the internal skill registry).
The versions here are the public learning copies; a submission-flavored
copy with the deployment-metadata frontmatter the registry requires
lives separately when the publishing process kicks off.

## Format

Each skill is a folder with a top-level `SKILL.md` and an optional
`references/` subdirectory of on-demand-loadable documents. Format
follows the [Anthropic skill specification][skillspec]. Skills are
usable with Claude Code, Claude.ai, or the Anthropic SDK directly.

[skillspec]: https://docs.anthropic.com/en/docs/claude-code/skills

## Composition

The skills cross-reference each other:

- `cdc-streaming-pipeline` (or `dsql-to-analytics-pipeline`) +
  `lakehouse-redshift` - hot path landing in Redshift, with
  external-schema setup for any cold-path Iceberg view.
- `cdc-streaming-pipeline` (or `dsql-to-analytics-pipeline`) +
  `firehose-iceberg-pipeline` (or `streaming-into-data-lake`) -
  hot path + cold path tee, both fed off the same Kinesis stream.
- `firehose-iceberg-pipeline` (or `streaming-into-data-lake`) +
  `lakehouse-redshift` - Iceberg ingest + Redshift querying it.

Each skill stands alone but composes cleanly with the others.

## Companion repository

The `cdc-streaming-pipeline`, `firehose-iceberg-pipeline`, and tiering
patterns are exercised end-to-end in the
[dsql-redshift-cdc-pipeline](https://github.com/jaingxyz/dsql-redshift-cdc-pipeline)
sample repository: live CFN templates, deploy scripts, transform
Lambdas, and unified-view SQL all driven through a working e-commerce
simulator. Read those skills first, then run the companion repo to
see them in production-shaped infrastructure.
