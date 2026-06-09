# aws-data-skills

[![License: AGPL v3+](https://img.shields.io/badge/license-AGPL--3.0--or--later-blue)](./LICENSE)

Three independent, composable skills for building data pipelines on AWS.

## Skills

- **lakehouse-redshift** - Query and operate a Redshift Serverless lakehouse with Iceberg/S3 Tables integration.
- **cdc-streaming-pipeline** - Stand up a change-data-capture pipeline (Aurora DSQL -> Kinesis -> Lambda -> Redshift).
- **firehose-iceberg-pipeline** - Ingest streaming data into Apache Iceberg tables on S3 via Amazon Data Firehose.

## Format

Each skill is a folder containing a `SKILL.md` file in the [Anthropic skill format](https://docs.anthropic.com/en/docs/claude-code/skills). They are usable with Claude Code, Claude.ai, or the Anthropic SDK.

## Composition

The skills cross-reference each other. For example, if you're using `cdc-streaming-pipeline` and also need to query the resulting Redshift warehouse, load `lakehouse-redshift`. If `firehose-iceberg-pipeline` lands data you want to query alongside Redshift-native tables, `lakehouse-redshift` covers the federated query path. Each skill stands alone but composes cleanly with the others.
