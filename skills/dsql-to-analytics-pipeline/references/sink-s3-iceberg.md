# Sink: S3 Tables (Iceberg) via Firehose

This page is a routing layer. The Firehose-to-Iceberg sink has its own
set of footguns (transform Lambda, column-mapping by name, two-phase
deploy because Firehose validates the destination at create time) that
are owned by the sibling `streaming-into-data-lake` skill in the
data-analytics plugin family. This page covers ONLY the DSQL-CDC-shape
specifics that the consumer side needs to know about.

## When to pick this sink

- You need long-retention archive of CDC history (years), where the
  Redshift hot-path table would grow uneconomically large.
- You want the change history queryable from Athena, EMR, Glue, or
  Spark in addition to Redshift.
- You want time-travel and Iceberg's snapshot semantics for audit /
  replay scenarios.

## When NOT to pick this sink

- Your only consumer is Redshift Serverless and recent state is the
  primary access pattern. Stick with the Redshift sink in
  [sink-redshift.md](sink-redshift.md). The cold-path archive can be
  added later without changes to the hot path.
- You need sub-minute freshness end-to-end. Firehose buffers; the
  Lambda + Redshift Data API path is faster for recent state.

## Architecture sketch

```
DSQL CDC -> Kinesis Data Stream -> Firehose (with transform Lambda)
                                       |
                                       v
                              S3 Tables (Iceberg) bucket
                                       |
                                       v
                       Athena / Redshift external schema / Spark
```

Note: this is in addition to (not instead of) the Lambda + Redshift
Data API path on the hot side. A common production layout uses both:
the Lambda lands recent events into Redshift `cdc_events` for low
latency, while Firehose archives the same Kinesis stream into Iceberg
for long retention.

## Why a transform Lambda is required

Firehose's Iceberg destination maps top-level JSON keys to Iceberg
columns by name. The DSQL CDC envelope is nested:

```json
{
  "op": "c",
  "ts_ms": 1717508412345,
  "source": { "table": "orders" },
  "after": { "id": "...", "customer_id": "..." }
}
```

If you point Firehose at this envelope directly, you get an Iceberg
table with columns named `op`, `ts_ms`, `source`, `before`, `after`,
where `after` is a struct that few query engines unnest cleanly. To
land the SAME append-only shape used in the Redshift sink (one row
per CDC event, full row payload as JSON, source-table tag,
commit-timestamp), the transform Lambda must flatten the envelope:

```python
# Sketch only. Full transform-Lambda details and the CFN two-phase
# deploy live in the streaming-into-data-lake skill.
def lambda_handler(event, context):
    output = []
    for record in event["records"]:
        payload = json.loads(base64.b64decode(record["data"]))
        op = payload["op"]
        row = payload["after"] if op == "c" else payload["before"]
        flattened = {
            "source_table": payload.get("source", {}).get("table"),
            "operation": op,
            "record_id": str(row["id"]),
            "event_data": json.dumps(row),
            "commit_timestamp_ms": payload["ts_ms"],
        }
        output.append({
            "recordId": record["recordId"],
            "result": "Ok",
            "data": base64.b64encode(
                (json.dumps(flattened) + "\n").encode("utf-8")
            ).decode("utf-8"),
        })
    return {"records": output}
```

The destination Iceberg table is then a near-mirror of the Redshift
`cdc_events` shape:

```sql
CREATE TABLE cdc_events_archive (
    source_table          STRING,
    operation             STRING,
    record_id             STRING,
    event_data            STRING,   -- JSON; parse downstream
    commit_timestamp_ms   BIGINT
)
PARTITIONED BY (source_table, days(from_unixtime(commit_timestamp_ms / 1000)))
TBLPROPERTIES ('format-version' = '2');
```

(Exact DDL syntax depends on the Iceberg catalog: AWS Glue, S3 Tables
namespace, or REST. The `streaming-into-data-lake` skill in the
data-analytics plugin has the complete table-creation snippets.)

## Cross-reference: streaming-into-data-lake

For the Firehose-side details, load the `streaming-into-data-lake`
skill in the data-analytics plugin. It owns:

- The Firehose `IcebergDestinationConfiguration` shape, including the
  `DestinationTableConfigurationList` block and the IAM trust the
  Firehose service needs.
- The two-phase CFN deploy required because Firehose validates the
  destination Iceberg table synchronously at create time. The table
  must exist in the Glue / S3 Tables catalog BEFORE the Firehose
  resource is created, so split the stack: phase 1 creates the bucket
  + Glue database + Iceberg table, phase 2 adds the Firehose stream.
- Firehose role permissions for S3 Tables namespaces vs Glue catalogs
  (the policy shapes differ).
- Buffering / size hints (`SizeInMBs`, `IntervalInSeconds`) and
  small-file mitigation.
- Compaction and snapshot retention via S3 Tables auto-maintenance.

This page intentionally does not duplicate that material. If a
specific Firehose footgun bites you, that skill is the source of
truth.

## Querying the archive from Redshift Serverless

Once the Iceberg table is populated, you can read it from Redshift
Serverless via an external schema. The setup pattern (LF access-control
mode, federated Glue catalog ARN format with bucket-nested paths,
`WITH NO SCHEMA BINDING` views, hot+cold UNION) is owned by the
external-schema / lakehouse-style skill in your data-analytics plugin
family. Load that skill when you are ready to wire cross-engine reads.

## Related

- [append-only-pattern.md](append-only-pattern.md) for the row shape
  the transform Lambda emits.
- [sink-redshift.md](sink-redshift.md) for the hot-path sink that
  this archive complements.
- [cdc-stream-setup.md](cdc-stream-setup.md) for the upstream Kinesis
  + IAM trust that feeds both sinks.
