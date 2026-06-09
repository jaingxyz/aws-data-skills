# Firehose Transform Lambda

The Firehose transform Lambda has one job: take each input record and emit exactly one output record whose top-level JSON keys exactly match the destination Iceberg column names, with two extra metadata keys Firehose requires.

## The column-mapping footgun

Firehose writes records to Iceberg by parsing each record as JSON and matching top-level keys to Iceberg column names. The match is case-sensitive and exact. There is no implicit normalization.

Failure modes seen in production:

| Lambda emits | Iceberg schema has | Result |
|---|---|---|
| `record_id` | `recordid` | Every record discarded; error bucket logs `Iceberg.MissingColumnWithinRecord` |
| `customerId` | `customer_id` | Same |
| `Amount` | `amount` | Same |
| `event_ts` (ISO-8601 string) | `event_ts` (`timestamp`) | Records silently land with timestamps in year 1970 or year 50000+ |

Mitigation: write a unit test that diffs the Lambda's output keys against the Glue table schema before deploying. The deploy script SHOULD fail closed when the diff is non-empty.

## Microsecond timestamps

Iceberg `timestamp` and `timestamp_ntz` columns expect microseconds since epoch as int64. Common bugs:

```python
# CORRECT, primary case: source carries a millisecond timestamp (e.g.
# DSQL CDC `ts_ms`). Multiply by 1000 for microseconds. This preserves
# the source-side commit ordering, which is what downstream
# ROW_NUMBER reconstruction relies on.
output["event_ts"] = int(payload["ts_ms"]) * 1000

# CORRECT, when source provides a datetime object
output["event_ts"] = int(dt.timestamp() * 1_000_000)

# WRONG: wall-clock at the Lambda. Loses source commit ordering;
# multiple events in the same Firehose buffer share one timestamp.
output["event_ts"] = int(time.time() * 1_000_000)

# WRONG: milliseconds when the column type is timestamp, lands rows with
# timestamps ~1000x too small
output["event_ts"] = int(time.time() * 1000)

# WRONG: ISO string, Firehose discards as Iceberg.SchemaTypeMismatch because the type does not match
output["event_ts"] = datetime.utcnow().isoformat()
```

For `date` columns, emit days since epoch as int32. For `string` timestamps where Iceberg type is `string`, ISO-8601 is fine.

## Required output metadata

Per the Firehose data transformation contract, every output record MUST include:

- `recordId` (string) - echoed unchanged from the input record. Firehose uses this to acknowledge per-record success.
- `result` (string) - one of `Ok`, `Dropped`, `ProcessingFailed`. Anything else fails the entire batch.
- `data` (string, base64-encoded) - the JSON payload Firehose will parse for column matching. Required for `Ok`. Optional for `Dropped` / `ProcessingFailed`.

Additionally, ONLY when the stream is in MERGE mode (`AppendOnly: false`) or has multiple tables in `DestinationTableConfigurationList`, the record must also include:

- `metadata.otfMetadata.destinationDatabaseName` and `metadata.otfMetadata.destinationTableName` - per-record routing for fan-out.
- `metadata.otfMetadata.operation` - one of `insert`, `update`, `delete`.

For the typical CDC-archive case (`AppendOnly: true`, single static destination table), DO NOT include `metadata.otfMetadata`. Firehose interprets a present-but-stale `metadata` block as a routing decision and may reject the record. The companion repo's transform Lambda emits only `{recordId, result, data}` for this reason.

## Generalized template

```python
import base64
import json
import time
from typing import Any

def transform(payload: dict[str, Any]) -> dict[str, Any]:
    """Map a source record to the Iceberg column shape.
    Override this per pipeline. Return None to drop the record."""
    # Example: rename keys, convert timestamps to microseconds, attach a primary key.
    out = {
        "record_id": str(payload["id"]),
        "customer_id": payload["customerId"],
        "amount": float(payload["amount"]),
        "event_ts": int(payload["epochSeconds"] * 1_000_000),
    }
    return out

def lambda_handler(event, _context):
    output_records = []
    for record in event["records"]:
        record_id = record["recordId"]
        try:
            raw = base64.b64decode(record["data"]).decode("utf-8")
            payload = json.loads(raw)
            transformed = transform(payload)
            if transformed is None:
                output_records.append({
                    "recordId": record_id,
                    "result": "Dropped",
                    "data": record["data"],
                })
                continue
            encoded = base64.b64encode(
                json.dumps(transformed).encode("utf-8")
            ).decode("utf-8")
            # AppendOnly: true (the recommended default for CDC archives).
            # Single static destination table, no per-record routing.
            output_records.append({
                "recordId": record_id,
                "result": "Ok",
                "data": encoded,
            })
            # MERGE mode (AppendOnly: false) or fan-out: uncomment and set
            # the destination + operation. Firehose rejects stale metadata
            # blocks under AppendOnly, so leave commented in single-table mode.
            # output_records[-1]["metadata"] = {
            #     "otfMetadata": {
            #         "destinationDatabaseName": "<db>",
            #         "destinationTableName": "<table>",
            #         "operation": "insert",  # or "update" / "delete"
            #     }
            # }
        except Exception:  # noqa: BLE001
            output_records.append({
                "recordId": record_id,
                "result": "ProcessingFailed",
                "data": record["data"],
            })
    return {"records": output_records}
```

The default shape above is `AppendOnly: true`: one output record per input, no `metadata` block. For MERGE mode, uncomment the routing block and set the destination and operation per record.

## Lambda role permissions

The Lambda's execution role needs:

- Standard `AWSLambdaBasicExecutionRole` for CloudWatch Logs.
- No Iceberg or S3 permissions; the Lambda never writes to Iceberg directly. Firehose does that with the Firehose role.

## Buffer and timeout

- Lambda timeout: 60 seconds. Firehose times out the invoke at 60 s and treats the batch as failed.
- Memory: 256 MB is enough for JSON-only transforms. Bump to 1024 MB if the transform decompresses or parses large payloads.
- Reserved concurrency: leave unset. Firehose throttles itself.
