# Consumer Lambda Correctness Patterns

Each pattern below has a "this is what breaks if you skip it" footnote.
Each was earned by debugging a real silent-data-loss class.

## Why the patterns matter

The Redshift Data API is async. The Kinesis event source mapping
treats Lambda success as "advance the iterator". The combination
means a Lambda that returns 200 OK without verifying the SQL actually
ran will quietly lose data and the only way to discover it is when a
downstream view is missing rows days later. Every pattern here exists
to keep that from happening.

---

## 1. Parameterized SQL only. Never string-concatenate.

The Redshift Data API supports named parameters via the `Parameters=`
argument on `execute_statement`. Use them. CDC payloads contain
user-supplied data from the source DB; concatenating it into SQL is
unsafe and wrong, even for an internal pipeline.

```python
def _build_parameterized_insert(rows: list[dict]) -> tuple[str, list[dict]]:
    value_clauses = []
    parameters = []
    for i, r in enumerate(rows):
        # CAST(:ts AS BIGINT) is required: the Data API type-infers
        # numeric parameters as INTEGER, but ms-since-epoch (13 digits)
        # overflows INT4. Dividing by 1000.0 (not 1000) preserves
        # sub-second precision in commit_timestamp; integer division
        # truncates to whole seconds and the ROW_NUMBER ordering will
        # tie-break poorly under bursty writes.
        value_clauses.append(
            f"(:t{i}, :op{i}, :id{i}, JSON_PARSE(:d{i}), "
            f"TIMESTAMP 'epoch' + CAST(:ts{i} AS BIGINT) / 1000.0 * INTERVAL '1 second')"
        )
        parameters.extend([
            {"name": f"t{i}",  "value": r["table"]},
            {"name": f"op{i}", "value": r["op"]},
            {"name": f"id{i}", "value": str(r["record_id"])},
            {"name": f"d{i}",  "value": json.dumps(r["row"])},
            {"name": f"ts{i}", "value": str(r["commit_ts_ms"])},
        ])
    sql = (
        "INSERT INTO cdc_events "
        "(source_table, operation, record_id, event_data, commit_timestamp) "
        f"VALUES {', '.join(value_clauses)}"
    )
    return sql, parameters
```

Two non-obvious bits:

- **`JSON_PARSE(:d{i})`** converts the JSON-string parameter into a
  `SUPER` value at insert time. Without it, the value lands as plain
  text and `event_data."col"::TYPE` subscripting fails at view time.
- **`CAST(:ts{i} AS BIGINT)`** is required. The Data API type-infers
  numeric params as INTEGER (INT4); 13-digit millisecond timestamps
  overflow silently and you get a wrong (or NULL) commit timestamp
  with no error.

What breaks if you skip parameterization: SQL injection on any source
field that contains a quote, a backslash, or a non-ASCII character.

## 2. The Redshift Data API has a 200-parameter cap. Chunk to stay under it.

A single `execute_statement` rejects more than 200 parameters. With 5
parameters per CDC row, that means at most 40 rows per statement.

```python
PARAMS_PER_ROW = 5
MAX_PARAMS_PER_STATEMENT = 200
ROWS_PER_CHUNK = int(os.environ.get(
    "ROWS_PER_CHUNK", MAX_PARAMS_PER_STATEMENT // PARAMS_PER_ROW
))  # 40
```

If you raise the per-row parameter count (e.g. by adding columns to the
INSERT), `ROWS_PER_CHUNK` MUST come down accordingly. Do not override
the env var above `200 // PARAMS_PER_ROW` without re-doing the math.

What breaks if you skip chunking: a `BatchSize: 100` Kinesis batch
sends 500 parameters in one call, the Data API returns
`Number of parameters in statement exceeds maximum allowed (200)`,
the Lambda raises, the batch retries, the bisect splits it, every
half still has more than 200 parameters, every retry fails the same
way, and after `MaximumRetryAttempts` the records age out into the
DLQ. Total throughput: zero.

## 3. `execute_statement` is async. ALWAYS poll `describe_statement`.

This is the single most dangerous footgun.
`redshift-data:ExecuteStatement` returns a statement ID immediately;
the SQL has not run yet. If you do not poll for completion:

- A FAILED statement looks like SUCCESS to the Lambda.
- The Lambda returns 200 OK to Kinesis.
- Kinesis advances the shard iterator past records that never landed
  in Redshift.
- You discover the data loss days later when current-state views are
  missing rows.

```python
def _await_statement(statement_id: str) -> None:
    deadline = time.monotonic() + STATEMENT_POLL_TIMEOUT_S
    delay = 0.2
    while True:
        resp = redshift.describe_statement(Id=statement_id)
        status = resp.get("Status")
        if status == "FINISHED":
            return
        if status in ("FAILED", "ABORTED"):
            raise RuntimeError(
                f"Redshift statement {statement_id} ended in {status}: "
                f"{resp.get('Error', '<no error message>')}"
            )
        if time.monotonic() > deadline:
            raise RuntimeError(
                f"Redshift statement {statement_id} did not finish within "
                f"{STATEMENT_POLL_TIMEOUT_S}s (last status={status})"
            )
        time.sleep(delay)
        delay = min(delay * 2, 1.0)  # exponential backoff with cap
```

Exponential backoff capped at 1 second so a long-running statement
does not waste Lambda execution budget on sleeps.

What breaks if you skip the poll: silent data loss, indistinguishable
from a network blip, discovered weeks later.

## 4. Re-raise on failure. Let Kinesis retry.

When a chunk fails, RAISE. Do not catch and return.

```python
for chunk_index, chunk in enumerate(chunks):
    sql, parameters = _build_parameterized_insert(chunk)
    try:
        response = redshift.execute_statement(
            WorkgroupName=WORKGROUP, Database=DATABASE,
            Sql=sql, Parameters=parameters,
        )
        statement_ids.append(response["Id"])
        _await_statement(response["Id"])
    except Exception:
        logger.exception(
            "Chunk %d/%d failed (already-submitted statement_ids=%s)",
            chunk_index + 1, len(chunks), statement_ids,
        )
        raise  # Kinesis retries the batch.
```

Logging the already-submitted statement IDs before re-raising is
important: on retry, those chunks will be inserted AGAIN. The
append-only pattern absorbs that duplication safely (later
reconstruction picks the latest by `commit_timestamp`), but operators
reading CloudWatch logs need to know what already landed.

## 5. Poison records: skip, do not raise

A poison record is a malformed payload (missing `op`, missing PK,
missing timestamp, undecodable Base64). Handle these BEFORE submitting
SQL. A poison record should be:

1. Logged at WARNING with enough context to identify it (record key,
   approximate position).
2. Counted as `skipped`.
3. Not added to the SQL submission list.

```python
def lambda_handler(event, context):
    rows = []
    skipped = 0
    for record in event.get("Records", []):
        try:
            raw = base64.b64decode(record["kinesis"]["data"])
            payload = json.loads(raw)
        except (KeyError, TypeError, ValueError) as e:
            logger.error("Failed to decode record: %s", e)
            skipped += 1
            continue

        op = payload.get("op")
        # DSQL preview: only 'c' and 'd' ops. For non-DSQL sources
        # accept 'u' too.
        if op == "c":
            row = payload.get("after")
        elif op == "d":
            row = payload.get("before")
        else:
            logger.warning("Skipping unknown op: %s", op)
            skipped += 1
            continue
        if not row:
            logger.warning("Skipping op=%s with empty row payload", op)
            skipped += 1
            continue

        record_id = row.get("id")
        ts_ms = payload.get("ts_ms")
        if record_id is None or ts_ms is None:
            logger.warning(
                "Skipping op=%s payload with missing id=%r or ts_ms=%r",
                op, record_id, ts_ms,
            )
            skipped += 1
            continue

        rows.append({
            "table": payload.get("source", {}).get("table", "unknown"),
            "op": op,
            "record_id": record_id,
            "row": row,
            "commit_ts_ms": ts_ms,
        })

    if not rows:
        logger.info("No well-formed rows in batch (skipped=%d)", skipped)
        return {"processed": 0, "skipped": skipped}

    chunks = [rows[i:i + ROWS_PER_CHUNK] for i in range(0, len(rows), ROWS_PER_CHUNK)]
    statement_ids = []
    for chunk_index, chunk in enumerate(chunks):
        sql, parameters = _build_parameterized_insert(chunk)
        try:
            response = redshift.execute_statement(
                WorkgroupName=WORKGROUP, Database=DATABASE,
                Sql=sql, Parameters=parameters,
            )
            statement_ids.append(response["Id"])
            _await_statement(response["Id"])
        except Exception:
            logger.exception(
                "Chunk %d/%d failed (already-submitted statement_ids=%s)",
                chunk_index + 1, len(chunks), statement_ids,
            )
            raise

    return {"processed": len(rows), "skipped": skipped, "statement_ids": statement_ids}
```

Why not raise on poison records: raising would force Kinesis to retry
the ENTIRE batch including the well-formed records, then bisect, ad
infinitum until `MaximumRetryAttempts` exhausts. The poison record
wedges the shard for minutes. Counting + skipping lets the well-formed
records land.

The trade-off: skipped records are lost unless you also write them to
an audit S3 bucket or a dedicated DLQ. That is acceptable for most CDC
pipelines because truly malformed source data is rare and almost
always indicates a producer bug rather than a data-loss event.

## 6. Lambda timeout sizing

The function must cover the worst-case poll wait per batch:

```
timeout >= ceil(BatchSize / ROWS_PER_CHUNK) * STATEMENT_POLL_TIMEOUT_S + boto3_overhead
```

With the recommended defaults:
- `BatchSize: 100`
- `ROWS_PER_CHUNK: 40` (3 chunks max: 40 + 40 + 20)
- `STATEMENT_POLL_TIMEOUT_S: 20`

That is `3 * 20 = 60s` for the polls plus boto3 RTT, so a function
timeout of 120s gives healthy headroom. A 30s function timeout will
fire mid-poll under load and cause apparent failures that are
actually just clock-budget misconfiguration.

## 7. Environment variables

The reference handler reads these:

| Variable                    | Default | Notes                                                   |
| --------------------------- | ------- | ------------------------------------------------------- |
| `REDSHIFT_WORKGROUP`        | -       | Required. Serverless workgroup name.                    |
| `REDSHIFT_DATABASE`         | `dev`   | Database within the workgroup.                          |
| `ROWS_PER_CHUNK`            | `40`    | Cap is `200 / params_per_row`.                          |
| `STATEMENT_POLL_TIMEOUT_S`  | `20`    | Per-statement deadline. Lambda timeout must accommodate. |

## What's next

- The CFN snippet for the EventSourceMapping is in
  [event-source-mapping.yaml](event-source-mapping.yaml).
- The IAM policy and SUPER projection rules for the Redshift sink are
  in [sink-redshift.md](sink-redshift.md).
