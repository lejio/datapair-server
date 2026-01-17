# DATAPAIR SERVER
Version 1
---

Start the server:

```shell
cargo run
```

### Endpoint: /data

Takes in start_ts and end_ts timestamps. Then returns the L3 data for NVDA during that time.

```shell
curl -X POST http://127.0.0.1:8080/data -H "Content-Type: application/json" -d '{"start_ts": 1761130860000000000, "end_ts": 1761130920000000000}' -o output.json
```

### Endpoint: /time/to_timestamp

Takes in human readable time and converts it to datetime

```shell
curl -s -X POST http://127.0.0.1:8080/time/to_timestamp -H "Content-Type: application/json" -d '{"datetime": "2025-10-22 11:01:00"}' | jq .
```

### Endpoint: /time/to_datetime

Takes in datetime and converts it back to human readable format

```shell
curl -s -X POST http://127.0.0.1:8080/time/to_datetime -H "Content-Type: application/json" -d '{"timestamp_ns": 1761130860010235508}' | jq .
```

## QuestDB ingestion

Use the new ingestion helper in [src/bin/ingest_questdb.rs](src/bin/ingest_questdb.rs) to stream a `.dbn.zst` day file into QuestDB over the REST endpoints on `localhost:9000`.

### Table design (L3 MBO scale)

```sql
CREATE TABLE IF NOT EXISTS mbo_ticks (
		ts_event TIMESTAMP,
		symbol SYMBOL CAPACITY 4096,
		instrument_id INT,
		order_id LONG,
		price_raw LONG,
		price DOUBLE,
		size INT,
		flags INT,
		channel_id SHORT,
		action SYMBOL,
		side SYMBOL,
		ts_recv TIMESTAMP,
		ts_in_delta INT,
		sequence INT
) TIMESTAMP(ts_event)
	PARTITION BY DAY
	WAL;
ALTER TABLE mbo_ticks ALTER COLUMN symbol ADD INDEX CAPACITY 65536;
```

- Partition by day and WAL keeps ingestion resilient and lets you backfill.
- `symbol` is indexed for fast symbol/date slicing; `price_raw` holds the original 1e-9 scaled integer and `price` is a convenience float.
- `ts_event` is the designated timestamp; QuestDB stores nanoseconds, matching DBN.

### Run the ingester

```shell
cargo run --bin ingest_questdb /path/to/DAY.dbn.zst
```

Environment knobs:
- `QUESTDB_URL` (default `http://localhost:9000`)
- `QDB_BATCH_BYTES` (default 3_000_000)
- `QDB_FLUSH_ROWS` (default 50_000)

### Quick query examples

```shell
# Count rows for one symbol and day
curl -s "http://localhost:9000/exec?query=select count(*) from mbo_ticks where symbol='NVDA' and ts_event between '2024-10-22T00:00:00Z' and '2024-10-23T00:00:00Z'"

# Latest top-of-book snapshot for a symbol
curl -s "http://localhost:9000/exec?query=select * from mbo_ticks where symbol='NVDA' and ts_event > now()-1m order by ts_event desc limit 20"
```

---

## Hybrid Architecture: QuestDB + Parquet/Polars

For large-scale analytics across billions of MBO rows (~60 days × 11k symbols), we use a **hybrid approach**:

| Use Case | Technology | Why |
|----------|------------|-----|
| **Ingestion** | QuestDB ILP | High-throughput append-only writes |
| **Live queries** | QuestDB SQL | Real-time slicing by symbol/time |
| **Heavy analytics** | Parquet + Polars | Columnar, zero-copy, parallel scans |
| **Backtesting** | Parquet + DuckDB/Polars | Custom aggregations, joins, ML prep |

### Export to Parquet

Export completed days from QuestDB to Parquet files for analytics:

```shell
# Export a single day
cargo run --bin export_parquet -- --date 2025-10-22 --output ./parquet/

# Export a date range
cargo run --bin export_parquet -- --start 2025-10-01 --end 2025-10-31 --output ./parquet/

# Export one symbol only
cargo run --bin export_parquet -- --date 2025-10-22 --symbol NVDA --output ./parquet/
```

Parquet files use Zstd compression and are partitioned by date: `mbo_2025-10-22.parquet`

### Query Parquet files with Polars

Fast local analytics on exported Parquet files:

```shell
# View data for a symbol on a date
cargo run --bin query_parquet -- --dir ./parquet --symbol NVDA --date 2025-10-22

# Run custom SQL (table is named 'df')
cargo run --bin query_parquet -- --dir ./parquet --sql "SELECT symbol, COUNT(*) as cnt FROM df GROUP BY symbol ORDER BY cnt DESC"

# Generate OHLCV bars at 1-minute intervals (trades only)
cargo run --bin query_parquet -- --dir ./parquet --symbol NVDA --date 2025-10-22 --ohlcv 1m

# Output as CSV or JSON
cargo run --bin query_parquet -- --dir ./parquet --symbol NVDA --limit 100 --format csv
```

### When to use each

| Task | Recommended |
|------|-------------|
| Ingest new data | `ingest_questdb` |
| Query last N minutes | QuestDB HTTP/SQL |
| Scan full day for one symbol | QuestDB or Parquet |
| Cross-symbol aggregations | Parquet + Polars |
| Backtest with custom logic | Load Parquet in Python/Rust |
| Train ML models | Export → Parquet → DataFrame |

### Schema optimization (for billions of rows)

For production scale, update the table DDL:

```sql
-- Drop and recreate with optimized partitioning
DROP TABLE mbo_ticks;

CREATE TABLE IF NOT EXISTS mbo_ticks (
    ts_event TIMESTAMP,
    symbol SYMBOL CAPACITY 16384,
    instrument_id INT,
    order_id LONG,
    price_raw LONG,
    price DOUBLE,
    size INT,
    flags INT,
    channel_id SHORT,
    action SYMBOL,
    side SYMBOL,
    ts_recv TIMESTAMP,
    ts_in_delta INT,
    sequence INT
) TIMESTAMP(ts_event)
  PARTITION BY HOUR
  WAL;

ALTER TABLE mbo_ticks ALTER COLUMN symbol ADD INDEX CAPACITY 131072;
```

Key changes:
- `PARTITION BY HOUR` instead of DAY (better for billions of rows)
- `SYMBOL CAPACITY 16384` (supports 11k+ symbols)
- `INDEX CAPACITY 131072` (larger index for more efficient lookups)

# datapair-server
# datapair-server
