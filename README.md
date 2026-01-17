# DATAPAIR SERVER
Version 2.0 - QuestDB Edition
---

High-performance market data server with **QuestDB** backend for Level 3 MBO (Market By Order) data.

## Quick Start

```shell
# Build the server
cargo build --release

# Start everything (QuestDB → Server → Newt tunnel)
.\start.bat

# Or run manually
set QUESTDB_URL=http://localhost:9000
cargo run --release
```

Server runs on `http://127.0.0.1:8080` and queries QuestDB on `http://localhost:9000`.

---

## API Endpoints

### 📊 Level-of-Detail (LOD) MBO API

#### GET `/api/mbo/bars` - OHLCV Aggregated Bars

Returns OHLCV bars for a symbol over a time range with automatic resolution adjustment.

**Query Parameters:**
- `symbol` (string, required) - Ticker symbol (e.g., "NVDA", "AAPL")
- `start` (string, required) - Start time (ISO 8601 or date: "2025-10-22" or "2025-10-22T09:30:00Z")
- `end` (string, required) - End time (ISO 8601 or date)
- `resolution` (string, required) - Bar interval: `"1s"`, `"10s"`, `"1m"`, `"5m"`, `"15m"`, `"1h"`, `"1d"`, or `"auto"`

**Headers:**
- `Accept: application/json` (default) - Returns JSON response
- `Accept: application/vnd.apache.arrow.stream` - Returns Apache Arrow IPC stream (for high-performance clients)

**Example:**
```shell
# Get 5-minute bars for NVDA on Oct 22, 2025
curl "http://127.0.0.1:8080/api/mbo/bars?symbol=NVDA&start=2025-10-22&end=2025-10-23&resolution=5m"

# Get bars with Arrow format (zero-copy deserialization)
curl -H "Accept: application/vnd.apache.arrow.stream" \
  "http://127.0.0.1:8080/api/mbo/bars?symbol=AAPL&start=2025-10-22T09:30:00Z&end=2025-10-22T16:00:00Z&resolution=1m" \
  -o bars.arrow
```

**Response (JSON):**
```json
{
  "symbol": "NVDA",
  "resolution": "5m",
  "count": 78,
  "bars": [
    {
      "ts_event": "2025-10-22T09:30:00Z",
      "open": 135.42,
      "high": 135.89,
      "low": 135.21,
      "close": 135.67,
      "volume": 125430,
      "trades": 342
    }
  ]
}
```

---

#### GET `/api/mbo/ticks` - Raw Tick Data

Returns individual tick-level MBO events for fine-grained analysis.

**Query Parameters:**
- `symbol` (string, required) - Ticker symbol
- `start` (string, required) - Start time (ISO 8601)
- `end` (string, required) - End time (ISO 8601)
- `limit` (int, optional, default=10000, max=50000) - Max ticks to return
- `offset` (int, optional, default=0) - Pagination offset

**Headers:**
- `Accept: application/json` or `application/vnd.apache.arrow.stream`

**Example:**
```shell
# Get first 5000 ticks for TSLA
curl "http://127.0.0.1:8080/api/mbo/ticks?symbol=TSLA&start=2025-10-22T09:30:00Z&end=2025-10-22T09:31:00Z&limit=5000"

# Paginate through ticks
curl "http://127.0.0.1:8080/api/mbo/ticks?symbol=TSLA&start=2025-10-22T09:30:00Z&end=2025-10-22T09:35:00Z&limit=10000&offset=10000"
```

**Response (JSON):**
```json
{
  "symbol": "TSLA",
  "count": 5000,
  "has_more": true,
  "ticks": [
    {
      "ts_event": "2025-10-22T09:30:00.123456789Z",
      "price": 234.56,
      "size": 100,
      "side": "B",
      "action": "A",
      "order_id": 1234567890
    }
  ]
}
```

---

#### GET `/api/mbo/symbols` - List Available Symbols

Returns all distinct symbols in the database.

**Example:**
```shell
curl "http://127.0.0.1:8080/api/mbo/symbols"
```

**Response:**
```json
{
  "symbols": ["AAPL", "GOOGL", "MSFT", "NVDA", "TSLA", ...]
}
```

---

#### GET `/api/mbo/range` - Data Availability Range

Returns the time range of available data for a symbol.

**Query Parameters:**
- `symbol` (string, optional, default="NVDA") - Ticker symbol

**Example:**
```shell
curl "http://127.0.0.1:8080/api/mbo/range?symbol=AAPL"
```

**Response:**
```json
{
  "symbol": "AAPL",
  "min_ts": "2025-10-22T09:30:00.000000000Z",
  "max_ts": "2025-10-22T16:00:00.000000000Z",
  "count": 15234567
}
```

---

### 🕐 Utility Endpoints

#### POST `/time/to_timestamp` - Convert DateTime to Nanosecond Timestamp

**Request Body:**
```json
{
  "datetime": "2025-10-22 11:01:00"
}
```

**Example:**
```shell
curl -X POST http://127.0.0.1:8080/time/to_timestamp \
  -H "Content-Type: application/json" \
  -d '{"datetime": "2025-10-22 11:01:00"}' | jq .
```

**Response:**
```json
{
  "timestamp_ns": 1761130860000000000,
  "datetime_utc": "2025-10-22T11:01:00Z"
}
```

---

#### POST `/time/to_datetime` - Convert Nanosecond Timestamp to DateTime

**Request Body:**
```json
{
  "timestamp_ns": 1761130860010235508
}
```

**Example:**
```shell
curl -X POST http://127.0.0.1:8080/time/to_datetime \
  -H "Content-Type: application/json" \
  -d '{"timestamp_ns": 1761130860010235508}' | jq .
```

**Response:**
```json
{
  "timestamp_ns": 1761130860010235508,
  "datetime_utc": "2025-10-22T11:01:00.010235508Z"
}
```

---

### 🔧 Legacy Endpoints

#### POST `/data` - Raw L3 Data (Deprecated)

Returns raw MBO data for NVDA between timestamps. **Use `/api/mbo/ticks` instead.**

**Request Body:**
```json
{
  "start_ts": 1761130860000000000,
  "end_ts": 1761130920000000000
}
```

**Example:**
```shell
curl -X POST http://127.0.0.1:8080/data \
  -H "Content-Type: application/json" \
  -d '{"start_ts": 1761130860000000000, "end_ts": 1761130920000000000}' \
  -o output.json
```

---

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
