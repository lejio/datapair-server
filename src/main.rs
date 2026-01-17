use actix_web::{ get, post, web, App, HttpRequest, HttpResponse, HttpServer, Responder };
use arrow::array::{Float64Array, Int32Array, Int64Array, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use dbn::decode::AsyncDbnDecoder;
use dbn::Record;
use dbn::encode::json::Encoder;
use dbn::encode::EncodeRecord;
use dbn::enums::RType;
use dbn::record::{MboMsg, TradeMsg, Mbp1Msg, Mbp10Msg, OhlcvMsg, InstrumentDefMsg};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;

/// QuestDB HTTP client for LOD queries
struct QuestDbClient {
    client: reqwest::Client,
    base_url: String,
}

impl QuestDbClient {
    fn new(base_url: &str) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: base_url.to_string(),
        }
    }

    async fn query(&self, sql: &str) -> Result<serde_json::Value, String> {
        let url = format!("{}/exec", self.base_url);
        let resp = self.client
            .get(&url)
            .query(&[("query", sql)])
            .send()
            .await
            .map_err(|e| format!("Request failed: {}", e))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("QuestDB error {}: {}", status, body));
        }

        resp.json().await.map_err(|e| format!("JSON parse error: {}", e))
    }
}

/// Shared app state
struct AppState {
    questdb: QuestDbClient,
}

#[derive(Deserialize)]
struct RangeRequest {
    start_ts: u64,
    end_ts: u64,
}

#[derive(Deserialize)]
struct TimeToTimestampRequest {
    datetime: String,  // e.g., "2025-10-22 11:01:00" or "2025-10-22T11:01:00Z"
}

#[derive(Deserialize)]
struct TimestampToTimeRequest {
    timestamp_ns: u64,
}

#[derive(Serialize)]
struct TimestampResponse {
    timestamp_ns: u64,
    datetime_utc: String,
}

// =============================================================================
// LOD (Level-of-Detail) API for Dynamic Zoom Visualization
// =============================================================================

#[derive(Deserialize)]
struct BarsRequest {
    symbol: String,
    start: String,      // ISO timestamp or "2025-10-22"
    end: String,        // ISO timestamp or "2025-10-23"
    resolution: String, // "1s", "10s", "1m", "5m", "15m", "1h", "1d"
}

#[derive(Deserialize)]
struct TicksRequest {
    symbol: String,
    start: String,
    end: String,
    #[serde(default = "default_limit")]
    limit: u32,
    #[serde(default)]
    offset: u32,
}

fn default_limit() -> u32 { 10000 }

#[derive(Serialize)]
struct OhlcvBar {
    ts: i64,          // Unix ms for JS compatibility
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: i64,
    trades: i64,
}

#[derive(Serialize)]
struct BarsResponse {
    symbol: String,
    resolution: String,
    count: usize,
    bars: Vec<OhlcvBar>,
}

#[derive(Serialize)]
struct Tick {
    ts: i64,          // Unix ms
    ts_ns: i64,       // Full nanosecond precision
    price: f64,
    size: i32,
    side: String,
    action: String,
    order_id: i64,
}

#[derive(Serialize)]
struct TicksResponse {
    symbol: String,
    count: usize,
    has_more: bool,
    ticks: Vec<Tick>,
}

/// Determine optimal resolution based on time range
fn auto_resolution(start: &str, end: &str) -> &'static str {
    // Parse timestamps to estimate duration
    let duration_ms = estimate_duration_ms(start, end);
    
    match duration_ms {
        d if d > 7 * 24 * 3600 * 1000 => "1d",   // > 1 week -> daily
        d if d > 24 * 3600 * 1000 => "1h",       // > 1 day -> hourly
        d if d > 4 * 3600 * 1000 => "15m",       // > 4 hours -> 15min
        d if d > 3600 * 1000 => "5m",            // > 1 hour -> 5min
        d if d > 15 * 60 * 1000 => "1m",         // > 15 min -> 1min
        d if d > 5 * 60 * 1000 => "10s",         // > 5 min -> 10sec
        _ => "1s",                               // < 5 min -> 1sec
    }
}

fn estimate_duration_ms(start: &str, end: &str) -> i64 {
    let parse_ts = |s: &str| -> i64 {
        DateTime::parse_from_rfc3339(s)
            .map(|dt| dt.timestamp_millis())
            .or_else(|_| {
                NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                    .map(|ndt| ndt.and_utc().timestamp_millis())
            })
            .or_else(|_| {
                NaiveDateTime::parse_from_str(&format!("{} 00:00:00", s), "%Y-%m-%d %H:%M:%S")
                    .map(|ndt| ndt.and_utc().timestamp_millis())
            })
            .unwrap_or(0)
    };
    
    (parse_ts(end) - parse_ts(start)).abs()
}

/// Convert resolution string to QuestDB SAMPLE BY clause
fn resolution_to_sample_by(res: &str) -> &'static str {
    match res {
        "1s" => "1s",
        "10s" => "10s",
        "1m" => "1m",
        "5m" => "5m",
        "15m" => "15m",
        "1h" => "1h",
        "1d" => "1d",
        "auto" => "1m", // Default, will be overridden
        _ => "1m",
    }
}

/// GET /api/mbo/bars - OHLCV bars at specified resolution
/// 
/// Query params:
/// - symbol: e.g., "NVDA"
/// - start: ISO timestamp or date
/// - end: ISO timestamp or date  
/// - resolution: "1s", "10s", "1m", "5m", "15m", "1h", "1d", or "auto"
/// 
/// Headers:
/// - Accept: application/json (default) or application/vnd.apache.arrow.stream
#[get("/api/mbo/bars")]
async fn get_bars(
    req: HttpRequest,
    query: web::Query<BarsRequest>,
    state: web::Data<Arc<AppState>>,
) -> impl Responder {
    let resolution = if query.resolution == "auto" {
        auto_resolution(&query.start, &query.end)
    } else {
        resolution_to_sample_by(&query.resolution)
    };
    
    // QuestDB SAMPLE BY query for OHLCV aggregation
    // Filter for trades only (action='T')
    let sql = format!(
        r#"SELECT 
            ts_event,
            first(price) as open,
            max(price) as high,
            min(price) as low,
            last(price) as close,
            sum(size) as volume,
            count() as trades
        FROM mbo_ticks
        WHERE symbol = '{}'
          AND action = 'T'
          AND ts_event >= '{}'
          AND ts_event < '{}'
        SAMPLE BY {}"#,
        query.symbol, query.start, query.end, resolution
    );

    match state.questdb.query(&sql).await {
        Ok(json) => {
            let bars = parse_bars_response(&json);
            
            // Return Arrow IPC if requested, otherwise JSON
            if wants_arrow(&req) {
                match bars_to_arrow(&bars) {
                    Ok(arrow_data) => HttpResponse::Ok()
                        .content_type("application/vnd.apache.arrow.stream")
                        .insert_header(("X-Arrow-Count", bars.len().to_string()))
                        .body(arrow_data),
                    Err(e) => HttpResponse::InternalServerError().body(e),
                }
            } else {
                HttpResponse::Ok().json(BarsResponse {
                    symbol: query.symbol.clone(),
                    resolution: resolution.to_string(),
                    count: bars.len(),
                    bars,
                })
            }
        }
        Err(e) => HttpResponse::InternalServerError().body(e),
    }
}

/// GET /api/mbo/ticks - Raw tick data for zoomed-in views
/// 
/// Headers:
/// - Accept: application/json (default) or application/vnd.apache.arrow.stream
#[get("/api/mbo/ticks")]
async fn get_ticks(
    req: HttpRequest,
    query: web::Query<TicksRequest>,
    state: web::Data<Arc<AppState>>,
) -> impl Responder {
    let limit = query.limit.min(50000); // Cap at 50k
    
    let sql = format!(
        r#"SELECT 
            ts_event, price, size, side, action, order_id
        FROM mbo_ticks
        WHERE symbol = '{}'
          AND ts_event >= '{}'
          AND ts_event < '{}'
        ORDER BY ts_event
        LIMIT {},{}"#,
        query.symbol, query.start, query.end, query.offset, limit + 1
    );

    match state.questdb.query(&sql).await {
        Ok(json) => {
            let (ticks, has_more) = parse_ticks_response(&json, limit as usize);
            
            // Return Arrow IPC if requested, otherwise JSON
            if wants_arrow(&req) {
                match ticks_to_arrow(&ticks) {
                    Ok(arrow_data) => HttpResponse::Ok()
                        .content_type("application/vnd.apache.arrow.stream")
                        .insert_header(("X-Arrow-Count", ticks.len().to_string()))
                        .insert_header(("X-Has-More", has_more.to_string()))
                        .body(arrow_data),
                    Err(e) => HttpResponse::InternalServerError().body(e),
                }
            } else {
                HttpResponse::Ok().json(TicksResponse {
                    symbol: query.symbol.clone(),
                    count: ticks.len(),
                    has_more,
                    ticks,
                })
            }
        }
        Err(e) => HttpResponse::InternalServerError().body(e),
    }
}

/// GET /api/mbo/symbols - List available symbols
#[get("/api/mbo/symbols")]
async fn get_symbols(state: web::Data<Arc<AppState>>) -> impl Responder {
    let sql = "SELECT DISTINCT symbol FROM mbo_ticks ORDER BY symbol";
    
    match state.questdb.query(sql).await {
        Ok(json) => {
            let symbols = parse_symbols_response(&json);
            HttpResponse::Ok().json(symbols)
        }
        Err(e) => HttpResponse::InternalServerError().body(e),
    }
}

/// GET /api/mbo/range - Get available data range for a symbol
#[get("/api/mbo/range")]
async fn get_range(
    query: web::Query<std::collections::HashMap<String, String>>,
    state: web::Data<Arc<AppState>>,
) -> impl Responder {
    let symbol = query.get("symbol").map(|s| s.as_str()).unwrap_or("NVDA");
    
    let sql = format!(
        r#"SELECT 
            min(ts_event) as start,
            max(ts_event) as end,
            count() as total_ticks
        FROM mbo_ticks
        WHERE symbol = '{}'"#,
        symbol
    );
    
    match state.questdb.query(&sql).await {
        Ok(json) => HttpResponse::Ok().json(json),
        Err(e) => HttpResponse::InternalServerError().body(e),
    }
}

// Helper functions to parse QuestDB JSON responses
fn parse_bars_response(json: &serde_json::Value) -> Vec<OhlcvBar> {
    let mut bars = Vec::new();
    
    if let Some(dataset) = json.get("dataset") {
        if let Some(rows) = dataset.as_array() {
            for row in rows {
                if let Some(arr) = row.as_array() {
                    // QuestDB returns: [timestamp, open, high, low, close, volume, trades]
                    if arr.len() >= 7 {
                        let ts_str = arr[0].as_str().unwrap_or("");
                        let ts = parse_questdb_timestamp(ts_str);
                        
                        bars.push(OhlcvBar {
                            ts,
                            open: arr[1].as_f64().unwrap_or(0.0),
                            high: arr[2].as_f64().unwrap_or(0.0),
                            low: arr[3].as_f64().unwrap_or(0.0),
                            close: arr[4].as_f64().unwrap_or(0.0),
                            volume: arr[5].as_i64().unwrap_or(0),
                            trades: arr[6].as_i64().unwrap_or(0),
                        });
                    }
                }
            }
        }
    }
    
    bars
}

fn parse_ticks_response(json: &serde_json::Value, limit: usize) -> (Vec<Tick>, bool) {
    let mut ticks = Vec::new();
    
    if let Some(dataset) = json.get("dataset") {
        if let Some(rows) = dataset.as_array() {
            for (i, row) in rows.iter().enumerate() {
                if i >= limit {
                    return (ticks, true); // has_more = true
                }
                
                if let Some(arr) = row.as_array() {
                    // [ts_event, price, size, side, action, order_id]
                    if arr.len() >= 6 {
                        let ts_str = arr[0].as_str().unwrap_or("");
                        let ts_ms = parse_questdb_timestamp(ts_str);
                        
                        ticks.push(Tick {
                            ts: ts_ms,
                            ts_ns: ts_ms * 1_000_000, // Approximate
                            price: arr[1].as_f64().unwrap_or(0.0),
                            size: arr[2].as_i64().unwrap_or(0) as i32,
                            side: arr[3].as_str().unwrap_or("").to_string(),
                            action: arr[4].as_str().unwrap_or("").to_string(),
                            order_id: arr[5].as_i64().unwrap_or(0),
                        });
                    }
                }
            }
        }
    }
    
    (ticks, false)
}

fn parse_symbols_response(json: &serde_json::Value) -> Vec<String> {
    let mut symbols = Vec::new();
    
    if let Some(dataset) = json.get("dataset") {
        if let Some(rows) = dataset.as_array() {
            for row in rows {
                if let Some(arr) = row.as_array() {
                    if let Some(sym) = arr.first().and_then(|v| v.as_str()) {
                        symbols.push(sym.to_string());
                    }
                }
            }
        }
    }
    
    symbols
}

fn parse_questdb_timestamp(ts_str: &str) -> i64 {
    // QuestDB returns timestamps like "2025-10-22T14:30:00.000000Z"
    DateTime::parse_from_rfc3339(ts_str)
        .map(|dt| dt.timestamp_millis())
        .unwrap_or(0)
}

// =============================================================================
// Arrow IPC Serialization (10-50x faster than JSON for large datasets)
// =============================================================================

/// Check if client accepts Arrow IPC format
fn wants_arrow(req: &HttpRequest) -> bool {
    req.headers()
        .get("Accept")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.contains("application/vnd.apache.arrow.stream") || s.contains("application/arrow"))
        .unwrap_or(false)
}

/// Convert OHLCV bars to Arrow IPC stream
fn bars_to_arrow(bars: &[OhlcvBar]) -> Result<Vec<u8>, String> {
    let schema = Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("open", DataType::Float64, false),
        Field::new("high", DataType::Float64, false),
        Field::new("low", DataType::Float64, false),
        Field::new("close", DataType::Float64, false),
        Field::new("volume", DataType::Int64, false),
        Field::new("trades", DataType::Int64, false),
    ]);

    let ts: Int64Array = bars.iter().map(|b| b.ts).collect();
    let open: Float64Array = bars.iter().map(|b| b.open).collect();
    let high: Float64Array = bars.iter().map(|b| b.high).collect();
    let low: Float64Array = bars.iter().map(|b| b.low).collect();
    let close: Float64Array = bars.iter().map(|b| b.close).collect();
    let volume: Int64Array = bars.iter().map(|b| b.volume).collect();
    let trades: Int64Array = bars.iter().map(|b| b.trades).collect();

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(ts),
            Arc::new(open),
            Arc::new(high),
            Arc::new(low),
            Arc::new(close),
            Arc::new(volume),
            Arc::new(trades),
        ],
    ).map_err(|e| format!("Arrow batch error: {}", e))?;

    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema)
            .map_err(|e| format!("Arrow writer error: {}", e))?;
        writer.write(&batch).map_err(|e| format!("Arrow write error: {}", e))?;
        writer.finish().map_err(|e| format!("Arrow finish error: {}", e))?;
    }
    
    Ok(buf)
}

/// Convert ticks to Arrow IPC stream
fn ticks_to_arrow(ticks: &[Tick]) -> Result<Vec<u8>, String> {
    let schema = Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("ts_ns", DataType::Int64, false),
        Field::new("price", DataType::Float64, false),
        Field::new("size", DataType::Int32, false),
        Field::new("side", DataType::Utf8, false),
        Field::new("action", DataType::Utf8, false),
        Field::new("order_id", DataType::Int64, false),
    ]);

    let ts: Int64Array = ticks.iter().map(|t| t.ts).collect();
    let ts_ns: Int64Array = ticks.iter().map(|t| t.ts_ns).collect();
    let price: Float64Array = ticks.iter().map(|t| t.price).collect();
    let size: Int32Array = ticks.iter().map(|t| t.size).collect();
    
    let mut side_builder = StringBuilder::new();
    let mut action_builder = StringBuilder::new();
    for t in ticks {
        side_builder.append_value(&t.side);
        action_builder.append_value(&t.action);
    }
    let side = side_builder.finish();
    let action = action_builder.finish();
    
    let order_id: Int64Array = ticks.iter().map(|t| t.order_id).collect();

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(ts),
            Arc::new(ts_ns),
            Arc::new(price),
            Arc::new(size),
            Arc::new(side),
            Arc::new(action),
            Arc::new(order_id),
        ],
    ).map_err(|e| format!("Arrow batch error: {}", e))?;

    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema)
            .map_err(|e| format!("Arrow writer error: {}", e))?;
        writer.write(&batch).map_err(|e| format!("Arrow write error: {}", e))?;
        writer.finish().map_err(|e| format!("Arrow finish error: {}", e))?;
    }
    
    Ok(buf)
}

#[get("/")]
async fn hello() -> impl Responder {
    HttpResponse::Ok().body("Hello world!")
}

#[post("/echo")]
async fn echo(req_body: String) -> impl Responder {
    HttpResponse::Ok().body(req_body)
}

#[post("/data")]
async fn get_data(req: web::Json<RangeRequest>) -> impl Responder {
    // Path to the data file
    let file_path = PathBuf::from("./data/NVDA.dbn.zst");
    
    if !file_path.exists() {
        return HttpResponse::InternalServerError().body("Data file not found");
    }

    let mut decoder = match AsyncDbnDecoder::from_zstd_file(&file_path).await {
        Ok(d) => d,
        Err(e) => return HttpResponse::InternalServerError().body(format!("Failed to open file: {}", e)),
    };

    let mut buffer = Vec::new();
    buffer.push(b'[');
    let mut first = true;

    loop {
        match decoder.decode_record_ref().await {
            Ok(Some(rec_ref)) => {
                let ts = rec_ref.header().ts_event;
                if ts >= req.start_ts && ts <= req.end_ts {
                    if !first {
                        buffer.push(b',');
                    }
                    first = false;
                    
                    // Encode record to JSON
                    // We create a new encoder for each record to write to the buffer
                    // This might be inefficient but works.
                    // Encoder::new(writer, should_pretty_print, use_colors, should_output_header?)
                    let mut encoder = Encoder::new(&mut buffer, false, false, false);
                    
                    let rtype = rec_ref.header().rtype();
                    match rtype {
                        Ok(RType::Mbo) => {
                            if let Some(rec) = rec_ref.get::<MboMsg>() {
                                if let Err(e) = encoder.encode_record(rec) {
                                    return HttpResponse::InternalServerError().body(format!("Error encoding record: {}", e));
                                }
                            }
                        }
                        Ok(RType::Mbp0) => {
                            if let Some(rec) = rec_ref.get::<TradeMsg>() {
                                if let Err(e) = encoder.encode_record(rec) {
                                    return HttpResponse::InternalServerError().body(format!("Error encoding record: {}", e));
                                }
                            }
                        }
                        Ok(RType::Mbp1) => {
                            if let Some(rec) = rec_ref.get::<Mbp1Msg>() {
                                if let Err(e) = encoder.encode_record(rec) {
                                    return HttpResponse::InternalServerError().body(format!("Error encoding record: {}", e));
                                }
                            }
                        }
                        Ok(RType::Mbp10) => {
                            if let Some(rec) = rec_ref.get::<Mbp10Msg>() {
                                if let Err(e) = encoder.encode_record(rec) {
                                    return HttpResponse::InternalServerError().body(format!("Error encoding record: {}", e));
                                }
                            }
                        }
                        Ok(RType::Ohlcv1S) | Ok(RType::Ohlcv1M) | Ok(RType::Ohlcv1H) | Ok(RType::Ohlcv1D) | Ok(RType::OhlcvEod) => {
                            if let Some(rec) = rec_ref.get::<OhlcvMsg>() {
                                if let Err(e) = encoder.encode_record(rec) {
                                    return HttpResponse::InternalServerError().body(format!("Error encoding record: {}", e));
                                }
                            }
                        }
                        Ok(RType::InstrumentDef) => {
                            if let Some(rec) = rec_ref.get::<InstrumentDefMsg>() {
                                if let Err(e) = encoder.encode_record(rec) {
                                    return HttpResponse::InternalServerError().body(format!("Error encoding record: {}", e));
                                }
                            }
                        }
                        _ => {
                            // Skip other types or handle them if needed
                        }
                    }
                }
            }
            Ok(None) => break,
            Err(e) => return HttpResponse::InternalServerError().body(format!("Error reading record: {}", e)),
        }
    }

    buffer.push(b']');

    let json_string = String::from_utf8(buffer).unwrap_or_default();
    HttpResponse::Ok()
        .content_type("application/json")
        .body(json_string)
}

#[get("/hi")]
async fn hello2() -> impl Responder {
    HttpResponse::Ok().body("Testing route 2")
}

async fn manual_hello() -> impl Responder {
    HttpResponse::Ok().body("Hey there!")
}

// Lets talk about some of the stuff we can do

// I need a middleware for the route. This would act as a authentication measure and checks if each request is signed properly

// Next I would need to unpack the contents in the body in a certain format. Errors would be accounted for.

// For example, the error would let the user know what is the status code and what went wrong

// Then I would develop dynamic zoom. A rust backed library that compiles time series data for fast range based retrival.
// What it is good for: Zooming of data. For example, this zoom object would a fidelity measurement, that would determine the order in which the data is being viewed. 
// The higher the order, the more "zoomed" out it would be. When the data is retrieved for the first time, it would be stored in a cache, what it is I am still figuring that out. I want this "library" to be as scalable as possible. Meaning that an internal cache should be used. The cache would have a devalidation period, and higher order compilation would use the lower cached data points for speed and efficiency. When a certain range of the data is already been retrieved previously, that would be used if that range is then accessed again. Meaning that at almost every level of zoom, a certain amount of data is cached. Something I am worried about is that this would led to a space confined problem. For example when you are handling terabytes of data, it is dumb to cache series of data.. unless there is a way to access faster. For example.. take a look at timeseries db

// kb+

// So kb+ is basically timeseries database entirely held in the memory. This means that my 64gb memory would come in handy and the entire, ig gigabytes of data would need to be first loaded onto the RAM to proceed usage. This means that I would need a shit load of RAM.

// Holdup, did they just say they can access stored data like it is in the RAM??? Well, if that is true, that changes everything since I have terabytes upon terabytes of data.

/// Convert human-readable datetime to nanosecond timestamp
/// POST /time/to_timestamp
/// Body: {"datetime": "2025-10-22 11:01:00"} or {"datetime": "2025-10-22T11:01:00Z"}
#[post("/time/to_timestamp")]
async fn time_to_timestamp(req: web::Json<TimeToTimestampRequest>) -> impl Responder {
    let datetime_str = &req.datetime;
    
    // Try parsing different formats
    let parsed: Option<DateTime<Utc>> = 
        // ISO 8601 with timezone
        DateTime::parse_from_rfc3339(datetime_str)
            .map(|dt| dt.with_timezone(&Utc))
            .ok()
        // Without timezone (assume UTC)
        .or_else(|| {
            NaiveDateTime::parse_from_str(datetime_str, "%Y-%m-%d %H:%M:%S")
                .ok()
                .map(|ndt| Utc.from_utc_datetime(&ndt))
        })
        // Date only
        .or_else(|| {
            NaiveDateTime::parse_from_str(&format!("{} 00:00:00", datetime_str), "%Y-%m-%d %H:%M:%S")
                .ok()
                .map(|ndt| Utc.from_utc_datetime(&ndt))
        });
    
    match parsed {
        Some(dt) => {
            let timestamp_ns = dt.timestamp_nanos_opt().unwrap_or(0) as u64;
            HttpResponse::Ok().json(TimestampResponse {
                timestamp_ns,
                datetime_utc: dt.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
            })
        }
        None => HttpResponse::BadRequest().body(format!(
            "Could not parse datetime: '{}'. Try formats: '2025-10-22 11:01:00' or '2025-10-22T11:01:00Z'",
            datetime_str
        ))
    }
}

/// Convert nanosecond timestamp to human-readable datetime
/// POST /time/to_datetime
/// Body: {"timestamp_ns": 1761130860000000000}
#[post("/time/to_datetime")]
async fn timestamp_to_time(req: web::Json<TimestampToTimeRequest>) -> impl Responder {
    let timestamp_ns = req.timestamp_ns;
    let secs = (timestamp_ns / 1_000_000_000) as i64;
    let nsecs = (timestamp_ns % 1_000_000_000) as u32;
    
    match DateTime::from_timestamp(secs, nsecs) {
        Some(dt) => {
            HttpResponse::Ok().json(TimestampResponse {
                timestamp_ns,
                datetime_utc: dt.format("%Y-%m-%d %H:%M:%S.%f UTC").to_string(),
            })
        }
        None => HttpResponse::BadRequest().body("Invalid timestamp")
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let questdb_url = std::env::var("QUESTDB_URL")
        .unwrap_or_else(|_| "http://localhost:9000".to_string());
    
    let state = Arc::new(AppState {
        questdb: QuestDbClient::new(&questdb_url),
    });

    println!("Starting server on http://127.0.0.1:8080");
    println!("QuestDB endpoint: {}", questdb_url);

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(state.clone()))
            // LOD API endpoints
            .service(get_bars)
            .service(get_ticks)
            .service(get_symbols)
            .service(get_range)
            // Legacy endpoints
            .service(hello)
            .service(echo)
            .service(get_data)
            .service(time_to_timestamp)
            .service(timestamp_to_time)
            .route("/hey", web::get().to(manual_hello))
    })
        .bind(("127.0.0.1", 8080))?
        .run().await
}
