//! Hot/Cold Query Service with LRU Cache
//!
//! Architecture:
//! - Cold Storage: Parquet files on E: drive (queried via DuckDB)
//! - Hot Cache: QuestDB on D: drive (fast queries, LRU eviction)
//! - Metadata: Tracks what's cached, access times, sizes
//!
//! Usage:
//!   query_service --parquet-dir E:/parquet --questdb-url http://localhost:9000 --cache-limit 500
//!
//! API:
//!   GET /query?symbol=NVDA&start=2025-10-22T14:00:00&end=2025-10-22T15:00:00
//!   GET /query?symbol=NVDA&date=2025-10-22&cache=false  (skip cache warming)
//!   GET /cache/stats
//!   POST /cache/evict  (manual eviction)

use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use chrono::{DateTime, Utc};
use duckdb::Connection;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, RwLock},
    time::Instant,
};
use tokio::sync::Mutex as TokioMutex;

// ============================================================================
// Configuration
// ============================================================================

#[derive(Clone)]
struct AppConfig {
    parquet_dir: PathBuf,
    questdb_url: String,
    cache_limit_gb: f64,
    metadata_csv: PathBuf,
}

// ============================================================================
// Cache Metadata
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CacheEntry {
    date: String,
    symbol: String,
    hour: u32,
    minute: u32,
    row_count: usize,
    size_bytes: u64,
    file_path: String,
    is_hot: bool,
    last_accessed: Option<DateTime<Utc>>,
    access_count: u32,
}

#[derive(Default)]
struct CacheState {
    entries: HashMap<String, CacheEntry>, // key: "date/symbol/HH-MM"
    hot_size_bytes: u64,
}

impl CacheState {
    fn load_from_csv(path: &PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        let mut state = CacheState::default();
        
        if !path.exists() {
            return Ok(state);
        }

        let content = std::fs::read_to_string(path)?;
        for line in content.lines().skip(1) {
            // Skip header
            let parts: Vec<&str> = line.split(',').collect();
            if parts.len() >= 8 {
                let entry = CacheEntry {
                    date: parts[0].to_string(),
                    symbol: parts[1].to_string(),
                    hour: parts[2].parse().unwrap_or(0),
                    minute: parts[3].parse().unwrap_or(0),
                    row_count: parts[4].parse().unwrap_or(0),
                    size_bytes: parts[5].parse().unwrap_or(0),
                    file_path: parts[6].to_string(),
                    is_hot: false,
                    last_accessed: None,
                    access_count: 0,
                };
                let key = format!("{}/{}/{:02}-{:02}", entry.date, entry.symbol, entry.hour, entry.minute);
                state.entries.insert(key, entry);
            }
        }

        Ok(state)
    }

    fn get_partition_key(date: &str, symbol: &str, hour: u32, minute: u32) -> String {
        format!("{}/{}/{:02}-{:02}", date, symbol, hour, minute)
    }

    fn mark_hot(&mut self, key: &str) {
        if let Some(entry) = self.entries.get_mut(key) {
            if !entry.is_hot {
                entry.is_hot = true;
                self.hot_size_bytes += entry.size_bytes;
            }
            entry.last_accessed = Some(Utc::now());
            entry.access_count += 1;
        }
    }

    fn mark_cold(&mut self, key: &str) {
        if let Some(entry) = self.entries.get_mut(key) {
            if entry.is_hot {
                entry.is_hot = false;
                self.hot_size_bytes = self.hot_size_bytes.saturating_sub(entry.size_bytes);
            }
        }
    }

    fn get_lru_hot_entries(&self, count: usize) -> Vec<String> {
        let mut hot_entries: Vec<_> = self
            .entries
            .iter()
            .filter(|(_, e)| e.is_hot)
            .collect();

        // Sort by last_accessed (oldest first), then by access_count (least accessed first)
        hot_entries.sort_by(|(_, a), (_, b)| {
            match (&a.last_accessed, &b.last_accessed) {
                (Some(ta), Some(tb)) => ta.cmp(tb),
                (None, Some(_)) => std::cmp::Ordering::Less,
                (Some(_), None) => std::cmp::Ordering::Greater,
                (None, None) => a.access_count.cmp(&b.access_count),
            }
        });

        hot_entries
            .into_iter()
            .take(count)
            .map(|(k, _)| k.clone())
            .collect()
    }

    fn hot_size_gb(&self) -> f64 {
        self.hot_size_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
    }
}

// ============================================================================
// DuckDB Query Engine (for cold storage)
// ============================================================================

struct DuckDBEngine {
    conn: Connection,
    parquet_dir: PathBuf,
}

impl DuckDBEngine {
    fn new(parquet_dir: PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        let conn = Connection::open_in_memory()?;
        Ok(Self { conn, parquet_dir })
    }

    fn query_parquet(
        &self,
        symbol: &str,
        date: &str,
        start_hour: u32,
        start_minute: u32,
        end_hour: u32,
        end_minute: u32,
    ) -> Result<Vec<MboRow>, Box<dyn std::error::Error>> {
        // Build glob pattern for the time range
        let symbol_dir = self.parquet_dir.join(date).join(symbol);
        
        if !symbol_dir.exists() {
            return Ok(Vec::new());
        }

        let glob_pattern = format!("{}/*.parquet", symbol_dir.display());
        
        let query = format!(
            r#"
            SELECT 
                ts_event, symbol, instrument_id, order_id, price_raw, price,
                size, flags, channel_id, action, side, ts_recv, ts_in_delta, sequence
            FROM read_parquet('{}')
            WHERE 
                (EXTRACT(HOUR FROM to_timestamp(ts_event / 1000000000)) * 100 + 
                 (EXTRACT(MINUTE FROM to_timestamp(ts_event / 1000000000))::INT / 15) * 15) >= {}
                AND 
                (EXTRACT(HOUR FROM to_timestamp(ts_event / 1000000000)) * 100 + 
                 (EXTRACT(MINUTE FROM to_timestamp(ts_event / 1000000000))::INT / 15) * 15) < {}
            ORDER BY ts_event
            "#,
            glob_pattern,
            start_hour * 100 + start_minute,
            end_hour * 100 + end_minute
        );

        let mut stmt = self.conn.prepare(&query)?;
        let rows = stmt.query_map([], |row| {
            Ok(MboRow {
                ts_event: row.get(0)?,
                symbol: row.get(1)?,
                instrument_id: row.get(2)?,
                order_id: row.get(3)?,
                price_raw: row.get(4)?,
                price: row.get(5)?,
                size: row.get(6)?,
                flags: row.get(7)?,
                channel_id: row.get(8)?,
                action: row.get(9)?,
                side: row.get(10)?,
                ts_recv: row.get(11)?,
                ts_in_delta: row.get(12)?,
                sequence: row.get(13)?,
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }

        Ok(results)
    }

    fn query_symbol_date(
        &self,
        symbol: &str,
        date: &str,
    ) -> Result<Vec<MboRow>, Box<dyn std::error::Error>> {
        let symbol_dir = self.parquet_dir.join(date).join(symbol);
        
        if !symbol_dir.exists() {
            return Ok(Vec::new());
        }

        let glob_pattern = format!("{}/*.parquet", symbol_dir.display());
        
        let query = format!(
            r#"
            SELECT 
                ts_event, symbol, instrument_id, order_id, price_raw, price,
                size, flags, channel_id, action, side, ts_recv, ts_in_delta, sequence
            FROM read_parquet('{}')
            ORDER BY ts_event
            "#,
            glob_pattern
        );

        let mut stmt = self.conn.prepare(&query)?;
        let rows = stmt.query_map([], |row| {
            Ok(MboRow {
                ts_event: row.get(0)?,
                symbol: row.get(1)?,
                instrument_id: row.get(2)?,
                order_id: row.get(3)?,
                price_raw: row.get(4)?,
                price: row.get(5)?,
                size: row.get(6)?,
                flags: row.get(7)?,
                channel_id: row.get(8)?,
                action: row.get(9)?,
                side: row.get(10)?,
                ts_recv: row.get(11)?,
                ts_in_delta: row.get(12)?,
                sequence: row.get(13)?,
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }

        Ok(results)
    }
}

// ============================================================================
// Data Models
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MboRow {
    ts_event: i64,
    symbol: String,
    instrument_id: i32,
    order_id: i64,
    price_raw: i64,
    price: f64,
    size: i32,
    flags: i32,
    channel_id: i16,
    action: String,
    side: String,
    ts_recv: i64,
    ts_in_delta: i32,
    sequence: i32,
}

#[derive(Debug, Deserialize)]
struct QueryParams {
    symbol: String,
    date: Option<String>,
    start: Option<String>,
    end: Option<String>,
    #[serde(default = "default_true")]
    cache: bool,
    #[serde(default)]
    limit: Option<usize>,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Serialize)]
struct QueryResponse {
    symbol: String,
    row_count: usize,
    source: String, // "hot" or "cold"
    query_time_ms: u64,
    data: Vec<MboRow>,
}

#[derive(Debug, Serialize)]
struct CacheStats {
    hot_size_gb: f64,
    cache_limit_gb: f64,
    hot_partitions: usize,
    total_partitions: usize,
    hot_entries: Vec<CacheEntrySummary>,
}

#[derive(Debug, Serialize)]
struct CacheEntrySummary {
    key: String,
    size_mb: f64,
    access_count: u32,
    last_accessed: Option<String>,
}

// ============================================================================
// QuestDB Client (for hot cache)
// ============================================================================

struct QuestDBClient {
    client: Client,
    base_url: String,
}

impl QuestDBClient {
    fn new(base_url: &str) -> Self {
        Self {
            client: Client::new(),
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }

    async fn query(&self, sql: &str) -> Result<Vec<MboRow>, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{}/exec", self.base_url);
        let resp = self
            .client
            .get(&url)
            .query(&[("query", sql)])
            .send()
            .await?;

        if !resp.status().is_success() {
            let body = resp.text().await?;
            return Err(format!("QuestDB query failed: {}", body).into());
        }

        let json: serde_json::Value = resp.json().await?;
        let mut rows = Vec::new();

        if let Some(dataset) = json.get("dataset").and_then(|d| d.as_array()) {
            for record in dataset {
                if let Some(arr) = record.as_array() {
                    if arr.len() >= 14 {
                        rows.push(MboRow {
                            ts_event: arr[0].as_str().and_then(|s| s.parse().ok()).unwrap_or(0),
                            symbol: arr[1].as_str().unwrap_or("").to_string(),
                            instrument_id: arr[2].as_i64().unwrap_or(0) as i32,
                            order_id: arr[3].as_i64().unwrap_or(0),
                            price_raw: arr[4].as_i64().unwrap_or(0),
                            price: arr[5].as_f64().unwrap_or(0.0),
                            size: arr[6].as_i64().unwrap_or(0) as i32,
                            flags: arr[7].as_i64().unwrap_or(0) as i32,
                            channel_id: arr[8].as_i64().unwrap_or(0) as i16,
                            action: arr[9].as_str().unwrap_or("").to_string(),
                            side: arr[10].as_str().unwrap_or("").to_string(),
                            ts_recv: arr[11].as_str().and_then(|s| s.parse().ok()).unwrap_or(0),
                            ts_in_delta: arr[12].as_i64().unwrap_or(0) as i32,
                            sequence: arr[13].as_i64().unwrap_or(0) as i32,
                        });
                    }
                }
            }
        }

        Ok(rows)
    }

    async fn check_partition_exists(
        &self,
        symbol: &str,
        date: &str,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let sql = format!(
            "SELECT count() FROM mbo_cache WHERE symbol = '{}' AND ts_event >= '{}T00:00:00Z' AND ts_event < '{}T00:00:00Z' LIMIT 1",
            symbol, date, date
        );
        let rows = self.query(&sql).await?;
        Ok(!rows.is_empty())
    }

    async fn insert_rows(&self, rows: &[MboRow]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if rows.is_empty() {
            return Ok(());
        }

        // Use ILP format for fast ingestion
        let mut ilp_batch = String::new();
        for row in rows {
            ilp_batch.push_str(&format!(
                "mbo_cache,symbol={},action={},side={} instrument_id={}i,order_id={}i,price_raw={}i,price={},size={}i,flags={}i,channel_id={}i,ts_in_delta={}i,sequence={}i,ts_recv={}i {}\n",
                row.symbol, row.action, row.side,
                row.instrument_id, row.order_id, row.price_raw, row.price,
                row.size, row.flags, row.channel_id, row.ts_in_delta,
                row.sequence, row.ts_recv, row.ts_event
            ));
        }

        let url = format!("{}/write?precision=n", self.base_url);
        let resp = self.client.post(&url).body(ilp_batch).send().await?;

        if !resp.status().is_success() {
            let body = resp.text().await?;
            return Err(format!("QuestDB insert failed: {}", body).into());
        }

        Ok(())
    }

    async fn ensure_cache_table(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let sql = r#"
            CREATE TABLE IF NOT EXISTS mbo_cache (
                ts_event TIMESTAMP,
                symbol SYMBOL capacity 16384,
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
            ) timestamp(ts_event)
              PARTITION BY DAY
              WAL;
        "#;
        
        let url = format!("{}/exec", self.base_url);
        let _ = self.client.get(&url).query(&[("query", sql)]).send().await?;
        Ok(())
    }

    async fn drop_partition(&self, date: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let sql = format!(
            "ALTER TABLE mbo_cache DROP PARTITION WHERE ts_event >= '{}T00:00:00Z' AND ts_event < '{}T23:59:59Z'",
            date, date
        );
        let url = format!("{}/exec", self.base_url);
        let _ = self.client.get(&url).query(&[("query", &sql)]).send().await?;
        Ok(())
    }
}

// ============================================================================
// App State
// ============================================================================

struct AppState {
    config: AppConfig,
    cache_state: RwLock<CacheState>,
    duckdb: TokioMutex<DuckDBEngine>,
    questdb: QuestDBClient,
}

// ============================================================================
// HTTP Handlers
// ============================================================================

async fn query_handler(
    params: web::Query<QueryParams>,
    state: web::Data<Arc<AppState>>,
) -> impl Responder {
    let started = Instant::now();
    let symbol = &params.symbol;

    // Determine date range
    let date = if let Some(d) = &params.date {
        d.clone()
    } else if let Some(start) = &params.start {
        start.split('T').next().unwrap_or("").to_string()
    } else {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": "Must specify 'date' or 'start' parameter"
        }));
    };

    // Check hot cache first
    let _cache_key = format!("{}/{}", date, symbol);
    let is_hot = {
        let cache = state.cache_state.read().unwrap();
        cache.entries.values().any(|e| e.date == date && e.symbol == *symbol && e.is_hot)
    };

    let (rows, source) = if is_hot {
        // Query from QuestDB (hot)
        let sql = format!(
            "SELECT * FROM mbo_cache WHERE symbol = '{}' AND ts_event >= '{}T00:00:00Z' AND ts_event < '{}T23:59:59Z' ORDER BY ts_event",
            symbol, date, date
        );
        match state.questdb.query(&sql).await {
            Ok(r) => (r, "hot"),
            Err(e) => {
                return HttpResponse::InternalServerError().json(serde_json::json!({
                    "error": format!("QuestDB query failed: {}", e)
                }));
            }
        }
    } else {
        // Query from Parquet (cold) via DuckDB
        let duckdb = state.duckdb.lock().await;
        match duckdb.query_symbol_date(symbol, &date) {
            Ok(r) => {
                drop(duckdb); // Release lock before async cache warming
                
                // Async cache warming if enabled
                if params.cache && !r.is_empty() {
                    let rows_clone = r.clone();
                    let state_clone = state.clone();
                    let date_clone = date.clone();
                    let symbol_clone = symbol.clone();
                    
                    tokio::spawn(async move {
                        if let Err(e) = warm_cache(&state_clone, &date_clone, &symbol_clone, &rows_clone).await {
                            eprintln!("Cache warming failed: {}", e);
                        }
                    });
                }
                
                (r, "cold")
            }
            Err(e) => {
                return HttpResponse::InternalServerError().json(serde_json::json!({
                    "error": format!("DuckDB query failed: {}", e)
                }));
            }
        }
    };

    // Update access stats
    {
        let mut cache = state.cache_state.write().unwrap();
        for entry in cache.entries.values_mut() {
            if entry.date == date && entry.symbol == *symbol {
                entry.last_accessed = Some(Utc::now());
                entry.access_count += 1;
            }
        }
    }

    let row_count = rows.len();
    let limited_rows = if let Some(limit) = params.limit {
        rows.into_iter().take(limit).collect()
    } else {
        rows
    };

    HttpResponse::Ok().json(QueryResponse {
        symbol: symbol.clone(),
        row_count,
        source: source.to_string(),
        query_time_ms: started.elapsed().as_millis() as u64,
        data: limited_rows,
    })
}

async fn warm_cache(
    state: &Arc<AppState>,
    date: &str,
    symbol: &str,
    rows: &[MboRow],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Check if we need to evict first
    let cache_limit_bytes = (state.config.cache_limit_gb * 1024.0 * 1024.0 * 1024.0) as u64;
    let estimated_size: u64 = rows.len() as u64 * 150; // ~150 bytes per row estimate
    // Phase 1: compute eviction keys and dates without performing async operations under lock
    let (eviction_keys, eviction_dates) = {
        let cache = state.cache_state.read().unwrap();
        let current_hot = cache.hot_size_bytes;
        let mut required_bytes: i64 = (current_hot + estimated_size)
            .saturating_sub(cache_limit_bytes) as i64;

        if required_bytes <= 0 {
            (Vec::new(), Vec::new())
        } else {
            let mut keys = Vec::new();
            let mut dates = Vec::new();
            let mut accumulated: u64 = 0;

            // Get all hot entries ordered by LRU and take as many as needed
            let ordered = cache.get_lru_hot_entries(cache.entries.len());
            for key in ordered {
                if let Some(entry) = cache.entries.get(&key) {
                    keys.push(key.clone());
                    dates.push(entry.date.clone());
                    accumulated += entry.size_bytes;
                    if accumulated as i64 >= required_bytes {
                        break;
                    }
                }
            }

            // Deduplicate dates to avoid redundant drops
            let mut unique_dates: Vec<String> = Vec::new();
            for d in dates {
                if !unique_dates.contains(&d) {
                    unique_dates.push(d);
                }
            }

            (keys, unique_dates)
        }
    };

    // Phase 2: perform async drops without holding locks
    for d in &eviction_dates {
        let _ = state.questdb.drop_partition(d).await;
    }

    // Phase 3: mark evicted entries as cold and update sizes under write lock
    if !eviction_keys.is_empty() {
        let mut cache = state.cache_state.write().unwrap();
        for key in &eviction_keys {
            cache.mark_cold(key);
        }
    }

    // Insert into QuestDB
    state.questdb.insert_rows(rows).await?;

    // Update cache state
    {
        let mut cache = state.cache_state.write().unwrap();
        let mut total_size_added = 0u64;
        for entry in cache.entries.values_mut() {
            if entry.date == date && entry.symbol == *symbol {
                if !entry.is_hot {
                    total_size_added += entry.size_bytes;
                }
                entry.is_hot = true;
                entry.last_accessed = Some(Utc::now());
            }
        }
        cache.hot_size_bytes += total_size_added;
    }

    Ok(())
}

async fn cache_stats_handler(state: web::Data<Arc<AppState>>) -> impl Responder {
    let cache = state.cache_state.read().unwrap();
    
    let hot_entries: Vec<CacheEntrySummary> = cache
        .entries
        .iter()
        .filter(|(_, e)| e.is_hot)
        .map(|(k, e)| CacheEntrySummary {
            key: k.clone(),
            size_mb: e.size_bytes as f64 / (1024.0 * 1024.0),
            access_count: e.access_count,
            last_accessed: e.last_accessed.map(|t| t.to_rfc3339()),
        })
        .collect();

    let stats = CacheStats {
        hot_size_gb: cache.hot_size_gb(),
        cache_limit_gb: state.config.cache_limit_gb,
        hot_partitions: hot_entries.len(),
        total_partitions: cache.entries.len(),
        hot_entries,
    };

    HttpResponse::Ok().json(stats)
}

async fn evict_handler(state: web::Data<Arc<AppState>>) -> impl Responder {
    let mut evicted = Vec::new();
    
    {
        let mut cache = state.cache_state.write().unwrap();
        let lru_keys = cache.get_lru_hot_entries(10); // Evict 10 LRU entries
        
        for key in lru_keys {
            cache.mark_cold(&key);
            evicted.push(key);
        }
    }

    HttpResponse::Ok().json(serde_json::json!({
        "evicted": evicted,
        "count": evicted.len()
    }))
}

async fn health_handler() -> impl Responder {
    HttpResponse::Ok().json(serde_json::json!({
        "status": "ok",
        "timestamp": Utc::now().to_rfc3339()
    }))
}

// ============================================================================
// Main
// ============================================================================

use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "query_service", about = "Hot/Cold MBO Query Service")]
struct Args {
    /// Parquet directory (cold storage)
    #[arg(long, default_value = "E:/parquet")]
    parquet_dir: PathBuf,

    /// QuestDB URL (hot cache)
    #[arg(long, default_value = "http://localhost:9000")]
    questdb_url: String,

    /// Cache size limit in GB
    #[arg(long, default_value = "500")]
    cache_limit: f64,

    /// HTTP server port
    #[arg(long, default_value = "8080")]
    port: u16,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();

    let config = AppConfig {
        parquet_dir: args.parquet_dir.clone(),
        questdb_url: args.questdb_url.clone(),
        cache_limit_gb: args.cache_limit,
        metadata_csv: args.parquet_dir.join("metadata.csv"),
    };

    // Initialize components
    let cache_state = CacheState::load_from_csv(&config.metadata_csv)
        .expect("Failed to load metadata");

    let duckdb = DuckDBEngine::new(config.parquet_dir.clone())
        .expect("Failed to initialize DuckDB");

    let questdb = QuestDBClient::new(&config.questdb_url);
    
    // Ensure cache table exists
    if let Err(e) = questdb.ensure_cache_table().await {
        eprintln!("Warning: Could not ensure QuestDB cache table: {}", e);
    }

    let state = Arc::new(AppState {
        config: config.clone(),
        cache_state: RwLock::new(cache_state),
        duckdb: TokioMutex::new(duckdb),
        questdb,
    });

    println!("Starting Query Service...");
    println!("  Parquet dir: {:?}", config.parquet_dir);
    println!("  QuestDB URL: {}", config.questdb_url);
    println!("  Cache limit: {} GB", config.cache_limit_gb);
    println!("  Listening on: http://0.0.0.0:{}", args.port);

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(state.clone()))
            .route("/query", web::get().to(query_handler))
            .route("/cache/stats", web::get().to(cache_stats_handler))
            .route("/cache/evict", web::post().to(evict_handler))
            .route("/health", web::get().to(health_handler))
    })
    .bind(("0.0.0.0", args.port))?
    .run()
    .await
}
