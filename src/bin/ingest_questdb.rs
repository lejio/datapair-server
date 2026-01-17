use dbn::{
    decode::{AsyncDbnDecoder, DbnMetadata},
    enums::RType,
    record::MboMsg,
    Record, SymbolIndex, TsSymbolMap,
};
use futures::stream::{self, StreamExt};
use reqwest::Client;
use std::{collections::HashSet, convert::TryFrom, env, fmt::Write, path::PathBuf, sync::Arc, time::Instant};
use tokio::sync::Semaphore;

const DEFAULT_BATCH_BYTES: usize = 3_000_000; // ~3 MB batches to keep HTTP requests reasonable
const DEFAULT_FLUSH_ROWS: usize = 50_000;
const DEFAULT_DATA_DIR: &str = "/mnt/d/data";
const DEFAULT_CONCURRENCY: usize = 2; // Number of files to process concurrently
const DEFAULT_SYMBOL_FILTER: &str = "/mnt/c/Server2/symbols_filter.txt";

/// Load symbol filter from file (one symbol per line)
fn load_symbol_filter(path: &str) -> Option<HashSet<String>> {
    match std::fs::read_to_string(path) {
        Ok(contents) => {
            let symbols: HashSet<String> = contents
                .lines()
                .map(|s| s.trim().to_uppercase())
                .filter(|s| !s.is_empty() && !s.starts_with('#'))
                .collect();
            if symbols.is_empty() {
                None
            } else {
                println!("Loaded {} symbols from filter file", symbols.len());
                Some(symbols)
            }
        }
        Err(_) => {
            println!("No symbol filter file found, ingesting ALL symbols");
            None
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    
    // Load symbol filter
    let symbol_filter_path = env::var("SYMBOL_FILTER").unwrap_or_else(|_| DEFAULT_SYMBOL_FILTER.to_string());
    let symbol_filter = load_symbol_filter(&symbol_filter_path);
    let symbol_filter = Arc::new(symbol_filter);
    
    // If a specific file is provided, use it; otherwise scan for all .mbo.dbn.zst files
    let files: Vec<PathBuf> = if let Some(path) = args.get(1) {
        vec![PathBuf::from(path)]
    } else {
        let data_dir = env::var("DATA_DIR").unwrap_or_else(|_| DEFAULT_DATA_DIR.to_string());
        find_mbo_files(&data_dir)?
    };

    if files.is_empty() {
        println!("No .mbo.dbn.zst files found to ingest.");
        return Ok(());
    }

    println!("Found {} file(s) to ingest:", files.len());
    for f in &files {
        println!("  - {:?}", f);
    }

    let questdb_url = env::var("QUESTDB_URL").unwrap_or_else(|_| "http://localhost:9000".to_string());
    
    // Check which files have already been ingested
    let client = Client::new();
    let already_ingested = check_ingested_files(&client, &questdb_url, &files).await.unwrap_or_default();
    let files_to_ingest: Vec<PathBuf> = files.into_iter()
        .filter(|f| !already_ingested.contains(&f.file_name().unwrap_or_default().to_string_lossy().to_string()))
        .collect();
    
    if files_to_ingest.is_empty() {
        println!("All files have already been ingested.");
        return Ok(());
    }
    
    if already_ingested.len() > 0 {
        println!("\nSkipping {} already-ingested file(s):", already_ingested.len());
        for f in &already_ingested {
            println!("  - {}", f);
        }
        println!("\nRemaining {} file(s) to ingest:", files_to_ingest.len());
        for f in &files_to_ingest {
            println!("  - {:?}", f);
        }
    }
    let batch_bytes = env::var("QDB_BATCH_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_BATCH_BYTES);
    let flush_rows = env::var("QDB_FLUSH_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_FLUSH_ROWS);
    let concurrency = env::var("QDB_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_CONCURRENCY);

    let client = Client::builder().gzip(true).build()?;
    ensure_table(&client, &questdb_url).await?;

    let started = Instant::now();
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let questdb_url = Arc::new(questdb_url);
    
    // Process files concurrently with a semaphore to limit parallelism
    let results: Vec<_> = stream::iter(files_to_ingest)
        .map(|file_path| {
            let client = client.clone();
            let questdb_url = Arc::clone(&questdb_url);
            let semaphore = Arc::clone(&semaphore);
            let symbol_filter = Arc::clone(&symbol_filter);
            async move {
                let _permit = semaphore.acquire().await.unwrap();
                ingest_file(&client, &questdb_url, &file_path, batch_bytes, flush_rows, symbol_filter.as_ref().as_ref()).await
            }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;

    // Summarize results
    let mut total_rows = 0usize;
    let mut success_count = 0usize;
    let mut error_count = 0usize;
    
    for result in results {
        match result {
            Ok((path, rows)) => {
                println!("✓ {:?}: {} rows", path, rows);
                total_rows += rows;
                success_count += 1;
            }
            Err((path, e)) => {
                eprintln!("✗ {:?}: {}", path, e);
                error_count += 1;
            }
        }
    }

    println!("\n=== Summary ===");
    println!("Total rows ingested: {}", total_rows);
    println!("Successful files: {}", success_count);
    println!("Failed files: {}", error_count);
    println!("Total time: {:.2?}", started.elapsed());
    
    Ok(())
}


/// Check which files have already been ingested by querying the date ranges
async fn check_ingested_files(
    client: &Client,
    questdb_url: &str,
    files: &[PathBuf],
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut ingested = Vec::new();
    
    for file_path in files {
        if let Some(file_name) = file_path.file_name().and_then(|n| n.to_str()) {
            // Extract date from filename like "xnas-itch-20251113.mbo.dbn.zst"
            if let Some(date_str) = extract_date_from_filename(file_name) {
                // Query to check if we have data for this date
                let query = format!(
                    "SELECT COUNT(*) as cnt FROM mbo_ticks WHERE ts_event >= '{}' AND ts_event < '{}' + 1d;",
                    date_str, date_str
                );
                let url = format!("{}/exec?query={}", questdb_url, urlencoding::encode(&query));
                
                if let Ok(resp) = client.get(&url).send().await {
                    if resp.status().is_success() {
                        if let Ok(text) = resp.text().await {
                            // Parse the count from the response
                            // Response format: {"query":"...","columns":[...],"dataset":[[count]],"count":1}
                            if let Some(count) = parse_count_from_response(&text) {
                                if count > 0 {
                                    ingested.push(file_name.to_string());
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    
    Ok(ingested)
}

/// Extract date string (YYYY-MM-DD) from filename like "xnas-itch-20251113.mbo.dbn.zst"
fn extract_date_from_filename(filename: &str) -> Option<String> {
    // Look for 8-digit date pattern YYYYMMDD
    let re = regex::Regex::new(r"(\d{4})(\d{2})(\d{2})").ok()?;
    let caps = re.captures(filename)?;
    Some(format!("{}-{}-{}", &caps[1], &caps[2], &caps[3]))
}

/// Parse count from QuestDB JSON response
fn parse_count_from_response(text: &str) -> Option<u64> {
    // Simple parse: look for "dataset":[[NUMBER]]
    let start = text.find("\"dataset\":[[")? + 12;
    let end = text[start..].find("]]")? + start;
    text[start..end].parse().ok()
}

/// Find all .mbo.dbn.zst files in the given directory
fn find_mbo_files(dir: &str) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    let mut files = Vec::new();
    let path = PathBuf::from(dir);
    
    if !path.exists() {
        return Err(format!("Data directory does not exist: {}", dir).into());
    }
    
    for entry in std::fs::read_dir(&path)? {
        let entry = entry?;
        let file_path = entry.path();
        if file_path.is_file() {
            if let Some(name) = file_path.file_name().and_then(|n| n.to_str()) {
                if name.ends_with(".mbo.dbn.zst") {
                    files.push(file_path);
                }
            }
        }
    }
    
    // Sort files by name for consistent ordering
    files.sort();
    Ok(files)
}

/// Ingest a single file into QuestDB
async fn ingest_file(
    client: &Client,
    questdb_url: &str,
    file_path: &PathBuf,
    batch_bytes: usize,
    flush_rows: usize,
    symbol_filter: Option<&HashSet<String>>,
) -> Result<(PathBuf, usize), (PathBuf, String)> {
    let result = ingest_file_inner(client, questdb_url, file_path, batch_bytes, flush_rows, symbol_filter).await;
    match result {
        Ok(rows) => Ok((file_path.clone(), rows)),
        Err(e) => Err((file_path.clone(), e.to_string())),
    }
}

async fn ingest_file_inner(
    client: &Client,
    questdb_url: &str,
    file_path: &PathBuf,
    batch_bytes: usize,
    flush_rows: usize,
    symbol_filter: Option<&HashSet<String>>,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    let mut decoder = AsyncDbnDecoder::from_zstd_file(&file_path).await?;
    let metadata = decoder.metadata().clone();
    let symbol_map = TsSymbolMap::try_from(&metadata)?;

    println!(
        "Ingesting {:?} (schema: {:?}, symbols: {:?})",
        file_path, metadata.schema, metadata.symbols
    );

    let mut batch = String::with_capacity(batch_bytes + 1_024);
    let mut rows_in_batch = 0usize;
    let mut total_rows = 0usize;
    let mut skipped_rows = 0usize;
    let file_started = Instant::now();

    while let Some(rec_ref) = decoder.decode_record_ref().await? {
        if !matches!(rec_ref.header().rtype(), Ok(RType::Mbo)) {
            continue;
        }

        if let Some(mbo) = rec_ref.get::<MboMsg>() {
            let symbol = match symbol_map.get_for_rec(mbo) {
                Some(s) => s,
                None => continue,
            };

            // Filter by symbol if filter is provided
            if let Some(filter) = symbol_filter {
                if !filter.contains(&symbol.to_uppercase()) {
                    skipped_rows += 1;
                    continue;
                }
            }

            append_ilp_line(&mut batch, symbol, mbo)?;
            rows_in_batch += 1;
            total_rows += 1;

            if batch.len() >= batch_bytes || rows_in_batch >= flush_rows {
                send_batch(&client, &questdb_url, &batch).await?;
                batch.clear();
                rows_in_batch = 0;
            }
        }
    }

    if !batch.is_empty() {
        send_batch(&client, &questdb_url, &batch).await?;
    }

    println!(
        "Finished {:?}: {} rows ingested, {} skipped in {:.2?}",
        file_path.file_name().unwrap_or_default(),
        total_rows,
        skipped_rows,
        file_started.elapsed()
    );
    Ok(total_rows)
}

async fn ensure_table(client: &Client, base_url: &str) -> Result<(), Box<dyn std::error::Error>> {
    let create_stmt = r#"
        CREATE TABLE IF NOT EXISTS mbo_ticks (
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
          PARTITION BY HOUR
          WAL;
    "#;
    run_sql(client, base_url, create_stmt, false).await?;

    let index_stmt = "ALTER TABLE mbo_ticks ALTER COLUMN symbol ADD INDEX CAPACITY 65536;";
    run_sql(client, base_url, index_stmt, true).await?;

    Ok(())
}

async fn run_sql(
    client: &Client,
    base_url: &str,
    query: &str,
    ignore_exists: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let url = format!("{}/exec", base_url.trim_end_matches('/'));
    let resp = client.get(&url).query(&[("query", query)]).send().await?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        if ignore_exists && body.to_lowercase().contains("exist") {
            return Ok(());
        }
        return Err(format!("QuestDB query failed: {} - {}", status, body).into());
    }
    Ok(())
}

async fn send_batch(client: &Client, base_url: &str, batch: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if batch.is_empty() {
        return Ok(());
    }

    let url = format!("{}/write?precision=n", base_url.trim_end_matches('/'));
    let resp = client.post(url).body(batch.to_owned()).send().await?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("QuestDB ingest failed: {} - {}", status, body).into());
    }
    Ok(())
}

fn append_ilp_line(buf: &mut String, symbol: &str, mbo: &MboMsg) -> Result<(), std::fmt::Error> {
    let action = to_ascii(mbo.action);
    let side = to_ascii(mbo.side);
    let price = mbo.price as f64 / 1_000_000_000.0;
    let ts_event = mbo.hd.ts_event as i64;
    let ts_recv = mbo.ts_recv as i64;
    let escaped_symbol = escape_tag(symbol);

    // Tags: symbol, action, side (categorical)
    // Fields: all numeric values including channel_id
    write!(
        buf,
        "mbo_ticks,symbol={},action={},side={} instrument_id={}i,order_id={}i,price_raw={}i,price={},size={}i,flags={}i,channel_id={}i,ts_in_delta={}i,sequence={}i,ts_recv={}i {}\n",
        escaped_symbol,
        action,
        side,
        mbo.hd.instrument_id,
        mbo.order_id,
        mbo.price,
        price,
        mbo.size,
        mbo.flags.raw(),
        mbo.channel_id,
        mbo.ts_in_delta,
        mbo.sequence,
        ts_recv,
        ts_event
    )
}

fn to_ascii(val: i8) -> char {
    let byte = val as u8;
    if byte.is_ascii_graphic() {
        byte as char
    } else {
        '_'
    }
}

fn escape_tag(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            ',' | ' ' | '=' => out.push('\\'),
            _ => {}
        }
        out.push(ch);
    }
    out
}
