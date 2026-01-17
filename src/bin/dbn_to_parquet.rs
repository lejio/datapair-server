//! Convert .mbo.dbn.zst files directly to Parquet for cold storage.
//!
//! Usage:
//!   dbn_to_parquet --input D:/data --output E:/parquet
//!   dbn_to_parquet --file D:/data/xnas-itch-20251022.mbo.dbn.zst --output E:/parquet
//!
//! Output: E:/parquet/2025-10-22/NVDA/mbo_14-00.parquet (partitioned by date/symbol/time)
//! Metadata: E:/parquet/metadata.csv

use chrono::{DateTime, Timelike, Utc};
use clap::Parser;
use dbn::{
    decode::{AsyncDbnDecoder, DbnMetadata},
    enums::RType,
    record::MboMsg,
    Record, SymbolIndex, TsSymbolMap,
};
use polars::prelude::*;
use std::{
    collections::HashMap,
    convert::TryFrom,
    fs::OpenOptions,
    io::Write,
    path::PathBuf,
    sync::Mutex,
    time::Instant,
};

const FLUSH_ROWS: usize = 500_000; // Flush every 500K rows per symbol to limit memory
const PERIODIC_FLUSH_INTERVAL_ROWS: usize = 1_000_000; // Force flush all buffers periodically to cap memory

#[derive(Parser, Debug)]
#[command(name = "dbn_to_parquet", about = "Convert DBN files to Parquet")]
struct Args {
    /// Input directory containing .mbo.dbn.zst files
    #[arg(long)]
    input: Option<PathBuf>,

    /// Single file to convert
    #[arg(long)]
    file: Option<PathBuf>,

    /// Output directory for Parquet files
    #[arg(long, short)]
    output: PathBuf,
}

struct PartitionBuffer {
    ts_event: Vec<i64>,
    instrument_id: Vec<u32>,
    order_id: Vec<u64>,
    price_raw: Vec<i64>,
    price: Vec<f64>,
    size: Vec<u32>,
    flags: Vec<u8>,
    channel_id: Vec<u8>,
    action: Vec<u8>,
    side: Vec<u8>,
    ts_recv: Vec<i64>,
    ts_in_delta: Vec<i32>,
    sequence: Vec<u32>,
    flush_count: usize,  // Track number of flushes for unique filenames
}

impl PartitionBuffer {
    fn new() -> Self {
        Self {
            ts_event: Vec::new(),
            instrument_id: Vec::new(),
            order_id: Vec::new(),
            price_raw: Vec::new(),
            price: Vec::new(),
            size: Vec::new(),
            flags: Vec::new(),
            channel_id: Vec::new(),
            action: Vec::new(),
            side: Vec::new(),
            ts_recv: Vec::new(),
            ts_in_delta: Vec::new(),
            sequence: Vec::new(),
            flush_count: 0,
        }
    }

    fn len(&self) -> usize {
        self.ts_event.len()
    }

    fn push(&mut self, _symbol: &str, mbo: &MboMsg) {
        let ts_event = mbo.hd.ts_event as i64;
        let ts_recv = mbo.ts_recv as i64;
        let price = mbo.price as f64 / 1_000_000_000.0;
        let action = mbo.action as u8;
        let side = mbo.side as u8;

        self.ts_event.push(ts_event);
        self.instrument_id.push(mbo.hd.instrument_id);
        self.order_id.push(mbo.order_id);
        self.price_raw.push(mbo.price);
        self.price.push(price);
        self.size.push(mbo.size);
        self.flags.push(mbo.flags.raw());
        self.channel_id.push(mbo.channel_id);
        self.action.push(action);
        self.side.push(side);
        self.ts_recv.push(ts_recv);
        self.ts_in_delta.push(mbo.ts_in_delta);
        self.sequence.push(mbo.sequence);
    }

    fn to_dataframe(&self) -> PolarsResult<DataFrame> {
        DataFrame::new(vec![
            Column::new("ts_event".into(), &self.ts_event),
            Column::new("instrument_id".into(), self.instrument_id.iter().map(|&x| x as i32).collect::<Vec<_>>()),
            Column::new("order_id".into(), self.order_id.iter().map(|&x| x as i64).collect::<Vec<_>>()),
            Column::new("price_raw".into(), &self.price_raw),
            Column::new("price".into(), &self.price),
            Column::new("size".into(), self.size.iter().map(|&x| x as i32).collect::<Vec<_>>()),
            Column::new("flags".into(), self.flags.iter().map(|&x| x as i32).collect::<Vec<_>>()),
            Column::new("channel_id".into(), self.channel_id.iter().map(|&x| x as i16).collect::<Vec<_>>()),
            Column::new("action".into(), self.action.iter().map(|&x| x as i32).collect::<Vec<_>>()),
            Column::new("side".into(), self.side.iter().map(|&x| x as i32).collect::<Vec<_>>()),
            Column::new("ts_recv".into(), &self.ts_recv),
            Column::new("ts_in_delta".into(), &self.ts_in_delta),
            Column::new("sequence".into(), self.sequence.iter().map(|&x| x as i32).collect::<Vec<_>>()),
        ])
    }

    fn clear(&mut self) {
        self.ts_event.clear();
        self.instrument_id.clear();
        self.order_id.clear();
        self.price_raw.clear();
        self.price.clear();
        self.size.clear();
        self.flags.clear();
        self.channel_id.clear();
        self.action.clear();
        self.side.clear();
        self.ts_recv.clear();
        self.ts_in_delta.clear();
        self.sequence.clear();
    }
}

/// Partition key: (date, symbol, time_key)
fn get_partition_key(ts_nanos: u64, symbol: &str) -> (String, String, u32) {
    let secs = (ts_nanos / 1_000_000_000) as i64;
    let dt = DateTime::from_timestamp(secs, 0).unwrap_or(DateTime::UNIX_EPOCH);
    let date = dt.format("%Y-%m-%d").to_string();
    let hour = dt.hour();
    let minute_bucket = (dt.minute() / 15) * 15;
    let time_key = hour * 100 + minute_bucket; // e.g., 1430 for 14:30
    (date, symbol.to_string(), time_key)
}

/// Metadata entry for cache management
#[derive(Clone)]
struct PartitionMeta {
    date: String,
    symbol: String,
    hour: u32,
    minute: u32,
    row_count: usize,
    size_bytes: u64,
    file_path: String,
    created_at: String,
}

fn write_partition(
    buffer: &PartitionBuffer,
    output_dir: &PathBuf,
    date: &str,
    symbol: &str,
    time_key: u32,
    flush_seq: usize,
) -> Result<Option<PartitionMeta>, Box<dyn std::error::Error + Send + Sync>> {
    if buffer.len() == 0 {
        return Ok(None);
    }

    let hour = time_key / 100;
    let minute = time_key % 100;

    // Create date/symbol subdirectory: E:/parquet/2025-10-22/NVDA/
    let symbol_dir = output_dir.join(date).join(symbol);
    std::fs::create_dir_all(&symbol_dir)?;

    // Use sequential naming to avoid memory-intensive appending
    // e.g., mbo_14-00_000.parquet, mbo_14-00_001.parquet, etc.
    let filename = format!("mbo_{:02}-{:02}_{:03}.parquet", hour, minute, flush_seq);
    let path = symbol_dir.join(&filename);

    let mut df = buffer.to_dataframe()?;
    let row_count = buffer.len();

    // Write directly without appending - DuckDB can query multiple files via glob
    let file = std::fs::File::create(&path)?;
    ParquetWriter::new(file)
        .with_compression(ParquetCompression::Zstd(Some(ZstdLevel::try_new(3)?)))
        .finish(&mut df)?;

    // Explicitly drop dataframe to free memory immediately
    drop(df);

    // Get file size
    let size_bytes = std::fs::metadata(&path)?.len();
    let rel_path = format!("{}/{}/{}", date, symbol, filename);

    Ok(Some(PartitionMeta {
        date: date.to_string(),
        symbol: symbol.to_string(),
        hour,
        minute,
        row_count,
        size_bytes,
        file_path: rel_path,
        created_at: Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
    }))
}

async fn convert_file(
    file_path: &PathBuf,
    output_dir: &PathBuf,
    metadata_entries: &Mutex<Vec<PartitionMeta>>,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    println!("Converting {:?}...", file_path.file_name().unwrap_or_default());
    let started = Instant::now();

    let mut decoder = AsyncDbnDecoder::from_zstd_file(&file_path).await?;
    let metadata = decoder.metadata().clone();
    let symbol_map = TsSymbolMap::try_from(&metadata)?;

    // Buffers keyed by (date, symbol, time_key)
    let mut buffers: HashMap<(String, String, u32), PartitionBuffer> = HashMap::new();
    let mut total_rows = 0usize;
    let mut flushed_rows = 0usize;
    let mut symbols_seen: std::collections::HashSet<String> = std::collections::HashSet::new();

    while let Some(rec_ref) = decoder.decode_record_ref().await? {
        if !matches!(rec_ref.header().rtype(), Ok(RType::Mbo)) {
            continue;
        }

        if let Some(mbo) = rec_ref.get::<MboMsg>() {
            let symbol = match symbol_map.get_for_rec(mbo) {
                Some(s) => s,
                None => continue,
            };

            let (date, sym, time_key) = get_partition_key(mbo.hd.ts_event, symbol);
            let key = (date.clone(), sym.clone(), time_key);
            symbols_seen.insert(sym.clone());

            let buffer = buffers.entry(key).or_insert_with(PartitionBuffer::new);
            buffer.push(symbol, mbo);
            total_rows += 1;

            // Flush large buffers to prevent OOM
            if buffer.len() >= FLUSH_ROWS {
                if let Some(meta) = write_partition(buffer, output_dir, &date, &sym, time_key, buffer.flush_count)? {
                    metadata_entries.lock().unwrap().push(meta);
                }
                flushed_rows += buffer.len();
                buffer.flush_count += 1;
                buffer.clear();
                print!("\r  Processed {} rows, {} symbols, flushed {}...", total_rows, symbols_seen.len(), flushed_rows);
                std::io::stdout().flush().ok();
            }

            // Periodic flush of all buffers to cap memory for rarely-active partitions
            if total_rows % PERIODIC_FLUSH_INTERVAL_ROWS == 0 {
                for ((d, s, t), b) in buffers.iter_mut() {
                    if b.len() > 0 {
                        if let Some(meta) = write_partition(b, output_dir, d, s, *t, b.flush_count)? {
                            metadata_entries.lock().unwrap().push(meta);
                        }
                        flushed_rows += b.len();
                        b.flush_count += 1;
                        b.clear();
                    }
                }
                print!("\r  Periodic flush at {} rows, flushed {}...", total_rows, flushed_rows);
                std::io::stdout().flush().ok();
            }
        }
    }

    // Flush remaining buffers
    for ((date, symbol, time_key), buffer) in &buffers {
        if buffer.len() > 0 {
            if let Some(meta) = write_partition(buffer, output_dir, date, symbol, *time_key, buffer.flush_count)? {
                metadata_entries.lock().unwrap().push(meta);
            }
        }
    }

    // Clear buffers to free memory
    buffers.clear();

    println!(
        "\n  Done: {} rows, {} symbols in {:.2?}",
        total_rows,
        symbols_seen.len(),
        started.elapsed()
    );

    Ok(total_rows)
}

fn find_dbn_files(dir: &PathBuf) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.ends_with(".mbo.dbn.zst") {
                    files.push(path);
                }
            }
        }
    }
    files.sort();
    Ok(files)
}

fn write_metadata_csv(
    entries: &[PartitionMeta],
    output_dir: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let path = output_dir.join("metadata.csv");
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)?;

    // Write header
    writeln!(file, "date,symbol,hour,minute,row_count,size_bytes,file_path,created_at")?;

    // Write entries
    for e in entries {
        writeln!(
            file,
            "{},{},{},{},{},{},{},{}",
            e.date, e.symbol, e.hour, e.minute, e.row_count, e.size_bytes, e.file_path, e.created_at
        )?;
    }

    println!("Metadata written to {:?}", path);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let files: Vec<PathBuf> = if let Some(file) = args.file {
        vec![file]
    } else if let Some(input_dir) = args.input {
        find_dbn_files(&input_dir)?
    } else {
        eprintln!("Error: Specify either --input or --file");
        std::process::exit(1);
    };

    if files.is_empty() {
        println!("No .mbo.dbn.zst files found.");
        return Ok(());
    }

    println!("Found {} files to convert:", files.len());
    for f in &files {
        println!("  - {:?}", f.file_name().unwrap_or_default());
    }

    std::fs::create_dir_all(&args.output)?;

    let metadata_entries: Mutex<Vec<PartitionMeta>> = Mutex::new(Vec::new());
    let mut total = 0usize;
    let started = Instant::now();

    for file in files {
        match convert_file(&file, &args.output, &metadata_entries).await {
            Ok(rows) => total += rows,
            Err(e) => eprintln!("Error converting {:?}: {}", file, e),
        }
    }

    // Write metadata CSV
    let entries = metadata_entries.lock().unwrap();
    write_metadata_csv(&entries, &args.output)?;

    println!("\n=== Complete ===");
    println!("Total rows: {}", total);
    println!("Total partitions: {}", entries.len());
    println!("Total time: {:.2?}", started.elapsed());
    println!("Output: {:?}", args.output);

    Ok(())
}
