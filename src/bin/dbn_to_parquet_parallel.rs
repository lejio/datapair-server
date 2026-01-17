//! FAST parallel DBN to Parquet converter with optimizations
//!
//! Performance improvements:
//! - Multi-threaded: Decode in one thread, write in parallel worker pool
//! - Hourly partitions instead of 15-min (fewer files, less overhead)
//! - Uncompressed Parquet (Zstd compression is SLOW - DuckDB handles compression well)
//! - Larger batches (5M rows per partition before flush)
//! - Channel-based pipeline for concurrent decode/write
//!
//! Usage:
//!   dbn_to_parquet_parallel --input D:/data --output E:/parquet --threads 8

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
    sync::{Arc, Mutex},
    time::Instant,
};
use tokio::sync::mpsc;

const FLUSH_ROWS: usize = 250_000; // 250K rows per partition (faster first output, still memory safe)
const CHANNEL_BUFFER: usize = 100; // Channel buffer size

#[derive(Parser, Debug)]
#[command(name = "dbn_to_parquet_parallel", about = "Fast parallel DBN to Parquet converter")]
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

    /// Number of writer threads
    #[arg(long, default_value = "8")]
    threads: usize,

    /// Use compression (slower but smaller files)
    #[arg(long)]
    compress: bool,
}

#[derive(Clone)]
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
        }
    }

    fn with_capacity(cap: usize) -> Self {
        Self {
            ts_event: Vec::with_capacity(cap),
            instrument_id: Vec::with_capacity(cap),
            order_id: Vec::with_capacity(cap),
            price_raw: Vec::with_capacity(cap),
            price: Vec::with_capacity(cap),
            size: Vec::with_capacity(cap),
            flags: Vec::with_capacity(cap),
            channel_id: Vec::with_capacity(cap),
            action: Vec::with_capacity(cap),
            side: Vec::with_capacity(cap),
            ts_recv: Vec::with_capacity(cap),
            ts_in_delta: Vec::with_capacity(cap),
            sequence: Vec::with_capacity(cap),
        }
    }

    fn len(&self) -> usize {
        self.ts_event.len()
    }

    fn push(&mut self, _symbol: &str, mbo: &MboMsg) {
        self.ts_event.push(mbo.hd.ts_event as i64);
        self.instrument_id.push(mbo.hd.instrument_id);
        self.order_id.push(mbo.order_id);
        self.price_raw.push(mbo.price);
        self.price.push(mbo.price as f64 / 1_000_000_000.0);
        self.size.push(mbo.size);
        self.flags.push(mbo.flags.raw());
        self.channel_id.push(mbo.channel_id);
        self.action.push(mbo.action as u8);
        self.side.push(mbo.side as u8);
        self.ts_recv.push(mbo.ts_recv as i64);
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
}

/// Partition key: (date, symbol, hour) - HOURLY instead of 15-min
fn get_partition_key(ts_nanos: u64, symbol: &str) -> (String, String, u32) {
    let secs = (ts_nanos / 1_000_000_000) as i64;
    let dt = DateTime::from_timestamp(secs, 0).unwrap_or(DateTime::UNIX_EPOCH);
    let date = dt.format("%Y-%m-%d").to_string();
    let hour = dt.hour();
    (date, symbol.to_string(), hour)
}

#[derive(Clone)]
struct PartitionMeta {
    date: String,
    symbol: String,
    hour: u32,
    row_count: usize,
    size_bytes: u64,
    file_path: String,
    created_at: String,
}

#[derive(Clone)]
struct WriteTask {
    buffer: PartitionBuffer,
    date: String,
    symbol: String,
    hour: u32,
    flush_seq: usize,
    compress: bool,
}

async fn writer_worker(
    _rx: mpsc::Receiver<WriteTask>,
    _output_dir: PathBuf,
    _metadata: Arc<Mutex<Vec<PartitionMeta>>>,
    _worker_id: usize,
) {
    // This function is now inlined in main for simpler channel handling
}

async fn convert_file(
    file_path: &PathBuf,
    _output_dir: &PathBuf,
    tx: mpsc::Sender<WriteTask>,
    compress: bool,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    println!("Converting {:?}...", file_path.file_name().unwrap_or_default());
    let started = Instant::now();

    let mut decoder = AsyncDbnDecoder::from_zstd_file(&file_path).await?;
    let metadata = decoder.metadata().clone();
    let symbol_map = TsSymbolMap::try_from(&metadata)?;

    // Buffers keyed by (date, symbol, hour)
    let mut buffers: HashMap<(String, String, u32), (PartitionBuffer, usize)> = HashMap::new();
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

            let (date, sym, hour) = get_partition_key(mbo.hd.ts_event, symbol);
            let key = (date.clone(), sym.clone(), hour);
            symbols_seen.insert(sym.clone());

            let (buffer, flush_count) = buffers.entry(key).or_insert_with(|| {
                (PartitionBuffer::with_capacity(FLUSH_ROWS), 0)
            });
            
            buffer.push(symbol, mbo);
            total_rows += 1;

            // Flush large buffers
            if buffer.len() >= FLUSH_ROWS {
                let task = WriteTask {
                    buffer: buffer.clone(),
                    date: date.clone(),
                    symbol: sym.clone(),
                    hour,
                    flush_seq: *flush_count,
                    compress,
                };
                
                if tx.send(task).await.is_err() {
                    eprintln!("Writer channel closed!");
                    break;
                }
                
                flushed_rows += buffer.len();
                *flush_count += 1;
                *buffer = PartitionBuffer::with_capacity(FLUSH_ROWS);
                
                print!("\r  Processed {} rows, {} symbols, sent {} to writers... ({:.0} rows/sec)", 
                    total_rows, symbols_seen.len(), flushed_rows,
                    total_rows as f64 / started.elapsed().as_secs_f64());
                std::io::stdout().flush().ok();
            }
        }
    }

    // Flush remaining buffers
    for ((date, symbol, hour), (buffer, flush_count)) in buffers {
        if buffer.len() > 0 {
            let task = WriteTask {
                buffer,
                date,
                symbol,
                hour,
                flush_seq: flush_count,
                compress,
            };
            let _ = tx.send(task).await;
        }
    }

    let elapsed = started.elapsed();
    let rows_per_sec = total_rows as f64 / elapsed.as_secs_f64();
    
    println!(
        "\n  Done: {} rows, {} symbols in {:.2?} ({:.0} rows/sec)",
        total_rows, symbols_seen.len(), elapsed, rows_per_sec
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
    let csv_path = output_dir.join("metadata.csv");
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&csv_path)?;

    writeln!(file, "date,symbol,hour,row_count,size_bytes,file_path,created_at")?;
    for entry in entries {
        writeln!(
            file,
            "{},{},{},{},{},{},{}",
            entry.date,
            entry.symbol,
            entry.hour,
            entry.row_count,
            entry.size_bytes,
            entry.file_path,
            entry.created_at
        )?;
    }

    println!("Wrote metadata: {:?}", csv_path);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let started = Instant::now();

    std::fs::create_dir_all(&args.output)?;

    println!("Fast Parallel DBN to Parquet Converter");
    println!("======================================");
    println!("Output: {:?}", args.output);
    println!("Threads: {}", args.threads);
    println!("Compression: {}", if args.compress { "Zstd-1" } else { "None (faster)" });
    println!("Partition: Hourly (fewer files)");
    println!("Batch size: {} rows", FLUSH_ROWS);
    println!();

    // Create channel for write tasks
    let (tx, rx) = mpsc::channel::<WriteTask>(CHANNEL_BUFFER);
    let metadata = Arc::new(Mutex::new(Vec::<PartitionMeta>::new()));
    let rx = Arc::new(tokio::sync::Mutex::new(rx));

    // Spawn writer workers
    let mut handles = Vec::new();
    for i in 0..args.threads {
        let rx_clone = rx.clone();
        let output_clone = args.output.clone();
        let metadata_clone = metadata.clone();
        
        let handle = tokio::spawn(async move {
            loop {
                let task = {
                    let mut rx_guard = rx_clone.lock().await;
                    rx_guard.recv().await
                };
                
                match task {
                    Some(task) => {
                        // Process task inline
                        if task.buffer.len() == 0 {
                            continue;
                        }

                        let symbol_dir = output_clone.join(&task.date).join(&task.symbol);
                        
                        if i == 0 { // Only worker 0 prints to avoid spam
                            eprint!(".");
                        }
                        if let Err(e) = std::fs::create_dir_all(&symbol_dir) {
                            eprintln!("Worker {}: Failed to create dir {:?}: {}", i, symbol_dir, e);
                            continue;
                        }

                        let filename = format!("mbo_{:02}_{:03}.parquet", task.hour, task.flush_seq);
                        let path = symbol_dir.join(&filename);

                        match task.buffer.to_dataframe() {
                            Ok(mut df) => {
                                let row_count = df.height();
                                
                                match std::fs::File::create(&path) {
                                    Ok(file) => {
                                        let mut writer = ParquetWriter::new(file);
                                        
                                        if task.compress {
                                            writer = writer.with_compression(
                                                ParquetCompression::Zstd(ZstdLevel::try_new(1).ok())
                                            );
                                        } else {
                                            writer = writer.with_compression(ParquetCompression::Uncompressed);
                                        }
                                        
                                        if let Err(e) = writer.finish(&mut df) {
                                            eprintln!("Worker {}: Failed to write parquet: {}", i, e);
                                            continue;
                                        }
                                        
                                        if let Ok(meta_fs) = std::fs::metadata(&path) {
                                            let size_bytes = meta_fs.len();
                                            let rel_path = format!("{}/{}/{}", task.date, task.symbol, filename);
                                            
                                            metadata_clone.lock().unwrap().push(PartitionMeta {
                                                date: task.date,
                                                symbol: task.symbol,
                                                hour: task.hour,
                                                row_count,
                                                size_bytes,
                                                file_path: rel_path,
                                                created_at: Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                                            });
                                        }
                                    }
                                    Err(e) => eprintln!("Worker {}: Failed to create file: {}", i, e),
                                }
                            }
                            Err(e) => eprintln!("Worker {}: Failed to create DataFrame: {}", i, e),
                        }
                    }
                    None => break,
                }
            }
            println!("Worker {} finished", i);
        });
        handles.push(handle);
    }

    // Convert files
    let files = if let Some(file) = args.file {
        vec![file]
    } else if let Some(input_dir) = args.input {
        find_dbn_files(&input_dir)?
    } else {
        return Err("Must specify --input or --file".into());
    };

    println!("Found {} DBN files to convert\n", files.len());

    let mut total_converted = 0;
    for file in &files {
        match convert_file(file, &args.output, tx.clone(), args.compress).await {
            Ok(rows) => total_converted += rows,
            Err(e) => eprintln!("Error converting {:?}: {}", file, e),
        }
    }

    // Close channel and wait for workers
    drop(tx);
    for handle in handles {
        let _ = handle.await;
    }

    // Write metadata
    let entries = metadata.lock().unwrap().clone();
    write_metadata_csv(&entries, &args.output)?;

    let elapsed = started.elapsed();
    let rows_per_sec = total_converted as f64 / elapsed.as_secs_f64();
    
    println!("\n======================================");
    println!("Total converted: {} rows", total_converted);
    println!("Total time: {:.2?}", elapsed);
    println!("Average speed: {:.0} rows/sec", rows_per_sec);
    println!("Partitions created: {}", entries.len());

    Ok(())
}
