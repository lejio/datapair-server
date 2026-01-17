//! Export MBO data from QuestDB to Parquet files for fast analytics.
//!
//! Usage:
//!   cargo run --bin export_parquet -- --date 2025-10-22 --output ./parquet/
//!   cargo run --bin export_parquet -- --start 2025-10-01 --end 2025-10-31 --output ./parquet/
//!
//! Exports are partitioned by hour: mbo_2025-10-22_14.parquet

use chrono::{NaiveDate, NaiveDateTime, Duration};
use clap::Parser;
use polars::prelude::*;
use std::path::PathBuf;
use tokio_postgres::NoTls;

#[derive(Parser, Debug)]
#[command(name = "export_parquet", about = "Export QuestDB MBO data to Parquet")]
struct Args {
    /// Single date to export (YYYY-MM-DD)
    #[arg(long)]
    date: Option<NaiveDate>,

    /// Start date for range export (YYYY-MM-DD)
    #[arg(long)]
    start: Option<NaiveDate>,

    /// End date for range export (YYYY-MM-DD, exclusive)
    #[arg(long)]
    end: Option<NaiveDate>,

    /// Output directory for Parquet files
    #[arg(long, short, default_value = "../../../data_parquet")]
    output: PathBuf,

    /// QuestDB PostgreSQL host
    #[arg(long, default_value = "localhost")]
    host: String,

    /// QuestDB PostgreSQL port
    #[arg(long, default_value = "8812")]
    port: u16,

    /// Optional symbol filter (exports all if not specified)
    #[arg(long)]
    symbol: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Determine date range
    let dates: Vec<NaiveDate> = if let Some(d) = args.date {
        vec![d]
    } else if let (Some(start), Some(end)) = (args.start, args.end) {
        let mut dates = Vec::new();
        let mut current = start;
        while current < end {
            dates.push(current);
            current = current.succ_opt().unwrap();
        }
        dates
    } else {
        eprintln!("Error: Specify either --date or both --start and --end");
        std::process::exit(1);
    };

    // Create output directory
    std::fs::create_dir_all(&args.output)?;

    // Connect to QuestDB PostgreSQL wire
    let conn_str = format!(
        "host={} port={} user=admin password=quest dbname=qdb",
        args.host, args.port
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;

    // Spawn connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    println!("Connected to QuestDB at {}:{}", args.host, args.port);

    let mut total_rows = 0u64;
    let mut total_files = 0u32;

    for date in dates {
        println!("Processing {}...", date);
        
        // Export in 15-minute intervals to reduce memory usage
        for hour in 0..24 {
            for quarter in 0..4 {
                let minute = quarter * 15;
                let start_dt = date.and_hms_opt(hour, minute, 0).unwrap();
                let end_dt = start_dt + Duration::minutes(15);
                
                let start_ts = format!("{}Z", start_dt.format("%Y-%m-%dT%H:%M:%S"));
                let end_ts = format!("{}Z", end_dt.format("%Y-%m-%dT%H:%M:%S"));

                let query = if let Some(ref sym) = args.symbol {
                    format!(
                        r#"SELECT 
                            ts_event, symbol, instrument_id, order_id, price_raw, price,
                            size, flags, channel_id, action, side, ts_recv, ts_in_delta, sequence
                        FROM mbo_ticks 
                        WHERE ts_event >= '{}' AND ts_event < '{}' AND symbol = '{}'
                        ORDER BY ts_event"#,
                        start_ts, end_ts, sym
                    )
                } else {
                    format!(
                        r#"SELECT 
                            ts_event, symbol, instrument_id, order_id, price_raw, price,
                            size, flags, channel_id, action, side, ts_recv, ts_in_delta, sequence
                        FROM mbo_ticks 
                        WHERE ts_event >= '{}' AND ts_event < '{}'
                        ORDER BY ts_event"#,
                        start_ts, end_ts
                    )
                };

                let rows = client.query(&query, &[]).await?;

                if rows.is_empty() {
                    continue;
                }

                // Build columns from rows
                let row_count = rows.len();
                let mut ts_event: Vec<i64> = Vec::with_capacity(row_count);
                let mut symbol: Vec<String> = Vec::with_capacity(row_count);
                let mut instrument_id: Vec<i32> = Vec::with_capacity(row_count);
                let mut order_id: Vec<i64> = Vec::with_capacity(row_count);
                let mut price_raw: Vec<i64> = Vec::with_capacity(row_count);
                let mut price: Vec<f64> = Vec::with_capacity(row_count);
                let mut size: Vec<i32> = Vec::with_capacity(row_count);
                let mut flags: Vec<i32> = Vec::with_capacity(row_count);
                let mut channel_id: Vec<i16> = Vec::with_capacity(row_count);
                let mut action: Vec<String> = Vec::with_capacity(row_count);
                let mut side: Vec<String> = Vec::with_capacity(row_count);
                let mut ts_recv: Vec<i64> = Vec::with_capacity(row_count);
                let mut ts_in_delta: Vec<i32> = Vec::with_capacity(row_count);
                let mut sequence: Vec<i32> = Vec::with_capacity(row_count);

                for row in &rows {
                    // QuestDB returns timestamps as chrono types via pg wire
                    let ts_evt: chrono::NaiveDateTime = row.get(0);
                    ts_event.push(ts_evt.and_utc().timestamp_nanos_opt().unwrap_or(0));

                    symbol.push(row.get::<_, String>(1));
                    instrument_id.push(row.get(2));
                    order_id.push(row.get(3));
                    price_raw.push(row.get(4));
                    price.push(row.get(5));
                    size.push(row.get(6));
                    flags.push(row.get(7));
                    channel_id.push(row.get(8));
                    action.push(row.get::<_, String>(9));
                    side.push(row.get::<_, String>(10));

                    let ts_r: chrono::NaiveDateTime = row.get(11);
                    ts_recv.push(ts_r.and_utc().timestamp_nanos_opt().unwrap_or(0));

                    ts_in_delta.push(row.get(12));
                    sequence.push(row.get(13));
                }
                
                // Drop rows to free memory before DataFrame creation
                drop(rows);

                // Build DataFrame
                let df = DataFrame::new(vec![
                    Column::new("ts_event".into(), ts_event),
                    Column::new("symbol".into(), symbol),
                    Column::new("instrument_id".into(), instrument_id),
                    Column::new("order_id".into(), order_id),
                    Column::new("price_raw".into(), price_raw),
                    Column::new("price".into(), price),
                    Column::new("size".into(), size),
                    Column::new("flags".into(), flags),
                    Column::new("channel_id".into(), channel_id),
                    Column::new("action".into(), action),
                    Column::new("side".into(), side),
                    Column::new("ts_recv".into(), ts_recv),
                    Column::new("ts_in_delta".into(), ts_in_delta),
                    Column::new("sequence".into(), sequence),
                ])?;

                // Write to Parquet with 15-min naming: mbo_2025-10-22_14-30.parquet
                let filename = if let Some(ref sym) = args.symbol {
                    format!("mbo_{}_{}_{:02}-{:02}.parquet", sym, date, hour, minute)
                } else {
                    format!("mbo_{}_{:02}-{:02}.parquet", date, hour, minute)
                };
                let path = args.output.join(&filename);

                let file = std::fs::File::create(&path)?;
                ParquetWriter::new(file)
                    .with_compression(ParquetCompression::Zstd(None))
                    .finish(&mut df.clone())?;

                total_rows += row_count as u64;
                total_files += 1;

                println!(
                    "  {} {:02}:{:02} - {} rows -> {}",
                    date, hour, minute, row_count, filename
                );
            }
        }
    }

    println!("\nDone! Exported {} total rows to {} files", total_rows, total_files);
    Ok(())
}
