//! Query Parquet files with Polars for fast analytics.
//!
//! Usage examples:
//!   # Get all trades for NVDA on a date
//!   cargo run --bin query_parquet -- --dir ./parquet --symbol NVDA --date 2025-10-22
//!
//!   # Run a custom SQL query
//!   cargo run --bin query_parquet -- --dir ./parquet --sql "SELECT symbol, COUNT(*) as cnt FROM df GROUP BY symbol"
//!
//!   # Get OHLCV at 1-minute bars
//!   cargo run --bin query_parquet -- --dir ./parquet --symbol NVDA --date 2025-10-22 --ohlcv 1m

use chrono::NaiveDate;
use clap::{Parser, ValueEnum};
use polars::prelude::*;
use polars::sql::SQLContext;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "query_parquet", about = "Query MBO Parquet files with Polars")]
struct Args {
    /// Directory containing Parquet files
    #[arg(long, short, default_value = "../../../data_parquet")]
    dir: PathBuf,

    /// Filter by symbol
    #[arg(long)]
    symbol: Option<String>,

    /// Filter by date (YYYY-MM-DD)
    #[arg(long)]
    date: Option<NaiveDate>,

    /// Start date (YYYY-MM-DD)
    #[arg(long)]
    start: Option<NaiveDate>,

    /// End date (YYYY-MM-DD, exclusive)
    #[arg(long)]
    end: Option<NaiveDate>,

    /// Custom SQL query (table is named 'df')
    #[arg(long)]
    sql: Option<String>,

    /// Output OHLCV bars at specified interval
    #[arg(long)]
    ohlcv: Option<String>,

    /// Limit output rows
    #[arg(long, default_value = "20")]
    limit: u32,

    /// Output format
    #[arg(long, value_enum, default_value = "table")]
    format: OutputFormat,
}

#[derive(Debug, Clone, ValueEnum)]
enum OutputFormat {
    Table,
    Csv,
    Json,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Find parquet files to load
    let patterns = build_file_patterns(&args)?;
    if patterns.is_empty() {
        eprintln!("No matching Parquet files found in {}", args.dir.display());
        return Ok(());
    }

    println!("Loading {} file(s)...", patterns.len());

    // Lazy load all matching files
    let lf_args = ScanArgsParquet::default();
    let mut lf = LazyFrame::scan_parquet_files(
        patterns.clone().into(),
        lf_args,
    )?;

    // Apply symbol filter
    if let Some(ref sym) = args.symbol {
        lf = lf.filter(col("symbol").eq(lit(sym.clone())));
    }

    // Handle custom SQL
    if let Some(ref sql_query) = args.sql {
        let ctx = SQLContext::new();
        let mut df = lf.collect()?;
        let mut ctx = ctx;
        ctx.register("df", df.lazy());
        let result = ctx.execute(sql_query)?;
        df = result.collect()?;
        output_dataframe(&df, &args)?;
        return Ok(());
    }

    // Handle OHLCV aggregation
    if let Some(ref interval) = args.ohlcv {
        let df = compute_ohlcv(lf, interval)?;
        output_dataframe(&df, &args)?;
        return Ok(());
    }

    // Default: just show data
    let df = lf.limit(args.limit).collect()?;
    output_dataframe(&df, &args)?;

    Ok(())
}

fn build_file_patterns(args: &Args) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    let mut files = Vec::new();

    // Determine which dates to load
    let dates: Option<Vec<NaiveDate>> = if let Some(d) = args.date {
        Some(vec![d])
    } else if let (Some(start), Some(end)) = (args.start, args.end) {
        let mut dates = Vec::new();
        let mut current = start;
        while current < end {
            dates.push(current);
            current = current.succ_opt().unwrap();
        }
        Some(dates)
    } else {
        None
    };

    if let Some(dates) = dates {
        // Load specific date files
        for date in dates {
            if let Some(ref sym) = args.symbol {
                // Try symbol-specific file first
                let sym_file = args.dir.join(format!("mbo_{}_{}.parquet", sym, date));
                if sym_file.exists() {
                    files.push(sym_file);
                    continue;
                }
            }
            // Fall back to date file
            let date_file = args.dir.join(format!("mbo_{}.parquet", date));
            if date_file.exists() {
                files.push(date_file);
            }
        }
    } else {
        // Load all parquet files
        for entry in std::fs::read_dir(&args.dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().map_or(false, |e| e == "parquet") {
                files.push(path);
            }
        }
        files.sort();
    }

    Ok(files)
}

fn compute_ohlcv(lf: LazyFrame, interval: &str) -> PolarsResult<DataFrame> {
    // Parse interval (e.g., "1m", "5m", "1h")
    let duration = parse_interval(interval);

    // Filter for trade actions only (if you have 'T' for trades)
    // For MBO data, trades are typically action='T'
    let lf = lf.filter(col("action").eq(lit("T")));

    // Convert ts_event to datetime and group
    lf.with_column(
        (col("ts_event") / lit(1_000_000i64)).cast(DataType::Datetime(TimeUnit::Milliseconds, None)).alias("datetime")
    )
    .group_by_dynamic(
        col("datetime"),
        [],
        DynamicGroupOptions {
            every: duration.clone(),
            period: duration,
            offset: Duration::parse("0s"),
            ..Default::default()
        },
    )
    .agg([
        col("price").first().alias("open"),
        col("price").max().alias("high"),
        col("price").min().alias("low"),
        col("price").last().alias("close"),
        col("size").sum().alias("volume"),
        col("price").count().alias("trades"),
    ])
    .sort(["datetime"], Default::default())
    .collect()
}

fn parse_interval(s: &str) -> Duration {
    let s = s.trim();
    if s.ends_with('m') || s.ends_with('M') {
        let n: i64 = s[..s.len()-1].parse().unwrap_or(1);
        Duration::parse(&format!("{}m", n))
    } else if s.ends_with('h') || s.ends_with('H') {
        let n: i64 = s[..s.len()-1].parse().unwrap_or(1);
        Duration::parse(&format!("{}h", n))
    } else if s.ends_with('s') || s.ends_with('S') {
        let n: i64 = s[..s.len()-1].parse().unwrap_or(1);
        Duration::parse(&format!("{}s", n))
    } else {
        Duration::parse("1m")
    }
}

fn output_dataframe(df: &DataFrame, args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    match args.format {
        OutputFormat::Table => {
            println!("{}", df);
        }
        OutputFormat::Csv => {
            let mut buf = Vec::new();
            CsvWriter::new(&mut buf).finish(&mut df.clone())?;
            println!("{}", String::from_utf8(buf)?);
        }
        OutputFormat::Json => {
            let mut buf = Vec::new();
            JsonWriter::new(&mut buf)
                .with_json_format(JsonFormat::Json)
                .finish(&mut df.clone())?;
            println!("{}", String::from_utf8(buf)?);
        }
    }
    println!("\n{} rows × {} columns", df.height(), df.width());
    Ok(())
}
