// //! DuckDB Evaluation Script
// //!
// //! Tests DuckDB performance for querying Parquet files directly.
// //! Compares against QuestDB for common query patterns.
// //!
// //! Usage:
// //!   cargo run --release --bin eval_duckdb -- --parquet-dir E:/parquet

// use duckdb::{Connection, Result};
// use std::path::PathBuf;
// use std::time::Instant;

// use clap::Parser;

// #[derive(Parser, Debug)]
// #[command(name = "eval_duckdb", about = "Evaluate DuckDB for Parquet queries")]
// struct Args {
//     /// Parquet directory
//     #[arg(long, default_value = "E:/parquet")]
//     parquet_dir: PathBuf,

//     /// Symbol to query
//     #[arg(long, default_value = "NVDA")]
//     symbol: String,

//     /// Date to query (YYYY-MM-DD)
//     #[arg(long, default_value = "2025-10-22")]
//     date: String,
// }

// fn main() -> Result<()> {
//     let args = Args::parse();
    
//     println!("DuckDB Evaluation");
//     println!("=================");
//     println!("Parquet dir: {:?}", args.parquet_dir);
//     println!("Symbol: {}", args.symbol);
//     println!("Date: {}", args.date);
//     println!();

//     let conn = Connection::open_in_memory()?;

//     // Construct paths
//     let symbol_dir = args.parquet_dir.join(&args.date).join(&args.symbol);
//     let glob_pattern = format!("{}/*.parquet", symbol_dir.display());

//     println!("Glob pattern: {}", glob_pattern);
//     println!();

//     // Test 1: Count rows
//     println!("Test 1: Count all rows");
//     let start = Instant::now();
//     let mut stmt = conn.prepare(&format!(
//         "SELECT COUNT(*) FROM read_parquet('{}')",
//         glob_pattern
//     ))?;
//     let count: i64 = stmt.query_row([], |row| row.get(0))?;
//     println!("  Count: {} rows", count);
//     println!("  Time: {:?}", start.elapsed());
//     println!();

//     // Test 2: Scan all columns
//     println!("Test 2: Full scan (all columns)");
//     let start = Instant::now();
//     let mut stmt = conn.prepare(&format!(
//         "SELECT * FROM read_parquet('{}') LIMIT 1000000",
//         glob_pattern
//     ))?;
//     let mut rows = stmt.query([])?;
//     let mut scanned = 0;
//     while let Some(_) = rows.next()? {
//         scanned += 1;
//     }
//     println!("  Scanned: {} rows", scanned);
//     println!("  Time: {:?}", start.elapsed());
//     println!();

//     // Test 3: Aggregate query
//     println!("Test 3: Aggregate (OHLCV by minute)");
//     let start = Instant::now();
//     let mut stmt = conn.prepare(&format!(
//         r#"
//         SELECT 
//             date_trunc('minute', to_timestamp(ts_event / 1000000000)) as minute,
//             MIN(price) as low,
//             MAX(price) as high,
//             FIRST(price) as open,
//             LAST(price) as close,
//             SUM(size) as volume,
//             COUNT(*) as trades
//         FROM read_parquet('{}')
//         WHERE action = 'T'
//         GROUP BY minute
//         ORDER BY minute
//         "#,
//         glob_pattern
//     ))?;
//     let mut rows = stmt.query([])?;
//     let mut bars = 0;
//     while let Some(_) = rows.next()? {
//         bars += 1;
//     }
//     println!("  Bars: {}", bars);
//     println!("  Time: {:?}", start.elapsed());
//     println!();

//     // Test 4: Time range filter
//     println!("Test 4: Time range filter (1 hour window)");
//     let start = Instant::now();
//     let mut stmt = conn.prepare(&format!(
//         r#"
//         SELECT COUNT(*) 
//         FROM read_parquet('{}')
//         WHERE ts_event >= 1729612800000000000 
//           AND ts_event < 1729616400000000000
//         "#,
//         glob_pattern
//     ))?;
//     let count: i64 = stmt.query_row([], |row| row.get(0))?;
//     println!("  Count: {} rows", count);
//     println!("  Time: {:?}", start.elapsed());
//     println!();

//     // Test 5: Schema inspection
//     println!("Test 5: Schema inspection");
//     let start = Instant::now();
//     let mut stmt = conn.prepare(&format!(
//         "DESCRIBE SELECT * FROM read_parquet('{}' , filename=true)",
//         glob_pattern
//     ))?;
//     let mut rows = stmt.query([])?;
//     println!("  Schema:");
//     while let Some(row) = rows.next()? {
//         let name: String = row.get(0)?;
//         let dtype: String = row.get(1)?;
//         println!("    {}: {}", name, dtype);
//     }
//     println!("  Time: {:?}", start.elapsed());
//     println!();

//     // Test 6: Multi-day query (if data exists)
//     println!("Test 6: Multi-symbol glob");
//     let multi_glob = format!("{}/**/*.parquet", args.parquet_dir.display());
//     let start = Instant::now();
//     let mut stmt = conn.prepare(&format!(
//         r#"
//         SELECT symbol, COUNT(*) as row_count
//         FROM read_parquet('{}')
//         GROUP BY symbol
//         ORDER BY row_count DESC
//         LIMIT 10
//         "#,
//         multi_glob
//     ))?;
//     let mut rows = stmt.query([])?;
//     println!("  Top symbols by row count:");
//     while let Some(row) = rows.next()? {
//         let symbol: String = row.get(0)?;
//         let count: i64 = row.get(1)?;
//         println!("    {}: {} rows", symbol, count);
//     }
//     println!("  Time: {:?}", start.elapsed());
//     println!();

//     // Test 7: Memory usage estimate
//     println!("Test 7: Estimate data size");
//     let start = Instant::now();
//     let mut stmt = conn.prepare(&format!(
//         r#"
//         SELECT 
//             COUNT(*) as rows,
//             SUM(octet_length(symbol)) as symbol_bytes,
//             COUNT(*) * 8 * 6 as numeric_bytes_approx
//         FROM read_parquet('{}')
//         "#,
//         glob_pattern
//     ))?;
//     let mut rows_iter = stmt.query([])?;
//     if let Some(row) = rows_iter.next()? {
//         let rows: i64 = row.get(0)?;
//         let symbol_bytes: i64 = row.get(1)?;
//         let numeric_bytes: i64 = row.get(2)?;
//         let total_mb = (symbol_bytes + numeric_bytes) as f64 / (1024.0 * 1024.0);
//         println!("  Rows: {}", rows);
//         println!("  Estimated size: {:.2} MB in memory", total_mb);
//     }
//     println!("  Time: {:?}", start.elapsed());
//     println!();

//     println!("=================");
//     println!("DuckDB Evaluation Complete");
//     println!();
//     println!("Key findings:");
//     println!("- DuckDB can query Parquet files directly without loading");
//     println!("- Glob patterns allow multi-file queries");
//     println!("- Columnar format enables fast aggregations");
//     println!("- Good for analytical queries on cold storage");

//     Ok(())
// }
