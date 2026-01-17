use dbn::{
    decode::{AsyncDbnDecoder, DbnMetadata},
    enums::RType,
    record::MboMsg,
    Record,
};
use std::{env, path::PathBuf};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = env::var("DATA_DIR").unwrap_or_else(|_| "/mnt/e/data".to_string());
    let path = PathBuf::from(&data_dir);

    if !path.exists() {
        eprintln!("Data directory does not exist: {}", data_dir);
        return Ok(());
    }

    let mut files: Vec<PathBuf> = std::fs::read_dir(&path)?
        .filter_map(|entry| {
            entry.ok().and_then(|e| {
                let p = e.path();
                if p.is_file() {
                    if let Some(name) = p.file_name().and_then(|n| n.to_str()) {
                        if name.ends_with(".mbo.dbn.zst") {
                            return Some(p);
                        }
                    }
                }
                None
            })
        })
        .collect();

    files.sort();

    println!("Counting rows in {} files...\n", files.len());

    let mut total_rows = 0usize;
    let mut file_count = 0usize;

    for file_path in &files {
        match count_file(&file_path).await {
            Ok(count) => {
                println!(
                    "✓ {:?}: {} rows",
                    file_path.file_name().unwrap_or_default(),
                    count
                );
                total_rows += count;
                file_count += 1;
            }
            Err(e) => {
                eprintln!(
                    "✗ {:?}: {}",
                    file_path.file_name().unwrap_or_default(),
                    e
                );
            }
        }
    }

    println!("\n=== Summary ===");
    println!("Total files: {}", files.len());
    println!("Successfully counted: {}", file_count);
    println!("Total rows: {}", total_rows);
    println!("Average per file: {}", total_rows / file_count.max(1));

    Ok(())
}

async fn count_file(file_path: &PathBuf) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    let mut decoder = AsyncDbnDecoder::from_zstd_file(&file_path).await?;
    let mut count = 0usize;

    while let Some(rec_ref) = decoder.decode_record_ref().await? {
        if matches!(rec_ref.header().rtype(), Ok(RType::Mbo)) {
            if rec_ref.get::<MboMsg>().is_some() {
                count += 1;
            }
        }
    }

    Ok(count)
}
