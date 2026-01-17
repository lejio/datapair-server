use databento::dbn::decode::AsyncDbnDecoder;
use dbn::decode::DbnMetadata;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = PathBuf::from("../datapair-main-server/data/NVDA.dbn.zst");
    let mut decoder = AsyncDbnDecoder::from_zstd_file(&file_path).await?;
    let metadata = decoder.metadata();
    println!("Start: {}", metadata.start);
    println!("End: {:?}", metadata.end);
    Ok(())
}
