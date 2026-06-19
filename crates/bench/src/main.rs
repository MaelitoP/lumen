//! # Lumen Bench – single-node spike
//! Ingest `--generate N` synthetic docs through `lumen-core` and measure a sample query.

use std::path::PathBuf;
use std::time::Instant;

use clap::Parser;
use lipsum::lipsum;
use lumen_core::Index;
use rand::Rng;

#[derive(Parser, Debug)]
#[command(about = "Lumen baseline ingest/search benchmark")]
struct Args {
    #[arg(long, default_value_t = 1_000_000)]
    generate: u64,

    #[arg(long, default_value = "bench-index")]
    index_path: PathBuf,
}

fn main() -> lumen_core::Result<()> {
    let args = Args::parse();
    let mut index = Index::open(&args.index_path, 256 * 1024 * 1024)?;

    let mut rng = rand::thread_rng();
    let lorem_words = [
        "lorem",
        "ipsum",
        "dolor",
        "amet",
        "consectetur",
        "adipiscing",
        "elit",
    ];

    println!("Ingesting {} docs…", args.generate);
    let ingest_start = Instant::now();
    for i in 0..args.generate {
        let body = format!(
            "{} {} {}",
            lorem_words[rng.gen_range(0..lorem_words.len())],
            lipsum(rng.gen_range(10..30)),
            i
        );
        index.add_document(&format!("Document #{i}"), &body)?;
        if i % 100_000 == 0 && i != 0 {
            println!("  …{} docs", i);
        }
    }
    index.commit()?;
    let ingest_elapsed = ingest_start.elapsed();

    println!(
        "Ingest throughput: {:.1} docs/s",
        args.generate as f64 / ingest_elapsed.as_secs_f64()
    );

    let query_start = Instant::now();
    let hits = index.search("lorem ipsum", 10)?;
    let query_elapsed = query_start.elapsed();

    println!("Query latency: {query_elapsed:?}");
    println!("Top hits: {}", hits.len());

    Ok(())
}
