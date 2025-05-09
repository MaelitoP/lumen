//! # Lumen Bench – single‑node spike
//! Ingest `--generate N` synthetic docs into a Tantivy index and measure a sample query.

use clap::Parser;
use lipsum::lipsum;
use rand::Rng;
use std::path::PathBuf;
use std::time::Instant;
use tantivy::collector::TopDocs;
use tantivy::schema::*;
use tantivy::{doc, Index};

#[derive(Parser, Debug)]
#[command(about = "Lumen baseline ingest/search benchmark" )]
struct Args {
    #[arg(long, default_value_t = 1_000_000)]
    generate: u64,

    #[arg(long, default_value = "bench-index")]
    index_path: PathBuf,
}

fn main() -> tantivy::Result<()> {
    let args = Args::parse();

    let mut schema_builder = Schema::builder();
    let title = schema_builder.add_text_field("title", TEXT | STORED);
    let body = schema_builder.add_text_field("body", TEXT);
    let schema = schema_builder.build();

    let index = if args.index_path.exists() {
        Index::open_in_dir(&args.index_path)?
    } else {
        std::fs::create_dir_all(&args.index_path)?;
        Index::create_in_dir(&args.index_path, schema.clone())?
    };

    let mut writer = index.writer(256 * 1024 * 1024)?; // 256 MB mem‑budget
    let mut rng = rand::thread_rng();
    let lorem_words = ["lorem", "ipsum", "dolor", "amet", "consectetur", "adipiscing", "elit"];

    println!("Ingesting {} docs…", args.generate);
    let ingest_start = Instant::now();
    for i in 0..args.generate {
        let doc = doc!(
            title => format!("Document #{i}"),
            body  => format!("{} {} {}", lorem_words[rng.gen_range(0..7)], lipsum( rng.gen_range(10..30) ), i),
        );
        writer.add_document(doc);
        if i % 100_000 == 0 && i != 0 { println!("  …{} docs", i); }
    }
    writer.commit()?;
    let ingest_elapsed = ingest_start.elapsed();

    println!("Ingest throughput: {:.1} docs/s", args.generate as f64 / ingest_elapsed.as_secs_f64());

    let reader = index.reader()?;
    let searcher = reader.searcher();
    let qp = tantivy::query::QueryParser::for_index(&index, vec![title, body]);
    let query = qp.parse_query("lorem ipsum")?;

    let query_start = Instant::now();
    let top_docs = searcher.search(&query, &TopDocs::with_limit(10))?;
    let query_elapsed = query_start.elapsed();

    println!("Query latency: {:?}", query_elapsed);
    println!("Top docs IDs: {:?}", top_docs);

    Ok(())
}
