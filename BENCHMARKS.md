# Benchmarks

A single-node baseline measured with `lumen-bench`, which drives the
`lumen-core` `Index` abstraction. These numbers exist to track the engine's
starting point, not to make competitive claims. The methodology is deliberately
simple and the results are not statistically rigorous.

## What is measured

`lumen-bench`:

1. Generates `N` synthetic documents (`title` plus a short lorem-ipsum `body`).
2. Indexes them through `lumen_core::Index::add_document`, then `commit`s once.
3. Runs a single `"lorem ipsum"` query and times it.

- **Ingest throughput** is `N / wall_clock`, where the wall clock spans the whole
  ingest loop and the final commit (segment serialization included).
- **Query latency** is the wall-clock time of one `search` call returning the top
  10 hits. Because nearly every document matches `lorem`/`ipsum`, this is closer
  to a worst-case scan than a selective lookup. There is no warm-up run, and the
  reader is opened inside the timed call.

## Environment

| | |
| --- | --- |
| Machine | Apple M4 Pro, 14 cores, 48 GB RAM |
| OS | macOS 26.5.1 |
| Toolchain | rustc 1.96.0 |
| Tantivy | 0.22 |
| Build | `--release` |
| Writer memory budget | 256 MB |

## Results

`--generate 1000000`, three consecutive runs, each into a fresh index directory:

| Run | Ingest throughput | Query latency |
| --- | ----------------- | ------------- |
| 1 | 206,778 docs/s | 12.57 ms |
| 2 | 205,968 docs/s | 12.45 ms |
| 3 | 204,092 docs/s | 12.10 ms |

## Reproducing

```bash
cargo run -p lumen-bench --release -- --generate 1000000 --index-path /tmp/lumen-bench
```

On macOS, install libiconv first (see the README).

## Caveats

- Synthetic data with very low term diversity; not representative of real text.
- A single query against an almost-entirely-matching corpus; selective queries
  would be faster.
- No warm-up, no percentiles, single thread of queries. Treat the numbers as an
  order-of-magnitude baseline on one machine.
