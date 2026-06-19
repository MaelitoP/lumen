# Lumen

Lumen is an experimental, unfinished search engine written in Rust. It is a
personal project for studying how search engines and distributed systems are
built from the inside: Tantivy segment storage, write-ahead logging, an
Elasticsearch-style API surface, shard replication, and cluster metadata.

It is not a product and not production-ready. Most of what is described below is
design intent, not implemented yet — see the status table for what actually
exists today.

## What it explores

The questions the project is meant to work through:

- How a Tantivy-based index behaves on a single node (ingest throughput, query
  latency) before any distribution is added.
- What a per-shard write-ahead log would need to look like for durable ingest.
- How cluster metadata (index → shard → node mapping) could be coordinated with
  embedded Raft instead of an external store.
- How primary/replica segment replication compares to routing every write
  through a consensus log.
- What a small Elasticsearch-like REST/JSON API would map onto internally.

These directions are design intent, not working code. None of them are
implemented yet beyond the single-node benchmark below.

## Current status

| Crate           | Purpose (intended)                     | Status                              |
| --------------- | -------------------------------------- | ----------------------------------- |
| `lumen-bench`   | Single-node Tantivy ingest/query spike | Implemented and runnable            |
| `lumen-core`    | Index and shard logic                  | Placeholder — not implemented       |
| `lumen-api`     | REST/JSON gateway                      | Placeholder — not implemented       |
| `lumen-cluster` | Cluster metadata and coordination      | Placeholder — not implemented       |
| `lumen-cli`     | Administration CLI                     | Stub — prints a work-in-progress line |

The only component that does real work today is the benchmark. The other crates
are empty placeholders that mark where planned work would go.

## Building

Requires the toolchain pinned in [`rust-toolchain.toml`](rust-toolchain.toml).

```bash
git clone https://github.com/MaelitoP/lumen && cd lumen
cargo build --workspace
```

On macOS, Tantivy's zstd dependency links against libiconv:

```bash
brew install libiconv
export LIBICONV_LIB_DIR=$(brew --prefix libiconv)/lib
cargo build --workspace
```

## Running the benchmark

`lumen-bench` generates synthetic documents, indexes them into a local Tantivy
directory, and reports ingest throughput and the latency of a sample query.

```bash
cargo run -p lumen-bench --release -- --generate 1000000
```

Flags:

- `--generate N` — number of synthetic documents to index (default: 1,000,000).
- `--index-path PATH` — index directory (default: `bench-index`, gitignored).

The index directory is reused across runs if it already exists.

## Development

Common tasks are wrapped in the [`justfile`](justfile):

```bash
just build    # cargo build --workspace
just test     # cargo test --workspace
just fmt      # cargo fmt --all --check
just clippy   # cargo clippy --all-targets --all-features -- -D warnings
```

A `Dockerfile.dev` and `docker-compose.yml` provide a Linux build container,
which is useful for running the benchmark without the macOS libiconv setup.

## License

Apache-2.0. See [`LICENSE`](LICENSE).
