Lumen is an experimental, single-node document database written in Rust. It is a
personal project for learning how databases are built from the inside: storage
and full-text indexing (on Tantivy), write-ahead logging and crash recovery,
schema and mapping validation, and an HTTP API. It is a learning project,
not a production database.

A complete single node works today. You create collections with explicit, typed
mappings, index and replace documents by id, and run full-text and exact search
over them through an HTTP/JSON API. Every acknowledged write is appended to a
write-ahead log and fsync'd before the response, so it survives a crash,
including kill -9; on macOS the WAL uses F_FULLFSYNC for power-loss durability.
Documents become searchable at the next checkpoint, when buffered writes are
committed to the index. Distribution across nodes via Raft is planned, not built.

Building it needs the Rust toolchain pinned in rust-toolchain.toml. Build the
workspace, then run the API:

    cargo build --workspace
    cargo run -p lumen-api -- --data-dir ./data --bind 127.0.0.1:7700

Licensed under Apache-2.0; see LICENSE.
