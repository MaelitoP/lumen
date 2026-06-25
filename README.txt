Lumen is an experimental document database written in Rust. It is a personal project for learning how databases are built from the inside: storage and full-text indexing on Tantivy, write-ahead logging and crash recovery, schema and mapping validation, an HTTP API, and replication across nodes with Raft through openraft. It is a learning project, not a production database.

A complete single node works today. You can create collections with explicit, typed mappings, index and replace documents by id, and run full-text and exact search over them through an HTTP/JSON API. Every acknowledged write is appended to a write-ahead log and fsync’d before the response, so it survives a crash, including kill -9; on macOS the WAL uses F_FULLFSYNC for power-loss durability. Documents become searchable at the next checkpoint, when buffered writes are committed to the index.

A multi-node mode works too. Nodes form a single Raft group and every mutation flows through one replicated log, so each node applies the same committed writes in the same order; replication is the log itself. Writes are routed to the leader. Kill the leader and the cluster elects a new one and keeps serving. A restarted node rejoins and catches up by replaying the log or installing a snapshot.

Building it needs the Rust toolchain pinned in rust-toolchain.toml. Build the workspace, then run a single node:

    cargo build --workspace
    cargo run -p lumen-api -- standalone --data-dir ./data --bind 127.0.0.1:7700

To run a node in a Raft cluster, use the cluster subcommand. The cluster is then formed through the management API: init, add learners, change membership.

    cargo run -p lumen-api -- cluster --id 1 --raft-addr 127.0.0.1:8080 \
        --data-dir ./data1 --bind 127.0.0.1:7700

Run the test suite with cargo. On macOS, Tantivy’s zstd-sys needs libiconv, so export LIBRARY_PATH first:

    export LIBRARY_PATH="$(brew --prefix libiconv)/lib"
    cargo test --workspace

Licensed under Apache-2.0; see LICENSE.
