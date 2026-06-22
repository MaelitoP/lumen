# Lumen

Lumen is an experimental, single-node **document database** written in Rust. It
is a personal project for learning how databases and distributed systems are
built from the inside — storage and indexing (on Tantivy), write-ahead logging
and crash recovery, schema/mapping, and an HTTP/JSON API. It is a learning
project, not a production database.

A complete single node works today: an HTTP API over collections with explicit
mappings, document CRUD, full-text and exact search, and a write-ahead log that
survives a crash. Distribution across nodes (via Raft) is planned, not built.

## What works today

- **Collections with explicit mappings** — typed fields (`text`, `keyword`,
  `i64`, `f64`, `date`) with per-field roles (`indexed`, `fast`). Validation is
  strict: a document is rejected if a field has the wrong type or isn't in the
  mapping. Mappings are immutable (drop + recreate to change).
- **Document CRUD** — index with a client- or server-assigned `_id`; replace
  (upsert), get, and delete by id. The original JSON is stored and returned.
- **Search** — full-text and exact queries via Tantivy's query parser, with
  pagination; the original document comes back with each hit.
- **Durability** — every write is appended to a write-ahead log and fsync'd
  before the request is acknowledged, so an acknowledged write survives a crash
  (including `kill -9`). On macOS the WAL uses `F_FULLFSYNC` for power-loss
  durability.

## Crates

| Crate           | Role                                                                       | Status      |
| --------------- | -------------------------------------------------------------------------- | ----------- |
| `lumen-proto`   | Protobuf wire/durable types (commands, WAL entries, mappings)              | Implemented |
| `lumen-core`    | The engine: mappings, catalog, collections, CRUD, search, WAL + recovery   | Implemented |
| `lumen-api`     | HTTP/JSON server (axum)                                                     | Implemented |
| `lumen-cluster` | Raft-based distribution (planned)                                          | Placeholder |
| `lumen-cli`     | Command-line client                                                        | Placeholder |

## Build

Uses the toolchain pinned in `rust-toolchain.toml` (Rust 1.85).

```bash
git clone https://github.com/MaelitoP/lumen && cd lumen
cargo build --workspace
```

On macOS, Tantivy's `zstd-sys` links against libiconv; point the linker at
Homebrew's copy (otherwise the build fails with `ld: library not found for -liconv`):

```bash
brew install libiconv
export LIBRARY_PATH="$(brew --prefix libiconv)/lib"
cargo build --workspace
```

## Run

```bash
cargo run -p lumen-api -- --data-dir ./data --bind 127.0.0.1:7700
```

Flags (all optional; environment variable in parentheses):

- `--data-dir` (`LUMEN_DATA_DIR`, default `data`) — where collections and the WAL live.
- `--bind` (`LUMEN_BIND`, default `127.0.0.1:7700`) — listen address.
- `--checkpoint-interval-secs` (`LUMEN_CHECKPOINT_INTERVAL_SECS`, default `30`) —
  how often buffered writes are committed and the WAL is trimmed (see
  [Durability and visibility](#durability-and-visibility)).

## API

| Method & path | Purpose |
| --- | --- |
| `PUT /collections/{name}` | Create a collection with a mapping (request body). Idempotent: same mapping → `200`, different → `409`. |
| `GET /collections` | List collection names. |
| `GET /collections/{name}` | Describe a collection (its mapping). |
| `DELETE /collections/{name}` | Drop a collection. |
| `POST /collections/{name}/documents` | Index a document; the server generates the `_id`. |
| `PUT /collections/{name}/documents/{id}` | Index/replace a document with a client-chosen `_id`. |
| `GET /collections/{name}/documents/{id}` | Fetch a document by id. |
| `DELETE /collections/{name}/documents/{id}` | Delete a document by id. |
| `GET /collections/{name}/documents/search?q=&limit=&offset=` | Search; returns hits with score and source. |

Errors come back as `{ "error": { "type", "message" } }` with the matching HTTP
status (`400` validation, `404` not found, `409` schema conflict, `500` internal).

### Quickstart

```bash
# create a collection with a mapping
curl -X PUT localhost:7700/collections/books \
  -H 'content-type: application/json' \
  -d '{"fields":{"title":{"type":"text","indexed":true},"year":{"type":"i64","indexed":true,"fast":true}}}'

# index a document (server-assigned id)
curl -X POST localhost:7700/collections/books/documents \
  -H 'content-type: application/json' \
  -d '{"title":"The Rust Programming Language","year":2018}'
# -> {"id":"<uuid>","result":"created"}

# index with a chosen id (upsert)
curl -X PUT localhost:7700/collections/books/documents/tdg \
  -H 'content-type: application/json' \
  -d '{"title":"Designing Data-Intensive Applications","year":2017}'

# search (see "Durability and visibility" for when a write becomes searchable)
curl 'localhost:7700/collections/books/documents/search?q=rust'
# -> {"hits":[{"id":"<uuid>","score":0.69,"source":{"title":"The Rust Programming Language","year":2018}}],"total":1,"took_ms":0}
```

`scripts/smoke.sh` runs this whole flow — including a `kill -9` and restart to
demonstrate crash recovery — against a real server process.

## Durability and visibility

A write is **durable** the moment it is acknowledged: it has been appended to the
WAL and fsync'd. It becomes **searchable** at the next checkpoint, when buffered
writes are committed to the index — by default every 30 seconds, and on a clean
shutdown. (Tantivy makes documents visible only on commit; there is no cheaper
"refresh", so the checkpoint interval trades search freshness against commit
cost.) Lower `--checkpoint-interval-secs` for fresher search at the price of more
frequent commits. Either way, an acknowledged write is never lost across a crash.

## Testing

```bash
cargo test --workspace      # unit + integration tests (incl. WAL crash recovery)
bash scripts/smoke.sh       # end-to-end HTTP smoke test against a live server
```

The `justfile` wraps the common checks (`just build`, `just test`, `just fmt`,
`just clippy`). A `Dockerfile.dev` and `docker-compose.yml` provide a Linux build
container if you'd rather not set up libiconv locally.

## Roadmap

Today Lumen is a complete single-node document database. The planned next step is
to distribute it across nodes: writes flow through a single Raft-replicated log
(`lumen-cluster`), giving replication and failover. Vector / embedding search is a
later direction.

## License

Apache-2.0. See `LICENSE`.
