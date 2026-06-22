# Building Lumen

Lumen uses the toolchain pinned in `rust-toolchain.toml` (Rust 1.85).

```bash
git clone https://github.com/MaelitoP/lumen && cd lumen
cargo build --workspace
```

## macOS

Tantivy's `zstd-sys` links against libiconv. Point the linker at Homebrew's copy,
otherwise the build fails with `ld: library not found for -liconv`:

```bash
brew install libiconv
export LIBRARY_PATH="$(brew --prefix libiconv)/lib"
cargo build --workspace
```

## Docker

`Dockerfile.dev` and `docker-compose.yml` provide a Linux build container if you'd
rather not set up libiconv locally.

## Running

```bash
cargo run -p lumen-api -- --data-dir ./data --bind 127.0.0.1:7700
```

Flags (all optional; environment variable in parentheses):

- `--data-dir` (`LUMEN_DATA_DIR`, default `data`) — where collections and the WAL live.
- `--bind` (`LUMEN_BIND`, default `127.0.0.1:7700`) — listen address.
- `--checkpoint-interval-secs` (`LUMEN_CHECKPOINT_INTERVAL_SECS`, default `30`) —
  how often buffered writes are committed and the WAL is trimmed (see the
  durability section in API.md).

## Testing

```bash
cargo test --workspace      # unit + integration tests (incl. WAL crash recovery)
bash scripts/smoke.sh       # end-to-end HTTP smoke test against a live server
```

The `justfile` wraps the common checks: `just build`, `just test`, `just fmt`,
`just clippy`.
