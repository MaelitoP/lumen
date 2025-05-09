# lumen
> *Lumen ✨—a fast, approachable search engine where “less is more.”*

## Quick start
```bash
git clone https://github.com/<org>/lumen && cd lumen
cargo build --workspace
```

### Run one-shot benchmark
> **macOS users:** Tantivy’s zstd dependency links to **libiconv**.  
> `brew install libiconv && export LIBICONV_LIB_DIR=$(brew --prefix libiconv)/lib` before `cargo build`.

```bash
cargo run -p lumen-bench -- --generate 1_000_000
```
