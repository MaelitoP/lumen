set shell := ["zsh", "-c"]

default: build

build:
    cargo build --workspace

test:
    cargo test --workspace

fmt:
    cargo fmt --all --check

clippy:
    cargo clippy --all-targets --all-features -- -D warnings

bench:
    cargo run -p lumen-bench --release -- --generate 1000000

dev:
    docker compose run --rm dev bash
