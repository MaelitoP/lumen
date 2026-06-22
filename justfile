set shell := ["zsh", "-c"]

docker_service := "dev"

default:
    @just --list

build:
    @cargo build --workspace

test:
    @cargo test --workspace

fmt:
    @cargo fmt --all --check

clippy:
    @cargo clippy --all-targets --all-features -- -D warnings

docker_dev:
    @docker compose run --rm {{docker_service}} bash

docker_build:
    @docker compose build --no-cache
