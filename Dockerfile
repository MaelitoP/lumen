FROM rust:1.85-slim AS builder
WORKDIR /opt/lumen
COPY . .
RUN cargo build -p lumen-api --release

FROM debian:bookworm-slim AS runtime
RUN useradd --system --uid 10001 lumen \
    && mkdir -p /data \
    && chown lumen:lumen /data
COPY --from=builder /opt/lumen/target/release/lumen /usr/local/bin/lumen
USER lumen
EXPOSE 7700 8080
ENTRYPOINT ["lumen"]
