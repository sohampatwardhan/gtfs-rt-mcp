# Multi-arch image. Build with:
#   docker buildx build --platform linux/amd64,linux/arm64 -t gtfs-rt-mcp .
# A plain `docker build` produces the architecture of the machine it runs on,
# including linux/arm64 on a Raspberry Pi.

FROM rust:1.99-bookworm AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto
COPY src ./src
COPY tests/fixtures ./tests/fixtures
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 10001 --create-home gtfs
COPY --from=build /src/target/release/gtfs-rt-mcp /usr/local/bin/gtfs-rt-mcp
USER 10001
EXPOSE 8091
ENTRYPOINT ["gtfs-rt-mcp"]
CMD ["http", "--bind", "0.0.0.0:8091"]
