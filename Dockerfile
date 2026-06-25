# syntax=docker/dockerfile:1

ARG CARGO_PROFILE=docker

FROM rust:1.89-bookworm AS chef
ARG SCCACHE_VERSION=0.16.0
RUN apt-get update \
    && apt-get install -y --no-install-recommends mold clang curl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && cargo install cargo-chef --locked \
    && curl -fsSL \
        "https://github.com/mozilla/sccache/releases/download/v${SCCACHE_VERSION}/sccache-v${SCCACHE_VERSION}-x86_64-unknown-linux-musl.tar.gz" \
        | tar xz -C /usr/local/bin --strip-components=1 "sccache-v${SCCACHE_VERSION}-x86_64-unknown-linux-musl/sccache"
WORKDIR /app

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY .cargo ./.cargo
COPY src ./src
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
ARG CARGO_PROFILE=docker

ENV RUSTC_WRAPPER=sccache \
    SCCACHE_DIR=/sccache \
    SCCACHE_CACHE_SIZE=10G

COPY --from=planner /app/recipe.json recipe.json
COPY .cargo ./.cargo

# Cache dependency compilation separately from application source changes.
RUN --mount=type=cache,target=/usr/local/cargo/registry,id=quest-router-cargo-registry \
    --mount=type=cache,target=/usr/local/cargo/git,id=quest-router-cargo-git \
    --mount=type=cache,target=/sccache,id=quest-router-sccache \
    --mount=type=cache,target=/app/target,id=quest-router-target-${CARGO_PROFILE} \
    cargo chef cook --profile "${CARGO_PROFILE}" --recipe-path recipe.json

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN --mount=type=cache,target=/usr/local/cargo/registry,id=quest-router-cargo-registry \
    --mount=type=cache,target=/usr/local/cargo/git,id=quest-router-cargo-git \
    --mount=type=cache,target=/sccache,id=quest-router-sccache \
    --mount=type=cache,target=/app/target,id=quest-router-target-${CARGO_PROFILE} \
    cargo build --profile "${CARGO_PROFILE}" \
    && install -Dm755 "/app/target/${CARGO_PROFILE}/quest-router" /tmp/quest-router

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /tmp/quest-router /usr/local/bin/quest-router
ENTRYPOINT ["quest-router"]
CMD ["--config", "/config/quest-router.toml"]
