# local-e2e: the build stage of deploy/release/Containerfile.* as an image of
# its own (the same Rust image, toolchain and packages; libclang-dev, curl and
# ca-certificates are the fabric build's extras). build-binaries.sh runs cargo
# in it with the target and registry bind-mounted from $E2E_HOME, so the four
# images' binaries share one target (the release build uses one podman cache
# mount per image) and nothing lands in the container store.
ARG RUST_IMAGE=docker.io/library/rust:1.97.1-slim-trixie
FROM ${RUST_IMAGE}
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      protobuf-compiler libprotobuf-dev pkg-config libssl-dev clang libclang-dev g++ make cmake perl git curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*
ENV RUSTUP_TOOLCHAIN=1.97.1 \
    CARGO_TERM_COLOR=never
WORKDIR /src
