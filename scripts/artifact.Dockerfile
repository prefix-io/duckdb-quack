# Clean-room artifact build: clones a pushed commit (never the working tree) so
# every published extension binary is traceable to exact source. Run via
# scripts/release.sh, which drives one build per platform with buildx.
#
# The base MUST NOT be newer than the oldest consuming image: mono's
# duckdb-warehouse and unified images are both Debian bookworm (glibc 2.36), and
# an extension built against a newer glibc fails to LOAD there (caught by the
# image builds' --validate step).

FROM debian:bookworm AS build
# clang, not g++: bookworm's gcc 12 rejects the duckdb unique_ptr
# derived-to-base conversions used throughout upstream quack code.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates clang cmake git libcurl4-openssl-dev libssl-dev make ninja-build python3 \
  && rm -rf /var/lib/apt/lists/*
ENV CC=clang CXX=clang++

ARG QUACK_REPO=https://github.com/prefix-io/duckdb-quack
ARG QUACK_COMMIT
RUN test -n "$QUACK_COMMIT" || (echo "QUACK_COMMIT build arg is required" && false)

RUN git clone ${QUACK_REPO} /src \
  && cd /src \
  && git checkout ${QUACK_COMMIT} \
  && git submodule update --init

WORKDIR /src
RUN GEN=ninja make release

# Provenance: record exactly what was built.
RUN cd /src \
  && echo "quack_commit=$(git rev-parse HEAD)" > /src/PROVENANCE \
  && echo "duckdb_commit=$(git -C duckdb rev-parse HEAD)" >> /src/PROVENANCE \
  && echo "duckdb_version=$(git -C duckdb describe --tags)" >> /src/PROVENANCE

FROM scratch AS artifact
COPY --from=build /src/build/release/extension/quack/quack.duckdb_extension /
COPY --from=build /src/PROVENANCE /
