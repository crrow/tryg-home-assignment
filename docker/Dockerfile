# Base stage with common dependencies
FROM rust:1.88-bookworm AS base

# Configure proxy if provided via build args
ARG HTTP_PROXY
ARG HTTPS_PROXY
ARG NO_PROXY
ENV HTTP_PROXY=${HTTP_PROXY}
ENV HTTPS_PROXY=${HTTPS_PROXY}
ENV NO_PROXY=${NO_PROXY}

# Install protocol buffers compiler and build tools
RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Install cargo-chef and sccache for build optimization
RUN cargo install cargo-chef --version ^0.1
RUN cargo install sccache --version ^0.8

# Configure sccache
ENV RUSTC_WRAPPER=sccache
ENV SCCACHE_DIR=/sccache

# Planner stage - generates the recipe for dependencies
FROM base AS planner
WORKDIR /app
COPY . .
# Generate dependency recipe with cache mounts
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=$SCCACHE_DIR,sharing=locked \
    cargo chef prepare --recipe-path recipe.json

# Builder stage - compiles dependencies and application
FROM base AS builder
WORKDIR /app

# Copy the recipe from planner stage
COPY --from=planner /app/recipe.json recipe.json

# Build dependencies with cache mounts
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=$SCCACHE_DIR,sharing=locked \
    cargo chef cook --release --recipe-path recipe.json

# Copy source code
COPY . .

# Build the application with cache mounts
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=$SCCACHE_DIR,sharing=locked \
    cargo build --release --package rsketch-cmd

# Runtime stage
FROM debian:bookworm-slim

# Configure proxy if provided via build args
ARG HTTP_PROXY
ARG HTTPS_PROXY
ARG NO_PROXY
ENV HTTP_PROXY=${HTTP_PROXY}
ENV HTTPS_PROXY=${HTTPS_PROXY}
ENV NO_PROXY=${NO_PROXY}

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user
RUN useradd -m -u 1001 rsketch

# Set working directory
WORKDIR /app

# Copy the binary from builder stage
COPY --from=builder /app/target/release/rsketch-cmd /usr/local/bin/rsketch

# Create data directory and set ownership
RUN mkdir -p /app/data && chown -R rsketch:rsketch /app

# Switch to non-root user
USER rsketch

# Expose default ports
EXPOSE 50051 3000

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:3000/health || exit 1

# Default command to start the server
CMD ["rsketch", "server", "--host", "0.0.0.0", "--db-path", "/app/data"] 