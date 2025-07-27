# List available just recipes
@help:
    just -l

@fmt:
    cargo clippy --fix --allow-dirty --allow-staged -- -W unused-imports
    cargo +nightly fmt --all
    taplo format
    taplo format --check
    hawkeye format

# Calculate code
@cloc:
    cloc . --exclude-dir=vendor,docs,tests,examples,build,scripts,tools,target

@clean:
    cargo clean

@lint:
    cargo clippy --all --tests --all-features --no-deps

# Example
@example-hello:
    cargo run --example hello-world

# Binary
@run:
    cargo run --package binary hello

alias c := check
@check:
    cargo check --all --all-features --all-targets

alias t := test
@test:
    cargo nextest run --verbose

@build-cli:
    # Build the rsketch-cmd package
    cargo build --package rsketch-cmd

@run-cli:
    cargo run --package rsketch-cmd server

# Docker commands
@docker-build:
    # Build Docker image with BuildKit optimizations and proxy support
    cd docker && DOCKER_BUILDKIT=1 docker build \
    
      --build-arg NO_PROXY=localhost,127.0.0.1 \
      -f Dockerfile -t rsketch-server ..

@docker-build-no-proxy:
    # Build Docker image without proxy (for users without proxy)
    cd docker && DOCKER_BUILDKIT=1 docker build -f Dockerfile -t rsketch-server ..

@docker-run:
    # Run the Docker container
    docker run -d \
      --name rsketch-server \
      -p 50051:50051 \
      -p 3000:3000 \
      -v rsketch_data:/app/data \
      rsketch-server

@docker-stop:
    # Stop and remove the Docker container
    docker stop rsketch-server || true
    docker rm rsketch-server || true

@docker-logs:
    # View Docker container logs
    docker logs -f rsketch-server

# Docker Compose commands
@docker-up:
    # Start services with docker-compose
    cd docker && DOCKER_BUILDKIT=1 docker-compose up --build

@docker-up-bg:
    # Start services in background with docker-compose
    cd docker && DOCKER_BUILDKIT=1 docker-compose up -d --build

@docker-down:
    # Stop docker-compose services
    cd docker && docker-compose down

@docker-clean:
    # Clean up Docker resources
    docker system prune -f
    docker volume prune -f

alias d-build := docker-build
alias d-up := docker-up
alias d-down := docker-down
alias d-logs := docker-logs
