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
