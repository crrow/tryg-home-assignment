# Project Documentation

## Introduction

This project is a high-performance, persistent key-value-timestamp store written in Rust. It's designed to store data on disk and provide fast access to values based on a key and a specific point in time. The store is built with a modular architecture, separating concerns into different components for clarity and maintainability.

## Core Concepts

The storage engine is based on a Log-Structured Merge-Tree (LSM-Tree), a data structure commonly used in high-throughput databases. Here are the key concepts:

### Key-Value-Timestamp Model

Instead of just storing key-value pairs, this database also associates a timestamp with each entry. This allows for powerful, time-based lookups. For example, you can retrieve the value of a key as it was at a specific moment in the past.

### Memtable

All incoming writes are first written to an in-memory data structure called the **Memtable**. The Memtable is optimized for fast writes and also serves recent reads. To ensure durability, all writes are also recorded in a **Write-Ahead Log (WAL)** before being acknowledged.

### SSTable (Sorted String Table)

When the Memtable reaches a certain size, it is flushed to disk as an **SSTable**. An SSTable is an immutable, sorted file containing key-value pairs. The sorted nature of SSTables allows for efficient lookups and scans.

### Compaction

Over time, many SSTables can accumulate on disk. **Compaction** is the process of merging these SSTables together to:

-   **Reduce read amplification:** Fewer files need to be checked to find a key.
-   **Reclaim space:** Old or deleted data is purged.
-   **Maintain performance:** The database remains efficient as data grows.

The compaction process runs in the background and combines smaller SSTables into larger, more efficient ones.

### Manifest

The **Manifest** file is a critical component that keeps track of all the SSTables in the database, their levels, and their key ranges. It acts as a catalog for the storage engine, allowing it to quickly find the right files for a given query.

## Architecture

The project is organized as a Rust workspace with several crates, each with a specific responsibility:

-   `crates/api`: Contains the Protocol Buffer (Protobuf) definitions for the gRPC API.
-   `crates/client`: A client library for interacting with the server via both gRPC and HTTP.
-   `crates/cmd`: The main entry point for the application, handling command-line arguments.
-   `crates/common`: Shared utilities and data structures used across the project.
-   `crates/db`: The core storage engine, implementing the LSM-Tree, SSTables, and compaction logic.
-   `crates/server`: The gRPC and HTTP servers that expose the database's functionality over the network.

## API

The database provides two primary APIs:

-   **gRPC API:** A high-performance, low-latency API defined using Protocol Buffers. This is the recommended way to interact with the database for performance-critical applications.
-   **HTTP/REST API:** A simple, JSON-based API for easy integration with web applications and other services.

Both APIs expose the core `PUT`, `GET`, and `DELETE` operations.

## How to Build and Run

### Build

To build the project, run the following command:

```bash
cargo build --release
```

### Run the Server

To start the database server, use the following command:

```bash
cargo run -- server --host 127.0.0.1 --port 50051 --http-port 3000 --db-path ./data
```

### Run the Client Example

An example client is provided to demonstrate basic usage:

```bash
cargo run --example basic_usage
```

## Testing

To run the full suite of tests, including unit and integration tests, use the following command:

```bash
just test
```
