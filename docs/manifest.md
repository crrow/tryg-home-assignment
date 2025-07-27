# The Manifest File

## Overview

The `MANIFEST` file is a critical component of the database's architecture. It acts as a transactional log that records all changes to the state of the SSTable files. Its primary purpose is to ensure the database can be reliably recovered to a consistent state after a restart.

In an LSM-Tree, the set of SSTable files and their corresponding levels change over time due to compaction. The `MANIFEST` file is the source of truth that tracks these changes.

## Role in the Database

The `MANIFEST` file serves several key functions:

1.  **Durability:** It provides a durable record of the database's structure. By logging changes to the file set, the database can be restored to its exact state, even after a crash.
2.  **Consistency:** It ensures that the view of the database is always consistent. When a compaction finishes, the `MANIFEST` is updated atomically to reflect the removal of old files and the addition of new ones.
3.  **Recovery:** On startup, the database reads the `MANIFEST` file to rebuild its understanding of which SSTables exist, which levels they belong to, and what their key ranges are. This is much faster and more reliable than scanning the entire database directory.

## How It Works

The `MANIFEST` file is implemented as an append-only log of `ManifestEdit` records. This design ensures that updates are fast and crash-safe. Each edit represents a single change to the database's file structure.

There are two primary types of edits:

-   `AddFile`: This record is written when a new SSTable is created, typically after a memtable is flushed or a compaction is completed. It contains all the necessary metadata about the new file, including:
    -   `file_number`: A unique identifier for the SSTable.
    -   `level`: The compaction level the file belongs to.
    -   `file_size`: The size of the file in bytes.
    -   `smallest_key` and `largest_key`: The key range covered by the file.

-   `RemoveFile`: This record is written when an SSTable is no longer needed, usually after it has been compacted into a new, larger file. It simply contains the `file_number` and `level` of the file to be removed.

### The Process

1.  **Initial State:** When a database is created, a new `MANIFEST` file is created.
2.  **Memtable Flush:** When a memtable is flushed to an SSTable, an `AddFile` edit is appended to the `MANIFEST`.
3.  **Compaction:** When a compaction runs:
    -   First, `AddFile` edits are written for all the new SSTables created by the compaction.
    -   Then, `RemoveFile` edits are written for all the input SSTables that were merged.
    This order ensures that if a crash occurs during the process, the database can recover without losing data.
4.  **Startup/Recovery:** When the database starts, it reads the `MANIFEST` file from beginning to end, replaying each edit in order. This process reconstructs the complete state of the file levels in memory, allowing the database to operate correctly.

### Format

The `MANIFEST` file is a binary file for efficiency and to avoid parsing complexities. Each `ManifestEdit` is serialized and appended to the file. This append-only nature makes writes very fast and reduces the risk of corruption.

### Fallback Mechanism

In the rare event that the `MANIFEST` file is missing or corrupted, the database has a fallback mechanism. It will scan the SSTable directory on the filesystem, read the metadata from each SSTable file, and rebuild the manifest from scratch. While this process is slower, it provides an extra layer of resilience.
