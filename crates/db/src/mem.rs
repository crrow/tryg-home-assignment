// Copyright 2025 Crrow
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    path::Path,
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use byteorder::ReadBytesExt;
use fusio::Read;
use okaywal::WriteAheadLog;
use rsketch_common::readable_size::ReadableSize;

use crate::{
    batch::WriteBatch,
    format::{Codec, InternalValue},
    memtable::{MemTable, immut::ImmutableMemTables},
};

pub(crate) const WAL_DIR: &str = "wal";

/// Default memtable size limit in bytes (4MB)
pub(crate) const DEFAULT_MEMTABLE_SIZE_LIMIT: ReadableSize = ReadableSize::mb(4);

/// MemManager holds the memtable and immutable memtables.
/// Uses RwLock for better concurrent read performance and atomic size tracking.
#[derive(Debug)]
pub(crate) struct MemManager {
    /// Current active memtable - protected by RwLock for concurrent reads
    memtable:            RwLock<MemTable>,
    /// Immutable memtables - already thread-safe internally
    immutables:          ImmutableMemTables,
    /// Size limit for memtable rotation
    memtable_size_limit: ReadableSize,
    /// Current memtable size - atomic for lockless access
    current_size:        AtomicU64,
    /// Unique ID generator for memtables
    next_memtable_id:    AtomicUsize,
}

impl MemManager {
    /// Creates a new MemManager with default settings
    pub(crate) fn new() -> Self {
        Self {
            memtable:            RwLock::new(MemTable::new()),
            immutables:          ImmutableMemTables::new(),
            memtable_size_limit: DEFAULT_MEMTABLE_SIZE_LIMIT,
            current_size:        AtomicU64::new(0),
            next_memtable_id:    AtomicUsize::new(1),
        }
    }

    /// Returns the current memtable size in bytes (lockless)
    pub(crate) fn current_size(&self) -> u64 { self.current_size.load(Ordering::Acquire) }

    /// Returns the memtable size limit
    pub(crate) fn size_limit(&self) -> u64 { self.memtable_size_limit.as_bytes() }

    /// Checks if the current memtable should be rotated (lockless)
    fn should_rotate(&self) -> bool { self.current_size() >= self.memtable_size_limit.as_bytes() }

    /// Rotates the current memtable to immutable and creates a new active
    /// memtable This operation requires exclusive access to the memtable
    fn rotate_memtable(&self) -> std::io::Result<()> {
        let mut memtable_guard = self
            .memtable
            .write()
            .map_err(|_| std::io::Error::other("Failed to acquire write lock"))?;

        // Replace with new memtable
        let old_memtable = std::mem::replace(&mut *memtable_guard, MemTable::new());

        // Generate unique ID and move to immutables
        let memtable_id = self.next_memtable_id.fetch_add(1, Ordering::AcqRel) as u64;
        self.immutables.add(memtable_id, Arc::new(old_memtable));

        // Reset size counter
        self.current_size.store(0, Ordering::Release);

        Ok(())
    }

    fn apply_internal_values(&self, internal_values: &[InternalValue]) -> std::io::Result<bool> {
        let memtable_guard = self
            .memtable
            .write()
            .map_err(|_| std::io::Error::other("Failed to acquire write lock"))?;

        let mut total_size = 0;
        for internal_value in internal_values {
            memtable_guard.insert(internal_value.clone());
            total_size += internal_value.size();
        }

        // Drop the read lock before potentially acquiring write lock for rotation
        drop(memtable_guard);

        // Update size atomically
        let new_size = self.current_size.fetch_add(total_size, Ordering::AcqRel) + total_size;
        // Check if we need to rotate after applying the batch
        let rotated = if new_size >= self.memtable_size_limit.as_bytes() {
            self.rotate_memtable()?;
            true
        } else {
            false
        };
        Ok(rotated)
    }

    /// Applies a WriteBatch to the memtable with concurrent safety
    fn apply_batch(&self, batch: &WriteBatch) -> std::io::Result<bool> {
        // Get read lock for applying batch
        let memtable_guard = self
            .memtable
            .read()
            .map_err(|_| std::io::Error::other("Failed to acquire read lock"))?;

        // Calculate batch size
        let batch_size = (batch.approximate_size() - batch.header_size()) as u64;

        // Apply all operations in the batch
        for internal_value in batch.iter() {
            memtable_guard.insert(internal_value.clone());
        }

        // Update size atomically
        let new_size = self.current_size.fetch_add(batch_size, Ordering::AcqRel) + batch_size;

        // Drop the read lock before potentially acquiring write lock for rotation
        drop(memtable_guard);

        // Check if we need to rotate after applying the batch
        let rotated = if new_size >= self.memtable_size_limit.as_bytes() {
            self.rotate_memtable()?;
            true
        } else {
            false
        };

        Ok(rotated)
    }

    /// Writes a WriteBatch to the WAL
    fn write_batch(&self, wal: &WriteAheadLog, batch: &WriteBatch) -> std::io::Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        // Begin a new WAL entry
        let mut writer = wal.begin_entry()?;

        // Write batch header
        let header = batch.encode_header()?;
        writer.write_chunk(&header)?;

        // Write each internal value as a chunk
        for internal_value in batch.iter() {
            let size = internal_value.size();
            let mut chunk_writer = writer.begin_chunk(size as u32)?;
            internal_value.encode_into(&mut chunk_writer)?;
            chunk_writer.finish()?;
        }

        // Commit the entry
        writer.commit()?;
        Ok(())
    }

    /// Writes and applies a batch atomically
    /// Returns true if memtable was rotated during this operation
    pub(crate) fn write_and_apply_batch(
        &self,
        wal: &WriteAheadLog,
        batch: &WriteBatch,
    ) -> std::io::Result<bool> {
        // First write to WAL for durability
        self.write_batch(wal, batch)?;

        // Then apply to memtable (may trigger rotation)
        self.apply_batch(batch)
    }

    /// Forces a memtable rotation regardless of size
    pub(crate) fn force_rotate(&self) -> std::io::Result<()> { self.rotate_memtable() }

    /// Returns the total number of immutable memtables
    pub(crate) fn immutable_count(&self) -> usize { self.immutables.len() }

    /// Returns true if there are any immutable memtables
    pub(crate) fn has_immutables(&self) -> bool { !self.immutables.is_empty() }

    /// Performs a read operation on the memtable with minimal locking
    pub(crate) fn get(&self, key: &[u8], timestamp: u64, seqno: u64) -> Option<InternalValue> {
        // First check the active memtable
        {
            let memtable = self.memtable.read().ok()?;
            if let Some(value) = memtable.get(key, timestamp, seqno) {
                return Some(value);
            }
        }

        // Then check immutable memtables (newest first)
        let immutables = self.immutables.read_tables();
        for (_, memtable) in immutables.iter().rev() {
            if let Some(value) = memtable.get(key, timestamp, seqno) {
                return Some(value);
            }
        }

        None
    }

    /// Returns statistics about the MemManager
    pub(crate) fn stats(&self) -> MemManagerStats {
        MemManagerStats {
            current_size:    self.current_size(),
            size_limit:      self.size_limit(),
            immutable_count: self.immutable_count(),
            should_rotate:   self.should_rotate(),
        }
    }
}

/// Statistics about MemManager state
#[derive(Debug, Clone)]
pub(crate) struct MemManagerStats {
    pub current_size:    u64,
    pub size_limit:      u64,
    pub immutable_count: usize,
    pub should_rotate:   bool,
}

/// Thread-safe reference to MemManager
pub(crate) type MemManagerRef = Arc<MemManager>;

/// Opens and recovers a WAL from the given path, returning the WAL instance.
/// The MemManager is updated with recovered data during the process.
pub(crate) fn open_wal<P: AsRef<Path>>(
    path: P,
    mm: MemManagerRef,
) -> std::io::Result<WriteAheadLog> {
    let lm = LogManager(mm);
    let path = path.as_ref();
    let wal_path = path.join(WAL_DIR);

    // Create WAL directory if it doesn't exist
    std::fs::create_dir_all(&wal_path)?;

    let wal = WriteAheadLog::recover(wal_path, lm).map_err(std::io::Error::other)?;

    Ok(wal)
}

/// LogManager wraps a MemManagerRef and implements okaywal::LogManager
/// for WAL recovery operations.
#[derive(Debug)]
struct LogManager(MemManagerRef);

impl okaywal::LogManager for LogManager {
    /// Recovers a single entry from the WAL by deserializing and inserting
    /// it into the appropriate memtable
    fn recover(&mut self, entry: &mut okaywal::Entry<'_>) -> std::io::Result<()> {
        // Read the first chunk which should be the batch header
        let header_data = match entry.read_chunk().map_err(std::io::Error::other)? {
            okaywal::ReadChunkResult::Chunk(mut chunk) => {
                use std::io::Read;
                let mut chunk_data = Vec::new();
                chunk.read_to_end(&mut chunk_data)?;
                chunk_data
            }
            okaywal::ReadChunkResult::EndOfEntry => {
                // Empty entry, nothing to recover
                return Ok(());
            }
            okaywal::ReadChunkResult::AbortedEntry => {
                // Entry was aborted, skip it
                return Ok(());
            }
        };

        // Parse the batch header to get sequence number and count
        if header_data.len() < 12 {
            // Invalid header size, skip this entry
            return Ok(());
        }

        let mut cursor = std::io::Cursor::new(&header_data);
        let _ = cursor.read_u64::<byteorder::LittleEndian>()?;
        let count = cursor.read_u32::<byteorder::LittleEndian>()?;

        if count == 0 {
            // Empty batch, nothing to recover
            return Ok(());
        }

        // Read the remaining chunks as InternalValues
        let mut internal_values = Vec::with_capacity(count as usize);

        for _ in 0..count {
            match entry.read_chunk().map_err(std::io::Error::other)? {
                okaywal::ReadChunkResult::Chunk(mut chunk) => {
                    use std::io::Read;
                    let mut chunk_data = Vec::new();
                    chunk.read_to_end(&mut chunk_data)?;

                    // Decode the InternalValue from this chunk
                    let mut value_cursor = std::io::Cursor::new(&chunk_data);
                    if let Ok(internal_value) = InternalValue::decode_from(&mut value_cursor) {
                        internal_values.push(internal_value);
                    } else {
                        // Failed to decode InternalValue, skip this chunk
                        tracing::warn!(
                            "Failed to decode InternalValue from WAL chunk during recovery"
                        );
                        continue;
                    }
                }
                okaywal::ReadChunkResult::EndOfEntry => {
                    // Unexpected end of entry, we expected more chunks
                    tracing::warn!(
                        "Unexpected end of WAL entry during recovery, expected {} chunks but got \
                         {}",
                        count,
                        internal_values.len()
                    );
                    break;
                }
                okaywal::ReadChunkResult::AbortedEntry => {
                    // Entry was aborted, skip remaining processing
                    return Ok(());
                }
            }
        }

        // Verify we read all expected chunks
        if internal_values.len() != count as usize {
            tracing::warn!(
                "WAL entry recovery mismatch: expected {} values, got {}",
                count,
                internal_values.len()
            );
        }

        // If we have any valid InternalValues, apply them to the memtable
        if !internal_values.is_empty() {
            self.0.apply_internal_values(&internal_values)?;
        }

        Ok(())
    }

    /// Handles checkpointing of WAL segments to persistent storage.
    /// This is called when the WAL needs to checkpoint data to allow
    /// segment files to be reclaimed.
    fn checkpoint_to(
        &mut self,
        _last_checkpointed_id: okaywal::EntryId,
        checkpointed_entries: &mut okaywal::SegmentReader,
        _wal: &okaywal::WriteAheadLog,
    ) -> std::io::Result<()> {
        todo!()
    }

    /// Determines whether a segment should be recovered.
    /// We recover all segments by default.
    fn should_recover_segment(
        &mut self,
        _segment: &okaywal::RecoveredSegment,
    ) -> std::io::Result<okaywal::Recovery> {
        Ok(okaywal::Recovery::Recover)
    }
}

#[cfg(test)]
mod tests {
    use test_case::test_case;

    use super::*;

    fn create_test_batch(sequence: u64, entries: &[(&str, &str, u64)]) -> WriteBatch {
        let mut batch = WriteBatch::new(sequence);
        for (key, value, timestamp) in entries.iter() {
            batch.put(key.as_bytes(), value.as_bytes(), *timestamp);
        }
        batch
    }

    fn create_test_batch_with_size(
        sequence: u64,
        key: &str,
        value_size: usize,
        timestamp: u64,
    ) -> WriteBatch {
        let mut batch = WriteBatch::new(sequence);
        let value = vec![b'x'; value_size]; // Create buffer with specified size
        batch.put(key.as_bytes(), &value, timestamp);
        batch
    }

    fn create_mem_manager_with_limit(limit: u64) -> MemManager {
        MemManager {
            memtable:            RwLock::new(MemTable::new()),
            immutables:          ImmutableMemTables::new(),
            memtable_size_limit: ReadableSize(limit),
            current_size:        AtomicU64::new(0),
            next_memtable_id:    AtomicUsize::new(1),
        }
    }

    #[test]
    fn test_memmanager_creation() {
        let mem_manager = MemManager::new();

        assert_eq!(mem_manager.current_size(), 0);
        assert_eq!(
            mem_manager.size_limit(),
            DEFAULT_MEMTABLE_SIZE_LIMIT.as_bytes()
        );
        assert_eq!(mem_manager.immutable_count(), 0);
        assert!(!mem_manager.has_immutables());
        assert!(!mem_manager.should_rotate());
    }

    /// Test size-based rotation with different size limits and data sizes
    #[test_case(50, vec![("key1", "very_long_value_that_exceeds_limit", 1000)], true; "single large entry exceeds limit")]
    #[test_case(100, vec![("key1", "medium_value", 1000), ("key2", "another_medium_value", 2000)], true; "multiple entries exceed limit")]
    #[test_case(1000, vec![("key1", "small", 1000)], false; "small entry under limit")]
    #[test_case(50, vec![("key1", "value1", 1000), ("key2", "value2", 2000), ("key3", "value3", 3000)], true; "multiple small entries with very low limit")]
    fn test_size_based_rotation(
        size_limit: u64,
        entries: Vec<(&str, &str, u64)>,
        should_rotate: bool,
    ) {
        let mem_manager = create_mem_manager_with_limit(size_limit);
        let batch = create_test_batch(1, &entries);

        // Debug: Print actual batch size for failed cases
        let actual_size = (batch.approximate_size() - batch.header_size()) as u64;
        if (actual_size > size_limit) != should_rotate {
            println!(
                "Debug: size_limit={size_limit}, actual_size={actual_size}, \
                 should_rotate={should_rotate}"
            );
        }

        let rotated = mem_manager.apply_batch(&batch).unwrap();

        assert_eq!(
            rotated,
            should_rotate,
            "Expected rotation: {}, got: {}, with limit: {} and {} entries (actual size: {})",
            should_rotate,
            rotated,
            size_limit,
            entries.len(),
            actual_size
        );

        if should_rotate {
            assert!(
                mem_manager.immutable_count() > 0,
                "Should have immutable memtables after rotation"
            );
            assert!(
                mem_manager.has_immutables(),
                "Should report having immutables"
            );
        } else {
            assert_eq!(
                mem_manager.immutable_count(),
                0,
                "Should not have immutable memtables"
            );
            assert!(
                !mem_manager.has_immutables(),
                "Should not report having immutables"
            );
        }
    }

    /// Test multiple consecutive rotations
    ///
    /// Batch size calculation with specified value sizes:
    /// - InternalKey: ~40 bytes (UserKey + timestamp + seqno + value_type)
    /// - UserValue: specified size (e.g., 30 bytes)
    /// - MemTable overhead: ~10-15 bytes (skiplist node overhead)
    /// - Total per entry: ~80-85 bytes with 30-byte values
    #[test_case(50, 3, 30, 3; "small limit with 30-byte values triggers multiple rotations")]
    #[test_case(100, 5, 30, 2; "medium limit with 30-byte values triggers some rotations")] // ~85 bytes per batch, rotation every 2nd batch
    #[test_case(300, 2, 30, 0; "large limit with 30-byte values triggers no rotation")]
    fn test_multiple_rotations_with_size(
        size_limit: u64,
        num_batches: usize,
        value_size: usize,
        min_expected_rotations: usize,
    ) {
        let mem_manager = create_mem_manager_with_limit(size_limit);

        for i in 0..num_batches {
            let batch = create_test_batch_with_size(
                i as u64 * 10,
                &format!("key{i}"),
                value_size,
                i as u64 * 1000 + 1000,
            );

            mem_manager.apply_batch(&batch).unwrap();
        }

        let final_count = mem_manager.immutable_count();

        assert!(
            final_count >= min_expected_rotations,
            "Expected at least {min_expected_rotations} rotations, got {final_count} (value size: \
             {value_size} bytes, limit: {size_limit} bytes)"
        );
    }

    /// Test size-based rotation with different size limits and data sizes
    #[test_case(60, 10, false; "small limit with 10-byte value under limit")] // 51 bytes < 60
    #[test_case(80, 50, true; "medium limit with 50-byte value exceeds limit")] // 91 bytes > 80
    #[test_case(200, 100, false; "large limit with 100-byte value under limit")] // 141 bytes < 200
    fn test_size_based_rotation_with_size(size_limit: u64, value_size: usize, should_rotate: bool) {
        let mem_manager = create_mem_manager_with_limit(size_limit);
        let batch = create_test_batch_with_size(1, "test_key", value_size, 1000);

        let rotated = mem_manager.apply_batch(&batch).unwrap();

        assert_eq!(
            rotated,
            should_rotate,
            "Expected rotation: {should_rotate}, got: {rotated}, with limit: {size_limit} bytes \
             and value size: {value_size} bytes (actual batch size: ~{} bytes)",
            match value_size {
                10 => 51,
                30 => 71,
                50 => 91,
                100 => 141,
                _ => value_size + 41, // rough estimate: value_size + overhead
            }
        );

        if should_rotate {
            assert!(
                mem_manager.immutable_count() > 0,
                "Should have immutable memtables after rotation"
            );
            assert!(
                mem_manager.has_immutables(),
                "Should report having immutables"
            );
        } else {
            assert_eq!(
                mem_manager.immutable_count(),
                0,
                "Should not have immutable memtables"
            );
            assert!(
                !mem_manager.has_immutables(),
                "Should not report having immutables"
            );
        }
    }

    /// Test that force rotation works regardless of size
    #[test_case(0; "empty memtable")]
    #[test_case(1; "memtable with one entry")]
    #[test_case(3; "memtable with multiple entries")]
    fn test_force_rotation(num_entries: usize) {
        let mem_manager = MemManager::new();

        // Add data based on test case
        for i in 0..num_entries {
            let batch =
                create_test_batch(i as u64, &[(&format!("key{i}"), "value", 1000 + i as u64)]);
            mem_manager.apply_batch(&batch).unwrap();
        }

        let initial_immutable_count = mem_manager.immutable_count();
        let initial_size = mem_manager.current_size();

        // Force rotation
        mem_manager.force_rotate().unwrap();

        // Should always create an immutable memtable and reset size
        assert_eq!(mem_manager.immutable_count(), initial_immutable_count + 1);
        assert!(mem_manager.has_immutables());
        assert_eq!(mem_manager.current_size(), 0);
    }

    /// Test get operations across different memtable states
    #[test_case(vec![("key1", "value1", 1000)], vec![("key2", "value2", 2000)], "key1", Some("value1"); "find in immutable after rotation")]
    #[test_case(vec![("key1", "value1", 1000)], vec![("key2", "value2", 2000)], "key2", Some("value2"); "find in active after rotation")]
    #[test_case(vec![("key1", "value1", 1000)], vec![("key2", "value2", 2000)], "key3", None; "key not found")]
    #[test_case(vec![("key1", "old_value", 1000)], vec![("key1", "new_value", 2000)], "key1", Some("new_value"); "newer value in active overrides immutable")]
    fn test_get_across_memtables(
        first_batch_entries: Vec<(&str, &str, u64)>,
        second_batch_entries: Vec<(&str, &str, u64)>,
        search_key: &str,
        expected_value: Option<&str>,
    ) {
        let mem_manager = create_mem_manager_with_limit(80);

        // Add first batch (will likely trigger rotation)
        let batch1 = create_test_batch(1, &first_batch_entries);
        mem_manager.apply_batch(&batch1).unwrap();

        // Add second batch (after potential rotation)
        let batch2 = create_test_batch(2, &second_batch_entries);
        mem_manager.apply_batch(&batch2).unwrap();

        // Search for the key
        let result = mem_manager.get(search_key.as_bytes(), u64::MAX, u64::MAX);

        match expected_value {
            Some(expected) => {
                assert!(result.is_some(), "Should find key: {search_key}");
                let found_value = result.unwrap();
                assert_eq!(
                    found_value.value.as_ref(),
                    expected.as_bytes(),
                    "Value mismatch for key: {search_key}"
                );
            }
            None => {
                assert!(result.is_none(), "Should not find key: {search_key}");
            }
        }
    }

    /// Test stats reporting at different states
    #[test_case(0, 0, 0, false; "initial empty state")]
    #[test_case(1, 0, 1, false; "after adding data, no rotation")]
    #[test_case(1, 1, 0, false; "after force rotation")]
    fn test_stats_consistency(
        initial_batches: usize,
        force_rotations: usize,
        expected_min_size: u64,
        expected_should_rotate: bool,
    ) {
        let mem_manager = MemManager::new();

        // Add initial batches
        for i in 0..initial_batches {
            let batch = create_test_batch(i as u64, &[(&format!("key{i}"), "value", 1000)]);
            mem_manager.apply_batch(&batch).unwrap();
        }

        // Perform force rotations
        for _ in 0..force_rotations {
            mem_manager.force_rotate().unwrap();
        }

        let stats = mem_manager.stats();

        if initial_batches > 0 && force_rotations == 0 {
            assert!(
                stats.current_size >= expected_min_size,
                "Current size {} should be at least {}",
                stats.current_size,
                expected_min_size
            );
        } else if force_rotations > 0 {
            assert_eq!(stats.current_size, 0, "Size should be 0 after rotation");
            assert_eq!(
                stats.immutable_count, force_rotations,
                "Should have {force_rotations} immutable memtables"
            );
        }

        assert_eq!(stats.should_rotate, expected_should_rotate);
    }

    /// Test concurrent operations with different thread counts
    #[test_case(2, 5; "2 threads, 5 operations each")]
    #[test_case(4, 10; "4 threads, 10 operations each")]
    #[test_case(8, 3; "8 threads, 3 operations each")]
    fn test_concurrent_operations(num_threads: usize, ops_per_thread: usize) {
        use std::{sync::Arc, thread};

        let mem_manager = Arc::new(MemManager::new());
        let mut handles = vec![];

        // Spawn threads
        for thread_id in 0..num_threads {
            let mm = Arc::clone(&mem_manager);
            let handle = thread::spawn(move || {
                for i in 0..ops_per_thread {
                    let batch = create_test_batch(
                        (thread_id * 100 + i) as u64,
                        &[(&format!("key_{thread_id}_{i}"), "value", 1000 + i as u64)],
                    );
                    mm.apply_batch(&batch).unwrap();
                }
            });
            handles.push(handle);
        }

        // Wait for completion
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify consistency
        let stats = mem_manager.stats();
        let total_expected_ops = num_threads * ops_per_thread;

        // Should have some data either in active or immutable memtables
        assert!(
            stats.current_size > 0 || stats.immutable_count > 0,
            "Should have data after {total_expected_ops} total operations"
        );

        // Should be able to find at least some inserted keys
        let mut found_keys = 0;
        for thread_id in 0..num_threads.min(2) {
            // Check first 2 threads to avoid slow test
            let result = mem_manager.get(format!("key_{thread_id}_0").as_bytes(), 1000, u64::MAX);
            if result.is_some() {
                found_keys += 1;
            }
        }

        assert!(found_keys > 0, "Should find at least some inserted keys");
    }

    /// Test apply_internal_values with different value sizes and counts
    #[test_case(vec![("key1", "small_value", 1000, 1)], false; "single small value")]
    #[test_case(vec![("key1", "value1", 1000, 1), ("key2", "value2", 2000, 2)], false; "multiple small values")]
    #[test_case(vec![("key1", "very_long_value_that_might_trigger_rotation_if_limit_is_small", 1000, 1)], false; "single large value with default limit")]
    fn test_apply_internal_values(
        values_spec: Vec<(&str, &str, u64, u64)>,
        expected_rotation_with_default_limit: bool,
    ) {
        let mem_manager = MemManager::new();

        let internal_values: Vec<InternalValue> = values_spec
            .into_iter()
            .map(|(key, value, timestamp, seqno)| InternalValue::make(key, timestamp, seqno, value))
            .collect();

        let initial_size = mem_manager.current_size();
        let rotated = mem_manager.apply_internal_values(&internal_values).unwrap();

        assert_eq!(rotated, expected_rotation_with_default_limit);
        assert!(
            mem_manager.current_size() >= initial_size,
            "Size should increase or stay same"
        );

        // Verify values were inserted
        if !internal_values.is_empty() {
            let first_key = &internal_values[0].key.user_key.key;
            let result = mem_manager.get(first_key, u64::MAX, u64::MAX);
            assert!(result.is_some(), "Should find inserted value");
        }
    }

    #[test]
    fn test_size_tracking_accuracy() {
        let mem_manager = MemManager::new();

        let initial_size = mem_manager.current_size();
        assert_eq!(initial_size, 0);

        // Apply a batch and track size manually
        let batch = create_test_batch(1, &[("test_key", "test_value", 1000)]);
        let expected_size_increase = (batch.approximate_size() - batch.header_size()) as u64;

        mem_manager.apply_batch(&batch).unwrap();

        let new_size = mem_manager.current_size();
        assert!(new_size > initial_size, "Size should have increased");

        // The actual size might differ slightly due to internal overhead
        // but should be in the right ballpark
        assert!(
            new_size >= expected_size_increase / 2,
            "Size increase {new_size} should be at least half of expected {expected_size_increase}"
        );
    }

    /// Test WAL writing functionality
    #[test_case(vec![("key1", "value1", 1000)]; "single entry batch")]
    #[test_case(vec![("key1", "value1", 1000), ("key2", "value2", 2000)]; "multiple entry batch")]
    #[test_case(vec![("key1", "value1", 1000), ("key1", "value2", 2000)]; "same key different timestamps")]
    fn test_wal_write_batch(entries: Vec<(&str, &str, u64)>) {
        use std::sync::Arc;

        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let mem_manager = Arc::new(MemManager::new());

        // Open WAL
        let wal = open_wal(temp_dir.path(), mem_manager.clone()).unwrap();

        // Create and write batch
        let batch = create_test_batch(1, &entries);
        let initial_size = mem_manager.current_size();

        // Write batch to WAL and apply to memtable
        let rotated = mem_manager.write_and_apply_batch(&wal, &batch).unwrap();

        // Verify data was applied to memtable
        assert!(
            mem_manager.current_size() > initial_size,
            "Memtable size should increase"
        );

        // Verify we can read the data back
        for (key, expected_value, timestamp) in &entries {
            let result = mem_manager.get(key.as_bytes(), *timestamp, u64::MAX);
            assert!(result.is_some(), "Should find key: {key}");
            assert_eq!(result.unwrap().value.as_ref(), expected_value.as_bytes());
        }

        // WAL should have been written (implicit test - no errors means success)
        assert!(
            !rotated || mem_manager.immutable_count() > 0,
            "Rotation state should be consistent"
        );
    }

    /// Test WAL recovery functionality
    #[test]
    fn test_wal_recovery() {
        use std::sync::Arc;

        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();

        // Phase 1: Write data to WAL
        let test_data = vec![
            ("sensor1", "temp=25.5", 1000),
            ("sensor2", "humidity=60", 2000),
            ("sensor1", "temp=26.0", 3000),
        ];

        {
            let mem_manager = Arc::new(MemManager::new());
            let wal = open_wal(temp_dir.path(), mem_manager.clone()).unwrap();

            // Write multiple batches
            for (i, (key, value, timestamp)) in test_data.iter().enumerate() {
                let batch = create_test_batch(i as u64 + 1, &[(key, value, *timestamp)]);
                mem_manager.write_and_apply_batch(&wal, &batch).unwrap();
            }

            // Verify data is in memtable
            assert!(
                mem_manager.current_size() > 0,
                "Should have data in memtable"
            );
        } // MemManager and WAL are dropped here

        // Phase 2: Recover from WAL
        {
            let mem_manager_recovered = Arc::new(MemManager::new());
            assert_eq!(
                mem_manager_recovered.current_size(),
                0,
                "New memmanager should be empty"
            );

            // Open WAL again - this should trigger recovery
            let _wal_recovered = open_wal(temp_dir.path(), mem_manager_recovered.clone()).unwrap();

            // Verify all data was recovered
            assert!(
                mem_manager_recovered.current_size() > 0,
                "Should have recovered data"
            );

            // Verify each key-value pair was recovered correctly
            for (key, expected_value, timestamp) in &test_data {
                let result = mem_manager_recovered.get(key.as_bytes(), *timestamp, u64::MAX);
                assert!(result.is_some(), "Should find recovered key: {key}");
                let recovered_value = result.unwrap();
                assert_eq!(
                    recovered_value.value.as_ref(),
                    expected_value.as_bytes(),
                    "Recovered value should match original for key: {key}"
                );
                assert_eq!(
                    recovered_value.key.user_key.timestamp, *timestamp,
                    "Recovered timestamp should match for key: {key}"
                );
            }
        }
    }

    /// Test WAL recovery with rotation during recovery
    #[test]
    fn test_wal_recovery_with_rotation() {
        use std::sync::Arc;

        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();

        // Write a lot of data that should trigger rotation during recovery
        let large_data: Vec<_> = (0..10)
            .map(|i| {
                (
                    format!("key{i}"),
                    format!("large_value_to_trigger_rotation_{i}"),
                    1000 + i,
                )
            })
            .collect();

        // Phase 1: Write data
        {
            let mem_manager = Arc::new(MemManager::new());
            let wal = open_wal(temp_dir.path(), mem_manager.clone()).unwrap();

            for (i, (key, value, timestamp)) in large_data.iter().enumerate() {
                let batch = create_test_batch(i as u64 + 1, &[(key, value, *timestamp)]);
                mem_manager.write_and_apply_batch(&wal, &batch).unwrap();
            }
        }

        // Phase 2: Recover with small memtable limit to force rotation
        {
            let mem_manager_recovered = Arc::new(MemManager {
                memtable:            RwLock::new(MemTable::new()),
                immutables:          ImmutableMemTables::new(),
                memtable_size_limit: ReadableSize::kb(100), // Small limit to force rotation
                current_size:        AtomicU64::new(0),
                next_memtable_id:    AtomicUsize::new(1),
            });

            let _wal_recovered = open_wal(temp_dir.path(), mem_manager_recovered.clone()).unwrap();

            // Should have data in both active and immutable memtables
            assert!(
                mem_manager_recovered.current_size() > 0
                    || mem_manager_recovered.immutable_count() > 0,
                "Should have data after recovery"
            );

            // Should be able to find all data despite rotations
            for (key, expected_value, timestamp) in &large_data {
                let result = mem_manager_recovered.get(key.as_bytes(), *timestamp, u64::MAX);
                assert!(
                    result.is_some(),
                    "Should find key after recovery with rotation: {key}"
                );
                assert_eq!(result.unwrap().value.as_ref(), expected_value.as_bytes());
            }
        }
    }

    /// Test WAL with empty batches and edge cases
    #[test]
    fn test_wal_edge_cases() {
        use std::sync::Arc;

        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let mem_manager = Arc::new(MemManager::new());
        let wal = open_wal(temp_dir.path(), mem_manager.clone()).unwrap();

        // Test empty batch
        let empty_batch = WriteBatch::new(1);
        assert!(empty_batch.is_empty());

        // Writing empty batch should not fail
        let result = mem_manager.write_and_apply_batch(&wal, &empty_batch);
        assert!(result.is_ok(), "Empty batch should be handled gracefully");
        assert_eq!(
            mem_manager.current_size(),
            0,
            "Empty batch should not change size"
        );

        // Test batch with tombstone (delete)
        let mut delete_batch = WriteBatch::new(2);
        delete_batch.delete(b"deleted_key", 1000);

        let result = mem_manager.write_and_apply_batch(&wal, &delete_batch);
        assert!(result.is_ok(), "Delete batch should work");
        assert!(
            mem_manager.current_size() > 0,
            "Tombstone should take space"
        );
    }

    /// Test WAL recovery ordering and sequence numbers
    #[test]
    fn test_wal_recovery_ordering() {
        use std::sync::Arc;

        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();

        // Write data with specific sequence numbers
        {
            let mem_manager = Arc::new(MemManager::new());
            let wal = open_wal(temp_dir.path(), mem_manager.clone()).unwrap();

            // Write batches with ascending sequence numbers
            for seq in [10, 20, 30, 40, 50] {
                let batch = create_test_batch(
                    seq,
                    &[(&format!("key_seq_{seq}"), &format!("value_{seq}"), 1000)],
                );
                mem_manager.write_and_apply_batch(&wal, &batch).unwrap();
            }
        }

        // Recover and verify sequence is preserved
        {
            let mem_manager_recovered = Arc::new(MemManager::new());
            let _wal_recovered = open_wal(temp_dir.path(), mem_manager_recovered.clone()).unwrap();

            // All keys should be recoverable
            for seq in [10, 20, 30, 40, 50] {
                let key = format!("key_seq_{seq}");
                let result = mem_manager_recovered.get(key.as_bytes(), 1000, u64::MAX);
                assert!(result.is_some(), "Should recover key: {key}");

                let recovered = result.unwrap();
                assert_eq!(
                    recovered.key.seqno, seq,
                    "Sequence number should be preserved"
                );
                assert_eq!(recovered.value.as_ref(), format!("value_{seq}").as_bytes());
            }
        }
    }

    /// Test concurrent WAL operations
    #[test]
    fn test_wal_concurrent_operations() {
        use std::{sync::Arc, thread};

        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let mem_manager = Arc::new(MemManager::new());
        let wal = Arc::new(open_wal(temp_dir.path(), mem_manager.clone()).unwrap());

        let num_threads = 4;
        let ops_per_thread = 5;
        let mut handles = vec![];

        // Spawn multiple threads writing to WAL concurrently
        for thread_id in 0..num_threads {
            let mm = Arc::clone(&mem_manager);
            let wal_ref = Arc::clone(&wal);

            let handle = thread::spawn(move || {
                for i in 0..ops_per_thread {
                    let batch = create_test_batch(
                        thread_id * 100 + i,
                        &[(
                            &format!("thread_{thread_id}_key_{i}"),
                            "concurrent_value",
                            1000 + i,
                        )],
                    );
                    mm.write_and_apply_batch(&wal_ref, &batch).unwrap();
                }
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify all data was written correctly
        let total_expected = num_threads * ops_per_thread;
        assert!(
            mem_manager.current_size() > 0,
            "Should have data from concurrent writes"
        );

        // Check that we can find data from all threads
        let mut found_keys = 0;
        for thread_id in 0..num_threads {
            for i in 0..ops_per_thread {
                let key = format!("thread_{thread_id}_key_{i}");
                if mem_manager
                    .get(key.as_bytes(), 1000 + i, u64::MAX)
                    .is_some()
                {
                    found_keys += 1;
                }
            }
        }

        assert_eq!(
            found_keys, total_expected,
            "Should find all {total_expected} keys from concurrent operations"
        );
    }

    #[test]
    fn debug_batch_sizes_with_specified_sizes() {
        // Debug test to understand actual sizes with specified value sizes

        let small_value_batch = create_test_batch_with_size(1, "test_key", 10, 1000);
        println!(
            "10-byte value batch size: {}",
            (small_value_batch.approximate_size() - small_value_batch.header_size())
        );

        let medium_value_batch = create_test_batch_with_size(1, "test_key", 30, 1000);
        println!(
            "30-byte value batch size: {}",
            (medium_value_batch.approximate_size() - medium_value_batch.header_size())
        );

        let large_value_batch = create_test_batch_with_size(1, "test_key", 50, 1000);
        println!(
            "50-byte value batch size: {}",
            (large_value_batch.approximate_size() - large_value_batch.header_size())
        );

        let very_large_value_batch = create_test_batch_with_size(1, "test_key", 100, 1000);
        println!(
            "100-byte value batch size: {}",
            (very_large_value_batch.approximate_size() - very_large_value_batch.header_size())
        );
    }
}
