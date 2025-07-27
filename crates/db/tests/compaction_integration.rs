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

use tempfile::TempDir;
use tryg_db::{CompactionOptions, DB};

#[test]
fn test_compaction_and_manifest_integration() {
    let temp_dir = TempDir::new().unwrap();

    // Use custom compaction options to trigger compaction quickly
    let compaction_options = CompactionOptions {
        l0_compaction_trigger: 2, // Trigger compaction when L0 has 2 files
        ..Default::default()
    };

    let db = DB::options(temp_dir.path())
        .with_compaction_options(compaction_options)
        .open()
        .unwrap();

    // --- Phase 1: Write enough data to trigger multiple memtable flushes ---

    // Write data to create the first SSTable file
    db.put(b"key1", b"value1", 1000).unwrap();
    db.put(b"key2", b"value2", 2000).unwrap();
    db.flush().unwrap(); // Flushes memtable to an L0 SSTable

    // Write more data to create a second SSTable file
    db.put(b"key3", b"value3", 3000).unwrap();
    db.put(b"key4", b"value4", 4000).unwrap();
    db.flush().unwrap(); // Flushes memtable to a second L0 SSTable

    // At this point, we should have two L0 files, which should trigger compaction

    // --- Phase 2: Verify Compaction ---

    // Manually trigger compaction to ensure it runs for the test.
    db.compact().unwrap();
    let stats = db.stats().unwrap();

    // After compaction, L0 should be empty or have fewer files,
    // and L1 should now contain files.
    let l0_files = stats.compaction_stats.level_stats[0].file_count;
    let l1_files = stats.compaction_stats.level_stats[1].file_count;

    assert!(
        l1_files > 0,
        "L1 should have files after compaction. L0 files: {l0_files}, L1 files: {l1_files}"
    );

    // --- Phase 3: Verify Data Integrity After Compaction ---

    // All data should still be accessible
    assert_eq!(db.get(b"key1", 1000).unwrap(), Some(b"value1".to_vec()));
    assert_eq!(db.get(b"key2", 2000).unwrap(), Some(b"value2".to_vec()));
    assert_eq!(db.get(b"key3", 3000).unwrap(), Some(b"value3".to_vec()));
    assert_eq!(db.get(b"key4", 4000).unwrap(), Some(b"value4".to_vec()));

    // --- Phase 4: Test Manifest Recovery ---

    // Close the database
    db.close().unwrap();

    // Re-open the database, which will force recovery from the manifest
    let recovered_db = DB::options(temp_dir.path()).open().unwrap();

    // Verify that the data is still present after recovery
    assert_eq!(
        recovered_db.get(b"key1", 1000).unwrap(),
        Some(b"value1".to_vec())
    );
    assert_eq!(
        recovered_db.get(b"key4", 4000).unwrap(),
        Some(b"value4".to_vec())
    );

    // Check the stats of the recovered database
    let recovered_stats = recovered_db.stats().unwrap();
    let recovered_l1_files = recovered_stats.compaction_stats.level_stats[1].file_count;
    assert!(
        recovered_l1_files > 0,
        "Recovered database should have files in L1"
    );

    recovered_db.close().unwrap();
}
