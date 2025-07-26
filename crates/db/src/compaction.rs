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
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, RwLock},
    time::SystemTime,
};

use rsketch_common::readable_size::ReadableSize;
use snafu::{ResultExt, ensure};
use tracing::{debug, info, warn};

use crate::{
    format::{Entry, InternalKey},
    sst::{
        err::{IOSnafu, InvalidSSTFileSnafu, Result},
        table::{TableBuilder, TableReader},
    },
};

/// Default compaction thresholds and parameters
pub(crate) const DEFAULT_L0_COMPACTION_TRIGGER: usize = 4; // Trigger when L0 has 4 files
pub(crate) const DEFAULT_L0_SLOW_DOWN_TRIGGER: usize = 8; // Slow down writes when L0 has 8 files
pub(crate) const DEFAULT_L0_STOP_TRIGGER: usize = 12; // Stop writes when L0 has 12 files
pub(crate) const DEFAULT_MAX_BYTES_FOR_LEVEL_BASE: ReadableSize = ReadableSize::mb(10); // 10MB for L1
pub(crate) const DEFAULT_LEVEL_SIZE_MULTIPLIER: u64 = 10; // Each level is 10x larger than previous
pub(crate) const DEFAULT_MAX_COMPACTION_BYTES: ReadableSize = ReadableSize::mb(25); // Max bytes per compaction
pub(crate) const DEFAULT_TARGET_FILE_SIZE_BASE: ReadableSize = ReadableSize::mb(2); // 2MB per file
pub(crate) const DEFAULT_MAX_LEVELS: usize = 7; // L0 through L6

/// Configuration for compaction behavior
#[derive(Debug, Clone)]
pub struct CompactionOptions {
    /// Number of L0 files that trigger compaction
    pub l0_compaction_trigger:    usize,
    /// Number of L0 files that slow down writes
    pub l0_slow_down_trigger:     usize,
    /// Number of L0 files that stop writes
    pub l0_stop_trigger:          usize,
    /// Target size for level 1 (base level)
    pub max_bytes_for_level_base: ReadableSize,
    /// Multiplier for level sizes (each level is this many times larger)
    pub level_size_multiplier:    u64,
    /// Maximum bytes to include in a single compaction
    pub max_compaction_bytes:     ReadableSize,
    /// Target size for each SSTable file
    pub target_file_size_base:    ReadableSize,
    /// Maximum number of levels
    pub max_levels:               usize,
}

impl Default for CompactionOptions {
    fn default() -> Self {
        Self {
            l0_compaction_trigger:    DEFAULT_L0_COMPACTION_TRIGGER,
            l0_slow_down_trigger:     DEFAULT_L0_SLOW_DOWN_TRIGGER,
            l0_stop_trigger:          DEFAULT_L0_STOP_TRIGGER,
            max_bytes_for_level_base: DEFAULT_MAX_BYTES_FOR_LEVEL_BASE,
            level_size_multiplier:    DEFAULT_LEVEL_SIZE_MULTIPLIER,
            max_compaction_bytes:     DEFAULT_MAX_COMPACTION_BYTES,
            target_file_size_base:    DEFAULT_TARGET_FILE_SIZE_BASE,
            max_levels:               DEFAULT_MAX_LEVELS,
        }
    }
}

/// Metadata about an SSTable file
#[derive(Debug, Clone)]
pub struct SstableMetadata {
    /// Unique file identifier
    pub file_number:   u64,
    /// File path
    pub file_path:     PathBuf,
    /// File size in bytes
    pub file_size:     u64,
    /// Smallest key in the file
    pub smallest_key:  Option<InternalKey>,
    /// Largest key in the file
    pub largest_key:   Option<InternalKey>,
    /// Level this file belongs to (0 = L0, 1 = L1, etc.)
    pub level:         usize,
    /// Number of entries in the file
    pub entry_count:   u32,
    /// When the file was created
    pub creation_time: SystemTime,
}

impl SstableMetadata {
    /// Creates metadata from a TableReader
    pub fn from_table_reader(
        file_number: u64,
        file_path: PathBuf,
        file_size: u64,
        level: usize,
        reader: &TableReader,
    ) -> Self {
        Self {
            file_number,
            file_path,
            file_size,
            smallest_key: reader.first_key().cloned(),
            largest_key: reader.last_key().cloned(),
            level,
            entry_count: reader.entry_count(),
            creation_time: SystemTime::now(),
        }
    }

    /// Checks if this file's key range overlaps with another file's range
    pub fn overlaps_with(&self, other: &SstableMetadata) -> bool {
        match (
            &self.smallest_key,
            &self.largest_key,
            &other.smallest_key,
            &other.largest_key,
        ) {
            (Some(self_min), Some(self_max), Some(other_min), Some(other_max)) => {
                // Files overlap if self_min <= other_max && other_min <= self_max
                self_min <= other_max && other_min <= self_max
            }
            _ => false, // If any file has no keys, no overlap
        }
    }

    /// Checks if a key falls within this file's range
    pub fn contains_key(&self, key: &InternalKey) -> bool {
        match (&self.smallest_key, &self.largest_key) {
            (Some(min_key), Some(max_key)) => key >= min_key && key <= max_key,
            _ => false,
        }
    }
}

/// Represents a compaction job - a set of files to compact together
#[derive(Debug, Clone)]
pub struct CompactionJob {
    /// Unique identifier for this compaction job
    pub job_id:         u64,
    /// Level being compacted from (source level)
    pub input_level:    usize,
    /// Level being compacted to (output level)
    pub output_level:   usize,
    /// Files from the input level
    pub input_files:    Vec<SstableMetadata>,
    /// Files from the output level that overlap (need to be included)
    pub output_files:   Vec<SstableMetadata>,
    /// Reason for this compaction
    pub reason:         CompactionReason,
    /// Estimated bytes that will be written (sum of all input files)
    pub estimated_size: u64,
}

/// Reasons why compaction was triggered
#[derive(Debug, Clone, PartialEq)]
pub enum CompactionReason {
    /// L0 has too many files
    L0TooManyFiles,
    /// A level has exceeded its size limit
    LevelSizeExceeded { level: usize },
    /// Manual compaction requested
    Manual,
    /// Memtable flush requires compaction space
    MemtableFlush,
}

impl CompactionJob {
    /// Creates a new compaction job
    pub fn new(
        job_id: u64,
        input_level: usize,
        output_level: usize,
        input_files: Vec<SstableMetadata>,
        output_files: Vec<SstableMetadata>,
        reason: CompactionReason,
    ) -> Self {
        let estimated_size = input_files.iter().map(|f| f.file_size).sum::<u64>()
            + output_files.iter().map(|f| f.file_size).sum::<u64>();

        Self {
            job_id,
            input_level,
            output_level,
            input_files,
            output_files,
            reason,
            estimated_size,
        }
    }

    /// Returns all input files (from both input and output levels)
    pub fn all_input_files(&self) -> impl Iterator<Item = &SstableMetadata> {
        self.input_files.iter().chain(self.output_files.iter())
    }

    /// Estimates the number of output files this compaction will produce
    pub fn estimated_output_files(&self, target_file_size: u64) -> usize {
        if target_file_size == 0 {
            return 1;
        }
        let output_count = self.estimated_size.div_ceil(target_file_size);
        std::cmp::max(1, output_count) as usize
    }
}

/// Manages the levels and decides when compaction is needed
#[derive(Debug)]
pub struct CompactionManager {
    /// Configuration options
    options:       CompactionOptions,
    /// Current file metadata organized by level
    levels:        RwLock<Vec<Vec<SstableMetadata>>>,
    /// Next file number to assign
    next_file_num: Mutex<u64>,
    /// Next job ID to assign
    next_job_id:   Mutex<u64>,
    /// Directory where SSTable files are stored
    sstable_dir:   PathBuf,
    /// Currently running compaction jobs (job_id -> job)
    running_jobs:  Mutex<HashMap<u64, CompactionJob>>,
}

impl CompactionManager {
    /// Creates a new compaction manager
    pub fn new<P: AsRef<Path>>(sstable_dir: P, options: CompactionOptions) -> Self {
        let mut levels = Vec::with_capacity(options.max_levels);
        for _ in 0..options.max_levels {
            levels.push(Vec::new());
        }

        Self {
            options,
            levels: RwLock::new(levels),
            next_file_num: Mutex::new(1),
            next_job_id: Mutex::new(1),
            sstable_dir: sstable_dir.as_ref().to_path_buf(),
            running_jobs: Mutex::new(HashMap::new()),
        }
    }

    /// Adds a new SSTable file to the appropriate level
    pub fn add_file(&self, mut file: SstableMetadata) -> Result<()> {
        ensure!(
            file.level < self.options.max_levels,
            InvalidSSTFileSnafu {
                reason: format!(
                    "Level {} exceeds max levels {}",
                    file.level, self.options.max_levels
                ),
                path:   file.file_path.to_string_lossy().to_string(),
            }
        );

        let mut levels = self
            .levels
            .write()
            .map_err(|_| std::io::Error::other("Failed to acquire write lock"))
            .context(IOSnafu)?;

        // Assign file number if not set
        if file.file_number == 0 {
            let mut next_num = self
                .next_file_num
                .lock()
                .map_err(|_| std::io::Error::other("Failed to acquire file number lock"))
                .context(IOSnafu)?;
            file.file_number = *next_num;
            *next_num += 1;
        }

        let file_level = file.level;
        let file_number = file.file_number;
        levels[file.level].push(file);

        // Sort L1+ files by smallest key for efficient range queries
        if file_level > 0 {
            levels[file_level].sort_by(|a, b| match (&a.smallest_key, &b.smallest_key) {
                (Some(a_key), Some(b_key)) => a_key.cmp(b_key),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            });
        }

        debug!("Added file {} to level {}", file_number, file_level);
        Ok(())
    }

    /// Removes files from their current levels (typically after successful
    /// compaction)
    pub fn remove_files(&self, files: &[SstableMetadata]) -> Result<()> {
        let mut levels = self
            .levels
            .write()
            .map_err(|_| std::io::Error::other("Failed to acquire write lock"))
            .context(IOSnafu)?;

        for file in files {
            if file.level < levels.len() {
                levels[file.level].retain(|f| f.file_number != file.file_number);
                debug!(
                    "Removed file {} from level {}",
                    file.file_number, file.level
                );
            }
        }

        Ok(())
    }

    /// Checks if compaction is needed and returns a compaction job if so
    pub fn pick_compaction(&self) -> Result<Option<CompactionJob>> {
        let levels = self
            .levels
            .read()
            .map_err(|_| std::io::Error::other("Failed to acquire read lock"))
            .context(IOSnafu)?;

        // Check if any jobs are already running for the same levels
        let running_jobs = self
            .running_jobs
            .lock()
            .map_err(|_| std::io::Error::other("Failed to acquire jobs lock"))
            .context(IOSnafu)?;

        // First priority: L0 compaction (special case due to overlapping files)
        if levels[0].len() >= self.options.l0_compaction_trigger {
            // Check if L0->L1 compaction is already running
            if !running_jobs
                .values()
                .any(|job| job.input_level == 0 && job.output_level == 1)
            {
                if let Some(job) = self.pick_l0_compaction(&levels)? {
                    return Ok(Some(job));
                }
            }
        }

        // Second priority: Level-based compaction for L1+
        for level in 1..levels.len() - 1 {
            if self.level_needs_compaction(level, &levels[level]) {
                // Check if this level compaction is already running
                if !running_jobs.values().any(|job| job.input_level == level) {
                    if let Some(job) = self.pick_level_compaction(level, &levels)? {
                        return Ok(Some(job));
                    }
                }
            }
        }

        Ok(None)
    }

    /// Picks files for L0 compaction (L0 -> L1)
    fn pick_l0_compaction(&self, levels: &[Vec<SstableMetadata>]) -> Result<Option<CompactionJob>> {
        if levels[0].is_empty() {
            return Ok(None);
        }

        // For L0, we need to compact all files since they can overlap
        // But limit by max_compaction_bytes
        let mut input_files = levels[0].clone();
        let mut total_size = 0u64;

        // Sort L0 files by file number (creation order) to maintain consistency
        input_files.sort_by_key(|f| f.file_number);

        // Limit input files by size
        input_files.retain(|f| {
            if total_size + f.file_size <= self.options.max_compaction_bytes.as_bytes() {
                total_size += f.file_size;
                true
            } else {
                false
            }
        });

        if input_files.is_empty() {
            return Ok(None);
        }

        // Find overlapping files in L1
        let output_files = self.find_overlapping_files(1, &input_files, &levels[1]);

        let job_id = self.next_job_id();
        let job = CompactionJob::new(
            job_id,
            0, // L0
            1, // L1
            input_files,
            output_files,
            CompactionReason::L0TooManyFiles,
        );

        info!(
            "Picked L0 compaction job {}: {} input files, {} output files, {} bytes",
            job_id,
            job.input_files.len(),
            job.output_files.len(),
            job.estimated_size
        );

        Ok(Some(job))
    }

    /// Picks files for level-based compaction (Ln -> Ln+1)
    fn pick_level_compaction(
        &self,
        level: usize,
        levels: &[Vec<SstableMetadata>],
    ) -> Result<Option<CompactionJob>> {
        if levels[level].is_empty() || level + 1 >= levels.len() {
            return Ok(None);
        }

        // For level compaction, pick the file that hasn't been compacted recently
        // For simplicity, pick the first file (in sorted order)
        let input_file = levels[level][0].clone();
        let input_files = vec![input_file];

        // Find overlapping files in the next level
        let output_files = self.find_overlapping_files(level + 1, &input_files, &levels[level + 1]);

        let job_id = self.next_job_id();
        let job = CompactionJob::new(
            job_id,
            level,
            level + 1,
            input_files,
            output_files,
            CompactionReason::LevelSizeExceeded { level },
        );

        info!(
            "Picked L{} compaction job {}: {} input files, {} output files, {} bytes",
            level,
            job_id,
            job.input_files.len(),
            job.output_files.len(),
            job.estimated_size
        );

        Ok(Some(job))
    }

    /// Finds files in the target level that overlap with input files
    fn find_overlapping_files(
        &self,
        target_level: usize,
        input_files: &[SstableMetadata],
        level_files: &[SstableMetadata],
    ) -> Vec<SstableMetadata> {
        if input_files.is_empty() {
            return Vec::new();
        }

        // Find the overall key range of input files
        let (min_key, max_key): (Option<InternalKey>, Option<InternalKey>) =
            input_files.iter().fold((None, None), |(min, max), file| {
                let new_min = match (&min, &file.smallest_key) {
                    (None, Some(key)) => Some(key.clone()),
                    (Some(min_key), Some(key)) => Some(std::cmp::min(min_key.clone(), key.clone())),
                    (min, None) => min.clone(),
                };
                let new_max = match (&max, &file.largest_key) {
                    (None, Some(key)) => Some(key.clone()),
                    (Some(max_key), Some(key)) => Some(std::cmp::max(max_key.clone(), key.clone())),
                    (max, None) => max.clone(),
                };
                (new_min, new_max)
            });

        // Find overlapping files in the target level
        level_files
            .iter()
            .filter(|file| {
                match (&min_key, &max_key, &file.smallest_key, &file.largest_key) {
                    (Some(input_min), Some(input_max), Some(file_min), Some(file_max)) => {
                        // Files overlap if input_min <= file_max && file_min <= input_max
                        input_min <= file_max && file_min <= input_max
                    }
                    _ => false,
                }
            })
            .cloned()
            .collect()
    }

    /// Checks if a level needs compaction based on size
    fn level_needs_compaction(&self, level: usize, files: &[SstableMetadata]) -> bool {
        if level == 0 {
            return files.len() >= self.options.l0_compaction_trigger;
        }

        let total_size: u64 = files.iter().map(|f| f.file_size).sum();
        let level_limit = self.get_level_size_limit(level);

        total_size > level_limit
    }

    /// Calculates the size limit for a given level
    fn get_level_size_limit(&self, level: usize) -> u64 {
        if level == 0 {
            return u64::MAX; // L0 is limited by file count, not size
        }

        if level == 1 {
            return self.options.max_bytes_for_level_base.as_bytes();
        }

        // Each level is multiplier times larger than the previous
        let base = self.options.max_bytes_for_level_base.as_bytes();
        let multiplier = self.options.level_size_multiplier;

        // Calculate multiplier^(level-1) * base
        base * multiplier.pow((level - 1) as u32)
    }

    /// Executes a compaction job
    pub fn compact(&self, job: CompactionJob) -> Result<Vec<SstableMetadata>> {
        info!(
            "Starting compaction job {}: L{} -> L{}",
            job.job_id, job.input_level, job.output_level
        );

        // Mark job as running
        {
            let mut running_jobs = self
                .running_jobs
                .lock()
                .map_err(|_| std::io::Error::other("Failed to acquire jobs lock"))
                .context(IOSnafu)?;
            running_jobs.insert(job.job_id, job.clone());
        }

        let result = self.execute_compaction(&job);

        // Remove job from running jobs
        {
            let mut running_jobs = self
                .running_jobs
                .lock()
                .map_err(|_| std::io::Error::other("Failed to acquire jobs lock"))
                .context(IOSnafu)?;
            running_jobs.remove(&job.job_id);
        }

        match result {
            Ok(output_files) => {
                info!(
                    "Completed compaction job {}: produced {} output files",
                    job.job_id,
                    output_files.len()
                );
                Ok(output_files)
            }
            Err(e) => {
                warn!("Failed compaction job {}: {:?}", job.job_id, e);
                Err(e)
            }
        }
    }

    /// Actually performs the compaction by merging files
    fn execute_compaction(&self, job: &CompactionJob) -> Result<Vec<SstableMetadata>> {
        // Collect all entries from input files
        let mut all_entries = Vec::new();

        // Read from all input files
        for file in job.all_input_files() {
            let mut reader = TableReader::open(&file.file_path)?;
            let entries = reader.iter()?;
            all_entries.extend(entries);
        }

        // Sort entries by key (merge sort since individual files are already sorted)
        all_entries.sort_by(|a, b| a.key.cmp(&b.key));

        // Remove duplicates and tombstones (keep latest version based on sequence
        // number)
        let merged_entries = self.merge_entries(all_entries);

        if merged_entries.is_empty() {
            return Ok(Vec::new());
        }

        // Split into output files based on target file size
        let output_files = self.create_output_files(&merged_entries, job.output_level)?;

        Ok(output_files)
    }

    /// Merges entries, removing duplicates and applying tombstones
    fn merge_entries(&self, mut entries: Vec<Entry>) -> Vec<Entry> {
        if entries.is_empty() {
            return entries;
        }

        // Sort by key first, then by sequence number (descending to get latest first)
        entries.sort_by(|a, b| {
            a.key
                .user_key
                .cmp(&b.key.user_key)
                .then_with(|| b.key.seqno.cmp(&a.key.seqno)) // Higher seqno first
        });

        let mut result = Vec::new();
        let mut current_key: Option<Vec<u8>> = None;

        for entry in entries {
            let entry_key = entry.key.user_key.key.as_ref();

            // If this is a new key, include the entry (it's the latest version)
            if current_key.as_ref().is_none_or(|k| k != entry_key) {
                current_key = Some(entry_key.to_vec());

                // Only include non-tombstone entries in the final output
                if !entry.key.is_tombstone() {
                    result.push(entry);
                }
                // For tombstones, we skip them (they delete the key)
            }
            // For duplicate keys, skip (we already have the latest version)
        }

        result
    }

    /// Creates output SSTable files from merged entries
    fn create_output_files(
        &self,
        entries: &[Entry],
        output_level: usize,
    ) -> Result<Vec<SstableMetadata>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        let target_size = self.options.target_file_size_base.as_bytes();
        let mut output_files = Vec::new();
        let mut current_entries = Vec::new();
        let mut current_size = 0u64;

        for entry in entries {
            let entry_size = entry.size();

            // If adding this entry would exceed target size and we have entries, finish
            // current file
            if current_size + entry_size > target_size && !current_entries.is_empty() {
                let file = self.write_sstable_file(&current_entries, output_level)?;
                output_files.push(file);

                current_entries.clear();
                current_size = 0;
            }

            current_entries.push(entry.clone());
            current_size += entry_size;
        }

        // Write remaining entries
        if !current_entries.is_empty() {
            let file = self.write_sstable_file(&current_entries, output_level)?;
            output_files.push(file);
        }

        Ok(output_files)
    }

    /// Writes a single SSTable file with the given entries
    fn write_sstable_file(&self, entries: &[Entry], level: usize) -> Result<SstableMetadata> {
        if entries.is_empty() {
            return Err(std::io::Error::other("Cannot write empty SSTable file"))
                .context(IOSnafu)?;
        }

        let file_number = {
            let mut next_num = self
                .next_file_num
                .lock()
                .map_err(|_| std::io::Error::other("Failed to acquire file number lock"))
                .context(IOSnafu)?;
            let num = *next_num;
            *next_num += 1;
            num
        };

        let file_name = format!("{file_number:06}.sst");
        let file_path = self.sstable_dir.join(file_name);

        // Create table builder and write entries
        let mut builder = TableBuilder::open(&file_path)?;

        for entry in entries {
            // TableBuilder::add is now synchronous
            builder.add(entry.clone())?;
        }

        builder.finish()?;

        // Get file size
        let file_size = std::fs::metadata(&file_path).context(IOSnafu)?.len();

        // Create metadata
        let metadata = SstableMetadata {
            file_number,
            file_path,
            file_size,
            smallest_key: entries.first().map(|e| e.key.clone()),
            largest_key: entries.last().map(|e| e.key.clone()),
            level,
            entry_count: entries.len() as u32,
            creation_time: SystemTime::now(),
        };

        debug!(
            "Created SSTable file {} at level {} with {} entries",
            file_number,
            level,
            entries.len()
        );

        Ok(metadata)
    }

    /// Generates the next job ID
    fn next_job_id(&self) -> u64 {
        let mut next_id = self.next_job_id.lock().unwrap();
        let id = *next_id;
        *next_id += 1;
        id
    }

    /// Returns compaction statistics
    pub fn stats(&self) -> Result<CompactionStats> {
        let levels = self
            .levels
            .read()
            .map_err(|_| std::io::Error::other("Failed to acquire read lock"))
            .context(IOSnafu)?;

        let running_jobs = self
            .running_jobs
            .lock()
            .map_err(|_| std::io::Error::other("Failed to acquire jobs lock"))
            .context(IOSnafu)?;

        let mut level_stats = Vec::new();
        for (level, files) in levels.iter().enumerate() {
            let total_size: u64 = files.iter().map(|f| f.file_size).sum();
            let size_limit = self.get_level_size_limit(level);

            level_stats.push(LevelStats {
                level,
                file_count: files.len(),
                total_size,
                size_limit,
                needs_compaction: self.level_needs_compaction(level, files),
            });
        }

        Ok(CompactionStats {
            level_stats,
            running_jobs: running_jobs.len(),
        })
    }

    /// Returns files for a specific level (for SSTable search)
    pub fn get_level_files(&self, level: usize) -> Result<Vec<SstableMetadata>> {
        let levels = self
            .levels
            .read()
            .map_err(|_| std::io::Error::other("Failed to acquire read lock"))
            .context(IOSnafu)?;

        if level >= levels.len() {
            return Ok(Vec::new());
        }

        Ok(levels[level].clone())
    }

    /// Checks if writes should be slowed down due to L0 pressure
    pub fn should_slow_down_writes(&self) -> bool {
        if let Ok(levels) = self.levels.read() {
            levels[0].len() >= self.options.l0_slow_down_trigger
        } else {
            false
        }
    }

    /// Checks if writes should be stopped due to L0 pressure
    pub fn should_stop_writes(&self) -> bool {
        if let Ok(levels) = self.levels.read() {
            levels[0].len() >= self.options.l0_stop_trigger
        } else {
            false
        }
    }
}

/// Statistics about current compaction state
#[derive(Debug, Clone)]
pub struct CompactionStats {
    /// Statistics for each level
    pub level_stats:  Vec<LevelStats>,
    /// Number of currently running compaction jobs
    pub running_jobs: usize,
}

/// Statistics for a single level
#[derive(Debug, Clone)]
pub struct LevelStats {
    /// Level number (0 = L0, 1 = L1, etc.)
    pub level:            usize,
    /// Number of files in this level
    pub file_count:       usize,
    /// Total size of all files in bytes
    pub total_size:       u64,
    /// Size limit for this level
    pub size_limit:       u64,
    /// Whether this level needs compaction
    pub needs_compaction: bool,
}

/// Thread-safe reference to CompactionManager
pub type CompactionManagerRef = Arc<CompactionManager>;

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::format::{Entry, InternalKey, UserKey, ValueType};

    /// Helper function to create test entries
    fn create_test_entry(key: &str, value: &str, seqno: u64, timestamp: u64) -> Entry {
        let user_key = UserKey {
            key: key.as_bytes().into(),
            timestamp,
        };
        let internal_key = InternalKey {
            user_key,
            seqno,
            value_type: ValueType::Value,
        };
        Entry {
            key:   internal_key,
            value: value.as_bytes().into(),
        }
    }

    /// Helper function to create test SSTable metadata
    fn create_test_metadata(
        file_number: u64,
        level: usize,
        smallest_key: &str,
        largest_key: &str,
        file_size: u64,
    ) -> SstableMetadata {
        SstableMetadata {
            file_number,
            file_path: PathBuf::from(format!("test_{file_number:06}.sst")),
            file_size,
            smallest_key: Some(InternalKey {
                user_key:   UserKey {
                    key:       smallest_key.as_bytes().into(),
                    timestamp: 1000,
                },
                seqno:      1,
                value_type: ValueType::Value,
            }),
            largest_key: Some(InternalKey {
                user_key:   UserKey {
                    key:       largest_key.as_bytes().into(),
                    timestamp: 1000,
                },
                seqno:      1,
                value_type: ValueType::Value,
            }),
            level,
            entry_count: 100,
            creation_time: SystemTime::now(),
        }
    }

    #[test]
    fn test_compaction_options_default() {
        let options = CompactionOptions::default();
        assert_eq!(options.l0_compaction_trigger, DEFAULT_L0_COMPACTION_TRIGGER);
        assert_eq!(options.max_levels, DEFAULT_MAX_LEVELS);
    }

    #[test]
    fn test_sstable_metadata_overlaps() {
        let file1 = create_test_metadata(1, 1, "a", "d", 1000);
        let file2 = create_test_metadata(2, 1, "c", "f", 1000);
        let file3 = create_test_metadata(3, 1, "g", "j", 1000);

        assert!(file1.overlaps_with(&file2)); // a-d overlaps with c-f
        assert!(!file1.overlaps_with(&file3)); // a-d doesn't overlap with g-j
        assert!(!file2.overlaps_with(&file3)); // c-f doesn't overlap with g-j
    }

    #[test]
    fn test_compaction_manager_creation() {
        let temp_dir = TempDir::new().unwrap();
        let options = CompactionOptions::default();
        let manager = CompactionManager::new(temp_dir.path(), options);

        let stats = manager.stats().unwrap();
        assert_eq!(stats.level_stats.len(), DEFAULT_MAX_LEVELS);
        assert_eq!(stats.running_jobs, 0);

        for (i, level_stat) in stats.level_stats.iter().enumerate() {
            assert_eq!(level_stat.level, i);
            assert_eq!(level_stat.file_count, 0);
            assert_eq!(level_stat.total_size, 0);
            assert!(!level_stat.needs_compaction);
        }
    }

    #[test]
    fn test_add_files_to_levels() {
        let temp_dir = TempDir::new().unwrap();
        let manager = CompactionManager::new(temp_dir.path(), CompactionOptions::default());

        // Add files to different levels
        let file_l0 = create_test_metadata(1, 0, "a", "d", 1000);
        let file_l1 = create_test_metadata(2, 1, "e", "h", 2000);

        manager.add_file(file_l0).unwrap();
        manager.add_file(file_l1).unwrap();

        let stats = manager.stats().unwrap();
        assert_eq!(stats.level_stats[0].file_count, 1);
        assert_eq!(stats.level_stats[0].total_size, 1000);
        assert_eq!(stats.level_stats[1].file_count, 1);
        assert_eq!(stats.level_stats[1].total_size, 2000);
    }

    #[test]
    fn test_l0_compaction_trigger() {
        let temp_dir = TempDir::new().unwrap();
        let mut options = CompactionOptions::default();
        options.l0_compaction_trigger = 2; // Trigger at 2 files
        let manager = CompactionManager::new(temp_dir.path(), options);

        // Add one file - no compaction needed
        let file1 = create_test_metadata(1, 0, "a", "d", 1000);
        manager.add_file(file1).unwrap();
        assert!(manager.pick_compaction().unwrap().is_none());

        // Add second file - should trigger compaction
        let file2 = create_test_metadata(2, 0, "e", "h", 1000);
        manager.add_file(file2).unwrap();

        let compaction = manager.pick_compaction().unwrap();
        assert!(compaction.is_some());
        let job = compaction.unwrap();
        assert_eq!(job.input_level, 0);
        assert_eq!(job.output_level, 1);
        assert_eq!(job.input_files.len(), 2);
        assert_eq!(job.reason, CompactionReason::L0TooManyFiles);
    }

    #[test]
    fn test_level_size_limits() {
        let temp_dir = TempDir::new().unwrap();
        let manager = CompactionManager::new(temp_dir.path(), CompactionOptions::default());

        // Test level size calculation
        assert_eq!(manager.get_level_size_limit(0), u64::MAX); // L0 unlimited by size
        assert_eq!(
            manager.get_level_size_limit(1),
            DEFAULT_MAX_BYTES_FOR_LEVEL_BASE.as_bytes()
        );
        assert_eq!(
            manager.get_level_size_limit(2),
            DEFAULT_MAX_BYTES_FOR_LEVEL_BASE.as_bytes() * DEFAULT_LEVEL_SIZE_MULTIPLIER
        );
    }

    #[test]
    fn test_find_overlapping_files() {
        let temp_dir = TempDir::new().unwrap();
        let manager = CompactionManager::new(temp_dir.path(), CompactionOptions::default());

        let level_files = vec![
            create_test_metadata(1, 1, "a", "d", 1000),
            create_test_metadata(2, 1, "e", "h", 1000),
            create_test_metadata(3, 1, "i", "l", 1000),
        ];

        let input_files = vec![
            create_test_metadata(4, 0, "c", "f", 1000), // Overlaps with files 1 and 2
        ];

        let overlapping = manager.find_overlapping_files(1, &input_files, &level_files);

        assert_eq!(overlapping.len(), 2);
        assert!(overlapping.iter().any(|f| f.file_number == 1));
        assert!(overlapping.iter().any(|f| f.file_number == 2));
        assert!(!overlapping.iter().any(|f| f.file_number == 3));
    }

    #[test]
    fn test_merge_entries_deduplication() {
        let temp_dir = TempDir::new().unwrap();
        let manager = CompactionManager::new(temp_dir.path(), CompactionOptions::default());

        let entries = vec![
            create_test_entry("key1", "old_value", 1, 1000),
            create_test_entry("key1", "new_value", 2, 1000), // Newer version
            create_test_entry("key2", "value2", 1, 2000),
        ];

        let merged = manager.merge_entries(entries);

        assert_eq!(merged.len(), 2);
        // Should keep newer version of key1
        assert_eq!(merged[0].key.user_key.key.as_ref(), b"key1");
        assert_eq!(merged[0].value.as_ref(), b"new_value");
        assert_eq!(merged[0].key.seqno, 2);

        // Should keep key2
        assert_eq!(merged[1].key.user_key.key.as_ref(), b"key2");
        assert_eq!(merged[1].value.as_ref(), b"value2");
    }

    #[test]
    fn test_merge_entries_tombstones() {
        let temp_dir = TempDir::new().unwrap();
        let manager = CompactionManager::new(temp_dir.path(), CompactionOptions::default());

        let entries = vec![
            create_test_entry("key1", "value1", 1, 1000),
            Entry {
                key:   InternalKey {
                    user_key:   UserKey {
                        key:       "key1".as_bytes().into(),
                        timestamp: 1000,
                    },
                    seqno:      2,
                    value_type: ValueType::Tombstone,
                },
                value: "".as_bytes().into(),
            }, // Tombstone - should remove key1
            create_test_entry("key2", "value2", 1, 2000),
        ];

        let merged = manager.merge_entries(entries);

        // Should only have key2 (key1 was deleted by tombstone)
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].key.user_key.key.as_ref(), b"key2");
        assert_eq!(merged[0].value.as_ref(), b"value2");
    }

    #[test]
    fn test_write_protection_thresholds() {
        let temp_dir = TempDir::new().unwrap();
        let mut options = CompactionOptions::default();
        options.l0_slow_down_trigger = 2;
        options.l0_stop_trigger = 3;
        let manager = CompactionManager::new(temp_dir.path(), options);

        // Initially should not slow down or stop
        assert!(!manager.should_slow_down_writes());
        assert!(!manager.should_stop_writes());

        // Add files to reach slow down threshold
        manager
            .add_file(create_test_metadata(1, 0, "a", "d", 1000))
            .unwrap();
        assert!(!manager.should_slow_down_writes());

        manager
            .add_file(create_test_metadata(2, 0, "e", "h", 1000))
            .unwrap();
        assert!(manager.should_slow_down_writes());
        assert!(!manager.should_stop_writes());

        // Add one more to reach stop threshold
        manager
            .add_file(create_test_metadata(3, 0, "i", "l", 1000))
            .unwrap();
        assert!(manager.should_slow_down_writes());
        assert!(manager.should_stop_writes());
    }

    #[test]
    fn test_compaction_job_creation() {
        let input_files = vec![
            create_test_metadata(1, 0, "a", "d", 1000),
            create_test_metadata(2, 0, "e", "h", 1500),
        ];
        let output_files = vec![create_test_metadata(3, 1, "c", "f", 2000)];

        let job = CompactionJob::new(
            1,
            0,
            1,
            input_files.clone(),
            output_files.clone(),
            CompactionReason::L0TooManyFiles,
        );

        assert_eq!(job.job_id, 1);
        assert_eq!(job.input_level, 0);
        assert_eq!(job.output_level, 1);
        assert_eq!(job.estimated_size, 1000 + 1500 + 2000); // Sum of all input files
        assert_eq!(job.input_files.len(), 2);
        assert_eq!(job.output_files.len(), 1);

        // Test estimated output files
        assert_eq!(job.estimated_output_files(2000), 3); // 4500 bytes / 2000 = 3 files
        assert_eq!(job.estimated_output_files(5000), 1); // 4500 bytes / 5000 = 1 file
    }

    #[test]
    fn test_remove_files() {
        let temp_dir = TempDir::new().unwrap();
        let manager = CompactionManager::new(temp_dir.path(), CompactionOptions::default());

        // Add files
        let file1 = create_test_metadata(1, 0, "a", "d", 1000);
        let file2 = create_test_metadata(2, 0, "e", "h", 1000);
        let file3 = create_test_metadata(3, 1, "i", "l", 2000);

        manager.add_file(file1.clone()).unwrap();
        manager.add_file(file2.clone()).unwrap();
        manager.add_file(file3.clone()).unwrap();

        // Verify files were added
        let stats = manager.stats().unwrap();
        assert_eq!(stats.level_stats[0].file_count, 2);
        assert_eq!(stats.level_stats[1].file_count, 1);

        // Remove some files
        manager.remove_files(&[file1, file3]).unwrap();

        // Verify files were removed
        let stats = manager.stats().unwrap();
        assert_eq!(stats.level_stats[0].file_count, 1); // Only file2 remains
        assert_eq!(stats.level_stats[1].file_count, 0); // file3 was removed
    }
}
