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

use std::{path::PathBuf, sync::Arc};

use okaywal::LogManager;
use snafu::ResultExt;
use tracing::info;

use crate::{
    err::{IOSnafu, Result},
    mem::MemManagerRef,
};

#[derive(Debug, Clone)]
pub struct Options {
    path: PathBuf,
}

impl Options {
    /// Opens the database at the configured path.
    /// If a manifest file exists, attempts to recover; otherwise, creates a new
    /// database.
    pub fn open(self) -> Result<DB> {
        use crate::format::MANIFEST_FILE;
        info!("Opening database at {}", self.path.display());

        let manifest_path = self.path.join(MANIFEST_FILE);

        // try to recover from wal

        // Check if the manifest file exists to determine recovery or creation.
        let db = if manifest_path.try_exists().context(IOSnafu)? {
            self.recover()
        } else {
            self.create_new()
        }?;

        Ok(db)
    }

    /// Recovers the database from existing files.
    fn recover(self) -> Result<DB> { todo!() }

    /// Creates a new database at the specified path.
    fn create_new(self) -> Result<DB> { todo!() }
}

#[derive(Debug, Clone)]
pub struct DB(Arc<DBInner>);

#[derive(Debug)]
struct DBInner {
    mem_manager: MemManagerRef,
}
