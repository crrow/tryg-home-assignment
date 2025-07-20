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

use clap::{Parser, Subcommand};
use snafu::Whatever;
use tracing::info;

mod build_info;
mod command_hello;
mod command_server;

#[derive(Debug, Parser)]
#[command(
    name = "rsketch",
    about = "Time series engine command line interface",
    author = build_info::AUTHOR,
    version = build_info::FULL_VERSION,
    propagate_version = true
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Print a hello message
    Hello(command_hello::HelloArgs),
    /// Run the time series server
    Server(command_server::ServerArgs),
}

#[tokio::main]
async fn main() -> Result<(), Whatever> {
    // Initialize logging
    // Start configuring a `fmt` subscriber
    let subscriber = tracing_subscriber::fmt()
    // Use a more compact, abbreviated log format
    .compact()
    // Display source code file paths
    .with_file(true)
    // Display source code line numbers
    .with_line_number(true)
    // Display the thread ID an event was recorded on
    .with_thread_ids(true)
    // Don't display the event's target (module path)
    .with_target(false)
    // Build the subscriber
    .finish();
    tracing::subscriber::set_global_default(subscriber).expect("Failed to set global default");

    // Parse command line arguments
    let cli = Cli::parse();

    info!("Starting rsketch version {}", build_info::FULL_VERSION);

    // Execute the selected command
    match cli.command {
        Commands::Hello(args) => {
            command_hello::run(args).await?;
        }
        Commands::Server(args) => {
            command_server::run(args).await?;
        }
    }
    Ok(())
}
