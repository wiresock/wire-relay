// SPDX-License-Identifier: AGPL-3.0-or-later

use std::process::ExitCode;

use clap::Parser;
use wire_relay::cli::{Cli, execute};

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match execute(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("wire-relay: {error:#}");
            ExitCode::FAILURE
        }
    }
}
