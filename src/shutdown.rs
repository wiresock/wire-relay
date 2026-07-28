// SPDX-License-Identifier: AGPL-3.0-or-later

//! Cross-platform shutdown signal handling.

use std::io;

/// Wait for an interactive interrupt or service-manager termination signal.
#[cfg(unix)]
pub async fn wait_for_signal() -> io::Result<&'static str> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            result?;
            Ok("SIGINT")
        }
        received = terminate.recv() => {
            received.ok_or_else(|| {
                io::Error::new(io::ErrorKind::BrokenPipe, "SIGTERM stream closed")
            })?;
            Ok("SIGTERM")
        }
    }
}

/// Wait for Ctrl-C on platforms without Unix signals.
#[cfg(not(unix))]
pub async fn wait_for_signal() -> io::Result<&'static str> {
    tokio::signal::ctrl_c().await?;
    Ok("interrupt")
}
