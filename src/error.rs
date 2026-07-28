// SPDX-License-Identifier: AGPL-3.0-or-later

//! Shared runtime error categories.

use std::{io, net::SocketAddr};

use thiserror::Error;

/// Errors returned by the relay runtime.
#[derive(Debug, Error)]
pub enum RuntimeError {
    /// A listener could not be bound.
    #[error("failed to bind UDP listener {name} at {bind}: {source}")]
    ListenerBind {
        /// Configured listener name.
        name: String,
        /// Configured bind address.
        bind: SocketAddr,
        /// Operating-system error.
        #[source]
        source: io::Error,
    },

    /// A metrics endpoint could not be bound.
    #[error("failed to bind metrics endpoint {bind}: {source}")]
    MetricsBind {
        /// Configured metrics address.
        bind: SocketAddr,
        /// Operating-system error.
        #[source]
        source: io::Error,
    },

    /// The configured control transport is unavailable.
    #[error("control transport error: {0}")]
    Control(String),

    /// A startup preflight worker failed unexpectedly.
    #[error("startup preflight failed: {0}")]
    Startup(String),

    /// A reload could not be committed.
    #[error("reload rejected: {0}")]
    Reload(String),

    /// Shutdown exceeded its configured timeout.
    #[error("graceful shutdown timed out after {0:?}")]
    ShutdownTimeout(std::time::Duration),

    /// The control server stopped but could not remove its owned socket path.
    #[error("control socket cleanup failed during shutdown: {0}")]
    ControlCleanup(String),
}
