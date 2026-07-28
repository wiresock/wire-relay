// SPDX-License-Identifier: AGPL-3.0-or-later

//! Process-wide tracing initialization and reloadable level filtering.

use std::sync::OnceLock;

use tracing_subscriber::{
    EnvFilter, Registry, layer::SubscriberExt as _, reload, util::SubscriberInitExt as _,
};

use crate::config::LogLevel;

static FILTER_HANDLE: OnceLock<reload::Handle<EnvFilter, Registry>> = OnceLock::new();

/// Initializes stderr tracing. `RUST_LOG` takes precedence at startup.
pub fn init(level: LogLevel) -> Result<(), String> {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level.as_str()));
    let (filter_layer, handle) = reload::Layer::new(filter);
    tracing_subscriber::registry()
        .with(filter_layer)
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(false)
                .with_writer(std::io::stderr),
        )
        .try_init()
        .map_err(|error| format!("failed to initialize logging: {error}"))?;
    FILTER_HANDLE
        .set(handle)
        .map_err(|_| "logging reload handle was already initialized".to_owned())
}

/// Applies a configuration log level during transactional reload. Embedded
/// runtimes that did not initialize the process subscriber have nothing to do.
pub fn reload_level(level: LogLevel) -> Result<(), String> {
    if let Some(handle) = FILTER_HANDLE.get() {
        handle
            .reload(EnvFilter::new(level.as_str()))
            .map_err(|error| format!("failed to reload logging filter: {error}"))?;
    }
    Ok(())
}
