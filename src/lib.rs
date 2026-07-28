// SPDX-License-Identifier: AGPL-3.0-or-later

//! WireRelay is a bounded, transparent UDP datagram relay.
//!
//! The crate deliberately has no knowledge of WireGuard, AmneziaWG, or any
//! other payload protocol. Datagram boundaries and payload bytes are preserved.

pub mod cli;
pub mod config;
pub mod control;
pub mod dns;
pub mod error;
pub mod limits;
pub mod logging;
pub mod metrics;
pub mod relay;
pub mod runtime;
pub mod shutdown;

/// WireRelay application version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Local control protocol version implemented by this release.
pub const CONTROL_PROTOCOL_VERSION: u16 = 2;
