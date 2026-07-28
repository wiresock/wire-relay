// SPDX-License-Identifier: AGPL-3.0-or-later

//! Versioned local daemon control plane.

pub mod client;
pub mod protocol;
pub mod server;

pub use client::ControlClient;
pub use protocol::{
    ControlError, ControlErrorCode, ControlRequest, ControlResponse, RequestEnvelope,
    ResponseEnvelope,
};
