// SPDX-License-Identifier: AGPL-3.0-or-later

//! Client for the local WireRelay daemon control socket.

use std::{path::PathBuf, time::Duration};

#[cfg(unix)]
use std::io;

#[cfg(all(test, not(unix)))]
use super::protocol::ResponseEnvelope;
use super::protocol::{
    CONTROL_IO_TIMEOUT, ControlError, ControlErrorCode, ControlRequest, ControlResponse,
};
#[cfg(unix)]
use super::protocol::{RequestEnvelope, ResponseEnvelope, read_frame, write_frame};

/// One-request-per-connection control client.
#[derive(Clone, Debug)]
pub struct ControlClient {
    path: PathBuf,
    timeout: Duration,
}

impl ControlClient {
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            timeout: CONTROL_IO_TIMEOUT,
        }
    }

    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    #[cfg(unix)]
    pub async fn request(&self, request: ControlRequest) -> Result<ControlResponse, ControlError> {
        self.exchange(request).await.map_err(Failure::into_error)
    }

    /// Sends `request`, or `fallback` if the daemon closes the connection
    /// without answering it.
    ///
    /// A daemon that predates an operation cannot decode the request and
    /// closes the connection without a response, so this lets a newer CLI
    /// ask for a newer operation and still work with an older daemon. Use it
    /// only for read-only requests: a connection that drops mid-response is
    /// treated the same way.
    #[cfg(unix)]
    pub async fn request_with_fallback(
        &self,
        request: ControlRequest,
        fallback: ControlRequest,
    ) -> Result<ControlResponse, ControlError> {
        match self.exchange(request).await {
            Err(Failure::Unanswered(_)) => self.request(fallback).await,
            result => result.map_err(Failure::into_error),
        }
    }

    #[cfg(unix)]
    async fn exchange(&self, request: ControlRequest) -> Result<ControlResponse, Failure> {
        use tokio::net::UnixStream;

        let envelope = RequestEnvelope::new(request);
        let request_id = envelope.request_id;
        let operation = async {
            let mut stream = UnixStream::connect(&self.path).await.map_err(Stage::Send)?;
            write_frame(&mut stream, &envelope)
                .await
                .map_err(Stage::Send)?;
            read_frame::<_, ResponseEnvelope>(&mut stream)
                .await
                .map_err(Stage::Receive)
        };
        let response = tokio::time::timeout(self.timeout, operation)
            .await
            .map_err(|_| {
                Failure::Error(ControlError::new(
                    ControlErrorCode::Unavailable,
                    "control request timed out",
                ))
            })?
            .map_err(|stage| {
                let unanswered = matches!(
                    &stage,
                    Stage::Receive(error) if matches!(
                        error.kind(),
                        io::ErrorKind::UnexpectedEof | io::ErrorKind::ConnectionReset
                    )
                );
                let error = match stage {
                    Stage::Send(error) | Stage::Receive(error) => error,
                };
                let error = ControlError::new(
                    ControlErrorCode::Unavailable,
                    format!(
                        "cannot communicate with daemon at `{}`: {error}",
                        self.path.display()
                    ),
                );
                if unanswered {
                    Failure::Unanswered(error)
                } else {
                    Failure::Error(error)
                }
            })?;
        validate_response_metadata(&response, request_id).map_err(Failure::Error)?;
        response.into_result().map_err(Failure::Error)
    }

    #[cfg(not(unix))]
    pub async fn request_with_fallback(
        &self,
        request: ControlRequest,
        _fallback: ControlRequest,
    ) -> Result<ControlResponse, ControlError> {
        self.request(request).await
    }

    #[cfg(not(unix))]
    pub async fn request(&self, _request: ControlRequest) -> Result<ControlResponse, ControlError> {
        Err(ControlError::new(
            ControlErrorCode::Unavailable,
            format!(
                "Unix-domain control sockets are unsupported on this platform (`{}`)",
                self.path.display()
            ),
        ))
    }
}

/// Where a Unix control exchange failed.
#[cfg(unix)]
enum Stage {
    /// Connecting or writing the request.
    Send(io::Error),
    /// Reading the response.
    Receive(io::Error),
}

/// A failed control exchange.
#[cfg(unix)]
enum Failure {
    /// The daemon read the request and closed the connection without a
    /// response, as a daemon does with an operation it does not know.
    Unanswered(ControlError),
    Error(ControlError),
}

#[cfg(unix)]
impl Failure {
    fn into_error(self) -> ControlError {
        match self {
            Self::Unanswered(error) | Self::Error(error) => error,
        }
    }
}

#[cfg(any(unix, test))]
fn validate_response_metadata(
    response: &ResponseEnvelope,
    request_id: uuid::Uuid,
) -> Result<(), ControlError> {
    if response.protocol_version != crate::CONTROL_PROTOCOL_VERSION {
        return Err(ControlError::new(
            ControlErrorCode::UnsupportedVersion,
            format!(
                "daemon responded with control protocol version {}; client supports {}",
                response.protocol_version,
                crate::CONTROL_PROTOCOL_VERSION
            ),
        ));
    }
    if response.request_id != request_id {
        return Err(ControlError::new(
            ControlErrorCode::InvalidRequest,
            "control response request ID does not match",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::protocol::VersionSnapshot;

    #[test]
    fn rejects_mismatched_response_protocol_version() {
        let request_id = uuid::Uuid::new_v4();
        let mut response = ResponseEnvelope::success(
            request_id,
            ControlResponse::Version(VersionSnapshot {
                application: "test".to_owned(),
                protocol: crate::CONTROL_PROTOCOL_VERSION,
            }),
        );
        response.protocol_version = crate::CONTROL_PROTOCOL_VERSION.saturating_add(1);
        assert_eq!(
            validate_response_metadata(&response, request_id)
                .expect_err("mismatched protocol must fail")
                .code,
            ControlErrorCode::UnsupportedVersion
        );
    }
}
