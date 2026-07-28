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
        use tokio::net::UnixStream;

        let envelope = RequestEnvelope::new(request);
        let request_id = envelope.request_id;
        let operation = async {
            let mut stream = UnixStream::connect(&self.path).await?;
            write_frame(&mut stream, &envelope).await?;
            let response: ResponseEnvelope = read_frame(&mut stream).await?;
            Ok::<_, io::Error>(response)
        };
        let response = tokio::time::timeout(self.timeout, operation)
            .await
            .map_err(|_| {
                ControlError::new(ControlErrorCode::Unavailable, "control request timed out")
            })?
            .map_err(|error| {
                ControlError::new(
                    ControlErrorCode::Unavailable,
                    format!(
                        "cannot communicate with daemon at `{}`: {error}",
                        self.path.display()
                    ),
                )
            })?;
        validate_response_metadata(&response, request_id)?;
        response.into_result()
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
