// SPDX-License-Identifier: AGPL-3.0-or-later

//! Bounded Unix Domain Socket server for daemon-owned live state.

use std::{
    future::Future,
    io,
    path::{Path, PathBuf},
    pin::Pin,
};

use std::sync::Arc;
use tokio_util::sync::CancellationToken;
#[cfg(unix)]
use tokio_util::task::TaskTracker;

use super::protocol::{ControlError, ControlRequest, ControlResponse};

/// Maximum simultaneous local control clients.
pub const MAX_CONTROL_CONNECTIONS: usize = 32;

/// Async operation dispatcher implemented by the runtime.
pub trait ControlHandler: Send + Sync + 'static {
    fn handle(
        &self,
        request: ControlRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ControlResponse, ControlError>> + Send + '_>>;
}

/// A bound socket created during startup or reload preflight.
#[derive(Debug)]
pub struct PreparedControlServer {
    #[cfg(unix)]
    listener: tokio::net::UnixListener,
    path: PathBuf,
    #[cfg(unix)]
    path_guard: SocketPathGuard,
}

impl PreparedControlServer {
    #[cfg(unix)]
    pub async fn bind(path: impl Into<PathBuf>) -> io::Result<Self> {
        use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
        use tokio::net::UnixStream;

        let path = path.into();
        if !path.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "control socket path must be absolute",
            ));
        }
        let parent = path.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "control socket must have a parent directory",
            )
        })?;
        tokio::fs::create_dir_all(parent).await?;
        validate_control_parent(parent)?;

        match tokio::fs::symlink_metadata(&path).await {
            Ok(metadata) => {
                if !metadata.file_type().is_socket() {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        format!(
                            "refusing to replace non-socket control path `{}`",
                            path.display()
                        ),
                    ));
                }
                match tokio::time::timeout(
                    std::time::Duration::from_millis(250),
                    UnixStream::connect(&path),
                )
                .await
                {
                    Ok(Ok(_)) => {
                        return Err(io::Error::new(
                            io::ErrorKind::AddrInUse,
                            format!("a daemon is already listening at `{}`", path.display()),
                        ));
                    }
                    Ok(Err(error))
                        if matches!(
                            error.kind(),
                            io::ErrorKind::ConnectionRefused | io::ErrorKind::NotFound
                        ) =>
                    {
                        remove_stale_socket(&path, metadata.dev(), metadata.ino())?;
                    }
                    Ok(Err(error)) => return Err(error),
                    Err(_) => {
                        return Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            format!(
                                "timed out checking existing control socket `{}`",
                                path.display()
                            ),
                        ));
                    }
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }

        let listener = tokio::net::UnixListener::bind(&path)?;
        let metadata = match std::fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(inspect_error) => {
                drop(listener);
                return match std::fs::remove_file(&path) {
                    Ok(()) => Err(inspect_error),
                    Err(cleanup_error) if cleanup_error.kind() == io::ErrorKind::NotFound => {
                        Err(inspect_error)
                    }
                    Err(cleanup_error) => Err(io::Error::new(
                        inspect_error.kind(),
                        format!(
                            "could not inspect newly bound control socket: {inspect_error}; \
                             cleanup also failed: {cleanup_error}"
                        ),
                    )),
                };
            }
        };
        let path_guard = SocketPathGuard {
            path: path.clone(),
            device: metadata.dev(),
            inode: metadata.ino(),
            armed: true,
        };
        // The parent path is restricted to trusted owners above, so no
        // untrusted process can replace the pathname between these operations.
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o660))?;
        let current = std::fs::symlink_metadata(&path)?;
        if !path_guard.matches(&current) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "control socket path `{}` changed while it was being prepared",
                    path.display()
                ),
            ));
        }
        Ok(Self {
            listener,
            path: path.clone(),
            path_guard,
        })
    }

    #[cfg(not(unix))]
    pub async fn bind(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path = path.into();
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!(
                "Unix-domain control socket `{}` is unsupported on this platform",
                path.display()
            ),
        ))
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[cfg(unix)]
    #[must_use]
    pub fn activate(
        self,
        handler: Arc<dyn ControlHandler>,
        parent_cancel: &CancellationToken,
        tracker: &TaskTracker,
    ) -> ControlServerHandle {
        use tokio::sync::Semaphore;

        let cancel = parent_cancel.child_token();
        let task_cancel = cancel.clone();
        let path = self.path.clone();
        let cleanup_error = Arc::new(std::sync::Mutex::new(None));
        let task_cleanup_error = Arc::clone(&cleanup_error);
        let semaphore = Arc::new(Semaphore::new(MAX_CONTROL_CONNECTIONS));
        let connection_tracker = tracker.clone();

        tracker.spawn(async move {
            let mut path_guard = self.path_guard;
            let mut last_accept_warning = None;
            loop {
                tokio::select! {
                    biased;
                    () = task_cancel.cancelled() => break,
                    accepted = self.listener.accept() => {
                        match accepted {
                            Ok((stream, _address)) => {
                                let Ok(permit) = Arc::clone(&semaphore).try_acquire_owned() else {
                                    continue;
                                };
                                let connection_handler = Arc::clone(&handler);
                                connection_tracker.spawn(async move {
                                    let _permit = permit;
                                    serve_connection(stream, connection_handler).await;
                                });
                            }
                            Err(error) => {
                                let now = std::time::Instant::now();
                                if last_accept_warning.is_none_or(|last| {
                                    now.saturating_duration_since(last)
                                        >= std::time::Duration::from_secs(5)
                                }) {
                                    tracing::warn!(%error, "control socket accept failed");
                                    last_accept_warning = Some(now);
                                }
                                tokio::time::sleep(
                                    std::time::Duration::from_millis(100),
                                ).await;
                            }
                        }
                    }
                }
            }
            if let Err(async_error) = path_guard.remove_async().await {
                tracing::warn!(
                    path = %path_guard.path.display(),
                    error = %async_error,
                    "failed to remove owned control socket asynchronously; retrying"
                );
                if let Err(sync_error) = path_guard.remove_sync() {
                    tracing::warn!(
                        path = %path_guard.path.display(),
                        error = %sync_error,
                        "failed to remove owned control socket during shutdown"
                    );
                    // This is the terminal, reported cleanup outcome. Prevent
                    // Drop from performing an unobservable third retry that
                    // could make the stored shutdown result inaccurate.
                    path_guard.armed = false;
                    *task_cleanup_error
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(format!(
                        "asynchronous removal failed: {async_error}; \
                         synchronous retry failed: {sync_error}"
                    ));
                }
            }
        });

        ControlServerHandle {
            path,
            cancel,
            cleanup_error,
        }
    }
}

#[cfg(unix)]
fn validate_control_parent(parent: &Path) -> io::Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let effective_uid = rustix::process::geteuid().as_raw();
    let mut current = PathBuf::new();
    for component in parent.components() {
        current.push(component.as_os_str());
        let metadata = std::fs::symlink_metadata(&current)?;
        if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "control socket ancestor `{}` must be a real directory",
                    current.display()
                ),
            ));
        }

        let owner = metadata.uid();
        if owner != 0 && owner != effective_uid {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!(
                    "control socket ancestor `{}` is owned by untrusted uid {owner}",
                    current.display()
                ),
            ));
        }

        let mode = metadata.permissions().mode();
        let writable_by_others = mode & 0o022 != 0;
        let sticky = mode & 0o1000 != 0;
        if writable_by_others && (!sticky || owner != 0 || current == parent) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!(
                    "control socket ancestor `{}` is writable by an untrusted group or user",
                    current.display()
                ),
            ));
        }
    }
    Ok(())
}

#[cfg(unix)]
fn remove_stale_socket(path: &Path, device: u64, inode: u64) -> io::Result<()> {
    use std::os::unix::fs::{FileTypeExt, MetadataExt};

    match std::fs::symlink_metadata(path) {
        Ok(metadata)
            if metadata.file_type().is_socket()
                && metadata.dev() == device
                && metadata.ino() == inode =>
        {
            std::fs::remove_file(path)
        }
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!(
                "control socket path `{}` changed while stale-socket ownership was checked",
                path.display()
            ),
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

/// Active control server handle.
#[derive(Debug)]
pub struct ControlServerHandle {
    path: PathBuf,
    cancel: CancellationToken,
    cleanup_error: Arc<std::sync::Mutex<Option<String>>>,
}

impl ControlServerHandle {
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn stop(&self) {
        self.cancel.cancel();
    }

    #[must_use]
    pub fn cleanup_error(&self) -> Option<String> {
        self.cleanup_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

#[cfg(unix)]
async fn serve_connection(mut stream: tokio::net::UnixStream, handler: Arc<dyn ControlHandler>) {
    use crate::CONTROL_PROTOCOL_VERSION;

    use super::protocol::{
        CONTROL_IO_TIMEOUT, ControlErrorCode, RequestEnvelope, ResponseEnvelope, read_frame,
        write_frame,
    };

    let request = match tokio::time::timeout(
        CONTROL_IO_TIMEOUT,
        read_frame::<_, RequestEnvelope>(&mut stream),
    )
    .await
    {
        Ok(Ok(request)) => request,
        Ok(Err(error)) => {
            tracing::debug!(%error, "invalid control request");
            return;
        }
        Err(_) => {
            tracing::debug!("control client read timed out");
            return;
        }
    };
    let request_id = request.request_id;
    let response = if request.protocol_version == CONTROL_PROTOCOL_VERSION {
        match handler.handle(request.request).await {
            Ok(response) => ResponseEnvelope::success(request_id, response),
            Err(error) => ResponseEnvelope::failure(request_id, error),
        }
    } else {
        ResponseEnvelope::failure(
            request_id,
            ControlError::new(
                ControlErrorCode::UnsupportedVersion,
                format!(
                    "unsupported control protocol version {}; daemon supports {}",
                    request.protocol_version, CONTROL_PROTOCOL_VERSION
                ),
            ),
        )
    };

    if let Err(error) =
        tokio::time::timeout(CONTROL_IO_TIMEOUT, write_frame(&mut stream, &response))
            .await
            .unwrap_or_else(|_| {
                Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "control response timed out",
                ))
            })
    {
        tracing::debug!(%error, "failed to write control response");
    }
}

#[cfg(unix)]
#[derive(Debug)]
struct SocketPathGuard {
    path: PathBuf,
    device: u64,
    inode: u64,
    armed: bool,
}

#[cfg(unix)]
impl SocketPathGuard {
    fn matches(&self, metadata: &std::fs::Metadata) -> bool {
        use std::os::unix::fs::{FileTypeExt, MetadataExt};

        metadata.file_type().is_socket()
            && metadata.dev() == self.device
            && metadata.ino() == self.inode
    }

    async fn remove_async(&mut self) -> io::Result<()> {
        match tokio::fs::symlink_metadata(&self.path).await {
            Ok(metadata) if self.matches(&metadata) => {
                match tokio::fs::remove_file(&self.path).await {
                    Ok(()) => {
                        self.armed = false;
                        Ok(())
                    }
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {
                        self.armed = false;
                        Ok(())
                    }
                    Err(error) => Err(error),
                }
            }
            Ok(_) => {
                tracing::warn!(
                    path = %self.path.display(),
                    "control socket path was replaced; refusing to unlink the replacement"
                );
                self.armed = false;
                Ok(())
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                self.armed = false;
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    fn remove_sync(&mut self) -> io::Result<()> {
        match std::fs::symlink_metadata(&self.path) {
            Ok(metadata) if self.matches(&metadata) => match std::fs::remove_file(&self.path) {
                Ok(()) => {
                    self.armed = false;
                    Ok(())
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    self.armed = false;
                    Ok(())
                }
                Err(error) => Err(error),
            },
            Ok(_) => {
                tracing::warn!(
                    path = %self.path.display(),
                    "control socket path was replaced; refusing to unlink the replacement"
                );
                self.armed = false;
                Ok(())
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                self.armed = false;
                Ok(())
            }
            Err(error) => Err(error),
        }
    }
}

#[cfg(unix)]
impl Drop for SocketPathGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Err(error) = self.remove_sync() {
            tracing::warn!(
                path = %self.path.display(),
                %error,
                "failed to remove owned control socket during final cleanup"
            );
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::{
        fs,
        os::unix::{fs::PermissionsExt, net::UnixListener as StdUnixListener},
    };

    use tempfile::tempdir;

    use super::{remove_stale_socket, validate_control_parent};

    #[test]
    fn rejects_writable_or_symlinked_control_directories() {
        let temporary = tempdir().expect("temporary directory must be created");
        validate_control_parent(temporary.path()).expect("private temporary directory is trusted");

        let writable = temporary.path().join("writable");
        fs::create_dir(&writable).expect("test directory must be created");
        fs::set_permissions(&writable, fs::Permissions::from_mode(0o770))
            .expect("test permissions must be set");
        let error =
            validate_control_parent(&writable).expect_err("group-writable directory must fail");
        assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);

        let trusted = temporary.path().join("trusted");
        fs::create_dir(&trusted).expect("trusted directory must be created");
        let linked = temporary.path().join("linked");
        std::os::unix::fs::symlink(&trusted, &linked).expect("test symlink must be created");
        let error = validate_control_parent(&linked).expect_err("symlinked directory must fail");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn stale_cleanup_refuses_a_replacement_path() {
        use std::os::unix::fs::MetadataExt;

        let temporary = tempdir().expect("temporary directory must be created");
        let socket_path = temporary.path().join("control.sock");
        let listener =
            StdUnixListener::bind(&socket_path).expect("test Unix socket must be created");
        let metadata =
            fs::symlink_metadata(&socket_path).expect("socket metadata must be available");
        fs::remove_file(&socket_path).expect("original socket path must be removed");
        fs::write(&socket_path, b"replacement").expect("replacement file must be created");

        let error = remove_stale_socket(&socket_path, metadata.dev(), metadata.ino())
            .expect_err("replacement must not be removed");
        assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
        assert_eq!(
            fs::read(&socket_path).expect("replacement must remain"),
            b"replacement"
        );
        drop(listener);
    }
}
