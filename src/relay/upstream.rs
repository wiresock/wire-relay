// SPDX-License-Identifier: AGPL-3.0-or-later

//! Address-family-aware connected upstream UDP sockets.

use std::{io, net::SocketAddr};

use tokio::net::UdpSocket;

/// Bind and connect a dedicated UDP socket to the fixed backend address.
pub async fn connect(backend: SocketAddr) -> io::Result<UdpSocket> {
    let wildcard = if backend.is_ipv4() {
        SocketAddr::from(([0, 0, 0, 0], 0))
    } else {
        SocketAddr::from(([0_u16; 8], 0))
    };
    let socket = UdpSocket::bind(wildcard).await?;
    socket.connect(backend).await?;
    Ok(socket)
}
