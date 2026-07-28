// SPDX-License-Identifier: AGPL-3.0-or-later

use std::net::SocketAddr;

use criterion::{Criterion, criterion_group, criterion_main};
use wire_relay::relay::{ListenerId, SessionKey};

fn session_key_construction(criterion: &mut Criterion) {
    let client: SocketAddr = "198.51.100.20:51820"
        .parse()
        .unwrap_or_else(|error| panic!("static benchmark address must parse: {error}"));
    criterion.bench_function("session_key_construction", |bencher| {
        bencher.iter(|| SessionKey::new(ListenerId::new(7), client));
    });
}

criterion_group!(benches, session_key_construction);
criterion_main!(benches);
