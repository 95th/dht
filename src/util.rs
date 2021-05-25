use std::net::{IpAddr, SocketAddr};

pub fn to_ipv6(addr: SocketAddr) -> SocketAddr {
    let ip = match addr.ip() {
        IpAddr::V4(v4) => v4.to_ipv6_mapped(),
        IpAddr::V6(v6) => v6,
    };
    (ip, addr.port()).into()
}
