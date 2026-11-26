//! Optional LAN multicast auto-discovery and automatic peer joining.

use std::{
    io,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    pin::pin,
    time::Duration,
};

use iroh::{Endpoint, EndpointAddr, EndpointId, TransportAddr};
use iroh_gossip::TopicId;
use serde::{Deserialize, Serialize};
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use tokio::{
    net::UdpSocket,
    sync::mpsc,
    time::{Instant, sleep},
};
use tracing::{debug, warn};

use crate::actor::InternalMsg;

/// Default IPv4 multicast group used by `membermesh` LAN auto-discovery (`239.255.42.99`).
pub const DEFAULT_MULTICAST_ADDR: Ipv4Addr = Ipv4Addr::new(239, 255, 42, 99);

/// Default UDP port used by `membermesh` LAN auto-discovery (`5354`).
pub const DEFAULT_DISCOVERY_PORT: u16 = 5354;

/// Default interval between periodic LAN discovery beacons (`3s`).
pub const DEFAULT_BEACON_INTERVAL: Duration = Duration::from_secs(3);

const BEACON_MAGIC: [u8; 4] = *b"MMBD";
const BEACON_VERSION: u8 = 1;
const MAX_BEACON_BYTES: usize = 1200;
const RECV_BUF_BYTES: usize = 1500;
const MIN_BEACON_INTERVAL_MS: u64 = 50;

/// Configuration for optional UDP multicast LAN peer auto-discovery.
///
/// # Example
///
/// ```
/// use std::time::Duration;
/// use membermesh::DiscoveryConfig;
///
/// let cfg = DiscoveryConfig::lan()
///     .with_port(5355)
///     .with_beacon_interval(Duration::from_secs(2));
/// assert_eq!(cfg.port(), 5355);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DiscoveryConfig {
    multicast_addr: Ipv4Addr,
    port: u16,
    interface: Option<Ipv4Addr>,
    beacon_interval: Duration,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self::lan()
    }
}

impl DiscoveryConfig {
    /// Returns the default LAN auto-discovery configuration (`239.255.42.99:5354`, 3s beacons).
    #[must_use]
    pub const fn lan() -> Self {
        Self {
            multicast_addr: DEFAULT_MULTICAST_ADDR,
            port: DEFAULT_DISCOVERY_PORT,
            interface: None,
            beacon_interval: DEFAULT_BEACON_INTERVAL,
        }
    }

    /// Returns a loopback-scoped auto-discovery configuration suitable for local tests on `port`.
    #[must_use]
    pub const fn loopback(port: u16) -> Self {
        Self {
            multicast_addr: DEFAULT_MULTICAST_ADDR,
            port,
            interface: Some(Ipv4Addr::LOCALHOST),
            beacon_interval: Duration::from_millis(250),
        }
    }

    /// Sets the IPv4 multicast group address used for discovery beacons.
    #[must_use]
    pub const fn with_multicast_addr(mut self, multicast_addr: Ipv4Addr) -> Self {
        self.multicast_addr = multicast_addr;
        self
    }

    /// Sets the UDP port used for discovery beacons.
    #[must_use]
    pub const fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Pins multicast join and outbound traffic to a specific IPv4 interface address.
    #[must_use]
    pub const fn with_interface(mut self, interface: Ipv4Addr) -> Self {
        self.interface = Some(interface);
        self
    }

    /// Sets the periodic beacon broadcast interval.
    #[must_use]
    pub const fn with_beacon_interval(mut self, beacon_interval: Duration) -> Self {
        self.beacon_interval = beacon_interval;
        self
    }

    /// Returns the configured IPv4 multicast group address.
    #[must_use]
    pub const fn multicast_addr(&self) -> Ipv4Addr {
        self.multicast_addr
    }

    /// Returns the configured UDP port.
    #[must_use]
    pub const fn port(&self) -> u16 {
        self.port
    }

    /// Returns the optional pinned IPv4 interface address.
    #[must_use]
    pub const fn interface(&self) -> Option<Ipv4Addr> {
        self.interface
    }

    /// Returns the periodic beacon broadcast interval.
    #[must_use]
    pub const fn beacon_interval(&self) -> Duration {
        self.beacon_interval
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct DiscoveryBeacon {
    magic: [u8; 4],
    version: u8,
    topic_id: [u8; 32],
    addr: EndpointAddr,
}

pub(crate) fn encode_beacon(topic_id: TopicId, addr: &EndpointAddr) -> Option<Vec<u8>> {
    let beacon = DiscoveryBeacon {
        magic: BEACON_MAGIC,
        version: BEACON_VERSION,
        topic_id: *topic_id.as_bytes(),
        addr: addr.clone(),
    };
    if let Ok(bytes) = postcard::to_allocvec(&beacon)
        && bytes.len() <= MAX_BEACON_BYTES
    {
        return Some(bytes);
    }

    // Fall back to keeping at most 4 transport addresses so the beacon fits within one datagram.
    let mut trimmed = EndpointAddr::new(addr.id);
    for transport in addr.addrs.iter().take(4) {
        trimmed.addrs.insert(transport.clone());
    }
    let trimmed_beacon = DiscoveryBeacon {
        magic: BEACON_MAGIC,
        version: BEACON_VERSION,
        topic_id: *topic_id.as_bytes(),
        addr: trimmed,
    };
    let bytes = postcard::to_allocvec(&trimmed_beacon).ok()?;
    (bytes.len() <= MAX_BEACON_BYTES).then_some(bytes)
}

pub(crate) fn decode_beacon(bytes: &[u8]) -> Option<(TopicId, EndpointAddr)> {
    if bytes.len() > MAX_BEACON_BYTES {
        return None;
    }
    let beacon: DiscoveryBeacon = postcard::from_bytes(bytes).ok()?;
    if beacon.magic != BEACON_MAGIC || beacon.version != BEACON_VERSION {
        return None;
    }
    Some((TopicId::from_bytes(beacon.topic_id), beacon.addr))
}

fn bind_discovery_socket(config: &DiscoveryConfig) -> io::Result<UdpSocket> {
    if !config.multicast_addr.is_multicast() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{} is not a multicast IPv4 address", config.multicast_addr),
        ));
    }

    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_address(true)?;
    #[cfg(all(
        unix,
        not(any(target_os = "solaris", target_os = "illumos", target_os = "cygwin"))
    ))]
    socket.set_reuse_port(true)?;

    let bind_addr = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, config.port);
    socket.bind(&SockAddr::from(bind_addr))?;

    match config.interface {
        Some(iface) => {
            socket.join_multicast_v4(&config.multicast_addr, &iface)?;
            socket.set_multicast_if_v4(&iface)?;
        }
        None => {
            if let Err(err) =
                socket.join_multicast_v4(&config.multicast_addr, &Ipv4Addr::UNSPECIFIED)
            {
                debug!("default multicast join failed ({err}); falling back to loopback interface");
                socket.join_multicast_v4(&config.multicast_addr, &Ipv4Addr::LOCALHOST)?;
                socket.set_multicast_if_v4(&Ipv4Addr::LOCALHOST)?;
            }
        }
    }

    socket.set_multicast_loop_v4(true)?;
    socket.set_multicast_ttl_v4(1)?;
    socket.set_nonblocking(true)?;

    let std_socket: std::net::UdpSocket = socket.into();
    UdpSocket::from_std(std_socket)
}

fn jittered_delay(local_id: EndpointId, base_ms: u64, tick: u64) -> Duration {
    let id_bytes = local_id.as_bytes();
    let b0 = u64::from(id_bytes.first().copied().unwrap_or(0));
    let b1 = u64::from(id_bytes.get(1).copied().unwrap_or(0));
    let seed = b0 ^ (b1 << 8) ^ tick.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let max_jitter = base_ms / 5;
    let jitter_ms = if max_jitter > 0 { seed % max_jitter } else { 0 };
    Duration::from_millis(base_ms.saturating_add(jitter_ms))
}

fn sanitize_local_addr(mut addr: EndpointAddr, prefer_loopback: bool) -> EndpointAddr {
    if prefer_loopback {
        let has_ip = addr.addrs.iter().any(|t| matches!(t, TransportAddr::Ip(_)));
        if !has_ip {
            return addr;
        }
        addr.addrs.retain(|t| match t {
            TransportAddr::Ip(sock) => sock.ip().is_loopback(),
            TransportAddr::Relay(_) | _ => true,
        });
    }
    addr
}

async fn send_beacon(
    endpoint: &Endpoint,
    socket: &UdpSocket,
    topic_id: TopicId,
    target: SocketAddr,
    prefer_loopback: bool,
) {
    let addr = sanitize_local_addr(endpoint.addr(), prefer_loopback);
    if let Some(bytes) = encode_beacon(topic_id, &addr)
        && let Err(err) = socket.send_to(&bytes, target).await
    {
        debug!("LAN discovery beacon send failed: {err}");
    }
}

pub(crate) fn spawn_discovery_task(
    endpoint: Endpoint,
    topic_id: TopicId,
    config: &DiscoveryConfig,
    internal_tx: mpsc::UnboundedSender<InternalMsg>,
) -> Option<tokio::task::JoinHandle<()>> {
    let socket = match bind_discovery_socket(config) {
        Ok(sock) => sock,
        Err(err) => {
            warn!("LAN auto-discovery disabled: failed to bind multicast socket: {err}");
            return None;
        }
    };

    let target = SocketAddr::V4(SocketAddrV4::new(config.multicast_addr, config.port));
    let prefer_loopback = config.interface == Some(Ipv4Addr::LOCALHOST);
    let local_id = endpoint.id();
    let base_ms = u64::try_from(config.beacon_interval.as_millis())
        .unwrap_or(u64::MAX)
        .max(MIN_BEACON_INTERVAL_MS);

    let handle = tokio::spawn(async move {
        // Emit an immediate beacon so newly joined peers discover each other promptly.
        send_beacon(&endpoint, &socket, topic_id, target, prefer_loopback).await;

        let mut tick_count: u64 = 1;
        let mut timer = pin!(sleep(jittered_delay(local_id, base_ms, tick_count)));
        let mut recv_buf = [0u8; RECV_BUF_BYTES];

        loop {
            tokio::select! {
                () = &mut timer => {
                    send_beacon(&endpoint, &socket, topic_id, target, prefer_loopback).await;
                    tick_count = tick_count.wrapping_add(1);
                    let next_deadline = Instant::now() + jittered_delay(local_id, base_ms, tick_count);
                    timer.as_mut().reset(next_deadline);
                }
                recv_res = socket.recv_from(&mut recv_buf) => {
                    match recv_res {
                        Ok((len, _src)) => {
                            let Some(slice) = recv_buf.get(..len) else {
                                continue;
                            };
                            let Some((beacon_topic, addr)) = decode_beacon(slice) else {
                                continue;
                            };
                            if beacon_topic != topic_id || addr.id == local_id {
                                continue;
                            }
                            if internal_tx.send(InternalMsg::DiscoveredPeer(addr)).is_err() {
                                break;
                            }
                        }
                        Err(err) => {
                            debug!("LAN discovery socket recv error: {err}");
                            sleep(Duration::from_millis(50)).await;
                        }
                    }
                }
            }
        }
    });

    Some(handle)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use iroh::{SecretKey, TransportAddr};
    use iroh_gossip::TopicId;

    use super::*;

    #[test]
    fn beacon_roundtrips_endpoint_addr_and_topic() {
        let key = SecretKey::from_bytes(&[0x11; 32]);
        let mut addr = EndpointAddr::new(key.public());
        addr.addrs.insert(TransportAddr::Ip(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            9001,
        )));
        let topic = TopicId::from_bytes([0xAB; 32]);

        let bytes = encode_beacon(topic, &addr).expect("encode beacon");
        let (decoded_topic, decoded_addr) = decode_beacon(&bytes).expect("decode beacon");

        assert_eq!(decoded_topic, topic);
        assert_eq!(decoded_addr, addr);
    }

    #[test]
    fn decode_beacon_rejects_wrong_magic_version_and_garbage() {
        let key = SecretKey::from_bytes(&[0x22; 32]);
        let addr = EndpointAddr::new(key.public());
        let topic = TopicId::from_bytes([0xCD; 32]);

        let wrong_magic = DiscoveryBeacon {
            magic: *b"XXXX",
            version: BEACON_VERSION,
            topic_id: *topic.as_bytes(),
            addr: addr.clone(),
        };
        let wrong_magic_bytes = postcard::to_allocvec(&wrong_magic).unwrap();
        assert!(decode_beacon(&wrong_magic_bytes).is_none());

        let wrong_version = DiscoveryBeacon {
            magic: BEACON_MAGIC,
            version: BEACON_VERSION + 1,
            topic_id: *topic.as_bytes(),
            addr,
        };
        let wrong_version_bytes = postcard::to_allocvec(&wrong_version).unwrap();
        assert!(decode_beacon(&wrong_version_bytes).is_none());

        assert!(decode_beacon(&[]).is_none());
        assert!(decode_beacon(&[0xFF; 16]).is_none());
        assert!(decode_beacon(&[0u8; MAX_BEACON_BYTES + 1]).is_none());
    }

    #[test]
    fn encode_beacon_trims_oversized_addr_within_mtu_bound() {
        let key = SecretKey::from_bytes(&[0x33; 32]);
        let mut addr = EndpointAddr::new(key.public());
        for port in 10_000..10_200 {
            addr.addrs.insert(TransportAddr::Ip(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
                port,
            )));
        }
        let topic = TopicId::from_bytes([0xEF; 32]);

        let bytes = encode_beacon(topic, &addr).expect("trimmed beacon should encode");
        assert!(bytes.len() <= MAX_BEACON_BYTES);

        let (decoded_topic, decoded_addr) = decode_beacon(&bytes).expect("decode trimmed");
        assert_eq!(decoded_topic, topic);
        assert_eq!(decoded_addr.id, addr.id);
        assert!(decoded_addr.addrs.len() <= 4);
    }

    #[test]
    fn bind_rejects_non_multicast_address() {
        let cfg = DiscoveryConfig::loopback(0).with_multicast_addr(Ipv4Addr::LOCALHOST);
        let err = bind_discovery_socket(&cfg).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }
}
