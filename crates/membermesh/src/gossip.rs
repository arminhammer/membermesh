use bytes::Bytes;
use iroh::{EndpointAddr, address_lookup::memory::MemoryLookup};
use iroh_gossip::{
    Event as GossipEvent, Gossip, GossipReceiver, GossipSender, JoinOptions, TopicId,
};
use membermesh_core::{AddrHint, MAX_ADDR_HINT_BYTES, PeerId};
use n0_future::StreamExt as _;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::{MemberMeshError, error::ExternalError, signer::peer_id_from_endpoint};

/// Encodes an `iroh` [`EndpointAddr`] into a bounded [`AddrHint`] if it fits within [`MAX_ADDR_HINT_BYTES`].
#[must_use]
pub fn encode_addr_hint(addr: &EndpointAddr) -> Option<AddrHint> {
    if let Ok(bytes) = postcard::to_allocvec(addr)
        && bytes.len() <= MAX_ADDR_HINT_BYTES
        && let Ok(hint) = AddrHint::try_new(bytes)
    {
        return Some(hint);
    }
    // Fall back to keeping at most the first 2 transport addresses so it fits.
    let mut trimmed = EndpointAddr::new(addr.id);
    for transport in addr.addrs.iter().take(2) {
        trimmed.addrs.insert(transport.clone());
    }
    let bytes = postcard::to_allocvec(&trimmed).ok()?;
    AddrHint::try_new(bytes).ok()
}

/// Decodes a bounded [`AddrHint`] back into an `iroh` [`EndpointAddr`].
#[must_use]
pub fn decode_addr_hint(hint: &AddrHint) -> Option<EndpointAddr> {
    postcard::from_bytes(hint.as_bytes()).ok()
}

/// Registers an optional [`AddrHint`] into the local [`MemoryLookup`] table.
pub fn register_addr_hint(lookup: &MemoryLookup, hint: Option<&AddrHint>) {
    if let Some(h) = hint
        && let Some(addr) = decode_addr_hint(h)
    {
        lookup.add_endpoint_info(addr);
    }
}

#[derive(Debug)]
pub(crate) enum GossipBridgeEvent {
    Received(Bytes),
    NeighborUp(PeerId),
    Lagged,
}

/// Bridge managing the `iroh-gossip` topic sender and background event-reader task.
#[derive(Debug)]
pub(crate) struct GossipBridge {
    sender: GossipSender,
    reader_task: tokio::task::JoinHandle<()>,
}

impl GossipBridge {
    /// Subscribes to `topic_id` with initial `bootstrap` peers and forwards events to `tx`.
    pub(crate) async fn subscribe(
        gossip: &Gossip,
        topic_id: TopicId,
        bootstrap: Vec<iroh::EndpointId>,
        tx: mpsc::UnboundedSender<GossipBridgeEvent>,
    ) -> Result<(Self, GossipTopicJoinWaiter), MemberMeshError> {
        let opts = JoinOptions::with_bootstrap(bootstrap);
        let topic = gossip
            .subscribe_with_opts(topic_id, opts)
            .await
            .map_err(|err| MemberMeshError::Gossip(ExternalError::from_display(err)))?;
        let (sender, mut receiver) = topic.split();

        let (joined_tx, joined_rx) = tokio::sync::oneshot::channel();
        let reader_task = tokio::spawn(async move {
            let mut joined_signal = Some(joined_tx);
            if receiver.is_joined()
                && let Some(sig) = joined_signal.take()
            {
                let _ = sig.send(());
            }
            while let Some(item) = receiver.next().await {
                match item {
                    Ok(GossipEvent::Received(msg)) => {
                        let _ = tx.send(GossipBridgeEvent::Received(msg.content));
                    }
                    Ok(GossipEvent::NeighborUp(peer)) => {
                        if let Some(sig) = joined_signal.take() {
                            let _ = sig.send(());
                        }
                        let _ = tx.send(GossipBridgeEvent::NeighborUp(peer_id_from_endpoint(peer)));
                    }
                    Ok(GossipEvent::Lagged) => {
                        warn!("iroh-gossip receiver lagged; scheduling out-of-band PushPullSync");
                        let _ = tx.send(GossipBridgeEvent::Lagged);
                    }
                    Ok(GossipEvent::NeighborDown(_) | _) => {}
                    Err(err) => {
                        debug!("iroh-gossip stream ended with error: {err}");
                        break;
                    }
                }
            }
        });

        Ok((
            Self {
                sender,
                reader_task,
            },
            GossipTopicJoinWaiter { rx: joined_rx },
        ))
    }

    pub(crate) async fn join_peers(
        &self,
        peers: Vec<iroh::EndpointId>,
    ) -> Result<(), MemberMeshError> {
        if peers.is_empty() {
            return Ok(());
        }
        self.sender
            .join_peers(peers)
            .await
            .map_err(|err| MemberMeshError::Gossip(ExternalError::from_display(err)))
    }

    pub(crate) async fn broadcast(&self, bytes: Bytes) -> Result<(), MemberMeshError> {
        if bytes.len() > self.sender.max_content_size() {
            return Err(MemberMeshError::Core(
                membermesh_core::CoreError::FrameTooLarge {
                    size: bytes.len(),
                    max: self.sender.max_content_size(),
                },
            ));
        }
        self.sender
            .broadcast(bytes)
            .await
            .map_err(|err| MemberMeshError::Gossip(ExternalError::from_display(err)))
    }
}

pub(crate) struct GossipTopicJoinWaiter {
    rx: tokio::sync::oneshot::Receiver<()>,
}

impl GossipTopicJoinWaiter {
    pub(crate) async fn wait_bounded(self, limit: std::time::Duration) {
        let _ = tokio::time::timeout(limit, self.rx).await;
    }
}

impl Drop for GossipBridge {
    fn drop(&mut self) {
        self.reader_task.abort();
    }
}

const _: fn() = || {
    fn assert_send<T: Send>() {}
    assert_send::<GossipReceiver>();
};
