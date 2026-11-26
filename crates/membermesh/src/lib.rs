#![deny(missing_docs)]
//! Peer-to-peer cluster membership and advisory leader election over `iroh` and `iroh-gossip`.

mod actor;
mod error;
mod gossip;
mod signer;
mod sync;

use std::sync::Arc;

use iroh::{
    Endpoint, EndpointAddr, EndpointId, address_lookup::memory::MemoryLookup, protocol::Router,
};
use iroh_gossip::{Gossip, TopicId};
use membermesh_core::{MemberMetadata, MemberView, MembershipEvent, PeerAge, ProtocolConfig};
use tokio::sync::broadcast;

use crate::actor::{MeshActorHandle, build_control_wiring, spawn_actor};

pub use error::{ExternalError, MemberMeshError};
pub use membermesh_core::crate_version;
pub use signer::{IrohSigner, IrohVerifier, endpoint_id_from_peer, peer_id_from_endpoint};
pub use sync::{ControlClient, ControlFuture, ControlHandler, ControlProtocol, SYNC_ALPN};

/// Pre-wired [`MemberMesh`] handle returned by [`MemberMesh::prepare_attach`] so the caller can
/// spawn their [`Router`] before joining bootstrap peers.
pub struct PreparedMemberMesh {
    endpoint: Endpoint,
    gossip: Gossip,
    topic_id: TopicId,
    config: ProtocolConfig,
    metadata: MemberMetadata,
    client: ControlClient,
    internal_tx: tokio::sync::mpsc::UnboundedSender<actor::InternalMsg>,
    internal_rx: tokio::sync::mpsc::UnboundedReceiver<actor::InternalMsg>,
}

impl std::fmt::Debug for PreparedMemberMesh {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedMemberMesh")
            .field("endpoint_id", &self.endpoint.id())
            .field("topic_id", &self.topic_id)
            .finish_non_exhaustive()
    }
}

impl PreparedMemberMesh {
    /// Starts the cluster actor and joins `bootstrap` peers once the caller's [`Router`] is running.
    ///
    /// # Errors
    ///
    /// Returns [`MemberMeshError`] if subscribing to the gossip topic or initializing fails.
    pub async fn start(self, bootstrap: Vec<EndpointAddr>) -> Result<MemberMesh, MemberMeshError> {
        let spawned = spawn_actor(
            self.endpoint.clone(),
            &self.gossip,
            self.topic_id,
            self.config,
            self.metadata,
            bootstrap,
            self.client,
            self.internal_tx,
            self.internal_rx,
        )
        .await?;

        Ok(MemberMesh {
            endpoint: self.endpoint,
            handle: spawned.handle,
            lookup: spawned.lookup,
            router: None,
            actor_task: Arc::new(spawned.task),
        })
    }
}

/// Live handle to a `membermesh` cluster participant running over `iroh` and `iroh-gossip`.
#[derive(Clone, Debug)]
pub struct MemberMesh {
    endpoint: Endpoint,
    handle: MeshActorHandle,
    lookup: MemoryLookup,
    router: Option<Arc<Router>>,
    actor_task: Arc<tokio::task::JoinHandle<()>>,
}

impl MemberMesh {
    /// Prepares a `membermesh` instance for a caller-managed [`Endpoint`] and [`Router`].
    ///
    /// Register both `iroh_gossip::ALPN` and the returned [`ControlProtocol`] (under [`SYNC_ALPN`])
    /// on your [`Router`], spawn the router, and then call [`PreparedMemberMesh::start`].
    #[must_use]
    pub fn prepare_attach(
        endpoint: Endpoint,
        gossip: Gossip,
        topic_id: TopicId,
        config: ProtocolConfig,
        metadata: MemberMetadata,
    ) -> (PreparedMemberMesh, ControlProtocol) {
        let (control_protocol, client, internal_tx, internal_rx) =
            build_control_wiring(&endpoint, &config);
        let prepared = PreparedMemberMesh {
            endpoint,
            gossip,
            topic_id,
            config,
            metadata,
            client,
            internal_tx,
            internal_rx,
        };
        (prepared, control_protocol)
    }
    /// Attaches a `membermesh` cluster instance to an existing caller-managed [`Endpoint`] and [`Gossip`].
    ///
    /// Returns `(mesh, control_protocol)`. The caller must register BOTH
    /// `iroh_gossip::ALPN` and [`SYNC_ALPN`] on their [`Router`] before joining peers.
    ///
    /// # Errors
    ///
    /// Returns [`MemberMeshError`] if subscribing to `topic_id` or initializing local state fails.
    pub async fn attach(
        endpoint: Endpoint,
        gossip: &Gossip,
        topic_id: TopicId,
        config: ProtocolConfig,
        metadata: MemberMetadata,
        bootstrap: Vec<EndpointAddr>,
    ) -> Result<(Self, ControlProtocol), MemberMeshError> {
        let (control_protocol, client, internal_tx, internal_rx) =
            build_control_wiring(&endpoint, &config);
        let spawned = spawn_actor(
            endpoint.clone(),
            gossip,
            topic_id,
            config,
            metadata,
            bootstrap,
            client,
            internal_tx,
            internal_rx,
        )
        .await?;

        Ok((
            Self {
                endpoint,
                handle: spawned.handle,
                lookup: spawned.lookup,
                router: None,
                actor_task: Arc::new(spawned.task),
            },
            control_protocol,
        ))
    }

    /// Spawns a standalone [`MemberMesh`] with an owned [`Router`] (for simple callers & tests).
    ///
    /// # Errors
    ///
    /// Returns [`MemberMeshError`] if subscribing or bootstrapping fails.
    pub async fn spawn_on_endpoint(
        endpoint: Endpoint,
        topic_id: TopicId,
        config: ProtocolConfig,
        metadata: MemberMetadata,
        bootstrap: Vec<EndpointAddr>,
    ) -> Result<Self, MemberMeshError> {
        let gossip = Gossip::builder().spawn(endpoint.clone());
        let (control_protocol, client, internal_tx, internal_rx) =
            build_control_wiring(&endpoint, &config);
        let router = Router::builder(endpoint.clone())
            .accept(iroh_gossip::ALPN, gossip.clone())
            .accept(SYNC_ALPN, control_protocol)
            .spawn();
        let spawned = spawn_actor(
            endpoint.clone(),
            &gossip,
            topic_id,
            config,
            metadata,
            bootstrap,
            client,
            internal_tx,
            internal_rx,
        )
        .await?;

        Ok(Self {
            endpoint,
            handle: spawned.handle,
            lookup: spawned.lookup,
            router: Some(Arc::new(router)),
            actor_task: Arc::new(spawned.task),
        })
    }

    /// Returns this node's [`EndpointId`].
    #[must_use]
    pub fn id(&self) -> EndpointId {
        self.endpoint.id()
    }

    /// Returns this node's current [`EndpointAddr`].
    #[must_use]
    pub fn addr(&self) -> EndpointAddr {
        self.endpoint.addr()
    }

    /// Returns a reference to the mesh's [`MemoryLookup`] address book.
    #[must_use]
    pub const fn address_lookup(&self) -> &MemoryLookup {
        &self.lookup
    }

    /// Joins additional bootstrap peers after startup.
    ///
    /// # Errors
    ///
    /// Returns [`MemberMeshError`] if the mesh actor is closed or gossip join fails.
    pub async fn join(
        &self,
        peers: impl IntoIterator<Item = EndpointAddr>,
    ) -> Result<(), MemberMeshError> {
        self.handle.join_peers(peers.into_iter().collect()).await
    }

    /// Returns all active (`Alive` or `Suspect`) members sorted deterministically by `(join_seq, peer_id)`.
    ///
    /// # Errors
    ///
    /// Returns [`MemberMeshError::Closed`] if the actor has shut down.
    pub async fn members(&self) -> Result<Vec<MemberView>, MemberMeshError> {
        self.handle.members().await
    }

    /// Returns the deterministic advisory leader (oldest active peer).
    ///
    /// # Errors
    ///
    /// Returns [`MemberMeshError::Closed`] if the actor has shut down.
    pub async fn leader(&self) -> Result<Option<MemberView>, MemberMeshError> {
        self.handle.leader().await
    }

    /// Returns the advisory leader only if at least `min_alive` peers are currently `Alive`.
    ///
    /// # Errors
    ///
    /// Returns [`MemberMeshError::Closed`] if the actor has shut down.
    pub async fn leader_with_quorum(
        &self,
        min_alive: usize,
    ) -> Result<Option<MemberView>, MemberMeshError> {
        self.handle.leader_with_quorum(min_alive).await
    }

    /// Returns join sequence and locally observed uptime for `peer_id`.
    ///
    /// # Errors
    ///
    /// Returns [`MemberMeshError::Closed`] if the actor has shut down.
    pub async fn peer_age(&self, peer_id: EndpointId) -> Result<Option<PeerAge>, MemberMeshError> {
        self.handle.peer_age(peer_id_from_endpoint(peer_id)).await
    }

    /// Updates local bounded metadata and broadcasts a new signed claim.
    ///
    /// # Errors
    ///
    /// Returns [`MemberMeshError`] if the actor has closed.
    pub async fn set_metadata(&self, metadata: MemberMetadata) -> Result<(), MemberMeshError> {
        self.handle.update_metadata(metadata).await
    }

    /// Subscribes to live [`MembershipEvent`] notifications.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<MembershipEvent> {
        self.handle.subscribe()
    }

    /// Gracefully leaves the cluster, broadcasting a signed `Left` claim before stopping.
    ///
    /// # Errors
    ///
    /// Returns [`MemberMeshError`] if the actor is already closed.
    pub async fn leave(&self) -> Result<(), MemberMeshError> {
        self.handle.leave().await
    }

    /// Shuts down the actor and (if standalone) its owned [`Router`] and [`Endpoint`].
    pub async fn shutdown(&self) {
        self.actor_task.abort();
        if let Some(router) = &self.router {
            let _ = router.shutdown().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::crate_version;

    #[test]
    fn reexported_version_matches() {
        assert_ne!(crate_version(), "");
    }
}
