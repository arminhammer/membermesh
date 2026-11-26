#![deny(missing_docs)]
//! Peer-to-peer cluster membership and advisory leader election over `iroh` and `iroh-gossip`.
//!
//! # Quickstart
//!
//! ```no_run
//! use iroh::RelayMode;
//! use iroh_gossip::TopicId;
//! use membermesh::MemberMesh;
//!
//! # async fn demo() -> Result<(), membermesh::MemberMeshError> {
//! let topic = TopicId::from_bytes([0x42; 32]);
//! let node1 = MemberMesh::builder(topic)
//!     .relay_mode(RelayMode::Disabled)
//!     .spawn()
//!     .await?;
//! let node2 = MemberMesh::builder(topic)
//!     .relay_mode(RelayMode::Disabled)
//!     .bootstrap([node1.addr()])
//!     .spawn()
//!     .await?;
//! println!("leader: {:?}", node2.leader().await?);
//! node2.leave().await?;
//! node2.shutdown().await;
//! node1.shutdown().await;
//! # Ok(())
//! # }
//! ```

mod actor;
mod admission;
mod config;
mod error;
mod events;
mod gossip;
mod signer;
mod sync;
pub mod telemetry;

use std::sync::Arc;

use iroh::{
    Endpoint, EndpointAddr, EndpointId, address_lookup::memory::MemoryLookup, protocol::Router,
};
use iroh_gossip::{Gossip, TopicId};
use membermesh_core::{MemberMetadata, MemberView, PeerAge, ProtocolConfig};

use crate::actor::{MeshActorHandle, build_control_wiring, spawn_actor};

pub use admission::{AdmissionPolicy, AllowListAdmission, OpenAdmission, SharedSecretAdmission};
pub use config::MemberMeshBuilder;
pub use error::{ExternalError, MemberMeshError};
pub use events::EventStream;
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
    admission: Arc<dyn AdmissionPolicy>,
    client: ControlClient,
    internal_tx: tokio::sync::mpsc::UnboundedSender<actor::InternalMsg>,
    internal_rx: tokio::sync::mpsc::UnboundedReceiver<actor::InternalMsg>,
    metrics: telemetry::MeshMetrics,
    #[cfg(feature = "otel")]
    meter: Option<opentelemetry::metrics::Meter>,
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
    /// Attaches a custom [`AdmissionPolicy`] before starting the cluster actor.
    #[must_use]
    pub fn with_admission(mut self, admission: Arc<dyn AdmissionPolicy>) -> Self {
        self.admission = admission;
        self
    }

    /// Attaches a custom OpenTelemetry [`opentelemetry::metrics::Meter`] before starting.
    #[cfg(feature = "otel")]
    #[must_use]
    pub fn with_meter(mut self, meter: opentelemetry::metrics::Meter) -> Self {
        self.meter = Some(meter);
        self
    }

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
            self.admission,
            bootstrap,
            self.client,
            self.internal_tx,
            self.internal_rx,
            self.metrics,
            #[cfg(feature = "otel")]
            self.meter,
        )
        .await?;

        Ok(MemberMesh {
            endpoint: self.endpoint,
            handle: spawned.handle,
            lookup: spawned.lookup,
            router: None,
            owns_endpoint: false,
            actor_task: Arc::new(spawned.task),
        })
    }
}

/// Live handle to a `membermesh` cluster participant running over `iroh` and `iroh-gossip`.
///
/// # Doc-test smoke check
///
/// ```
/// use std::sync::Arc;
/// use iroh_gossip::TopicId;
/// use membermesh::{MemberMesh, OpenAdmission, SharedSecretAdmission};
///
/// let topic = TopicId::from_bytes([7u8; 32]);
/// let _builder = MemberMesh::builder(topic)
///     .admission(Arc::new(SharedSecretAdmission::from_passphrase("cluster-secret")));
/// let _open = Arc::new(OpenAdmission);
/// ```
#[derive(Clone, Debug)]
pub struct MemberMesh {
    endpoint: Endpoint,
    handle: MeshActorHandle,
    lookup: MemoryLookup,
    router: Option<Arc<Router>>,
    owns_endpoint: bool,
    actor_task: Arc<tokio::task::JoinHandle<()>>,
}

impl MemberMesh {
    /// Returns a [`MemberMeshBuilder`] for spawning a standalone [`MemberMesh`] node.
    #[must_use]
    pub fn builder(topic_id: TopicId) -> MemberMeshBuilder {
        MemberMeshBuilder::new(topic_id)
    }

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
        let metrics = telemetry::MeshMetrics::lazy();
        let (control_protocol, client, internal_tx, internal_rx) =
            build_control_wiring(&endpoint, &config, &metrics);
        let prepared = PreparedMemberMesh {
            endpoint,
            gossip,
            topic_id,
            config,
            metadata,
            admission: Arc::new(OpenAdmission),
            client,
            internal_tx,
            internal_rx,
            metrics,
            #[cfg(feature = "otel")]
            meter: None,
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
        let metrics = telemetry::MeshMetrics::lazy();
        let (control_protocol, client, internal_tx, internal_rx) =
            build_control_wiring(&endpoint, &config, &metrics);
        let spawned = spawn_actor(
            endpoint.clone(),
            gossip,
            topic_id,
            config,
            metadata,
            Arc::new(OpenAdmission),
            bootstrap,
            client,
            internal_tx,
            internal_rx,
            metrics,
            #[cfg(feature = "otel")]
            None,
        )
        .await?;

        Ok((
            Self {
                endpoint,
                handle: spawned.handle,
                lookup: spawned.lookup,
                router: None,
                owns_endpoint: false,
                actor_task: Arc::new(spawned.task),
            },
            control_protocol,
        ))
    }

    /// Spawns a standalone [`MemberMesh`] on an existing [`Endpoint`] with an owned [`Router`].
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
        Self::spawn_on_endpoint_with_admission_owned(
            endpoint,
            topic_id,
            config,
            metadata,
            Arc::new(OpenAdmission),
            bootstrap,
            false,
            #[cfg(feature = "otel")]
            None,
        )
        .await
    }

    /// Spawns a standalone [`MemberMesh`] on an existing [`Endpoint`] with an [`AdmissionPolicy`].
    ///
    /// # Errors
    ///
    /// Returns [`MemberMeshError`] if subscribing or bootstrapping fails.
    pub async fn spawn_on_endpoint_with_admission(
        endpoint: Endpoint,
        topic_id: TopicId,
        config: ProtocolConfig,
        metadata: MemberMetadata,
        admission: Arc<dyn AdmissionPolicy>,
        bootstrap: Vec<EndpointAddr>,
    ) -> Result<Self, MemberMeshError> {
        Self::spawn_on_endpoint_with_admission_owned(
            endpoint,
            topic_id,
            config,
            metadata,
            admission,
            bootstrap,
            false,
            #[cfg(feature = "otel")]
            None,
        )
        .await
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "internal constructor wiring endpoint ownership, admission policy, and optional OTel meter"
    )]
    pub(crate) async fn spawn_on_endpoint_with_admission_owned(
        endpoint: Endpoint,
        topic_id: TopicId,
        config: ProtocolConfig,
        metadata: MemberMetadata,
        admission: Arc<dyn AdmissionPolicy>,
        bootstrap: Vec<EndpointAddr>,
        owns_endpoint: bool,
        #[cfg(feature = "otel")] meter: Option<opentelemetry::metrics::Meter>,
    ) -> Result<Self, MemberMeshError> {
        let gossip = Gossip::builder().spawn(endpoint.clone());
        let metrics = telemetry::MeshMetrics::lazy();
        let (control_protocol, client, internal_tx, internal_rx) =
            build_control_wiring(&endpoint, &config, &metrics);
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
            admission,
            bootstrap,
            client,
            internal_tx,
            internal_rx,
            metrics,
            #[cfg(feature = "otel")]
            meter,
        )
        .await?;

        Ok(Self {
            endpoint,
            handle: spawned.handle,
            lookup: spawned.lookup,
            router: Some(Arc::new(router)),
            owns_endpoint,
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

    /// Subscribes to a typed [`EventStream`] of [`membermesh_core::MembershipEvent`] notifications.
    #[must_use]
    pub fn subscribe(&self) -> EventStream {
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
        if self.owns_endpoint {
            self.endpoint.close().await;
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
