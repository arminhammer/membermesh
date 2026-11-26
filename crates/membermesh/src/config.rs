use std::sync::Arc;

use iroh::{
    Endpoint, EndpointAddr, RelayMode, SecretKey,
    endpoint::presets::{Minimal, N0},
};
use iroh_gossip::TopicId;
use membermesh_core::{MemberMetadata, ProtocolConfig};

use crate::{AdmissionPolicy, MemberMesh, MemberMeshError, OpenAdmission, error::ExternalError};

/// Builder for spawning a standalone [`MemberMesh`] with its own `iroh` [`Endpoint`] and `Router`.
#[derive(Debug)]
pub struct MemberMeshBuilder {
    topic_id: TopicId,
    secret_key: Option<SecretKey>,
    config: ProtocolConfig,
    metadata: MemberMetadata,
    admission: Arc<dyn AdmissionPolicy>,
    bootstrap: Vec<EndpointAddr>,
    relay_mode: RelayMode,
    #[cfg(feature = "otel")]
    meter: Option<opentelemetry::metrics::Meter>,
}

impl MemberMeshBuilder {
    /// Creates a new builder for `topic_id` with LAN defaults and [`OpenAdmission`].
    #[must_use]
    pub fn new(topic_id: TopicId) -> Self {
        Self {
            topic_id,
            secret_key: None,
            config: ProtocolConfig::lan(),
            metadata: MemberMetadata::default(),
            admission: Arc::new(OpenAdmission),
            bootstrap: Vec::new(),
            relay_mode: RelayMode::Default,
            #[cfg(feature = "otel")]
            meter: None,
        }
    }

    /// Sets an explicit Ed25519 [`SecretKey`] instead of generating an ephemeral key.
    #[must_use]
    pub fn secret_key(mut self, secret_key: SecretKey) -> Self {
        self.secret_key = Some(secret_key);
        self
    }

    /// Configures SWIM + Lifeguard timing and gossip parameters.
    #[must_use]
    pub const fn config(mut self, config: ProtocolConfig) -> Self {
        self.config = config;
        self
    }

    /// Attaches bounded initial [`MemberMetadata`] to the local peer's claims.
    #[must_use]
    pub fn metadata(mut self, metadata: MemberMetadata) -> Self {
        self.metadata = metadata;
        self
    }

    /// Configures the cluster [`AdmissionPolicy`] enforced on all incoming claims and snapshots.
    #[must_use]
    pub fn admission(mut self, admission: Arc<dyn AdmissionPolicy>) -> Self {
        self.admission = admission;
        self
    }

    /// Adds initial bootstrap peer [`EndpointAddr`]s to contact and join.
    #[must_use]
    pub fn bootstrap(mut self, peers: impl IntoIterator<Item = EndpointAddr>) -> Self {
        self.bootstrap.extend(peers);
        self
    }

    /// Configures how `iroh` handles relay servers (e.g., `RelayMode::Disabled` for local/offline tests).
    #[must_use]
    pub fn relay_mode(mut self, relay_mode: RelayMode) -> Self {
        self.relay_mode = relay_mode;
        self
    }

    /// Configures a custom OpenTelemetry [`opentelemetry::metrics::Meter`] for this node.
    ///
    /// When omitted, `membermesh` uses `opentelemetry::global::meter(telemetry::METER_NAME)`.
    #[cfg(feature = "otel")]
    #[must_use]
    pub fn meter(mut self, meter: opentelemetry::metrics::Meter) -> Self {
        self.meter = Some(meter);
        self
    }

    /// Binds an `iroh` [`Endpoint`], spawns `Gossip` + `Router`, and starts the cluster actor.
    ///
    /// # Errors
    ///
    /// Returns [`MemberMeshError`] if endpoint binding or mesh startup fails.
    pub async fn spawn(self) -> Result<MemberMesh, MemberMeshError> {
        let mut builder = if matches!(self.relay_mode, RelayMode::Disabled) {
            Endpoint::builder(Minimal)
        } else {
            Endpoint::builder(N0)
        };
        builder = builder.relay_mode(self.relay_mode);
        if let Some(secret_key) = self.secret_key {
            builder = builder.secret_key(secret_key);
        }
        let endpoint = builder
            .bind()
            .await
            .map_err(|err| MemberMeshError::EndpointBind(ExternalError::from_display(err)))?;

        MemberMesh::spawn_on_endpoint_with_admission_owned(
            endpoint,
            self.topic_id,
            self.config,
            self.metadata,
            self.admission,
            self.bootstrap,
            true,
            #[cfg(feature = "otel")]
            self.meter,
        )
        .await
    }
}
