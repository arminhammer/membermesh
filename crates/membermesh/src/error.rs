use std::fmt;

use membermesh_core::CoreError;
use thiserror::Error;

/// Boxed external error wrapper so `n0_error` types do not leak into public signatures.
#[derive(Debug, Error)]
#[error("{message}")]
pub struct ExternalError {
    message: String,
}

impl ExternalError {
    pub(crate) fn from_display(err: impl fmt::Display) -> Self {
        Self {
            message: err.to_string(),
        }
    }
}

/// Errors returned by `membermesh` networking, protocol, and lifecycle operations.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum MemberMeshError {
    /// Core protocol, signature, or wire framing error.
    #[error(transparent)]
    Core(#[from] CoreError),

    /// Failed to bind or configure the `iroh` QUIC endpoint.
    #[error("failed to bind iroh endpoint: {0}")]
    EndpointBind(#[source] ExternalError),

    /// Failed to convert between `PeerId` and `iroh::EndpointId`.
    #[error("invalid endpoint public key: {0}")]
    InvalidEndpointId(#[source] ExternalError),

    /// Underlying `iroh` QUIC connection or stream error.
    #[error("transport error: {0}")]
    Transport(#[source] ExternalError),

    /// Direct control or anti-entropy sync RPC timed out.
    #[error("control/sync RPC timed out")]
    RpcTimeout,

    /// Underlying gossip layer error.
    #[error("gossip error: {0}")]
    Gossip(#[source] ExternalError),

    /// Mesh actor or topic subscription has been closed.
    #[error("membermesh actor is closed")]
    Closed,

    /// Admission policy rejected a peer or claim.
    #[error("cluster admission denied for peer {peer_short}")]
    AdmissionDenied {
        /// Short peer identifier that was denied.
        peer_short: String,
    },
}
