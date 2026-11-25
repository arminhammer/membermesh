use iroh::{EndpointId, SecretKey, Signature};
use membermesh_core::{ClaimSigner, ClaimVerifier, CoreError, PeerId};

use crate::{MemberMeshError, error::ExternalError};

/// Converts an `iroh` [`EndpointId`] into a `membermesh-core` [`PeerId`].
#[must_use]
pub fn peer_id_from_endpoint(endpoint_id: EndpointId) -> PeerId {
    PeerId::from_bytes(*endpoint_id.as_bytes())
}

/// Converts a `membermesh-core` [`PeerId`] back into an `iroh` [`EndpointId`].
///
/// # Errors
///
/// Returns [`MemberMeshError::InvalidEndpointId`] if the 32 bytes are not a valid Ed25519 point.
pub fn endpoint_id_from_peer(peer_id: PeerId) -> Result<EndpointId, MemberMeshError> {
    EndpointId::from_bytes(peer_id.as_bytes())
        .map_err(|err| MemberMeshError::InvalidEndpointId(ExternalError::from_display(err)))
}

/// [`ClaimSigner`] backed by an `iroh` [`SecretKey`].
#[derive(Clone, Debug)]
pub struct IrohSigner {
    secret_key: SecretKey,
    peer_id: PeerId,
}

impl IrohSigner {
    /// Wraps an `iroh` [`SecretKey`] for signing membership claims and indirect alerts.
    #[must_use]
    pub fn new(secret_key: SecretKey) -> Self {
        let peer_id = peer_id_from_endpoint(secret_key.public());
        Self {
            secret_key,
            peer_id,
        }
    }

    /// Returns the `iroh` [`EndpointId`] of this signer.
    #[must_use]
    pub fn endpoint_id(&self) -> EndpointId {
        self.secret_key.public()
    }
}

impl ClaimSigner for IrohSigner {
    fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    fn sign(&self, payload: &[u8]) -> [u8; 64] {
        self.secret_key.sign(payload).to_bytes()
    }
}

/// [`ClaimVerifier`] verifying Ed25519 signatures using `iroh` [`EndpointId`] keys.
#[derive(Clone, Copy, Debug, Default)]
pub struct IrohVerifier;

impl ClaimVerifier for IrohVerifier {
    fn verify(
        &self,
        peer_id: PeerId,
        payload: &[u8],
        signature: &[u8; 64],
    ) -> Result<(), CoreError> {
        let endpoint_id = EndpointId::from_bytes(peer_id.as_bytes()).map_err(|_| {
            CoreError::InvalidSignature {
                peer_short: peer_id.fmt_short(),
            }
        })?;
        let sig = Signature::from_bytes(signature);
        endpoint_id
            .verify(payload, &sig)
            .map_err(|_| CoreError::InvalidSignature {
                peer_short: peer_id.fmt_short(),
            })
    }
}
