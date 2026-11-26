#![deny(missing_docs)]
//! Peer-to-peer cluster membership and advisory leader election over `iroh` and `iroh-gossip`.

mod error;
mod signer;
mod sync;

pub use error::{ExternalError, MemberMeshError};
pub use membermesh_core::crate_version;
pub use signer::{IrohSigner, IrohVerifier, endpoint_id_from_peer, peer_id_from_endpoint};
pub use sync::{ControlClient, ControlFuture, ControlHandler, ControlProtocol, SYNC_ALPN};

#[cfg(test)]
mod tests {
    use super::crate_version;

    #[test]
    fn reexported_version_matches() {
        assert_ne!(crate_version(), "");
    }
}
