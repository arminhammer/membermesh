#![deny(missing_docs)]
//! Pure Sans-I/O cluster membership, SWIM + Lifeguard failure detection,
//! and deterministic join-order advisory leader election.
//!
//! # Example
//!
//! ```
//! use membermesh_core::{
//!     ClaimSigner, ClaimVerifier, CoreError, LocalNodeInit, MemberMetadata,
//!     MembershipTable, PeerId, SessionId, SimInstant,
//! };
//!
//! struct DummySigner(PeerId);
//! impl ClaimSigner for DummySigner {
//!     fn peer_id(&self) -> PeerId { self.0 }
//!     fn sign(&self, _payload: &[u8]) -> [u8; 64] { [0u8; 64] }
//! }
//! struct DummyVerifier;
//! impl ClaimVerifier for DummyVerifier {
//!     fn verify(&self, _: PeerId, _: &[u8], _: &[u8; 64]) -> Result<(), CoreError> { Ok(()) }
//! }
//!
//! let signer = DummySigner(PeerId::from_bytes([1u8; 32]));
//! let table = MembershipTable::found(
//!     &signer,
//!     LocalNodeInit {
//!         session_id: SessionId(1),
//!         joined_at_unix_ms: 1_700_000_000_000,
//!         metadata: MemberMetadata::default(),
//!         addr_hint: None,
//!         admission_token: None,
//!     },
//!     SimInstant(0),
//! )
//! .expect("found cluster");
//! assert_eq!(table.leader().map(|m| m.join_seq), Some(1));
//! ```

mod engine;
mod error;
mod state;
mod swim;
mod types;
mod wire;

pub use engine::{Engine, EngineAction, EngineInput};
pub use error::CoreError;
pub use state::{
    ApplyOutcome, LocalNodeInit, MemberEntryState, MemberView, MembershipEvent, MembershipTable,
    PeerAge, SimInstant,
};
pub use swim::{
    ActiveSuspicion, LhaState, MAX_LHA_SCORE, ProtocolConfig, TransmitLimitedQueue, ceil_log10,
    compute_suspicion_timeout_ms, log2_fp1024,
};
pub use types::{
    AcceptAllPolicy, AddrHint, ClaimPolicy, ClaimSigner, ClaimVerifier, Incarnation,
    IndirectAlertPayload, IndirectKind, JoinOrder, MAX_ADDR_HINT_BYTES, MAX_METADATA_BYTES,
    MemberClaim, MemberMetadata, MemberStatus, PeerId, SessionId, SignedClaim, SignedIndirectAlert,
};
pub use wire::{
    ControlMessage, DEFAULT_MAX_CONTROL_BYTES, DEFAULT_MAX_GOSSIP_BYTES, GossipEnvelope,
    GossipItem, GossipMessage, SyncSnapshot, WIRE_VERSION, decode_control, encode_control,
};

/// Workspace scaffold version marker for initial smoke checks.
#[must_use]
pub const fn crate_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
mod tests {
    use super::crate_version;

    #[test]
    fn crate_version_is_non_empty() {
        assert_ne!(crate_version(), "");
    }
}
