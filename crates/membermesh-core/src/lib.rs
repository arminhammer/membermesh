#![deny(missing_docs)]
//! Pure Sans-I/O cluster membership, SWIM + Lifeguard failure detection,
//! and deterministic join-order advisory leader election.

mod error;
mod state;
mod types;
mod wire;

pub use error::CoreError;
pub use state::{
    ApplyOutcome, LocalNodeInit, MemberEntryState, MemberView, MembershipEvent, MembershipTable,
    PeerAge, SimInstant,
};
pub use types::{
    AddrHint, ClaimSigner, ClaimVerifier, Incarnation, IndirectAlertPayload, IndirectKind,
    JoinOrder, MAX_ADDR_HINT_BYTES, MAX_METADATA_BYTES, MemberClaim, MemberMetadata, MemberStatus,
    PeerId, SessionId, SignedClaim, SignedIndirectAlert,
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
