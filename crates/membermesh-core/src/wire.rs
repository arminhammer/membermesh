use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::{CoreError, PeerId, SignedClaim, SignedIndirectAlert};

/// Current wire framing protocol version.
pub const WIRE_VERSION: u8 = 1;

/// Default maximum encoded gossip envelope size in bytes.
///
/// Sized safely below `iroh-gossip`'s default 4096-byte encoded message limit
/// (which leaves roughly 4056 bytes for application payloads).
pub const DEFAULT_MAX_GOSSIP_BYTES: usize = 3900;

/// Default maximum encoded control/sync RPC frame size in bytes (1 MiB).
pub const DEFAULT_MAX_CONTROL_BYTES: usize = 1024 * 1024;

/// A single membership state update carried inside a [`GossipMessage`].
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub enum GossipItem {
    /// Self-signed `Alive` or `Left` membership claim.
    Claim(SignedClaim),
    /// Peer-signed `Suspect` or `Dead` failure assertion.
    IndirectAlert(SignedIndirectAlert),
}

/// Payload carried inside a [`GossipEnvelope`].
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub enum GossipMessage {
    /// A single membership update.
    Single(GossipItem),
    /// A bounded batch of membership updates piggybacked together.
    Compound(Vec<GossipItem>),
}

/// Versioned outer gossip envelope.
///
/// Every broadcast includes a monotonic `nonce` so identical membership updates
/// produce distinct wire bytes and are not dropped by `iroh-gossip`'s 90-second
/// content-hash deduplication cache.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct GossipEnvelope {
    /// Wire protocol version ([`WIRE_VERSION`]).
    pub version: u8,
    /// Monotonic sender-local nonce preventing content-hash dedup collisions.
    pub nonce: u64,
    /// Sender peer identity.
    pub sender: PeerId,
    /// Gossip message payload.
    pub message: GossipMessage,
}

impl GossipEnvelope {
    /// Creates a new version-1 [`GossipEnvelope`].
    #[must_use]
    pub const fn new(nonce: u64, sender: PeerId, message: GossipMessage) -> Self {
        Self {
            version: WIRE_VERSION,
            nonce,
            sender,
            message,
        }
    }

    /// Encodes the envelope using `postcard`, rejecting frames larger than `max_bytes`.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::Encode`] on serialization failure or
    /// [`CoreError::FrameTooLarge`] if the encoded frame exceeds `max_bytes`.
    pub fn encode(&self, max_bytes: usize) -> Result<Bytes, CoreError> {
        let encoded = postcard::to_allocvec(self).map_err(|err| CoreError::Encode {
            detail: err.to_string(),
        })?;
        if encoded.len() > max_bytes {
            return Err(CoreError::FrameTooLarge {
                size: encoded.len(),
                max: max_bytes,
            });
        }
        Ok(Bytes::from(encoded))
    }

    /// Decodes a [`GossipEnvelope`] from `bytes`, enforcing `max_bytes` and [`WIRE_VERSION`].
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::FrameTooLarge`], [`CoreError::UnsupportedVersion`],
    /// or [`CoreError::Decode`] on invalid inputs.
    pub fn decode(bytes: &[u8], max_bytes: usize) -> Result<Self, CoreError> {
        if bytes.len() > max_bytes {
            return Err(CoreError::FrameTooLarge {
                size: bytes.len(),
                max: max_bytes,
            });
        }
        let (&version, _) = bytes.split_first().ok_or_else(|| CoreError::Decode {
            detail: "empty gossip envelope".to_owned(),
        })?;
        if version != WIRE_VERSION {
            return Err(CoreError::UnsupportedVersion { version });
        }
        let envelope: Self = postcard::from_bytes(bytes).map_err(|err| CoreError::Decode {
            detail: err.to_string(),
        })?;
        if envelope.version != WIRE_VERSION {
            return Err(CoreError::UnsupportedVersion {
                version: envelope.version,
            });
        }
        Ok(envelope)
    }
}

/// Full-state snapshot exchanged during Push/Pull anti-entropy synchronization.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct SyncSnapshot {
    /// Sender peer identity.
    pub sender: PeerId,
    /// Highest Lamport join sequence known to the sender.
    pub lamport_clock: u64,
    /// All known self-signed claims (alive, suspected, or tombstoned).
    pub claims: Vec<SignedClaim>,
    /// Active indirect suspicion/death alerts.
    pub indirect_alerts: Vec<SignedIndirectAlert>,
}

/// Direct QUIC control and anti-entropy sync RPC messages sent over `/membermesh/1`.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ControlMessage {
    /// Direct SWIM liveness probe.
    Ping {
        /// Probe sequence number.
        seq: u64,
        /// Probing peer.
        from: PeerId,
    },
    /// Indirect SWIM probe request asking an intermediary to ping `target`.
    PingReq {
        /// Probe sequence number.
        seq: u64,
        /// Original probing peer.
        from: PeerId,
        /// Target peer to probe.
        target: PeerId,
    },
    /// Positive SWIM probe acknowledgment.
    Ack {
        /// Matching probe sequence number.
        seq: u64,
        /// Responding peer.
        from: PeerId,
    },
    /// Negative SWIM indirect probe acknowledgment (intermediary could not reach target).
    Nack {
        /// Matching probe sequence number.
        seq: u64,
        /// Intermediary peer.
        from: PeerId,
    },
    /// Bidirectional Push/Pull anti-entropy membership state snapshot.
    PushPullSync(Box<SyncSnapshot>),
}

/// Encodes a [`ControlMessage`] into bounded `postcard` bytes.
///
/// # Errors
///
/// Returns [`CoreError::Encode`] or [`CoreError::FrameTooLarge`].
pub fn encode_control(msg: &ControlMessage, max_bytes: usize) -> Result<Bytes, CoreError> {
    let mut out = Vec::with_capacity(64);
    out.push(WIRE_VERSION);
    let payload = postcard::to_allocvec(msg).map_err(|err| CoreError::Encode {
        detail: err.to_string(),
    })?;
    out.extend_from_slice(&payload);
    if out.len() > max_bytes {
        return Err(CoreError::FrameTooLarge {
            size: out.len(),
            max: max_bytes,
        });
    }
    Ok(Bytes::from(out))
}

/// Decodes a [`ControlMessage`] from bounded `postcard` bytes.
///
/// # Errors
///
/// Returns [`CoreError::FrameTooLarge`], [`CoreError::UnsupportedVersion`],
/// or [`CoreError::Decode`].
pub fn decode_control(bytes: &[u8], max_bytes: usize) -> Result<ControlMessage, CoreError> {
    if bytes.len() > max_bytes {
        return Err(CoreError::FrameTooLarge {
            size: bytes.len(),
            max: max_bytes,
        });
    }
    let (&version, rest) = bytes.split_first().ok_or_else(|| CoreError::Decode {
        detail: "empty control frame".to_owned(),
    })?;
    if version != WIRE_VERSION {
        return Err(CoreError::UnsupportedVersion { version });
    }
    postcard::from_bytes(rest).map_err(|err| CoreError::Decode {
        detail: err.to_string(),
    })
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::{Incarnation, MemberClaim, MemberMetadata, MemberStatus, SessionId};

    proptest! {
        #[test]
        fn decode_gossip_never_panics_on_arbitrary_bytes(data in prop::collection::vec(any::<u8>(), 0..8192)) {
            let _ = GossipEnvelope::decode(&data, DEFAULT_MAX_GOSSIP_BYTES);
        }

        #[test]
        fn decode_control_never_panics_on_arbitrary_bytes(data in prop::collection::vec(any::<u8>(), 0..8192)) {
            let _ = decode_control(&data, DEFAULT_MAX_CONTROL_BYTES);
        }

        #[test]
        fn valid_control_ping_ack_roundtrips(seq in any::<u64>(), peer_bytes in any::<[u8; 32]>()) {
            let peer = PeerId::from_bytes(peer_bytes);
            let ping = ControlMessage::Ping { seq, from: peer };
            let encoded = encode_control(&ping, DEFAULT_MAX_CONTROL_BYTES).expect("encode ping");
            let decoded = decode_control(&encoded, DEFAULT_MAX_CONTROL_BYTES).expect("decode ping");
            prop_assert_eq!(decoded, ping);
        }

        #[test]
        fn metadata_bounds_are_enforced(len in 0usize..1024usize) {
            let bytes = vec![0xAB; len];
            let res = MemberMetadata::try_new(bytes);
            if len <= crate::MAX_METADATA_BYTES {
                prop_assert!(res.is_ok());
            } else {
                let is_too_large = matches!(res, Err(CoreError::MetadataTooLarge { .. }));
                prop_assert!(is_too_large);
            }
        }
    }

    #[test]
    fn sample_claim_encode_respects_bounds() {
        let claim = MemberClaim {
            subject: PeerId::from_bytes([1u8; 32]),
            session_id: SessionId(7),
            incarnation: Incarnation(1),
            join_seq: 1,
            joined_at_unix_ms: 1_700_000_000_000,
            status: MemberStatus::Alive,
            metadata: MemberMetadata::default(),
            addr_hint: None,
        };
        let signed = SignedClaim {
            claim,
            signature: [0u8; 64],
        };
        let env = GossipEnvelope::new(
            1,
            PeerId::from_bytes([1u8; 32]),
            GossipMessage::Single(GossipItem::Claim(signed)),
        );
        let bytes = env.encode(DEFAULT_MAX_GOSSIP_BYTES).expect("encode");
        let decoded = GossipEnvelope::decode(&bytes, DEFAULT_MAX_GOSSIP_BYTES).expect("decode");
        assert_eq!(decoded, env);
    }
}

#[cfg(kani)]
mod kani_proofs {
    use super::*;
    use crate::MemberMetadata;

    #[kani::proof]
    fn verify_gossip_decode_panic_free() {
        let len: usize = kani::any();
        kani::assume(len <= 128);
        let mut buf = [0u8; 128];
        for b in &mut buf {
            *b = kani::any();
        }
        if let Some(slice) = buf.get(..len) {
            let _ = GossipEnvelope::decode(slice, DEFAULT_MAX_GOSSIP_BYTES);
        }
    }

    #[kani::proof]
    fn verify_control_decode_panic_free() {
        let len: usize = kani::any();
        kani::assume(len <= 128);
        let mut buf = [0u8; 128];
        for b in &mut buf {
            *b = kani::any();
        }
        if let Some(slice) = buf.get(..len) {
            let _ = decode_control(slice, DEFAULT_MAX_CONTROL_BYTES);
        }
    }

    #[kani::proof]
    fn verify_metadata_bound() {
        let len: usize = kani::any();
        kani::assume(len <= 1024);
        let bytes = vec![0u8; len];
        let res = MemberMetadata::try_new(bytes);
        assert_eq!(res.is_ok(), len <= crate::MAX_METADATA_BYTES);
    }
}
