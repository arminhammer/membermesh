use std::fmt;

use serde::{Deserialize, Serialize};

use crate::CoreError;

/// Maximum allowed byte length for [`MemberMetadata`].
///
/// Kept small so signed claims fit comfortably within `iroh-gossip`'s
/// 4096-byte default maximum message size.
pub const MAX_METADATA_BYTES: usize = 512;

/// Maximum allowed byte length for [`AddrHint`].
pub const MAX_ADDR_HINT_BYTES: usize = 256;

const CLAIM_SIGNING_DOMAIN: &[u8] = b"membermesh-claim-v1";
const ALERT_SIGNING_DOMAIN: &[u8] = b"membermesh-alert-v1";

/// Cryptographic 32-byte peer identifier (matches an Ed25519 public key / `iroh::EndpointId`).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PeerId([u8; 32]);

impl PeerId {
    /// Creates a [`PeerId`] from a raw 32-byte Ed25519 public key.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the underlying 32-byte array.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Returns the first 5 bytes formatted as lowercase hex for concise logs and diagnostics.
    #[must_use]
    pub fn fmt_short(&self) -> String {
        encode_hex(&self.0[..5])
    }
}

impl From<[u8; 32]> for PeerId {
    fn from(bytes: [u8; 32]) -> Self {
        Self::from_bytes(bytes)
    }
}

impl AsRef<[u8; 32]> for PeerId {
    fn as_ref(&self) -> &[u8; 32] {
        self.as_bytes()
    }
}

impl fmt::Debug for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PeerId({})", self.fmt_short())
    }
}

impl fmt::Display for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&encode_hex(&self.0))
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        let hi = usize::from(byte >> 4);
        let lo = usize::from(byte & 0x0f);
        if let (Some(&h), Some(&l)) = (HEX.get(hi), HEX.get(lo)) {
            out.push(char::from(h));
            out.push(char::from(l));
        }
    }
    out
}

/// Ephemeral per-process boot identifier.
///
/// Same `SessionId` across a network partition allows refuting `Dead` while
/// preserving `join_seq`; a process restart generates a fresh `SessionId`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct SessionId(pub u64);

impl fmt::Display for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

/// Monotonic incarnation counter used by a peer to refute false `Suspect` or `Dead` claims.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct Incarnation(pub u64);

impl Incarnation {
    /// Returns the next incarnation counter, saturating at `u64::MAX`.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

impl fmt::Display for Incarnation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Deterministic total order key for cluster membership and advisory leader election.
///
/// Ordered first by Lamport `join_seq` (lowest = oldest), breaking ties by `peer_id`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct JoinOrder {
    /// Lamport join sequence assigned when the peer joined the cluster.
    pub join_seq: u64,
    /// Peer identity used as deterministic tie-breaker.
    pub peer_id: PeerId,
}

/// Lifecycle status of a cluster member.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub enum MemberStatus {
    /// Member is healthy and participating in the mesh.
    Alive,
    /// Member missed probes and is currently under Lifeguard suspicion.
    Suspect,
    /// Member failed suspicion timeout and is considered unreachable/dead.
    Dead,
    /// Member gracefully departed the cluster.
    Left,
}

impl fmt::Display for MemberStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::Alive => "Alive",
            Self::Suspect => "Suspect",
            Self::Dead => "Dead",
            Self::Left => "Left",
        };
        f.write_str(label)
    }
}

/// Bounded opaque application metadata attached to a member claim (max 512 bytes).
#[derive(Clone, PartialEq, Eq, Hash, Default, Debug, Serialize)]
pub struct MemberMetadata(Vec<u8>);

impl MemberMetadata {
    /// Validates and constructs [`MemberMetadata`].
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::MetadataTooLarge`] if `bytes.len() > MAX_METADATA_BYTES`.
    pub fn try_new(bytes: impl Into<Vec<u8>>) -> Result<Self, CoreError> {
        let bytes = bytes.into();
        if bytes.len() > MAX_METADATA_BYTES {
            return Err(CoreError::MetadataTooLarge {
                size: bytes.len(),
                max: MAX_METADATA_BYTES,
            });
        }
        Ok(Self(bytes))
    }

    /// Returns the raw metadata bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl<'de> Deserialize<'de> for MemberMetadata {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = Vec::<u8>::deserialize(deserializer)?;
        Self::try_new(raw).map_err(serde::de::Error::custom)
    }
}

/// Bounded opaque endpoint addressing hint propagated inside claims (max 256 bytes).
#[derive(Clone, PartialEq, Eq, Hash, Default, Debug, Serialize)]
pub struct AddrHint(Vec<u8>);

impl AddrHint {
    /// Validates and constructs an [`AddrHint`].
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::AddrHintTooLarge`] if `bytes.len() > MAX_ADDR_HINT_BYTES`.
    pub fn try_new(bytes: impl Into<Vec<u8>>) -> Result<Self, CoreError> {
        let bytes = bytes.into();
        if bytes.len() > MAX_ADDR_HINT_BYTES {
            return Err(CoreError::AddrHintTooLarge {
                size: bytes.len(),
                max: MAX_ADDR_HINT_BYTES,
            });
        }
        Ok(Self(bytes))
    }

    /// Returns the raw hint bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl<'de> Deserialize<'de> for AddrHint {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = Vec::<u8>::deserialize(deserializer)?;
        Self::try_new(raw).map_err(serde::de::Error::custom)
    }
}

/// Self-asserted membership claim signed by `subject`.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct MemberClaim {
    /// Peer described and signed by this claim.
    pub subject: PeerId,
    /// Ephemeral process session identifier.
    pub session_id: SessionId,
    /// Monotonic incarnation number for refuting false suspicions.
    pub incarnation: Incarnation,
    /// Immutable Lamport join sequence for deterministic ordering.
    pub join_seq: u64,
    /// Informational wall-clock join timestamp (milliseconds since Unix epoch).
    /// Never used for ordering or leader selection.
    pub joined_at_unix_ms: u64,
    /// Asserted lifecycle status (`Alive` or `Left` for self-signed claims).
    pub status: MemberStatus,
    /// Bounded application-defined metadata.
    pub metadata: MemberMetadata,
    /// Optional bounded addressing hint for non-neighbor direct dials.
    pub addr_hint: Option<AddrHint>,
}

/// Cryptographically signed [`MemberClaim`].
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct SignedClaim {
    /// The underlying claim.
    pub claim: MemberClaim,
    /// Ed25519 signature over domain-separated canonical bytes of `claim`.
    #[serde(with = "sig_bytes")]
    pub signature: [u8; 64],
}

impl SignedClaim {
    /// Signs `claim` using `signer`, verifying `claim.subject == signer.peer_id()`.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::SubjectMismatch`] if `claim.subject != signer.peer_id()`,
    /// or [`CoreError::Encode`] if canonical serialization fails.
    pub fn sign(claim: MemberClaim, signer: &impl ClaimSigner) -> Result<Self, CoreError> {
        let signer_id = signer.peer_id();
        if claim.subject != signer_id {
            return Err(CoreError::SubjectMismatch {
                subject: claim.subject.fmt_short(),
                signer: signer_id.fmt_short(),
            });
        }
        let signable = canonical_signable_bytes(CLAIM_SIGNING_DOMAIN, &claim)?;
        let signature = signer.sign(&signable);
        Ok(Self { claim, signature })
    }

    /// Verifies the Ed25519 signature against `self.claim.subject`.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidSignature`] if the signature is invalid, or
    /// [`CoreError::Encode`] if canonical serialization fails.
    pub fn verify(&self, verifier: &impl ClaimVerifier) -> Result<(), CoreError> {
        let signable = canonical_signable_bytes(CLAIM_SIGNING_DOMAIN, &self.claim)?;
        verifier.verify(self.claim.subject, &signable, &self.signature)
    }
}

/// Kind of indirect failure-detection assertion reported by a peer.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub enum IndirectKind {
    /// Target missed probes and is suspected unreachable.
    Suspect,
    /// Suspicion timer expired and target is declared dead.
    Dead,
}

/// Unsigned payload of a peer-reported suspicion or death assertion.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct IndirectAlertPayload {
    /// Target peer being reported.
    pub subject: PeerId,
    /// Target peer's session ID when last observed alive.
    pub session_id: SessionId,
    /// Target peer's incarnation being asserted against.
    pub incarnation: Incarnation,
    /// Target peer's join sequence.
    pub join_seq: u64,
    /// Whether this asserts `Suspect` or `Dead`.
    pub kind: IndirectKind,
    /// Reporting peer identity.
    pub reporter: PeerId,
}

/// Signed peer-reported `Suspect` or `Dead` assertion.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct SignedIndirectAlert {
    /// The indirect alert payload.
    pub payload: IndirectAlertPayload,
    /// Ed25519 signature by `payload.reporter`.
    #[serde(with = "sig_bytes")]
    pub signature: [u8; 64],
}

impl SignedIndirectAlert {
    /// Signs an indirect `Suspect` or `Dead` alert with `signer`.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::SubjectMismatch`] if `payload.reporter != signer.peer_id()`,
    /// or [`CoreError::Encode`] if canonical serialization fails.
    pub fn sign(
        payload: IndirectAlertPayload,
        signer: &impl ClaimSigner,
    ) -> Result<Self, CoreError> {
        let signer_id = signer.peer_id();
        if payload.reporter != signer_id {
            return Err(CoreError::SubjectMismatch {
                subject: payload.reporter.fmt_short(),
                signer: signer_id.fmt_short(),
            });
        }
        let signable = canonical_signable_bytes(ALERT_SIGNING_DOMAIN, &payload)?;
        let signature = signer.sign(&signable);
        Ok(Self { payload, signature })
    }

    /// Verifies the reporter's signature on this indirect alert.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidSignature`] if verification fails.
    pub fn verify(&self, verifier: &impl ClaimVerifier) -> Result<(), CoreError> {
        let signable = canonical_signable_bytes(ALERT_SIGNING_DOMAIN, &self.payload)?;
        verifier.verify(self.payload.reporter, &signable, &self.signature)
    }
}

/// Abstraction for signing membership claims and indirect alerts.
pub trait ClaimSigner: Send + Sync {
    /// Returns the public [`PeerId`] corresponding to this signer's private key.
    fn peer_id(&self) -> PeerId;

    /// Produces a 64-byte Ed25519 signature over `payload`.
    fn sign(&self, payload: &[u8]) -> [u8; 64];
}

/// Abstraction for verifying Ed25519 signatures on membership claims and alerts.
pub trait ClaimVerifier: Send + Sync {
    /// Verifies that `signature` is a valid signature by `peer_id` over `payload`.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidSignature`] when verification fails.
    fn verify(
        &self,
        peer_id: PeerId,
        payload: &[u8],
        signature: &[u8; 64],
    ) -> Result<(), CoreError>;
}

fn canonical_signable_bytes<T: Serialize>(domain: &[u8], value: &T) -> Result<Vec<u8>, CoreError> {
    let encoded = postcard::to_allocvec(value).map_err(|err| CoreError::Encode {
        detail: err.to_string(),
    })?;
    let mut out = Vec::with_capacity(domain.len() + encoded.len());
    out.extend_from_slice(domain);
    out.extend_from_slice(&encoded);
    Ok(out)
}

mod sig_bytes {
    use serde::{Deserializer, Serializer};

    pub fn serialize<S>(sig: &[u8; 64], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(sig)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<[u8; 64], D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SigVisitor;

        impl serde::de::Visitor<'_> for SigVisitor {
            type Value = [u8; 64];

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a 64-byte Ed25519 signature")
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                v.try_into()
                    .map_err(|_| E::invalid_length(v.len(), &"64 bytes"))
            }
        }

        deserializer.deserialize_bytes(SigVisitor)
    }
}
