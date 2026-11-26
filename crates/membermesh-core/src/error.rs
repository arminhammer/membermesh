use thiserror::Error;

/// Errors returned by `membermesh-core` types, wire codecs, and state-machine operations.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum CoreError {
    /// Member metadata exceeded [`crate::MAX_METADATA_BYTES`].
    #[error("member metadata size {size} exceeds maximum {max} bytes")]
    MetadataTooLarge {
        /// Actual byte length provided.
        size: usize,
        /// Maximum allowed byte length.
        max: usize,
    },

    /// Address hint exceeded [`crate::MAX_ADDR_HINT_BYTES`].
    #[error("address hint size {size} exceeds maximum {max} bytes")]
    AddrHintTooLarge {
        /// Actual byte length provided.
        size: usize,
        /// Maximum allowed byte length.
        max: usize,
    },

    /// Encoded or received wire frame exceeded the configured limit.
    #[error("wire frame size {size} exceeds maximum {max} bytes")]
    FrameTooLarge {
        /// Actual byte length.
        size: usize,
        /// Maximum allowed byte length.
        max: usize,
    },

    /// Wire envelope carried an unsupported protocol version.
    #[error("unsupported wire protocol version {version}")]
    UnsupportedVersion {
        /// Version byte read from the wire.
        version: u8,
    },

    /// Wire encoding failed.
    #[error("wire encoding failed: {detail}")]
    Encode {
        /// Serialization error detail.
        detail: String,
    },

    /// Wire decoding failed.
    #[error("wire decoding failed: {detail}")]
    Decode {
        /// Deserialization error detail.
        detail: String,
    },

    /// Cryptographic signature verification failed.
    #[error("invalid signature for peer {peer_short}")]
    InvalidSignature {
        /// Short identifier of the expected signing peer.
        peer_short: String,
    },

    /// Claim subject did not match the signing peer identity.
    #[error("claim subject {subject} does not match signer {signer}")]
    SubjectMismatch {
        /// Subject recorded in the claim.
        subject: String,
        /// Identity of the signer.
        signer: String,
    },

    /// Protocol configuration violated an invariant.
    #[error("invalid protocol configuration: {reason}")]
    InvalidConfig {
        /// Description of the invalid configuration setting.
        reason: String,
    },
}
