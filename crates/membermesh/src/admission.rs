use std::{collections::BTreeSet, fmt, sync::Arc};

use iroh::EndpointId;
use iroh_gossip::TopicId;
use membermesh_core::{ClaimPolicy, CoreError, MemberClaim, SessionId};

use crate::{MemberMeshError, signer::endpoint_id_from_peer};

const ADMISSION_DOMAIN: &[u8] = b"membermesh-admission-v1";

/// Cluster admission policy controlling which peers may join a `membermesh` topic.
pub trait AdmissionPolicy: Send + Sync + fmt::Debug {
    /// Computes the optional 32-byte admission credential embedded in this node's local [`MemberClaim`].
    fn local_token(
        &self,
        _topic_id: TopicId,
        _local_id: EndpointId,
        _session_id: SessionId,
    ) -> Option<[u8; 32]> {
        None
    }

    /// Validates whether a remote `claim` is authorized to enter the topic.
    ///
    /// # Errors
    ///
    /// Returns [`MemberMeshError::AdmissionDenied`] if the peer or credential is rejected.
    fn verify_claim(&self, topic_id: TopicId, claim: &MemberClaim) -> Result<(), MemberMeshError>;
}

/// Default permissive [`AdmissionPolicy`] allowing any valid Ed25519 peer into the cluster.
#[derive(Clone, Copy, Default, Debug)]
pub struct OpenAdmission;

impl AdmissionPolicy for OpenAdmission {
    fn verify_claim(
        &self,
        _topic_id: TopicId,
        _claim: &MemberClaim,
    ) -> Result<(), MemberMeshError> {
        Ok(())
    }
}

/// Allow-list [`AdmissionPolicy`] admitting only an explicit set of [`EndpointId`]s.
#[derive(Clone, Debug, Default)]
pub struct AllowListAdmission {
    allowed: BTreeSet<EndpointId>,
}

impl AllowListAdmission {
    /// Constructs an [`AllowListAdmission`] permitting every [`EndpointId`] in `allowed`.
    #[must_use]
    pub fn new(allowed: impl IntoIterator<Item = EndpointId>) -> Self {
        Self {
            allowed: allowed.into_iter().collect(),
        }
    }

    /// Returns `true` if `endpoint_id` is in the allow-list.
    #[must_use]
    pub fn contains(&self, endpoint_id: &EndpointId) -> bool {
        self.allowed.contains(endpoint_id)
    }
}

impl AdmissionPolicy for AllowListAdmission {
    fn verify_claim(&self, _topic_id: TopicId, claim: &MemberClaim) -> Result<(), MemberMeshError> {
        let Ok(endpoint_id) = endpoint_id_from_peer(claim.subject) else {
            return Err(MemberMeshError::AdmissionDenied {
                peer_short: claim.subject.fmt_short(),
            });
        };
        if self.allowed.contains(&endpoint_id) {
            Ok(())
        } else {
            Err(MemberMeshError::AdmissionDenied {
                peer_short: claim.subject.fmt_short(),
            })
        }
    }
}

/// Shared-secret [`AdmissionPolicy`] requiring a keyed BLAKE3 token bound to
/// `(topic_id, subject, session_id)`.
#[derive(Clone)]
pub struct SharedSecretAdmission {
    key: [u8; 32],
}

impl fmt::Debug for SharedSecretAdmission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedSecretAdmission")
            .field("key", &"<redacted>")
            .finish()
    }
}

impl SharedSecretAdmission {
    /// Creates a [`SharedSecretAdmission`] from a raw 32-byte symmetric cluster key.
    #[must_use]
    pub const fn new(key: [u8; 32]) -> Self {
        Self { key }
    }

    /// Derives a 32-byte cluster key from an arbitrary passphrase using domain-separated BLAKE3.
    #[must_use]
    pub fn from_passphrase(passphrase: &str) -> Self {
        let key = blake3::derive_key("membermesh shared secret key v1", passphrase.as_bytes());
        Self { key }
    }

    fn compute_token(
        &self,
        topic_id: TopicId,
        subject: &[u8; 32],
        session_id: SessionId,
    ) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new_keyed(&self.key);
        hasher.update(ADMISSION_DOMAIN);
        hasher.update(topic_id.as_bytes());
        hasher.update(subject);
        hasher.update(&session_id.0.to_be_bytes());
        *hasher.finalize().as_bytes()
    }
}

impl AdmissionPolicy for SharedSecretAdmission {
    fn local_token(
        &self,
        topic_id: TopicId,
        local_id: EndpointId,
        session_id: SessionId,
    ) -> Option<[u8; 32]> {
        Some(self.compute_token(topic_id, local_id.as_bytes(), session_id))
    }

    fn verify_claim(&self, topic_id: TopicId, claim: &MemberClaim) -> Result<(), MemberMeshError> {
        let Some(provided) = claim.admission_token else {
            return Err(MemberMeshError::AdmissionDenied {
                peer_short: claim.subject.fmt_short(),
            });
        };
        let expected = blake3::Hash::from(self.compute_token(
            topic_id,
            claim.subject.as_bytes(),
            claim.session_id,
        ));
        if blake3::Hash::from(provided) == expected {
            Ok(())
        } else {
            Err(MemberMeshError::AdmissionDenied {
                peer_short: claim.subject.fmt_short(),
            })
        }
    }
}

#[derive(Debug)]
pub(crate) struct CorePolicyAdapter {
    topic_id: TopicId,
    policy: Arc<dyn AdmissionPolicy>,
}

impl CorePolicyAdapter {
    pub(crate) const fn new(topic_id: TopicId, policy: Arc<dyn AdmissionPolicy>) -> Self {
        Self { topic_id, policy }
    }
}

impl ClaimPolicy for CorePolicyAdapter {
    fn authorize(&self, claim: &MemberClaim) -> Result<(), CoreError> {
        self.policy
            .verify_claim(self.topic_id, claim)
            .map_err(|_| CoreError::AdmissionDenied {
                peer_short: claim.subject.fmt_short(),
            })
    }
}
