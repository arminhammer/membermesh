use std::collections::BTreeMap;

use ed25519_dalek::{Signature, Signer as _, SigningKey, VerifyingKey};
use membermesh_core::{ClaimSigner, ClaimVerifier, CoreError, PeerId};

/// Deterministic Ed25519 signer derived from a human-readable peer label for tests.
#[derive(Debug, Clone)]
pub struct TestSigner {
    signing_key: SigningKey,
    peer_id: PeerId,
}

impl TestSigner {
    /// Derives a deterministic [`TestSigner`] from `name` via BLAKE3.
    #[must_use]
    pub fn from_name(name: &str) -> Self {
        let seed: [u8; 32] = *blake3::hash(name.as_bytes()).as_bytes();
        let signing_key = SigningKey::from_bytes(&seed);
        let peer_id = PeerId::from_bytes(signing_key.verifying_key().to_bytes());
        Self {
            signing_key,
            peer_id,
        }
    }
}

impl ClaimSigner for TestSigner {
    fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    fn sign(&self, payload: &[u8]) -> [u8; 64] {
        let sig: Signature = self.signing_key.sign(payload);
        sig.to_bytes()
    }
}

/// Ed25519 signature verifier for [`PeerId`] keys in tests.
#[derive(Debug, Default, Clone, Copy)]
pub struct TestVerifier;

impl ClaimVerifier for TestVerifier {
    fn verify(
        &self,
        peer_id: PeerId,
        payload: &[u8],
        signature: &[u8; 64],
    ) -> Result<(), CoreError> {
        let verifying_key = VerifyingKey::from_bytes(peer_id.as_bytes()).map_err(|_| {
            CoreError::InvalidSignature {
                peer_short: peer_id.fmt_short(),
            }
        })?;
        let sig = Signature::from_bytes(signature);
        verifying_key
            .verify_strict(payload, &sig)
            .map_err(|_| CoreError::InvalidSignature {
                peer_short: peer_id.fmt_short(),
            })
    }
}

/// Named registry of deterministic [`TestSigner`]s for BDD scenarios.
#[derive(Debug, Default)]
pub struct PeerDirectory {
    signers: BTreeMap<String, TestSigner>,
}

impl PeerDirectory {
    /// Returns (creating if absent) the deterministic [`TestSigner`] for `name`.
    pub fn signer(&mut self, name: &str) -> &TestSigner {
        self.signers
            .entry(name.to_owned())
            .or_insert_with(|| TestSigner::from_name(name))
    }

    /// Returns the deterministic [`PeerId`] for `name`.
    pub fn peer_id(&mut self, name: &str) -> PeerId {
        self.signer(name).peer_id()
    }
}
