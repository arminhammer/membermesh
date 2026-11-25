use cucumber::{given, then, when};
use membermesh_core::{
    ClaimSigner as _, CoreError, DEFAULT_MAX_GOSSIP_BYTES, GossipEnvelope, GossipItem,
    GossipMessage, Incarnation, MemberClaim, MemberMetadata, MemberStatus, SessionId, SignedClaim,
};
use membermesh_testkit::crypto::TestVerifier;

use super::CoreWorld;

#[given(regex = r#"^a peer "([^"]+)" with a valid Ed25519 signer$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String regex captures by value"
)]
fn peer_with_signer(world: &mut CoreWorld, name: String) {
    let _ = world.peers.signer(&name);
}

#[given(regex = r#"^a valid alive claim for "([^"]+)" at join sequence (\d+)$"#)]
fn valid_alive_claim(world: &mut CoreWorld, name: String, join_seq: u64) {
    let peer_id = world.peers.peer_id(&name);
    let claim = MemberClaim {
        subject: peer_id,
        session_id: SessionId(100 + join_seq),
        incarnation: Incarnation(1),
        join_seq,
        joined_at_unix_ms: 1_700_000_000_000,
        status: MemberStatus::Alive,
        metadata: MemberMetadata::default(),
        addr_hint: None,
    };
    world.claims.insert(name, claim);
}

#[when(
    regex = r#"^"([^"]+)" signs the claim and encodes it into a gossip envelope with nonce (\d+)$"#
)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String regex captures by value"
)]
fn sign_and_encode(world: &mut CoreWorld, name: String, nonce: u64) {
    let claim = world.claims.get(&name).expect("claim missing").clone();
    let signer = world.peers.signer(&name);
    let signed_claim = SignedClaim::sign(claim, signer).expect("signing should succeed");
    let env = GossipEnvelope::new(
        nonce,
        signer.peer_id(),
        GossipMessage::Single(GossipItem::Claim(signed_claim.clone())),
    );
    let bytes = env
        .encode(DEFAULT_MAX_GOSSIP_BYTES)
        .expect("encode should succeed");
    world.last_signed = Some(signed_claim);
    world.last_encoded = Some(bytes);
}

#[when("the envelope is decoded and verified")]
fn decode_and_verify(world: &mut CoreWorld) {
    let bytes = world.last_encoded.as_ref().expect("encoded bytes missing");
    let env =
        GossipEnvelope::decode(bytes, DEFAULT_MAX_GOSSIP_BYTES).expect("decode should succeed");
    match env.message {
        GossipMessage::Single(GossipItem::Claim(signed_claim)) => {
            let res = signed_claim.verify(&TestVerifier);
            world.last_verify_result = Some(res);
            world.last_decoded_claim = Some(signed_claim);
        }
        GossipMessage::Single(GossipItem::IndirectAlert(_)) | GossipMessage::Compound(_) | _ => {
            panic!("unexpected gossip message variant")
        }
    }
}

#[then(regex = r#"^verification succeeds and returns the claim for "([^"]+)"$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String regex captures by value"
)]
fn verify_succeeds_for_peer(world: &mut CoreWorld, name: String) {
    let res = world
        .last_verify_result
        .as_ref()
        .expect("verify result missing");
    assert!(res.is_ok(), "expected verification Ok, got {res:?}");
    let expected_peer = world.peers.peer_id(&name);
    let decoded = world
        .last_decoded_claim
        .as_ref()
        .expect("decoded claim missing");
    assert_eq!(decoded.claim.subject, expected_peer);
}

#[when(regex = r#"^"([^"]+)" signs the claim and its join sequence is tampered to (\d+)$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String regex captures by value"
)]
fn sign_and_tamper(world: &mut CoreWorld, name: String, tampered_seq: u64) {
    let claim = world.claims.get(&name).expect("claim missing").clone();
    let signer = world.peers.signer(&name);
    let mut signed_claim = SignedClaim::sign(claim, signer).expect("signing should succeed");
    signed_claim.claim.join_seq = tampered_seq;
    world.last_signed = Some(signed_claim);
}

#[when("the tampered claim is verified")]
fn verify_tampered(world: &mut CoreWorld) {
    let signed_claim = world.last_signed.as_ref().expect("signed claim missing");
    world.last_verify_result = Some(signed_claim.verify(&TestVerifier));
}

#[then("verification fails with an invalid signature error")]
fn assert_invalid_signature(world: &mut CoreWorld) {
    let res = world
        .last_verify_result
        .as_ref()
        .expect("verify result missing");
    assert!(
        matches!(res, Err(CoreError::InvalidSignature { .. })),
        "expected InvalidSignature, got {res:?}"
    );
}

#[when(regex = r#"^"([^"]+)" attempts to sign a claim claiming subject "([^"]+)"$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String regex captures by value"
)]
fn attempt_spoofed_sign(world: &mut CoreWorld, signer_name: String, subject_name: String) {
    let subject_id = world.peers.peer_id(&subject_name);
    let claim = MemberClaim {
        subject: subject_id,
        session_id: SessionId(1),
        incarnation: Incarnation(1),
        join_seq: 1,
        joined_at_unix_ms: 1_700_000_000_000,
        status: MemberStatus::Alive,
        metadata: MemberMetadata::default(),
        addr_hint: None,
    };
    let signer = world.peers.signer(&signer_name);
    world.last_error = SignedClaim::sign(claim, signer).err();
}

#[then("signing fails with a subject mismatch error")]
fn assert_subject_mismatch(world: &mut CoreWorld) {
    assert!(
        matches!(world.last_error, Some(CoreError::SubjectMismatch { .. })),
        "expected SubjectMismatch, got {:?}",
        world.last_error
    );
}

#[when(regex = r"^I construct member metadata of (\d+) bytes$")]
fn construct_metadata(world: &mut CoreWorld, size: usize) {
    world.last_metadata_result = Some(MemberMetadata::try_new(vec![0x42; size]));
}

#[then("metadata construction fails with a metadata too large error")]
fn assert_metadata_too_large(world: &mut CoreWorld) {
    assert!(
        matches!(
            world.last_metadata_result,
            Some(Err(CoreError::MetadataTooLarge { .. }))
        ),
        "expected MetadataTooLarge, got {:?}",
        world.last_metadata_result
    );
}

#[when(regex = r#"^"([^"]+)" encodes the same signed claim with nonces (\d+) and (\d+)$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String regex captures by value"
)]
fn encode_with_two_nonces(world: &mut CoreWorld, name: String, n1: u64, n2: u64) {
    let claim = world.claims.get(&name).expect("claim missing").clone();
    let signer = world.peers.signer(&name);
    let signed_claim = SignedClaim::sign(claim, signer).expect("signing should succeed");
    let b1 = GossipEnvelope::new(
        n1,
        signer.peer_id(),
        GossipMessage::Single(GossipItem::Claim(signed_claim.clone())),
    )
    .encode(DEFAULT_MAX_GOSSIP_BYTES)
    .expect("encode 1");
    let b2 = GossipEnvelope::new(
        n2,
        signer.peer_id(),
        GossipMessage::Single(GossipItem::Claim(signed_claim)),
    )
    .encode(DEFAULT_MAX_GOSSIP_BYTES)
    .expect("encode 2");
    world.encoded_pair = Some((b1, b2));
}

#[then("the two wire byte payloads are distinct")]
fn assert_distinct_payloads(world: &mut CoreWorld) {
    let (b1, b2) = world.encoded_pair.as_ref().expect("encoded pair missing");
    assert_ne!(b1, b2, "different nonces must produce distinct wire hashes");
}

#[when(regex = r"^I decode a gossip envelope with wire version (\d+)$")]
fn decode_version(world: &mut CoreWorld, version: u8) {
    let raw = [version, 0, 1, 2, 3];
    world.last_error = GossipEnvelope::decode(&raw, DEFAULT_MAX_GOSSIP_BYTES).err();
}

#[then("decoding fails with an unsupported version error")]
fn assert_unsupported_version(world: &mut CoreWorld) {
    assert!(
        matches!(world.last_error, Some(CoreError::UnsupportedVersion { .. })),
        "expected UnsupportedVersion, got {:?}",
        world.last_error
    );
}

#[when("I decode a frame exceeding the maximum wire size")]
fn decode_oversized(world: &mut CoreWorld) {
    let raw = vec![1u8; DEFAULT_MAX_GOSSIP_BYTES + 1];
    world.last_error = GossipEnvelope::decode(&raw, DEFAULT_MAX_GOSSIP_BYTES).err();
}

#[then("decoding fails with a frame too large error")]
fn assert_frame_too_large(world: &mut CoreWorld) {
    assert!(
        matches!(world.last_error, Some(CoreError::FrameTooLarge { .. })),
        "expected FrameTooLarge, got {:?}",
        world.last_error
    );
}
