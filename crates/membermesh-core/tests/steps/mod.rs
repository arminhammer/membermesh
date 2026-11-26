mod membership;
mod swim;
mod wire;

use std::collections::BTreeMap;

use bytes::Bytes;
use cucumber::{World, given, then, when};
use membermesh_core::{CoreError, MemberClaim, MemberMetadata, MembershipTable, SignedClaim};
use membermesh_testkit::{
    crypto::PeerDirectory, gossip_cosim::GossipCosimCluster, sim::SimCluster,
};

#[derive(Default, World)]
pub struct CoreWorld {
    pub version: Option<&'static str>,
    pub peers: PeerDirectory,
    pub claims: BTreeMap<String, MemberClaim>,
    pub last_signed: Option<SignedClaim>,
    pub last_encoded: Option<Bytes>,
    pub encoded_pair: Option<(Bytes, Bytes)>,
    pub last_decoded_claim: Option<SignedClaim>,
    pub last_error: Option<CoreError>,
    pub last_verify_result: Option<Result<(), CoreError>>,
    pub last_metadata_result: Option<Result<MemberMetadata, CoreError>>,
    pub tables: BTreeMap<String, MembershipTable>,
    pub peer_order: Vec<String>,
    pub pending_claims: Vec<SignedClaim>,
    pub sim_cluster: Option<SimCluster>,
    pub cosim_cluster: Option<GossipCosimCluster>,
}

impl std::fmt::Debug for CoreWorld {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoreWorld")
            .field("peer_order", &self.peer_order)
            .finish_non_exhaustive()
    }
}

#[given("the membermesh-core library is loaded")]
fn core_loaded(_world: &mut CoreWorld) {}

#[when("I query the crate version")]
fn query_version(world: &mut CoreWorld) {
    world.version = Some(membermesh_core::crate_version());
}

#[then("a non-empty version string is returned")]
fn assert_version(world: &mut CoreWorld) {
    let version = world.version.expect("version should be populated");
    assert_ne!(version, "", "crate_version must not be empty");
}
