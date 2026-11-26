use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicU16, Ordering},
    },
    time::{Duration, Instant},
};

use cucumber::{given, then, when};
use iroh::RelayMode;
use iroh_gossip::TopicId;
use membermesh::{DiscoveryConfig, MemberMesh, SharedSecretAdmission, peer_id_from_endpoint};
use membermesh_core::ProtocolConfig;

use super::MeshWorld;

static NEXT_DISCOVERY_PORT: AtomicU16 = AtomicU16::new(45100);
const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(12);
const POLL_INTERVAL: Duration = Duration::from_millis(60);

fn topic_from_name(name: &str) -> TopicId {
    TopicId::from_bytes(*blake3::hash(name.as_bytes()).as_bytes())
}

#[given(regex = r"^a loopback auto-discovery port for the scenario$")]
fn allocate_discovery_port(world: &mut MeshWorld) {
    world.discovery_port = NEXT_DISCOVERY_PORT.fetch_add(1, Ordering::Relaxed);
}

#[when(
    regex = r#"^node "([^"]+)" starts with auto-discovery on topic "([^"]+)" without bootstrap tickets$"#
)]
async fn start_with_auto_discovery(world: &mut MeshWorld, name: String, topic_name: String) {
    let port = world.discovery_port;
    assert_ne!(
        port, 0,
        "discovery port must be allocated before starting nodes"
    );
    let mesh = MemberMesh::builder(topic_from_name(&topic_name))
        .relay_mode(RelayMode::Disabled)
        .config(ProtocolConfig::lan())
        .discovery(DiscoveryConfig::loopback(port))
        .spawn()
        .await
        .expect("spawn auto-discovery node");
    world.meshes.insert(name, mesh);
}

#[when(
    regex = r#"^node "([^"]+)" starts on topic "([^"]+)" bootstrapping from "([^"]+)" without auto-discovery$"#
)]
async fn start_with_bootstrap_only(
    world: &mut MeshWorld,
    name: String,
    topic_name: String,
    target: String,
) {
    let bootstrap_addr = world
        .meshes
        .get(&target)
        .expect("bootstrap target must already be running")
        .addr();
    let mesh = MemberMesh::builder(topic_from_name(&topic_name))
        .relay_mode(RelayMode::Disabled)
        .config(ProtocolConfig::lan())
        .bootstrap([bootstrap_addr])
        .spawn()
        .await
        .expect("spawn ticket-bootstrapped node");
    world.meshes.insert(name, mesh);
}

#[when(
    regex = r#"^node "([^"]+)" starts with auto-discovery on topic "([^"]+)" and passphrase "([^"]+)"$"#
)]
async fn start_with_auto_discovery_and_secret(
    world: &mut MeshWorld,
    name: String,
    topic_name: String,
    passphrase: String,
) {
    let port = world.discovery_port;
    assert_ne!(
        port, 0,
        "discovery port must be allocated before starting nodes"
    );
    let admission = Arc::new(SharedSecretAdmission::from_passphrase(&passphrase));
    let mesh = MemberMesh::builder(topic_from_name(&topic_name))
        .relay_mode(RelayMode::Disabled)
        .config(ProtocolConfig::lan())
        .admission(admission)
        .discovery(DiscoveryConfig::loopback(port))
        .spawn()
        .await
        .expect("spawn secret-protected auto-discovery node");
    world.meshes.insert(name, mesh);
}

#[when(regex = r"^the discovery cluster waits (\d+) milliseconds$")]
async fn discovery_cluster_waits(_world: &mut MeshWorld, ms: u64) {
    tokio::time::sleep(Duration::from_millis(ms)).await;
}

#[then(regex = r#"^discovery node "([^"]+)" sees (\d+) member in its cluster$"#)]
async fn discovery_node_sees_count(world: &mut MeshWorld, name: String, expected: usize) {
    let mesh = world.meshes.get(&name).expect("mesh");
    let members = mesh.members().await.expect("members");
    assert_eq!(
        members.len(),
        expected,
        "expected {expected} member(s) on {name}, got {members:?}"
    );
}

#[then(
    regex = r#"^nodes "([^"]+)" converge on a (\d+)-node cluster with a shared advisory leader$"#
)]
async fn nodes_converge_shared_leader(world: &mut MeshWorld, csv: String, count: usize) {
    let names: Vec<&str> = csv.split(',').collect();
    assert_eq!(names.len(), count);
    let expected_ids: BTreeSet<_> = names
        .iter()
        .map(|n| peer_id_from_endpoint(world.meshes.get(*n).expect("mesh").id()))
        .collect();

    let deadline = Instant::now() + DISCOVERY_TIMEOUT;
    loop {
        let mut leaders = BTreeSet::new();
        let mut all_match = true;
        let mut diag = Vec::new();

        for name in &names {
            let mesh = world.meshes.get(*name).expect("mesh");
            let views = mesh.members().await.expect("members");
            let got_ids: BTreeSet<_> = views.iter().map(|v| v.peer_id).collect();
            let leader = mesh.leader().await.expect("leader").map(|m| m.peer_id);
            if got_ids != expected_ids || leader.is_none() {
                all_match = false;
            }
            if let Some(l) = leader {
                leaders.insert(l);
            }
            diag.push(format!(
                "{name}: count={}, leader={leader:?}",
                got_ids.len()
            ));
        }

        if all_match && leaders.len() == 1 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for auto-discovery convergence (expected {count} nodes): {}",
            diag.join(" | ")
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

#[then(
    regex = r#"^discovery nodes "([^"]+)" converge on a (\d+)-node cluster excluding "([^"]+)"$"#
)]
async fn discovery_nodes_converge_excluding(
    world: &mut MeshWorld,
    csv: String,
    count: usize,
    excluded: String,
) {
    let names: Vec<&str> = csv.split(',').collect();
    assert_eq!(names.len(), count);
    let expected_ids: BTreeSet<_> = names
        .iter()
        .map(|n| peer_id_from_endpoint(world.meshes.get(*n).expect("mesh").id()))
        .collect();
    let excluded_id =
        peer_id_from_endpoint(world.meshes.get(&excluded).expect("excluded mesh").id());

    let deadline = Instant::now() + DISCOVERY_TIMEOUT;
    loop {
        let mut leaders = BTreeSet::new();
        let mut all_match = true;
        for name in &names {
            let mesh = world.meshes.get(*name).expect("mesh");
            let views = mesh.members().await.expect("members");
            let got_ids: BTreeSet<_> = views.iter().map(|v| v.peer_id).collect();
            let leader = mesh.leader().await.expect("leader").map(|m| m.peer_id);
            if got_ids != expected_ids || got_ids.contains(&excluded_id) || leader.is_none() {
                all_match = false;
            }
            if let Some(l) = leader {
                leaders.insert(l);
            }
        }
        if all_match && leaders.len() == 1 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {csv} to converge excluding {excluded}"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

#[then(
    regex = r#"^discovery node "([^"]+)" never joins "([^"]+)" or "([^"]+)" within (\d+) milliseconds$"#
)]
async fn discovery_node_never_joins(
    world: &mut MeshWorld,
    excluded: String,
    first: String,
    second: String,
    ms: u64,
) {
    tokio::time::sleep(Duration::from_millis(ms)).await;
    let excluded_id = peer_id_from_endpoint(world.meshes.get(&excluded).expect("excluded").id());
    for name in [&first, &second] {
        let mesh = world.meshes.get(name).expect("mesh");
        let members = mesh.members().await.expect("members");
        assert!(
            members.iter().all(|m| m.peer_id != excluded_id),
            "{excluded} unexpectedly joined {name}: {members:?}"
        );
    }
}
