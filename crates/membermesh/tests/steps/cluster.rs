use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use cucumber::{given, then, when};
use iroh_gossip::{Gossip, TopicId};
use membermesh::{MemberMesh, SYNC_ALPN, peer_id_from_endpoint};
use membermesh_core::{MemberMetadata, ProtocolConfig};
use membermesh_testkit::{
    cluster::{LocalIrohCluster, PartitionedHandler},
    trace::{ClusterTraceSample, NodeObservation},
};

use super::MeshWorld;

const CONVERGE_TIMEOUT: Duration = Duration::from_secs(18);
const POLL_INTERVAL: Duration = Duration::from_millis(80);

fn test_topic() -> TopicId {
    TopicId::from_bytes([0x5A; 32])
}

async fn spawn_live_node(
    cluster: &mut LocalIrohCluster,
    name: &str,
    bootstrap: Vec<iroh::EndpointAddr>,
) -> MemberMesh {
    let (secret_key, endpoint) = cluster.bind_endpoint(name).await.expect("bind");
    let gossip = Gossip::builder().spawn(endpoint.clone());
    let (prepared, control) = MemberMesh::prepare_attach(
        endpoint.clone(),
        gossip.clone(),
        test_topic(),
        ProtocolConfig::lan(),
        MemberMetadata::default(),
    );
    let matrix = cluster.matrix().clone();
    let local_id = endpoint.id();
    let router = iroh::protocol::Router::builder(endpoint.clone())
        .accept(
            iroh_gossip::ALPN,
            PartitionedHandler::new(local_id, matrix.clone(), gossip),
        )
        .accept(
            SYNC_ALPN,
            PartitionedHandler::new(local_id, matrix, control),
        )
        .spawn();
    cluster.insert_node(name, secret_key, endpoint, Some(router));
    prepared.start(bootstrap).await.expect("start mesh")
}

async fn record_trace_sample(world: &mut MeshWorld, settled: bool, quorum: Option<usize>) {
    let mut nodes = BTreeMap::new();
    let names: Vec<String> = world.meshes.keys().cloned().collect();
    for name in names {
        let Some(mesh) = world.meshes.get(&name) else {
            continue;
        };
        let Ok(roster) = mesh.members().await else {
            continue;
        };
        let leader = roster.first().map(|m| m.peer_id);
        let quorum_leader = quorum.and_then(|q| {
            let alive = roster
                .iter()
                .filter(|m| m.status == membermesh_core::MemberStatus::Alive)
                .count();
            (alive >= q)
                .then(|| roster.first().map(|m| m.peer_id))
                .flatten()
        });
        nodes.insert(
            name,
            NodeObservation {
                roster,
                leader,
                quorum_leader,
            },
        );
    }
    world
        .trace
        .push_sample(ClusterTraceSample { settled, nodes });
}

fn roster_names_from_views(
    cluster: &LocalIrohCluster,
    all_names: &[String],
    views: &[membermesh_core::MemberView],
) -> Vec<String> {
    views
        .iter()
        .filter_map(|v| {
            all_names
                .iter()
                .find(|n| cluster.id(n).map(peer_id_from_endpoint) == Some(v.peer_id))
                .cloned()
        })
        .collect()
}

async fn wait_for_convergence(
    world: &mut MeshWorld,
    observers: &[String],
    expected_roster: &[String],
    expected_leader: &str,
) {
    let deadline = Instant::now() + CONVERGE_TIMEOUT;
    loop {
        let mut all_match = true;
        let mut diag = Vec::new();
        {
            let cluster = world.cluster.as_ref().expect("cluster");
            for name in observers {
                let mesh = world.meshes.get(name).expect("mesh");
                let views = mesh.members().await.expect("members");
                let got_names = roster_names_from_views(cluster, &world.node_order, &views);
                let got_leader = mesh.leader().await.expect("leader").and_then(|l| {
                    world
                        .node_order
                        .iter()
                        .find(|n| cluster.id(n).map(peer_id_from_endpoint) == Some(l.peer_id))
                        .cloned()
                });
                if got_names != expected_roster || got_leader.as_deref() != Some(expected_leader) {
                    all_match = false;
                }
                diag.push(format!(
                    "{name}: roster={got_names:?}, leader={got_leader:?}"
                ));
            }
        }
        record_trace_sample(world, false, None).await;
        if all_match {
            record_trace_sample(world, true, None).await;
            return;
        }
        let last_snapshot = diag.join(" | ");
        assert!(
            Instant::now() < deadline,
            "timed out waiting for convergence (expected roster={expected_roster:?}, leader={expected_leader}): {last_snapshot}"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

#[given(
    regex = r#"^an offline (\d+)-node membermesh cluster "([^"]+)" bootstrapped sequentially$"#
)]
async fn live_cluster_bootstrapped(world: &mut MeshWorld, count: usize, csv: String) {
    let names: Vec<String> = csv.split(',').map(str::to_owned).collect();
    assert_eq!(names.len(), count);
    world.node_order.clone_from(&names);

    let mut cluster = LocalIrohCluster::new();
    let mut prev_addr = None;
    for name in &names {
        let bootstrap = prev_addr.into_iter().collect();
        let mesh = spawn_live_node(&mut cluster, name, bootstrap).await;
        prev_addr = Some(mesh.addr());
        world.meshes.insert(name.clone(), mesh);
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    world.cluster = Some(cluster);
}

#[when(regex = r"^all (\d+) live nodes have converged$")]
async fn all_live_converged(world: &mut MeshWorld, count: usize) {
    let expected = world.node_order.clone();
    assert_eq!(expected.len(), count);
    let leader = expected.first().expect("leader").clone();
    wait_for_convergence(world, &expected, &expected, &leader).await;
}

#[then(regex = r#"^all (\d+) live nodes converge on the ordered roster "([^"]+)"$"#)]
async fn all_live_converge_on_roster(world: &mut MeshWorld, count: usize, csv: String) {
    let expected: Vec<String> = csv.split(',').map(str::to_owned).collect();
    assert_eq!(expected.len(), count);
    let leader = expected.first().expect("leader").clone();
    let observers = expected.clone();
    wait_for_convergence(world, &observers, &expected, &leader).await;
}

#[then(regex = r#"^all (\d+) live nodes elect "([^"]+)" as leader$"#)]
async fn all_live_elect_leader(world: &mut MeshWorld, count: usize, expected_leader: String) {
    let cluster = world.cluster.as_ref().expect("cluster");
    assert_eq!(world.meshes.len(), count);
    let expected_id = peer_id_from_endpoint(cluster.id(&expected_leader).expect("leader id"));
    for (name, mesh) in &world.meshes {
        let leader = mesh.leader().await.expect("leader").expect("some leader");
        assert_eq!(leader.peer_id, expected_id, "leader mismatch on {name}");
    }
}

#[when(regex = r#"^live node "([^"]+)" gracefully leaves$"#)]
async fn live_node_leaves(world: &mut MeshWorld, name: String) {
    if let Some(mesh) = world.meshes.remove(&name) {
        if let Ok(members) = mesh.members().await
            && let Some(self_view) = members
                .into_iter()
                .find(|m| m.peer_id == peer_id_from_endpoint(mesh.id()))
        {
            world
                .trace
                .record_left_session(self_view.peer_id, self_view.session_id);
        }
        mesh.leave().await.expect("graceful leave");
        mesh.shutdown().await;
    }
    world.crashed.push(name);
}

#[then(regex = r#"^the remaining (\d+) live nodes converge on "([^"]+)" with leader "([^"]+)"$"#)]
async fn remaining_live_converge(
    world: &mut MeshWorld,
    count: usize,
    csv: String,
    expected_leader: String,
) {
    let expected: Vec<String> = csv.split(',').map(str::to_owned).collect();
    assert_eq!(expected.len(), count);
    let observers = expected.clone();
    wait_for_convergence(world, &observers, &expected, &expected_leader).await;
}

#[when(regex = r#"^live node "([^"]+)" abruptly crashes$"#)]
async fn live_node_crashes(world: &mut MeshWorld, name: String) {
    if let Some(mesh) = world.meshes.remove(&name) {
        mesh.shutdown().await;
    }
    if let Some(cluster) = world.cluster.as_ref()
        && let Some(crashed_id) = cluster.id(&name)
    {
        for other in world.meshes.keys() {
            if let Some(other_id) = cluster.id(other) {
                cluster.matrix().sever(crashed_id, other_id);
            }
        }
    }
    world.crashed.push(name);
}

#[then(
    regex = r#"^the remaining (\d+) live nodes detect the failure and converge on "([^"]+)" with leader "([^"]+)"$"#
)]
async fn remaining_detect_failure(
    world: &mut MeshWorld,
    count: usize,
    csv: String,
    expected_leader: String,
) {
    remaining_live_converge(world, count, csv, expected_leader).await;
}

#[when(regex = r#"^the live cluster is partitioned into "([^"]+)" and "([^"]+)"$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String captures by value"
)]
fn partition_live_cluster(world: &mut MeshWorld, left_csv: String, right_csv: String) {
    let cluster = world.cluster.as_ref().expect("cluster");
    let left: Vec<_> = left_csv
        .split(',')
        .map(|n| cluster.id(n).expect("left id"))
        .collect();
    let right: Vec<_> = right_csv
        .split(',')
        .map(|n| cluster.id(n).expect("right id"))
        .collect();
    cluster.matrix().partition(&left, &right);
}

#[then(
    regex = r#"^the live minority "([^"]+)" has no quorum (\d+) leader while "([^"]+)" elects "([^"]+)" with quorum (\d+)$"#
)]
async fn live_minority_suppressed_majority_elects(
    world: &mut MeshWorld,
    minority_csv: String,
    q1: usize,
    majority_csv: String,
    expected_leader: String,
    q2: usize,
) {
    let minority: Vec<String> = minority_csv.split(',').map(str::to_owned).collect();
    let majority: Vec<String> = majority_csv.split(',').map(str::to_owned).collect();
    let expected_id = peer_id_from_endpoint(
        world
            .cluster
            .as_ref()
            .expect("cluster")
            .id(&expected_leader)
            .expect("leader id"),
    );

    let deadline = Instant::now() + CONVERGE_TIMEOUT;
    loop {
        let mut ok = true;
        for name in &minority {
            let mesh = world.meshes.get(name).expect("mesh");
            if mesh.leader_with_quorum(q1).await.expect("quorum").is_some() {
                ok = false;
            }
        }
        for name in &majority {
            let mesh = world.meshes.get(name).expect("mesh");
            let ql = mesh.leader_with_quorum(q2).await.expect("quorum");
            if ql.as_ref().map(|m| m.peer_id) != Some(expected_id) {
                ok = false;
            }
        }
        if ok {
            record_trace_sample(world, true, Some(q2)).await;
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for partition quorum divergence"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

#[when("the live partition heals")]
fn live_partition_heals(world: &mut MeshWorld) {
    world.cluster.as_ref().expect("cluster").heal_all();
}

#[then(regex = r#"^all (\d+) live nodes reconverge on "([^"]+)" with leader "([^"]+)"$"#)]
async fn all_live_reconverge(
    world: &mut MeshWorld,
    count: usize,
    csv: String,
    expected_leader: String,
) {
    remaining_live_converge(world, count, csv, expected_leader).await;
}

#[when(regex = r#"^live node "([^"]+)" joins bootstrapping only from "([^"]+)"$"#)]
async fn live_late_join(world: &mut MeshWorld, new_name: String, seed_name: String) {
    let seed_addr = world.meshes.get(&seed_name).expect("seed mesh").addr();
    let mut cluster = world.cluster.take().expect("cluster");
    let mesh = spawn_live_node(&mut cluster, &new_name, vec![seed_addr]).await;
    world.cluster = Some(cluster);
    world.node_order.push(new_name.clone());
    world.meshes.insert(new_name, mesh);
}

#[then("the live trace invariant checker confirms zero violations")]
fn verify_live_trace(world: &mut MeshWorld) {
    world
        .trace
        .verify_invariants()
        .expect("live trace invariants must hold");
}
