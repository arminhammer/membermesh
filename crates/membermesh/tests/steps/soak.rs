use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use cucumber::{given, then, when};
use iroh_gossip::{Gossip, TopicId};
use membermesh::{MemberMesh, SYNC_ALPN, peer_id_from_endpoint};
use membermesh_core::{MemberMetadata, MemberStatus, ProtocolConfig};
use membermesh_testkit::{
    cluster::{LocalIrohCluster, PartitionedHandler},
    sim::SimCluster,
    trace::{ClusterTraceSample, NodeObservation},
};

use super::MeshWorld;

const CONVERGE_TIMEOUT: Duration = Duration::from_secs(25);
const POLL_INTERVAL: Duration = Duration::from_millis(80);

fn soak_topic() -> TopicId {
    TopicId::from_bytes([0x9C; 32])
}

async fn spawn_restartable_node(
    cluster: &mut LocalIrohCluster,
    name: &str,
    bootstrap: Vec<iroh::EndpointAddr>,
) -> MemberMesh {
    let (secret_key, endpoint) = cluster.bind_endpoint(name).await.expect("bind");
    let gossip = Gossip::builder().spawn(endpoint.clone());
    let (prepared, control) = MemberMesh::prepare_attach(
        endpoint.clone(),
        gossip.clone(),
        soak_topic(),
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
    prepared.start(bootstrap).await.expect("start soak mesh")
}

async fn wait_rolling_convergence(world: &mut MeshWorld, expected_names: &[String]) {
    let deadline = Instant::now() + CONVERGE_TIMEOUT;
    loop {
        let mut all_ok = true;
        let mut sample_nodes = BTreeMap::new();
        {
            let cluster = world.cluster.as_ref().expect("cluster");
            let expected_ids: Vec<_> = expected_names
                .iter()
                .map(|n| peer_id_from_endpoint(cluster.id(n).expect("id")))
                .collect();

            for name in expected_names {
                let mesh = world.meshes.get(name).expect("mesh");
                let Ok(roster) = mesh.members().await else {
                    all_ok = false;
                    continue;
                };
                let got_ids: Vec<_> = roster.iter().map(|m| m.peer_id).collect();
                if got_ids != expected_ids {
                    all_ok = false;
                }
                let leader = roster.first().map(|m| m.peer_id);
                sample_nodes.insert(
                    name.clone(),
                    NodeObservation {
                        roster,
                        leader,
                        quorum_leader: None,
                    },
                );
            }
        }
        world.trace.push_sample(ClusterTraceSample {
            settled: all_ok,
            nodes: sample_nodes,
        });
        if all_ok {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for rolling restart convergence on {expected_names:?}"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

#[given("a virtual 20-node SimCluster running for 1000 virtual seconds with partitions and churn")]
async fn run_20_node_sim_soak(world: &mut MeshWorld) {
    let (sim, survivors) = tokio::task::spawn_blocking(|| {
        let mut sim = SimCluster::new(&[], ProtocolConfig::lan(), 42).expect("init sim cluster");
        let mut initial_names = Vec::with_capacity(20);
        for i in 1..=20 {
            let name = format!("sim-{i:02}");
            sim.join_node(&name).expect("join initial node");
            initial_names.push(name);
            sim.advance_ms(300);
        }
        sim.advance_ms(12_000);

        // Wave 1: 8-vs-12 network partition for 15s (< dead_reap_ttl_ms = 30s), then heal for 30s.
        let left: Vec<&str> = initial_names[..8].iter().map(String::as_str).collect();
        let right: Vec<&str> = initial_names[8..].iter().map(String::as_str).collect();
        sim.partition(&left, &right);
        sim.advance_ms(15_000);
        sim.heal_all();
        sim.advance_ms(30_000);

        // Wave 2: 3 crashes + 2 graceful leaves + 2 fresh joins.
        for crashed in ["sim-03", "sim-07", "sim-12"] {
            sim.crash_node(crashed);
        }
        for left_peer in ["sim-15", "sim-18"] {
            sim.leave_node(left_peer);
        }
        sim.advance_ms(15_000);
        sim.join_node("sim-21").expect("join sim-21");
        sim.advance_ms(1_000);
        sim.join_node("sim-22").expect("join sim-22");
        sim.advance_ms(90_000);

        // Wave 3: scheduler stall + gossip lag injection.
        sim.stall_node("sim-05", 8_000);
        sim.trigger_gossip_lagged("sim-10");

        // Advance remaining budget to reach 1000 virtual seconds (1_000_000 ms) cleanly settled.
        let remaining = 1_000_000_u64.saturating_sub(sim.now().0);
        sim.advance_ms(remaining);
        assert!(sim.now().0 >= 1_000_000);

        let survivors: Vec<String> = (1..=22)
            .map(|i| format!("sim-{i:02}"))
            .filter(|n| {
                !matches!(
                    n.as_str(),
                    "sim-03" | "sim-07" | "sim-12" | "sim-15" | "sim-18"
                )
            })
            .collect();

        (sim, survivors)
    })
    .await
    .expect("spawn_blocking soak");

    world.sim = Some(sim);
    world.sim_survivors = survivors;
}

#[then("all surviving virtual nodes converge on the identical ordered roster and leader")]
fn sim_survivors_converge(world: &mut MeshWorld) {
    let sim = world.sim.as_ref().expect("sim");
    let first = world.sim_survivors.first().expect("survivor");
    let expected_roster = sim.roster_names(first);
    let expected_leader = sim.leader_name(first).expect("leader");
    assert_eq!(expected_roster.len(), world.sim_survivors.len());
    assert_eq!(expected_roster.first(), Some(&expected_leader));

    for survivor in &world.sim_survivors {
        assert_eq!(
            sim.roster_names(survivor),
            expected_roster,
            "roster mismatch on {survivor}"
        );
        assert_eq!(
            sim.leader_name(survivor).as_deref(),
            Some(expected_leader.as_str()),
            "leader mismatch on {survivor}"
        );
    }
}

#[then(regex = r"^reaped tombstone tables remain bounded below (\d+) entries per survivor$")]
fn sim_tombstones_bounded(world: &mut MeshWorld, max_tombstones: usize) {
    let sim = world.sim.as_ref().expect("sim");
    for survivor in &world.sim_survivors {
        let all = sim.all_members(survivor);
        let active = sim.members(survivor);
        let tombstones = all.len().saturating_sub(active.len());
        assert!(
            tombstones < max_tombstones,
            "{survivor} has {tombstones} un-reaped tombstones (limit {max_tombstones})"
        );
    }
}

#[given(regex = r#"^an offline (\d+)-node membermesh cluster "([^"]+)" for rolling restart$"#)]
async fn live_eight_node_cluster(world: &mut MeshWorld, count: usize, csv: String) {
    let names: Vec<String> = csv.split(',').map(str::to_owned).collect();
    assert_eq!(names.len(), count);
    world.node_order.clone_from(&names);

    let mut cluster = LocalIrohCluster::new();
    let mut prev_addr = None;
    for name in &names {
        let mesh =
            spawn_restartable_node(&mut cluster, name, prev_addr.into_iter().collect()).await;
        prev_addr = Some(mesh.addr());
        world.meshes.insert(name.clone(), mesh);
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    world.cluster = Some(cluster);
    let order = world.node_order.clone();
    wait_rolling_convergence(world, &order).await;
}

#[when(regex = r"^the current leader gracefully leaves and rejoins sequentially (\d+) times$")]
async fn rolling_restart_leader(world: &mut MeshWorld, rounds: usize) {
    for _ in 0..rounds {
        let leader_name = world.node_order.remove(0);
        let seed_name = world.node_order.first().expect("remaining node").clone();
        let seed_addr = world.meshes.get(&seed_name).expect("seed mesh").addr();

        if let Some(mesh) = world.meshes.remove(&leader_name) {
            let local_peer = peer_id_from_endpoint(mesh.id());
            if let Ok(members) = mesh.members().await
                && let Some(self_view) = members
                    .into_iter()
                    .find(|m| m.peer_id == local_peer && m.status == MemberStatus::Alive)
            {
                world
                    .trace
                    .record_left_session(self_view.peer_id, self_view.session_id);
            }
            mesh.leave().await.expect("graceful leave");
            mesh.shutdown().await;
        }
        if let Some(cluster) = world.cluster.as_mut() {
            cluster.remove_and_close(&leader_name).await;
        }

        // Wait briefly for departure dissemination, then restart with the same key & fresh SessionId.
        tokio::time::sleep(Duration::from_millis(120)).await;
        let mut cluster = world.cluster.take().expect("cluster");
        let restarted = spawn_restartable_node(&mut cluster, &leader_name, vec![seed_addr]).await;
        world.cluster = Some(cluster);
        world.meshes.insert(leader_name.clone(), restarted);
        world.node_order.push(leader_name);

        let expected = world.node_order.clone();
        wait_rolling_convergence(world, &expected).await;
    }
}

#[then(regex = r"^all (\d+) live nodes remain converged on the updated join order$")]
async fn all_live_remain_converged(world: &mut MeshWorld, count: usize) {
    assert_eq!(world.node_order.len(), count);
    let expected = world.node_order.clone();
    wait_rolling_convergence(world, &expected).await;
}
