use cucumber::{given, then, when};
use membermesh_core::ProtocolConfig;
use membermesh_testkit::{gossip_cosim::GossipCosimCluster, sim::SimCluster};

use super::CoreWorld;

#[given(regex = r#"^a simulated LAN cluster of peers "([^"]+)"$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String captures by value"
)]
fn simulated_lan_cluster(world: &mut CoreWorld, csv: String) {
    let names: Vec<&str> = csv.split(',').collect();
    let cluster = SimCluster::new(&names, ProtocolConfig::lan(), 0xDEAD_BEEF).expect("sim cluster");
    world.sim_cluster = Some(cluster);
}

#[when(regex = r"^the cluster stabilizes for (\d+) ms$")]
fn stabilize_cluster(world: &mut CoreWorld, ms: u64) {
    world
        .sim_cluster
        .as_mut()
        .expect("sim cluster")
        .advance_ms(ms);
}

#[when(regex = r#"^peer "([^"]+)" silently crashes$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String captures by value"
)]
fn crash_peer(world: &mut CoreWorld, name: String) {
    world
        .sim_cluster
        .as_mut()
        .expect("sim cluster")
        .crash_node(&name);
}

#[when(regex = r"^the simulation advances by (\d+) ms$")]
fn advance_simulation(world: &mut CoreWorld, ms: u64) {
    world
        .sim_cluster
        .as_mut()
        .expect("sim cluster")
        .advance_ms(ms);
}

#[then(regex = r#"^"([^"]+)" and "([^"]+)" see the active roster "([^"]+)"$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String captures by value"
)]
fn two_sim_peers_see_roster(world: &mut CoreWorld, p1: String, p2: String, expected_csv: String) {
    let expected: Vec<String> = expected_csv.split(',').map(str::to_owned).collect();
    let cluster = world.sim_cluster.as_ref().expect("sim cluster");
    for peer in [&p1, &p2] {
        assert_eq!(cluster.roster_names(peer), expected, "roster on {peer}");
    }
}

#[then(regex = r#"^"([^"]+)" and "([^"]+)" elect "([^"]+)" as leader$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String captures by value"
)]
fn two_sim_peers_elect_leader(world: &mut CoreWorld, p1: String, p2: String, leader: String) {
    // Disambiguate cleanly between MembershipTable scenarios and SimCluster scenarios.
    if let Some(cluster) = world.sim_cluster.as_ref() {
        for peer in [&p1, &p2] {
            assert_eq!(
                cluster.leader_name(peer).as_deref(),
                Some(leader.as_str()),
                "leader on {peer}"
            );
        }
    } else {
        let expected_id = world.peers.peer_id(&leader);
        for name in [&p1, &p2] {
            let l = world
                .tables
                .get(name)
                .expect("table")
                .leader()
                .expect("leader");
            assert_eq!(l.peer_id, expected_id, "leader mismatch on {name}");
        }
    }
}

#[when(
    regex = r#"^the direct link between "([^"]+)" and "([^"]+)" is severed symmetrically while "([^"]+)" can reach both$"#
)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String captures by value"
)]
fn sever_direct_link(world: &mut CoreWorld, a: String, b: String, relay: String) {
    let _ = relay;
    world
        .sim_cluster
        .as_mut()
        .expect("sim cluster")
        .sever_link(&a, &b);
}

#[then(regex = r#"^"([^"]+)" still sees "([^"]+)" in the active roster$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String captures by value"
)]
fn still_sees_in_roster(world: &mut CoreWorld, observer: String, target: String) {
    let roster = world
        .sim_cluster
        .as_ref()
        .expect("sim cluster")
        .roster_names(&observer);
    assert!(
        roster.contains(&target),
        "expected {observer} roster {roster:?} to contain {target}"
    );
}

#[when(regex = r#"^"([^"]+)" experiences a local CPU stall for (\d+) ms$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String captures by value"
)]
fn stall_node(world: &mut CoreWorld, name: String, ms: u64) {
    world
        .sim_cluster
        .as_mut()
        .expect("sim cluster")
        .stall_node(&name, ms);
}

#[then(regex = r#"^all (\d+) peers remain alive with "([^"]+)" as leader$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String captures by value"
)]
fn all_peers_remain_alive(world: &mut CoreWorld, count: usize, leader: String) {
    let cluster = world.sim_cluster.as_ref().expect("sim cluster");
    for name in ["alice", "bob", "carol"] {
        assert_eq!(cluster.roster_names(name).len(), count, "count on {name}");
        assert_eq!(
            cluster.leader_name(name).as_deref(),
            Some(leader.as_str()),
            "leader on {name}"
        );
    }
}

#[when(regex = r#"^the cluster is partitioned into "([^"]+)" and "([^"]+)"$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String captures by value"
)]
fn partition_cluster(world: &mut CoreWorld, left_csv: String, right_csv: String) {
    let left: Vec<&str> = left_csv.split(',').collect();
    let right: Vec<&str> = right_csv.split(',').collect();
    world
        .sim_cluster
        .as_mut()
        .expect("sim cluster")
        .partition(&left, &right);
}

#[then(regex = r#"^the minority partition "([^"]+)" has no quorum (\d+) leader$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String captures by value"
)]
fn minority_has_no_quorum(world: &mut CoreWorld, minority_csv: String, quorum: usize) {
    let cluster = world.sim_cluster.as_ref().expect("sim cluster");
    for name in minority_csv.split(',') {
        assert_eq!(
            cluster.leader_with_quorum_name(name, quorum),
            None,
            "minority peer {name} must not hold quorum"
        );
    }
}

#[then(regex = r#"^the majority partition "([^"]+)" elects "([^"]+)" with quorum (\d+)$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String captures by value"
)]
fn majority_elects_quorum(
    world: &mut CoreWorld,
    majority_csv: String,
    leader: String,
    quorum: usize,
) {
    let cluster = world.sim_cluster.as_ref().expect("sim cluster");
    for name in majority_csv.split(',') {
        assert_eq!(
            cluster.leader_with_quorum_name(name, quorum).as_deref(),
            Some(leader.as_str()),
            "majority peer {name}"
        );
    }
}

#[when(regex = r"^the partition heals and the simulation advances by (\d+) ms$")]
fn heal_and_advance(world: &mut CoreWorld, ms: u64) {
    let cluster = world.sim_cluster.as_mut().expect("sim cluster");
    cluster.heal_all();
    cluster.advance_ms(ms);
}

#[then(regex = r#"^all (\d+) simulated peers converge on "([^"]+)" with leader "([^"]+)"$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String captures by value"
)]
fn all_sim_converge(world: &mut CoreWorld, count: usize, expected_csv: String, leader: String) {
    let expected: Vec<String> = expected_csv.split(',').map(str::to_owned).collect();
    assert_eq!(expected.len(), count);
    let cluster = world.sim_cluster.as_ref().expect("sim cluster");
    for name in &expected {
        assert_eq!(cluster.roster_names(name), expected, "roster on {name}");
        assert_eq!(
            cluster.leader_name(name).as_deref(),
            Some(leader.as_str()),
            "leader on {name}"
        );
    }
}

#[when(regex = r#"^"([^"]+)" drops incoming gossip while "([^"]+)" joins via "([^"]+)"$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String captures by value"
)]
fn drop_gossip_while_joining(
    world: &mut CoreWorld,
    lagged_peer: String,
    new_peer: String,
    seed: String,
) {
    let _ = seed;
    let cluster = world.sim_cluster.as_mut().expect("sim cluster");
    cluster.set_drop_gossip(&lagged_peer, true);
    let _ = cluster.join_node(&new_peer).expect("join new peer");
    cluster.advance_ms(200);
    cluster.set_drop_gossip(&lagged_peer, false);
}

#[when(regex = r#"^"([^"]+)" receives a GossipLagged signal and advances by (\d+) ms$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String captures by value"
)]
fn trigger_lagged_and_advance(world: &mut CoreWorld, name: String, ms: u64) {
    let cluster = world.sim_cluster.as_mut().expect("sim cluster");
    cluster.trigger_gossip_lagged(&name);
    cluster.advance_ms(ms);
}

#[then(
    regex = r#"^"([^"]+)" recovers "([^"]+)" via PushPullSync in the ordered roster "([^"]+)"$"#
)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String captures by value"
)]
fn recovers_via_sync(
    world: &mut CoreWorld,
    observer: String,
    recovered: String,
    expected_csv: String,
) {
    let expected: Vec<String> = expected_csv.split(',').map(str::to_owned).collect();
    let roster = world
        .sim_cluster
        .as_ref()
        .expect("sim cluster")
        .roster_names(&observer);
    assert!(roster.contains(&recovered));
    assert_eq!(roster, expected);
}

#[given(regex = r"^an iroh-gossip co-simulated cluster of (\d+) peers$")]
fn cosim_cluster_of_n(world: &mut CoreWorld, count: usize) {
    let cosim = GossipCosimCluster::new(count, 0xCAFE_BABE).expect("cosim");
    world.cosim_cluster = Some(cosim);
}

#[when(regex = r"^the co-simulation advances by (\d+) ms$")]
fn cosim_advance(world: &mut CoreWorld, ms: u64) {
    world.cosim_cluster.as_mut().expect("cosim").advance_ms(ms);
}

#[then(
    regex = r"^all (\d+) co-simulated peers agree on the identical (\d+)-peer order and oldest leader$"
)]
fn cosim_agree_all(world: &mut CoreWorld, count: usize, expected_len: usize) {
    let cosim = world.cosim_cluster.as_ref().expect("cosim");
    let expected = cosim.ordered_ids().to_vec();
    assert_eq!(expected.len(), count);
    assert_eq!(expected.len(), expected_len);
    let expected_leader = expected.first().copied();
    for &id in &expected {
        assert_eq!(cosim.roster_for(id), expected, "cosim roster on {id}");
        assert_eq!(
            cosim.leader_for(id),
            expected_leader,
            "cosim leader on {id}"
        );
    }
}

#[when(
    regex = r"^the oldest co-simulated peer crashes and the co-simulation advances by (\d+) ms$"
)]
fn cosim_oldest_crashes(world: &mut CoreWorld, ms: u64) {
    let cosim = world.cosim_cluster.as_mut().expect("cosim");
    cosim.crash_index(0);
    cosim.advance_ms(ms);
}

#[then(
    regex = r"^the remaining (\d+) co-simulated peers agree on the second-oldest peer as leader$"
)]
fn cosim_remaining_agree(world: &mut CoreWorld, remaining_count: usize) {
    let cosim = world.cosim_cluster.as_ref().expect("cosim");
    let remaining = &cosim.ordered_ids()[1..];
    assert_eq!(remaining.len(), remaining_count);
    let expected_leader = remaining.first().copied();
    for &id in remaining {
        assert_eq!(
            cosim.roster_for(id),
            remaining,
            "cosim remaining roster on {id}"
        );
        assert_eq!(
            cosim.leader_for(id),
            expected_leader,
            "cosim remaining leader on {id}"
        );
    }
}
