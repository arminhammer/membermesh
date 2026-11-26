use cucumber::{given, then, when};
use membermesh_core::{
    ClaimSigner as _, Incarnation, IndirectAlertPayload, IndirectKind, LocalNodeInit,
    MemberMetadata, MembershipTable, SessionId, SignedIndirectAlert, SimInstant,
};
use membermesh_testkit::crypto::TestVerifier;

use super::CoreWorld;

fn sync_all(world: &mut CoreWorld, now: SimInstant) {
    let names: Vec<String> = world.tables.keys().cloned().collect();
    for _round in 0..2 {
        let snapshots: Vec<_> = names
            .iter()
            .map(|n| world.tables.get(n).expect("table").snapshot())
            .collect();
        for name in &names {
            let signer = world.peers.signer(name).clone();
            let table = world.tables.get_mut(name).expect("table");
            for snap in &snapshots {
                let outcome = table
                    .merge_snapshot(snap, &TestVerifier, &signer, now)
                    .expect("merge snapshot");
                if let Some(refute) = outcome.refutation {
                    world.pending_claims.push(refute);
                }
            }
        }
    }
}

fn setup_sequential_cluster(world: &mut CoreWorld, names: &[&str]) {
    let now = SimInstant(100);
    world.peer_order = names.iter().map(|s| (*s).to_owned()).collect();
    let mut prev_snap = None;
    for (idx, &name) in names.iter().enumerate() {
        let signer = world.peers.signer(name).clone();
        let init = LocalNodeInit {
            session_id: SessionId(10 + idx as u64),
            joined_at_unix_ms: 1_700_000_000_000 + (idx as u64) * 1_000,
            metadata: MemberMetadata::default(),
            addr_hint: None,
        };
        let table = if idx == 0 {
            MembershipTable::found(&signer, init, now).expect("found")
        } else {
            MembershipTable::join_after_sync(&signer, init, prev_snap.as_ref(), &TestVerifier, now)
                .expect("join")
        };
        prev_snap = Some(table.snapshot());
        world.tables.insert(name.to_owned(), table);
    }
}

fn names_for_roster(world: &mut CoreWorld, table: &MembershipTable) -> Vec<String> {
    let members = table.members_ordered();
    members
        .into_iter()
        .map(|m| {
            world
                .peer_order
                .iter()
                .find(|name| world.peers.peer_id(name) == m.peer_id)
                .cloned()
                .expect("unknown peer in roster")
        })
        .collect()
}

#[given(regex = r#"^peers "([^"]+)", "([^"]+)", and "([^"]+)" join sequentially$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String captures by value"
)]
fn peers_join_sequentially(world: &mut CoreWorld, a: String, b: String, c: String) {
    setup_sequential_cluster(world, &[&a, &b, &c]);
}

#[given(regex = r#"^peers "([^"]+)", "([^"]+)", and "([^"]+)" have converged$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String captures by value"
)]
fn peers_have_converged_three(world: &mut CoreWorld, a: String, b: String, c: String) {
    setup_sequential_cluster(world, &[&a, &b, &c]);
    sync_all(world, SimInstant(200));
}

#[given(regex = r#"^peers "([^"]+)" and "([^"]+)" have converged$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String captures by value"
)]
fn peers_have_converged_two(world: &mut CoreWorld, a: String, b: String) {
    setup_sequential_cluster(world, &[&a, &b]);
    sync_all(world, SimInstant(200));
}

#[when("their membership tables synchronize in reverse delivery order")]
fn sync_reverse(world: &mut CoreWorld) {
    let mut snapshots: Vec<_> = world
        .peer_order
        .iter()
        .map(|n| world.tables.get(n).expect("table").snapshot())
        .collect();
    snapshots.reverse();
    let names = world.peer_order.clone();
    for name in &names {
        let signer = world.peers.signer(name).clone();
        let table = world.tables.get_mut(name).expect("table");
        for snap in &snapshots {
            let _ = table
                .merge_snapshot(snap, &TestVerifier, &signer, SimInstant(200))
                .expect("merge");
        }
    }
}

#[then(regex = r#"^every peer sees the ordered roster "([^"]+)"$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String captures by value"
)]
fn every_peer_sees_roster(world: &mut CoreWorld, expected_csv: String) {
    let expected: Vec<String> = expected_csv.split(',').map(str::to_owned).collect();
    let names: Vec<String> = world.tables.keys().cloned().collect();
    for name in names {
        let table = world.tables.get(&name).expect("table").clone();
        let actual = names_for_roster(world, &table);
        assert_eq!(actual, expected, "roster mismatch on peer {name}");
    }
}

#[then(regex = r#"^every peer elects "([^"]+)" as leader$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String captures by value"
)]
fn every_peer_elects_leader(world: &mut CoreWorld, leader_name: String) {
    let expected_id = world.peers.peer_id(&leader_name);
    for (name, table) in &world.tables {
        let leader = table.leader().expect("leader must exist");
        assert_eq!(leader.peer_id, expected_id, "leader mismatch on {name}");
    }
}

#[when(regex = r#"^"([^"]+)" suspects "([^"]+)" at incarnation (\d+)$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String captures by value"
)]
fn peer_suspects_target(
    world: &mut CoreWorld,
    reporter_name: String,
    target_name: String,
    inc: u64,
) {
    let target_id = world.peers.peer_id(&target_name);
    let target_view = world
        .tables
        .get(&reporter_name)
        .expect("reporter table")
        .member(target_id)
        .expect("target view");
    let reporter_signer = world.peers.signer(&reporter_name).clone();
    let alert = SignedIndirectAlert::sign(
        IndirectAlertPayload {
            subject: target_id,
            session_id: target_view.session_id,
            incarnation: Incarnation(inc),
            join_seq: target_view.join_seq,
            kind: IndirectKind::Suspect,
            reporter: reporter_signer.peer_id(),
        },
        &reporter_signer,
    )
    .expect("sign alert");
    let reporter_table = world
        .tables
        .get_mut(&reporter_name)
        .expect("reporter table");
    let _ = reporter_table
        .apply_indirect_alert(&alert, &TestVerifier, &reporter_signer, SimInstant(300))
        .expect("apply alert");

    let target_signer = world.peers.signer(&target_name).clone();
    let target_table = world.tables.get_mut(&target_name).expect("target table");
    let outcome = target_table
        .apply_indirect_alert(&alert, &TestVerifier, &target_signer, SimInstant(310))
        .expect("target apply");
    if let Some(refute) = outcome.refutation {
        world.pending_claims.push(refute);
    }
}

#[when(regex = r#"^"([^"]+)" processes the suspicion alert and broadcasts its refutation$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String captures by value"
)]
fn broadcast_refutation(world: &mut CoreWorld, target_name: String) {
    let _ = target_name;
    let refutations = std::mem::take(&mut world.pending_claims);
    assert!(!refutations.is_empty(), "expected a refutation claim");
    let names: Vec<String> = world.tables.keys().cloned().collect();
    for refute in refutations {
        for name in &names {
            let table = world.tables.get_mut(name).expect("table");
            let _ = table
                .apply_claim(refute.clone(), &TestVerifier, SimInstant(320))
                .expect("apply refutation");
        }
    }
}

#[then(regex = r#"^"([^"]+)" increments incarnation to (\d+) while keeping join sequence (\d+)$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String captures by value"
)]
fn check_inc_and_seq(world: &mut CoreWorld, target_name: String, inc: u64, join_seq: u64) {
    let target_id = world.peers.peer_id(&target_name);
    let view = world
        .tables
        .get(&target_name)
        .expect("table")
        .member(target_id)
        .expect("member");
    assert_eq!(view.incarnation, Incarnation(inc));
    assert_eq!(view.join_seq, join_seq);
}

#[then(regex = r#"^all peers still elect "([^"]+)" as leader$"#)]
fn all_peers_still_elect(world: &mut CoreWorld, leader_name: String) {
    every_peer_elects_leader(world, leader_name);
}

#[when(regex = r#"^"([^"]+)" gracefully leaves the cluster$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String captures by value"
)]
fn peer_gracefully_leaves(world: &mut CoreWorld, name: String) {
    let signer = world.peers.signer(&name).clone();
    let table = world.tables.get_mut(&name).expect("table");
    let (leave_claim, _) = table.leave(&signer, SimInstant(400)).expect("leave");
    world.pending_claims.push(leave_claim);
}

#[when(regex = r#"^the leave claim propagates to "([^"]+)" and "([^"]+)"$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String captures by value"
)]
fn propagate_leave(world: &mut CoreWorld, p1: String, p2: String) {
    let claims = std::mem::take(&mut world.pending_claims);
    for claim in claims {
        for name in [&p1, &p2] {
            let table = world.tables.get_mut(name).expect("table");
            let _ = table
                .apply_claim(claim.clone(), &TestVerifier, SimInstant(410))
                .expect("apply leave");
        }
    }
}

#[then(regex = r#"^"([^"]+)" and "([^"]+)" see the ordered roster "([^"]+)"$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String captures by value"
)]
fn two_peers_see_roster(world: &mut CoreWorld, p1: String, p2: String, expected_csv: String) {
    let expected: Vec<String> = expected_csv.split(',').map(str::to_owned).collect();
    for name in [&p1, &p2] {
        let table = world.tables.get(name).expect("table").clone();
        let actual = names_for_roster(world, &table);
        assert_eq!(actual, expected, "roster mismatch on {name}");
    }
}

#[then(regex = r#"^"([^"]+)" and "([^"]+)" elect "([^"]+)" as leader$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String captures by value"
)]
fn two_peers_elect_leader(world: &mut CoreWorld, p1: String, p2: String, leader_name: String) {
    let expected_id = world.peers.peer_id(&leader_name);
    for name in [&p1, &p2] {
        let leader = world
            .tables
            .get(name)
            .expect("table")
            .leader()
            .expect("leader");
        assert_eq!(leader.peer_id, expected_id, "leader mismatch on {name}");
    }
}

#[when(regex = r#"^"([^"]+)" gracefully leaves the cluster and restarts with a new session$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String captures by value"
)]
fn leave_and_restart(world: &mut CoreWorld, name: String) {
    peer_gracefully_leaves(world, name.clone());
    let others: Vec<String> = world
        .tables
        .keys()
        .filter(|k| *k != &name)
        .cloned()
        .collect();
    let claims = std::mem::take(&mut world.pending_claims);
    for claim in claims {
        for other in &others {
            let table = world.tables.get_mut(other).expect("table");
            let _ = table
                .apply_claim(claim.clone(), &TestVerifier, SimInstant(410))
                .expect("apply leave");
        }
    }
}

#[when(regex = r#"^the restarted "([^"]+)" rejoins via "([^"]+)"$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String captures by value"
)]
fn rejoin_via_peer(world: &mut CoreWorld, name: String, seed_name: String) {
    let seed_snap = world.tables.get(&seed_name).expect("seed").snapshot();
    let signer = world.peers.signer(&name).clone();
    let rejoined = MembershipTable::join_after_sync(
        &signer,
        LocalNodeInit {
            session_id: SessionId(999),
            joined_at_unix_ms: 1_700_050_000_000,
            metadata: MemberMetadata::default(),
            addr_hint: None,
        },
        Some(&seed_snap),
        &TestVerifier,
        SimInstant(500),
    )
    .expect("rejoin");
    world.tables.insert(name, rejoined);
    sync_all(world, SimInstant(510));
}

#[when(regex = r#"^"([^"]+)" marks "([^"]+)" dead at incarnation (\d+)$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String captures by value"
)]
fn peer_marks_dead(world: &mut CoreWorld, reporter_name: String, target_name: String, inc: u64) {
    let target_id = world.peers.peer_id(&target_name);
    let target_view = world
        .tables
        .get(&reporter_name)
        .expect("reporter")
        .member(target_id)
        .expect("target");
    let reporter_signer = world.peers.signer(&reporter_name).clone();
    let alert = SignedIndirectAlert::sign(
        IndirectAlertPayload {
            subject: target_id,
            session_id: target_view.session_id,
            incarnation: Incarnation(inc),
            join_seq: target_view.join_seq,
            kind: IndirectKind::Dead,
            reporter: reporter_signer.peer_id(),
        },
        &reporter_signer,
    )
    .expect("sign dead");

    let names: Vec<String> = world
        .tables
        .keys()
        .filter(|k| *k != &target_name)
        .cloned()
        .collect();
    for name in names {
        let signer = world.peers.signer(&name).clone();
        let table = world.tables.get_mut(&name).expect("table");
        let _ = table
            .apply_indirect_alert(&alert, &TestVerifier, &signer, SimInstant(600))
            .expect("apply dead");
    }
    world.pending_claims.clear();
    let encoded = postcard::to_allocvec(&alert).expect("encode alert");
    world.last_encoded = Some(encoded.into());
}

#[when(regex = r#"^"([^"]+)" still running its original session receives the dead claim$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String captures by value"
)]
fn original_session_receives_dead(world: &mut CoreWorld, target_name: String) {
    let bytes = world.last_encoded.as_ref().expect("stashed dead alert");
    let alert: SignedIndirectAlert = postcard::from_bytes(bytes).expect("decode alert");
    let signer = world.peers.signer(&target_name).clone();
    let table = world.tables.get_mut(&target_name).expect("target table");
    let outcome = table
        .apply_indirect_alert(&alert, &TestVerifier, &signer, SimInstant(610))
        .expect("target apply dead");
    if let Some(refute) = outcome.refutation {
        world.pending_claims.push(refute);
    }
}

#[then(regex = r#"^"([^"]+)" refutes with incarnation (\d+) and retains join sequence (\d+)$"#)]
fn check_refute_and_seq(world: &mut CoreWorld, name: String, inc: u64, join_seq: u64) {
    assert!(!world.pending_claims.is_empty(), "expected refutation");
    check_inc_and_seq(world, name, inc, join_seq);
}

#[then(regex = r#"^after sync "([^"]+)" and "([^"]+)" restore "([^"]+)" as leader$"#)]
fn sync_restores_leader(world: &mut CoreWorld, p1: String, p2: String, leader_name: String) {
    sync_all(world, SimInstant(620));
    two_peers_elect_leader(world, p1, p2, leader_name);
}

#[given(regex = r#"^"([^"]+)" and "([^"]+)" independently found clusters at join sequence (\d+)$"#)]
fn independent_founders(world: &mut CoreWorld, a: String, b: String, seq: u64) {
    let _ = seq;
    world.peer_order = vec![a.clone(), b.clone()];
    for (idx, name) in [a, b].into_iter().enumerate() {
        let signer = world.peers.signer(&name).clone();
        let table = MembershipTable::found(
            &signer,
            LocalNodeInit {
                session_id: SessionId(1 + idx as u64),
                joined_at_unix_ms: 1_700_000_000_000,
                metadata: MemberMetadata::default(),
                addr_hint: None,
            },
            SimInstant(100),
        )
        .expect("found");
        world.tables.insert(name, table);
    }
}

#[when(regex = r#"^"([^"]+)" and "([^"]+)" synchronize snapshots$"#)]
fn two_sync(world: &mut CoreWorld, a: String, b: String) {
    let _ = (a, b);
    sync_all(world, SimInstant(200));
}

#[then("both peers agree on the identical ordered roster and single leader")]
fn both_agree_identical(world: &mut CoreWorld) {
    let tables: Vec<_> = world.tables.values().collect();
    assert_eq!(tables.len(), 2);
    let r0: Vec<_> = tables[0]
        .members_ordered()
        .into_iter()
        .map(|m| m.peer_id)
        .collect();
    let r1: Vec<_> = tables[1]
        .members_ordered()
        .into_iter()
        .map(|m| m.peer_id)
        .collect();
    assert_eq!(r0, r1);
    assert_eq!(r0.len(), 2);
    assert_eq!(
        tables[0].leader().map(|m| m.peer_id),
        tables[1].leader().map(|m| m.peer_id)
    );
    assert!(tables[0].leader().is_some());
}

#[given(regex = r#"^peer "([^"]+)" joined at sequence (\d+) with wall clock (\d+)$"#)]
fn peer_with_wall_clock(world: &mut CoreWorld, name: String, seq: u64, unix_ms: u64) {
    if !world.peer_order.contains(&name) {
        world.peer_order.push(name.clone());
    }
    let signer = world.peers.signer(&name).clone();
    let init = LocalNodeInit {
        session_id: SessionId(seq),
        joined_at_unix_ms: unix_ms,
        metadata: MemberMetadata::default(),
        addr_hint: None,
    };
    let table = if seq == 1 {
        MembershipTable::found(&signer, init, SimInstant(100)).expect("found")
    } else {
        let prev = world
            .tables
            .values()
            .next()
            .expect("existing peer")
            .snapshot();
        MembershipTable::join_after_sync(&signer, init, Some(&prev), &TestVerifier, SimInstant(100))
            .expect("join")
    };
    world.tables.insert(name, table);
}

#[when("both peers synchronize")]
fn both_sync(world: &mut CoreWorld) {
    sync_all(world, SimInstant(200));
}

#[then(regex = r#"^both peers elect "([^"]+)" as leader despite skewed wall clocks$"#)]
fn both_elect_despite_skew(world: &mut CoreWorld, leader_name: String) {
    every_peer_elects_leader(world, leader_name);
}

#[when(regex = r#"^"([^"]+)" and "([^"]+)" are marked dead on "([^"]+)"$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String captures by value"
)]
fn mark_two_dead_on_peer(world: &mut CoreWorld, d1: String, d2: String, on_peer: String) {
    let reporter = world.peers.signer(&on_peer).clone();
    for dead_name in [d1, d2] {
        let target_id = world.peers.peer_id(&dead_name);
        let view = world
            .tables
            .get(&on_peer)
            .expect("table")
            .member(target_id)
            .expect("member");
        let alert = SignedIndirectAlert::sign(
            IndirectAlertPayload {
                subject: target_id,
                session_id: view.session_id,
                incarnation: view.incarnation,
                join_seq: view.join_seq,
                kind: IndirectKind::Dead,
                reporter: reporter.peer_id(),
            },
            &reporter,
        )
        .expect("sign dead");
        let _ = world
            .tables
            .get_mut(&on_peer)
            .expect("table")
            .apply_indirect_alert(&alert, &TestVerifier, &reporter, SimInstant(700))
            .expect("apply dead");
    }
}

#[then(regex = r#"^"([^"]+)" sees advisory leader "([^"]+)"$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String captures by value"
)]
fn peer_sees_advisory_leader(world: &mut CoreWorld, on_peer: String, expected_leader: String) {
    let expected_id = world.peers.peer_id(&expected_leader);
    let leader = world
        .tables
        .get(&on_peer)
        .expect("table")
        .leader()
        .expect("advisory leader");
    assert_eq!(leader.peer_id, expected_id);
}

#[then(regex = r#"^"([^"]+)" with quorum (\d+) sees no leader$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String captures by value"
)]
fn peer_with_quorum_sees_none(world: &mut CoreWorld, on_peer: String, quorum: usize) {
    assert!(
        world
            .tables
            .get(&on_peer)
            .expect("table")
            .leader_with_quorum(quorum)
            .is_none()
    );
}

#[when(regex = r#"^"([^"]+)" is marked dead at time (\d+) and reaped at time (\d+)$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String captures by value"
)]
fn mark_dead_and_reap(world: &mut CoreWorld, target_name: String, dead_ms: u64, reap_ms: u64) {
    let stale_snap = world.tables.get(&target_name).expect("target").snapshot();
    world.last_encoded = Some(
        postcard::to_allocvec(&stale_snap)
            .expect("encode snap")
            .into(),
    );

    let alice_signer = world.peers.signer("alice").clone();
    let target_id = world.peers.peer_id(&target_name);
    let view = world
        .tables
        .get("alice")
        .expect("alice")
        .member(target_id)
        .expect("member");
    let alert = SignedIndirectAlert::sign(
        IndirectAlertPayload {
            subject: target_id,
            session_id: view.session_id,
            incarnation: view.incarnation,
            join_seq: view.join_seq,
            kind: IndirectKind::Dead,
            reporter: alice_signer.peer_id(),
        },
        &alice_signer,
    )
    .expect("sign dead");

    let alice_table = world.tables.get_mut("alice").expect("alice");
    let _ = alice_table
        .apply_indirect_alert(&alert, &TestVerifier, &alice_signer, SimInstant(dead_ms))
        .expect("apply dead");
    let reaped = alice_table.reap_expired(SimInstant(reap_ms), 30_000, 30_000);
    assert_eq!(reaped, vec![target_id]);
}

#[when(regex = r#"^a stale snapshot containing "([^"]+)" at the dead session is merged$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String captures by value"
)]
fn merge_stale_snapshot(world: &mut CoreWorld, target_name: String) {
    let _ = target_name;
    let raw = world.last_encoded.as_ref().expect("stale snapshot");
    let snap: membermesh_core::SyncSnapshot = postcard::from_bytes(raw).expect("decode snap");
    let alice_signer = world.peers.signer("alice").clone();
    let alice_table = world.tables.get_mut("alice").expect("alice");
    let _ = alice_table
        .merge_snapshot(&snap, &TestVerifier, &alice_signer, SimInstant(70_100))
        .expect("merge stale");
}

#[then(regex = r#"^"([^"]+)" remains absent from the ordered roster$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String captures by value"
)]
fn remains_absent(world: &mut CoreWorld, target_name: String) {
    let target_id = world.peers.peer_id(&target_name);
    let alice_table = world.tables.get("alice").expect("alice");
    assert!(alice_table.member(target_id).is_none());
    assert!(
        alice_table
            .members_ordered()
            .into_iter()
            .all(|m| m.peer_id != target_id)
    );
}
