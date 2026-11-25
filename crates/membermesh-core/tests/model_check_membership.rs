//! Stateright exhaustive model checker for `MembershipTable` CRDT invariants.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::collections::BTreeSet;

use membermesh_core::{
    ClaimSigner, ClaimVerifier, CoreError, Incarnation, IndirectAlertPayload, IndirectKind,
    LocalNodeInit, MemberMetadata, MembershipTable, PeerId, SessionId, SignedClaim,
    SignedIndirectAlert, SimInstant, SyncSnapshot,
};
use serde::{Deserialize, Serialize};
use stateright::{Checker as _, Model, Property};

/// Fast deterministic signer for Stateright state-space exploration so CPU is
/// spent exploring protocol interleavings rather than Ed25519 curve arithmetic.
#[derive(Clone, Debug)]
struct ModelSigner(PeerId);

impl ModelSigner {
    fn from_index(idx: u8) -> Self {
        Self(PeerId::from_bytes([idx + 1; 32]))
    }
}

impl ClaimSigner for ModelSigner {
    fn peer_id(&self) -> PeerId {
        self.0
    }

    fn sign(&self, payload: &[u8]) -> [u8; 64] {
        let mut sig = [0u8; 64];
        let hash = blake3::hash(payload);
        sig[..32].copy_from_slice(self.0.as_bytes());
        sig[32..64].copy_from_slice(hash.as_bytes());
        sig
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct ModelVerifier;

impl ClaimVerifier for ModelVerifier {
    fn verify(
        &self,
        peer_id: PeerId,
        payload: &[u8],
        signature: &[u8; 64],
    ) -> Result<(), CoreError> {
        let expected_hash = blake3::hash(payload);
        if &signature[..32] == peer_id.as_bytes() && &signature[32..64] == expected_hash.as_bytes()
        {
            Ok(())
        } else {
            Err(CoreError::InvalidSignature {
                peer_short: peer_id.fmt_short(),
            })
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum NetworkMsg {
    Snapshot(SyncSnapshot),
    Alert(SignedIndirectAlert),
    Claim(SignedClaim),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ClusterModelState {
    node_snaps: [Vec<u8>; 3],
    in_flight: BTreeSet<Vec<u8>>,
    node0_seq_preserved: bool,
    step_count: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ClusterAction {
    SendSync(usize),
    Deliver { to: usize, msg_bytes: Vec<u8> },
    Suspect { reporter: usize, target: usize },
}

struct MembershipClusterModel {
    signers: [ModelSigner; 3],
}

impl MembershipClusterModel {
    fn new() -> Self {
        Self {
            signers: [
                ModelSigner::from_index(0),
                ModelSigner::from_index(1),
                ModelSigner::from_index(2),
            ],
        }
    }

    fn decode_table(&self, idx: usize, snap_bytes: &[u8]) -> MembershipTable {
        let snap: SyncSnapshot = postcard::from_bytes(snap_bytes).expect("decode snap");
        let signer = &self.signers[idx];
        let join_seq = (idx as u64) + 1;
        let init = LocalNodeInit {
            session_id: SessionId(join_seq),
            joined_at_unix_ms: 1_700_000_000_000,
            metadata: MemberMetadata::default(),
            addr_hint: None,
            admission_token: None,
        };
        let mut table = if idx == 0 {
            MembershipTable::found(signer, init, SimInstant(0)).expect("found")
        } else {
            let dummy_claim = membermesh_core::MemberClaim {
                subject: signer.peer_id(),
                session_id: SessionId(join_seq),
                incarnation: Incarnation(1),
                join_seq,
                joined_at_unix_ms: 1_700_000_000_000,
                status: membermesh_core::MemberStatus::Alive,
                metadata: MemberMetadata::default(),
                addr_hint: None,
                admission_token: None,
            };
            let signed_claim = SignedClaim::sign(dummy_claim, signer).expect("sign");
            MembershipTable::join_after_sync(
                signer,
                init,
                Some(&SyncSnapshot {
                    sender: signer.peer_id(),
                    lamport_clock: idx as u64,
                    claims: vec![signed_claim],
                    indirect_alerts: vec![],
                }),
                &ModelVerifier,
                SimInstant(0),
            )
            .expect("join")
        };
        let _ = table
            .merge_snapshot(&snap, &ModelVerifier, signer, SimInstant(0))
            .expect("merge");
        table
    }
}

impl Model for MembershipClusterModel {
    type State = ClusterModelState;
    type Action = ClusterAction;

    fn init_states(&self) -> Vec<Self::State> {
        let t0 = MembershipTable::found(
            &self.signers[0],
            LocalNodeInit {
                session_id: SessionId(1),
                joined_at_unix_ms: 1_700_000_000_000,
                metadata: MemberMetadata::default(),
                addr_hint: None,
                admission_token: None,
            },
            SimInstant(0),
        )
        .expect("t0");
        let s0 = t0.snapshot();
        let t1 = MembershipTable::join_after_sync(
            &self.signers[1],
            LocalNodeInit {
                session_id: SessionId(2),
                joined_at_unix_ms: 1_700_000_000_000,
                metadata: MemberMetadata::default(),
                addr_hint: None,
                admission_token: None,
            },
            Some(&s0),
            &ModelVerifier,
            SimInstant(0),
        )
        .expect("t1");
        let s1 = t1.snapshot();
        let t2 = MembershipTable::join_after_sync(
            &self.signers[2],
            LocalNodeInit {
                session_id: SessionId(3),
                joined_at_unix_ms: 1_700_000_000_000,
                metadata: MemberMetadata::default(),
                addr_hint: None,
                admission_token: None,
            },
            Some(&s1),
            &ModelVerifier,
            SimInstant(0),
        )
        .expect("t2");

        vec![ClusterModelState {
            node_snaps: [
                postcard::to_allocvec(&t0.snapshot()).expect("enc"),
                postcard::to_allocvec(&t1.snapshot()).expect("enc"),
                postcard::to_allocvec(&t2.snapshot()).expect("enc"),
            ],
            in_flight: BTreeSet::new(),
            node0_seq_preserved: true,
            step_count: 0,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        if state.step_count >= 6 {
            return;
        }
        for idx in 0..3 {
            actions.push(ClusterAction::SendSync(idx));
            for msg_bytes in &state.in_flight {
                actions.push(ClusterAction::Deliver {
                    to: idx,
                    msg_bytes: msg_bytes.clone(),
                });
            }
        }
        let t1 = self.decode_table(1, &state.node_snaps[1]);
        if let Some(n0_view) = t1.member(self.signers[0].peer_id())
            && n0_view.incarnation == Incarnation(1)
            && n0_view.status == membermesh_core::MemberStatus::Alive
        {
            actions.push(ClusterAction::Suspect {
                reporter: 1,
                target: 0,
            });
        }
    }

    fn next_state(&self, last_state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut next = last_state.clone();
        next.step_count = next.step_count.saturating_add(1);
        match action {
            ClusterAction::SendSync(from) => {
                if next.in_flight.len() >= 4 {
                    return None;
                }
                let snap: SyncSnapshot =
                    postcard::from_bytes(&last_state.node_snaps[from]).expect("decode");
                let bytes = postcard::to_allocvec(&NetworkMsg::Snapshot(snap)).expect("enc");
                if !next.in_flight.insert(bytes) {
                    return None;
                }
            }
            ClusterAction::Deliver { to, msg_bytes } => {
                let msg: NetworkMsg = postcard::from_bytes(&msg_bytes).expect("decode");
                let mut table = self.decode_table(to, &last_state.node_snaps[to]);
                let outcome = match msg {
                    NetworkMsg::Snapshot(snap) => table
                        .merge_snapshot(&snap, &ModelVerifier, &self.signers[to], SimInstant(10))
                        .expect("merge"),
                    NetworkMsg::Alert(alert) => table
                        .apply_indirect_alert(
                            &alert,
                            &ModelVerifier,
                            &self.signers[to],
                            SimInstant(10),
                        )
                        .expect("alert"),
                    NetworkMsg::Claim(claim) => table
                        .apply_claim(claim, &ModelVerifier, SimInstant(10))
                        .expect("claim"),
                };
                if !outcome.changed && outcome.refutation.is_none() {
                    return None;
                }
                if let Some(refute) = outcome.refutation {
                    let bytes =
                        postcard::to_allocvec(&NetworkMsg::Claim(refute)).expect("enc refute");
                    next.in_flight.insert(bytes);
                }
                next.node_snaps[to] = postcard::to_allocvec(&table.snapshot()).expect("enc");
            }
            ClusterAction::Suspect { reporter, target } => {
                if next.in_flight.len() >= 4 {
                    return None;
                }
                let mut table = self.decode_table(reporter, &last_state.node_snaps[reporter]);
                let target_id = self.signers[target].peer_id();
                let view = table.member(target_id)?;
                let alert = SignedIndirectAlert::sign(
                    IndirectAlertPayload {
                        subject: target_id,
                        session_id: view.session_id,
                        incarnation: view.incarnation,
                        join_seq: view.join_seq,
                        kind: IndirectKind::Suspect,
                        reporter: self.signers[reporter].peer_id(),
                    },
                    &self.signers[reporter],
                )
                .expect("sign");
                let _ = table
                    .apply_indirect_alert(
                        &alert,
                        &ModelVerifier,
                        &self.signers[reporter],
                        SimInstant(10),
                    )
                    .expect("apply");
                next.node_snaps[reporter] = postcard::to_allocvec(&table.snapshot()).expect("enc");
                let bytes = postcard::to_allocvec(&NetworkMsg::Alert(alert)).expect("enc alert");
                next.in_flight.insert(bytes);
            }
        }

        let t0 = self.decode_table(0, &next.node_snaps[0]);
        if t0
            .member(self.signers[0].peer_id())
            .is_none_or(|v| v.join_seq != 1)
        {
            next.node0_seq_preserved = false;
        }
        Some(next)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::<Self>::always("total_order_agreement", |model, state| {
                let tables = [
                    model.decode_table(0, &state.node_snaps[0]),
                    model.decode_table(1, &state.node_snaps[1]),
                    model.decode_table(2, &state.node_snaps[2]),
                ];
                for i in 0..3 {
                    for j in (i + 1)..3 {
                        let oi: Vec<PeerId> = tables[i]
                            .members_ordered()
                            .into_iter()
                            .map(|m| m.peer_id)
                            .collect();
                        let oj: Vec<PeerId> = tables[j]
                            .members_ordered()
                            .into_iter()
                            .map(|m| m.peer_id)
                            .collect();
                        let si: BTreeSet<PeerId> = oi.iter().copied().collect();
                        let sj: BTreeSet<PeerId> = oj.iter().copied().collect();
                        if si == sj
                            && (oi != oj
                                || tables[i].leader().map(|l| l.peer_id)
                                    != tables[j].leader().map(|l| l.peer_id))
                        {
                            return false;
                        }
                    }
                }
                true
            }),
            Property::<Self>::always("seniority_preserved_on_refute", |_model, state| {
                state.node0_seq_preserved
            }),
            Property::<Self>::always("quorum_exclusivity", |model, state| {
                let mut quorum_self_leaders = 0usize;
                for idx in 0..3 {
                    let t = model.decode_table(idx, &state.node_snaps[idx]);
                    if t.leader_with_quorum(2)
                        .is_some_and(|l| l.peer_id == model.signers[idx].peer_id())
                    {
                        quorum_self_leaders += 1;
                    }
                }
                quorum_self_leaders <= 1
            }),
            Property::<Self>::sometimes("all_nodes_converge", |model, state| {
                let tables = [
                    model.decode_table(0, &state.node_snaps[0]),
                    model.decode_table(1, &state.node_snaps[1]),
                    model.decode_table(2, &state.node_snaps[2]),
                ];
                let r0: Vec<PeerId> = tables[0]
                    .members_ordered()
                    .into_iter()
                    .map(|m| m.peer_id)
                    .collect();
                let r1: Vec<PeerId> = tables[1]
                    .members_ordered()
                    .into_iter()
                    .map(|m| m.peer_id)
                    .collect();
                let r2: Vec<PeerId> = tables[2]
                    .members_ordered()
                    .into_iter()
                    .map(|m| m.peer_id)
                    .collect();
                r0.len() == 3 && r0 == r1 && r1 == r2
            }),
        ]
    }
}

#[test]
fn stateright_membership_crdt_properties_hold() {
    let model = MembershipClusterModel::new();
    let checker = model.checker().threads(2).spawn_bfs().join();
    checker.assert_properties();
}
