//! Stateright exhaustive model checker for the Sans-I/O SWIM + Lifeguard `Engine`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::collections::{BTreeMap, BTreeSet};

use membermesh_core::{
    ClaimSigner, ClaimVerifier, ControlMessage, CoreError, Engine, EngineAction, EngineInput,
    LocalNodeInit, MemberMetadata, MemberStatus, PeerId, ProtocolConfig, SessionId, SimInstant,
    SyncSnapshot,
};
use serde::{Deserialize, Serialize};
use stateright::{Checker as _, Model, Property};

#[derive(Clone, Debug)]
struct FastSigner(PeerId);

impl FastSigner {
    fn from_index(idx: u8) -> Self {
        Self(PeerId::from_bytes([idx + 10; 32]))
    }
}

impl ClaimSigner for FastSigner {
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
struct FastVerifier;

impl ClaimVerifier for FastVerifier {
    fn verify(
        &self,
        peer_id: PeerId,
        payload: &[u8],
        signature: &[u8; 64],
    ) -> Result<(), CoreError> {
        let hash = blake3::hash(payload);
        if &signature[..32] == peer_id.as_bytes() && &signature[32..64] == hash.as_bytes() {
            Ok(())
        } else {
            Err(CoreError::InvalidSignature {
                peer_short: peer_id.fmt_short(),
            })
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
enum Msg {
    Gossip(Vec<u8>),
    Control { from: usize, bytes: Vec<u8> },
    SyncReq { from: usize, bytes: Vec<u8> },
    SyncResp { from: usize, bytes: Vec<u8> },
}

/// Abstract state of a 3-node SWIM `Engine` network under Stateright.
///
/// Reconstructs `Engine` instances deterministically from compact replay logs
/// `(partitioned, inputs)` so `Model::State` is small, `Hash + Eq`, and fast.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct EngineModelState {
    partitioned: bool,
    step_count: u8,
    /// Per-node `(time_ms, encoded_input_kind)` replay trace? Even cleaner:
    /// store the per-node `SyncSnapshot` + local clock advances, OR replay input history!
    /// Since max depth is 6 steps, replaying 0..=6 inputs per node takes microseconds!
    node_traces: [Vec<(u64, TraceInput)>; 3],
    in_flight: BTreeSet<(usize, Msg)>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum TraceInput {
    Tick,
    Gossip(Vec<u8>),
    Control { from: usize, bytes: Vec<u8> },
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum EngineModelAction {
    AdvanceTick { node: usize, delta_ms: u64 },
    Deliver { to: usize, msg: Msg },
    DropMsg { to: usize, msg: Msg },
    TogglePartition(bool),
}

struct EngineSwimModel {
    signers: [FastSigner; 3],
    config: ProtocolConfig,
}

impl EngineSwimModel {
    fn new() -> Self {
        Self {
            signers: [
                FastSigner::from_index(0),
                FastSigner::from_index(1),
                FastSigner::from_index(2),
            ],
            config: ProtocolConfig::lan(),
        }
    }

    fn peer_idx(&self, id: PeerId) -> Option<usize> {
        self.signers.iter().position(|s| s.peer_id() == id)
    }

    fn replay_node(
        &self,
        idx: usize,
        trace: &[(u64, TraceInput)],
    ) -> (Engine<FastSigner, FastVerifier>, u64) {
        let mut init_engines: Vec<Engine<FastSigner, FastVerifier>> = Vec::with_capacity(3);
        let mut prev_snap: Option<SyncSnapshot> = None;
        for i in 0..3 {
            let init = LocalNodeInit {
                session_id: SessionId((i as u64) + 1),
                joined_at_unix_ms: 1_700_000_000_000,
                metadata: MemberMetadata::default(),
                addr_hint: None,
                admission_token: None,
            };
            let (eng, _) = match prev_snap.as_ref() {
                None => Engine::found(
                    self.signers[i].clone(),
                    FastVerifier,
                    init,
                    self.config.clone(),
                    SimInstant(0),
                )
                .expect("found"),
                Some(snap) => Engine::join_after_sync(
                    self.signers[i].clone(),
                    FastVerifier,
                    init,
                    Some(snap),
                    self.config.clone(),
                    SimInstant(0),
                )
                .expect("join"),
            };
            prev_snap = Some(eng.table().snapshot());
            init_engines.push(eng);
        }

        let mut engine = init_engines.swap_remove(idx);
        let mut last_t = 0_u64;
        for (t, inp) in trace {
            last_t = *t;
            let input = match inp {
                TraceInput::Tick => EngineInput::Tick,
                TraceInput::Gossip(bytes) => EngineInput::GossipReceived(bytes.clone().into()),
                TraceInput::Control { from, bytes } => {
                    let msg = membermesh_core::decode_control(
                        bytes,
                        membermesh_core::DEFAULT_MAX_CONTROL_BYTES,
                    )
                    .expect("decode control");
                    EngineInput::ControlReceived {
                        from: self.signers[*from].peer_id(),
                        msg,
                    }
                }
            };
            let _ = engine.handle_input(SimInstant(*t), input);
        }
        (engine, last_t)
    }

    fn apply_and_collect(
        &self,
        state: &mut EngineModelState,
        idx: usize,
        now_ms: u64,
        trace_input: &TraceInput,
    ) {
        let mut trace = state.node_traces[idx].clone();
        trace.push((now_ms, trace_input.clone()));
        let (mut eng, _) = self.replay_node(idx, &state.node_traces[idx]);
        let input = match trace_input {
            TraceInput::Tick => EngineInput::Tick,
            TraceInput::Gossip(bytes) => EngineInput::GossipReceived(bytes.clone().into()),
            TraceInput::Control { from, bytes } => {
                let msg = membermesh_core::decode_control(
                    bytes,
                    membermesh_core::DEFAULT_MAX_CONTROL_BYTES,
                )
                .expect("decode");
                EngineInput::ControlReceived {
                    from: self.signers[*from].peer_id(),
                    msg,
                }
            }
        };
        let actions = eng.handle_input(SimInstant(now_ms), input);
        state.node_traces[idx] = trace;

        for act in actions {
            if state.in_flight.len() >= 8 {
                break;
            }
            match act {
                EngineAction::BroadcastGossip(bytes) => {
                    for to in 0..3 {
                        if to != idx {
                            state.in_flight.insert((to, Msg::Gossip(bytes.to_vec())));
                        }
                    }
                }
                EngineAction::SendControl { to, msg } => {
                    if let Some(to_idx) = self.peer_idx(to) {
                        let bytes = membermesh_core::encode_control(
                            &msg,
                            membermesh_core::DEFAULT_MAX_CONTROL_BYTES,
                        )
                        .expect("enc");
                        state.in_flight.insert((
                            to_idx,
                            Msg::Control {
                                from: idx,
                                bytes: bytes.to_vec(),
                            },
                        ));
                    }
                }
                EngineAction::InitiateSync { to, local_snapshot } => {
                    if let Some(to_idx) = self.peer_idx(to) {
                        let bytes = membermesh_core::encode_control(
                            &ControlMessage::PushPullSync(Box::new(local_snapshot)),
                            membermesh_core::DEFAULT_MAX_CONTROL_BYTES,
                        )
                        .expect("enc");
                        state.in_flight.insert((
                            to_idx,
                            Msg::SyncReq {
                                from: idx,
                                bytes: bytes.to_vec(),
                            },
                        ));
                    }
                }
                EngineAction::EmitEvent(_) | EngineAction::ScheduleWakeup(_) | _ => {}
            }
        }
    }
}

impl Model for EngineSwimModel {
    type State = EngineModelState;
    type Action = EngineModelAction;

    fn init_states(&self) -> Vec<Self::State> {
        // Seed node 2's initial claim broadcast to node 0 & 1 so cluster starts connected.
        let mut init_in_flight = BTreeSet::new();
        let (eng2, _) = self.replay_node(2, &[]);
        let snap2 = eng2.table().snapshot();
        let bytes = membermesh_core::encode_control(
            &ControlMessage::PushPullSync(Box::new(snap2)),
            membermesh_core::DEFAULT_MAX_CONTROL_BYTES,
        )
        .expect("enc");
        init_in_flight.insert((
            0,
            Msg::SyncReq {
                from: 2,
                bytes: bytes.to_vec(),
            },
        ));
        init_in_flight.insert((
            1,
            Msg::SyncReq {
                from: 2,
                bytes: bytes.to_vec(),
            },
        ));

        vec![EngineModelState {
            partitioned: false,
            step_count: 0,
            node_traces: [Vec::new(), Vec::new(), Vec::new()],
            in_flight: init_in_flight,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        if state.step_count >= 4 {
            return;
        }
        if !state.partitioned {
            actions.push(EngineModelAction::TogglePartition(true));
        }
        for node in 0..3 {
            actions.push(EngineModelAction::AdvanceTick {
                node,
                delta_ms: 500,
            });
        }
        for (to, msg) in &state.in_flight {
            actions.push(EngineModelAction::Deliver {
                to: *to,
                msg: msg.clone(),
            });
            actions.push(EngineModelAction::DropMsg {
                to: *to,
                msg: msg.clone(),
            });
        }
    }

    fn next_state(&self, last_state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut next = last_state.clone();
        next.step_count = next.step_count.saturating_add(1);
        match action {
            EngineModelAction::TogglePartition(p) => {
                next.partitioned = p;
            }
            EngineModelAction::AdvanceTick { node, delta_ms } => {
                let (_, last_t) = self.replay_node(node, &last_state.node_traces[node]);
                self.apply_and_collect(&mut next, node, last_t + delta_ms, &TraceInput::Tick);
            }
            EngineModelAction::Deliver { to, msg } => {
                next.in_flight.remove(&(to, msg.clone()));
                let (_, last_t) = self.replay_node(to, &last_state.node_traces[to]);
                let from = match &msg {
                    Msg::Gossip(_) => None,
                    Msg::Control { from, .. }
                    | Msg::SyncReq { from, .. }
                    | Msg::SyncResp { from, .. } => Some(*from),
                };
                // When partitioned: isolate node 0 from {1, 2}.
                if next.partitioned
                    && let Some(src) = from
                    && ((src == 0) != (to == 0))
                {
                    return Some(next);
                }
                match msg {
                    Msg::Gossip(bytes) => {
                        self.apply_and_collect(
                            &mut next,
                            to,
                            last_t + 10,
                            &TraceInput::Gossip(bytes),
                        );
                    }
                    Msg::Control { from, bytes } | Msg::SyncResp { from, bytes } => {
                        self.apply_and_collect(
                            &mut next,
                            to,
                            last_t + 10,
                            &TraceInput::Control { from, bytes },
                        );
                    }
                    Msg::SyncReq { from, bytes } => {
                        self.apply_and_collect(
                            &mut next,
                            to,
                            last_t + 10,
                            &TraceInput::Control { from, bytes },
                        );
                        let (eng, _) = self.replay_node(to, &next.node_traces[to]);
                        let reply = membermesh_core::encode_control(
                            &ControlMessage::PushPullSync(Box::new(eng.table().snapshot())),
                            membermesh_core::DEFAULT_MAX_CONTROL_BYTES,
                        )
                        .expect("enc");
                        next.in_flight.insert((
                            from,
                            Msg::SyncResp {
                                from: to,
                                bytes: reply.to_vec(),
                            },
                        ));
                    }
                }
            }
            EngineModelAction::DropMsg { to, msg } => {
                next.in_flight.remove(&(to, msg));
            }
        }
        Some(next)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::<Self>::always("total_order_agreement", |model, state| {
                let engines: Vec<_> = (0..3)
                    .map(|i| model.replay_node(i, &state.node_traces[i]).0)
                    .collect();
                for i in 0..3 {
                    for j in (i + 1)..3 {
                        let oi: Vec<PeerId> = engines[i]
                            .members_ordered()
                            .into_iter()
                            .map(|m| m.peer_id)
                            .collect();
                        let oj: Vec<PeerId> = engines[j]
                            .members_ordered()
                            .into_iter()
                            .map(|m| m.peer_id)
                            .collect();
                        let si: BTreeSet<PeerId> = oi.iter().copied().collect();
                        let sj: BTreeSet<PeerId> = oj.iter().copied().collect();
                        if si == sj
                            && (oi != oj
                                || engines[i].leader().map(|l| l.peer_id)
                                    != engines[j].leader().map(|l| l.peer_id))
                        {
                            return false;
                        }
                    }
                }
                true
            }),
            Property::<Self>::always("seniority_preserved_on_refute", |model, state| {
                let (eng0, _) = model.replay_node(0, &state.node_traces[0]);
                eng0.table()
                    .member(model.signers[0].peer_id())
                    .is_some_and(|v| v.join_seq == 1)
            }),
            Property::<Self>::always("settled_quorum_exclusivity", |model, state| {
                let engines: Vec<_> = (0..3)
                    .map(|i| model.replay_node(i, &state.node_traces[i]).0)
                    .collect();
                // Settled partition check: only assert when node 0 has marked {1,2} Dead
                // or {1,2} have marked 0 Dead.
                let n0_dead_others = [1, 2].iter().all(|&j| {
                    engines[0]
                        .table()
                        .member(model.signers[j].peer_id())
                        .is_some_and(|v| v.status == MemberStatus::Dead)
                });
                let settled = !state.partitioned || n0_dead_others;
                if !settled {
                    return true;
                }
                let mut quorum_holders = BTreeMap::new();
                for (idx, eng) in engines.iter().enumerate() {
                    if let Some(l) = eng.leader_with_quorum(2)
                        && l.peer_id == model.signers[idx].peer_id()
                    {
                        quorum_holders.insert(idx, l.peer_id);
                    }
                }
                quorum_holders.len() <= 1
            }),
            Property::<Self>::sometimes("all_nodes_converge", |model, state| {
                let engines: Vec<_> = (0..3)
                    .map(|i| model.replay_node(i, &state.node_traces[i]).0)
                    .collect();
                let r0: Vec<PeerId> = engines[0]
                    .members_ordered()
                    .into_iter()
                    .map(|m| m.peer_id)
                    .collect();
                let r1: Vec<PeerId> = engines[1]
                    .members_ordered()
                    .into_iter()
                    .map(|m| m.peer_id)
                    .collect();
                let r2: Vec<PeerId> = engines[2]
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
fn stateright_swim_engine_properties_hold() {
    let model = EngineSwimModel::new();
    let checker = model.checker().threads(2).spawn_bfs().join();
    checker.assert_properties();
}
