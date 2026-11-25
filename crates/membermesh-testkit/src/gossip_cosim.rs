use std::{
    cmp::Reverse,
    collections::{BTreeMap, BinaryHeap},
    time::Duration,
};

use iroh_gossip::proto::{
    Config as GossipProtoConfig, IO as GossipIO, InEvent as GossipInEvent,
    Message as GossipProtoMessage, PeerData, State as GossipProtoState, Timer as GossipTimer,
    TopicId,
    topic::{Command as GossipCommand, Event as GossipTopicEvent},
};
use membermesh_core::{
    ClaimSigner as _, ControlMessage, Engine, EngineAction, EngineInput, LocalNodeInit,
    MemberMetadata, PeerId, ProtocolConfig, SessionId, SimInstant, SyncSnapshot,
};
use n0_future::time::Instant as N0Instant;
use rand::{RngExt as _, SeedableRng as _, rngs::StdRng};

use crate::crypto::{TestSigner, TestVerifier};

#[derive(Clone, Debug)]
enum CosimEvent {
    EngineWakeup {
        peer: PeerId,
    },
    GossipTimer {
        peer: PeerId,
        timer: GossipTimer<PeerId>,
    },
    DeliverGossipProto {
        to: PeerId,
        from: PeerId,
        msg: GossipProtoMessage<PeerId>,
    },
    DeliverControl {
        to: PeerId,
        from: PeerId,
        msg: ControlMessage,
    },
    DeliverSync {
        to: PeerId,
        from: PeerId,
        snapshot: SyncSnapshot,
        is_reply: bool,
    },
}

#[derive(Clone, Debug)]
struct TimedCosimItem {
    at: SimInstant,
    seq: u64,
    event: CosimEvent,
}

impl PartialEq for TimedCosimItem {
    fn eq(&self, other: &Self) -> bool {
        (self.at, self.seq) == (other.at, other.seq)
    }
}

impl Eq for TimedCosimItem {}

impl PartialOrd for TimedCosimItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimedCosimItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.at, self.seq).cmp(&(other.at, other.seq))
    }
}

#[derive(Default)]
struct GossipIoCollector {
    sends: Vec<(PeerId, GossipProtoMessage<PeerId>)>,
    emits: Vec<GossipTopicEvent<PeerId>>,
    timers: Vec<(Duration, GossipTimer<PeerId>)>,
}

impl GossipIO<PeerId> for GossipIoCollector {
    fn send(
        &mut self,
        peer: PeerId,
        message: GossipProtoMessage<PeerId>,
    ) -> Option<GossipProtoMessage<PeerId>> {
        self.sends.push((peer, message));
        None
    }

    fn emit(&mut self, _topic: TopicId, event: GossipTopicEvent<PeerId>) {
        self.emits.push(event);
    }

    fn schedule(&mut self, delay: Duration, timer: GossipTimer<PeerId>) {
        self.timers.push((delay, timer));
    }

    fn peer_data(&mut self, _peer: PeerId, _data: PeerData) {}
}

struct CosimNode {
    engine: Engine<TestSigner, TestVerifier>,
    gossip: GossipProtoState<PeerId, StdRng>,
    crashed: bool,
}

/// Co-simulator wiring [`membermesh_core::Engine`] directly onto `iroh-gossip`'s
/// real `HyParView` + `PlumTree` protocol state machine ([`GossipProtoState`]).
pub struct GossipCosimCluster {
    now: SimInstant,
    base_instant: N0Instant,
    seq: u64,
    rng: StdRng,
    topic: TopicId,
    nodes: BTreeMap<PeerId, CosimNode>,
    ordered_ids: Vec<PeerId>,
    queue: BinaryHeap<Reverse<TimedCosimItem>>,
}

impl GossipCosimCluster {
    /// Builds an `N`-node cluster where all peers subscribe to a single `iroh-gossip`
    /// topic (default `HyParView` active view = 5) and join sequentially.
    ///
    /// # Errors
    ///
    /// Returns [`membermesh_core::CoreError`] if engine initialization fails.
    pub fn new(count: usize, seed: u64) -> Result<Self, membermesh_core::CoreError> {
        let mut rng = StdRng::seed_from_u64(seed);
        let topic = TopicId::from_bytes([0x77; 32]);
        let base_instant = N0Instant::now();
        let config = ProtocolConfig::lan();
        let gossip_cfg = GossipProtoConfig::default();

        let mut cluster = Self {
            now: SimInstant(0),
            base_instant,
            seq: 0,
            rng: StdRng::from_rng(&mut rng),
            topic,
            nodes: BTreeMap::new(),
            ordered_ids: Vec::new(),
            queue: BinaryHeap::new(),
        };

        for idx in 0..count {
            let label = format!("cosim-{idx}");
            let signer = TestSigner::from_name(&label);
            let peer_id = signer.peer_id();
            let init = LocalNodeInit {
                session_id: SessionId(u64::try_from(idx + 1).unwrap_or(1)),
                joined_at_unix_ms: 1_700_000_000_000 + u64::try_from(idx * 100).unwrap_or(0),
                metadata: MemberMetadata::default(),
                addr_hint: None,
            };

            let bootstrap_id = cluster.ordered_ids.last().copied();
            let bootstrap_snap = bootstrap_id
                .and_then(|id| cluster.nodes.get(&id).map(|n| n.engine.table().snapshot()));

            let (engine, actions) = match bootstrap_snap.as_ref() {
                None => Engine::found(signer, TestVerifier, init, config.clone(), cluster.now)?,
                Some(snap) => Engine::join_after_sync(
                    signer,
                    TestVerifier,
                    init,
                    Some(snap),
                    config.clone(),
                    cluster.now,
                )?,
            };

            let mut gossip = GossipProtoState::new(
                peer_id,
                PeerData::default(),
                gossip_cfg.clone(),
                StdRng::from_rng(&mut rng),
            );
            let join_peers = bootstrap_id.into_iter().collect();
            let mut io = GossipIoCollector::default();
            gossip.handle(
                GossipInEvent::Command(topic, GossipCommand::Join(join_peers)),
                cluster.n0_now(),
                &mut io,
            );

            cluster.ordered_ids.push(peer_id);
            cluster.nodes.insert(
                peer_id,
                CosimNode {
                    engine,
                    gossip,
                    crashed: false,
                },
            );
            cluster.drain_gossip_io(peer_id, io);
            cluster.dispatch_engine_actions(peer_id, actions);
        }

        Ok(cluster)
    }

    /// Returns the deterministic join-ordered list of peer IDs created at startup.
    #[must_use]
    pub fn ordered_ids(&self) -> &[PeerId] {
        &self.ordered_ids
    }

    /// Crashes the peer at `index` in startup join order.
    pub fn crash_index(&mut self, index: usize) {
        if let Some(&peer_id) = self.ordered_ids.get(index)
            && let Some(node) = self.nodes.get_mut(&peer_id)
        {
            node.crashed = true;
        }
    }

    /// Advances the virtual co-simulation clock by `duration_ms` milliseconds.
    pub fn advance_ms(&mut self, duration_ms: u64) {
        let target = self.now.saturating_add_ms(duration_ms);
        while let Some(Reverse(next)) = self.queue.peek() {
            if next.at > target {
                break;
            }
            let Some(Reverse(item)) = self.queue.pop() else {
                break;
            };
            self.now = item.at;
            self.process_event(item.event);
        }
        self.now = target;
    }

    /// Returns the ordered active roster (`PeerId`s) observed by `peer_id`.
    #[must_use]
    pub fn roster_for(&self, peer_id: PeerId) -> Vec<PeerId> {
        self.nodes
            .get(&peer_id)
            .map(|n| {
                n.engine
                    .members_ordered()
                    .into_iter()
                    .map(|m| m.peer_id)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Returns the advisory leader observed by `peer_id`.
    #[must_use]
    pub fn leader_for(&self, peer_id: PeerId) -> Option<PeerId> {
        self.nodes.get(&peer_id)?.engine.leader().map(|m| m.peer_id)
    }

    fn n0_now(&self) -> N0Instant {
        self.base_instant + Duration::from_millis(self.now.0)
    }

    fn process_event(&mut self, event: CosimEvent) {
        match event {
            CosimEvent::EngineWakeup { peer } => {
                let Some(node) = self.nodes.get_mut(&peer) else {
                    return;
                };
                if node.crashed {
                    return;
                }
                let actions = node.engine.handle_input(self.now, EngineInput::Tick);
                self.dispatch_engine_actions(peer, actions);
            }
            CosimEvent::GossipTimer { peer, timer } => {
                let n0_now = self.n0_now();
                let mut io = GossipIoCollector::default();
                let Some(node) = self.nodes.get_mut(&peer) else {
                    return;
                };
                if node.crashed {
                    return;
                }
                node.gossip
                    .handle(GossipInEvent::TimerExpired(timer), n0_now, &mut io);
                self.drain_gossip_io(peer, io);
            }
            CosimEvent::DeliverGossipProto { to, from, msg } => {
                let n0_now = self.n0_now();
                let mut io = GossipIoCollector::default();
                let Some(node) = self.nodes.get_mut(&to) else {
                    return;
                };
                if node.crashed {
                    // Simulate transport disconnect back to sender so HyParView heals!
                    if let Some(sender) = self.nodes.get_mut(&from)
                        && !sender.crashed
                    {
                        let mut disconn_io = GossipIoCollector::default();
                        sender.gossip.handle(
                            GossipInEvent::PeerDisconnected(to),
                            n0_now,
                            &mut disconn_io,
                        );
                        self.drain_gossip_io(from, disconn_io);
                    }
                    return;
                }
                node.gossip
                    .handle(GossipInEvent::RecvMessage(from, msg), n0_now, &mut io);
                self.drain_gossip_io(to, io);
            }
            CosimEvent::DeliverControl { to, from, msg } => {
                let Some(node) = self.nodes.get_mut(&to) else {
                    return;
                };
                if node.crashed {
                    return;
                }
                let actions = node
                    .engine
                    .handle_input(self.now, EngineInput::ControlReceived { from, msg });
                self.dispatch_engine_actions(to, actions);
            }
            CosimEvent::DeliverSync {
                to,
                from,
                snapshot,
                is_reply,
            } => {
                let (actions, reply) = {
                    let Some(node) = self.nodes.get_mut(&to) else {
                        return;
                    };
                    if node.crashed {
                        return;
                    }
                    let actions = node.engine.handle_input(
                        self.now,
                        EngineInput::ControlReceived {
                            from,
                            msg: ControlMessage::PushPullSync(Box::new(snapshot)),
                        },
                    );
                    let reply = (!is_reply).then(|| node.engine.table().snapshot());
                    (actions, reply)
                };
                self.dispatch_engine_actions(to, actions);
                if let Some(reply_snapshot) = reply {
                    let at = self.delivery_time();
                    self.push_event(
                        at,
                        CosimEvent::DeliverSync {
                            to: from,
                            from: to,
                            snapshot: reply_snapshot,
                            is_reply: true,
                        },
                    );
                }
            }
        }
    }

    fn drain_gossip_io(&mut self, peer: PeerId, io: GossipIoCollector) {
        for (to, msg) in io.sends {
            let at = self.delivery_time();
            self.push_event(
                at,
                CosimEvent::DeliverGossipProto {
                    to,
                    from: peer,
                    msg,
                },
            );
        }
        for (delay, timer) in io.timers {
            let delay_ms = u64::try_from(delay.as_millis()).unwrap_or(1).max(1);
            let at = self.now.saturating_add_ms(delay_ms);
            self.push_event(at, CosimEvent::GossipTimer { peer, timer });
        }

        let mut engine_actions = Vec::new();
        for ev in io.emits {
            match ev {
                GossipTopicEvent::NeighborUp(neighbor) => {
                    if let Some(node) = self.nodes.get_mut(&peer)
                        && !node.crashed
                    {
                        engine_actions.extend(
                            node.engine
                                .handle_input(self.now, EngineInput::NeighborHint(neighbor)),
                        );
                    }
                }
                GossipTopicEvent::Received(gossip_ev) => {
                    if let Some(node) = self.nodes.get_mut(&peer)
                        && !node.crashed
                    {
                        engine_actions.extend(node.engine.handle_input(
                            self.now,
                            EngineInput::GossipReceived(gossip_ev.content),
                        ));
                    }
                }
                GossipTopicEvent::NeighborDown(_) | _ => {}
            }
        }
        self.dispatch_engine_actions(peer, engine_actions);
    }

    fn dispatch_engine_actions(&mut self, peer: PeerId, actions: Vec<EngineAction>) {
        let n0_now = self.n0_now();
        for action in actions {
            match action {
                EngineAction::BroadcastGossip(bytes) => {
                    let mut io = GossipIoCollector::default();
                    if let Some(node) = self.nodes.get_mut(&peer)
                        && !node.crashed
                    {
                        node.gossip.handle(
                            GossipInEvent::Command(
                                self.topic,
                                GossipCommand::Broadcast(bytes, iroh_gossip::proto::Scope::Swarm),
                            ),
                            n0_now,
                            &mut io,
                        );
                    }
                    self.drain_gossip_io(peer, io);
                }
                EngineAction::SendControl { to, msg } => {
                    let at = self.delivery_time();
                    self.push_event(
                        at,
                        CosimEvent::DeliverControl {
                            to,
                            from: peer,
                            msg,
                        },
                    );
                }
                EngineAction::InitiateSync { to, local_snapshot } => {
                    let at = self.delivery_time();
                    self.push_event(
                        at,
                        CosimEvent::DeliverSync {
                            to,
                            from: peer,
                            snapshot: local_snapshot,
                            is_reply: false,
                        },
                    );
                }
                EngineAction::ScheduleWakeup(at) => {
                    self.push_event(at, CosimEvent::EngineWakeup { peer });
                }
                EngineAction::EmitEvent(_) | _ => {}
            }
        }
    }

    fn delivery_time(&mut self) -> SimInstant {
        let jitter = self.rng.random_range(5_u64..=25_u64);
        self.now.saturating_add_ms(jitter)
    }

    fn push_event(&mut self, at: SimInstant, event: CosimEvent) {
        self.seq = self.seq.saturating_add(1);
        self.queue.push(Reverse(TimedCosimItem {
            at,
            seq: self.seq,
            event,
        }));
    }
}
