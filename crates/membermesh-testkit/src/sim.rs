use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, BinaryHeap},
};

use bytes::Bytes;
use membermesh_core::{
    ClaimSigner as _, ControlMessage, Engine, EngineAction, EngineInput, LocalNodeInit,
    MemberMetadata, MemberView, MembershipEvent, PeerId, ProtocolConfig, SessionId, SimInstant,
    SyncSnapshot,
};
use rand::{RngExt as _, SeedableRng as _, rngs::StdRng};

use crate::crypto::{TestSigner, TestVerifier};

#[derive(Clone, Debug)]
enum ScheduledEvent {
    EngineWakeup {
        node: String,
        at: SimInstant,
    },
    DeliverGossip {
        to: String,
        from_id: PeerId,
        bytes: Bytes,
    },
    DeliverControl {
        to: String,
        from: String,
        from_id: PeerId,
        msg: ControlMessage,
    },
    DeliverSync {
        to: String,
        from: String,
        from_id: PeerId,
        snapshot: SyncSnapshot,
        is_reply: bool,
    },
}

#[derive(Clone, Debug)]
struct TimedItem {
    at: SimInstant,
    seq: u64,
    event: ScheduledEvent,
}

impl PartialEq for TimedItem {
    fn eq(&self, other: &Self) -> bool {
        (self.at, self.seq) == (other.at, other.seq)
    }
}

impl Eq for TimedItem {}

impl PartialOrd for TimedItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimedItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.at, self.seq).cmp(&(other.at, other.seq))
    }
}

struct SimNode {
    name: String,
    peer_id: PeerId,
    engine: Engine<TestSigner, TestVerifier>,
    crashed: bool,
    stalled_until: Option<SimInstant>,
    drop_incoming_gossip: bool,
    next_wakeup: Option<SimInstant>,
    events: Vec<MembershipEvent>,
}

/// Deterministic virtual-clock multi-node cluster simulator for [`Engine`].
pub struct SimCluster {
    now: SimInstant,
    seq: u64,
    rng: StdRng,
    config: ProtocolConfig,
    nodes: BTreeMap<String, SimNode>,
    by_id: BTreeMap<PeerId, String>,
    blocked_links: BTreeSet<(String, String)>,
    queue: BinaryHeap<Reverse<TimedItem>>,
    link_latency_min_ms: u64,
    link_latency_max_ms: u64,
    session_counter: u64,
}

impl SimCluster {
    /// Creates a cluster with `names` joining sequentially at `t = 0`.
    ///
    /// # Errors
    ///
    /// Returns [`membermesh_core::CoreError`] if engine initialization fails.
    pub fn new(
        names: &[&str],
        config: ProtocolConfig,
        seed: u64,
    ) -> Result<Self, membermesh_core::CoreError> {
        let mut cluster = Self {
            now: SimInstant(0),
            seq: 0,
            rng: StdRng::seed_from_u64(seed),
            config,
            nodes: BTreeMap::new(),
            by_id: BTreeMap::new(),
            blocked_links: BTreeSet::new(),
            queue: BinaryHeap::new(),
            link_latency_min_ms: 5,
            link_latency_max_ms: 20,
            session_counter: 0,
        };
        for &name in names {
            let _ = cluster.join_node(name)?;
        }
        Ok(cluster)
    }

    /// Joins a new (or restarted) peer using the first alive peer's snapshot as bootstrap.
    ///
    /// # Errors
    ///
    /// Returns [`membermesh_core::CoreError`] if engine creation fails.
    pub fn join_node(&mut self, name: &str) -> Result<PeerId, membermesh_core::CoreError> {
        self.session_counter = self.session_counter.saturating_add(1);
        let session_id = SessionId(self.session_counter);
        let signer = TestSigner::from_name(name);
        let peer_id = signer.peer_id();
        let init = LocalNodeInit {
            session_id,
            joined_at_unix_ms: 1_700_000_000_000_u64.saturating_add(self.now.0),
            metadata: MemberMetadata::default(),
            addr_hint: None,
            admission_token: None,
        };

        let seed_snap = self
            .nodes
            .values()
            .rev()
            .find(|n| !n.crashed)
            .map(|n| n.engine.table().snapshot());

        let (engine, actions) = match seed_snap.as_ref() {
            None => Engine::found(signer, TestVerifier, init, self.config.clone(), self.now)?,
            Some(snap) => Engine::join_after_sync(
                signer,
                TestVerifier,
                init,
                Some(snap),
                self.config.clone(),
                self.now,
            )?,
        };

        let node = SimNode {
            name: name.to_owned(),
            peer_id,
            engine,
            crashed: false,
            stalled_until: None,
            drop_incoming_gossip: false,
            next_wakeup: None,
            events: Vec::new(),
        };
        self.by_id.insert(peer_id, name.to_owned());
        self.nodes.insert(name.to_owned(), node);
        self.dispatch_actions(name, actions);

        // Also seed an immediate Push/Pull sync from the new node to existing alive peers
        // so gossip overlays connect smoothly in simulation.
        if let Some(first_alive) = self
            .nodes
            .values()
            .find(|n| !n.crashed && n.name != name)
            .map(|n| n.name.clone())
        {
            let snap = self.nodes.get(name).map(|n| n.engine.table().snapshot());
            if let Some(snapshot) = snap {
                let at = self.delivery_time();
                self.push_event(
                    at,
                    ScheduledEvent::DeliverSync {
                        to: first_alive,
                        from: name.to_owned(),
                        from_id: peer_id,
                        snapshot,
                        is_reply: false,
                    },
                );
            }
        }
        Ok(peer_id)
    }

    /// Returns the current virtual simulation timestamp.
    #[must_use]
    pub const fn now(&self) -> SimInstant {
        self.now
    }

    /// Advances the virtual simulation clock by `duration_ms` milliseconds.
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

    /// Blocks directed communication from `from` to `to`.
    pub fn block_link(&mut self, from: &str, to: &str) {
        self.blocked_links.insert((from.to_owned(), to.to_owned()));
    }

    /// Blocks bidirectional communication between `a` and `b`.
    pub fn sever_link(&mut self, a: &str, b: &str) {
        self.block_link(a, b);
        self.block_link(b, a);
    }

    /// Partitions the cluster so nodes in `left` cannot reach nodes in `right` and vice versa.
    pub fn partition(&mut self, left: &[&str], right: &[&str]) {
        for &l in left {
            for &r in right {
                self.sever_link(l, r);
            }
        }
    }

    /// Removes all link blocks (heals all network partitions).
    pub fn heal_all(&mut self) {
        self.blocked_links.clear();
    }

    /// Crashes node `name` immediately (drops all further inputs/ticks).
    pub fn crash_node(&mut self, name: &str) {
        if let Some(node) = self.nodes.get_mut(name) {
            node.crashed = true;
        }
    }

    /// Pauses node `name` for `stall_ms` (simulates a local host CPU / scheduler stall).
    pub fn stall_node(&mut self, name: &str, stall_ms: u64) {
        let wake_at = self.now.saturating_add_ms(stall_ms);
        if let Some(node) = self.nodes.get_mut(name) {
            node.stalled_until = Some(wake_at);
            node.next_wakeup = Some(wake_at);
            self.push_event(
                wake_at,
                ScheduledEvent::EngineWakeup {
                    node: name.to_owned(),
                    at: wake_at,
                },
            );
        }
    }

    /// Enables or disables dropping incoming gossip frames for `name`.
    pub fn set_drop_gossip(&mut self, name: &str, drop_gossip: bool) {
        if let Some(node) = self.nodes.get_mut(name) {
            node.drop_incoming_gossip = drop_gossip;
        }
    }

    /// Injects [`EngineInput::GossipLagged`] into node `name`.
    pub fn trigger_gossip_lagged(&mut self, name: &str) {
        let actions = match self.nodes.get_mut(name) {
            Some(node) if !node.crashed => node
                .engine
                .handle_input(self.now, EngineInput::GossipLagged),
            _ => return,
        };
        self.dispatch_actions(name, actions);
    }

    /// Initiates a graceful leave on `name`.
    pub fn leave_node(&mut self, name: &str) {
        let actions = match self.nodes.get_mut(name) {
            Some(node) if !node.crashed => {
                node.engine.handle_input(self.now, EngineInput::LocalLeave)
            }
            _ => return,
        };
        self.dispatch_actions(name, actions);
    }

    /// Returns the ordered active roster (`Alive | Suspect`) names as observed by `name`.
    #[must_use]
    pub fn roster_names(&self, name: &str) -> Vec<String> {
        let Some(node) = self.nodes.get(name) else {
            return Vec::new();
        };
        node.engine
            .members_ordered()
            .into_iter()
            .filter_map(|m| self.by_id.get(&m.peer_id).cloned())
            .collect()
    }

    /// Returns all active views observed by `name`.
    #[must_use]
    pub fn members(&self, name: &str) -> Vec<MemberView> {
        self.nodes
            .get(name)
            .map(|n| n.engine.members_ordered())
            .unwrap_or_default()
    }

    /// Returns all known non-reaped member views (including tombstones) observed by `name`.
    #[must_use]
    pub fn all_members(&self, name: &str) -> Vec<MemberView> {
        self.nodes
            .get(name)
            .map(|n| n.engine.table().all_members_ordered())
            .unwrap_or_default()
    }

    /// Returns the advisory leader name observed by `name`.
    #[must_use]
    pub fn leader_name(&self, name: &str) -> Option<String> {
        let leader = self.nodes.get(name)?.engine.leader()?;
        self.by_id.get(&leader.peer_id).cloned()
    }

    /// Returns the quorum-guarded leader name observed by `name`.
    #[must_use]
    pub fn leader_with_quorum_name(&self, name: &str, min_alive: usize) -> Option<String> {
        let leader = self.nodes.get(name)?.engine.leader_with_quorum(min_alive)?;
        self.by_id.get(&leader.peer_id).cloned()
    }

    /// Returns the [`PeerId`] assigned to `name`.
    #[must_use]
    pub fn peer_id(&self, name: &str) -> Option<PeerId> {
        self.nodes.get(name).map(|n| n.peer_id)
    }

    fn process_event(&mut self, event: ScheduledEvent) {
        match event {
            ScheduledEvent::EngineWakeup { node, at } => {
                let actions = {
                    let Some(n) = self.nodes.get_mut(&node) else {
                        return;
                    };
                    if n.crashed || n.next_wakeup != Some(at) {
                        return;
                    }
                    if let Some(until) = n.stalled_until {
                        if self.now < until {
                            return;
                        }
                        n.stalled_until = None;
                    }
                    n.next_wakeup = None;
                    n.engine.handle_input(self.now, EngineInput::Tick)
                };
                self.dispatch_actions(&node, actions);
            }
            ScheduledEvent::DeliverGossip { to, from_id, bytes } => {
                let _ = from_id;
                let actions = {
                    let Some(n) = self.nodes.get_mut(&to) else {
                        return;
                    };
                    if n.crashed || n.drop_incoming_gossip {
                        return;
                    }
                    if let Some(until) = n.stalled_until
                        && self.now < until
                    {
                        return;
                    }
                    n.engine
                        .handle_input(self.now, EngineInput::GossipReceived(bytes))
                };
                self.dispatch_actions(&to, actions);
            }
            ScheduledEvent::DeliverControl {
                to,
                from,
                from_id,
                msg,
            } => {
                let _ = from;
                let actions = {
                    let Some(n) = self.nodes.get_mut(&to) else {
                        return;
                    };
                    if n.crashed {
                        return;
                    }
                    if let Some(until) = n.stalled_until
                        && self.now < until
                    {
                        return;
                    }
                    n.engine.handle_input(
                        self.now,
                        EngineInput::ControlReceived { from: from_id, msg },
                    )
                };
                self.dispatch_actions(&to, actions);
            }
            ScheduledEvent::DeliverSync {
                to,
                from,
                from_id,
                snapshot,
                is_reply,
            } => {
                let (actions, reply_snap) = {
                    let Some(n) = self.nodes.get_mut(&to) else {
                        return;
                    };
                    if n.crashed {
                        return;
                    }
                    if let Some(until) = n.stalled_until
                        && self.now < until
                    {
                        return;
                    }
                    let actions = n.engine.handle_input(
                        self.now,
                        EngineInput::ControlReceived {
                            from: from_id,
                            msg: ControlMessage::PushPullSync(Box::new(snapshot)),
                        },
                    );
                    let reply = (!is_reply).then(|| n.engine.table().snapshot());
                    (actions, reply)
                };
                self.dispatch_actions(&to, actions);
                if let Some(reply_snapshot) = reply_snap
                    && !self.is_blocked(&to, &from)
                    && let Some(to_id) = self.peer_id(&to)
                {
                    let at = self.delivery_time();
                    self.push_event(
                        at,
                        ScheduledEvent::DeliverSync {
                            to: from,
                            from: to,
                            from_id: to_id,
                            snapshot: reply_snapshot,
                            is_reply: true,
                        },
                    );
                }
            }
        }
    }

    fn dispatch_actions(&mut self, from_name: &str, actions: Vec<EngineAction>) {
        let Some(from_id) = self.peer_id(from_name) else {
            return;
        };
        let all_names: Vec<String> = self.nodes.keys().cloned().collect();

        for action in actions {
            match action {
                EngineAction::BroadcastGossip(bytes) => {
                    for target in &all_names {
                        if target == from_name || self.is_blocked(from_name, target) {
                            continue;
                        }
                        let at = self.delivery_time();
                        self.push_event(
                            at,
                            ScheduledEvent::DeliverGossip {
                                to: target.clone(),
                                from_id,
                                bytes: bytes.clone(),
                            },
                        );
                    }
                }
                EngineAction::SendControl { to, msg } => {
                    if let Some(to_name) = self.by_id.get(&to).cloned()
                        && !self.is_blocked(from_name, &to_name)
                    {
                        let at = self.delivery_time();
                        self.push_event(
                            at,
                            ScheduledEvent::DeliverControl {
                                to: to_name,
                                from: from_name.to_owned(),
                                from_id,
                                msg,
                            },
                        );
                    }
                }
                EngineAction::InitiateSync { to, local_snapshot } => {
                    if let Some(to_name) = self.by_id.get(&to).cloned()
                        && !self.is_blocked(from_name, &to_name)
                    {
                        let at = self.delivery_time();
                        self.push_event(
                            at,
                            ScheduledEvent::DeliverSync {
                                to: to_name,
                                from: from_name.to_owned(),
                                from_id,
                                snapshot: local_snapshot,
                                is_reply: false,
                            },
                        );
                    }
                }
                EngineAction::EmitEvent(ev) => {
                    if let Some(n) = self.nodes.get_mut(from_name) {
                        n.events.push(ev);
                    }
                }
                EngineAction::ScheduleWakeup(at) => {
                    let should_schedule = self.nodes.get_mut(from_name).is_some_and(|n| {
                        let stalled = n.stalled_until.is_some_and(|until| at < until);
                        if stalled || n.next_wakeup == Some(at) {
                            false
                        } else {
                            n.next_wakeup = Some(at);
                            true
                        }
                    });
                    if should_schedule {
                        self.push_event(
                            at,
                            ScheduledEvent::EngineWakeup {
                                node: from_name.to_owned(),
                                at,
                            },
                        );
                    }
                }
                _ => {}
            }
        }
    }

    fn is_blocked(&self, from: &str, to: &str) -> bool {
        self.blocked_links
            .contains(&(from.to_owned(), to.to_owned()))
    }

    fn delivery_time(&mut self) -> SimInstant {
        let jitter = self
            .rng
            .random_range(self.link_latency_min_ms..=self.link_latency_max_ms);
        self.now.saturating_add_ms(jitter)
    }

    fn push_event(&mut self, at: SimInstant, event: ScheduledEvent) {
        self.seq = self.seq.saturating_add(1);
        self.queue.push(Reverse(TimedItem {
            at,
            seq: self.seq,
            event,
        }));
    }
}
