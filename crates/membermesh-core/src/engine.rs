use std::collections::BTreeMap;

use bytes::Bytes;
use tracing::debug;

use crate::{
    AcceptAllPolicy, ClaimPolicy, ClaimSigner, ClaimVerifier, ControlMessage, CoreError,
    GossipEnvelope, GossipItem, GossipMessage, IndirectAlertPayload, IndirectKind, LocalNodeInit,
    MemberMetadata, MemberStatus, MemberView, MembershipEvent, MembershipTable, PeerAge, PeerId,
    ProtocolConfig, SessionId, SignedIndirectAlert, SimInstant, SyncSnapshot,
    swim::{ActiveSuspicion, LhaState, TransmitLimitedQueue},
};

/// Input event fed into the pure Sans-I/O [`Engine`].
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum EngineInput {
    /// Periodic or scheduled timer wakeup fired at `now`.
    Tick,
    /// Raw bytes received from `iroh-gossip` for this cluster topic.
    GossipReceived(Bytes),
    /// Decoded control or Push/Pull sync RPC received from `from` over `/membermesh/1`.
    ControlReceived {
        /// Sending peer identity.
        from: PeerId,
        /// Control message payload.
        msg: ControlMessage,
    },
    /// Opportunistic overlay neighbor hint from `iroh-gossip` (`Event::NeighborUp`).
    NeighborHint(PeerId),
    /// Notification that `iroh-gossip` dropped events (`Event::Lagged`).
    GossipLagged,
    /// Local node is initiating a graceful departure from the cluster.
    LocalLeave,
    /// Local node updates its bounded application metadata.
    UpdateMetadata(MemberMetadata),
}

/// Side-effect command emitted by [`Engine::handle_input`] for the runtime adapter to execute.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum EngineAction {
    /// Broadcast encoded [`GossipEnvelope`] bytes onto `iroh-gossip`.
    BroadcastGossip(Bytes),
    /// Send a direct SWIM control message to `to` over `/membermesh/1`.
    SendControl {
        /// Destination peer.
        to: PeerId,
        /// Control message to deliver.
        msg: ControlMessage,
    },
    /// Initiate a bidirectional Push/Pull anti-entropy sync with `to`.
    InitiateSync {
        /// Target peer to sync with.
        to: PeerId,
        /// Local snapshot to push in the exchange.
        local_snapshot: SyncSnapshot,
    },
    /// Observable cluster membership or leadership transition event.
    EmitEvent(MembershipEvent),
    /// Arm or re-arm the runtime timer for the next scheduled wakeup at `SimInstant`.
    ScheduleWakeup(SimInstant),
}

#[derive(Clone, Debug)]
struct PendingProbe {
    target: PeerId,
    seq: u64,
    direct_deadline: SimInstant,
    round_deadline: SimInstant,
    indirect_sent: bool,
    received_nack_or_ack: bool,
}

#[derive(Clone, Debug)]
struct PendingIndirectRelay {
    requester: PeerId,
    requester_seq: u64,
    deadline: SimInstant,
}

/// Pure Sans-I/O SWIM + Lifeguard cluster membership engine.
#[derive(Debug)]
pub struct Engine<S, V> {
    signer: S,
    verifier: V,
    policy: Box<dyn ClaimPolicy>,
    config: ProtocolConfig,
    table: MembershipTable,
    lha: LhaState,
    queue: TransmitLimitedQueue,
    gossip_nonce: u64,
    probe_seq: u64,
    probe_order: Vec<PeerId>,
    probe_cursor: usize,
    pending_probe: Option<PendingProbe>,
    pending_indirect: BTreeMap<u64, PendingIndirectRelay>,
    suspicions: BTreeMap<PeerId, ActiveSuspicion>,
    next_probe_at: SimInstant,
    next_sync_at: SimInstant,
    next_reap_at: SimInstant,
    rng_state: u64,
    left: bool,
}

impl<S: ClaimSigner, V: ClaimVerifier> Engine<S, V> {
    /// Creates a new cluster engine founded by the local node at `join_seq = 1`.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if `config` is invalid or signing the initial claim fails.
    pub fn found(
        signer: S,
        verifier: V,
        init: LocalNodeInit,
        config: ProtocolConfig,
        now: SimInstant,
    ) -> Result<(Self, Vec<EngineAction>), CoreError> {
        Self::found_with_policy(
            signer,
            verifier,
            Box::new(AcceptAllPolicy),
            init,
            config,
            now,
        )
    }

    /// Creates a new cluster engine founded by the local node with a custom [`ClaimPolicy`].
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if `config` is invalid or signing the initial claim fails.
    pub fn found_with_policy(
        signer: S,
        verifier: V,
        policy: Box<dyn ClaimPolicy>,
        init: LocalNodeInit,
        config: ProtocolConfig,
        now: SimInstant,
    ) -> Result<(Self, Vec<EngineAction>), CoreError> {
        config.validate()?;
        let rng_seed = seed_from(signer.peer_id(), init.session_id);
        let table = MembershipTable::found(&signer, init, now)?;
        Ok(Self::finish_init(
            signer, verifier, policy, table, config, rng_seed, now,
        ))
    }

    /// Creates a cluster engine initialized from an optional bootstrap [`SyncSnapshot`].
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if `config` is invalid, snapshot verification fails,
    /// or signing the local claim fails.
    pub fn join_after_sync(
        signer: S,
        verifier: V,
        init: LocalNodeInit,
        initial_snapshot: Option<&SyncSnapshot>,
        config: ProtocolConfig,
        now: SimInstant,
    ) -> Result<(Self, Vec<EngineAction>), CoreError> {
        Self::join_after_sync_with_policy(
            signer,
            verifier,
            Box::new(AcceptAllPolicy),
            init,
            initial_snapshot,
            config,
            now,
        )
    }

    /// Creates a cluster engine initialized from an optional snapshot using a custom [`ClaimPolicy`].
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if `config` is invalid, snapshot verification fails,
    /// or signing the local claim fails.
    pub fn join_after_sync_with_policy(
        signer: S,
        verifier: V,
        policy: Box<dyn ClaimPolicy>,
        init: LocalNodeInit,
        initial_snapshot: Option<&SyncSnapshot>,
        config: ProtocolConfig,
        now: SimInstant,
    ) -> Result<(Self, Vec<EngineAction>), CoreError> {
        config.validate()?;
        let rng_seed = seed_from(signer.peer_id(), init.session_id);
        let table = MembershipTable::join_after_sync_with_policy(
            &signer,
            init,
            initial_snapshot,
            &verifier,
            policy.as_ref(),
            now,
        )?;
        Ok(Self::finish_init(
            signer, verifier, policy, table, config, rng_seed, now,
        ))
    }

    fn finish_init(
        signer: S,
        verifier: V,
        policy: Box<dyn ClaimPolicy>,
        table: MembershipTable,
        config: ProtocolConfig,
        rng_state: u64,
        now: SimInstant,
    ) -> (Self, Vec<EngineAction>) {
        let mut queue = TransmitLimitedQueue::default();
        if let Some(claim) = table.local_claim() {
            queue.push(GossipItem::Claim(claim.clone()));
        }
        let next_probe_at = now.saturating_add_ms(config.probe_interval_ms);
        let next_sync_at = now.saturating_add_ms(config.anti_entropy_interval_ms);
        let next_reap_at = now.saturating_add_ms(config.reap_interval_ms);

        let mut engine = Self {
            signer,
            verifier,
            policy,
            config,
            table,
            lha: LhaState::default(),
            queue,
            gossip_nonce: 0,
            probe_seq: 0,
            probe_order: Vec::new(),
            probe_cursor: 0,
            pending_probe: None,
            pending_indirect: BTreeMap::new(),
            suspicions: BTreeMap::new(),
            next_probe_at,
            next_sync_at,
            next_reap_at,
            rng_state,
            left: false,
        };
        let mut actions = Vec::new();
        for view in engine.table.members_ordered() {
            actions.push(EngineAction::EmitEvent(MembershipEvent::Joined(view)));
        }
        engine.flush_gossip(&mut actions, 1);
        if let Some(wakeup) = engine.next_wakeup() {
            actions.push(EngineAction::ScheduleWakeup(wakeup));
        }
        (engine, actions)
    }

    /// Returns a reference to the underlying [`MembershipTable`].
    #[must_use]
    pub const fn table(&self) -> &MembershipTable {
        &self.table
    }

    /// Returns the active [`ProtocolConfig`].
    #[must_use]
    pub const fn config(&self) -> &ProtocolConfig {
        &self.config
    }

    /// Returns the local peer's Lifeguard LHA score (`0..=8`).
    #[must_use]
    pub const fn lha_score(&self) -> u8 {
        self.lha.score()
    }

    /// Returns all active (`Alive` or `Suspect`) members sorted by `(join_seq, peer_id)`.
    #[must_use]
    pub fn members_ordered(&self) -> Vec<MemberView> {
        self.table.members_ordered()
    }

    /// Returns the deterministic advisory leader (oldest active peer).
    #[must_use]
    pub fn leader(&self) -> Option<MemberView> {
        self.table.leader()
    }

    /// Returns the advisory leader only if at least `min_alive` members are `Alive`.
    #[must_use]
    pub fn leader_with_quorum(&self, min_alive: usize) -> Option<MemberView> {
        self.table.leader_with_quorum(min_alive)
    }

    /// Returns logical join sequence and observed uptime for `peer_id`.
    #[must_use]
    pub fn peer_age(&self, peer_id: PeerId, now: SimInstant) -> Option<PeerAge> {
        self.table.peer_age(peer_id, now)
    }

    /// Returns the earliest timestamp at which the engine next needs a [`EngineInput::Tick`].
    #[must_use]
    pub fn next_wakeup(&self) -> Option<SimInstant> {
        if self.left {
            return None;
        }
        let mut earliest = self
            .next_probe_at
            .min(self.next_sync_at)
            .min(self.next_reap_at);
        if let Some(probe) = &self.pending_probe {
            let probe_deadline = if probe.indirect_sent {
                probe.round_deadline
            } else {
                probe.direct_deadline
            };
            earliest = earliest.min(probe_deadline);
        }
        for relay in self.pending_indirect.values() {
            earliest = earliest.min(relay.deadline);
        }
        for susp in self.suspicions.values() {
            earliest = earliest.min(susp.deadline);
        }
        Some(earliest)
    }

    /// Processes a single [`EngineInput`] at monotonic time `now` and returns [`EngineAction`]s.
    pub fn handle_input(&mut self, now: SimInstant, input: EngineInput) -> Vec<EngineAction> {
        let mut actions = Vec::new();
        if self.left {
            return actions;
        }

        match input {
            EngineInput::Tick => self.handle_tick(now, &mut actions),
            EngineInput::GossipReceived(bytes) => {
                self.handle_gossip_bytes(now, &bytes, &mut actions);
            }
            EngineInput::ControlReceived { from, msg } => {
                self.handle_control(now, from, msg, &mut actions);
            }
            EngineInput::NeighborHint(peer_id) => {
                if peer_id != self.table.local_id() && self.table.member(peer_id).is_none() {
                    actions.push(EngineAction::InitiateSync {
                        to: peer_id,
                        local_snapshot: self.table.snapshot(),
                    });
                }
            }
            EngineInput::GossipLagged => {
                if let Some(target) = self.pick_random_alive_peer(None) {
                    actions.push(EngineAction::InitiateSync {
                        to: target,
                        local_snapshot: self.table.snapshot(),
                    });
                }
            }
            EngineInput::LocalLeave => {
                self.left = true;
                match self.table.leave(&self.signer, now) {
                    Ok((leave_claim, outcome)) => {
                        for ev in outcome.events {
                            actions.push(EngineAction::EmitEvent(ev));
                        }
                        self.queue.push(GossipItem::Claim(leave_claim));
                        self.flush_gossip(&mut actions, 2);
                    }
                    Err(err) => {
                        debug!("failed to sign local leave claim: {err}");
                    }
                }
            }
            EngineInput::UpdateMetadata(metadata) => {
                match self
                    .table
                    .update_local_metadata(metadata, &self.signer, now)
                {
                    Ok((updated_claim, outcome)) => {
                        for ev in outcome.events {
                            actions.push(EngineAction::EmitEvent(ev));
                        }
                        self.queue.push(GossipItem::Claim(updated_claim));
                        self.flush_gossip(&mut actions, 1);
                    }
                    Err(err) => {
                        debug!("failed to sign metadata update claim: {err}");
                    }
                }
            }
        }

        if let Some(wakeup) = self.next_wakeup() {
            actions.push(EngineAction::ScheduleWakeup(wakeup));
        }
        actions
    }

    fn handle_tick(&mut self, now: SimInstant, actions: &mut Vec<EngineAction>) {
        // Detect local CPU/event-loop starvation when a tick arrives significantly late.
        let stall_threshold = self
            .next_probe_at
            .saturating_add_ms(self.lha.scale_ms(self.config.probe_timeout_ms));
        if now > stall_threshold {
            self.lha.increment();
            self.next_probe_at =
                now.saturating_add_ms(self.lha.scale_ms(self.config.probe_interval_ms));
            // Extend active suspicions proportionally to avoid false evictions right after stall.
            for susp in self.suspicions.values_mut() {
                let extra_ms = self.config.probe_interval_ms.saturating_mul(2);
                susp.deadline = susp.deadline.max(now.saturating_add_ms(extra_ms));
            }
        }

        // 1. Expire indirect relay probes and reply Nack to requesters.
        let expired_relays: Vec<u64> = self
            .pending_indirect
            .iter()
            .filter_map(|(&seq, r)| (now >= r.deadline).then_some(seq))
            .collect();
        for seq in expired_relays {
            if let Some(relay) = self.pending_indirect.remove(&seq) {
                actions.push(EngineAction::SendControl {
                    to: relay.requester,
                    msg: ControlMessage::Nack {
                        seq: relay.requester_seq,
                        from: self.table.local_id(),
                    },
                });
            }
        }

        // 2. Progress pending outbound probe.
        if let Some(mut probe) = self.pending_probe.take() {
            if !probe.indirect_sent && now >= probe.direct_deadline {
                let intermediaries = self.pick_intermediaries(probe.target);
                for peer in intermediaries {
                    actions.push(EngineAction::SendControl {
                        to: peer,
                        msg: ControlMessage::PingReq {
                            seq: probe.seq,
                            from: self.table.local_id(),
                            target: probe.target,
                        },
                    });
                }
                probe.indirect_sent = true;
                self.pending_probe = Some(probe);
            } else if now >= probe.round_deadline {
                if !probe.received_nack_or_ack {
                    self.lha.increment();
                }
                self.suspect_peer(probe.target, now, actions);
            } else {
                self.pending_probe = Some(probe);
            }
        }

        // 3. Expire suspicions whose Lifeguard deadline elapsed -> mark Dead.
        let expired_suspects: Vec<PeerId> = self
            .suspicions
            .iter()
            .filter_map(|(&peer, susp)| (now >= susp.deadline).then_some(peer))
            .collect();
        for peer_id in expired_suspects {
            self.suspicions.remove(&peer_id);
            self.mark_peer_dead(peer_id, now, actions);
        }

        // 4. Start next SWIM probe round if due.
        if self.pending_probe.is_none()
            && now >= self.next_probe_at
            && let Some(target) = self.next_probe_target()
        {
            self.probe_seq = self.probe_seq.saturating_add(1);
            let seq = self.probe_seq;
            let timeout_ms = self.lha.scale_ms(self.config.probe_timeout_ms);
            let direct_deadline = now.saturating_add_ms(timeout_ms);
            let round_deadline = now.saturating_add_ms(timeout_ms.saturating_mul(2));
            self.pending_probe = Some(PendingProbe {
                target,
                seq,
                direct_deadline,
                round_deadline,
                indirect_sent: false,
                received_nack_or_ack: false,
            });
            actions.push(EngineAction::SendControl {
                to: target,
                msg: ControlMessage::Ping {
                    seq,
                    from: self.table.local_id(),
                },
            });
            self.next_probe_at =
                now.saturating_add_ms(self.lha.scale_ms(self.config.probe_interval_ms));
        } else if now >= self.next_probe_at {
            self.next_probe_at =
                now.saturating_add_ms(self.lha.scale_ms(self.config.probe_interval_ms));
        }

        // 5. Periodic Push/Pull anti-entropy sync (alive peer + one dead peer for partition healing).
        if now >= self.next_sync_at {
            let local_snapshot = self.table.snapshot();
            if let Some(target) = self.pick_random_alive_peer(None) {
                actions.push(EngineAction::InitiateSync {
                    to: target,
                    local_snapshot: local_snapshot.clone(),
                });
            }
            if let Some(dead_target) = self.pick_random_dead_peer() {
                actions.push(EngineAction::InitiateSync {
                    to: dead_target,
                    local_snapshot,
                });
            }
            self.next_sync_at = now.saturating_add_ms(self.config.anti_entropy_interval_ms);
        }

        // 6. Periodic tombstone reaping.
        if now >= self.next_reap_at {
            let _ = self.table.reap_expired(
                now,
                self.config.dead_reap_ttl_ms,
                self.config.left_reap_ttl_ms,
            );
            self.next_reap_at = now.saturating_add_ms(self.config.reap_interval_ms);
        }

        // 7. Re-disseminate bounded gossip updates if any remain queued.
        self.flush_gossip(actions, 1);
    }

    fn handle_gossip_bytes(
        &mut self,
        now: SimInstant,
        bytes: &[u8],
        actions: &mut Vec<EngineAction>,
    ) {
        let Ok(env) = GossipEnvelope::decode(bytes, self.config.max_gossip_bytes) else {
            debug!("dropping invalid gossip envelope");
            return;
        };
        let items = match env.message {
            GossipMessage::Single(item) => vec![item],
            GossipMessage::Compound(items) => items,
        };
        let mut any_new = false;
        for item in items {
            any_new |= self.apply_gossip_item(now, item, actions);
        }
        if any_new {
            self.flush_gossip(actions, 1);
        }
    }

    fn apply_gossip_item(
        &mut self,
        now: SimInstant,
        item: GossipItem,
        actions: &mut Vec<EngineAction>,
    ) -> bool {
        match item {
            GossipItem::Claim(claim) => {
                let subject = claim.claim.subject;
                let Ok(outcome) = self.table.apply_claim_with_policy(
                    claim.clone(),
                    &self.verifier,
                    self.policy.as_ref(),
                    now,
                ) else {
                    return false;
                };
                for ev in outcome.events {
                    actions.push(EngineAction::EmitEvent(ev));
                }
                if let Some(view) = self.table.member(subject)
                    && view.status == MemberStatus::Alive
                {
                    self.suspicions.remove(&subject);
                }
                if outcome.changed {
                    self.queue.push(GossipItem::Claim(claim));
                }
                outcome.changed
            }
            GossipItem::IndirectAlert(alert) => {
                let subject = alert.payload.subject;
                let reporter = alert.payload.reporter;
                let kind = alert.payload.kind;
                let Ok(outcome) =
                    self.table
                        .apply_indirect_alert(&alert, &self.verifier, &self.signer, now)
                else {
                    return false;
                };
                for ev in outcome.events {
                    actions.push(EngineAction::EmitEvent(ev));
                }
                if let Some(refute) = outcome.refutation {
                    self.lha.increment();
                    self.queue.push(GossipItem::Claim(refute));
                }
                self.reconcile_suspicion_after_alert(now, subject, reporter, kind);
                if outcome.changed {
                    self.queue.push(GossipItem::IndirectAlert(alert));
                }
                outcome.changed
            }
        }
    }

    fn handle_control(
        &mut self,
        now: SimInstant,
        from: PeerId,
        msg: ControlMessage,
        actions: &mut Vec<EngineAction>,
    ) {
        match msg {
            ControlMessage::Ping {
                seq,
                from: requester,
            } => {
                let _ = from;
                actions.push(EngineAction::SendControl {
                    to: requester,
                    msg: ControlMessage::Ack {
                        seq,
                        from: self.table.local_id(),
                    },
                });
            }
            ControlMessage::PingReq {
                seq,
                from: requester,
                target,
            } => {
                let _ = from;
                self.probe_seq = self.probe_seq.saturating_add(1);
                let relay_seq = self.probe_seq;
                self.pending_indirect.insert(
                    relay_seq,
                    PendingIndirectRelay {
                        requester,
                        requester_seq: seq,
                        deadline: now.saturating_add_ms(self.config.probe_timeout_ms),
                    },
                );
                actions.push(EngineAction::SendControl {
                    to: target,
                    msg: ControlMessage::Ping {
                        seq: relay_seq,
                        from: self.table.local_id(),
                    },
                });
            }
            ControlMessage::Ack {
                seq,
                from: responder,
            } => {
                let _ = from;
                if let Some(relay) = self.pending_indirect.remove(&seq) {
                    actions.push(EngineAction::SendControl {
                        to: relay.requester,
                        msg: ControlMessage::Ack {
                            seq: relay.requester_seq,
                            from: self.table.local_id(),
                        },
                    });
                    return;
                }
                if let Some(probe) = &self.pending_probe
                    && probe.seq == seq
                    && (probe.target == responder || probe.indirect_sent)
                {
                    self.pending_probe = None;
                    self.lha.decrement();
                }
            }
            ControlMessage::Nack { seq, .. } => {
                if let Some(probe) = &mut self.pending_probe
                    && probe.seq == seq
                {
                    probe.received_nack_or_ack = true;
                }
            }
            ControlMessage::PushPullSync(snapshot) => {
                let Ok(outcome) = self.table.merge_snapshot_with_policy(
                    &snapshot,
                    &self.verifier,
                    &self.signer,
                    self.policy.as_ref(),
                    now,
                ) else {
                    debug!("dropping invalid PushPullSync snapshot from {from}");
                    return;
                };
                for ev in outcome.events {
                    actions.push(EngineAction::EmitEvent(ev));
                }
                if let Some(refute) = outcome.refutation {
                    self.lha.increment();
                    self.queue.push(GossipItem::Claim(refute));
                    self.flush_gossip(actions, 1);
                }
                for view in self.table.all_members_ordered() {
                    if view.status == MemberStatus::Alive {
                        self.suspicions.remove(&view.peer_id);
                    }
                }
            }
        }
    }

    fn suspect_peer(&mut self, target: PeerId, now: SimInstant, actions: &mut Vec<EngineAction>) {
        let Some(view) = self.table.member(target) else {
            return;
        };
        if !matches!(view.status, MemberStatus::Alive | MemberStatus::Suspect) {
            return;
        }
        let Ok(alert) = SignedIndirectAlert::sign(
            IndirectAlertPayload {
                subject: target,
                session_id: view.session_id,
                incarnation: view.incarnation,
                join_seq: view.join_seq,
                kind: IndirectKind::Suspect,
                reporter: self.table.local_id(),
            },
            &self.signer,
        ) else {
            return;
        };
        if let Ok(outcome) =
            self.table
                .apply_indirect_alert(&alert, &self.verifier, &self.signer, now)
        {
            for ev in outcome.events {
                actions.push(EngineAction::EmitEvent(ev));
            }
            if outcome.changed {
                self.queue.push(GossipItem::IndirectAlert(alert));
                self.flush_gossip(actions, 1);
            }
        }
        self.suspicions
            .entry(target)
            .or_insert_with(|| ActiveSuspicion::new(target, now, &self.config, self.lha));
    }

    fn mark_peer_dead(&mut self, target: PeerId, now: SimInstant, actions: &mut Vec<EngineAction>) {
        let Some(view) = self.table.member(target) else {
            return;
        };
        if view.status != MemberStatus::Suspect {
            return;
        }
        let Ok(alert) = SignedIndirectAlert::sign(
            IndirectAlertPayload {
                subject: target,
                session_id: view.session_id,
                incarnation: view.incarnation,
                join_seq: view.join_seq,
                kind: IndirectKind::Dead,
                reporter: self.table.local_id(),
            },
            &self.signer,
        ) else {
            return;
        };
        if let Ok(outcome) =
            self.table
                .apply_indirect_alert(&alert, &self.verifier, &self.signer, now)
        {
            for ev in outcome.events {
                actions.push(EngineAction::EmitEvent(ev));
            }
            if outcome.changed {
                self.queue.push(GossipItem::IndirectAlert(alert));
                self.flush_gossip(actions, 1);
            }
        }
    }

    fn reconcile_suspicion_after_alert(
        &mut self,
        now: SimInstant,
        subject: PeerId,
        reporter: PeerId,
        kind: IndirectKind,
    ) {
        if subject == self.table.local_id() {
            return;
        }
        let Some(view) = self.table.member(subject) else {
            self.suspicions.remove(&subject);
            return;
        };
        match (kind, view.status) {
            (IndirectKind::Suspect, MemberStatus::Suspect) => {
                let susp = self
                    .suspicions
                    .entry(subject)
                    .or_insert_with(|| ActiveSuspicion::new(subject, now, &self.config, self.lha));
                if reporter != self.table.local_id() {
                    let _ = susp.add_confirmation(reporter, &self.config, self.lha);
                }
            }
            (_, MemberStatus::Dead | MemberStatus::Left | MemberStatus::Alive) => {
                self.suspicions.remove(&subject);
            }
            (IndirectKind::Dead, MemberStatus::Suspect) => {}
        }
    }

    fn flush_gossip(&mut self, actions: &mut Vec<EngineAction>, max_batches: usize) {
        let cluster_size = self.table.members_ordered().len().max(1);
        for _ in 0..max_batches {
            if self.queue.is_empty() {
                break;
            }
            self.gossip_nonce = self.gossip_nonce.saturating_add(1);
            if let Some(bytes) = self.queue.pop_encoded_batch(
                self.table.local_id(),
                self.gossip_nonce,
                cluster_size,
                self.config.retransmit_mult,
                self.config.max_gossip_bytes,
            ) {
                actions.push(EngineAction::BroadcastGossip(bytes));
            } else {
                break;
            }
        }
    }

    fn next_probe_target(&mut self) -> Option<PeerId> {
        let local = self.table.local_id();
        let active: Vec<PeerId> = self
            .table
            .members_ordered()
            .into_iter()
            .map(|m| m.peer_id)
            .filter(|&id| id != local)
            .collect();
        if active.is_empty() {
            self.probe_order.clear();
            self.probe_cursor = 0;
            return None;
        }

        // Refresh probe order when exhausted or when members changed significantly.
        self.probe_order.retain(|id| active.contains(id));
        if self.probe_cursor >= self.probe_order.len() || self.probe_order.len() != active.len() {
            let mut order = active;
            self.shuffle_peers(&mut order);
            self.probe_order = order;
            self.probe_cursor = 0;
        }

        let target = self.probe_order.get(self.probe_cursor).copied();
        self.probe_cursor = self.probe_cursor.saturating_add(1);
        target
    }

    fn pick_intermediaries(&mut self, exclude: PeerId) -> Vec<PeerId> {
        let local = self.table.local_id();
        let mut candidates: Vec<PeerId> = self
            .table
            .members_ordered()
            .into_iter()
            .filter(|m| m.status == MemberStatus::Alive)
            .map(|m| m.peer_id)
            .filter(|&id| id != local && id != exclude)
            .collect();
        self.shuffle_peers(&mut candidates);
        candidates.truncate(self.config.indirect_checks);
        candidates
    }

    fn pick_random_alive_peer(&mut self, exclude: Option<PeerId>) -> Option<PeerId> {
        let local = self.table.local_id();
        let candidates: Vec<PeerId> = self
            .table
            .members_ordered()
            .into_iter()
            .filter(|m| m.status == MemberStatus::Alive)
            .map(|m| m.peer_id)
            .filter(|&id| id != local && Some(id) != exclude)
            .collect();
        if candidates.is_empty() {
            return None;
        }
        let idx = self.next_rand_idx(candidates.len());
        candidates.get(idx).copied()
    }

    fn pick_random_dead_peer(&mut self) -> Option<PeerId> {
        let local = self.table.local_id();
        let candidates: Vec<PeerId> = self
            .table
            .all_members_ordered()
            .into_iter()
            .filter(|m| m.status == MemberStatus::Dead && m.peer_id != local)
            .map(|m| m.peer_id)
            .collect();
        if candidates.is_empty() {
            return None;
        }
        let idx = self.next_rand_idx(candidates.len());
        candidates.get(idx).copied()
    }

    fn shuffle_peers(&mut self, peers: &mut [PeerId]) {
        let len = peers.len();
        for i in (1..len).rev() {
            let j = self.next_rand_idx(i + 1);
            peers.swap(i, j);
        }
    }

    fn next_rand_idx(&mut self, upper_exclusive: usize) -> usize {
        if upper_exclusive <= 1 {
            return 0;
        }
        let bound = u64::try_from(upper_exclusive).unwrap_or(u64::MAX);
        usize::try_from(self.next_rand() % bound).unwrap_or(0)
    }

    fn next_rand(&mut self) -> u64 {
        self.rng_state = self.rng_state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.rng_state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}

fn seed_from(peer: PeerId, session: SessionId) -> u64 {
    let b = peer.as_bytes();
    let head = u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]);
    head ^ session.0.rotate_left(17)
}
