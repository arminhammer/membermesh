use std::{collections::BTreeMap, time::Duration};

use crate::{
    AddrHint, ClaimSigner, ClaimVerifier, CoreError, Incarnation, IndirectKind, JoinOrder,
    MemberClaim, MemberMetadata, MemberStatus, PeerId, SessionId, SignedClaim, SignedIndirectAlert,
    SyncSnapshot,
};

/// Monotonic virtual timestamp in milliseconds used inside `membermesh-core`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub struct SimInstant(pub u64);

impl SimInstant {
    /// Returns the elapsed duration since `earlier`, saturating at zero.
    #[must_use]
    pub const fn elapsed_since(self, earlier: Self) -> Duration {
        Duration::from_millis(self.0.saturating_sub(earlier.0))
    }

    /// Advances the timestamp by `delta_ms` milliseconds, saturating at `u64::MAX`.
    #[must_use]
    pub const fn saturating_add_ms(self, delta_ms: u64) -> Self {
        Self(self.0.saturating_add(delta_ms))
    }
}

/// Logical ordering and observed age information for a cluster member.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct PeerAge {
    /// Immutable Lamport join sequence assigned when the peer joined.
    pub join_seq: u64,
    /// Duration this node has locally observed the peer as alive.
    pub observed_alive_for: Duration,
    /// Informational wall-clock timestamp reported by the peer on join.
    pub joined_at_unix_ms: u64,
}

/// Public snapshot of a single member's state in [`MembershipTable`].
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MemberView {
    /// Cryptographic peer identifier.
    pub peer_id: PeerId,
    /// Ephemeral process session identifier.
    pub session_id: SessionId,
    /// Current incarnation counter.
    pub incarnation: Incarnation,
    /// Immutable Lamport join sequence (oldest = smallest).
    pub join_seq: u64,
    /// Informational wall-clock join timestamp in Unix milliseconds.
    pub joined_at_unix_ms: u64,
    /// Current lifecycle status.
    pub status: MemberStatus,
    /// Application-defined metadata.
    pub metadata: MemberMetadata,
    /// Optional bounded endpoint address hint.
    pub addr_hint: Option<AddrHint>,
    /// Local monotonic timestamp when this peer was first observed alive in this session.
    pub observed_alive_since: SimInstant,
}

impl MemberView {
    /// Returns the deterministic join-order sort key `(join_seq, peer_id)`.
    #[must_use]
    pub const fn join_order(&self) -> JoinOrder {
        JoinOrder {
            join_seq: self.join_seq,
            peer_id: self.peer_id,
        }
    }
}

/// Observable membership and leadership transitions emitted by [`MembershipTable`].
#[derive(Clone, PartialEq, Eq, Debug)]
#[non_exhaustive]
pub enum MembershipEvent {
    /// A peer became `Alive` in the cluster.
    Joined(MemberView),
    /// An `Alive` peer updated its incarnation, metadata, or address hint.
    Updated(MemberView),
    /// A peer transitioned into `Suspect` status.
    Suspected {
        /// Suspected peer identity.
        peer_id: PeerId,
        /// Incarnation under suspicion.
        incarnation: Incarnation,
    },
    /// A peer transitioned into `Dead` status.
    Failed(MemberView),
    /// A peer gracefully left the cluster.
    Left(MemberView),
    /// The deterministic advisory leader changed.
    LeaderChanged {
        /// Previous advisory leader, if any.
        previous: Option<PeerId>,
        /// New advisory leader, if any.
        current: Option<PeerId>,
        /// Monotonically increasing local leader-change epoch.
        epoch: u64,
    },
}

/// Result returned after applying a claim, indirect alert, or snapshot to [`MembershipTable`].
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct ApplyOutcome {
    /// Whether the membership table changed.
    pub changed: bool,
    /// Ordered events emitted by this state transition.
    pub events: Vec<MembershipEvent>,
    /// Self-signed `Alive` refutation claim emitted if the local peer was falsely suspected or marked dead.
    pub refutation: Option<SignedClaim>,
}

/// Canonical join-semilattice entry for a single peer's protocol state.
///
/// Equality and merge order depend strictly on protocol fields so `merge` is
/// commutative, associative, and idempotent regardless of delivery order.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MemberEntryState {
    /// Winning self-signed `Alive` or `Left` claim for this peer.
    pub claim: SignedClaim,
    /// Effective status derived from `claim` and any dominating `alert`.
    pub effective_status: MemberStatus,
    /// Effective incarnation of `effective_status`.
    pub effective_incarnation: Incarnation,
    /// Dominating indirect `Suspect` or `Dead` alert, if any.
    pub alert: Option<SignedIndirectAlert>,
}

impl MemberEntryState {
    /// Constructs an entry directly from a self-signed `Alive` or `Left` claim.
    #[must_use]
    pub const fn from_claim(claim: SignedClaim) -> Self {
        let effective_status = claim.claim.status;
        let effective_incarnation = claim.claim.incarnation;
        Self {
            claim,
            effective_status,
            effective_incarnation,
            alert: None,
        }
    }

    /// Merges two verified entries for the same `PeerId` as a join-semilattice.
    ///
    /// Guarantees commutativity, associativity, and idempotence across all inputs
    /// by taking independent joins on the total orders of `claim` and `alert` and
    /// deriving `(effective_status, effective_incarnation)` deterministically.
    #[must_use]
    pub fn merge(&self, other: &Self) -> Self {
        let winning_claim = if claim_rank(&other.claim) > claim_rank(&self.claim) {
            other.claim.clone()
        } else {
            self.claim.clone()
        };

        let winning_alert = pick_max_alert(self.alert.as_ref(), other.alert.as_ref());

        let (effective_status, effective_incarnation) = match &winning_alert {
            Some(alert) if alert_applies_to_claim(alert, &winning_claim) => {
                let status = match alert.payload.kind {
                    IndirectKind::Suspect => MemberStatus::Suspect,
                    IndirectKind::Dead => MemberStatus::Dead,
                };
                (status, alert.payload.incarnation)
            }
            _ => (winning_claim.claim.status, winning_claim.claim.incarnation),
        };

        Self {
            claim: winning_claim,
            effective_status,
            effective_incarnation,
            alert: winning_alert,
        }
    }
}

#[derive(Clone, Debug)]
struct StoredEntry {
    state: MemberEntryState,
    status_since: SimInstant,
    observed_alive_since: SimInstant,
}

impl StoredEntry {
    fn to_view(&self) -> MemberView {
        let claim = &self.state.claim.claim;
        MemberView {
            peer_id: claim.subject,
            session_id: claim.session_id,
            incarnation: self.state.effective_incarnation,
            join_seq: claim.join_seq,
            joined_at_unix_ms: claim.joined_at_unix_ms,
            status: self.state.effective_status,
            metadata: claim.metadata.clone(),
            addr_hint: claim.addr_hint.clone(),
            observed_alive_since: self.observed_alive_since,
        }
    }
}

const fn self_status_rank(status: MemberStatus) -> u8 {
    match status {
        MemberStatus::Alive => 0,
        MemberStatus::Suspect => 1,
        MemberStatus::Dead => 2,
        MemberStatus::Left => 3,
    }
}

const fn indirect_kind_rank(kind: IndirectKind) -> u8 {
    match kind {
        IndirectKind::Suspect => 1,
        IndirectKind::Dead => 2,
    }
}

fn claim_rank(signed: &SignedClaim) -> (u64, Incarnation, u8, SessionId, u64, [u8; 64]) {
    let c = &signed.claim;
    (
        c.join_seq,
        c.incarnation,
        self_status_rank(c.status),
        c.session_id,
        c.joined_at_unix_ms,
        signed.signature,
    )
}

fn alert_applies_to_claim(alert: &SignedIndirectAlert, claim: &SignedClaim) -> bool {
    let p = &alert.payload;
    let c = &claim.claim;
    c.status != MemberStatus::Left
        && p.subject == c.subject
        && p.session_id == c.session_id
        && p.join_seq == c.join_seq
        && p.incarnation >= c.incarnation
}

fn alert_rank(alert: &SignedIndirectAlert) -> (u64, Incarnation, u8, SessionId, PeerId, [u8; 64]) {
    (
        alert.payload.join_seq,
        alert.payload.incarnation,
        indirect_kind_rank(alert.payload.kind),
        alert.payload.session_id,
        alert.payload.reporter,
        alert.signature,
    )
}

fn pick_max_alert(
    a: Option<&SignedIndirectAlert>,
    b: Option<&SignedIndirectAlert>,
) -> Option<SignedIndirectAlert> {
    match (a, b) {
        (Some(lhs), Some(rhs)) => {
            if alert_rank(rhs) > alert_rank(lhs) {
                Some(rhs.clone())
            } else {
                Some(lhs.clone())
            }
        }
        (Some(lhs), None) => Some(lhs.clone()),
        (None, Some(rhs)) => Some(rhs.clone()),
        (None, None) => None,
    }
}

/// Parameters describing the local node's identity and attributes when joining a cluster.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct LocalNodeInit {
    /// Ephemeral process session identifier.
    pub session_id: SessionId,
    /// Informational wall-clock join timestamp (milliseconds since Unix epoch).
    pub joined_at_unix_ms: u64,
    /// Bounded application metadata.
    pub metadata: MemberMetadata,
    /// Optional bounded endpoint addressing hint.
    pub addr_hint: Option<AddrHint>,
}

/// Deterministic cluster membership table implementing Lamport join ordering,
/// incarnation-based refutation, and advisory leader selection.
#[derive(Clone, Debug)]
pub struct MembershipTable {
    local_id: PeerId,
    local_session_id: SessionId,
    lamport_clock: u64,
    cluster_epoch: u64,
    current_leader: Option<PeerId>,
    entries: BTreeMap<PeerId, StoredEntry>,
    reaped_high_water: BTreeMap<PeerId, (u64, Incarnation)>,
}

impl MembershipTable {
    /// Creates a new cluster founded by the local node at `join_seq = 1`.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if signing the initial local `Alive` claim fails.
    pub fn found(
        signer: &impl ClaimSigner,
        init: LocalNodeInit,
        now: SimInstant,
    ) -> Result<Self, CoreError> {
        let local_id = signer.peer_id();
        let claim = MemberClaim {
            subject: local_id,
            session_id: init.session_id,
            incarnation: Incarnation(1),
            join_seq: 1,
            joined_at_unix_ms: init.joined_at_unix_ms,
            status: MemberStatus::Alive,
            metadata: init.metadata,
            addr_hint: init.addr_hint,
        };
        let initial_claim = SignedClaim::sign(claim, signer)?;
        let mut entries = BTreeMap::new();
        entries.insert(
            local_id,
            StoredEntry {
                state: MemberEntryState::from_claim(initial_claim),
                status_since: now,
                observed_alive_since: now,
            },
        );
        Ok(Self {
            local_id,
            local_session_id: init.session_id,
            lamport_clock: 1,
            cluster_epoch: 1,
            current_leader: Some(local_id),
            entries,
            reaped_high_water: BTreeMap::new(),
        })
    }

    /// Creates a table by merging an optional bootstrap snapshot and joining at `max_lamport + 1`.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if verifying the snapshot or signing the local claim fails.
    pub fn join_after_sync(
        signer: &impl ClaimSigner,
        init: LocalNodeInit,
        snapshot: Option<&SyncSnapshot>,
        verifier: &impl ClaimVerifier,
        now: SimInstant,
    ) -> Result<Self, CoreError> {
        let local_id = signer.peer_id();
        let mut table = Self {
            local_id,
            local_session_id: init.session_id,
            lamport_clock: 0,
            cluster_epoch: 0,
            current_leader: None,
            entries: BTreeMap::new(),
            reaped_high_water: BTreeMap::new(),
        };

        if let Some(snap) = snapshot {
            let _ = table.merge_snapshot(snap, verifier, signer, now)?;
        }

        let join_seq = table.lamport_clock.saturating_add(1).max(1);
        table.lamport_clock = join_seq;

        let claim = MemberClaim {
            subject: local_id,
            session_id: init.session_id,
            incarnation: Incarnation(1),
            join_seq,
            joined_at_unix_ms: init.joined_at_unix_ms,
            status: MemberStatus::Alive,
            metadata: init.metadata,
            addr_hint: init.addr_hint,
        };
        let initial_claim = SignedClaim::sign(claim, signer)?;
        let _ = table.apply_claim_internal(initial_claim, verifier, now)?;
        Ok(table)
    }

    /// Returns the local node's [`PeerId`].
    #[must_use]
    pub const fn local_id(&self) -> PeerId {
        self.local_id
    }

    /// Returns the local node's [`SessionId`].
    #[must_use]
    pub const fn local_session_id(&self) -> SessionId {
        self.local_session_id
    }

    /// Returns the highest Lamport join sequence observed by this node.
    #[must_use]
    pub const fn lamport_clock(&self) -> u64 {
        self.lamport_clock
    }

    /// Returns the current local leader-change epoch.
    #[must_use]
    pub const fn cluster_epoch(&self) -> u64 {
        self.cluster_epoch
    }

    /// Returns the local node's current signed self-claim, if present.
    #[must_use]
    pub fn local_claim(&self) -> Option<&SignedClaim> {
        self.entries.get(&self.local_id).map(|e| &e.state.claim)
    }

    /// Returns the current entry view for `peer_id`, if known and not reaped.
    #[must_use]
    pub fn member(&self, peer_id: PeerId) -> Option<MemberView> {
        self.entries.get(&peer_id).map(StoredEntry::to_view)
    }

    /// Returns all active (`Alive` or `Suspect`) members sorted deterministically by `(join_seq, peer_id)`.
    ///
    /// Keeping `Suspect` members in the active roster (matching `memberlist`)
    /// ensures transient probe drops do not prematurely evict a peer or flap leadership
    /// before Lifeguard suspicion expires into `Dead`.
    #[must_use]
    pub fn members_ordered(&self) -> Vec<MemberView> {
        let mut active: Vec<MemberView> = self
            .entries
            .values()
            .filter(|e| {
                matches!(
                    e.state.effective_status,
                    MemberStatus::Alive | MemberStatus::Suspect
                )
            })
            .map(StoredEntry::to_view)
            .collect();
        active.sort_by_key(MemberView::join_order);
        active
    }

    /// Returns the number of members currently in [`MemberStatus::Alive`].
    #[must_use]
    pub fn alive_count(&self) -> usize {
        self.entries
            .values()
            .filter(|e| e.state.effective_status == MemberStatus::Alive)
            .count()
    }

    /// Returns all known non-reaped members (any status) sorted by `(join_seq, peer_id)`.
    #[must_use]
    pub fn all_members_ordered(&self) -> Vec<MemberView> {
        let mut all: Vec<MemberView> = self.entries.values().map(StoredEntry::to_view).collect();
        all.sort_by_key(MemberView::join_order);
        all
    }

    /// Returns the oldest active (`Alive` or `Suspect`) member as the deterministic advisory leader.
    #[must_use]
    pub fn leader(&self) -> Option<MemberView> {
        self.members_ordered().into_iter().next()
    }

    /// Returns the advisory leader only if at least `min_alive` members are currently `Alive`.
    #[must_use]
    pub fn leader_with_quorum(&self, min_alive: usize) -> Option<MemberView> {
        if self.alive_count() < min_alive {
            return None;
        }
        self.leader()
    }

    /// Returns join-order and observed-uptime metadata for `peer_id` at monotonic time `now`.
    #[must_use]
    pub fn peer_age(&self, peer_id: PeerId, now: SimInstant) -> Option<PeerAge> {
        let entry = self.entries.get(&peer_id)?;
        let claim = &entry.state.claim.claim;
        let observed_alive_for = match entry.state.effective_status {
            MemberStatus::Alive | MemberStatus::Suspect => {
                now.elapsed_since(entry.observed_alive_since)
            }
            MemberStatus::Dead | MemberStatus::Left => Duration::ZERO,
        };
        Some(PeerAge {
            join_seq: claim.join_seq,
            observed_alive_for,
            joined_at_unix_ms: claim.joined_at_unix_ms,
        })
    }

    /// Verifies and merges a self-signed [`SignedClaim`] into the table.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidSignature`] if signature verification fails, or
    /// [`CoreError`] if signing a self-refutation fails.
    pub fn apply_claim(
        &mut self,
        claim_msg: SignedClaim,
        verifier: &impl ClaimVerifier,
        now: SimInstant,
    ) -> Result<ApplyOutcome, CoreError> {
        self.apply_claim_internal(claim_msg, verifier, now)
    }

    /// Verifies and merges a peer-signed `Suspect` or `Dead` alert.
    ///
    /// If the alert targets the local peer's active `SessionId` at `incarnation >= local`,
    /// this node immediately refutes by bumping `incarnation` while keeping its `join_seq`.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidSignature`] if the alert signature is invalid, or
    /// [`CoreError`] if signing a refutation fails.
    pub fn apply_indirect_alert(
        &mut self,
        alert: &SignedIndirectAlert,
        verifier: &impl ClaimVerifier,
        signer: &impl ClaimSigner,
        now: SimInstant,
    ) -> Result<ApplyOutcome, CoreError> {
        alert.verify(verifier)?;
        self.lamport_clock = self.lamport_clock.max(alert.payload.join_seq);
        let outcome = self.apply_verified_alert(alert, signer, now)?;
        Ok(outcome)
    }

    /// Verifies and merges a full [`SyncSnapshot`] from another peer.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if any claim/alert signature fails verification.
    pub fn merge_snapshot(
        &mut self,
        snapshot: &SyncSnapshot,
        verifier: &impl ClaimVerifier,
        signer: &impl ClaimSigner,
        now: SimInstant,
    ) -> Result<ApplyOutcome, CoreError> {
        self.lamport_clock = self.lamport_clock.max(snapshot.lamport_clock);
        let mut combined = ApplyOutcome::default();

        for claim in &snapshot.claims {
            let step = self.apply_claim_internal(claim.clone(), verifier, now)?;
            combined.changed |= step.changed;
            combined.events.extend(step.events);
            if step.refutation.is_some() {
                combined.refutation = step.refutation;
            }
        }

        for alert in &snapshot.indirect_alerts {
            if self.is_below_reaped_high_water(
                alert.payload.subject,
                alert.payload.join_seq,
                alert.payload.incarnation,
            ) {
                continue;
            }
            if let Some(existing) = self.entries.get(&alert.payload.subject)
                && existing.state.alert.as_ref() == Some(alert)
            {
                continue;
            }
            alert.verify(verifier)?;
            self.lamport_clock = self.lamport_clock.max(alert.payload.join_seq);
            let step = self.apply_verified_alert(alert, signer, now)?;
            combined.changed |= step.changed;
            combined.events.extend(step.events);
            if step.refutation.is_some() {
                combined.refutation = step.refutation;
            }
        }

        Ok(combined)
    }

    /// Marks the local node as gracefully `Left`, returning the signed claim to broadcast.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if signing fails.
    pub fn leave(
        &mut self,
        signer: &impl ClaimSigner,
        now: SimInstant,
    ) -> Result<(SignedClaim, ApplyOutcome), CoreError> {
        let (incarnation, join_seq, joined_at_unix_ms, metadata, addr_hint) =
            match self.entries.get(&self.local_id) {
                Some(existing) => {
                    let c = &existing.state.claim.claim;
                    (
                        existing.state.effective_incarnation.next(),
                        c.join_seq,
                        c.joined_at_unix_ms,
                        c.metadata.clone(),
                        c.addr_hint.clone(),
                    )
                }
                None => (
                    Incarnation(1),
                    self.lamport_clock.saturating_add(1).max(1),
                    0,
                    MemberMetadata::default(),
                    None,
                ),
            };
        let leave_claim = MemberClaim {
            subject: self.local_id,
            session_id: self.local_session_id,
            incarnation,
            join_seq,
            joined_at_unix_ms,
            status: MemberStatus::Left,
            metadata,
            addr_hint,
        };
        let departure_claim = SignedClaim::sign(leave_claim, signer)?;
        let prev_view = self.entries.get(&self.local_id).map(StoredEntry::to_view);
        let new_state = MemberEntryState::from_claim(departure_claim.clone());
        let stored = StoredEntry {
            state: new_state,
            status_since: now,
            observed_alive_since: now,
        };
        let new_view = stored.to_view();
        self.entries.insert(self.local_id, stored);

        let mut outcome = ApplyOutcome {
            changed: true,
            events: Vec::new(),
            refutation: None,
        };
        if prev_view
            .as_ref()
            .is_none_or(|p| p.status != MemberStatus::Left)
        {
            outcome.events.push(MembershipEvent::Left(new_view));
        }
        self.record_leader_change(&mut outcome.events);
        Ok((departure_claim, outcome))
    }

    /// Updates local metadata by incrementing the local incarnation while preserving `join_seq`.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if signing fails.
    pub fn update_local_metadata(
        &mut self,
        metadata: MemberMetadata,
        signer: &impl ClaimSigner,
        now: SimInstant,
    ) -> Result<(SignedClaim, ApplyOutcome), CoreError> {
        let (incarnation, join_seq, joined_at_unix_ms, addr_hint, observed_alive_since) =
            match self.entries.get(&self.local_id) {
                Some(existing) => {
                    let c = &existing.state.claim.claim;
                    (
                        existing.state.effective_incarnation.next(),
                        c.join_seq,
                        c.joined_at_unix_ms,
                        c.addr_hint.clone(),
                        existing.observed_alive_since,
                    )
                }
                None => (
                    Incarnation(1),
                    self.lamport_clock.saturating_add(1).max(1),
                    0,
                    None,
                    now,
                ),
            };
        let claim = MemberClaim {
            subject: self.local_id,
            session_id: self.local_session_id,
            incarnation,
            join_seq,
            joined_at_unix_ms,
            status: MemberStatus::Alive,
            metadata,
            addr_hint,
        };
        let updated_claim = SignedClaim::sign(claim, signer)?;
        let prev = self.entries.get(&self.local_id).map(StoredEntry::to_view);
        let stored = StoredEntry {
            state: MemberEntryState::from_claim(updated_claim.clone()),
            status_since: now,
            observed_alive_since,
        };
        let new_view = stored.to_view();
        self.entries.insert(self.local_id, stored);

        let mut outcome = ApplyOutcome {
            changed: true,
            events: Vec::new(),
            refutation: None,
        };
        if prev.is_some() {
            outcome.events.push(MembershipEvent::Updated(new_view));
        } else {
            outcome.events.push(MembershipEvent::Joined(new_view));
        }
        self.record_leader_change(&mut outcome.events);
        Ok((updated_claim, outcome))
    }

    /// Exports the current membership state as a [`SyncSnapshot`].
    #[must_use]
    pub fn snapshot(&self) -> SyncSnapshot {
        let claims = self
            .entries
            .values()
            .map(|e| e.state.claim.clone())
            .collect();
        let indirect_alerts = self
            .entries
            .values()
            .filter_map(|e| {
                e.state
                    .alert
                    .as_ref()
                    .filter(|al| alert_applies_to_claim(al, &e.state.claim))
                    .cloned()
            })
            .collect();
        SyncSnapshot {
            sender: self.local_id,
            lamport_clock: self.lamport_clock,
            claims,
            indirect_alerts,
        }
    }

    /// Reaps `Dead` and `Left` tombstones older than their TTLs while recording
    /// high-water `(join_seq, incarnation)` bounds so stale syncs never resurrect them.
    pub fn reap_expired(
        &mut self,
        now: SimInstant,
        dead_ttl_ms: u64,
        left_ttl_ms: u64,
    ) -> Vec<PeerId> {
        let mut to_reap = Vec::new();
        for (&peer_id, entry) in &self.entries {
            if peer_id == self.local_id {
                continue;
            }
            let ttl_ms = match entry.state.effective_status {
                MemberStatus::Dead => Some(dead_ttl_ms),
                MemberStatus::Left => Some(left_ttl_ms),
                MemberStatus::Alive | MemberStatus::Suspect => None,
            };
            if let Some(ttl) = ttl_ms
                && now.0.saturating_sub(entry.status_since.0) >= ttl
            {
                to_reap.push(peer_id);
            }
        }

        for peer_id in &to_reap {
            if let Some(removed) = self.entries.remove(peer_id) {
                let hw = (
                    removed.state.claim.claim.join_seq,
                    removed.state.effective_incarnation,
                );
                self.reaped_high_water
                    .entry(*peer_id)
                    .and_modify(|existing| {
                        if hw > *existing {
                            *existing = hw;
                        }
                    })
                    .or_insert(hw);
            }
        }

        to_reap
    }

    fn is_below_reaped_high_water(
        &self,
        peer_id: PeerId,
        join_seq: u64,
        incarnation: Incarnation,
    ) -> bool {
        self.reaped_high_water
            .get(&peer_id)
            .is_some_and(|&(hw_seq, hw_inc)| (join_seq, incarnation) <= (hw_seq, hw_inc))
    }

    fn apply_claim_internal(
        &mut self,
        claim_msg: SignedClaim,
        verifier: &impl ClaimVerifier,
        now: SimInstant,
    ) -> Result<ApplyOutcome, CoreError> {
        let peer_id = claim_msg.claim.subject;
        // Fast path: skip expensive Ed25519 verification for identical or strictly older claims
        // than an already-verified entry or reaped high-water mark.
        if self.is_below_reaped_high_water(
            peer_id,
            claim_msg.claim.join_seq,
            claim_msg.claim.incarnation,
        ) {
            return Ok(ApplyOutcome::default());
        }
        if let Some(existing) = self.entries.get(&peer_id)
            && (existing.state.claim == claim_msg
                || claim_rank(&claim_msg) <= claim_rank(&existing.state.claim))
        {
            return Ok(ApplyOutcome::default());
        }

        claim_msg.verify(verifier)?;
        self.lamport_clock = self.lamport_clock.max(claim_msg.claim.join_seq);

        let candidate = MemberEntryState::from_claim(claim_msg);
        Ok(self.merge_peer_state(peer_id, &candidate, now))
    }

    fn apply_verified_alert(
        &mut self,
        alert: &SignedIndirectAlert,
        signer: &impl ClaimSigner,
        now: SimInstant,
    ) -> Result<ApplyOutcome, CoreError> {
        let p = &alert.payload;

        if self.is_below_reaped_high_water(p.subject, p.join_seq, p.incarnation) {
            return Ok(ApplyOutcome::default());
        }

        // Local peer refutes suspicion or death against its active SessionId while preserving join_seq!
        if p.subject == self.local_id && p.session_id == self.local_session_id {
            let mut outcome = ApplyOutcome::default();
            let needs_refute = self.entries.get(&self.local_id).is_some_and(|local| {
                local.state.claim.claim.status != MemberStatus::Left
                    && p.incarnation >= local.state.effective_incarnation
            });
            if needs_refute && let Some(local) = self.entries.get(&self.local_id) {
                let c = &local.state.claim.claim;
                let bumped = MemberClaim {
                    subject: self.local_id,
                    session_id: self.local_session_id,
                    incarnation: p.incarnation.next(),
                    join_seq: c.join_seq,
                    joined_at_unix_ms: c.joined_at_unix_ms,
                    status: MemberStatus::Alive,
                    metadata: c.metadata.clone(),
                    addr_hint: c.addr_hint.clone(),
                };
                let refuted = SignedClaim::sign(bumped, signer)?;
                let mut step = self.merge_peer_state(
                    self.local_id,
                    &MemberEntryState::from_claim(refuted.clone()),
                    now,
                );
                step.refutation = Some(refuted);
                outcome = step;
            }
            return Ok(outcome);
        }

        let Some(existing) = self.entries.get(&p.subject) else {
            return Ok(ApplyOutcome::default());
        };
        if !alert_applies_to_claim(alert, &existing.state.claim) {
            return Ok(ApplyOutcome::default());
        }

        let mut candidate = existing.state.clone();
        candidate.alert = Some(alert.clone());
        let candidate = existing.state.merge(&candidate);
        Ok(self.merge_peer_state(p.subject, &candidate, now))
    }

    fn merge_peer_state(
        &mut self,
        peer_id: PeerId,
        candidate: &MemberEntryState,
        now: SimInstant,
    ) -> ApplyOutcome {
        let mut outcome = ApplyOutcome::default();

        match self.entries.get(&peer_id) {
            None => {
                let stored = StoredEntry {
                    state: candidate.clone(),
                    status_since: now,
                    observed_alive_since: now,
                };
                let view = stored.to_view();
                self.entries.insert(peer_id, stored);
                outcome.changed = true;
                match view.status {
                    MemberStatus::Alive => outcome.events.push(MembershipEvent::Joined(view)),
                    MemberStatus::Dead => outcome.events.push(MembershipEvent::Failed(view)),
                    MemberStatus::Left => outcome.events.push(MembershipEvent::Left(view)),
                    MemberStatus::Suspect => outcome.events.push(MembershipEvent::Suspected {
                        peer_id: view.peer_id,
                        incarnation: view.incarnation,
                    }),
                }
            }
            Some(existing) => {
                let merged = existing.state.merge(candidate);
                if merged == existing.state {
                    return outcome;
                }
                let prev_status = existing.state.effective_status;
                let prev_session = existing.state.claim.claim.session_id;
                let prev_alive_since = existing.observed_alive_since;
                let prev_status_since = existing.status_since;

                let new_status = merged.effective_status;
                let new_session = merged.claim.claim.session_id;

                let observed_alive_since = if prev_session != new_session
                    || prev_status != MemberStatus::Alive && new_status == MemberStatus::Alive
                {
                    now
                } else {
                    prev_alive_since
                };
                let status_since = if prev_status == new_status {
                    prev_status_since
                } else {
                    now
                };

                let stored = StoredEntry {
                    state: merged,
                    status_since,
                    observed_alive_since,
                };
                let view = stored.to_view();
                self.entries.insert(peer_id, stored);
                outcome.changed = true;

                if prev_status != new_status {
                    match new_status {
                        MemberStatus::Alive => outcome.events.push(MembershipEvent::Joined(view)),
                        MemberStatus::Suspect => outcome.events.push(MembershipEvent::Suspected {
                            peer_id: view.peer_id,
                            incarnation: view.incarnation,
                        }),
                        MemberStatus::Dead => outcome.events.push(MembershipEvent::Failed(view)),
                        MemberStatus::Left => outcome.events.push(MembershipEvent::Left(view)),
                    }
                } else if new_status == MemberStatus::Alive {
                    outcome.events.push(MembershipEvent::Updated(view));
                }
            }
        }

        self.record_leader_change(&mut outcome.events);
        outcome
    }

    fn record_leader_change(&mut self, events: &mut Vec<MembershipEvent>) {
        let next_leader = self.leader().map(|m| m.peer_id);
        if next_leader != self.current_leader {
            let previous = self.current_leader;
            self.current_leader = next_leader;
            self.cluster_epoch = self.cluster_epoch.saturating_add(1);
            events.push(MembershipEvent::LeaderChanged {
                previous,
                current: next_leader,
                epoch: self.cluster_epoch,
            });
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn entry_strategy(peer: PeerId) -> impl Strategy<Value = MemberEntryState> {
        (
            1u64..=5u64,
            1u64..=5u64,
            prop_oneof![Just(MemberStatus::Alive), Just(MemberStatus::Left)],
            1u64..=3u64,
            any::<[u8; 64]>(),
            prop::option::of((
                1u64..=5u64,
                prop_oneof![Just(IndirectKind::Suspect), Just(IndirectKind::Dead)],
                any::<[u8; 32]>(),
                any::<[u8; 64]>(),
            )),
        )
            .prop_map(move |(join_seq, inc, status, session, sig, maybe_alert)| {
                let claim = SignedClaim {
                    claim: MemberClaim {
                        subject: peer,
                        session_id: SessionId(session),
                        incarnation: Incarnation(inc),
                        join_seq,
                        joined_at_unix_ms: 1_700_000_000_000,
                        status,
                        metadata: MemberMetadata::default(),
                        addr_hint: None,
                    },
                    signature: sig,
                };
                let mut state = MemberEntryState::from_claim(claim);
                if let Some((al_inc, kind, rep, al_sig)) = maybe_alert {
                    let alert = SignedIndirectAlert {
                        payload: crate::IndirectAlertPayload {
                            subject: peer,
                            session_id: SessionId(session),
                            incarnation: Incarnation(al_inc),
                            join_seq,
                            kind,
                            reporter: PeerId::from_bytes(rep),
                        },
                        signature: al_sig,
                    };
                    let with_alert = MemberEntryState {
                        alert: Some(alert),
                        ..state.clone()
                    };
                    state = state.merge(&with_alert);
                }
                state
            })
    }

    proptest! {
        #[test]
        fn entry_merge_is_commutative_associative_and_idempotent(
            peer_bytes in any::<[u8; 32]>(),
            a in entry_strategy(PeerId::from_bytes([7u8; 32])),
            b in entry_strategy(PeerId::from_bytes([7u8; 32])),
            c in entry_strategy(PeerId::from_bytes([7u8; 32])),
        ) {
            let _ = peer_bytes;
            // Idempotence
            prop_assert_eq!(&a.merge(&a), &a);
            // Commutativity
            prop_assert_eq!(a.merge(&b), b.merge(&a));
            // Associativity
            prop_assert_eq!(a.merge(&b).merge(&c), a.merge(&b.merge(&c)));
        }
    }
}

#[cfg(kani)]
mod kani_proofs {
    use super::*;

    fn any_entry(peer: PeerId) -> MemberEntryState {
        let join_seq: u64 = kani::any();
        let inc: u64 = kani::any();
        let session: u64 = kani::any();
        let left: bool = kani::any();
        let claim = SignedClaim {
            claim: MemberClaim {
                subject: peer,
                session_id: SessionId(session),
                incarnation: Incarnation(inc),
                join_seq,
                joined_at_unix_ms: 0,
                status: if left {
                    MemberStatus::Left
                } else {
                    MemberStatus::Alive
                },
                metadata: MemberMetadata::default(),
                addr_hint: None,
            },
            signature: [kani::any(); 64],
        };
        MemberEntryState::from_claim(claim)
    }

    #[kani::proof]
    fn verify_entry_merge_semilattice_laws() {
        let peer = PeerId::from_bytes([1u8; 32]);
        let a = any_entry(peer);
        let b = any_entry(peer);
        let c = any_entry(peer);

        assert!(a.merge(&a) == a);
        assert!(a.merge(&b) == b.merge(&a));
        assert!(a.merge(&b).merge(&c) == a.merge(&b.merge(&c)));
    }
}
