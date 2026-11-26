use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};

use crate::{
    CoreError, DEFAULT_MAX_GOSSIP_BYTES, GossipEnvelope, GossipItem, GossipMessage, PeerId,
    SimInstant,
};

/// Maximum Lifeguard Local Health Awareness (LHA) score (`0..=8`).
pub const MAX_LHA_SCORE: u8 = 8;

/// Protocol timing and failure-detection configuration for [`crate::Engine`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ProtocolConfig {
    /// Base interval between SWIM probe rounds (milliseconds).
    pub probe_interval_ms: u64,
    /// Timeout waiting for a direct `Ping` `Ack` before triggering indirect `PingReq`s (milliseconds).
    pub probe_timeout_ms: u64,
    /// Number of indirect intermediary peers (`k`) queried on a missed direct probe.
    pub indirect_checks: usize,
    /// Minimum Lifeguard suspicion timeout multiplier (`min = probe_interval * suspicion_min_mult`).
    pub suspicion_min_mult: u64,
    /// Maximum Lifeguard suspicion timeout multiplier (`max = probe_interval * suspicion_max_mult`).
    pub suspicion_max_mult: u64,
    /// Number of independent corroborating confirmations (`c`) to decay suspicion toward `min`.
    pub suspicion_confirmations: usize,
    /// Interval between periodic Push/Pull anti-entropy syncs (milliseconds).
    pub anti_entropy_interval_ms: u64,
    /// Retransmit multiplier `lambda` (`max_transmits = lambda * ceil(log10(N + 1))`).
    pub retransmit_mult: u32,
    /// Minimum age before reaping a `Dead` tombstone (milliseconds).
    pub dead_reap_ttl_ms: u64,
    /// Minimum age before reaping a `Left` tombstone (milliseconds).
    pub left_reap_ttl_ms: u64,
    /// Maximum encoded gossip envelope byte size.
    pub max_gossip_bytes: usize,
    /// Interval between tombstone reap sweeps (milliseconds).
    pub reap_interval_ms: u64,
}

impl ProtocolConfig {
    /// Fast timing profile tuned for low-latency LAN or local simulation meshes.
    #[must_use]
    pub const fn lan() -> Self {
        Self {
            probe_interval_ms: 500,
            probe_timeout_ms: 250,
            indirect_checks: 3,
            suspicion_min_mult: 4,
            suspicion_max_mult: 6,
            suspicion_confirmations: 3,
            anti_entropy_interval_ms: 2_000,
            retransmit_mult: 3,
            dead_reap_ttl_ms: 30_000,
            left_reap_ttl_ms: 30_000,
            max_gossip_bytes: DEFAULT_MAX_GOSSIP_BYTES,
            reap_interval_ms: 5_000,
        }
    }

    /// Balanced timing profile tuned for WAN / internet direct QUIC meshes.
    #[must_use]
    pub const fn wan() -> Self {
        Self {
            probe_interval_ms: 1_000,
            probe_timeout_ms: 500,
            indirect_checks: 3,
            suspicion_min_mult: 4,
            suspicion_max_mult: 6,
            suspicion_confirmations: 3,
            anti_entropy_interval_ms: 5_000,
            retransmit_mult: 4,
            dead_reap_ttl_ms: 60_000,
            left_reap_ttl_ms: 60_000,
            max_gossip_bytes: DEFAULT_MAX_GOSSIP_BYTES,
            reap_interval_ms: 10_000,
        }
    }

    /// Conservative timing profile tuned for NAT-traversed / `iroh` Relay fallback paths.
    #[must_use]
    pub const fn relay() -> Self {
        Self {
            probe_interval_ms: 2_000,
            probe_timeout_ms: 1_000,
            indirect_checks: 3,
            suspicion_min_mult: 4,
            suspicion_max_mult: 8,
            suspicion_confirmations: 3,
            anti_entropy_interval_ms: 8_000,
            retransmit_mult: 4,
            dead_reap_ttl_ms: 90_000,
            left_reap_ttl_ms: 90_000,
            max_gossip_bytes: DEFAULT_MAX_GOSSIP_BYTES,
            reap_interval_ms: 15_000,
        }
    }

    /// Validates configuration invariants.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidConfig`] if intervals, multipliers, or frame limits are invalid.
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.probe_interval_ms == 0 || self.probe_timeout_ms == 0 {
            return Err(CoreError::InvalidConfig {
                reason: "probe_interval_ms and probe_timeout_ms must be positive".to_owned(),
            });
        }
        if self.probe_timeout_ms.saturating_mul(2) > self.probe_interval_ms {
            return Err(CoreError::InvalidConfig {
                reason: "probe_timeout_ms * 2 must not exceed probe_interval_ms".to_owned(),
            });
        }
        if self.suspicion_min_mult == 0 || self.suspicion_max_mult < self.suspicion_min_mult {
            return Err(CoreError::InvalidConfig {
                reason: "suspicion_max_mult must be >= suspicion_min_mult > 0".to_owned(),
            });
        }
        if self.anti_entropy_interval_ms == 0 || self.retransmit_mult == 0 {
            return Err(CoreError::InvalidConfig {
                reason: "anti_entropy_interval_ms and retransmit_mult must be positive".to_owned(),
            });
        }
        if self.max_gossip_bytes < 256 {
            return Err(CoreError::InvalidConfig {
                reason: "max_gossip_bytes must be at least 256 bytes".to_owned(),
            });
        }
        Ok(())
    }
}

impl Default for ProtocolConfig {
    fn default() -> Self {
        Self::wan()
    }
}

/// Lifeguard Local Health Awareness (LHA) score (`0..=8`).
///
/// Tracks missed probes, missed indirect responses, or self-suspicion refutations
/// so a locally overloaded node scales its probe intervals and suspicion timeouts
/// instead of falsely evicting healthy cluster peers.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct LhaState {
    score: u8,
}

impl LhaState {
    /// Returns the current LHA saturation score (`0..=8`).
    #[must_use]
    pub const fn score(self) -> u8 {
        self.score
    }

    /// Increments the local health degradation score, clamped at [`MAX_LHA_SCORE`].
    pub const fn increment(&mut self) {
        if self.score < MAX_LHA_SCORE {
            self.score = self.score.saturating_add(1);
        }
    }

    /// Decrements the local health degradation score, clamped at `0`.
    pub const fn decrement(&mut self) {
        self.score = self.score.saturating_sub(1);
    }

    /// Returns the interval multiplier `score + 1` (`1..=9`).
    #[must_use]
    pub const fn multiplier(self) -> u64 {
        (self.score as u64).saturating_add(1)
    }

    /// Scales a base interval by `(score + 1)`.
    #[must_use]
    pub const fn scale_ms(self, base_ms: u64) -> u64 {
        base_ms.saturating_mul(self.multiplier())
    }
}

/// Fixed-point base-2 logarithm scaled by `1024` for deterministic, float-free
/// Lifeguard suspicion decay (`x >= 1`).
#[must_use]
pub fn log2_fp1024(x: u64) -> u64 {
    if x <= 1 {
        return 0;
    }
    let bit = u64::from(63_u32.saturating_sub(x.leading_zeros()));
    let base = 1_u64 << bit;
    let rem = x.saturating_sub(base);
    // Linear fractional interpolation across [base, 2*base), scaled by 1024.
    let frac = rem.saturating_mul(1024) >> bit;
    bit.saturating_mul(1024).saturating_add(frac)
}

/// Computes the deterministic Lifeguard suspicion timeout in milliseconds.
///
/// Decays logarithmically from `max_ms` (0 independent confirmations) toward
/// `min_ms` (`confirmations >= expected_confirmations`), scaled by the local node's
/// LHA health multiplier so locally stalled nodes wait longer before declaring peers dead.
#[must_use]
pub fn compute_suspicion_timeout_ms(
    config: &ProtocolConfig,
    confirmations: usize,
    lha: LhaState,
) -> u64 {
    let min_ms = config
        .probe_interval_ms
        .saturating_mul(config.suspicion_min_mult);
    let max_ms = config
        .probe_interval_ms
        .saturating_mul(config.suspicion_max_mult)
        .max(min_ms);
    let span = max_ms.saturating_sub(min_ms);

    let base_timeout = if span == 0 || confirmations == 0 || config.suspicion_confirmations == 0 {
        max_ms
    } else {
        let num = log2_fp1024((confirmations as u64).saturating_add(1));
        let den = log2_fp1024((config.suspicion_confirmations as u64).saturating_add(1)).max(1);
        let ratio_fp = num.min(den);
        let reduction = span.saturating_mul(ratio_fp).checked_div(den).unwrap_or(0);
        max_ms.saturating_sub(reduction).max(min_ms)
    };

    // Scale softly with LHA: add 50% of base_timeout per LHA point above 0 so a
    // locally stalled node does not prematurely mark healthy peers dead on wake-up.
    let extra = base_timeout
        .saturating_mul(u64::from(lha.score()))
        .checked_div(2)
        .unwrap_or(0);
    base_timeout.saturating_add(extra)
}

/// Active Lifeguard suspicion timer state for a suspected peer.
#[derive(Clone, Debug)]
pub struct ActiveSuspicion {
    /// Target peer being suspected.
    pub peer_id: PeerId,
    /// Time when suspicion began.
    pub started_at: SimInstant,
    /// Current computed deadline when this peer transitions to `Dead`.
    pub deadline: SimInstant,
    /// Distinct peers (excluding local) that corroborated suspicion.
    pub confirmations: BTreeSet<PeerId>,
}

impl ActiveSuspicion {
    /// Creates a new active suspicion entry.
    #[must_use]
    pub fn new(
        peer_id: PeerId,
        started_at: SimInstant,
        config: &ProtocolConfig,
        lha: LhaState,
    ) -> Self {
        let timeout_ms = compute_suspicion_timeout_ms(config, 0, lha);
        Self {
            peer_id,
            started_at,
            deadline: started_at.saturating_add_ms(timeout_ms),
            confirmations: BTreeSet::new(),
        }
    }

    /// Records a corroborating reporter and tightens `deadline` if applicable.
    pub fn add_confirmation(
        &mut self,
        reporter: PeerId,
        config: &ProtocolConfig,
        lha: LhaState,
    ) -> bool {
        if !self.confirmations.insert(reporter) {
            return false;
        }
        let timeout_ms = compute_suspicion_timeout_ms(config, self.confirmations.len(), lha);
        let new_deadline = self.started_at.saturating_add_ms(timeout_ms);
        if new_deadline < self.deadline {
            self.deadline = new_deadline;
        }
        true
    }
}

/// Integer ceiling of `log10(n)` (`>= 1` for `n >= 2`).
#[must_use]
pub const fn ceil_log10(n: usize) -> u32 {
    if n <= 10 {
        return 1;
    }
    let mut pow = 10_usize;
    let mut exp = 1_u32;
    while pow < n {
        match pow.checked_mul(10) {
            Some(next) => {
                pow = next;
                exp = exp.saturating_add(1);
            }
            None => return exp.saturating_add(1),
        }
    }
    exp
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum QueueKey {
    Claim(PeerId),
    Alert(PeerId),
}

#[derive(Clone, Debug)]
struct QueuedItem {
    item: GossipItem,
    transmits: u32,
    seq: u64,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct HeapOrder {
    transmits: u32,
    seq: u64,
}

/// HashiCorp-style bounded gossip retransmit queue (`TransmitLimitedQueue`).
///
/// Each membership update is transmitted up to `retransmit_mult * ceil(log10(N + 1))`
/// times, prioritizing items with the fewest transmissions so fresh updates propagate first.
#[derive(Clone, Debug, Default)]
pub struct TransmitLimitedQueue {
    items: BTreeMap<QueueKey, QueuedItem>,
    next_seq: u64,
}

impl TransmitLimitedQueue {
    /// Queues or supersedes an update item, resetting its transmit counter to 0.
    pub fn push(&mut self, item: GossipItem) {
        let key = match &item {
            GossipItem::Claim(c) => QueueKey::Claim(c.claim.subject),
            GossipItem::IndirectAlert(a) => QueueKey::Alert(a.payload.subject),
        };
        let seq = self.next_seq;
        self.next_seq = self.next_seq.saturating_add(1);
        self.items.insert(
            key,
            QueuedItem {
                item,
                transmits: 0,
                seq,
            },
        );
    }

    /// Returns whether any items remain queued for gossip transmission.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Pops and encodes the next bounded batch of gossip items that fits inside `max_bytes`.
    pub fn pop_encoded_batch(
        &mut self,
        sender: PeerId,
        nonce: u64,
        cluster_size: usize,
        retransmit_mult: u32,
        max_bytes: usize,
    ) -> Option<bytes::Bytes> {
        if self.items.is_empty() {
            return None;
        }
        let max_transmits = retransmit_mult
            .saturating_mul(ceil_log10(cluster_size.saturating_add(1)))
            .max(1);

        let mut heap: BinaryHeap<Reverse<(HeapOrder, QueueKey)>> = self
            .items
            .iter()
            .map(|(k, v)| {
                Reverse((
                    HeapOrder {
                        transmits: v.transmits,
                        seq: v.seq,
                    },
                    k.clone(),
                ))
            })
            .collect();

        let mut chosen_keys = Vec::new();
        let mut chosen_items = Vec::new();

        while let Some(Reverse((_, key))) = heap.pop() {
            let Some(entry) = self.items.get(&key) else {
                continue;
            };
            chosen_items.push(entry.item.clone());
            let candidate_msg = if chosen_items.len() == 1 {
                GossipMessage::Single(entry.item.clone())
            } else {
                GossipMessage::Compound(chosen_items.clone())
            };
            let env = GossipEnvelope::new(nonce, sender, candidate_msg);
            if env.encode(max_bytes).is_ok() {
                chosen_keys.push(key);
                if chosen_items.len() >= 8 {
                    break;
                }
            } else {
                chosen_items.pop();
                if chosen_items.is_empty() {
                    // Single item itself exceeded max_bytes; drop it so it doesn't wedge queue.
                    self.items.remove(&key);
                    continue;
                }
                break;
            }
        }

        if chosen_items.is_empty() {
            return None;
        }

        for key in &chosen_keys {
            let should_remove = if let Some(entry) = self.items.get_mut(key) {
                entry.transmits = entry.transmits.saturating_add(1);
                entry.transmits >= max_transmits
            } else {
                false
            };
            if should_remove {
                self.items.remove(key);
            }
        }

        let msg = match chosen_items.as_slice() {
            [single] => GossipMessage::Single(single.clone()),
            _ => GossipMessage::Compound(chosen_items),
        };
        GossipEnvelope::new(nonce, sender, msg)
            .encode(max_bytes)
            .ok()
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
    use super::*;

    #[test]
    fn suspicion_timeout_stays_within_bounds_and_scales_with_lha() {
        let cfg = ProtocolConfig::lan();
        let zero_lha = LhaState::default();
        let t0 = compute_suspicion_timeout_ms(&cfg, 0, zero_lha);
        let t1 = compute_suspicion_timeout_ms(&cfg, 1, zero_lha);
        let t3 = compute_suspicion_timeout_ms(&cfg, 3, zero_lha);
        assert_eq!(t0, cfg.probe_interval_ms * cfg.suspicion_max_mult);
        assert!(t1 <= t0);
        assert_eq!(t3, cfg.probe_interval_ms * cfg.suspicion_min_mult);

        let mut high_lha = LhaState::default();
        high_lha.increment();
        high_lha.increment();
        let t_stalled = compute_suspicion_timeout_ms(&cfg, 3, high_lha);
        assert!(t_stalled > t3);
    }
}

#[cfg(kani)]
mod kani_proofs {
    use super::*;

    #[kani::proof]
    fn verify_lifeguard_suspicion_math_never_panics_or_violates_bounds() {
        let mut cfg = ProtocolConfig::lan();
        let confirmations: usize = kani::any();
        let lha_steps: u8 = kani::any();
        kani::assume(lha_steps <= 12);
        let mut lha = LhaState::default();
        for _ in 0..lha_steps {
            lha.increment();
        }
        cfg.suspicion_confirmations = kani::any();
        kani::assume(cfg.suspicion_confirmations <= 16);

        let timeout = compute_suspicion_timeout_ms(&cfg, confirmations, lha);
        let min_ms = cfg.probe_interval_ms * cfg.suspicion_min_mult;
        assert!(timeout >= min_ms);
    }
}
