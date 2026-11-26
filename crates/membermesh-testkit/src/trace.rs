use std::collections::{BTreeMap, BTreeSet};

use membermesh_core::{MemberView, PeerId, SessionId};

/// Single sampled observation of a live cluster node's membership state.
#[derive(Clone, Debug)]
pub struct NodeObservation {
    /// Active ordered roster observed by this node.
    pub roster: Vec<MemberView>,
    /// Advisory leader observed by this node.
    pub leader: Option<PeerId>,
    /// Quorum-guarded leader observed by this node, if `quorum` was checked.
    pub quorum_leader: Option<PeerId>,
}

/// Timestamped cluster-wide snapshot recorded during live `iroh` cluster runs.
#[derive(Clone, Debug)]
pub struct ClusterTraceSample {
    /// Whether the cluster is currently partitioned or has settled post-partition.
    pub settled: bool,
    /// Observations keyed by node name.
    pub nodes: BTreeMap<String, NodeObservation>,
}

#[derive(Clone, Debug)]
struct RecordedSample {
    sample: ClusterTraceSample,
    left_sessions_so_far: BTreeSet<(PeerId, SessionId)>,
}

/// Recorder and formal invariant checker for live offline `iroh` cluster runs.
#[derive(Clone, Debug, Default)]
pub struct TraceRecorder {
    samples: Vec<RecordedSample>,
    left_sessions: BTreeSet<(PeerId, SessionId)>,
}

impl TraceRecorder {
    /// Creates an empty trace recorder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a gracefully departed `(PeerId, SessionId)` pair that must never resurrect
    /// in any subsequent settled observation.
    pub fn record_left_session(&mut self, peer_id: PeerId, session_id: SessionId) {
        self.left_sessions.insert((peer_id, session_id));
    }

    /// Appends a cluster-wide observation sample.
    pub fn push_sample(&mut self, sample: ClusterTraceSample) {
        let left_sessions_so_far = self.left_sessions.clone();
        self.samples.push(RecordedSample {
            sample,
            left_sessions_so_far,
        });
    }

    /// Verifies formal safety invariants across every recorded live cluster sample.
    ///
    /// # Errors
    ///
    /// Returns a descriptive violation string if any formal invariant is broken.
    pub fn verify_invariants(&self) -> Result<(), String> {
        for (idx, recorded) in self.samples.iter().enumerate() {
            let sample = &recorded.sample;
            // 1. Total order agreement whenever two nodes observe the same active PeerId set.
            let entries: Vec<(&String, &NodeObservation)> = sample.nodes.iter().collect();
            for i in 0..entries.len() {
                for j in (i + 1)..entries.len() {
                    let (n1, o1) = entries.get(i).ok_or("index error")?;
                    let (n2, o2) = entries.get(j).ok_or("index error")?;
                    let ord1: Vec<PeerId> = o1.roster.iter().map(|m| m.peer_id).collect();
                    let ord2: Vec<PeerId> = o2.roster.iter().map(|m| m.peer_id).collect();
                    let set1: BTreeSet<PeerId> = ord1.iter().copied().collect();
                    let set2: BTreeSet<PeerId> = ord2.iter().copied().collect();
                    if set1 == set2 && (ord1 != ord2 || o1.leader != o2.leader) {
                        return Err(format!(
                            "sample {idx}: total_order_agreement violated between {n1} and {n2}"
                        ));
                    }
                }
            }

            // 2. Settled quorum exclusivity & no resurrection of gracefully Left sessions.
            if sample.settled {
                let mut self_quorum_leaders = BTreeSet::new();
                for obs in sample.nodes.values() {
                    if let Some(ql) = obs.quorum_leader
                        && obs.roster.iter().any(|m| m.peer_id == ql)
                    {
                        self_quorum_leaders.insert(ql);
                    }
                }
                if self_quorum_leaders.len() > 1 {
                    return Err(format!(
                        "sample {idx}: settled_quorum_exclusivity violated: {self_quorum_leaders:?}"
                    ));
                }

                for (name, obs) in &sample.nodes {
                    for m in &obs.roster {
                        if recorded
                            .left_sessions_so_far
                            .contains(&(m.peer_id, m.session_id))
                        {
                            return Err(format!(
                                "sample {idx}: node {name} resurrected gracefully left session ({}, {})",
                                m.peer_id, m.session_id
                            ));
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
