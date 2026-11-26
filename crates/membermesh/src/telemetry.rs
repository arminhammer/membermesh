//! OpenTelemetry metric descriptors and internal instrumentation helpers for `membermesh`.

#[cfg(feature = "otel")]
use std::sync::{Arc, OnceLock};
use std::time::Duration;

#[cfg(feature = "otel")]
use iroh_gossip::TopicId;
use membermesh_core::{ControlMessage, MemberStatus, MembershipEvent};

/// OpenTelemetry meter name used by `membermesh`.
pub const METER_NAME: &str = "membermesh";

/// Gauge tracking active (`Alive` or `Suspect`) cluster members.
pub const METRIC_MEMBERS_ACTIVE: &str = "membermesh.members.active";
/// Gauge tracking currently `Alive` cluster members.
pub const METRIC_MEMBERS_ALIVE: &str = "membermesh.members.alive";
/// Gauge tracking currently `Suspect` cluster members.
pub const METRIC_MEMBERS_SUSPECT: &str = "membermesh.members.suspect";
/// Gauge tracking the local cluster leader-change epoch.
pub const METRIC_CLUSTER_EPOCH: &str = "membermesh.cluster.epoch";
/// Gauge tracking the local Lifeguard LHA health score (`0..=8`).
pub const METRIC_LHA_SCORE: &str = "membermesh.lha.score";
/// Gauge set to `1` when the local node is the current advisory leader, else `0`.
pub const METRIC_IS_LEADER: &str = "membermesh.is_leader";
/// Monotonic counter tracking emitted [`MembershipEvent`] notifications.
pub const METRIC_EVENTS_TOTAL: &str = "membermesh.events.total";
/// Monotonic counter tracking outbound `iroh-gossip` broadcasts.
pub const METRIC_GOSSIP_BROADCAST_TOTAL: &str = "membermesh.gossip.broadcast.total";
/// Histogram tracking encoded `iroh-gossip` broadcast payload sizes in bytes.
pub const METRIC_GOSSIP_BROADCAST_BYTES: &str = "membermesh.gossip.broadcast.bytes";
/// Monotonic counter tracking inbound `iroh-gossip` events.
pub const METRIC_GOSSIP_RECEIVED_TOTAL: &str = "membermesh.gossip.received.total";
/// Monotonic counter tracking outbound `/membermesh/1` QUIC control/sync RPCs.
pub const METRIC_RPC_SENT_TOTAL: &str = "membermesh.rpc.sent.total";
/// Monotonic counter tracking inbound `/membermesh/1` QUIC control/sync RPCs.
pub const METRIC_RPC_RECEIVED_TOTAL: &str = "membermesh.rpc.received.total";
/// Histogram tracking `/membermesh/1` RPC round-trip durations in seconds.
pub const METRIC_RPC_DURATION_SECS: &str = "membermesh.rpc.duration";
/// Monotonic counter tracking cluster admission policy evaluations (`allowed` vs `denied`).
pub const METRIC_ADMISSION_CHECKS_TOTAL: &str = "membermesh.admission.checks.total";

#[derive(Clone, Debug, Default)]
pub(crate) struct MeshMetrics {
    #[cfg(feature = "otel")]
    cell: Arc<OnceLock<Option<OtelInstruments>>>,
}

#[cfg(feature = "otel")]
#[derive(Clone, Debug)]
struct OtelInstruments {
    topic_label: String,
    members_active: opentelemetry::metrics::Gauge<u64>,
    members_alive: opentelemetry::metrics::Gauge<u64>,
    members_suspect: opentelemetry::metrics::Gauge<u64>,
    cluster_epoch: opentelemetry::metrics::Gauge<u64>,
    lha_score: opentelemetry::metrics::Gauge<u64>,
    is_leader: opentelemetry::metrics::Gauge<u64>,
    events_total: opentelemetry::metrics::Counter<u64>,
    gossip_broadcast_total: opentelemetry::metrics::Counter<u64>,
    gossip_broadcast_bytes: opentelemetry::metrics::Histogram<u64>,
    gossip_received_total: opentelemetry::metrics::Counter<u64>,
    rpc_sent_total: opentelemetry::metrics::Counter<u64>,
    rpc_received_total: opentelemetry::metrics::Counter<u64>,
    rpc_duration_secs: opentelemetry::metrics::Histogram<f64>,
    admission_checks_total: opentelemetry::metrics::Counter<u64>,
}

#[cfg(feature = "otel")]
impl OtelInstruments {
    fn new(meter: &opentelemetry::metrics::Meter, topic_label: String) -> Self {
        Self {
            topic_label,
            members_active: meter
                .u64_gauge(METRIC_MEMBERS_ACTIVE)
                .with_description("Number of active (Alive or Suspect) cluster members")
                .build(),
            members_alive: meter
                .u64_gauge(METRIC_MEMBERS_ALIVE)
                .with_description("Number of Alive cluster members")
                .build(),
            members_suspect: meter
                .u64_gauge(METRIC_MEMBERS_SUSPECT)
                .with_description("Number of Suspect cluster members")
                .build(),
            cluster_epoch: meter
                .u64_gauge(METRIC_CLUSTER_EPOCH)
                .with_description("Monotonic local leader-change epoch")
                .build(),
            lha_score: meter
                .u64_gauge(METRIC_LHA_SCORE)
                .with_description("Lifeguard Local Health Awareness score (0..=8)")
                .build(),
            is_leader: meter
                .u64_gauge(METRIC_IS_LEADER)
                .with_description("1 if this node is the current advisory leader, else 0")
                .build(),
            events_total: meter
                .u64_counter(METRIC_EVENTS_TOTAL)
                .with_description("Total membership events emitted by the engine")
                .build(),
            gossip_broadcast_total: meter
                .u64_counter(METRIC_GOSSIP_BROADCAST_TOTAL)
                .with_description("Total outbound iroh-gossip broadcasts")
                .build(),
            gossip_broadcast_bytes: meter
                .u64_histogram(METRIC_GOSSIP_BROADCAST_BYTES)
                .with_description("Encoded payload size of outbound iroh-gossip broadcasts")
                .with_unit("By")
                .build(),
            gossip_received_total: meter
                .u64_counter(METRIC_GOSSIP_RECEIVED_TOTAL)
                .with_description("Total inbound iroh-gossip bridge events")
                .build(),
            rpc_sent_total: meter
                .u64_counter(METRIC_RPC_SENT_TOTAL)
                .with_description("Total outbound /membermesh/1 QUIC control/sync RPCs")
                .build(),
            rpc_received_total: meter
                .u64_counter(METRIC_RPC_RECEIVED_TOTAL)
                .with_description("Total inbound /membermesh/1 QUIC control/sync RPCs")
                .build(),
            rpc_duration_secs: meter
                .f64_histogram(METRIC_RPC_DURATION_SECS)
                .with_description("Outbound /membermesh/1 QUIC RPC duration in seconds")
                .with_unit("s")
                .build(),
            admission_checks_total: meter
                .u64_counter(METRIC_ADMISSION_CHECKS_TOTAL)
                .with_description("Total cluster admission policy evaluations")
                .build(),
        }
    }
}

#[cfg(feature = "otel")]
pub(crate) fn format_topic_short(topic_id: TopicId) -> String {
    let [b0, b1, b2, b3, ..] = *topic_id.as_bytes();
    format!("{b0:02x}{b1:02x}{b2:02x}{b3:02x}")
}

impl MeshMetrics {
    pub(crate) fn noop() -> Self {
        #[cfg(feature = "otel")]
        {
            let cell = Arc::new(OnceLock::new());
            let _ = cell.set(None);
            Self { cell }
        }
        #[cfg(not(feature = "otel"))]
        {
            Self {}
        }
    }

    pub(crate) fn lazy() -> Self {
        Self::default()
    }

    #[cfg(feature = "otel")]
    pub(crate) fn init_with_optional_meter(
        &self,
        meter: Option<&opentelemetry::metrics::Meter>,
        topic_id: TopicId,
    ) {
        let fallback;
        let active_meter = if let Some(m) = meter {
            m
        } else {
            fallback = opentelemetry::global::meter(METER_NAME);
            &fallback
        };
        let _ = self.cell.set(Some(OtelInstruments::new(
            active_meter,
            format_topic_short(topic_id),
        )));
    }

    #[cfg(feature = "otel")]
    fn instruments(&self) -> Option<&OtelInstruments> {
        self.cell.get().and_then(Option::as_ref)
    }

    pub(crate) fn record_cluster_state(
        &self,
        statuses: impl Iterator<Item = MemberStatus>,
        cluster_epoch: u64,
        lha_score: u8,
        is_leader: bool,
    ) {
        #[cfg(feature = "otel")]
        if let Some(inst) = self.instruments() {
            use opentelemetry::KeyValue;

            let mut active = 0_u64;
            let mut alive = 0_u64;
            let mut suspect = 0_u64;
            for status in statuses {
                match status {
                    MemberStatus::Alive => {
                        active = active.saturating_add(1);
                        alive = alive.saturating_add(1);
                    }
                    MemberStatus::Suspect => {
                        active = active.saturating_add(1);
                        suspect = suspect.saturating_add(1);
                    }
                    MemberStatus::Dead | MemberStatus::Left | _ => {}
                }
            }
            let attrs = [KeyValue::new("topic", inst.topic_label.clone())];
            inst.members_active.record(active, &attrs);
            inst.members_alive.record(alive, &attrs);
            inst.members_suspect.record(suspect, &attrs);
            inst.cluster_epoch.record(cluster_epoch, &attrs);
            inst.lha_score.record(u64::from(lha_score), &attrs);
            inst.is_leader.record(u64::from(is_leader), &attrs);
        }
        #[cfg(not(feature = "otel"))]
        {
            let _ = (statuses, cluster_epoch, lha_score, is_leader);
        }
    }

    pub(crate) fn record_event(&self, event: &MembershipEvent) {
        #[cfg(feature = "otel")]
        if let Some(inst) = self.instruments() {
            use opentelemetry::KeyValue;

            inst.events_total.add(
                1,
                &[
                    KeyValue::new("topic", inst.topic_label.clone()),
                    KeyValue::new("event", event_kind_label(event)),
                ],
            );
        }
        #[cfg(not(feature = "otel"))]
        {
            let _ = event;
        }
    }

    pub(crate) fn record_gossip_broadcast(&self, bytes: usize, succeeded: bool) {
        #[cfg(feature = "otel")]
        if let Some(inst) = self.instruments() {
            use opentelemetry::KeyValue;

            let outcome = if succeeded { "ok" } else { "err" };
            let attrs = [
                KeyValue::new("topic", inst.topic_label.clone()),
                KeyValue::new("outcome", outcome),
            ];
            inst.gossip_broadcast_total.add(1, &attrs);
            if succeeded {
                let len_u64 = u64::try_from(bytes).unwrap_or(u64::MAX);
                inst.gossip_broadcast_bytes.record(len_u64, &attrs);
            }
        }
        #[cfg(not(feature = "otel"))]
        {
            let _ = (bytes, succeeded);
        }
    }

    pub(crate) fn record_gossip_received(&self, kind: &'static str) {
        #[cfg(feature = "otel")]
        if let Some(inst) = self.instruments() {
            use opentelemetry::KeyValue;

            inst.gossip_received_total.add(
                1,
                &[
                    KeyValue::new("topic", inst.topic_label.clone()),
                    KeyValue::new("kind", kind),
                ],
            );
        }
        #[cfg(not(feature = "otel"))]
        {
            let _ = kind;
        }
    }

    pub(crate) fn record_rpc_sent(
        &self,
        method: &'static str,
        outcome: &'static str,
        dur: Duration,
    ) {
        #[cfg(feature = "otel")]
        if let Some(inst) = self.instruments() {
            use opentelemetry::KeyValue;

            let attrs = [
                KeyValue::new("method", method),
                KeyValue::new("outcome", outcome),
            ];
            inst.rpc_sent_total.add(1, &attrs);
            inst.rpc_duration_secs.record(dur.as_secs_f64(), &attrs);
        }
        #[cfg(not(feature = "otel"))]
        {
            let _ = (method, outcome, dur);
        }
    }

    pub(crate) fn record_rpc_received(&self, method: &'static str) {
        #[cfg(feature = "otel")]
        if let Some(inst) = self.instruments() {
            use opentelemetry::KeyValue;

            inst.rpc_received_total
                .add(1, &[KeyValue::new("method", method)]);
        }
        #[cfg(not(feature = "otel"))]
        {
            let _ = method;
        }
    }

    pub(crate) fn record_admission(&self, allowed: bool) {
        #[cfg(feature = "otel")]
        if let Some(inst) = self.instruments() {
            use opentelemetry::KeyValue;

            let outcome = if allowed { "allowed" } else { "denied" };
            inst.admission_checks_total.add(
                1,
                &[
                    KeyValue::new("topic", inst.topic_label.clone()),
                    KeyValue::new("outcome", outcome),
                ],
            );
        }
        #[cfg(not(feature = "otel"))]
        {
            let _ = allowed;
        }
    }
}

pub(crate) const fn control_method_label(msg: &ControlMessage) -> &'static str {
    match msg {
        ControlMessage::Ping { .. } => "ping",
        ControlMessage::Ack { .. } => "ack",
        ControlMessage::Nack { .. } => "nack",
        ControlMessage::PingReq { .. } => "ping_req",
        ControlMessage::PushPullSync(_) => "push_pull_sync",
        _ => "other",
    }
}

#[cfg(feature = "otel")]
const fn event_kind_label(event: &MembershipEvent) -> &'static str {
    match event {
        MembershipEvent::Joined(_) => "joined",
        MembershipEvent::Updated(_) => "updated",
        MembershipEvent::Suspected { .. } => "suspected",
        MembershipEvent::Failed(_) => "failed",
        MembershipEvent::Left(_) => "left",
        MembershipEvent::LeaderChanged { .. } => "leader_changed",
        _ => "other",
    }
}
