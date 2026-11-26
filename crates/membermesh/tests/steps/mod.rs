mod api;
mod cluster;
mod soak;
mod sync;
mod telemetry;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use cucumber::World;
use membermesh::{ControlClient, MemberMesh};
use membermesh_core::{ControlMessage, MembershipEvent, MembershipTable};
use membermesh_testkit::{cluster::LocalIrohCluster, sim::SimCluster, trace::TraceRecorder};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};
use tokio::sync::Mutex;

#[derive(Default, World)]
pub struct MeshWorld {
    pub cluster: Option<LocalIrohCluster>,
    pub tables: BTreeMap<String, Arc<Mutex<MembershipTable>>>,
    pub clients: BTreeMap<String, ControlClient>,
    pub last_reply: Option<ControlMessage>,
    pub last_raw_err: Option<String>,
    pub meshes: BTreeMap<String, MemberMesh>,
    pub node_order: Vec<String>,
    pub crashed: Vec<String>,
    pub trace: TraceRecorder,
    pub observed_events: Option<Arc<Mutex<Vec<MembershipEvent>>>>,
    pub event_tasks: Vec<tokio::task::JoinHandle<()>>,
    pub sim: Option<SimCluster>,
    pub sim_survivors: Vec<String>,
    pub otel_metric_provider: Option<SdkMeterProvider>,
    pub otel_metric_exporter: Option<InMemoryMetricExporter>,
    pub captured_span_names: BTreeSet<String>,
}

impl std::fmt::Debug for MeshWorld {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MeshWorld")
            .field("tables", &self.tables.keys().collect::<Vec<_>>())
            .field("meshes", &self.meshes.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}
