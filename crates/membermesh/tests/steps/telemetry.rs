use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{Duration, Instant},
};

use cucumber::{given, then, when};
use iroh_gossip::{Gossip, TopicId};
use membermesh::{
    AdmissionPolicy, MemberMesh, OpenAdmission, SYNC_ALPN, SharedSecretAdmission,
    peer_id_from_endpoint, telemetry,
};
use membermesh_core::{MemberMetadata, ProtocolConfig};
use membermesh_testkit::cluster::{LocalIrohCluster, PartitionedHandler};
use opentelemetry::{metrics::MeterProvider as _, trace::TracerProvider as _};
use opentelemetry_sdk::{
    metrics::{
        InMemoryMetricExporter, SdkMeterProvider,
        data::{AggregatedMetrics, ResourceMetrics},
    },
    trace::{InMemorySpanExporter, SdkTracerProvider},
};
use tracing_subscriber::layer::SubscriberExt as _;

use super::MeshWorld;

const CONVERGE_TIMEOUT: Duration = Duration::from_secs(12);

fn otel_topic() -> TopicId {
    TopicId::from_bytes([0xC3; 32])
}

async fn spawn_telemetry_node(
    cluster: &mut LocalIrohCluster,
    name: &str,
    bootstrap: Vec<iroh::EndpointAddr>,
    admission: Arc<dyn AdmissionPolicy>,
    meter: Option<opentelemetry::metrics::Meter>,
) -> MemberMesh {
    let (secret_key, endpoint) = cluster.bind_endpoint(name).await.expect("bind");
    let gossip = Gossip::builder().spawn(endpoint.clone());
    let (prepared, control) = MemberMesh::prepare_attach(
        endpoint.clone(),
        gossip.clone(),
        otel_topic(),
        ProtocolConfig::lan(),
        MemberMetadata::default(),
    );
    let mut prepared = prepared.with_admission(admission);
    if let Some(m) = meter {
        prepared = prepared.with_meter(m);
    }
    let matrix = cluster.matrix().clone();
    let local_id = endpoint.id();
    let router = iroh::protocol::Router::builder(endpoint.clone())
        .accept(
            iroh_gossip::ALPN,
            PartitionedHandler::new(local_id, matrix.clone(), gossip),
        )
        .accept(
            SYNC_ALPN,
            PartitionedHandler::new(local_id, matrix, control),
        )
        .spawn();
    cluster.insert_node(name, secret_key, endpoint, Some(router));
    prepared
        .start(bootstrap)
        .await
        .expect("start telemetry node")
}

async fn wait_roster(world: &MeshWorld, expected: &[&str]) {
    let cluster = world.cluster.as_ref().expect("cluster");
    let expected_ids: Vec<_> = expected
        .iter()
        .map(|n| peer_id_from_endpoint(cluster.id(n).expect("id")))
        .collect();
    let deadline = Instant::now() + CONVERGE_TIMEOUT;
    loop {
        let mut ok = true;
        for name in expected {
            let mesh = world.meshes.get(*name).expect("mesh");
            let views = mesh.members().await.expect("members");
            let got: Vec<_> = views.iter().map(|v| v.peer_id).collect();
            if got != expected_ids {
                ok = false;
            }
        }
        if ok {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for telemetry cluster convergence"
        );
        tokio::time::sleep(Duration::from_millis(60)).await;
    }
}

fn collect_metric_names(batches: &[ResourceMetrics]) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for resource in batches {
        for scope in resource.scope_metrics() {
            for metric in scope.metrics() {
                names.insert(metric.name().to_owned());
            }
        }
    }
    names
}

fn admission_outcome_counts(batches: &[ResourceMetrics]) -> BTreeMap<String, u64> {
    let mut counts = BTreeMap::new();
    for resource in batches {
        for scope in resource.scope_metrics() {
            for metric in scope.metrics() {
                if metric.name() != telemetry::METRIC_ADMISSION_CHECKS_TOTAL {
                    continue;
                }
                if let AggregatedMetrics::U64(opentelemetry_sdk::metrics::data::MetricData::Sum(
                    sum,
                )) = metric.data()
                {
                    for dp in sum.data_points() {
                        for kv in dp.attributes() {
                            if kv.key.as_str() == "outcome" {
                                let total =
                                    counts.entry(kv.value.as_str().into_owned()).or_insert(0);
                                *total = (*total).max(dp.value());
                            }
                        }
                    }
                }
            }
        }
    }
    counts
}

#[given(
    regex = r#"^an offline (\d+)-node cluster "([^"]+)" wired with an in-memory OpenTelemetry meter on "([^"]+)"$"#
)]
async fn otel_cluster_with_meter(
    world: &mut MeshWorld,
    count: usize,
    csv: String,
    observed_node: String,
) {
    let names: Vec<String> = csv.split(',').map(str::to_owned).collect();
    assert_eq!(names.len(), count);
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter.clone())
        .build();
    let meter = provider.meter(telemetry::METER_NAME);

    let mut cluster = LocalIrohCluster::new();
    let mut prev = None;
    for name in &names {
        let node_meter = (*name == observed_node).then(|| meter.clone());
        let mesh = spawn_telemetry_node(
            &mut cluster,
            name,
            prev.into_iter().collect(),
            Arc::new(OpenAdmission),
            node_meter,
        )
        .await;
        prev = Some(mesh.addr());
        world.meshes.insert(name.clone(), mesh);
    }
    world.cluster = Some(cluster);
    world.node_order = names;
    world.otel_metric_provider = Some(provider);
    world.otel_metric_exporter = Some(exporter);
}

#[when(
    regex = r#"^all (\d+) telemetry nodes converge, "([^"]+)" updates metadata to "([^"]+)", and "([^"]+)" gracefully leaves$"#
)]
async fn telemetry_converge_update_leave(
    world: &mut MeshWorld,
    count: usize,
    updater: String,
    meta_value: String,
    leaver: String,
) {
    assert_eq!(world.node_order.len(), count);
    let refs: Vec<&str> = world.node_order.iter().map(String::as_str).collect();
    wait_roster(world, &refs).await;

    let meta = MemberMetadata::try_new(meta_value.into_bytes()).expect("metadata");
    world
        .meshes
        .get(&updater)
        .expect("updater")
        .set_metadata(meta)
        .await
        .expect("set_metadata");
    tokio::time::sleep(Duration::from_millis(180)).await;

    if let Some(mesh) = world.meshes.remove(&leaver) {
        mesh.leave().await.expect("leave");
        mesh.shutdown().await;
    }
    let remaining: Vec<&str> = world
        .node_order
        .iter()
        .filter(|n| *n != &leaver)
        .map(String::as_str)
        .collect();
    wait_roster(world, &remaining).await;
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step extracts owned String from regex capture"
)]
#[then(
    regex = r#"^"([^"]+)"'s OpenTelemetry metric reader contains membership gauges, lifecycle event counters, gossip counters, and RPC latency histograms$"#
)]
fn assert_otel_metrics_present(world: &mut MeshWorld, node: String) {
    assert!(world.meshes.contains_key(&node), "unknown node `{node}`");
    let provider = world.otel_metric_provider.as_ref().expect("provider");
    let exporter = world.otel_metric_exporter.as_ref().expect("exporter");
    provider.force_flush().expect("force_flush metrics");
    let finished = exporter.get_finished_metrics().expect("finished metrics");
    let names = collect_metric_names(&finished);

    for expected in [
        telemetry::METRIC_MEMBERS_ACTIVE,
        telemetry::METRIC_MEMBERS_ALIVE,
        telemetry::METRIC_MEMBERS_SUSPECT,
        telemetry::METRIC_CLUSTER_EPOCH,
        telemetry::METRIC_LHA_SCORE,
        telemetry::METRIC_IS_LEADER,
        telemetry::METRIC_EVENTS_TOTAL,
        telemetry::METRIC_GOSSIP_BROADCAST_TOTAL,
        telemetry::METRIC_GOSSIP_BROADCAST_BYTES,
        telemetry::METRIC_GOSSIP_RECEIVED_TOTAL,
        telemetry::METRIC_RPC_SENT_TOTAL,
        telemetry::METRIC_RPC_RECEIVED_TOTAL,
        telemetry::METRIC_RPC_DURATION_SECS,
    ] {
        assert!(
            names.contains(expected),
            "missing OpenTelemetry metric `{expected}` (got {names:?})"
        );
    }
}

#[given(
    regex = r#"^an offline secret-protected telemetry cluster "([^"]+)" with an in-memory OpenTelemetry meter on "([^"]+)"$"#
)]
async fn secret_telemetry_cluster(world: &mut MeshWorld, csv: String, observed_node: String) {
    let names: Vec<String> = csv.split(',').map(str::to_owned).collect();
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter.clone())
        .build();
    let meter = provider.meter(telemetry::METER_NAME);
    let policy: Arc<dyn AdmissionPolicy> =
        Arc::new(SharedSecretAdmission::from_passphrase("otel-test-secret"));

    let mut cluster = LocalIrohCluster::new();
    let mut prev = None;
    for name in &names {
        let node_meter = (*name == observed_node).then(|| meter.clone());
        let mesh = spawn_telemetry_node(
            &mut cluster,
            name,
            prev.into_iter().collect(),
            Arc::clone(&policy),
            node_meter,
        )
        .await;
        prev = Some(mesh.addr());
        world.meshes.insert(name.clone(), mesh);
    }
    world.cluster = Some(cluster);
    world.node_order.clone_from(&names);
    world.otel_metric_provider = Some(provider);
    world.otel_metric_exporter = Some(exporter);

    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    wait_roster(world, &refs).await;
}

#[when(regex = r#"^unauthorized "([^"]+)" attempts to join via "([^"]+)" with a wrong secret$"#)]
async fn unauthorized_join_with_wrong_secret(
    world: &mut MeshWorld,
    intruder: String,
    seed: String,
) {
    let seed_addr = world.meshes.get(&seed).expect("seed").addr();
    let wrong_policy: Arc<dyn AdmissionPolicy> =
        Arc::new(SharedSecretAdmission::from_passphrase("wrong-otel-secret"));
    let mut cluster = world.cluster.take().expect("cluster");
    let mesh =
        spawn_telemetry_node(&mut cluster, &intruder, vec![seed_addr], wrong_policy, None).await;
    world.cluster = Some(cluster);
    world.meshes.insert(intruder, mesh);
    tokio::time::sleep(Duration::from_millis(500)).await;
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step extracts owned String from regex capture"
)]
#[then(
    regex = r#"^"([^"]+)"'s OpenTelemetry metric reader records both allowed and denied admission checks$"#
)]
fn assert_admission_metrics(world: &mut MeshWorld, node: String) {
    assert!(world.meshes.contains_key(&node), "unknown node `{node}`");
    let provider = world.otel_metric_provider.as_ref().expect("provider");
    let exporter = world.otel_metric_exporter.as_ref().expect("exporter");
    provider.force_flush().expect("force_flush metrics");
    let finished = exporter.get_finished_metrics().expect("finished metrics");
    let counts = admission_outcome_counts(&finished);
    assert!(
        counts.get("allowed").copied().unwrap_or(0) >= 1,
        "expected >= 1 allowed admission metric, got {counts:?}"
    );
    assert!(
        counts.get("denied").copied().unwrap_or(0) >= 1,
        "expected >= 1 denied admission metric, got {counts:?}"
    );
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step extracts owned String from regex capture"
)]
#[given(
    regex = r#"^an isolated 2-node cluster "([^"]+)" instrumented with an in-memory OpenTelemetry tracer$"#
)]
fn isolated_tracing_cluster(world: &mut MeshWorld, csv: String) {
    let names: Vec<String> = csv.split(',').map(str::to_owned).collect();
    assert_eq!(names.len(), 2);
    world.node_order = names;
}

#[when(regex = r#"^"([^"]+)" and "([^"]+)" converge and "([^"]+)" gracefully leaves$"#)]
async fn run_isolated_traced_cluster(world: &mut MeshWorld, a: String, b: String, leaver: String) {
    let captured = tokio::task::spawn_blocking(move || {
        let span_exporter = InMemorySpanExporter::default();
        let tracer_provider = SdkTracerProvider::builder()
            .with_simple_exporter(span_exporter.clone())
            .build();
        let tracer = tracer_provider.tracer(telemetry::METER_NAME);
        let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
        let subscriber = tracing_subscriber::Registry::default().with(otel_layer);
        let _guard = tracing::subscriber::set_default(subscriber);

        {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("current_thread runtime");

            rt.block_on(async {
                let mut cluster = LocalIrohCluster::new();
                let mesh_a = spawn_telemetry_node(
                    &mut cluster,
                    &a,
                    Vec::new(),
                    Arc::new(OpenAdmission),
                    None,
                )
                .await;
                let mesh_b = spawn_telemetry_node(
                    &mut cluster,
                    &b,
                    vec![mesh_a.addr()],
                    Arc::new(OpenAdmission),
                    None,
                )
                .await;

                let mut meshes = BTreeMap::new();
                meshes.insert(a.clone(), mesh_a);
                meshes.insert(b.clone(), mesh_b);

                let deadline = Instant::now() + CONVERGE_TIMEOUT;
                loop {
                    let len_a = meshes[&a].members().await.expect("members a").len();
                    let len_b = meshes[&b].members().await.expect("members b").len();
                    if len_a == 2 && len_b == 2 {
                        break;
                    }
                    assert!(
                        Instant::now() < deadline,
                        "timed out waiting for traced convergence"
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }

                if let Some(m) = meshes.remove(&leaver) {
                    m.leave().await.expect("leave");
                    m.shutdown().await;
                }
                for (_, m) in meshes {
                    m.shutdown().await;
                }
                cluster.shutdown_all().await;
            });
        }

        tracer_provider.force_flush().expect("flush spans");
        let spans = span_exporter.get_finished_spans().expect("finished spans");
        let span_names: BTreeSet<String> = spans.into_iter().map(|s| s.name.into_owned()).collect();
        drop(tracer_provider);
        span_names
    })
    .await
    .expect("spawn_blocking traced scenario");

    world.captured_span_names = captured;
}

#[then("the OpenTelemetry span exporter captures membermesh RPC and actor lifecycle spans")]
fn assert_otel_spans_captured(world: &mut MeshWorld) {
    for expected in [
        "membermesh.actor.start",
        "membermesh.actor.leave",
        "membermesh.rpc.request",
        "membermesh.rpc.accept",
    ] {
        assert!(
            world.captured_span_names.contains(expected),
            "expected span `{expected}` in {:?}",
            world.captured_span_names
        );
    }
}
