use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use cucumber::{given, then, when};
use iroh::endpoint::Connection;
use iroh::protocol::{AcceptError, ProtocolHandler};
use iroh_gossip::{Gossip, TopicId};
use membermesh::{
    AdmissionPolicy, AllowListAdmission, EventStream, MemberMesh, OpenAdmission, SYNC_ALPN,
    SharedSecretAdmission, peer_id_from_endpoint,
};
use membermesh_core::{MemberMetadata, MembershipEvent, ProtocolConfig};
use membermesh_testkit::cluster::{LocalIrohCluster, PartitionedHandler};
use tokio::sync::Mutex;

use super::MeshWorld;

const ECHO_ALPN: &[u8] = b"/app/echo/1";
const TIMEOUT: Duration = Duration::from_secs(12);

#[derive(Debug, Clone)]
struct EchoProtocol;

impl ProtocolHandler for EchoProtocol {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let (mut send, mut recv) = connection.accept_bi().await?;
        let mut buf = [0u8; 4];
        recv.read_exact(&mut buf)
            .await
            .map_err(AcceptError::from_err)?;
        send.write_all(&buf).await.map_err(AcceptError::from_err)?;
        send.finish().map_err(AcceptError::from_err)?;
        let _ = connection.closed().await;
        Ok(())
    }
}

fn topic() -> TopicId {
    TopicId::from_bytes([0x77; 32])
}

async fn spawn_node_with_policy(
    cluster: &mut LocalIrohCluster,
    name: &str,
    bootstrap: Vec<iroh::EndpointAddr>,
    admission: Arc<dyn AdmissionPolicy>,
    mount_echo: bool,
) -> MemberMesh {
    let (secret_key, endpoint) = cluster.bind_endpoint(name).await.expect("bind");
    let gossip = Gossip::builder().spawn(endpoint.clone());
    let (prepared, control) = MemberMesh::prepare_attach(
        endpoint.clone(),
        gossip.clone(),
        topic(),
        ProtocolConfig::lan(),
        MemberMetadata::default(),
    );
    let prepared = prepared.with_admission(admission);
    let matrix = cluster.matrix().clone();
    let local_id = endpoint.id();
    let mut builder = iroh::protocol::Router::builder(endpoint.clone())
        .accept(
            iroh_gossip::ALPN,
            PartitionedHandler::new(local_id, matrix.clone(), gossip),
        )
        .accept(
            SYNC_ALPN,
            PartitionedHandler::new(local_id, matrix, control),
        );
    if mount_echo {
        builder = builder.accept(ECHO_ALPN, EchoProtocol);
    }
    let router = builder.spawn();
    cluster.insert_node(name, secret_key, endpoint, Some(router));
    prepared.start(bootstrap).await.expect("start mesh")
}

async fn wait_roster(world: &MeshWorld, expected: &[&str], leader_name: &str) {
    let cluster = world.cluster.as_ref().expect("cluster");
    let expected_ids: Vec<_> = expected
        .iter()
        .map(|n| peer_id_from_endpoint(cluster.id(n).expect("id")))
        .collect();
    let leader_id = peer_id_from_endpoint(cluster.id(leader_name).expect("leader"));
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let mut ok = true;
        for name in expected {
            let mesh = world.meshes.get(*name).expect("mesh");
            let views = mesh.members().await.expect("members");
            let got: Vec<_> = views.iter().map(|v| v.peer_id).collect();
            let got_leader = mesh.leader().await.expect("leader").map(|m| m.peer_id);
            if got != expected_ids || got_leader != Some(leader_id) {
                ok = false;
            }
        }
        if ok {
            return;
        }
        assert!(Instant::now() < deadline, "timed out waiting for roster");
        tokio::time::sleep(Duration::from_millis(60)).await;
    }
}

#[given(regex = r#"^an offline composable cluster "([^"]+)" sharing a custom "([^"]+)" protocol$"#)]
async fn composable_cluster_with_echo(world: &mut MeshWorld, csv: String, alpn: String) {
    assert_eq!(alpn.as_bytes(), ECHO_ALPN);
    let names: Vec<String> = csv.split(',').map(str::to_owned).collect();
    let mut cluster = LocalIrohCluster::new();
    let mut prev = None;
    for name in &names {
        let mesh = spawn_node_with_policy(
            &mut cluster,
            name,
            prev.into_iter().collect(),
            Arc::new(OpenAdmission),
            true,
        )
        .await;
        prev = Some(mesh.addr());
        world.meshes.insert(name.clone(), mesh);
    }
    world.cluster = Some(cluster);
    world.node_order = names;
}

#[then(regex = r#"^both composable nodes converge on the ordered roster "([^"]+)"$"#)]
async fn both_composable_converge(world: &mut MeshWorld, csv: String) {
    let names: Vec<&str> = csv.split(',').collect();
    wait_roster(world, &names, names[0]).await;
}

#[then(regex = r#"^"([^"]+)" can still invoke the custom "([^"]+)" protocol on "([^"]+)"$"#)]
async fn invoke_custom_protocol(world: &mut MeshWorld, from: String, alpn: String, to: String) {
    assert_eq!(alpn.as_bytes(), ECHO_ALPN);
    let cluster = world.cluster.as_ref().expect("cluster");
    let ep = &cluster.node(&from).expect("from").endpoint;
    let target_addr = cluster.addr(&to).expect("to addr");
    let conn = ep
        .connect(target_addr, ECHO_ALPN)
        .await
        .expect("connect echo");
    let (mut send, mut recv) = conn.open_bi().await.expect("open bi");
    send.write_all(b"ping").await.expect("write ping");
    send.finish().expect("finish");
    let mut resp = [0u8; 4];
    recv.read_exact(&mut resp).await.expect("read pong");
    assert_eq!(&resp, b"ping");
}

#[given(
    regex = r#"^an offline composable 2-node cluster "([^"]+)" with EventStream subscription on "([^"]+)"$"#
)]
async fn composable_with_event_stream(world: &mut MeshWorld, csv: String, observer: String) {
    let names: Vec<String> = csv.split(',').map(str::to_owned).collect();
    assert_eq!(names.len(), 2);
    let mut cluster = LocalIrohCluster::new();
    let mut prev = None;
    for name in &names {
        let mesh = spawn_node_with_policy(
            &mut cluster,
            name,
            prev.into_iter().collect(),
            Arc::new(OpenAdmission),
            false,
        )
        .await;
        prev = Some(mesh.addr());
        world.meshes.insert(name.clone(), mesh);
    }
    world.cluster = Some(cluster);
    world.node_order.clone_from(&names);
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    wait_roster(world, &refs, &names[0]).await;

    let stream = world.meshes.get(&observer).expect("observer").subscribe();
    let collected: Arc<Mutex<Vec<MembershipEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&collected);
    let task = tokio::spawn(async move {
        let mut s: EventStream = stream;
        while let Some(ev) = s.recv().await {
            sink.lock().await.push(ev);
        }
    });
    world.observed_events = Some(collected);
    world.event_tasks.push(task);
    tokio::time::sleep(Duration::from_millis(50)).await;
}

#[when(regex = r#"^"([^"]+)" updates its metadata to "([^"]+)"$"#)]
async fn node_updates_metadata(world: &mut MeshWorld, name: String, value: String) {
    let meta = MemberMetadata::try_new(value.into_bytes()).expect("metadata");
    world
        .meshes
        .get(&name)
        .expect("mesh")
        .set_metadata(meta)
        .await
        .expect("set_metadata");
    tokio::time::sleep(Duration::from_millis(120)).await;
}

#[when(regex = r#"^"([^"]+)" gracefully leaves the composable cluster$"#)]
async fn node_leaves_composable(world: &mut MeshWorld, name: String) {
    if let Some(mesh) = world.meshes.remove(&name) {
        mesh.leave().await.expect("leave");
        mesh.shutdown().await;
    }
}

#[then(
    regex = r#"^"([^"]+)"'s EventStream observes Updated, LeaderChanged, and Left for "([^"]+)"$"#
)]
async fn event_stream_observes_transitions(
    world: &mut MeshWorld,
    observer: String,
    subject: String,
) {
    let _ = observer;
    let cluster = world.cluster.as_ref().expect("cluster");
    let subject_id = peer_id_from_endpoint(cluster.id(&subject).expect("subject id"));
    let sink = world.observed_events.as_ref().expect("observed events");
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let guard = sink.lock().await;
        let saw_updated = guard.iter().any(|ev| {
            matches!(ev, MembershipEvent::Updated(v) if v.peer_id == subject_id && v.metadata.as_bytes() == b"role=primary")
        });
        let saw_leader_change = guard.iter().any(|ev| {
            matches!(
                ev,
                MembershipEvent::LeaderChanged {
                    previous: Some(prev),
                    ..
                } if *prev == subject_id
            )
        });
        let saw_left = guard
            .iter()
            .any(|ev| matches!(ev, MembershipEvent::Left(v) if v.peer_id == subject_id));
        if saw_updated && saw_leader_change && saw_left {
            return;
        }
        drop(guard);
        assert!(
            Instant::now() < deadline,
            "timed out waiting for EventStream Updated + LeaderChanged + Left"
        );
        tokio::time::sleep(Duration::from_millis(60)).await;
    }
}

#[given(
    regex = r#"^an offline allowlist-protected cluster allowing "([^"]+)" but excluding "([^"]+)"$"#
)]
async fn allowlist_cluster(world: &mut MeshWorld, allowed_csv: String, intruder: String) {
    let allowed_names: Vec<String> = allowed_csv.split(',').map(str::to_owned).collect();
    let mut cluster = LocalIrohCluster::new();

    // Bind endpoints first so we know every EndpointId for the allow-list.
    let mut prebound = Vec::with_capacity(allowed_names.len());
    for name in &allowed_names {
        let (secret_key, endpoint) = cluster.bind_endpoint(name).await.expect("bind");
        prebound.push((name.clone(), secret_key, endpoint));
    }
    let policy: Arc<dyn AdmissionPolicy> = Arc::new(AllowListAdmission::new(
        prebound.iter().map(|(_, _, ep)| ep.id()),
    ));

    let mut prev = None;
    for (name, secret_key, endpoint) in prebound {
        let gossip = Gossip::builder().spawn(endpoint.clone());
        let (prepared, control) = MemberMesh::prepare_attach(
            endpoint.clone(),
            gossip.clone(),
            topic(),
            ProtocolConfig::lan(),
            MemberMetadata::default(),
        );
        let prepared = prepared.with_admission(Arc::clone(&policy));
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
        cluster.insert_node(&name, secret_key, endpoint, Some(router));
        let mesh = prepared
            .start(prev.into_iter().collect())
            .await
            .expect("start allowlisted node");
        prev = Some(mesh.addr());
        world.meshes.insert(name, mesh);
    }

    let _ = intruder;
    world.cluster = Some(cluster);
    world.node_order.clone_from(&allowed_names);
    let refs: Vec<&str> = allowed_names.iter().map(String::as_str).collect();
    wait_roster(world, &refs, &allowed_names[0]).await;
}

#[when(regex = r#"^"([^"]+)" attempts to join via "([^"]+)"$"#)]
async fn intruder_attempts_join_open(world: &mut MeshWorld, intruder: String, seed: String) {
    let seed_addr = world.meshes.get(&seed).expect("seed").addr();
    let mut cluster = world.cluster.take().expect("cluster");
    let mesh = spawn_node_with_policy(
        &mut cluster,
        &intruder,
        vec![seed_addr],
        Arc::new(OpenAdmission),
        false,
    )
    .await;
    world.cluster = Some(cluster);
    world.meshes.insert(intruder, mesh);
    tokio::time::sleep(Duration::from_millis(600)).await;
}

#[given(regex = r#"^an offline secret-protected cluster "([^"]+)" using passphrase "([^"]+)"$"#)]
async fn secret_protected_cluster(world: &mut MeshWorld, csv: String, passphrase: String) {
    let names: Vec<String> = csv.split(',').map(str::to_owned).collect();
    let policy: Arc<dyn AdmissionPolicy> =
        Arc::new(SharedSecretAdmission::from_passphrase(&passphrase));
    let mut cluster = LocalIrohCluster::new();
    let mut prev = None;
    for name in &names {
        let mesh = spawn_node_with_policy(
            &mut cluster,
            name,
            prev.into_iter().collect(),
            Arc::clone(&policy),
            false,
        )
        .await;
        prev = Some(mesh.addr());
        world.meshes.insert(name.clone(), mesh);
    }
    world.cluster = Some(cluster);
    world.node_order.clone_from(&names);
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    wait_roster(world, &refs, &names[0]).await;
}

#[when(regex = r#"^"([^"]+)" attempts to join via "([^"]+)" with passphrase "([^"]+)"$"#)]
async fn intruder_attempts_join_wrong_secret(
    world: &mut MeshWorld,
    intruder: String,
    seed: String,
    passphrase: String,
) {
    let seed_addr = world.meshes.get(&seed).expect("seed").addr();
    let policy: Arc<dyn AdmissionPolicy> =
        Arc::new(SharedSecretAdmission::from_passphrase(&passphrase));
    let mut cluster = world.cluster.take().expect("cluster");
    let mesh =
        spawn_node_with_policy(&mut cluster, &intruder, vec![seed_addr], policy, false).await;
    world.cluster = Some(cluster);
    world.meshes.insert(intruder, mesh);
    tokio::time::sleep(Duration::from_millis(600)).await;
}

#[then(regex = r#"^"([^"]+)" and "([^"]+)" include "([^"]+)" and exclude "([^"]+)"$"#)]
async fn honest_nodes_exclude_intruder(
    world: &mut MeshWorld,
    a: String,
    b: String,
    included_csv: String,
    excluded: String,
) {
    let cluster = world.cluster.as_ref().expect("cluster");
    let included: Vec<_> = included_csv
        .split(',')
        .map(|n| peer_id_from_endpoint(cluster.id(n).expect("id")))
        .collect();
    let excluded_id = peer_id_from_endpoint(cluster.id(&excluded).expect("excluded id"));
    for name in [&a, &b] {
        let roster: Vec<_> = world
            .meshes
            .get(name)
            .expect("mesh")
            .members()
            .await
            .expect("members")
            .into_iter()
            .map(|m| m.peer_id)
            .collect();
        assert_eq!(roster, included, "unexpected roster on {name}");
        assert!(
            !roster.contains(&excluded_id),
            "{excluded} must not be admitted to {name}"
        );
    }
}
