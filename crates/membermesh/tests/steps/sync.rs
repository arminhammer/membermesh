use std::{collections::BTreeMap, sync::Arc, time::Duration};

use cucumber::{given, then, when};
use iroh::protocol::DynProtocolHandler;
use membermesh::{
    ControlClient, ControlFuture, ControlHandler, ControlProtocol, IrohSigner, IrohVerifier,
    SYNC_ALPN, peer_id_from_endpoint,
};
use membermesh_core::{
    ClaimSigner as _, ControlMessage, LocalNodeInit, MemberMetadata, MembershipTable, SessionId,
    SimInstant,
};
use membermesh_testkit::{
    cluster::{LocalIrohCluster, PartitionedHandler},
    crypto::TestSigner,
};
use tokio::sync::Mutex;

use super::MeshWorld;

#[derive(Debug)]
struct TestControlHandler {
    name: String,
    table: Arc<Mutex<MembershipTable>>,
    signer: IrohSigner,
    client: ControlClient,
    all_tables: Arc<Mutex<BTreeMap<String, Arc<Mutex<MembershipTable>>>>>,
}

impl ControlHandler for TestControlHandler {
    fn handle(&self, from: membermesh_core::PeerId, msg: ControlMessage) -> ControlFuture {
        let name = self.name.clone();
        let table = Arc::clone(&self.table);
        let signer = self.signer.clone();
        let client = self.client.clone();
        let _all = Arc::clone(&self.all_tables);
        Box::pin(async move {
            match msg {
                ControlMessage::Ping { seq, .. } => Some(ControlMessage::Ack {
                    seq,
                    from: signer.peer_id(),
                }),
                ControlMessage::PingReq { seq, target, .. } => {
                    let probe = ControlMessage::Ping {
                        seq,
                        from: signer.peer_id(),
                    };
                    match client.request(target, &probe).await {
                        Ok(ControlMessage::Ack { .. }) => Some(ControlMessage::Ack {
                            seq,
                            from: signer.peer_id(),
                        }),
                        _ => Some(ControlMessage::Nack {
                            seq,
                            from: signer.peer_id(),
                        }),
                    }
                }
                ControlMessage::PushPullSync(remote_snap) => {
                    let mut guard = table.lock().await;
                    let _ =
                        guard.merge_snapshot(&remote_snap, &IrohVerifier, &signer, SimInstant(10));
                    let reply = guard.snapshot();
                    let _ = (name, from);
                    Some(ControlMessage::PushPullSync(Box::new(reply)))
                }
                ControlMessage::Ack { .. } | ControlMessage::Nack { .. } | _ => None,
            }
        })
    }
}

async fn setup_cluster(world: &mut MeshWorld, names: &[&str]) {
    let mut cluster = LocalIrohCluster::new();
    let shared_tables = Arc::new(Mutex::new(BTreeMap::new()));

    for (idx, &name) in names.iter().enumerate() {
        let (secret_key, endpoint) = cluster.bind_endpoint(name).await.expect("bind");
        let signer = IrohSigner::new(secret_key.clone());
        let init = LocalNodeInit {
            session_id: SessionId((idx as u64) + 1),
            joined_at_unix_ms: 1_700_000_000_000,
            metadata: MemberMetadata::default(),
            addr_hint: None,
        };
        let table = Arc::new(Mutex::new(
            MembershipTable::found(&signer, init, SimInstant(0)).expect("found"),
        ));
        let client = ControlClient::new(endpoint.clone(), Duration::from_secs(3));
        let handler = Arc::new(TestControlHandler {
            name: name.to_owned(),
            table: Arc::clone(&table),
            signer,
            client: client.clone(),
            all_tables: Arc::clone(&shared_tables),
        });
        let protocol = PartitionedHandler::new(
            endpoint.id(),
            cluster.matrix().clone(),
            ControlProtocol::new(handler).with_max_control_bytes(4096),
        );
        let protocols: Vec<(&[u8], Box<dyn DynProtocolHandler>)> =
            vec![(SYNC_ALPN, Box::new(protocol))];
        let mut builder = iroh::protocol::Router::builder(endpoint.clone());
        for (alpn, h) in protocols {
            builder = builder.accept(alpn, h);
        }
        let router = builder.spawn();
        cluster.insert_node(name, secret_key, endpoint, Some(router));
        world.tables.insert(name.to_owned(), table);
        world.clients.insert(name.to_owned(), client);
    }

    world.cluster = Some(cluster);
}

#[given(regex = r#"^an offline local iroh cluster with peers "([^"]+)" and "([^"]+)"$"#)]
async fn cluster_two_peers(world: &mut MeshWorld, a: String, b: String) {
    setup_cluster(world, &[&a, &b]).await;
}

#[given(
    regex = r#"^an offline local iroh cluster with peers "([^"]+)", "([^"]+)", and "([^"]+)"$"#
)]
async fn cluster_three_peers(world: &mut MeshWorld, a: String, b: String, c: String) {
    setup_cluster(world, &[&a, &b, &c]).await;
}

#[when(regex = r#"^"([^"]+)" sends a direct Ping to "([^"]+)" over /membermesh/1$"#)]
async fn send_direct_ping(world: &mut MeshWorld, from_name: String, to_name: String) {
    let cluster = world.cluster.as_ref().expect("cluster");
    let from_id = peer_id_from_endpoint(cluster.id(&from_name).expect("from id"));
    let to_id = peer_id_from_endpoint(cluster.id(&to_name).expect("to id"));
    let client = world.clients.get(&from_name).expect("client");
    let reply = client
        .request(
            to_id,
            &ControlMessage::Ping {
                seq: 42,
                from: from_id,
            },
        )
        .await
        .expect("direct ping must succeed");
    world.last_reply = Some(reply);
}

#[then(regex = r#"^"([^"]+)" receives a matching Ack from "([^"]+)"$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String captures by value"
)]
fn receives_matching_ack(world: &mut MeshWorld, from_name: String, target_name: String) {
    let _ = from_name;
    let expected_from = peer_id_from_endpoint(
        world
            .cluster
            .as_ref()
            .expect("cluster")
            .id(&target_name)
            .expect("target id"),
    );
    assert_eq!(
        world.last_reply,
        Some(ControlMessage::Ack {
            seq: 42,
            from: expected_from
        })
    );
}

#[when(regex = r#"^the local iroh cluster blocks the link between "([^"]+)" and "([^"]+)"$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String captures by value"
)]
fn block_local_link(world: &mut MeshWorld, a: String, b: String) {
    world.cluster.as_ref().expect("cluster").sever(&a, &b);
}

#[when(regex = r#"^"([^"]+)" sends an indirect PingReq for "([^"]+)" via "([^"]+)"$"#)]
async fn send_indirect_ping_req(
    world: &mut MeshWorld,
    from_name: String,
    target_name: String,
    relay_name: String,
) {
    let cluster = world.cluster.as_ref().expect("cluster");
    let from_id = peer_id_from_endpoint(cluster.id(&from_name).expect("from id"));
    let target_id = peer_id_from_endpoint(cluster.id(&target_name).expect("target id"));
    let relay_id = peer_id_from_endpoint(cluster.id(&relay_name).expect("relay id"));

    let client = world.clients.get(&from_name).expect("client");
    assert!(
        client
            .request(
                target_id,
                &ControlMessage::Ping {
                    seq: 1,
                    from: from_id
                }
            )
            .await
            .is_err(),
        "direct ping across severed link should fail"
    );

    let reply = client
        .request(
            relay_id,
            &ControlMessage::PingReq {
                seq: 77,
                from: from_id,
                target: target_id,
            },
        )
        .await
        .expect("indirect PingReq via relay must succeed");
    world.last_reply = Some(reply);
}

#[then(regex = r#"^"([^"]+)" receives a matching Ack relayed through "([^"]+)"$"#)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber step macro extracts String captures by value"
)]
fn receives_relayed_ack(world: &mut MeshWorld, from_name: String, relay_name: String) {
    let _ = from_name;
    let expected_relay = peer_id_from_endpoint(
        world
            .cluster
            .as_ref()
            .expect("cluster")
            .id(&relay_name)
            .expect("relay id"),
    );
    assert_eq!(
        world.last_reply,
        Some(ControlMessage::Ack {
            seq: 77,
            from: expected_relay
        })
    );
}

#[given(
    regex = r#"^"([^"]+)" has peer "([^"]+)" in its table while "([^"]+)" has peer "([^"]+)"$"#
)]
async fn seed_disjoint_peers(
    world: &mut MeshWorld,
    a: String,
    extra_a: String,
    b: String,
    extra_b: String,
) {
    let signer_left = TestSigner::from_name(&extra_a);
    let signer_right = TestSigner::from_name(&extra_b);
    let claim_a = membermesh_core::SignedClaim::sign(
        membermesh_core::MemberClaim {
            subject: signer_left.peer_id(),
            session_id: SessionId(50),
            incarnation: membermesh_core::Incarnation(1),
            join_seq: 10,
            joined_at_unix_ms: 1_700_000_000_000,
            status: membermesh_core::MemberStatus::Alive,
            metadata: MemberMetadata::default(),
            addr_hint: None,
        },
        &signer_left,
    )
    .expect("sign");
    let claim_b = membermesh_core::SignedClaim::sign(
        membermesh_core::MemberClaim {
            subject: signer_right.peer_id(),
            session_id: SessionId(51),
            incarnation: membermesh_core::Incarnation(1),
            join_seq: 11,
            joined_at_unix_ms: 1_700_000_000_000,
            status: membermesh_core::MemberStatus::Alive,
            metadata: MemberMetadata::default(),
            addr_hint: None,
        },
        &signer_right,
    )
    .expect("sign");

    let verifier = membermesh_testkit::crypto::TestVerifier;
    let _ = world
        .tables
        .get(&a)
        .expect("a")
        .lock()
        .await
        .apply_claim(claim_a, &verifier, SimInstant(5))
        .expect("apply a");
    let _ = world
        .tables
        .get(&b)
        .expect("b")
        .lock()
        .await
        .apply_claim(claim_b, &verifier, SimInstant(5))
        .expect("apply b");
}

#[when(regex = r#"^"([^"]+)" executes a PushPullSync with "([^"]+)" over /membermesh/1$"#)]
async fn execute_push_pull(world: &mut MeshWorld, a: String, b: String) {
    let cluster = world.cluster.as_ref().expect("cluster");
    let b_id = peer_id_from_endpoint(cluster.id(&b).expect("b id"));
    let a_snap = world.tables.get(&a).expect("a").lock().await.snapshot();
    let reply = world
        .clients
        .get(&a)
        .expect("client")
        .request(b_id, &ControlMessage::PushPullSync(Box::new(a_snap)))
        .await
        .expect("PushPullSync");
    let ControlMessage::PushPullSync(remote_snap) = reply else {
        panic!("expected PushPullSync reply");
    };
    let signer_a = IrohSigner::new(cluster.node(&a).expect("node a").secret_key.clone());
    let _ = world
        .tables
        .get(&a)
        .expect("a")
        .lock()
        .await
        .merge_snapshot(&remote_snap, &IrohVerifier, &signer_a, SimInstant(15))
        .expect("merge a");
}

#[then(
    regex = r#"^both "([^"]+)" and "([^"]+)" contain "([^"]+)", "([^"]+)", "([^"]+)", and "([^"]+)"$"#
)]
async fn both_contain_four(
    world: &mut MeshWorld,
    a: String,
    b: String,
    p1: String,
    p2: String,
    p3: String,
    p4: String,
) {
    let cluster = world.cluster.as_ref().expect("cluster");
    let target_ids = [
        peer_id_from_endpoint(cluster.id(&p1).expect("p1")),
        peer_id_from_endpoint(cluster.id(&p2).expect("p2")),
        TestSigner::from_name(&p3).peer_id(),
        TestSigner::from_name(&p4).peer_id(),
    ];
    for side in [&a, &b] {
        let guard = world.tables.get(side).expect("side").lock().await;
        for id in target_ids {
            assert!(
                guard.member(id).is_some(),
                "peer {side} missing expected member {id}"
            );
        }
    }
}

#[when(regex = r#"^"([^"]+)" sends an oversized control frame to "([^"]+)"$"#)]
async fn send_oversized_frame(world: &mut MeshWorld, from_name: String, to_name: String) {
    let to_eid = world
        .cluster
        .as_ref()
        .expect("cluster")
        .id(&to_name)
        .expect("to id");
    let client = world.clients.get(&from_name).expect("client");
    let oversized = vec![0xAA; 8192];
    let err = client
        .request_raw(to_eid, &oversized)
        .await
        .expect_err("oversized frame must be rejected");
    world.last_raw_err = Some(err.to_string());
}

#[then(regex = r#"^"([^"]+)" rejects the frame cleanly without wedging valid subsequent Pings$"#)]
async fn rejects_cleanly_and_pings_still_work(world: &mut MeshWorld, to_name: String) {
    assert!(world.last_raw_err.is_some());
    send_direct_ping(world, "alice".to_owned(), to_name.clone()).await;
    receives_matching_ack(world, "alice".to_owned(), to_name);
}
