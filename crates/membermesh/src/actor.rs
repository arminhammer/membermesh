use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use iroh::{Endpoint, EndpointAddr, address_lookup::memory::MemoryLookup};
use iroh_gossip::{Gossip, TopicId};
use membermesh_core::{
    ControlMessage, Engine, EngineAction, EngineInput, LocalNodeInit, MemberMetadata, MemberView,
    MembershipEvent, PeerAge, PeerId, ProtocolConfig, SessionId, SimInstant, SyncSnapshot,
};
use tokio::sync::{Semaphore, mpsc, oneshot};
use tracing::debug;

use crate::{
    IrohSigner, IrohVerifier, MemberMeshError,
    gossip::{GossipBridge, GossipBridgeEvent, encode_addr_hint, register_addr_hint},
    signer::{endpoint_id_from_peer, peer_id_from_endpoint},
    sync::{ControlClient, ControlFuture, ControlHandler, ControlProtocol},
};

const MAX_INFLIGHT_RPCS: usize = 16;

#[derive(Debug)]
pub(crate) enum ActorCommand {
    Members(oneshot::Sender<Vec<MemberView>>),
    Leader(oneshot::Sender<Option<MemberView>>),
    LeaderWithQuorum {
        min_alive: usize,
        reply: oneshot::Sender<Option<MemberView>>,
    },
    PeerAge {
        peer_id: PeerId,
        reply: oneshot::Sender<Option<PeerAge>>,
    },
    UpdateMetadata {
        metadata: MemberMetadata,
        reply: oneshot::Sender<Result<(), MemberMeshError>>,
    },
    JoinAdditional {
        peers: Vec<EndpointAddr>,
        reply: oneshot::Sender<Result<(), MemberMeshError>>,
    },
    Leave(oneshot::Sender<Result<(), MemberMeshError>>),
}

#[derive(Debug)]
pub(crate) enum InternalMsg {
    Gossip(GossipBridgeEvent),
    ControlInbound {
        from: PeerId,
        msg: ControlMessage,
        reply_tx: Option<oneshot::Sender<Option<ControlMessage>>>,
    },
    ControlAsyncInput {
        from: PeerId,
        msg: ControlMessage,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct ActorControlHandler {
    local_id: PeerId,
    tx: mpsc::UnboundedSender<InternalMsg>,
    client: ControlClient,
}

impl ControlHandler for ActorControlHandler {
    fn handle(&self, from: PeerId, msg: ControlMessage) -> ControlFuture {
        let local_id = self.local_id;
        let tx = self.tx.clone();
        let client = self.client.clone();

        Box::pin(async move {
            match msg {
                ControlMessage::Ping { seq, .. } => Some(ControlMessage::Ack {
                    seq,
                    from: local_id,
                }),
                ControlMessage::PingReq { seq, target, .. } => {
                    let probe = ControlMessage::Ping {
                        seq,
                        from: local_id,
                    };
                    match client.request(target, &probe).await {
                        Ok(ControlMessage::Ack { .. }) => Some(ControlMessage::Ack {
                            seq,
                            from: local_id,
                        }),
                        _ => Some(ControlMessage::Nack {
                            seq,
                            from: local_id,
                        }),
                    }
                }
                ControlMessage::PushPullSync(snap) => {
                    let (reply_tx, reply_rx) = oneshot::channel();
                    if tx
                        .send(InternalMsg::ControlInbound {
                            from,
                            msg: ControlMessage::PushPullSync(snap),
                            reply_tx: Some(reply_tx),
                        })
                        .is_err()
                    {
                        return None;
                    }
                    reply_rx.await.ok().flatten()
                }
                ControlMessage::Ack { .. } | ControlMessage::Nack { .. } | _ => None,
            }
        })
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MeshActorHandle {
    cmd_tx: mpsc::Sender<ActorCommand>,
    events_tx: tokio::sync::broadcast::Sender<MembershipEvent>,
}

impl MeshActorHandle {
    pub(crate) fn subscribe(&self) -> tokio::sync::broadcast::Receiver<MembershipEvent> {
        self.events_tx.subscribe()
    }

    pub(crate) async fn members(&self) -> Result<Vec<MemberView>, MemberMeshError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(ActorCommand::Members(tx))
            .await
            .map_err(|_| MemberMeshError::Closed)?;
        rx.await.map_err(|_| MemberMeshError::Closed)
    }

    pub(crate) async fn leader(&self) -> Result<Option<MemberView>, MemberMeshError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(ActorCommand::Leader(tx))
            .await
            .map_err(|_| MemberMeshError::Closed)?;
        rx.await.map_err(|_| MemberMeshError::Closed)
    }

    pub(crate) async fn leader_with_quorum(
        &self,
        min_alive: usize,
    ) -> Result<Option<MemberView>, MemberMeshError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(ActorCommand::LeaderWithQuorum {
                min_alive,
                reply: tx,
            })
            .await
            .map_err(|_| MemberMeshError::Closed)?;
        rx.await.map_err(|_| MemberMeshError::Closed)
    }

    pub(crate) async fn peer_age(
        &self,
        peer_id: PeerId,
    ) -> Result<Option<PeerAge>, MemberMeshError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(ActorCommand::PeerAge { peer_id, reply: tx })
            .await
            .map_err(|_| MemberMeshError::Closed)?;
        rx.await.map_err(|_| MemberMeshError::Closed)
    }

    pub(crate) async fn update_metadata(
        &self,
        metadata: MemberMetadata,
    ) -> Result<(), MemberMeshError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(ActorCommand::UpdateMetadata {
                metadata,
                reply: tx,
            })
            .await
            .map_err(|_| MemberMeshError::Closed)?;
        rx.await.map_err(|_| MemberMeshError::Closed)?
    }

    pub(crate) async fn join_peers(&self, peers: Vec<EndpointAddr>) -> Result<(), MemberMeshError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(ActorCommand::JoinAdditional { peers, reply: tx })
            .await
            .map_err(|_| MemberMeshError::Closed)?;
        rx.await.map_err(|_| MemberMeshError::Closed)?
    }

    pub(crate) async fn leave(&self) -> Result<(), MemberMeshError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(ActorCommand::Leave(tx))
            .await
            .map_err(|_| MemberMeshError::Closed)?;
        rx.await.map_err(|_| MemberMeshError::Closed)?
    }
}

pub(crate) struct SpawnedActor {
    pub(crate) handle: MeshActorHandle,
    pub(crate) lookup: MemoryLookup,
    pub(crate) task: tokio::task::JoinHandle<()>,
}

pub(crate) fn build_control_wiring(
    endpoint: &Endpoint,
    config: &ProtocolConfig,
) -> (
    ControlProtocol,
    ControlClient,
    mpsc::UnboundedSender<InternalMsg>,
    mpsc::UnboundedReceiver<InternalMsg>,
) {
    let rpc_timeout = Duration::from_millis(config.probe_timeout_ms.saturating_mul(2).max(1_000));
    let client = ControlClient::new(endpoint.clone(), rpc_timeout);
    let (internal_tx, internal_rx) = mpsc::unbounded_channel();
    let (control_protocol, slot) = ControlProtocol::deferred();
    let handler: Arc<dyn ControlHandler> = Arc::new(ActorControlHandler {
        local_id: peer_id_from_endpoint(endpoint.id()),
        tx: internal_tx.clone(),
        client: client.clone(),
    });
    let _ = slot.set(handler);
    (control_protocol, client, internal_tx, internal_rx)
}

#[expect(
    clippy::too_many_arguments,
    reason = "internal actor constructor wiring endpoint, gossip, signer, and control channels"
)]
pub(crate) async fn spawn_actor(
    endpoint: Endpoint,
    gossip: &Gossip,
    topic_id: TopicId,
    config: ProtocolConfig,
    metadata: MemberMetadata,
    bootstrap: Vec<EndpointAddr>,
    client: ControlClient,
    internal_tx: mpsc::UnboundedSender<InternalMsg>,
    internal_rx: mpsc::UnboundedReceiver<InternalMsg>,
) -> Result<SpawnedActor, MemberMeshError> {
    let lookup = MemoryLookup::new();
    if let Ok(addr_lookup) = endpoint.address_lookup() {
        addr_lookup.add(lookup.clone());
    }
    lookup.set_endpoint_info(endpoint.addr());
    for addr in &bootstrap {
        lookup.add_endpoint_info(addr.clone());
    }

    let bootstrap_ids: Vec<iroh::EndpointId> = bootstrap.iter().map(|a| a.id).collect();
    let signer = IrohSigner::new(endpoint.secret_key().clone());
    let local_peer = peer_id_from_endpoint(endpoint.id());
    let init = LocalNodeInit {
        session_id: random_session_id(),
        joined_at_unix_ms: current_unix_ms(),
        metadata,
        addr_hint: encode_addr_hint(&endpoint.addr()),
    };

    let initial_snapshot = fetch_bootstrap_snapshot(&client, &bootstrap, local_peer).await;
    let start_instant = tokio::time::Instant::now();
    let (engine, init_actions) = match initial_snapshot.as_ref() {
        None => Engine::found(signer, IrohVerifier, init, config, SimInstant(0))?,
        Some(snap) => Engine::join_after_sync(
            signer,
            IrohVerifier,
            init,
            Some(snap),
            config,
            SimInstant(0),
        )?,
    };

    for view in engine.table().all_members_ordered() {
        register_addr_hint(&lookup, view.addr_hint.as_ref());
    }

    let (gossip_tx, mut gossip_rx) = mpsc::unbounded_channel();
    let (bridge, join_waiter) =
        GossipBridge::subscribe(gossip, topic_id, bootstrap_ids.clone(), gossip_tx).await?;

    let fwd_tx = internal_tx.clone();
    let gossip_forwarder = tokio::spawn(async move {
        while let Some(ev) = gossip_rx.recv().await {
            if fwd_tx.send(InternalMsg::Gossip(ev)).is_err() {
                break;
            }
        }
    });

    if !bootstrap_ids.is_empty() {
        join_waiter.wait_bounded(Duration::from_millis(1_500)).await;
    }

    let (cmd_tx, cmd_rx) = mpsc::channel(128);
    let (events_tx, _) = tokio::sync::broadcast::channel(256);

    let ctx = ActorLoopCtx {
        local_peer,
        start_instant,
        engine,
        init_actions,
        bridge,
        client,
        events_tx: events_tx.clone(),
        lookup: lookup.clone(),
        internal_tx,
        internal_rx,
        cmd_rx,
        rpc_sem: Arc::new(Semaphore::new(MAX_INFLIGHT_RPCS)),
    };
    let task = tokio::spawn(run_actor_loop(ctx, gossip_forwarder));

    Ok(SpawnedActor {
        handle: MeshActorHandle { cmd_tx, events_tx },
        lookup,
        task,
    })
}

struct ActorLoopCtx {
    local_peer: PeerId,
    start_instant: tokio::time::Instant,
    engine: Engine<IrohSigner, IrohVerifier>,
    init_actions: Vec<EngineAction>,
    bridge: GossipBridge,
    client: ControlClient,
    events_tx: tokio::sync::broadcast::Sender<MembershipEvent>,
    lookup: MemoryLookup,
    internal_tx: mpsc::UnboundedSender<InternalMsg>,
    internal_rx: mpsc::UnboundedReceiver<InternalMsg>,
    cmd_rx: mpsc::Receiver<ActorCommand>,
    rpc_sem: Arc<Semaphore>,
}

async fn run_actor_loop(mut ctx: ActorLoopCtx, gossip_forwarder: tokio::task::JoinHandle<()>) {
    let _forwarder_guard = AbortHandleGuard(gossip_forwarder);
    let mut next_wakeup = ctx.engine.next_wakeup();

    let init_actions = std::mem::take(&mut ctx.init_actions);
    dispatch_actions(
        init_actions,
        &ctx.bridge,
        &ctx.client,
        &ctx.events_tx,
        &ctx.internal_tx,
        &ctx.rpc_sem,
        &mut next_wakeup,
    )
    .await;

    loop {
        let sleep_fut = async {
            match next_wakeup {
                Some(target) => {
                    let deadline = ctx.start_instant + Duration::from_millis(target.0);
                    tokio::time::sleep_until(deadline).await;
                }
                None => std::future::pending::<()>().await,
            }
        };

        tokio::select! {
            () = sleep_fut => {
                let now = sim_now(ctx.start_instant);
                let actions = ctx.engine.handle_input(now, EngineInput::Tick);
                dispatch_actions(
                    actions,
                    &ctx.bridge,
                    &ctx.client,
                    &ctx.events_tx,
                    &ctx.internal_tx,
                    &ctx.rpc_sem,
                    &mut next_wakeup,
                ).await;
            }
            maybe_internal = ctx.internal_rx.recv() => {
                let Some(msg) = maybe_internal else {
                    break;
                };
                handle_internal_msg(&mut ctx, msg, &mut next_wakeup).await;
            }
            maybe_cmd = ctx.cmd_rx.recv() => {
                let Some(cmd) = maybe_cmd else {
                    break;
                };
                if !handle_actor_cmd(&mut ctx, cmd, &mut next_wakeup).await {
                    break;
                }
            }
        }
    }
}

async fn handle_internal_msg(
    ctx: &mut ActorLoopCtx,
    msg: InternalMsg,
    next_wakeup: &mut Option<SimInstant>,
) {
    let now = sim_now(ctx.start_instant);
    let actions = match msg {
        InternalMsg::Gossip(GossipBridgeEvent::Received(bytes)) => {
            let acts = ctx
                .engine
                .handle_input(now, EngineInput::GossipReceived(bytes));
            register_all_hints(&ctx.engine, &ctx.lookup);
            acts
        }
        InternalMsg::Gossip(GossipBridgeEvent::NeighborUp(peer)) => ctx
            .engine
            .handle_input(now, EngineInput::NeighborHint(peer)),
        InternalMsg::Gossip(GossipBridgeEvent::Lagged) => {
            ctx.engine.handle_input(now, EngineInput::GossipLagged)
        }
        InternalMsg::ControlInbound {
            from,
            msg,
            reply_tx,
        } => {
            let acts = ctx
                .engine
                .handle_input(now, EngineInput::ControlReceived { from, msg });
            register_all_hints(&ctx.engine, &ctx.lookup);
            if let Some(rtx) = reply_tx {
                let reply = ControlMessage::PushPullSync(Box::new(ctx.engine.table().snapshot()));
                let _ = rtx.send(Some(reply));
            }
            acts
        }
        InternalMsg::ControlAsyncInput { from, msg } => {
            let acts = ctx
                .engine
                .handle_input(now, EngineInput::ControlReceived { from, msg });
            register_all_hints(&ctx.engine, &ctx.lookup);
            acts
        }
    };
    dispatch_actions(
        actions,
        &ctx.bridge,
        &ctx.client,
        &ctx.events_tx,
        &ctx.internal_tx,
        &ctx.rpc_sem,
        next_wakeup,
    )
    .await;
}

async fn handle_actor_cmd(
    ctx: &mut ActorLoopCtx,
    cmd: ActorCommand,
    next_wakeup: &mut Option<SimInstant>,
) -> bool {
    let now = sim_now(ctx.start_instant);
    match cmd {
        ActorCommand::Members(reply) => {
            let _ = reply.send(ctx.engine.members_ordered());
        }
        ActorCommand::Leader(reply) => {
            let _ = reply.send(ctx.engine.leader());
        }
        ActorCommand::LeaderWithQuorum { min_alive, reply } => {
            let _ = reply.send(ctx.engine.leader_with_quorum(min_alive));
        }
        ActorCommand::PeerAge { peer_id, reply } => {
            let _ = reply.send(ctx.engine.peer_age(peer_id, now));
        }
        ActorCommand::UpdateMetadata { metadata, reply } => {
            let actions = ctx
                .engine
                .handle_input(now, EngineInput::UpdateMetadata(metadata));
            dispatch_actions(
                actions,
                &ctx.bridge,
                &ctx.client,
                &ctx.events_tx,
                &ctx.internal_tx,
                &ctx.rpc_sem,
                next_wakeup,
            )
            .await;
            let _ = reply.send(Ok(()));
        }
        ActorCommand::JoinAdditional { peers, reply } => {
            let mut ids = Vec::with_capacity(peers.len());
            for addr in peers {
                ids.push(addr.id);
                ctx.lookup.add_endpoint_info(addr);
            }
            let res = ctx.bridge.join_peers(ids.clone()).await;
            if let Some(&first) = ids.first() {
                let snap = ctx.engine.table().snapshot();
                spawn_sync_rpc(
                    peer_id_from_endpoint(first),
                    snap,
                    ctx.client.clone(),
                    ctx.internal_tx.clone(),
                    Arc::clone(&ctx.rpc_sem),
                );
            }
            let _ = reply.send(res);
        }
        ActorCommand::Leave(reply) => {
            let actions = ctx.engine.handle_input(now, EngineInput::LocalLeave);
            dispatch_actions(
                actions,
                &ctx.bridge,
                &ctx.client,
                &ctx.events_tx,
                &ctx.internal_tx,
                &ctx.rpc_sem,
                next_wakeup,
            )
            .await;
            let snap = ctx.engine.table().snapshot();
            let peers: Vec<PeerId> = ctx
                .engine
                .table()
                .members_ordered()
                .into_iter()
                .map(|m| m.peer_id)
                .filter(|&id| id != ctx.local_peer)
                .take(3)
                .collect();
            for p in peers {
                let c = ctx.client.clone();
                let s = snap.clone();
                tokio::spawn(async move {
                    let _ = c
                        .send_one_way(p, &ControlMessage::PushPullSync(Box::new(s)))
                        .await;
                });
            }
            tokio::time::sleep(Duration::from_millis(120)).await;
            let _ = reply.send(Ok(()));
            return false;
        }
    }
    true
}

fn register_all_hints(engine: &Engine<IrohSigner, IrohVerifier>, lookup: &MemoryLookup) {
    for view in engine.table().all_members_ordered() {
        register_addr_hint(lookup, view.addr_hint.as_ref());
    }
}

async fn fetch_bootstrap_snapshot(
    client: &ControlClient,
    bootstrap: &[EndpointAddr],
    local_peer: PeerId,
) -> Option<SyncSnapshot> {
    let empty_snap = SyncSnapshot {
        sender: local_peer,
        lamport_clock: 0,
        claims: Vec::new(),
        indirect_alerts: Vec::new(),
    };
    for addr in bootstrap {
        let req = ControlMessage::PushPullSync(Box::new(empty_snap.clone()));
        if let Ok(ControlMessage::PushPullSync(remote_snap)) =
            client.request_addr(addr.clone(), &req).await
        {
            return Some(*remote_snap);
        }
    }
    None
}

async fn dispatch_actions(
    actions: Vec<EngineAction>,
    bridge: &GossipBridge,
    client: &ControlClient,
    events_tx: &tokio::sync::broadcast::Sender<MembershipEvent>,
    internal_tx: &mpsc::UnboundedSender<InternalMsg>,
    rpc_sem: &Arc<Semaphore>,
    next_wakeup: &mut Option<SimInstant>,
) {
    for action in actions {
        match action {
            EngineAction::BroadcastGossip(bytes) => {
                if let Err(err) = bridge.broadcast(bytes).await {
                    debug!("gossip broadcast skipped: {err}");
                }
            }
            EngineAction::SendControl { to, msg } => {
                let Ok(permit) = Arc::clone(rpc_sem).try_acquire_owned() else {
                    continue;
                };
                let client = client.clone();
                let internal_tx = internal_tx.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    match &msg {
                        ControlMessage::Ping { .. } | ControlMessage::PingReq { .. } => {
                            if let Ok(reply) = client.request(to, &msg).await {
                                let _ = internal_tx.send(InternalMsg::ControlAsyncInput {
                                    from: to,
                                    msg: reply,
                                });
                            }
                        }
                        ControlMessage::Ack { .. }
                        | ControlMessage::Nack { .. }
                        | ControlMessage::PushPullSync(_)
                        | _ => {
                            let _ = client.send_one_way(to, &msg).await;
                        }
                    }
                });
            }
            EngineAction::InitiateSync { to, local_snapshot } => {
                spawn_sync_rpc(
                    to,
                    local_snapshot,
                    client.clone(),
                    internal_tx.clone(),
                    Arc::clone(rpc_sem),
                );
            }
            EngineAction::EmitEvent(ev) => {
                let _ = events_tx.send(ev);
            }
            EngineAction::ScheduleWakeup(at) => {
                *next_wakeup = Some(at);
            }
            _ => {}
        }
    }
}

fn spawn_sync_rpc(
    to: PeerId,
    local_snapshot: SyncSnapshot,
    client: ControlClient,
    internal_tx: mpsc::UnboundedSender<InternalMsg>,
    rpc_sem: Arc<Semaphore>,
) {
    let Ok(permit) = rpc_sem.try_acquire_owned() else {
        return;
    };
    tokio::spawn(async move {
        let _permit = permit;
        let msg = ControlMessage::PushPullSync(Box::new(local_snapshot));
        if let Ok(reply) = client.request(to, &msg).await {
            let _ = internal_tx.send(InternalMsg::ControlAsyncInput {
                from: to,
                msg: reply,
            });
        }
    });
}

fn sim_now(start: tokio::time::Instant) -> SimInstant {
    let ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
    SimInstant(ms)
}

fn current_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

fn random_session_id() -> SessionId {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let seq = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    SessionId(current_unix_ms() ^ seq.rotate_left(32))
}

struct AbortHandleGuard(tokio::task::JoinHandle<()>);

impl Drop for AbortHandleGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

const _: fn(PeerId) -> Result<iroh::EndpointId, MemberMeshError> = endpoint_id_from_peer;
