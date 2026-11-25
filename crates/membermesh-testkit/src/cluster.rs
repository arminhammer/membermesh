use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use iroh::{
    Endpoint, EndpointAddr, EndpointId, RelayMode, SecretKey,
    address_lookup::memory::MemoryLookup,
    endpoint::{Connection, IdleTimeout, QuicTransportConfig, WeakConnectionHandle, presets},
    protocol::{AcceptError, DynProtocolHandler, ProtocolHandler, Router},
};

type ConnectionTable = Arc<Mutex<BTreeMap<EndpointId, Vec<(EndpointId, WeakConnectionHandle)>>>>;

/// Thread-safe directed network partition matrix for offline local `iroh` clusters.
#[derive(Clone, Debug, Default)]
pub struct PartitionMatrix {
    blocked: Arc<RwLock<BTreeSet<(EndpointId, EndpointId)>>>,
    connections: ConnectionTable,
}

impl PartitionMatrix {
    /// Blocks directed traffic from `from` to `to` and closes any live connections between them.
    pub fn block(&self, from: EndpointId, to: EndpointId) {
        if let Ok(mut guard) = self.blocked.write() {
            guard.insert((from, to));
        }
        self.close_live_between(from, to);
    }

    /// Blocks bidirectional traffic between `a` and `b` and closes active QUIC connections.
    pub fn sever(&self, a: EndpointId, b: EndpointId) {
        if let Ok(mut guard) = self.blocked.write() {
            guard.insert((a, b));
            guard.insert((b, a));
        }
        self.close_live_between(a, b);
        self.close_live_between(b, a);
    }

    /// Partitions `left` from `right` bidirectionally.
    pub fn partition(&self, left: &[EndpointId], right: &[EndpointId]) {
        for &l in left {
            for &r in right {
                self.sever(l, r);
            }
        }
    }

    /// Removes all partition rules so all endpoints can communicate freely.
    pub fn heal_all(&self) {
        if let Ok(mut guard) = self.blocked.write() {
            guard.clear();
        }
    }

    /// Returns `true` if `from -> to` is currently partitioned.
    #[must_use]
    pub fn is_blocked(&self, from: EndpointId, to: EndpointId) -> bool {
        self.blocked
            .read()
            .is_ok_and(|guard| guard.contains(&(from, to)))
    }

    /// Registers an accepted connection handle on `local` from `remote`.
    pub fn record_connection(&self, local: EndpointId, remote: EndpointId, conn: &Connection) {
        if let Ok(mut guard) = self.connections.lock() {
            let list = guard.entry(local).or_default();
            list.retain(|(_, weak)| weak.upgrade().is_some());
            list.push((remote, conn.weak_handle()));
        }
    }

    fn close_live_between(&self, remote: EndpointId, local: EndpointId) {
        if let Ok(mut guard) = self.connections.lock()
            && let Some(list) = guard.get_mut(&local)
        {
            for (peer, weak) in list.iter() {
                if *peer == remote
                    && let Some(conn) = weak.upgrade()
                {
                    conn.close(403u32.into(), b"partitioned");
                }
            }
            list.retain(|(_, weak)| weak.upgrade().is_some());
        }
    }
}

/// [`ProtocolHandler`] decorator enforcing [`PartitionMatrix`] on inbound QUIC connections.
#[derive(Debug)]
pub struct PartitionedHandler<H> {
    local_id: EndpointId,
    matrix: PartitionMatrix,
    inner: H,
}

impl<H> PartitionedHandler<H> {
    /// Wraps `inner` with [`PartitionMatrix`] enforcement for `local_id`.
    #[must_use]
    pub const fn new(local_id: EndpointId, matrix: PartitionMatrix, inner: H) -> Self {
        Self {
            local_id,
            matrix,
            inner,
        }
    }
}

impl<H: ProtocolHandler> ProtocolHandler for PartitionedHandler<H> {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let remote = connection.remote_id();
        if self.matrix.is_blocked(remote, self.local_id)
            || self.matrix.is_blocked(self.local_id, remote)
        {
            connection.close(403u32.into(), b"partitioned");
            return Ok(());
        }
        self.matrix
            .record_connection(self.local_id, remote, &connection);
        self.inner.accept(connection).await
    }

    async fn shutdown(&self) {
        self.inner.shutdown().await;
    }
}

/// Offline loopback `iroh` node bundle managed by [`LocalIrohCluster`].
pub struct LocalIrohNode {
    /// Node name.
    pub name: String,
    /// Node Ed25519 secret key.
    pub secret_key: SecretKey,
    /// Bound `iroh` endpoint (`RelayMode::Disabled`, loopback only).
    pub endpoint: Endpoint,
    /// Running protocol router (optional if node acts purely as client).
    pub router: Option<Router>,
}

/// Offline multi-node `iroh` cluster harness using `MemoryLookup` and `RelayMode::Disabled`.
pub struct LocalIrohCluster {
    lookup: MemoryLookup,
    matrix: PartitionMatrix,
    nodes: BTreeMap<String, LocalIrohNode>,
}

impl Default for LocalIrohCluster {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalIrohCluster {
    /// Creates an empty offline cluster harness.
    #[must_use]
    pub fn new() -> Self {
        Self {
            lookup: MemoryLookup::new(),
            matrix: PartitionMatrix::default(),
            nodes: BTreeMap::new(),
        }
    }

    /// Returns a reference to the shared [`MemoryLookup`].
    #[must_use]
    pub fn lookup(&self) -> &MemoryLookup {
        &self.lookup
    }

    /// Returns a reference to the [`PartitionMatrix`].
    #[must_use]
    pub const fn matrix(&self) -> &PartitionMatrix {
        &self.matrix
    }

    /// Spawns a new offline [`Endpoint`] without registering router protocols yet.
    ///
    /// # Errors
    ///
    /// Returns an error string if binding the loopback QUIC socket fails.
    pub async fn bind_endpoint(&self, name: &str) -> Result<(SecretKey, Endpoint), String> {
        let seed: [u8; 32] = *blake3::hash(name.as_bytes()).as_bytes();
        let secret_key = SecretKey::from_bytes(&seed);
        let transport = QuicTransportConfig::builder()
            .keep_alive_interval(Duration::from_millis(500))
            .max_idle_timeout(Some(
                IdleTimeout::try_from(Duration::from_secs(2)).map_err(|e| e.to_string())?,
            ))
            .build();

        let endpoint = Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Disabled)
            .secret_key(secret_key.clone())
            .transport_config(transport)
            .bind()
            .await
            .map_err(|e| e.to_string())?;

        if let Ok(addr_lookup) = endpoint.address_lookup() {
            addr_lookup.add(self.lookup.clone());
        }
        self.lookup.set_endpoint_info(endpoint.addr());
        Ok((secret_key, endpoint))
    }

    /// Registers an already-bound endpoint + router under `name`.
    pub fn insert_node(
        &mut self,
        name: &str,
        secret_key: SecretKey,
        endpoint: Endpoint,
        router: Option<Router>,
    ) {
        self.lookup.set_endpoint_info(endpoint.addr());
        self.nodes.insert(
            name.to_owned(),
            LocalIrohNode {
                name: name.to_owned(),
                secret_key,
                endpoint,
                router,
            },
        );
    }

    /// Spawns a node and mounts the provided `(alpn, handler)` pairs behind [`PartitionedHandler`].
    ///
    /// # Errors
    ///
    /// Returns an error string if endpoint binding fails.
    pub async fn spawn_with_protocols(
        &mut self,
        name: &str,
        protocols: Vec<(&[u8], Box<dyn DynProtocolHandler>)>,
    ) -> Result<EndpointId, String> {
        let (secret_key, endpoint) = self.bind_endpoint(name).await?;
        let id = endpoint.id();
        let mut builder = Router::builder(endpoint.clone());
        for (alpn, handler) in protocols {
            builder = builder.accept(alpn, handler);
        }
        let router = builder.spawn();
        self.lookup.set_endpoint_info(endpoint.addr());
        self.insert_node(name, secret_key, endpoint, Some(router));
        Ok(id)
    }

    /// Returns the [`LocalIrohNode`] for `name`.
    #[must_use]
    pub fn node(&self, name: &str) -> Option<&LocalIrohNode> {
        self.nodes.get(name)
    }

    /// Returns the [`EndpointId`] for `name`.
    #[must_use]
    pub fn id(&self, name: &str) -> Option<EndpointId> {
        self.nodes.get(name).map(|n| n.endpoint.id())
    }

    /// Returns the [`EndpointAddr`] for `name`.
    #[must_use]
    pub fn addr(&self, name: &str) -> Option<EndpointAddr> {
        self.nodes.get(name).map(|n| n.endpoint.addr())
    }

    /// Severs traffic between `a` and `b` using [`PartitionMatrix`].
    pub fn sever(&self, a: &str, b: &str) {
        if let (Some(ia), Some(ib)) = (self.id(a), self.id(b)) {
            self.matrix.sever(ia, ib);
        }
    }

    /// Heals all active partitions.
    pub fn heal_all(&self) {
        self.matrix.heal_all();
    }

    /// Shuts down and removes a single node by `name`, freeing its sockets before restart.
    pub async fn remove_and_close(&mut self, name: &str) {
        if let Some(node) = self.nodes.remove(name) {
            if let Some(router) = node.router {
                let _ = router.shutdown().await;
            }
            node.endpoint.close().await;
        }
    }

    /// Shuts down and closes every endpoint cleanly.
    pub async fn shutdown_all(self) {
        for (_, node) in self.nodes {
            if let Some(router) = node.router {
                let _ = router.shutdown().await;
            }
            node.endpoint.close().await;
        }
    }
}
