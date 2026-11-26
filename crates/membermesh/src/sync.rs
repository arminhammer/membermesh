use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, OnceLock},
    time::Duration,
};

use iroh::{
    Endpoint, EndpointAddr, EndpointId,
    endpoint::Connection,
    protocol::{AcceptError, ProtocolHandler},
};
use membermesh_core::{
    ControlMessage, DEFAULT_MAX_CONTROL_BYTES, PeerId, decode_control, encode_control,
};
use tracing::debug;

use crate::{
    MemberMeshError,
    error::ExternalError,
    signer::{endpoint_id_from_peer, peer_id_from_endpoint},
};

/// ALPN identifier for `membermesh` direct QUIC probes and Push/Pull anti-entropy sync.
pub const SYNC_ALPN: &[u8] = b"/membermesh/1";

/// Boxed future returned by an inbound [`ControlHandler`].
pub type ControlFuture = Pin<Box<dyn Future<Output = Option<ControlMessage>> + Send>>;

/// Async callback handling inbound [`ControlMessage`] RPCs received over `/membermesh/1`.
pub trait ControlHandler: Send + Sync + std::fmt::Debug + 'static {
    /// Handles a single decoded request from `from`, returning an optional response frame.
    fn handle(&self, from: PeerId, msg: ControlMessage) -> ControlFuture;
}

#[derive(Clone, Debug)]
enum HandlerSlot {
    Bound(Arc<dyn ControlHandler>),
    Deferred(Arc<OnceLock<Arc<dyn ControlHandler>>>),
}

impl HandlerSlot {
    fn get(&self) -> Option<&Arc<dyn ControlHandler>> {
        match self {
            Self::Bound(h) => Some(h),
            Self::Deferred(cell) => cell.get(),
        }
    }
}

/// [`iroh::protocol::ProtocolHandler`] serving `/membermesh/1` direct SWIM probes and Push/Pull sync.
#[derive(Clone, Debug)]
pub struct ControlProtocol {
    handler: HandlerSlot,
    max_control_bytes: usize,
}

impl ControlProtocol {
    /// Creates a new [`ControlProtocol`] delegating inbound requests to `handler`.
    #[must_use]
    pub fn new(handler: Arc<dyn ControlHandler>) -> Self {
        Self {
            handler: HandlerSlot::Bound(handler),
            max_control_bytes: DEFAULT_MAX_CONTROL_BYTES,
        }
    }

    pub(crate) fn deferred() -> (Self, Arc<OnceLock<Arc<dyn ControlHandler>>>) {
        let cell = Arc::new(OnceLock::new());
        let proto = Self {
            handler: HandlerSlot::Deferred(Arc::clone(&cell)),
            max_control_bytes: DEFAULT_MAX_CONTROL_BYTES,
        };
        (proto, cell)
    }

    /// Overrides the maximum accepted control/sync frame size in bytes.
    #[must_use]
    pub const fn with_max_control_bytes(mut self, max_control_bytes: usize) -> Self {
        self.max_control_bytes = max_control_bytes;
        self
    }

    /// Reads one length-prefixed frame, dispatches to `handler`, and writes any reply frame.
    async fn serve_connection(&self, conn: Connection) -> Result<(), MemberMeshError> {
        let Some(handler) = self.handler.get() else {
            return Err(MemberMeshError::Closed);
        };
        let from = peer_id_from_endpoint(conn.remote_id());
        let (mut send, mut recv) = conn
            .accept_bi()
            .await
            .map_err(|err| MemberMeshError::Transport(ExternalError::from_display(err)))?;

        let frame = read_bounded_frame(&mut recv, self.max_control_bytes).await?;
        let msg = decode_control(&frame, self.max_control_bytes)?;
        if let Some(reply) = handler.handle(from, msg).await {
            let encoded = encode_control(&reply, self.max_control_bytes)?;
            write_bounded_frame(&mut send, &encoded).await?;
        }
        send.finish()
            .map_err(|err| MemberMeshError::Transport(ExternalError::from_display(err)))?;
        let _ = conn.closed().await;
        Ok(())
    }
}

impl ProtocolHandler for ControlProtocol {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        if let Err(err) = self.serve_connection(connection).await {
            debug!("control stream rejected/closed: {err}");
        }
        Ok(())
    }
}

/// Direct QUIC RPC client for `/membermesh/1` control messages and Push/Pull sync.
#[derive(Clone, Debug)]
pub struct ControlClient {
    endpoint: Endpoint,
    max_control_bytes: usize,
    rpc_timeout: Duration,
}

impl ControlClient {
    /// Creates a new [`ControlClient`] bound to `endpoint`.
    #[must_use]
    pub const fn new(endpoint: Endpoint, rpc_timeout: Duration) -> Self {
        Self {
            endpoint,
            max_control_bytes: DEFAULT_MAX_CONTROL_BYTES,
            rpc_timeout,
        }
    }

    /// Overrides the maximum encoded frame size in bytes.
    #[must_use]
    pub const fn with_max_control_bytes(mut self, max_control_bytes: usize) -> Self {
        self.max_control_bytes = max_control_bytes;
        self
    }

    /// Sends a one-way [`ControlMessage`] (such as `Ping`, `Ack`, `Nack`) to `to`.
    ///
    /// # Errors
    ///
    /// Returns [`MemberMeshError`] on key conversion, timeout, encoding, or QUIC error.
    pub async fn send_one_way(
        &self,
        to: PeerId,
        msg: &ControlMessage,
    ) -> Result<(), MemberMeshError> {
        let target = endpoint_id_from_peer(to)?;
        self.send_one_way_addr(EndpointAddr::new(target), msg).await
    }

    /// Sends a one-way [`ControlMessage`] to an explicit [`EndpointAddr`].
    ///
    /// # Errors
    ///
    /// Returns [`MemberMeshError`] on timeout, encoding, or QUIC error.
    pub async fn send_one_way_addr(
        &self,
        addr: impl Into<EndpointAddr>,
        msg: &ControlMessage,
    ) -> Result<(), MemberMeshError> {
        let encoded = encode_control(msg, self.max_control_bytes)?;
        let addr = addr.into();
        let fut = async {
            let conn = self
                .endpoint
                .connect(addr, SYNC_ALPN)
                .await
                .map_err(|err| MemberMeshError::Transport(ExternalError::from_display(err)))?;
            let (mut send, _recv) = conn
                .open_bi()
                .await
                .map_err(|err| MemberMeshError::Transport(ExternalError::from_display(err)))?;
            write_bounded_frame(&mut send, &encoded).await?;
            send.finish()
                .map_err(|err| MemberMeshError::Transport(ExternalError::from_display(err)))?;
            Ok::<(), MemberMeshError>(())
        };
        tokio::time::timeout(self.rpc_timeout, fut)
            .await
            .map_err(|_| MemberMeshError::RpcTimeout)?
    }

    /// Sends raw pre-framed bytes (used in tests for malformed/oversized frame checks) and awaits a reply.
    ///
    /// # Errors
    ///
    /// Returns [`MemberMeshError`] on transport failure or timeout.
    pub async fn request_raw(
        &self,
        to: EndpointId,
        raw_frame_body: &[u8],
    ) -> Result<Vec<u8>, MemberMeshError> {
        let fut = async {
            let conn = self
                .endpoint
                .connect(to, SYNC_ALPN)
                .await
                .map_err(|err| MemberMeshError::Transport(ExternalError::from_display(err)))?;
            let (mut send, mut recv) = conn
                .open_bi()
                .await
                .map_err(|err| MemberMeshError::Transport(ExternalError::from_display(err)))?;
            write_bounded_frame(&mut send, raw_frame_body).await?;
            send.finish()
                .map_err(|err| MemberMeshError::Transport(ExternalError::from_display(err)))?;
            read_bounded_frame(&mut recv, self.max_control_bytes).await
        };
        tokio::time::timeout(self.rpc_timeout, fut)
            .await
            .map_err(|_| MemberMeshError::RpcTimeout)?
    }

    /// Executes a request/response [`ControlMessage`] exchange (such as `Ping` -> `Ack` or `PushPullSync`).
    ///
    /// # Errors
    ///
    /// Returns [`MemberMeshError`] on key conversion, timeout, encoding/decoding, or QUIC error.
    pub async fn request(
        &self,
        to: PeerId,
        msg: &ControlMessage,
    ) -> Result<ControlMessage, MemberMeshError> {
        let target = endpoint_id_from_peer(to)?;
        self.request_addr(EndpointAddr::new(target), msg).await
    }

    /// Executes a request/response [`ControlMessage`] exchange against an explicit [`EndpointAddr`].
    ///
    /// # Errors
    ///
    /// Returns [`MemberMeshError`] on timeout, encoding/decoding, or QUIC error.
    pub async fn request_addr(
        &self,
        addr: impl Into<EndpointAddr>,
        msg: &ControlMessage,
    ) -> Result<ControlMessage, MemberMeshError> {
        let encoded = encode_control(msg, self.max_control_bytes)?;
        let addr = addr.into();
        let fut = async {
            let conn = self
                .endpoint
                .connect(addr, SYNC_ALPN)
                .await
                .map_err(|err| MemberMeshError::Transport(ExternalError::from_display(err)))?;
            let (mut send, mut recv) = conn
                .open_bi()
                .await
                .map_err(|err| MemberMeshError::Transport(ExternalError::from_display(err)))?;
            write_bounded_frame(&mut send, &encoded).await?;
            send.finish()
                .map_err(|err| MemberMeshError::Transport(ExternalError::from_display(err)))?;
            let reply_bytes = read_bounded_frame(&mut recv, self.max_control_bytes).await?;
            let reply = decode_control(&reply_bytes, self.max_control_bytes)?;
            Ok::<ControlMessage, MemberMeshError>(reply)
        };
        tokio::time::timeout(self.rpc_timeout, fut)
            .await
            .map_err(|_| MemberMeshError::RpcTimeout)?
    }
}

async fn read_bounded_frame(
    recv: &mut iroh::endpoint::RecvStream,
    max_bytes: usize,
) -> Result<Vec<u8>, MemberMeshError> {
    let mut len_buf = [0u8; 4];
    recv.read_exact(&mut len_buf)
        .await
        .map_err(|err| MemberMeshError::Transport(ExternalError::from_display(err)))?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > max_bytes {
        return Err(MemberMeshError::Core(
            membermesh_core::CoreError::FrameTooLarge {
                size: len,
                max: max_bytes,
            },
        ));
    }
    let mut buf = vec![0u8; len];
    recv.read_exact(&mut buf)
        .await
        .map_err(|err| MemberMeshError::Transport(ExternalError::from_display(err)))?;
    Ok(buf)
}

async fn write_bounded_frame(
    send: &mut iroh::endpoint::SendStream,
    payload: &[u8],
) -> Result<(), MemberMeshError> {
    let len_u32 = u32::try_from(payload.len()).map_err(|_| {
        MemberMeshError::Core(membermesh_core::CoreError::FrameTooLarge {
            size: payload.len(),
            max: u32::MAX as usize,
        })
    })?;
    send.write_all(&len_u32.to_be_bytes())
        .await
        .map_err(|err| MemberMeshError::Transport(ExternalError::from_display(err)))?;
    send.write_all(payload)
        .await
        .map_err(|err| MemberMeshError::Transport(ExternalError::from_display(err)))?;
    Ok(())
}
