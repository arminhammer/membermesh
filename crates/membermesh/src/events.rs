use membermesh_core::MembershipEvent;
use tokio::sync::broadcast::{Receiver, error::RecvError};

/// Live stream of [`MembershipEvent`] notifications for a cluster.
#[derive(Debug)]
pub struct EventStream {
    rx: Receiver<MembershipEvent>,
}

impl EventStream {
    pub(crate) const fn new(rx: Receiver<MembershipEvent>) -> Self {
        Self { rx }
    }

    /// Waits for the next [`MembershipEvent`], automatically skipping past lagged messages.
    ///
    /// Returns `None` once the cluster actor shuts down or leaves.
    pub async fn recv(&mut self) -> Option<MembershipEvent> {
        loop {
            match self.rx.recv().await {
                Ok(event) => return Some(event),
                Err(RecvError::Lagged(_)) => {}
                Err(RecvError::Closed) => return None,
            }
        }
    }

    /// Creates a fresh independent subscription starting from future events.
    #[must_use]
    pub fn resubscribe(&self) -> Self {
        Self {
            rx: self.rx.resubscribe(),
        }
    }
}
