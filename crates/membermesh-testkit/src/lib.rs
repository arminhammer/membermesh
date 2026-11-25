#![deny(missing_docs)]
//! Shared test-only utilities, BDD hang watchdogs, and simulation harnesses.

/// Offline multi-node `iroh` loopback cluster harness and partition injector.
pub mod cluster;
/// Deterministic Ed25519 signers and verifiers for tests.
pub mod crypto;
/// Co-simulator running `membermesh_core::Engine` over `iroh_gossip::proto::State`.
pub mod gossip_cosim;
/// Deterministic virtual-clock network simulator for `membermesh_core::Engine`.
pub mod sim;

use std::{fmt, future::Future, time::Duration};

/// Error returned when a BDD scenario exceeds its configured hang watchdog timeout.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct WatchdogTimeout {
    /// Name of the scenario that timed out.
    pub scenario: String,
    /// Configured timeout duration.
    pub limit: Duration,
}

impl fmt::Display for WatchdogTimeout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BDD scenario '{}' exceeded hang watchdog limit of {:?}",
            self.scenario, self.limit
        )
    }
}

impl std::error::Error for WatchdogTimeout {}

/// Returns the BDD scenario watchdog limit from `BDD_SCENARIO_TIMEOUT_SECS` (default 30s).
#[must_use]
pub fn scenario_timeout_from_env() -> Duration {
    let secs = std::env::var("BDD_SCENARIO_TIMEOUT_SECS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|&val| val > 0)
        .unwrap_or(30);
    Duration::from_secs(secs)
}

/// Runs an async step or scenario body under a hang watchdog timeout.
///
/// # Errors
///
/// Returns [`WatchdogTimeout`] if `fut` does not complete within `limit`.
pub async fn with_watchdog<F, T>(
    scenario: impl Into<String>,
    limit: Duration,
    fut: F,
) -> Result<T, WatchdogTimeout>
where
    F: Future<Output = T>,
{
    let scenario = scenario.into();
    tokio::time::timeout(limit, fut)
        .await
        .map_err(|_| WatchdogTimeout { scenario, limit })
}
