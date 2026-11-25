#![deny(missing_docs)]
//! Peer-to-peer cluster membership and advisory leader election over `iroh` and `iroh-gossip`.

pub use membermesh_core::crate_version;

#[cfg(test)]
mod tests {
    use super::crate_version;

    #[test]
    fn reexported_version_matches() {
        assert_ne!(crate_version(), "");
    }
}
