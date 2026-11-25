#![deny(missing_docs)]
//! Pure Sans-I/O cluster membership, SWIM + Lifeguard failure detection,
//! and deterministic join-order advisory leader election.

/// Workspace scaffold version marker for initial smoke checks.
#[must_use]
pub const fn crate_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
mod tests {
    use super::crate_version;

    #[test]
    fn crate_version_is_non_empty() {
        assert_ne!(crate_version(), "");
    }
}
