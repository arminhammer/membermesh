//! Thin demo CLI for running a `membermesh` cluster node.

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    println!("membermesh-cli v{}", membermesh::crate_version());
    Ok(())
}
