//! Demo CLI for running local and multi-host `membermesh` clusters over `iroh`.

use std::{fmt::Write as _, sync::Arc, time::Duration};

use anyhow::{Context as _, Result, bail};
use clap::{Args, Parser, Subcommand, ValueEnum};
use iroh::{EndpointAddr, RelayMode};
use iroh_gossip::TopicId;
use membermesh::{
    AdmissionPolicy, DEFAULT_DISCOVERY_PORT, DEFAULT_MULTICAST_ADDR, MemberMesh, OpenAdmission,
    SharedSecretAdmission, peer_id_from_endpoint,
};
use membermesh_core::{MemberMetadata, MemberView, PeerId, ProtocolConfig};
use tokio::time::MissedTickBehavior;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    name = "membermesh-cli",
    version,
    about = "Peer-to-peer cluster membership and advisory leader election demo over iroh"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Start a membermesh node and join a cluster topic.
    Start(StartArgs),
}

#[derive(Debug, Args)]
struct StartArgs {
    /// Human-readable cluster topic name (hashed with BLAKE3 into a `TopicId`).
    #[arg(long)]
    topic: String,

    /// Hex-encoded bootstrap `EndpointAddr` ticket printed by an existing node (repeatable).
    #[arg(long = "bootstrap")]
    bootstrap: Vec<String>,

    /// Enable automatic LAN peer discovery via UDP multicast (can be combined with --bootstrap).
    #[arg(long = "auto-discover")]
    auto_discover: bool,

    /// Timing profile controlling SWIM probes and Lifeguard suspicion bounds.
    #[arg(long, value_enum, default_value_t = Profile::Lan)]
    profile: Profile,

    /// Optional shared passphrase enabling keyed BLAKE3 cluster admission.
    #[arg(long)]
    secret: Option<String>,

    /// Optional short UTF-8 metadata label (max 512 bytes) attached to this node's claims.
    #[arg(long)]
    metadata: Option<String>,

    /// Disable iroh relay servers (useful for pure LAN or offline loopback demos).
    #[arg(long)]
    no_relay: bool,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Profile {
    Lan,
    Wan,
    Relay,
}

impl From<Profile> for ProtocolConfig {
    fn from(profile: Profile) -> Self {
        match profile {
            Profile::Lan => Self::lan(),
            Profile::Wan => Self::wan(),
            Profile::Relay => Self::relay(),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    match cli.command {
        Command::Start(args) => run_start(args).await,
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();
}

async fn run_start(args: StartArgs) -> Result<()> {
    let topic_id = TopicId::from_bytes(*blake3::hash(args.topic.as_bytes()).as_bytes());
    let mut bootstrap = Vec::with_capacity(args.bootstrap.len());
    for raw in &args.bootstrap {
        bootstrap.push(decode_ticket(raw).context("invalid --bootstrap ticket")?);
    }

    let metadata = match args.metadata {
        Some(text) => MemberMetadata::try_new(text.into_bytes())?,
        None => MemberMetadata::default(),
    };
    let admission: Arc<dyn AdmissionPolicy> = match args.secret {
        Some(pass) => Arc::new(SharedSecretAdmission::from_passphrase(&pass)),
        None => Arc::new(OpenAdmission),
    };
    let relay_mode = if args.no_relay {
        RelayMode::Disabled
    } else {
        RelayMode::Default
    };

    let mesh = MemberMesh::builder(topic_id)
        .config(args.profile.into())
        .metadata(metadata)
        .admission(admission)
        .bootstrap(bootstrap)
        .auto_discovery(args.auto_discover)
        .relay_mode(relay_mode)
        .spawn()
        .await
        .context("failed to start membermesh node")?;

    let local_peer = peer_id_from_endpoint(mesh.id());
    let ticket = encode_ticket(&mesh.addr()).context("failed to encode local ticket")?;
    println!("Node ID:   {}", mesh.id());
    println!("Topic ID:  {topic_id:?}");
    println!("Bootstrap: --bootstrap {ticket}");
    if args.auto_discover {
        println!(
            "Discovery: enabled (UDP multicast {DEFAULT_MULTICAST_ADDR}:{DEFAULT_DISCOVERY_PORT})"
        );
    } else {
        println!("Discovery: disabled (pass --auto-discover to enable LAN multicast)");
    }
    println!("Press Ctrl+C to gracefully leave...\n");

    let mut events = mesh.subscribe();
    let mut refresh_interval = tokio::time::interval(Duration::from_secs(1));
    refresh_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    render_table(&mesh, local_peer).await?;

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("\nCtrl+C received, leaving cluster gracefully...");
                break;
            }
            maybe_ev = events.recv() => {
                if maybe_ev.is_none() {
                    break;
                }
                render_table(&mesh, local_peer).await?;
            }
            _ = refresh_interval.tick() => {
                render_table(&mesh, local_peer).await?;
            }
        }
    }

    if let Err(err) = mesh.leave().await {
        eprintln!("leave warning: {err}");
    }
    mesh.shutdown().await;
    Ok(())
}

async fn render_table(mesh: &MemberMesh, local_peer: PeerId) -> Result<()> {
    let members = mesh.members().await?;
    let leader_id = mesh.leader().await?.map(|m| m.peer_id);

    println!("SEQ | PEER            | STATUS  | INC | AGE      | ROLE");
    println!("----+-----------------+---------+-----+----------+----------------");
    for member in members {
        print_member_row(mesh, local_peer, leader_id, &member).await;
    }
    println!();
    Ok(())
}

async fn print_member_row(
    mesh: &MemberMesh,
    local_peer: PeerId,
    leader_id: Option<PeerId>,
    member: &MemberView,
) {
    let age_ms = match membermesh::endpoint_id_from_peer(member.peer_id) {
        Ok(ep) => mesh
            .peer_age(ep)
            .await
            .ok()
            .flatten()
            .map_or(0, |a| a.observed_alive_for.as_secs()),
        Err(_) => 0,
    };
    let role = match (
        Some(member.peer_id) == leader_id,
        member.peer_id == local_peer,
    ) {
        (true, true) => "LEADER (self)",
        (true, false) => "LEADER",
        (false, true) => "follower (self)",
        (false, false) => "follower",
    };
    println!(
        "{:<3} | {:<15} | {:<7} | {:<3} | {:>6}s  | {}",
        member.join_seq,
        member.peer_id.fmt_short(),
        member.status.to_string(),
        member.incarnation,
        age_ms,
        role
    );
}

fn encode_ticket(addr: &EndpointAddr) -> Result<String> {
    let bytes = postcard::to_allocvec(addr).context("postcard serialize EndpointAddr")?;
    Ok(bytes_to_hex(&bytes))
}

fn decode_ticket(hex: &str) -> Result<EndpointAddr> {
    let trimmed = hex.trim();
    if trimmed.is_empty() {
        bail!("bootstrap ticket is empty");
    }
    let bytes = hex_to_bytes(trimmed)?;
    postcard::from_bytes(&bytes).context("postcard deserialize EndpointAddr")
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

fn hex_to_bytes(hex: &str) -> Result<Vec<u8>> {
    if !hex.len().is_multiple_of(2) {
        bail!("hex string must have even length");
    }
    let mut out = Vec::with_capacity(hex.len() / 2);
    for &[hi_byte, lo_byte] in hex.as_bytes().as_chunks::<2>().0 {
        let hi = decode_nibble(hi_byte)?;
        let lo = decode_nibble(lo_byte)?;
        out.push((hi << 4) | lo);
    }
    Ok(out)
}

fn decode_nibble(byte: u8) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => bail!("invalid hex byte: 0x{byte:02x}"),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use iroh::SecretKey;

    use super::*;

    #[test]
    fn ticket_roundtrip_preserves_endpoint_addr() {
        let key = SecretKey::from_bytes(&[7u8; 32]);
        let addr = EndpointAddr::new(key.public());
        let encoded = encode_ticket(&addr).expect("encode ticket");
        let decoded = decode_ticket(&encoded).expect("decode ticket");
        assert_eq!(decoded.id, addr.id);
    }

    #[test]
    fn decode_ticket_rejects_invalid_inputs() {
        assert!(decode_ticket("").is_err());
        assert!(decode_ticket("abc").is_err());
        assert!(decode_ticket("zzzz").is_err());
        assert!(decode_ticket("deadbeef").is_err());
    }

    #[test]
    fn cli_accepts_auto_discover_and_bootstrap_flags() {
        let auto_only = Cli::try_parse_from([
            "membermesh-cli",
            "start",
            "--topic",
            "demo",
            "--auto-discover",
        ])
        .expect("parse --auto-discover");
        let Command::Start(args) = auto_only.command;
        assert!(args.auto_discover);
        assert_eq!(args.bootstrap, Vec::<String>::new());

        let ticket_only = Cli::try_parse_from([
            "membermesh-cli",
            "start",
            "--topic",
            "demo",
            "--bootstrap",
            "deadbeef",
        ])
        .expect("parse --bootstrap");
        let Command::Start(args) = ticket_only.command;
        assert!(!args.auto_discover);
        assert_eq!(args.bootstrap, vec!["deadbeef"]);

        let both = Cli::try_parse_from([
            "membermesh-cli",
            "start",
            "--topic",
            "demo",
            "--bootstrap",
            "deadbeef",
            "--auto-discover",
        ])
        .expect("parse combined flags");
        let Command::Start(args) = both.command;
        assert!(args.auto_discover);
        assert_eq!(args.bootstrap, vec!["deadbeef"]);
    }
}
