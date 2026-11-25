mod sync;

use std::{collections::BTreeMap, sync::Arc};

use cucumber::World;
use membermesh::ControlClient;
use membermesh_core::{ControlMessage, MembershipTable};
use membermesh_testkit::cluster::LocalIrohCluster;
use tokio::sync::Mutex;

#[derive(Default, World)]
pub struct MeshWorld {
    pub cluster: Option<LocalIrohCluster>,
    pub tables: BTreeMap<String, Arc<Mutex<MembershipTable>>>,
    pub clients: BTreeMap<String, ControlClient>,
    pub last_reply: Option<ControlMessage>,
    pub last_raw_err: Option<String>,
}

impl std::fmt::Debug for MeshWorld {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MeshWorld")
            .field("tables", &self.tables.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}
