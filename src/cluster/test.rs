use std::fs::DirBuilder;

use mongodb::bson::{bson, doc, Bson};
use serde::Deserialize;
use uuid::Uuid;

use super::*;

#[derive(Debug)]
struct AutoShutdownCluster {
    cluster: Cluster,
}

impl AutoShutdownCluster {
    fn with_paths(topology: Topology, paths: Vec<PathBuf>) -> Self {
        Self {
            cluster: Cluster::with_paths(topology, paths).unwrap(),
        }
    }
}

// impl Drop for AutoShutdownCluster {
//     fn drop(&mut self) {
//         let _ = self
//             .client
//             .database("admin")
//             .run_command(doc! { "shutdown": 1 }, None);
//     }
// }

impl std::ops::Deref for AutoShutdownCluster {
    type Target = Cluster;

    fn deref(&self) -> &Self::Target {
        &self.cluster
    }
}

#[derive(Debug, Deserialize)]
struct ReplSetStatus {
    ok: f64,
}

fn create_temp_dir() -> PathBuf {
    let dir = std::env::temp_dir().join(Uuid::new_v4().to_string());
    DirBuilder::new().create(&dir).unwrap();
    dir
}

#[test]
fn create_and_initiate_repl_set() {
    let db_dirs: Vec<_> = (0..3).map(|_| create_temp_dir()).collect();

    let cluster = AutoShutdownCluster::with_paths(
        Topology::ReplicaSet {
            nodes: 3,
            set_name: "test-repl-set".into(),
        },
        db_dirs,
    );

    let response = cluster
        .client
        .database("admin")
        .run_command(doc! { "replSetGetStatus" : 1 }, None)
        .unwrap();

    let ReplSetStatus { ok } = mongodb::bson::from_bson(Bson::Document(response)).unwrap();

    assert_eq!(ok, 1.0);
}
