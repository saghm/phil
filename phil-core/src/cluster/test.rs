use mongodb::bson::{bson, doc, Bson};
use serde::Deserialize;
use tempdir::TempDir;
use uuid::Uuid;

use super::*;

#[derive(Debug)]
struct AutoShutdownCluster {
    cluster: Cluster,
}

impl AutoShutdownCluster {
    fn new(options: ClusterOptions) -> Self {
        Self {
            cluster: Cluster::new(options).unwrap(),
        }
    }
}

impl Drop for AutoShutdownCluster {
    fn drop(&mut self) {
        let _ = self
            .client
            .database("admin")
            .run_command(doc! { "shutdown": 1 }, None);
    }
}

impl std::ops::Deref for AutoShutdownCluster {
    type Target = Cluster;

    fn deref(&self) -> &Self::Target {
        &self.cluster
    }
}

#[derive(Debug, Deserialize)]
struct ReplSetStatus {
    set: String,
}

fn create_temp_dir() -> TempDir {
    TempDir::new(&Uuid::new_v4().to_string()).unwrap()
}

#[test]
fn create_and_initiate_repl_set() {
    let db_dirs: Vec<_> = (0..3).map(|_| create_temp_dir()).collect();

    let cluster_options = ClusterOptions::builder()
        .topology(Topology::ReplicaSet {
            nodes: 3,
            set_name: "test-repl-set".into(),
        })
        .paths(
            db_dirs
                .iter()
                .map(|t| t.path().to_path_buf())
                .collect::<Vec<_>>(),
        )
        .build();

    let cluster = AutoShutdownCluster::new(cluster_options);

    let response = cluster
        .client
        .database("admin")
        .run_command(doc! { "replSetGetStatus" : 1 }, None)
        .unwrap();

    let ReplSetStatus { set } = mongodb::bson::from_bson(Bson::Document(response)).unwrap();

    assert_eq!(set, "test-repl-set");
}
